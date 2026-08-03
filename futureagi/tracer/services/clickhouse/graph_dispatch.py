"""Bounded ClickHouse dispatch for Observe trace/span graphs."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from time import monotonic
from typing import Any
from uuid import UUID

from model_hub.models.choices import AnnotationTypeChoices
from model_hub.models.develop_annotations import AnnotationsLabels
from tracer.services.clickhouse.bounded_graph_reads import (
    GRAPH_CANDIDATE_LIMIT,
    GRAPH_DECORATION_CANDIDATE_DEADLINE_MS,
    GRAPH_MAX_POINTS,
    BoundedGraphReadError,
    GraphCandidateSample,
    aggregate_system_candidate_graph,
    read_graph_candidates,
)
from tracer.services.clickhouse.eval_logger_table import eval_logger_source
from tracer.services.clickhouse.query_builders import (
    TimeSeriesQueryBuilder,
)
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.read_budget import is_read_budget_error
from tracer.utils.helper import get_annotation_labels_for_project

GRAPH_WALL_DEADLINE_MS = 4_400
GRAPH_QUERY_TIMEOUT_MS = 1_200
GRAPH_DECORATION_TIMEOUT_MS = 900
GRAPH_EVENT_LIMIT = 2_000
_GRAPH_BASE_READ_SETTINGS = {
    "max_threads": 1,
    "max_block_size": 8192,
    "max_memory_usage": 256 * 1024 * 1024,
    "max_bytes_to_read": 512 * 1024 * 1024,
    "read_overflow_mode": "throw",
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}
GRAPH_READ_SETTINGS = {
    **_GRAPH_BASE_READ_SETTINGS,
    "max_result_rows": GRAPH_MAX_POINTS + 1,
}
GRAPH_EVENT_READ_SETTINGS = {
    **_GRAPH_BASE_READ_SETTINGS,
    "max_result_rows": GRAPH_EVENT_LIMIT + 1,
}
GRAPH_ENTITY_READ_SETTINGS = {
    **_GRAPH_BASE_READ_SETTINGS,
    "max_result_rows": GRAPH_CANDIDATE_LIMIT + 1,
}

SpanIdentity = tuple[str, str, int]
SpanEntityIdentity = tuple[str, str]


def _validated_project_id(project_id: Any) -> str:
    try:
        if project_id in (None, ""):
            raise ValueError
        return str(UUID(str(project_id)))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("A valid project_id is required") from exc


def _active_filters(filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in filters
        if (item.get("column_id") or item.get("columnId"))
        not in {"created_at", "start_time"}
        or BaseQueryBuilder.is_datetime_complement_filter(item)
    ]


def degraded_graph_response(metric_id: str, exc: Exception) -> dict[str, Any]:
    """Return a stable graph payload without leaking database diagnostics."""

    explicit_code = getattr(exc, "error_code", None)
    if explicit_code in {"deadline_exceeded", "read_budget_exceeded"}:
        error_code = "read_budget_exceeded"
    elif explicit_code == "sample_limit":
        error_code = "sample_limit"
    elif is_read_budget_error(exc):
        error_code = "read_budget_exceeded"
    else:
        error_code = "query_failed"
    return {
        "metric_name": str(metric_id or ""),
        "data": [],
        "query_complete": False,
        "query_status": "degraded",
        "query_error_code": error_code,
    }


def _remaining_timeout_ms(started: float, cap_ms: int) -> int:
    elapsed_ms = (monotonic() - started) * 1000
    remaining_ms = int(GRAPH_WALL_DEADLINE_MS - elapsed_ms)
    if remaining_ms < 25:
        raise BoundedGraphReadError("deadline_exceeded")
    return min(cap_ms, remaining_ms)


def _result_metric_value(point: dict[str, Any], fields: tuple[str, ...]) -> float | int:
    for field in fields:
        if point.get(field) is not None:
            return point[field]
    return 0


_SYSTEM_METRIC_FIELDS: dict[str, tuple[str, tuple[str, ...]]] = {
    "latency": ("latency", ("value", "latency")),
    "traffic": ("traffic", ("traffic", "value")),
    "tokens": ("tokens", ("value", "tokens")),
    "total_tokens": ("total_tokens", ("value", "tokens")),
    "prompt_tokens": ("prompt_tokens", ("value", "prompt_tokens")),
    "input_tokens": ("input_tokens", ("value", "prompt_tokens")),
    "completion_tokens": (
        "completion_tokens",
        ("value", "completion_tokens"),
    ),
    "output_tokens": ("output_tokens", ("value", "completion_tokens")),
    "cost": ("cost", ("value", "cost")),
    "error_rate": ("error_rate", ("value", "error_rate")),
}


def format_system_metric_graph(
    ch_data: dict[str, list[dict[str, Any]]], metric_id: str
) -> dict[str, Any]:
    normalized = str(metric_id or "latency").strip().lower()
    metric_key, value_fields = _SYSTEM_METRIC_FIELDS.get(
        normalized,
        (normalized if normalized in ch_data else "latency", ("value", normalized)),
    )
    metric_points = ch_data.get(metric_key, [])
    traffic_points = ch_data.get("traffic", [])
    traffic_by_timestamp = {
        point.get("timestamp"): _result_metric_value(point, ("traffic", "value"))
        for point in traffic_points
    }
    return {
        "metric_name": metric_id,
        "data": [
            {
                "timestamp": point.get("timestamp"),
                "value": _result_metric_value(point, value_fields),
                "primary_traffic": traffic_by_timestamp.get(point.get("timestamp"), 0),
            }
            for point in metric_points
        ],
    }


def _complete_metadata(
    *, started: float, query_count: int, rows_returned: int
) -> dict[str, Any]:
    return {
        "query_complete": True,
        "query_status": "complete",
        "query_count": query_count,
        "query_elapsed_ms": round((monotonic() - started) * 1000, 3),
        "query_rows_returned": rows_returned,
    }


def _ensure_point_budget(
    *, start_date: datetime, end_date: datetime, interval: str
) -> None:
    """Reject a zero-filled series that would exceed the graph contract."""

    for index, _ in enumerate(
        BaseQueryBuilder._generate_timestamp_range(start_date, end_date, interval)
    ):
        if index >= GRAPH_MAX_POINTS:
            raise BoundedGraphReadError("sample_limit")


def _candidate_trace_ids(sample: GraphCandidateSample) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(row.get("trace_id")) for row in sample.rows if row.get("trace_id")
        )
    )


def _require_renderable_sample(sample: GraphCandidateSample) -> None:
    """Accept only exact rows or an intentional bounded sample marker.

    A ``sample_limit`` candidate set contains proven matches selected by the
    deterministic full-window graph sampler. It is safe to return its bounded
    diagnostics and graph points when its metadata remains explicitly sampled.
    Partial rows from an unclassified or failed query remain a typed degraded
    error.
    """

    if sample.query_complete:
        return
    if (
        sample.query_status == "sampled"
        and sample.sampling_strategy
        and sample.rows
        and sample.sampling_strata > 0
        and sample.sampling_strata_completed == sample.sampling_strata
    ):
        return
    raise BoundedGraphReadError(sample.query_error_code or "sample_limit")


def enforce_exact_graph_data_contract(
    response: dict[str, Any],
) -> dict[str, Any]:
    """Publish points only for exact or explicitly sampled graph reads.

    Bounded candidate rows are useful internally for proving coverage and for
    exact finite decoration queries. Once any phase reports incomplete
    coverage, aggregating those rows produces a sample. ``query_status`` must
    say ``sampled`` before those values can enter ``data``; every other
    incomplete/degraded response remains empty.
    """

    if response.get("query_status") == "sampled":
        planned = response.get("query_sampling_strata")
        completed = response.get("query_sampling_strata_completed")
        if (
            response.get("query_sampling_strategy")
            and isinstance(planned, int)
            and not isinstance(planned, bool)
            and planned > 0
            and completed == planned
        ):
            return response
        return {
            **response,
            "data": [],
            "query_complete": False,
            "query_status": "degraded",
            "query_sampled": False,
            "query_error_code": response.get("query_error_code") or "sample_limit",
        }
    if (
        response.get("query_complete") is False
        or response.get("query_status") == "degraded"
    ):
        return {**response, "data": []}
    return response


def _span_identity(row: dict[str, Any]) -> SpanIdentity | None:
    trace_id = str(row.get("trace_id") or "")
    span_id = str(row.get("id") or "")
    start_time = row.get("start_time")
    if not trace_id or not span_id or not isinstance(start_time, datetime):
        return None
    utc_start = (
        start_time.replace(tzinfo=UTC)
        if start_time.tzinfo is None
        else start_time.astimezone(UTC)
    )
    delta = utc_start - datetime(1970, 1, 1, tzinfo=UTC)
    start_us = (
        delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    )
    return trace_id, span_id, start_us


def _span_entities(rows: tuple[dict[str, Any], ...]) -> tuple[SpanEntityIdentity, ...]:
    """Return trace-scoped score/logger keys at the external-table boundary."""

    return tuple(
        dict.fromkeys(
            (str(row.get("trace_id")), str(row.get("id")))
            for row in rows
            if row.get("trace_id") and row.get("id")
        )
    )


def _span_identity_dates(identities: tuple[SpanIdentity, ...]) -> tuple[date, ...]:
    """Return exact partition dates for immutable physical span identities."""

    return tuple(
        sorted(
            {
                datetime.fromtimestamp(identity[2] // 1_000_000, tz=UTC).date()
                for identity in identities
            }
        )
    )


def _external_entity_scope(
    entities: tuple[SpanEntityIdentity, ...],
    *,
    entities_param: str,
) -> tuple[str, dict[str, Any]]:
    """Scope logger/score rows without trusting bare span IDs.

    ``tracer_eval_logger`` has no project column and migration 0075 guarantees
    both IDs for span/trace targets. Score rows can be legacy-null, but a bare
    OTel span ID cannot prove trace ownership. Both paths therefore fail closed
    unless the external row carries the trace-scoped pair.
    """

    if not entities:
        return "0 = 1", {}
    predicate = (
        "(NOT isNull(trace_id) AND "
        f"(toString(trace_id), observation_span_id) IN %({entities_param})s)"
    )
    return predicate, {entities_param: entities}


def _finite_trace_span_candidates(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    project_id: str,
    started: float,
    timeout_cap_ms: int,
) -> tuple[tuple[SpanIdentity, ...], bool, int, int]:
    """Seed a finite identity superset for the sampled traces.

    Scope predicates intentionally belong only to this physical seed.  Every
    caller must replay the returned identities against all physical versions before a
    row can contribute to a graph.
    """

    trace_ids = _candidate_trace_ids(sample)
    if not trace_ids:
        return (), False, 0, 0
    query = """
    SELECT trace_id, id, start_time
    FROM spans
    PREWHERE project_id = toUUID(%(graph_project_id)s)
      AND trace_id IN %(graph_trace_ids)s
      AND start_time >= %(graph_start_date)s
      AND start_time < %(graph_end_date)s
    GROUP BY trace_id, id, start_time
    ORDER BY start_time DESC, id DESC, trace_id DESC
    LIMIT %(graph_entity_limit)s
    """
    result = analytics.execute_ch_query(
        query,
        {
            "graph_project_id": project_id,
            "graph_trace_ids": trace_ids,
            "graph_start_date": sample.window_start,
            "graph_end_date": sample.window_end,
            "graph_entity_limit": GRAPH_CANDIDATE_LIMIT + 1,
        },
        timeout_ms=_remaining_timeout_ms(started, timeout_cap_ms),
        settings=GRAPH_ENTITY_READ_SETTINGS,
    )
    rows = list(result.data or [])
    truncated = len(rows) > GRAPH_CANDIDATE_LIMIT
    span_identities = tuple(
        dict.fromkeys(
            identity
            for row in rows[:GRAPH_CANDIDATE_LIMIT]
            if (identity := _span_identity(row)) is not None
        )
    )
    return span_identities, truncated, 1, len(rows)


def _trace_system_metric_query(
    *,
    sample: GraphCandidateSample,
    span_identities: tuple[SpanIdentity, ...],
    interval: str,
    project_id: str,
) -> tuple[str, dict[str, Any]]:
    """Aggregate every live span belonging to a finite candidate trace set."""

    trace_ids = _candidate_trace_ids(sample)
    if not trace_ids or not span_identities:
        return "", {}
    bucket_fn = BaseQueryBuilder.time_bucket_expr(interval)
    query = f"""
    SELECT
        {bucket_fn}(latest_start_time) AS time_bucket,
        avg(latest_latency_ms) AS avg_latency,
        sum(latest_total_tokens) AS total_tokens,
        avg(latest_cost) AS avg_cost,
        count() AS traffic_count,
        sum(latest_prompt_tokens) AS prompt_tokens,
        sum(latest_completion_tokens) AS completion_tokens,
        countIf(upper(latest_status) IN ('ERROR', 'ERRORED', 'FAILED'))
            * 100.0 / greatest(count(), 1) AS error_rate
    FROM (
        SELECT
            id AS grouped_id,
            trace_id AS grouped_trace_id,
            start_time AS latest_start_time,
            argMax(latency_ms, _version) AS latest_latency_ms,
            argMax(cost, _version) AS latest_cost,
            argMax(total_tokens, _version) AS latest_total_tokens,
            argMax(prompt_tokens, _version) AS latest_prompt_tokens,
            argMax(completion_tokens, _version) AS latest_completion_tokens,
            argMax(status, _version) AS latest_status,
            argMax(is_deleted, _version) AS latest_is_deleted
        FROM spans
        PREWHERE project_id = toUUID(%(graph_project_id)s)
          AND toDate(start_time) IN %(graph_span_dates)s
          AND trace_id IN %(graph_trace_ids)s
          AND id IN %(graph_span_ids)s
        WHERE (
            trace_id,
            id,
            toUnixTimestamp64Micro(start_time)
        ) IN %(graph_span_identities)s
        GROUP BY trace_id, id, start_time
    )
    WHERE latest_is_deleted = 0
      AND latest_start_time >= %(graph_start_date)s
      AND latest_start_time < %(graph_end_date)s
    GROUP BY time_bucket
    ORDER BY time_bucket
    LIMIT %(graph_point_limit)s
    """
    return query, {
        "graph_project_id": project_id,
        "graph_span_dates": _span_identity_dates(span_identities),
        "graph_trace_ids": trace_ids,
        "graph_span_ids": tuple(
            dict.fromkeys(identity[1] for identity in span_identities)
        ),
        "graph_span_identities": span_identities,
        "graph_start_date": sample.window_start,
        "graph_end_date": sample.window_end,
        "graph_point_limit": GRAPH_MAX_POINTS + 1,
    }


def _fetch_trace_system_metric_graph(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    project_id: str,
    interval: str,
    metric_id: str,
    started: float,
    timeout_ms: int,
) -> dict[str, Any]:
    """Preserve trace-graph semantics by aggregating all child spans."""

    _ensure_point_budget(
        start_date=sample.window_start,
        end_date=sample.window_end,
        interval=interval,
    )
    (
        span_identities,
        span_ids_truncated,
        identity_query_count,
        identity_rows_returned,
    ) = _finite_trace_span_candidates(
        analytics=analytics,
        sample=sample,
        project_id=project_id,
        started=started,
        timeout_cap_ms=timeout_ms,
    )
    query, params = _trace_system_metric_query(
        sample=sample,
        span_identities=span_identities,
        interval=interval,
        project_id=project_id,
    )
    rows: list[dict[str, Any]] = []
    columns: list[str] = []
    metric_query_count = 0
    if query:
        result = analytics.execute_ch_query(
            query,
            params,
            timeout_ms=min(timeout_ms, _remaining_timeout_ms(started, timeout_ms)),
            settings=GRAPH_READ_SETTINGS,
        )
        rows = list(result.data or [])
        columns = list(result.columns or [])
        metric_query_count = 1
    if len(rows) > GRAPH_MAX_POINTS:
        raise BoundedGraphReadError("sample_limit")

    formatter = TimeSeriesQueryBuilder(
        project_id=project_id,
        filters=[],
        interval=interval,
    )
    formatter.start_date = sample.window_start
    formatter.end_date = sample.window_end
    response = format_system_metric_graph(
        formatter.format_result(rows, columns), metric_id
    )
    metadata = _decoration_metadata(
        sample=sample,
        truncated=span_ids_truncated,
        started=started,
        query_count=sample.query_count + identity_query_count + metric_query_count,
        rows_returned=(sample.rows_returned + identity_rows_returned + len(rows)),
    )
    response.update(metadata)
    return enforce_exact_graph_data_contract(response)


def fetch_system_metric_graph_ch(
    *,
    analytics: Any,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    metric_id: str,
    observe_type: str = "trace",
    timeout_ms: int = GRAPH_QUERY_TIMEOUT_MS,
) -> dict[str, Any]:
    """Read an exact graph or an explicitly incomplete cardinality sample."""

    started = monotonic()
    project_id = _validated_project_id(project_id)
    filters = list(filters or [])
    analyzed_window = BaseQueryBuilder.analyze_bounded_datetime_filters(
        filters,
        strict=True,
    )
    if analyzed_window.empty:
        return {
            "metric_name": str(metric_id or ""),
            "data": [],
            **_complete_metadata(started=started, query_count=0, rows_returned=0),
        }
    if _active_filters(filters):
        sample = read_graph_candidates(
            analytics=analytics,
            project_id=project_id,
            filters=filters,
            observe_type=observe_type,
        )
        _require_renderable_sample(sample)
        if str(observe_type or "").strip().lower() == "trace":
            return _fetch_trace_system_metric_graph(
                analytics=analytics,
                sample=sample,
                project_id=project_id,
                interval=interval,
                metric_id=metric_id,
                started=started,
                timeout_ms=timeout_ms,
            )
        response = aggregate_system_candidate_graph(
            sample,
            metric_id=metric_id,
            interval=interval,
        )
        response["query_elapsed_ms"] = round((monotonic() - started) * 1000, 3)
        return enforce_exact_graph_data_contract(response)

    builder = TimeSeriesQueryBuilder(
        project_id=project_id,
        filters=filters,
        interval=interval,
    )
    query, params = builder.build()
    assert builder.start_date is not None and builder.end_date is not None
    _ensure_point_budget(
        start_date=builder.start_date,
        end_date=builder.end_date,
        interval=interval,
    )
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=min(timeout_ms, _remaining_timeout_ms(started, timeout_ms)),
        settings=GRAPH_READ_SETTINGS,
    )
    if len(result.data or []) > GRAPH_MAX_POINTS:
        raise BoundedGraphReadError("sample_limit")
    response = format_system_metric_graph(
        builder.format_result(result.data, result.columns or []), metric_id
    )
    response.update(
        _complete_metadata(
            started=started,
            query_count=1,
            rows_returned=len(result.data or []),
        )
    )
    return response


def normalize_eval_graph_output_type(req_data_config: dict[str, Any]) -> str:
    raw = req_data_config.get("eval_output_type")
    if raw in (None, ""):
        raw = req_data_config.get("output_type", "SCORE")
    normalized = str(raw).strip().lower().replace("-", "_").replace("/", "_")
    normalized = "_".join(normalized.split())
    if normalized in {"bool", "pass_fail", "passfail"}:
        return "PASS_FAIL"
    if normalized in {"str_list", "choice", "choices"}:
        return "CHOICES"
    return "SCORE"


def _selected_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {"false", "failed", "fail", "no", "0"}
    return True


def _eval_entity_scope(
    sample: GraphCandidateSample, observe_type: str
) -> tuple[str, dict[str, Any]]:
    if observe_type == "span":
        span_entities = _span_entities(sample.rows)
        return _external_entity_scope(
            span_entities,
            entities_param="graph_span_entities",
        )

    trace_ids = tuple(
        str(row.get("trace_id")) for row in sample.rows if row.get("trace_id")
    )
    return "toString(trace_id) IN %(graph_trace_ids)s", {"graph_trace_ids": trace_ids}


def _finite_eval_rows(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    observe_type: str,
    eval_config_id: str,
    started: float,
) -> tuple[list[dict[str, Any]], bool, int, int]:
    table, _ = eval_logger_source()
    version = "_version" if table.endswith("_v2") else "_peerdb_version"
    if table.endswith("_v2"):
        deleted_projection = "is_deleted AS graph_is_deleted"
        live_predicate = "graph_is_deleted = 0"
    else:
        deleted_projection = (
            "_peerdb_is_deleted AS graph_is_deleted, deleted AS graph_soft_deleted"
        )
        live_predicate = (
            "graph_is_deleted = 0 AND "
            "(graph_soft_deleted = 0 OR graph_soft_deleted IS NULL)"
        )
    # Neither eval-logger schema has a tracer project column. Project and
    # window ownership are therefore proven by the finite CH25 span/trace
    # identities in ``sample`` before this config-scoped logger read runs.
    entity_predicate, entity_params = _eval_entity_scope(sample, observe_type)
    if not entity_params or not any(entity_params.values()):
        return [], False, 0, 0
    query = f"""
    SELECT created_at, output_bool, output_float, output_str, output_str_list, error
    FROM (
        SELECT
            created_at,
            trace_id,
            observation_span_id,
            output_bool,
            output_float,
            output_str,
            output_str_list,
            error,
            {deleted_projection}
        FROM {table}
        PREWHERE custom_eval_config_id = toUUID(%(graph_eval_config_id)s)
          AND created_at >= %(graph_start_date)s - INTERVAL 7 DAY
          AND created_at < %(graph_end_date)s + INTERVAL 7 DAY
        WHERE {entity_predicate}
        ORDER BY {version} DESC
        LIMIT 1 BY id
    )
    WHERE {live_predicate}
      AND created_at >= %(graph_start_date)s
      AND created_at < %(graph_end_date)s
      AND {entity_predicate}
    ORDER BY created_at
    LIMIT %(graph_event_limit)s
    """
    params = {
        **entity_params,
        "graph_eval_config_id": str(eval_config_id),
        "graph_start_date": sample.window_start,
        "graph_end_date": sample.window_end,
        "graph_event_limit": GRAPH_EVENT_LIMIT + 1,
    }
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=_remaining_timeout_ms(started, GRAPH_DECORATION_TIMEOUT_MS),
        settings=GRAPH_EVENT_READ_SETTINGS,
    )
    rows = list(result.data or [])
    truncated = len(rows) > GRAPH_EVENT_LIMIT
    return rows[:GRAPH_EVENT_LIMIT], truncated, 1, len(rows)


def _json_choices(row: dict[str, Any]) -> list[str]:
    raw = row.get("output_str_list")
    if isinstance(raw, list):
        return [str(value) for value in raw]
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = []
        if isinstance(parsed, list):
            return [str(value) for value in parsed]
    fallback = row.get("output_str")
    return [str(fallback)] if fallback not in (None, "", "ERROR") else []


def _zero_filled_points(
    *,
    sample: GraphCandidateSample,
    interval: str,
    values: dict[datetime, tuple[float, int]],
) -> list[dict[str, Any]]:
    if sample.window_start >= sample.window_end:
        return []
    timestamps = list(
        BaseQueryBuilder._generate_timestamp_range(
            sample.window_start, sample.window_end, interval
        )
    )
    if len(timestamps) > GRAPH_MAX_POINTS:
        raise BoundedGraphReadError("sample_limit")
    return [
        {
            "timestamp": timestamp.isoformat(),
            "value": round(values.get(timestamp, (0.0, 0))[0], 9),
            "primary_traffic": values.get(timestamp, (0.0, 0))[1],
        }
        for timestamp in timestamps
    ]


def _decoration_metadata(
    *,
    sample: GraphCandidateSample,
    truncated: bool,
    started: float,
    query_count: int,
    rows_returned: int,
) -> dict[str, Any]:
    """Describe exact decoration reads without presenting a sample as complete."""

    complete = sample.query_complete and not truncated
    sampled = sample.query_status == "sampled"
    metadata = sample.metadata()
    metadata.update(
        {
            "query_complete": complete,
            "query_status": (
                "sampled" if sampled else "complete" if complete else "degraded"
            ),
            "query_count": query_count,
            "query_elapsed_ms": round((monotonic() - started) * 1000, 3),
            "query_rows_returned": rows_returned,
        }
    )
    if complete:
        metadata.pop("query_error_code", None)
    elif sampled:
        metadata["query_error_code"] = sample.query_error_code or "sample_limit"
    else:
        metadata["query_error_code"] = "sample_limit"
    return metadata


def _finite_eval_graph(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    observe_type: str,
    interval: str,
    req_data_config: dict[str, Any],
    started: float,
) -> dict[str, Any]:
    metric_id = str(req_data_config.get("id") or "")
    output_type = normalize_eval_graph_output_type(req_data_config)
    selected = req_data_config.get("value")
    choices = list(req_data_config.get("choices") or [])
    selected_choice = str(selected) if selected not in (None, "") else None
    if output_type == "CHOICES" and selected_choice is None and choices:
        selected_choice = str(choices[0])

    rows, truncated, event_query_count, event_rows_returned = _finite_eval_rows(
        analytics=analytics,
        sample=sample,
        observe_type=observe_type,
        eval_config_id=metric_id,
        started=started,
    )
    states: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        if row.get("error") or str(row.get("output_str") or "") == "ERROR":
            continue
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        bucket = BaseQueryBuilder._normalize_timestamp(created_at, interval)
        state = states.setdefault(bucket, {"sum": 0.0, "count": 0})
        if output_type == "PASS_FAIL" and row.get("output_bool") is not None:
            matched = bool(row.get("output_bool")) == _selected_bool(selected)
            state["sum"] += 100.0 if matched else 0.0
            state["count"] += 1
        elif output_type == "CHOICES":
            row_choices = _json_choices(row)
            if row_choices:
                state["sum"] += 100.0 if selected_choice in row_choices else 0.0
                state["count"] += 1
        elif row.get("output_float") is not None:
            state["sum"] += float(row["output_float"]) * 100.0
            state["count"] += 1

    values = {
        bucket: (state["sum"] / max(state["count"], 1), state["count"])
        for bucket, state in states.items()
    }
    metadata = _decoration_metadata(
        sample=sample,
        truncated=truncated,
        started=started,
        query_count=sample.query_count + event_query_count,
        rows_returned=sample.rows_returned + event_rows_returned,
    )
    return enforce_exact_graph_data_contract(
        {
            "metric_name": metric_id,
            "data": _zero_filled_points(
                sample=sample,
                interval=interval,
                values=values,
            ),
            **metadata,
        }
    )


def fetch_eval_graph_ch(
    *,
    analytics: Any,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str = "trace",
    timeout_ms: int = GRAPH_QUERY_TIMEOUT_MS,
) -> dict[str, Any]:
    del timeout_ms  # Eval decorations use the stricter shared wall budget.
    started = monotonic()
    project_id = _validated_project_id(project_id)
    filters = list(filters or [])
    sample = read_graph_candidates(
        analytics=analytics,
        project_id=project_id,
        filters=filters,
        observe_type=observe_type,
        deadline_ms=GRAPH_DECORATION_CANDIDATE_DEADLINE_MS,
        allow_time_only_seed=not _active_filters(filters),
    )
    _require_renderable_sample(sample)
    return _finite_eval_graph(
        analytics=analytics,
        sample=sample,
        observe_type=observe_type,
        interval=interval,
        req_data_config=req_data_config,
        started=started,
    )


def annotation_output_type(
    label: AnnotationsLabels, requested: str | None = None
) -> str:
    if requested:
        return str(requested)
    if label.type in (
        AnnotationTypeChoices.NUMERIC.value,
        AnnotationTypeChoices.STAR.value,
    ):
        return "float"
    if label.type == AnnotationTypeChoices.THUMBS_UP_DOWN.value:
        return "bool"
    if label.type == AnnotationTypeChoices.CATEGORICAL.value:
        return "str_list"
    return "text"


def _finite_trace_span_ids(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    project_id: str,
    started: float,
) -> tuple[tuple[SpanIdentity, ...], bool, int, int]:
    """Resolve finite trace-span candidates against their global latest state."""

    (
        candidate_span_identities,
        truncated,
        seed_query_count,
        seed_rows_returned,
    ) = _finite_trace_span_candidates(
        analytics=analytics,
        sample=sample,
        project_id=project_id,
        started=started,
        timeout_cap_ms=GRAPH_DECORATION_TIMEOUT_MS,
    )
    trace_ids = _candidate_trace_ids(sample)
    if not trace_ids or not candidate_span_identities:
        return (), truncated, seed_query_count, seed_rows_returned
    query = """
    SELECT grouped_trace_id AS trace_id, grouped_id AS id, latest_start_time AS start_time
    FROM (
        SELECT
            id AS grouped_id,
            trace_id AS grouped_trace_id,
            start_time AS latest_start_time,
            argMax(is_deleted, _version) AS latest_is_deleted
        FROM spans
        PREWHERE project_id = toUUID(%(graph_project_id)s)
          AND toDate(start_time) IN %(graph_span_dates)s
          AND trace_id IN %(graph_trace_ids)s
          AND id IN %(graph_span_ids)s
        WHERE (
            trace_id,
            id,
            toUnixTimestamp64Micro(start_time)
        ) IN %(graph_span_identities)s
        GROUP BY trace_id, id, start_time
    )
    WHERE latest_is_deleted = 0
      AND latest_start_time >= %(graph_start_date)s
      AND latest_start_time < %(graph_end_date)s
    ORDER BY latest_start_time DESC, grouped_id DESC, grouped_trace_id DESC
    LIMIT %(graph_entity_limit)s
    """
    result = analytics.execute_ch_query(
        query,
        {
            "graph_project_id": project_id,
            "graph_span_dates": _span_identity_dates(candidate_span_identities),
            "graph_trace_ids": trace_ids,
            "graph_span_ids": tuple(
                dict.fromkeys(identity[1] for identity in candidate_span_identities)
            ),
            "graph_span_identities": candidate_span_identities,
            "graph_start_date": sample.window_start,
            "graph_end_date": sample.window_end,
            "graph_entity_limit": GRAPH_CANDIDATE_LIMIT + 1,
        },
        timeout_ms=_remaining_timeout_ms(started, GRAPH_DECORATION_TIMEOUT_MS),
        settings=GRAPH_ENTITY_READ_SETTINGS,
    )
    rows = list(result.data or [])
    span_identities = tuple(
        dict.fromkeys(
            identity for row in rows if (identity := _span_identity(row)) is not None
        )
    )
    return (
        span_identities,
        truncated,
        seed_query_count + 1,
        seed_rows_returned + len(rows),
    )


def _annotation_entity_scope(
    sample: GraphCandidateSample,
    observe_type: str,
    trace_span_identities: tuple[SpanIdentity, ...],
) -> tuple[str, dict[str, Any]]:
    if observe_type == "span":
        span_entities = _span_entities(sample.rows)
        return _external_entity_scope(
            span_entities,
            entities_param="graph_span_entities",
        )

    trace_ids = tuple(
        str(row.get("trace_id")) for row in sample.rows if row.get("trace_id")
    )
    predicates: list[str] = []
    params: dict[str, Any] = {}
    if trace_ids:
        predicates.append("toString(trace_id) IN %(graph_trace_ids)s")
        params["graph_trace_ids"] = trace_ids
    if trace_span_identities:
        span_entities = tuple(
            dict.fromkeys(
                (identity[0], identity[1]) for identity in trace_span_identities
            )
        )
        span_predicate, span_params = _external_entity_scope(
            span_entities,
            entities_param="graph_span_entities",
        )
        predicates.append(span_predicate)
        params.update(span_params)
    return "(" + " OR ".join(predicates) + ")", params


def _finite_annotation_rows(
    *,
    analytics: Any,
    sample: GraphCandidateSample,
    project_id: str,
    observe_type: str,
    trace_span_identities: tuple[SpanIdentity, ...],
    label_id: str,
    started: float,
) -> tuple[list[dict[str, Any]], bool, int, int]:
    entity_predicate, entity_params = _annotation_entity_scope(
        sample, observe_type, trace_span_identities
    )
    if not entity_params or not any(entity_params.values()):
        return [], False, 0, 0
    query = f"""
    SELECT created_at, value
    FROM (
        SELECT
            created_at,
            trace_id,
            observation_span_id,
            tracer_project_id AS graph_score_project_id,
            value,
            _peerdb_is_deleted AS graph_is_deleted,
            deleted AS graph_soft_deleted
        FROM model_hub_score
        PREWHERE label_id = toUUID(%(graph_label_id)s)
          AND created_at >= %(graph_start_date)s - INTERVAL 7 DAY
          AND created_at < %(graph_end_date)s + INTERVAL 7 DAY
        WHERE tracer_project_id = toUUID(%(graph_project_id)s)
          AND {entity_predicate}
        ORDER BY _peerdb_version DESC
        LIMIT 1 BY id
    )
    WHERE graph_score_project_id = toUUID(%(graph_project_id)s)
      AND graph_is_deleted = 0
      AND graph_soft_deleted = 0
      AND created_at >= %(graph_start_date)s
      AND created_at < %(graph_end_date)s
      AND {entity_predicate}
    ORDER BY created_at
    LIMIT %(graph_event_limit)s
    """
    params = {
        **entity_params,
        "graph_project_id": project_id,
        "graph_label_id": str(label_id),
        "graph_start_date": sample.window_start,
        "graph_end_date": sample.window_end,
        "graph_event_limit": GRAPH_EVENT_LIMIT + 1,
    }
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=_remaining_timeout_ms(started, GRAPH_DECORATION_TIMEOUT_MS),
        settings=GRAPH_EVENT_READ_SETTINGS,
    )
    rows = list(result.data or [])
    truncated = len(rows) > GRAPH_EVENT_LIMIT
    return rows[:GRAPH_EVENT_LIMIT], truncated, 1, len(rows)


def _annotation_value(payload: Any, output_type: str, selected: Any) -> float | None:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
    if not isinstance(payload, dict):
        return None
    if output_type == "float":
        raw = payload.get("rating", payload.get("value"))
        try:
            return float(raw) if raw is not None and not isinstance(raw, bool) else None
        except (TypeError, ValueError, OverflowError):
            return None
    if output_type == "bool":
        raw = payload.get("value")
        wanted = "up" if _selected_bool(selected) else "down"
        return 100.0 if str(raw).lower() == wanted else 0.0
    if output_type == "str_list":
        values = payload.get("selected") or []
        if isinstance(values, str):
            values = [values]
        return 100.0 if str(selected) in {str(value) for value in values} else 0.0
    return 1.0


def fetch_annotation_graph_ch(
    *,
    analytics: Any,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str,
    timeout_ms: int = GRAPH_QUERY_TIMEOUT_MS,
) -> dict[str, Any]:
    del timeout_ms  # Decoration reads use the stricter shared wall budget above.
    started = monotonic()
    project_id = _validated_project_id(project_id)
    label_id = str(req_data_config.get("id") or "")
    if not label_id:
        raise ValueError("Annotation label ID is required")
    # PostgreSQL is used only for the small ownership/name record. Trace/span
    # telemetry and graph values remain CH25-only.
    label = get_annotation_labels_for_project(project_id).get(id=label_id)
    sample = read_graph_candidates(
        analytics=analytics,
        project_id=project_id,
        filters=list(filters or []),
        observe_type=observe_type,
        deadline_ms=GRAPH_DECORATION_CANDIDATE_DEADLINE_MS,
        allow_time_only_seed=True,
    )
    _require_renderable_sample(sample)
    trace_span_identities: tuple[SpanIdentity, ...] = ()
    trace_span_ids_truncated = False
    span_identity_query_count = 0
    span_identity_rows_returned = 0
    if observe_type == "trace" and any(row.get("trace_id") for row in sample.rows):
        (
            trace_span_identities,
            trace_span_ids_truncated,
            span_identity_query_count,
            span_identity_rows_returned,
        ) = _finite_trace_span_ids(
            analytics=analytics, sample=sample, project_id=project_id, started=started
        )
    (
        rows,
        truncated,
        event_query_count,
        event_rows_returned,
    ) = _finite_annotation_rows(
        analytics=analytics,
        sample=sample,
        project_id=project_id,
        observe_type=observe_type,
        trace_span_identities=trace_span_identities,
        label_id=label_id,
        started=started,
    )
    output_type = annotation_output_type(label, req_data_config.get("output_type"))
    selected = req_data_config.get("value")
    states: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        created_at = row.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        value = _annotation_value(row.get("value"), output_type, selected)
        if value is None:
            continue
        bucket = BaseQueryBuilder._normalize_timestamp(created_at, interval)
        state = states.setdefault(bucket, {"sum": 0.0, "count": 0})
        state["sum"] += value
        state["count"] += 1
    values = {
        bucket: (state["sum"] / max(state["count"], 1), state["count"])
        for bucket, state in states.items()
    }
    metadata = _decoration_metadata(
        sample=sample,
        truncated=truncated or trace_span_ids_truncated,
        started=started,
        query_count=(
            sample.query_count + span_identity_query_count + event_query_count
        ),
        rows_returned=(
            sample.rows_returned + span_identity_rows_returned + event_rows_returned
        ),
    )
    return enforce_exact_graph_data_contract(
        {
            "metric_name": label_id,
            "name": label.name,
            "data": _zero_filled_points(
                sample=sample,
                interval=interval,
                values=values,
            ),
            **metadata,
        }
    )


__all__ = [
    "GRAPH_READ_SETTINGS",
    "GRAPH_WALL_DEADLINE_MS",
    "annotation_output_type",
    "degraded_graph_response",
    "enforce_exact_graph_data_contract",
    "fetch_annotation_graph_ch",
    "fetch_eval_graph_ch",
    "fetch_system_metric_graph_ch",
    "format_system_metric_graph",
    "normalize_eval_graph_output_type",
]
