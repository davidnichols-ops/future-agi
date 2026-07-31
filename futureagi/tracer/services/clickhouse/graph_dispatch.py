import concurrent.futures
import copy
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from model_hub.models.choices import AnnotationTypeChoices
from model_hub.models.develop_annotations import AnnotationsLabels
from tracer.services.clickhouse.query_builders import (
    AnnotationGraphQueryBuilder,
    TimeSeriesQueryBuilder,
)
from tracer.services.clickhouse.read_budget import is_read_budget_error
from tracer.utils.helper import get_annotation_labels_for_project

GRAPH_READ_TIMEOUT_MS = 750
GRAPH_READ_SETTINGS = {
    "max_threads": 2,
    "max_memory_usage": 256 * 1024 * 1024,
    "max_bytes_to_read": 1024 * 1024 * 1024,
    "read_overflow_mode": "throw",
    "max_result_rows": 2000,
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}

# Raw system-metric graphs are the one graph shape that must aggregate a
# project/time slice of ``spans`` after applying arbitrary row predicates.
# Production evidence shows this shape staying below the 256 MiB memory cap
# while narrowly exhausting the generic 750 ms / 1 GiB read limits. Keep eval
# and annotation graphs on the tighter defaults; grant only this shape bounded
# headroom after the builder has pruned unused metric columns.
SYSTEM_GRAPH_READ_TIMEOUT_MS = 1250
SYSTEM_GRAPH_READ_SETTINGS = {
    **GRAPH_READ_SETTINGS,
    "max_bytes_to_read": 1536 * 1024 * 1024,
}

# Raw attribute graphs are the only system-graph shape that still has to read
# typed Maps.  One 14/30-day statement can cross the byte or memory guard even
# when each UTC day is cheap.  Execute non-overlapping day windows with a small
# fixed fan-out and one shared deadline; every ClickHouse statement remains
# independently bounded and the original builder performs the final zero-fill.
SEGMENTED_GRAPH_MAX_WORKERS = 4
SEGMENTED_GRAPH_MAX_WINDOWS = 32
SEGMENTED_GRAPH_QUERY_TIMEOUT_MS = 1000
SEGMENTED_GRAPH_WALL_SECONDS = 2.2
SEGMENTED_GRAPH_WINDOW = timedelta(days=1)


def degraded_graph_response(metric_id: str, exc: Exception) -> dict[str, Any]:
    """Return a safe, machine-readable graph failure without leaking CH text."""
    error_code = "read_budget_exceeded" if is_read_budget_error(exc) else "query_failed"
    return {
        "metric_name": metric_id,
        "data": [],
        "query_complete": False,
        "query_status": "degraded",
        "query_error_code": error_code,
    }


def _utc_iso(value: datetime) -> str:
    """Serialize ClickHouse's naive-UTC boundaries without timezone ambiguity."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def normalize_eval_graph_output_type(req_data_config: dict[str, Any]) -> str:
    """Translate both backend enums and the frontend graph wire values."""
    raw_value = req_data_config.get("eval_output_type")
    if raw_value in (None, ""):
        raw_value = req_data_config.get("output_type", "SCORE")

    normalized = str(raw_value).strip().lower().replace("-", "_").replace("/", "_")
    normalized = "_".join(normalized.split())
    if normalized in {"bool", "pass_fail", "passfail"}:
        return "PASS_FAIL"
    if normalized in {"str_list", "choice", "choices"}:
        return "CHOICES"
    if normalized in {"float", "score", "percentage", "numeric", "number"}:
        return "SCORE"
    return "SCORE"


def _selected_bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {
            "false",
            "failed",
            "fail",
            "no",
            "0",
        }
    return True


def _invert_pass_fail_rows(rows: list[Any], columns: list[str]) -> list[Any]:
    """Turn pass-rate rows into fail-rate rows before zero filling."""
    if not rows:
        return rows

    value_index = columns.index("value") if "value" in columns else 1
    inverted = []
    for row in rows:
        if isinstance(row, dict):
            updated = dict(row)
            value = updated.get("value")
            updated["value"] = None if value is None else 100.0 - float(value)
            inverted.append(updated)
            continue

        updated = list(row)
        if len(updated) > value_index and updated[value_index] is not None:
            updated[value_index] = 100.0 - float(updated[value_index])
        inverted.append(tuple(updated))
    return inverted


def _format_eval_graph_response(
    formatted: Any,
    *,
    metric_id: str,
    selected_choice: Any = None,
) -> dict[str, Any]:
    """Keep every observe graph endpoint on its contracted single-series shape."""
    series = formatted
    if isinstance(formatted, list):
        series = None
        if selected_choice not in (None, ""):
            choice_suffix = f" - {selected_choice}"
            series = next(
                (
                    item
                    for item in formatted
                    if str(item.get("name", "")).endswith(choice_suffix)
                ),
                None,
            )
        elif formatted:
            series = formatted[0]

    return {
        "metric_name": metric_id,
        "data": series.get("data", []) if isinstance(series, dict) else [],
    }


_SYSTEM_METRIC_GRAPH_FIELDS: dict[str, tuple[str, tuple[str, ...]]] = {
    "latency": ("latency", ("value", "latency")),
    "tokens": ("tokens", ("value", "tokens")),
    "total_tokens": ("total_tokens", ("value", "tokens")),
    "cost": ("cost", ("value", "cost")),
    "traffic": ("traffic", ("traffic", "value")),
    "prompt_tokens": ("prompt_tokens", ("value", "prompt_tokens")),
    "input_tokens": ("input_tokens", ("value", "prompt_tokens")),
    "completion_tokens": ("completion_tokens", ("value", "completion_tokens")),
    "output_tokens": ("output_tokens", ("value", "completion_tokens")),
    "error_rate": ("error_rate", ("value", "error_rate")),
}


def _metric_point_value(point: dict[str, Any], fields: tuple[str, ...]) -> Any:
    for field in fields:
        if field in point and point[field] is not None:
            return point[field]
    return 0


def format_system_metric_graph(
    ch_data: dict[str, list[dict[str, Any]]], metric_id: str
) -> dict[str, Any]:
    normalized_metric = str(metric_id or "").strip().lower()
    metric_key, value_fields = _SYSTEM_METRIC_GRAPH_FIELDS.get(
        normalized_metric,
        (
            normalized_metric if normalized_metric in ch_data else "latency",
            ("value", normalized_metric),
        ),
    )
    metric_points = ch_data.get(metric_key, [])
    traffic_points = ch_data.get("traffic", [])
    traffic_by_ts = {
        point.get("timestamp"): _metric_point_value(point, ("traffic", "value"))
        for point in traffic_points
    }
    return {
        "metric_name": metric_id,
        "data": [
            {
                "timestamp": p.get("timestamp"),
                "value": _metric_point_value(p, value_fields),
                "primary_traffic": traffic_by_ts.get(p.get("timestamp"), 0),
            }
            for p in metric_points
        ],
    }


def _segmented_graph_filters(
    filters: list[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    """Replace the request's time predicates with one exact half-open window."""
    scoped = [
        copy.deepcopy(item)
        for item in filters
        if str(item.get("column_id") or item.get("columnId") or "")
        not in {"created_at", "start_time"}
    ]
    scoped.append(
        {
            "column_id": "created_at",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "datetime",
                "filter_op": "between",
                "filter_value": [start, end],
            },
        }
    )
    return scoped


def _segmented_graph_windows(
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime]]:
    """Partition a UTC request into non-overlapping midnight-aligned days."""
    if start >= end:
        return []
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        next_midnight = cursor.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_midnight <= cursor:
            next_midnight += SEGMENTED_GRAPH_WINDOW
        window_end = min(end, next_midnight)
        windows.append((cursor, window_end))
        cursor = window_end
        if len(windows) > SEGMENTED_GRAPH_MAX_WINDOWS:
            raise TimeoutError("attribute graph exceeds the bounded window count")
    return windows


def _fetch_segmented_attribute_graph_rows(
    *,
    analytics,
    builder: TimeSeriesQueryBuilder,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Execute exact raw-attribute buckets under one bounded wall deadline."""
    if builder.start_date is None or builder.end_date is None:
        raise ValueError("graph builder did not parse a bounded time range")

    windows = _segmented_graph_windows(builder.start_date, builder.end_date)
    if not windows:
        return [], []
    deadline = time.monotonic() + SEGMENTED_GRAPH_WALL_SECONDS

    def execute_window(index: int, window: tuple[datetime, datetime]):
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms < 25:
            raise TimeoutError("attribute graph shared deadline expired")
        start, end = window
        chunk_builder = TimeSeriesQueryBuilder(
            project_id=builder.project_id,
            project_ids=builder.project_ids,
            filters=_segmented_graph_filters(builder.filters, start=start, end=end),
            interval=builder.interval,
            system_metric_filters=builder.system_metric_filters,
            observe_type=builder.observe_type,
            metric_id=builder.metric_id,
            single_metric=builder.single_metric,
            # Once the parent selected the raw path, every segment must cover
            # its complete half-open window. A rollup segment can omit partial
            # boundary hours and would make the merged response incomplete.
            allow_attr_rollup=False,
        )
        query, params = chunk_builder.build()
        if chunk_builder.query_source != "raw" or not chunk_builder.attribute_filtered:
            raise RuntimeError(
                "segmented graph did not remain on the raw attribute path"
            )
        result = analytics.execute_ch_query(
            query,
            params,
            timeout_ms=min(SEGMENTED_GRAPH_QUERY_TIMEOUT_MS, remaining_ms),
            settings=SYSTEM_GRAPH_READ_SETTINGS,
        )
        return index, result

    results: list[Any] = [None] * len(windows)
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=min(SEGMENTED_GRAPH_MAX_WORKERS, len(windows))
    )
    futures = [
        executor.submit(execute_window, index, window)
        for index, window in enumerate(windows)
    ]
    completed = False
    try:
        for future in concurrent.futures.as_completed(
            futures,
            timeout=SEGMENTED_GRAPH_WALL_SECONDS,
        ):
            index, result = future.result()
            results[index] = result
        completed = all(result is not None for result in results)
    except concurrent.futures.TimeoutError as exc:
        raise TimeoutError("attribute graph shared deadline expired") from exc
    finally:
        executor.shutdown(wait=completed, cancel_futures=True)

    if not completed:
        raise TimeoutError("attribute graph did not complete every window")
    columns = list(results[0].columns or [])
    rows_by_bucket: dict[Any, dict[str, Any]] = {}
    weighted_fields = ("avg_latency", "avg_cost", "error_rate")
    summed_fields = (
        "total_tokens",
        "traffic_count",
        "prompt_tokens",
        "completion_tokens",
    )
    for result in results:
        if list(result.columns or []) != columns:
            raise RuntimeError("attribute graph window columns did not match")
        for row in result.data:
            if not isinstance(row, dict):
                raise RuntimeError("attribute graph window returned an invalid row")
            bucket = row.get("time_bucket")
            if bucket is None:
                raise RuntimeError("attribute graph window omitted its bucket")
            existing = rows_by_bucket.get(bucket)
            if existing is None:
                copied = dict(row)
                traffic = float(copied.get("traffic_count") or 0)
                copied["_segment_weight"] = traffic
                for field in weighted_fields:
                    copied[f"_{field}_weighted"] = (
                        float(copied.get(field) or 0) * traffic
                    )
                rows_by_bucket[bucket] = copied
                continue

            # Day chunks are already disjoint for hour/day graphs. Week/month/
            # year graphs intentionally produce the same outer bucket in
            # adjacent chunks; merge their aggregate states exactly. The raw
            # spans schema keeps latency and cost non-nullable, so traffic is
            # the denominator for both averages and for error-rate percentage.
            traffic = float(row.get("traffic_count") or 0)
            existing["_segment_weight"] += traffic
            for field in weighted_fields:
                existing[f"_{field}_weighted"] += float(row.get(field) or 0) * traffic
            for field in summed_fields:
                existing[field] = (existing.get(field) or 0) + (row.get(field) or 0)

    for row in rows_by_bucket.values():
        weight = row.pop("_segment_weight")
        for field in weighted_fields:
            weighted = row.pop(f"_{field}_weighted")
            row[field] = weighted / weight if weight else 0
    return [rows_by_bucket[key] for key in sorted(rows_by_bucket)], columns


def annotation_output_type(label: AnnotationsLabels, requested: str = None) -> str:
    if requested:
        return requested
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


def fetch_system_metric_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    metric_id: str,
    observe_type: str = "trace",
    timeout_ms: int = SYSTEM_GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    builder = TimeSeriesQueryBuilder(
        project_id=str(project_id),
        filters=filters,
        interval=interval,
        observe_type=observe_type,
        metric_id=metric_id,
        single_metric=True,
    )
    query, params = builder.build()
    if (
        builder.query_source == "raw"
        and builder.attribute_filtered
        and builder.raw_segmentation_safe
    ):
        rows, columns = _fetch_segmented_attribute_graph_rows(
            analytics=analytics,
            builder=builder,
        )
    else:
        result = analytics.execute_ch_query(
            query,
            params,
            timeout_ms=timeout_ms,
            settings=SYSTEM_GRAPH_READ_SETTINGS,
        )
        rows, columns = result.data, result.columns or []
    ch_data = builder.format_result(rows, columns)
    response = format_system_metric_graph(ch_data, metric_id)
    if builder.rollup_window_adjusted:
        assert builder.rollup_window_start is not None
        assert builder.rollup_window_end is not None
        response.update(
            {
                "query_complete": True,
                "query_status": "adjusted",
                "query_window_adjusted": True,
                "query_window_start": _utc_iso(builder.rollup_window_start),
                "query_window_end": _utc_iso(builder.rollup_window_end),
            }
        )
    return response


def fetch_eval_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str = "trace",
    timeout_ms: int = GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    from tracer.services.clickhouse.v2.query_builders.eval_metrics import (
        EvalMetricsQueryBuilderV2,
    )

    # These graph endpoints are CH25-only (their PostgreSQL fallback was
    # removed during the spans cutover), so do not let an absent rollout flag
    # silently select the legacy span schema.
    metric_id = str(req_data_config.get("id") or "")
    eval_output_type = normalize_eval_graph_output_type(req_data_config)
    selected_value = req_data_config.get("value")
    choices = list(req_data_config.get("choices") or [])
    if eval_output_type == "CHOICES":
        if selected_value not in (None, "") and not choices:
            choices = [str(selected_value)]
        if not choices:
            return {"metric_name": metric_id, "data": []}

    builder = EvalMetricsQueryBuilderV2(
        project_id=str(project_id),
        custom_eval_config_id=metric_id,
        filters=filters,
        interval=interval,
        eval_output_type=eval_output_type,
        choices=choices,
        observe_type=observe_type,
    )
    query, params = builder.build()
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=timeout_ms,
        settings=GRAPH_READ_SETTINGS,
    )
    columns = result.columns or []
    rows = result.data
    if eval_output_type == "PASS_FAIL" and not _selected_bool_value(selected_value):
        rows = _invert_pass_fail_rows(rows, columns)
    formatted = builder.format_result(rows, columns)
    return _format_eval_graph_response(
        formatted,
        metric_id=metric_id,
        selected_choice=selected_value if eval_output_type == "CHOICES" else None,
    )


def fetch_annotation_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str,
    timeout_ms: int = GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    label_id = req_data_config.get("id")
    if not label_id:
        raise ValueError("Annotation label ID is required")
    # Annotation labels can be project-local or org/shared labels that are
    # only connected to a project through Score rows. Use the same score-backed
    # lookup as list/filter config so graph metrics work for rendered labels.
    label = get_annotation_labels_for_project(project_id).get(id=label_id)
    builder = AnnotationGraphQueryBuilder(
        project_id=str(project_id),
        annotation_label_id=str(label_id),
        annotation_name=label.name,
        filters=filters,
        interval=interval,
        output_type=annotation_output_type(label, req_data_config.get("output_type")),
        value=req_data_config.get("value"),
        observe_type=observe_type,
    )
    query, params = builder.build()
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=timeout_ms,
        settings=GRAPH_READ_SETTINGS,
    )
    return builder.format_result(result.data, result.columns or [])
