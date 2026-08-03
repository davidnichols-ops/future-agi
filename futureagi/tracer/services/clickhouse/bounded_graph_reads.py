"""Finite CH25 candidate reads and in-process graph aggregation.

Filtered Observe graphs must not materialize a tenant/window-wide ``IN
(SELECT ...)`` set. This module reuses the list endpoint's selective-anchor /
ordered-prefix protocol. Small result sets remain exact; high-cardinality
result sets use a deterministic, full-window sample that is always marked
incomplete. Budget or query failures never become an allegedly exact graph.
"""

from __future__ import annotations

from collections.abc import Hashable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import structlog

from tracer.selectors.trace_filter_reads import read_bounded_filter_page
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.services.clickhouse.v2.query_builders.span_list import (
    SpanListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)

# The shared selector performs at most 24 finite 200-ID seed/classify batches.
# Keep one sentinel below that 4,800-row mechanical ceiling: results through
# 4,095 can be proven exhaustive, while row 4,096 proves a bounded degraded
# sample without ever constructing a tenant-wide Set.
GRAPH_CANDIDATE_LIMIT = 4_095
# A root-only trace classifier intentionally hydrates complete presentation
# rows in batches of 50 (the production-safe memory ceiling).  Asking the
# shared selector for 4,095 rows would require 83 minimum queries including
# the sentinel seed, above its hard 48-query request contract, so even a
# one-trace equality filter would fail before ClickHouse was queried.  Keep
# the exact root-only ceiling at 1,599: its 1,600-row sentinel needs at most
# four 512-row seeds plus 32 classifier batches.  Any-span trace filters retain the
# 4,095 ceiling because their directly-indexable classifier safely uses 512.
GRAPH_TRACE_ROOT_CANDIDATE_LIMIT = 1_599
GRAPH_CANDIDATE_DEADLINE_MS = 3_900
GRAPH_DECORATION_CANDIDATE_DEADLINE_MS = 3_100
GRAPH_MAX_POINTS = 10_000
GRAPH_ANY_SPAN_STRATA = 8
GRAPH_ANY_SPAN_ROWS_PER_STRATUM = 49
GRAPH_ANY_SPAN_DISTRIBUTED_AFTER = timedelta(hours=1)

logger = structlog.get_logger(__name__)


class BoundedGraphReadError(RuntimeError):
    """A sanitized graph-read failure safe to map into an API error code."""

    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


@dataclass(frozen=True)
class GraphCandidateSample:
    rows: tuple[dict[str, Any], ...]
    query_complete: bool
    query_status: str
    query_error_code: str | None
    window_start: datetime
    window_end: datetime
    elapsed_ms: float
    query_count: int
    rows_returned: int
    result_payload_bytes: int
    total_rows_lower_bound: int

    def metadata(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "query_complete": self.query_complete,
            "query_status": self.query_status,
            "query_window_start": self.window_start.isoformat(),
            "query_window_end": self.window_end.isoformat(),
            "query_sample_size": len(self.rows),
            "query_count": self.query_count,
            "query_elapsed_ms": round(self.elapsed_ms, 3),
            "query_rows_returned": self.rows_returned,
            "query_result_bytes": self.result_payload_bytes,
            "query_total_rows_lower_bound": self.total_rows_lower_bound,
        }
        if self.query_error_code:
            result["query_error_code"] = self.query_error_code
        return result


def _active_filters(filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in filters
        if (item.get("column_id") or item.get("columnId"))
        not in {"created_at", "start_time"}
        or BaseQueryBuilder.is_datetime_complement_filter(item)
    ]


def _identity_seed_filter(observe_type: str) -> dict[str, Any]:
    return {
        "column_id": "trace_id" if observe_type == "trace" else "id",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "text",
            "filter_op": "is_not_null",
            "filter_value": None,
        },
    }


def _incomplete_error_code(error_code: str | None) -> str:
    """Map internal selector reasons onto the public graph error contract."""

    if error_code in {"deadline_exceeded", "read_budget_exceeded"}:
        return "read_budget_exceeded"
    return "sample_limit"


def _filters_for_window(
    filters: list[dict[str, Any]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    """Replace every time predicate with one canonical half-open stratum.

    ``read_graph_candidates`` has already intersected the request's datetime
    predicates into ``window_start``/``window_end``.  Keeping an original
    ``greater_than``/``less_than`` operator while replacing its scalar value
    with a two-value range makes the bounded builder reject an otherwise valid
    long-window request.  Remove all original time leaves and append the exact
    stratum as ``between`` so every advertised datetime form follows the same
    finite distributed-read path.
    """

    # Positive time leaves are replaced by the exact stratum. Complements are
    # residual predicates and must survive every stratum; dropping them would
    # make a long-window graph disagree with the corresponding list.
    narrowed = deepcopy(_active_filters(filters))
    narrowed.append(
        {
            "column_id": "created_at",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "datetime",
                "filter_op": "between",
                "filter_value": [window_start.isoformat(), window_end.isoformat()],
            },
        }
    )
    return narrowed


def _candidate_row_key(
    row: dict[str, Any], *, key_field: str
) -> tuple[datetime, Hashable]:
    start_time = row.get("start_time")
    if not isinstance(start_time, datetime):
        start_time = datetime.min
    elif start_time.tzinfo is not None:
        start_time = start_time.replace(tzinfo=None)
    if key_field == "id":
        return start_time, (
            str(row.get("id") or ""),
            str(row.get("trace_id") or ""),
        )
    return start_time, str(row.get(key_field) or "")


def _read_time_distributed_candidates(
    *,
    analytics: Any,
    builder_class: type,
    project_id: str,
    filters: list[dict[str, Any]],
    mode: str,
    window_start: datetime,
    window_end: datetime,
    deadline_ms: int,
    classify_batch_size: int,
) -> GraphCandidateSample:
    """Sample arbitrary child-span filters across the full requested window.

    Trace attributes may live on any child span.  A single newest-first scan
    can consume its budget in the latest dense slice and show no older shape.
    Eight disjoint time strata keep the work finite (at most sixteen queries)
    and deterministic.  A stratum is marked complete only when its seed was
    exhausted, so the combined graph can never advertise a sample as exact.
    """

    stratum_count = min(
        GRAPH_ANY_SPAN_STRATA,
        max(1, deadline_ms // 250),
    )
    per_stratum_deadline_ms = max(25, deadline_ms // stratum_count)
    window_width = window_end - window_start
    key_field = "trace_id" if mode == "trace" else "id"
    rows_by_id: dict[Hashable, dict[str, Any]] = {}
    complete = True
    elapsed_ms = 0.0
    query_count = 0
    rows_returned = 0
    result_payload_bytes = 0
    total_rows_lower_bound = 0

    for index in range(stratum_count):
        stratum_start = window_start + (window_width * index / stratum_count)
        stratum_end = (
            window_end
            if index == stratum_count - 1
            else window_start + (window_width * (index + 1) / stratum_count)
        )
        stratum_filters = _filters_for_window(
            filters,
            window_start=stratum_start,
            window_end=stratum_end,
        )
        stratum_builder = builder_class(
            project_id=project_id,
            page_number=0,
            page_size=GRAPH_ANY_SPAN_ROWS_PER_STRATUM,
            filters=stratum_filters,
        )
        try:
            page = read_bounded_filter_page(
                builder=stratum_builder,
                analytics=analytics,
                filters=stratum_filters,
                key_field=key_field,
                page_number=0,
                page_size=GRAPH_ANY_SPAN_ROWS_PER_STRATUM,
                deadline_ms=per_stratum_deadline_ms,
                max_seed_attempts=1,
                max_query_count=2,
                # Fifty rows include the 49-row visible sample plus one
                # has-more sentinel. Keeping the working set below the
                # selective-anchor threshold intentionally chooses the
                # ordered finite seed for common any-span trace attributes;
                # otherwise the 513-ID probe would consume this stratum's
                # complete two-query budget before classification began.
                max_candidates=GRAPH_ANY_SPAN_ROWS_PER_STRATUM + 1,
                classify_batch_size=min(
                    classify_batch_size,
                    GRAPH_ANY_SPAN_ROWS_PER_STRATUM + 1,
                ),
                include_incomplete_rows=True,
            )
        except Exception as exc:
            # Compiler/programming defects are not degradable. They must reach
            # the API boundary, where the generic 500 contract hides private
            # SQL details. Only typed resource and transport failures may be
            # represented by stable graph error metadata here.
            if not (is_read_budget_error(exc) or is_clickhouse_query_error(exc)):
                raise
            logger.warning(
                "graph candidate stratum degraded",
                stratum_index=index,
                error_type=type(exc).__name__,
                exc_info=True,
            )
            public_code = (
                "read_budget_exceeded" if is_read_budget_error(exc) else "query_failed"
            )
            raise BoundedGraphReadError(public_code) from None

        elapsed_ms += page.elapsed_ms
        query_count += page.query_count
        rows_returned += page.rows_returned
        result_payload_bytes += page.result_payload_bytes
        total_rows_lower_bound += page.total_rows_lower_bound
        if not page.complete or page.has_more:
            complete = False
            public_code = (
                "sample_limit"
                if page.has_more
                else _incomplete_error_code(page.error_code)
            )
            if public_code != "sample_limit":
                raise BoundedGraphReadError(public_code)
        for row in page.rows:
            if mode == "trace":
                identity: Hashable = str(row.get("trace_id") or "")
            else:
                identity = stratum_builder.bounded_filter_row_identity(row)
            identity_is_valid = (
                all(value not in (None, "") for value in identity)
                if isinstance(identity, tuple)
                else bool(identity)
            )
            if identity_is_valid:
                rows_by_id[identity] = row

    rows = sorted(
        rows_by_id.values(),
        key=lambda row: _candidate_row_key(row, key_field=key_field),
        reverse=True,
    )
    if not complete and not rows:
        raise BoundedGraphReadError("sample_limit")

    error_code: str | None = None
    if not complete:
        error_code = "sample_limit"
    return GraphCandidateSample(
        rows=tuple(rows),
        query_complete=complete,
        query_status="complete" if complete else "degraded",
        query_error_code=error_code,
        window_start=window_start,
        window_end=window_end,
        elapsed_ms=elapsed_ms,
        query_count=query_count,
        rows_returned=rows_returned,
        result_payload_bytes=result_payload_bytes,
        total_rows_lower_bound=max(len(rows), total_rows_lower_bound),
    )


def read_graph_candidates(
    *,
    analytics: Any,
    project_id: str,
    filters: list[dict[str, Any]],
    observe_type: str,
    deadline_ms: int = GRAPH_CANDIDATE_DEADLINE_MS,
    allow_time_only_seed: bool = False,
) -> GraphCandidateSample:
    """Return an exact finite set or an explicitly incomplete graph sample.

    ``allow_time_only_seed`` is used by annotation graphs, whose score table
    cannot prove tracer project ownership on its own.  A tautological finite
    identity predicate sends those requests through the same bounded CH25
    selector without introducing a broad membership subquery.
    """

    mode = str(observe_type or "").strip().lower()
    if mode not in {"trace", "span"}:
        raise ValueError("observe_type must be trace or span")
    if deadline_ms <= 0:
        raise ValueError("deadline_ms must be positive")

    effective_filters = list(filters or [])
    if not _active_filters(effective_filters):
        if not allow_time_only_seed:
            raise ValueError("a bounded graph candidate read needs a row filter")
        effective_filters.append(_identity_seed_filter(mode))

    builder_class = (
        TraceListQueryBuilderV2 if mode == "trace" else SpanListQueryBuilderV2
    )
    builder = builder_class(
        project_id=str(project_id),
        page_number=0,
        page_size=GRAPH_CANDIDATE_LIMIT,
        filters=effective_filters,
    )
    if not builder.supports_bounded_filter_scan():
        error_code = builder.bounded_filter_degraded_error_code()
        raise BoundedGraphReadError(error_code or "unsupported_filter_shape")

    window_start, window_end = builder.parse_time_range(effective_filters)
    classify_batch_size = builder.recommended_filter_classify_batch_size()
    if window_end - window_start > GRAPH_ANY_SPAN_DISTRIBUTED_AFTER:
        return _read_time_distributed_candidates(
            analytics=analytics,
            builder_class=builder_class,
            project_id=str(project_id),
            filters=effective_filters,
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            deadline_ms=deadline_ms,
            classify_batch_size=int(classify_batch_size or 50),
        )

    candidate_limit = GRAPH_CANDIDATE_LIMIT
    if mode == "trace" and classify_batch_size == 50:
        candidate_limit = GRAPH_TRACE_ROOT_CANDIDATE_LIMIT
        builder = builder_class(
            project_id=str(project_id),
            page_number=0,
            page_size=candidate_limit,
            filters=effective_filters,
        )

    try:
        page = read_bounded_filter_page(
            builder=builder,
            analytics=analytics,
            filters=effective_filters,
            key_field="trace_id" if mode == "trace" else "id",
            page_number=0,
            page_size=candidate_limit,
            deadline_ms=deadline_ms,
            # Graph page zero may render proven candidate rows only when its
            # metadata remains explicitly incomplete. Numbered list and eval
            # task callers retain the selector default (False), so this does
            # not weaken their exactness contract.
            include_incomplete_rows=True,
        )
    except BoundedGraphReadError:
        raise
    except Exception as exc:
        if not (is_read_budget_error(exc) or is_clickhouse_query_error(exc)):
            raise
        logger.warning(
            "graph candidate read degraded",
            error_type=type(exc).__name__,
            exc_info=True,
        )
        public_code = (
            "read_budget_exceeded" if is_read_budget_error(exc) else "query_failed"
        )
        raise BoundedGraphReadError(public_code) from None
    if not page.complete:
        error_code = _incomplete_error_code(page.error_code)
        if error_code != "sample_limit" or not page.rows:
            raise BoundedGraphReadError(error_code)
        return GraphCandidateSample(
            rows=tuple(page.rows),
            query_complete=False,
            query_status="degraded",
            query_error_code=error_code,
            window_start=window_start,
            window_end=window_end,
            elapsed_ms=page.elapsed_ms,
            query_count=page.query_count,
            rows_returned=page.rows_returned,
            result_payload_bytes=page.result_payload_bytes,
            total_rows_lower_bound=page.total_rows_lower_bound,
        )

    # Every supported filter shape, including structured overflow arrays/maps,
    # is replayed against latest state for only the finite seed candidates.
    # Therefore an exhausted scan is exact; only this short-window path's
    # cardinality sentinel makes the visible prefix incomplete.
    sampled = page.has_more
    return GraphCandidateSample(
        rows=tuple(page.rows),
        query_complete=not sampled,
        query_status="degraded" if sampled else "complete",
        query_error_code="sample_limit" if sampled else None,
        window_start=window_start,
        window_end=window_end,
        elapsed_ms=page.elapsed_ms,
        query_count=page.query_count,
        rows_returned=page.rows_returned,
        result_payload_bytes=page.result_payload_bytes,
        total_rows_lower_bound=page.total_rows_lower_bound,
    )


def _numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _metric_value(metric_id: str, state: dict[str, Any]) -> float:
    if metric_id == "traffic":
        return float(state["traffic"])
    if metric_id in {"tokens", "total_tokens"}:
        return state["total_tokens"]
    if metric_id in {"prompt_tokens", "input_tokens"}:
        return state["prompt_tokens"]
    if metric_id in {"completion_tokens", "output_tokens"}:
        return state["completion_tokens"]
    if metric_id == "cost":
        return state["cost_sum"] / max(state["cost_count"], 1)
    if metric_id == "error_rate":
        return (state["error_count"] * 100.0) / max(state["traffic"], 1)
    return state["latency_sum"] / max(state["latency_count"], 1)


def aggregate_system_candidate_graph(
    sample: GraphCandidateSample,
    *,
    metric_id: str,
    interval: str,
) -> dict[str, Any]:
    """Aggregate finite latest-state rows without another ClickHouse scan.

    The public ``data`` field is exact-only. Candidate sampling can still
    return useful bounded diagnostics, but incomplete rows must never become
    ordinary traffic/count/cost/token/latency points.
    """

    if not sample.query_complete:
        return {
            "metric_name": metric_id,
            "data": [],
            **sample.metadata(),
        }

    if sample.window_start >= sample.window_end:
        return {
            "metric_name": metric_id,
            "data": [],
            **sample.metadata(),
        }

    buckets: dict[datetime, dict[str, Any]] = {}
    for row in sample.rows:
        timestamp = row.get("start_time")
        if not isinstance(timestamp, datetime):
            continue
        bucket = BaseQueryBuilder._normalize_timestamp(timestamp, interval)
        state = buckets.setdefault(
            bucket,
            {
                "traffic": 0,
                "latency_sum": 0.0,
                "latency_count": 0,
                "cost_sum": 0.0,
                "cost_count": 0,
                "total_tokens": 0.0,
                "prompt_tokens": 0.0,
                "completion_tokens": 0.0,
                "error_count": 0,
            },
        )
        state["traffic"] += 1
        latency = _numeric(row.get("latency_ms"))
        if latency is not None:
            state["latency_sum"] += latency
            state["latency_count"] += 1
        cost = _numeric(row.get("cost"))
        if cost is not None:
            state["cost_sum"] += cost
            state["cost_count"] += 1
        state["total_tokens"] += _numeric(row.get("total_tokens")) or 0.0
        state["prompt_tokens"] += _numeric(row.get("prompt_tokens")) or 0.0
        state["completion_tokens"] += _numeric(row.get("completion_tokens")) or 0.0
        if str(row.get("status") or "").upper() in {"ERROR", "ERRORED", "FAILED"}:
            state["error_count"] += 1

    timestamps = list(
        BaseQueryBuilder._generate_timestamp_range(
            sample.window_start,
            sample.window_end,
            interval,
        )
    )
    if len(timestamps) > GRAPH_MAX_POINTS:
        raise BoundedGraphReadError("sample_limit")

    normalized_metric = str(metric_id or "latency").strip().lower()
    data: list[dict[str, Any]] = []
    for timestamp in timestamps:
        state = buckets.get(timestamp)
        data.append(
            {
                "timestamp": timestamp.isoformat(),
                "value": round(_metric_value(normalized_metric, state), 9)
                if state
                else 0,
                "primary_traffic": state["traffic"] if state else 0,
            }
        )
    return {
        "metric_name": metric_id,
        "data": data,
        **sample.metadata(),
    }


__all__ = [
    "BoundedGraphReadError",
    "GRAPH_CANDIDATE_LIMIT",
    "GRAPH_CANDIDATE_DEADLINE_MS",
    "GRAPH_DECORATION_CANDIDATE_DEADLINE_MS",
    "GRAPH_MAX_POINTS",
    "GraphCandidateSample",
    "aggregate_system_candidate_graph",
    "read_graph_candidates",
]
