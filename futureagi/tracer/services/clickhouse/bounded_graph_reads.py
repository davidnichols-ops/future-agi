"""Finite CH25 candidate reads and in-process graph aggregation.

Filtered Observe graphs must not materialize a tenant/window-wide ``IN
(SELECT ...)`` set. This module reuses the list endpoint's selective-anchor /
ordered-prefix protocol. Results within the finite graph ceiling remain exact;
larger result sets use a deterministic full-window sample that is always marked
incomplete. Budget or query failures never become an allegedly exact graph.
"""

from __future__ import annotations

from collections.abc import Hashable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import monotonic
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
# 4,096 can be proven exhaustive, while row 4,097 proves a bounded degraded
# sample without ever constructing a tenant-wide Set.
GRAPH_CANDIDATE_LIMIT = 4_096
# A root-only trace classifier intentionally hydrates complete presentation
# rows in batches of 50 (the production-safe memory ceiling).  Asking the
# shared selector for 4,095 rows would require 83 minimum queries including
# the sentinel seed, above its hard 48-query request contract, so even a
# one-trace equality filter would fail before ClickHouse was queried.  Keep
# the exact root-only ceiling at 1,599: its 1,600-row sentinel needs at most
# four 512-row seeds plus 32 classifier batches.  Any-span trace filters retain the
# 4,096 ceiling because their directly-indexable classifier safely uses 512.
GRAPH_TRACE_ROOT_CANDIDATE_LIMIT = 1_599
GRAPH_CANDIDATE_DEADLINE_MS = 3_900
GRAPH_DECORATION_CANDIDATE_DEADLINE_MS = 3_100
GRAPH_MAX_POINTS = 10_000
GRAPH_ANY_SPAN_STRATA = 8
GRAPH_ANY_SPAN_ROWS_PER_STRATUM = 49
# A long-window sparse-anchor sentinel distinguishes a common predicate before
# the ordered stratum reads begin. Common predicates deliberately switch to a
# small representative ceiling: replaying 512 identities in each of eight
# strata consumed the whole graph deadline in production before the first
# stratum completed. Forty-nine identities still provide deterministic temporal
# coverage while keeping every latest-state classifier safely bounded.
GRAPH_ANY_SPAN_DISTRIBUTED_AFTER = timedelta(hours=1)
# Before distributing a long window, give a directly-indexable predicate one
# bounded chance to prove that the entire result is sparse.  The 513th raw
# identity is a sentinel; only an exhausted probe followed by latest-state
# classification is accepted as exact.
GRAPH_SPARSE_ANCHOR_LIMIT = 512
GRAPH_SPARSE_ANCHOR_DEADLINE_MS = 1_800

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
    sampling_strategy: str | None = None
    sampling_strata: int = 0
    sampling_strata_completed: int = 0

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
            "query_sampled": self.query_status == "sampled",
        }
        if self.sampling_strategy:
            result["query_sampling_strategy"] = self.sampling_strategy
            result["query_sampling_strata"] = self.sampling_strata
            result["query_sampling_strata_completed"] = self.sampling_strata_completed
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


def _has_structured_filter(filters: list[dict[str, Any]]) -> bool:
    """Return whether a full-window raw anchor is unsafe for this shape."""

    return any(
        (item.get("column_id") or item.get("columnId")) == "call_type"
        or str((item.get("filter_config") or {}).get("filter_type") or "").lower()
        in {"json", "map"}
        for item in _active_filters(filters)
    )


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
    rows_per_stratum: int = GRAPH_ANY_SPAN_ROWS_PER_STRATUM,
    prior_page: Any | None = None,
) -> GraphCandidateSample:
    """Read arbitrary child-span filters across bounded full-window strata.

    Trace attributes may live on any child span.  A single newest-first scan
    can consume its budget in the latest dense slice and show no older shape.
    Eight disjoint time strata keep the work finite and deterministic. A
    stratum is marked complete only when its seed was exhausted, so the
    combined graph can never advertise a sample as exact.
    """

    if not 1 <= rows_per_stratum <= GRAPH_ANY_SPAN_ROWS_PER_STRATUM:
        raise ValueError("graph rows_per_stratum exceeds the bounded contract")
    stratum_count = min(
        GRAPH_ANY_SPAN_STRATA,
        max(1, deadline_ms // 250),
    )
    distributed_started = monotonic()
    window_width = window_end - window_start
    key_field = "trace_id" if mode == "trace" else "id"
    rows_by_id: dict[Hashable, dict[str, Any]] = {}
    complete = True
    elapsed_ms = float(getattr(prior_page, "elapsed_ms", 0.0) or 0.0)
    query_count = int(getattr(prior_page, "query_count", 0) or 0)
    rows_returned = int(getattr(prior_page, "rows_returned", 0) or 0)
    result_payload_bytes = int(getattr(prior_page, "result_payload_bytes", 0) or 0)
    total_rows_lower_bound = int(getattr(prior_page, "total_rows_lower_bound", 0) or 0)
    sampling_strata_completed = 0
    sampling_error_code: str | None = None
    # Freeze the outer request window into an explicit positive time leaf.
    # When the caller omits a date filter, each builder otherwise derives its
    # own ``now - 30 days`` default a few microseconds apart.  Passing the raw
    # filters as the membership window can then make membership_start newer
    # than the first stratum_start and fail the containment guard before any
    # ClickHouse query runs.  Complements remain intact via
    # ``_filters_for_window`` while every stratum shares these exact bounds.
    membership_filters = _filters_for_window(
        filters,
        window_start=window_start,
        window_end=window_end,
    )

    for index in range(stratum_count):
        remaining_ms = deadline_ms - int((monotonic() - distributed_started) * 1000)
        if remaining_ms < 25:
            complete = False
            sampling_error_code = "read_budget_exceeded"
            break
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
        stratum_builder_kwargs: dict[str, Any] = {
            "project_id": project_id,
            "page_number": 0,
            "page_size": rows_per_stratum,
            "filters": stratum_filters,
        }
        if mode == "trace":
            stratum_builder_kwargs["bounded_identity_only"] = True
            # The stratum constrains root seed/order only. Classification must
            # replay each finite trace across the original request window so a
            # root in one stratum can match children in another.
            stratum_builder_kwargs["bounded_membership_filters"] = membership_filters
        stratum_builder = builder_class(**stratum_builder_kwargs)
        # One extra identity is the finite has-more sentinel. Keeping the whole
        # stratum working set at 50 avoids the 512-row classifier that exceeded
        # the production graph deadline.
        candidate_limit = rows_per_stratum + 1
        max_seed_attempts = (
            rows_per_stratum + 1 + candidate_limit - 1
        ) // candidate_limit
        bounded_classify_batch_size = min(
            classify_batch_size,
            candidate_limit,
        )
        classifiers_per_seed = (
            candidate_limit + bounded_classify_batch_size - 1
        ) // bounded_classify_batch_size
        max_query_count = max_seed_attempts * (1 + classifiers_per_seed)
        try:
            page = read_bounded_filter_page(
                builder=stratum_builder,
                analytics=analytics,
                filters=stratum_filters,
                key_field=key_field,
                page_number=0,
                page_size=rows_per_stratum,
                # Share one monotonic deadline across the complete stratified
                # read instead of assigning one eighth up front. Per-query
                # caps in the selector still bound a slow ClickHouse read, but
                # a healthy classifier may use the otherwise-idle budget from
                # adjacent strata.
                deadline_ms=remaining_ms,
                max_seed_attempts=max_seed_attempts,
                max_query_count=max_query_count,
                # The visible rows plus one has-more sentinel stay finite. A
                # sparse/unattested path retains the 49-row representative
                # ceiling.
                max_candidates=candidate_limit,
                classify_batch_size=bounded_classify_batch_size,
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
        public_code = None
        if not page.complete or page.has_more:
            complete = False
            public_code = (
                "sample_limit"
                if page.has_more
                else _incomplete_error_code(page.error_code)
            )
            if public_code != "sample_limit":
                sampling_error_code = public_code
        # A resource/transport failure is not temporal coverage. Only an
        # exhausted page or a bounded candidate/sample-limit response proves
        # that this stratum was actually classified. This prevents eight
        # failed reads from being advertised as an intentional sample.
        if page.complete or page.has_more or public_code == "sample_limit":
            sampling_strata_completed += 1
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
    full_strata_coverage = sampling_strata_completed == stratum_count
    error_code: str | None = None
    if not complete:
        error_code = sampling_error_code or "sample_limit"
    query_status = (
        "complete" if complete else "sampled" if full_strata_coverage else "degraded"
    )
    return GraphCandidateSample(
        rows=tuple(rows),
        query_complete=complete,
        query_status=query_status,
        query_error_code=error_code,
        window_start=window_start,
        window_end=window_end,
        elapsed_ms=elapsed_ms,
        query_count=query_count,
        rows_returned=rows_returned,
        result_payload_bytes=result_payload_bytes,
        total_rows_lower_bound=max(len(rows), total_rows_lower_bound),
        sampling_strategy=(None if complete else "time_stratified_latest_state"),
        sampling_strata=stratum_count if not complete else 0,
        sampling_strata_completed=(sampling_strata_completed if not complete else 0),
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
    builder_kwargs: dict[str, Any] = {
        "project_id": str(project_id),
        "page_number": 0,
        "page_size": GRAPH_CANDIDATE_LIMIT,
        "filters": effective_filters,
    }
    # Trace graph decoration performs its own finite metric replay after the
    # trace set is proven. Candidate discovery therefore needs identities and
    # root order only, avoiding needless presentation-column hydration.
    if mode == "trace":
        builder_kwargs["bounded_identity_only"] = True
    else:
        builder_kwargs["bounded_anchor_probe"] = True
    builder = builder_class(
        **builder_kwargs,
    )
    if not builder.supports_bounded_filter_scan():
        error_code = builder.bounded_filter_degraded_error_code()
        raise BoundedGraphReadError(error_code or "unsupported_filter_shape")

    window_start, window_end = builder.parse_time_range(effective_filters)
    classify_batch_size = builder.recommended_filter_classify_batch_size()
    if window_end - window_start > GRAPH_ANY_SPAN_DISTRIBUTED_AFTER:
        sparse_page = None
        distributed_deadline_ms = deadline_ms
        anchor_support = getattr(builder, "supports_filter_anchor_probe", None)
        if (
            callable(anchor_support)
            and bool(anchor_support())
            and not _has_structured_filter(effective_filters)
        ):
            anchor_started = monotonic()
            try:
                sparse_page = read_bounded_filter_page(
                    builder=builder,
                    analytics=analytics,
                    filters=effective_filters,
                    key_field="trace_id" if mode == "trace" else "id",
                    page_number=0,
                    page_size=GRAPH_SPARSE_ANCHOR_LIMIT,
                    deadline_ms=min(deadline_ms, GRAPH_SPARSE_ANCHOR_DEADLINE_MS),
                    max_seed_attempts=2,
                    max_query_count=5,
                    max_candidates=GRAPH_SPARSE_ANCHOR_LIMIT,
                    classify_batch_size=min(
                        int(classify_batch_size or 50),
                        GRAPH_SPARSE_ANCHOR_LIMIT,
                    ),
                    include_incomplete_rows=False,
                    anchor_probe_only=True,
                )
            except Exception as exc:
                if not (is_read_budget_error(exc) or is_clickhouse_query_error(exc)):
                    raise
                logger.warning(
                    "graph sparse anchor degraded",
                    error_type=type(exc).__name__,
                    exc_info=True,
                )
                if not is_read_budget_error(exc):
                    raise BoundedGraphReadError("query_failed") from None
                # Code 307 / read-budget failures on a full-window anchor do
                # not invalidate the smaller disjoint strata. Reuse no partial
                # anchor rows and spend only the remaining request budget on
                # deterministic latest-state samples.
                distributed_deadline_ms = max(
                    25,
                    deadline_ms - int((monotonic() - anchor_started) * 1000),
                )
                if distributed_deadline_ms <= 25:
                    raise BoundedGraphReadError("read_budget_exceeded") from None
            # ``complete`` alone can mean a proven *page prefix*.  Exact graph
            # membership requires the full sentinel probe to be exhausted,
            # represented here by a complete page with no has-more row.
            if (
                sparse_page is not None
                and sparse_page.complete
                and not sparse_page.has_more
            ):
                return GraphCandidateSample(
                    rows=tuple(sparse_page.rows),
                    query_complete=True,
                    query_status="complete",
                    query_error_code=None,
                    window_start=window_start,
                    window_end=window_end,
                    elapsed_ms=sparse_page.elapsed_ms,
                    query_count=sparse_page.query_count,
                    rows_returned=sparse_page.rows_returned,
                    result_payload_bytes=sparse_page.result_payload_bytes,
                    total_rows_lower_bound=max(
                        len(sparse_page.rows),
                        sparse_page.total_rows_lower_bound,
                    ),
                )
            if sparse_page is not None:
                if sparse_page.error_code not in {
                    "sample_limit",
                    "deadline_exceeded",
                    "read_budget_exceeded",
                }:
                    raise BoundedGraphReadError(
                        _incomplete_error_code(sparse_page.error_code)
                    )
                distributed_deadline_ms = max(
                    25,
                    deadline_ms - int(sparse_page.elapsed_ms),
                )
                if distributed_deadline_ms <= 25:
                    raise BoundedGraphReadError("read_budget_exceeded")
        return _read_time_distributed_candidates(
            analytics=analytics,
            builder_class=builder_class,
            project_id=str(project_id),
            filters=effective_filters,
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            deadline_ms=distributed_deadline_ms,
            classify_batch_size=int(classify_batch_size or 50),
            rows_per_stratum=GRAPH_ANY_SPAN_ROWS_PER_STRATUM,
            prior_page=sparse_page,
        )

    candidate_limit = GRAPH_CANDIDATE_LIMIT
    if mode == "trace" and classify_batch_size == 50:
        candidate_limit = GRAPH_TRACE_ROOT_CANDIDATE_LIMIT
        builder = builder_class(
            project_id=str(project_id),
            page_number=0,
            page_size=candidate_limit,
            filters=effective_filters,
            bounded_identity_only=True,
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
            query_status="sampled",
            query_error_code=error_code,
            window_start=window_start,
            window_end=window_end,
            elapsed_ms=page.elapsed_ms,
            query_count=page.query_count,
            rows_returned=page.rows_returned,
            result_payload_bytes=page.result_payload_bytes,
            total_rows_lower_bound=page.total_rows_lower_bound,
            sampling_strategy="bounded_latest_state_prefix",
            sampling_strata=1,
            sampling_strata_completed=1,
        )

    # Every supported filter shape, including structured overflow arrays/maps,
    # is replayed against latest state for only the finite seed candidates.
    # Therefore an exhausted scan is exact; only this short-window path's
    # cardinality sentinel makes the visible prefix incomplete.
    sampled = page.has_more
    return GraphCandidateSample(
        rows=tuple(page.rows),
        query_complete=not sampled,
        query_status="sampled" if sampled else "complete",
        query_error_code="sample_limit" if sampled else None,
        window_start=window_start,
        window_end=window_end,
        elapsed_ms=page.elapsed_ms,
        query_count=page.query_count,
        rows_returned=page.rows_returned,
        result_payload_bytes=page.result_payload_bytes,
        total_rows_lower_bound=page.total_rows_lower_bound,
        sampling_strategy="bounded_latest_state_prefix" if sampled else None,
        sampling_strata=1 if sampled else 0,
        sampling_strata_completed=1 if sampled else 0,
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

    Exact and explicitly sampled candidates use the same reducer. The response
    metadata remains authoritative: sampled values are never labelled exact.
    """

    if not sample.query_complete and sample.query_status != "sampled":
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
