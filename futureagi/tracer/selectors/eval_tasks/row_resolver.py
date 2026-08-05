"""Resolve an eval task's desired (in-scope) row set, deterministically.

The "did the row set change?" axis of the reconciler — the counterpart to the
config hash. Resolves the in-scope identity ids (span / trace / session ids, per
the task's row_type) in deterministic order. Historical span/trace membership
is kept bounded and fully buffered before any batch is yielded, so a failed
proof can never create a partial task; other row types retain streaming reads.

Selection reuses the UI list builders' filter compilation (the same builders
``list_spans_observe`` / ``list_voice_calls`` / ``list_traces_of_session`` /
``list_sessions`` use) so the eval set matches the list endpoints for the same
filters; on top of that filtered id set we apply deterministic hash sampling and
the row limit. The entry FKs are batch-resolved by the materializer later.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil
from typing import TYPE_CHECKING, Any

from tracer.models.eval_task import RowType, RunType
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.services.clickhouse.v2 import get_reader

if TYPE_CHECKING:
    from tracer.models.eval_task import EvalTask

# row_type → (UI list builder query type, identity column the builder emits).
_BUILDER_BY_ROW_TYPE = {
    "spans": ("SPAN_LIST", "id"),
    "voiceCalls": ("VOICE_CALL_LIST", "id"),
    "traces": ("TRACE_LIST", "trace_id"),
    "sessions": ("SESSION_LIST", "session_id"),
}

_EVAL_TASK_TOTAL_READ_SECONDS = 10.0
_EVAL_TASK_HISTORICAL_TRACE_READ_SECONDS = 22.0
_EVAL_TASK_HISTORICAL_TRACE_POPULATION_SECONDS = 60.0
_EVAL_TASK_MAX_READ_ATTEMPTS = 128
# Seed pages are identity-only and capped at the shared production-proven 512
# rows. Classifiers remain separately chunked to 200 physical identities.
_EVAL_TASK_MAX_CANDIDATES = 512
_EVAL_TASK_CLASSIFY_BATCH_SIZE = 200
_EVAL_TASK_TRACE_MEMBERSHIP_BATCH_SIZE = 100
_EVAL_TASK_TRACE_WITNESS_BATCH_SIZE = 100
_EVAL_TASK_TRACE_WITNESS_QUERY_RESERVE = 16
_EVAL_TASK_TRACE_WITNESS_SECONDS_RESERVE = 10.0
_EVAL_TASK_TRACE_WITNESS_QUERY_TIMEOUT_MS = 1_500
_EVAL_TASK_TRACE_WITNESS_PREFLIGHT_MS_PER_BATCH = 500
_EVAL_TASK_BUFFERED_ID_LIMIT = 10_000
_EVAL_TASK_STREAM_READ_SETTINGS = {
    "max_execution_time": 10,
    "timeout_overflow_mode": "throw",
    "max_threads": 2,
    "max_memory_usage": 512 * 1024 * 1024,
    "max_bytes_to_read": 2 * 1024 * 1024 * 1024,
    "read_overflow_mode": "throw",
}
_EVAL_TASK_TRACE_WITNESS_READ_SETTINGS = {
    "max_execution_time": 2,
    "max_threads": 1,
    "max_block_size": 8192,
    "max_rows_to_read": 5_000_000,
    "max_memory_usage": 256 * 1024 * 1024,
    "max_bytes_to_read": 512 * 1024 * 1024,
    "read_overflow_mode": "throw",
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}
_SAFE_READ_BUDGET_MESSAGE = (
    "Evaluation task row selection exceeded its read budget. "
    "Narrow the time range and retry."
)
_SAFE_ROW_LIMIT_MESSAGE = (
    "This evaluation task is too large for safe row selection. "
    "Reduce the row limit and retry."
)
_SAFE_AMBIGUOUS_SPAN_MESSAGE = (
    "Evaluation task row selection could not safely distinguish one or more "
    "spans. Narrow the filters and retry."
)
_SAFE_UNSUPPORTED_FILTER_MESSAGE = (
    "Evaluation task row selection contains a filter that cannot be resolved "
    "safely. Update the filters and retry."
)


class EvalTaskReadBudgetExceeded(RuntimeError):
    """Safe error used when exact task membership cannot be proven in budget."""


@dataclass(frozen=True)
class TraceFilterWitness:
    """Latest-state child identity that proved one trace filter leaf."""

    trace_id: str
    filter_ordinal: int
    column_id: str
    col_type: str
    project_id: str
    span_id: str
    start_time: datetime


@dataclass(frozen=True)
class ResolvedRowSet:
    """One completely-buffered desired-state proof.

    ``candidate_ids`` is C (logical rows affected by the frozen arrival/change
    window); ``matched_ids`` is M (the sampled subset of C matching latest full
    state). Historical and cursor-null passes are full-state proofs. Normal
    continuous passes are deltas, where only C may be removed/requeued.
    """

    candidate_ids: tuple[str, ...]
    matched_ids: tuple[str, ...]
    full_state: bool
    trace_filter_witnesses: tuple[TraceFilterWitness, ...] = ()


@dataclass(frozen=True)
class _BoundedHistoricalResult:
    ids: tuple[str, ...]
    trace_filter_witnesses: tuple[TraceFilterWitness, ...] = ()


def _trace_any_span_filter_descriptors(
    ui_filters: list[dict[str, Any]],
) -> tuple[tuple[str, str], ...]:
    """Return ``(column_id, col_type)`` in classifier witness order."""

    from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
        compile_trace_filter_plans,
    )

    descriptors: list[tuple[str, str]] = []
    for item in ui_filters:
        if not isinstance(item, dict):
            continue
        column_id = item.get("column_id") or item.get("columnId")
        if column_id in {"created_at", "start_time"}:
            continue
        try:
            plans = compile_trace_filter_plans([item])
        except (TypeError, ValueError):
            continue
        for plan in plans:
            if plan.scope != "any":
                continue
            config = item.get("filter_config") or item.get("filterConfig") or {}
            descriptors.append(
                (
                    str(column_id),
                    str(config.get("col_type") or config.get("colType") or ""),
                )
            )
    return tuple(descriptors)


def _trace_filter_witnesses_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    ui_filters: list[dict[str, Any]],
    project_id: str,
) -> tuple[TraceFilterWitness, ...]:
    descriptors = _trace_any_span_filter_descriptors(ui_filters)
    witnesses: list[TraceFilterWitness] = []
    seen: set[tuple[str, int]] = set()
    for row in rows:
        trace_id = str(row.get("trace_id") or "")
        if not trace_id:
            continue
        for ordinal, (column_id, col_type) in enumerate(descriptors):
            raw_identity = row.get(f"filter_witness_{ordinal}")
            if not isinstance(raw_identity, (list, tuple)) or len(raw_identity) != 2:
                continue
            span_id = str(raw_identity[0] or "")
            start_time = raw_identity[1]
            if not span_id or not isinstance(start_time, datetime):
                continue
            identity = (trace_id, ordinal)
            if identity in seen:
                continue
            seen.add(identity)
            witnesses.append(
                TraceFilterWitness(
                    trace_id=trace_id,
                    filter_ordinal=ordinal,
                    column_id=column_id,
                    col_type=col_type,
                    project_id=str(project_id),
                    span_id=span_id,
                    start_time=start_time,
                )
            )
    return tuple(witnesses)


def _validated_trace_filter_witnesses(
    rows: Iterable[dict[str, Any]],
    *,
    ui_filters: list[dict[str, Any]],
    project_id: str,
) -> tuple[TraceFilterWitness, ...]:
    """Return a complete exact witness matrix or fail the buffered selection."""

    buffered_rows = list(rows)
    descriptors = _trace_any_span_filter_descriptors(ui_filters)
    if not descriptors or not buffered_rows:
        return ()
    trace_ids = [str(row.get("trace_id") or "") for row in buffered_rows]
    if any(not trace_id for trace_id in trace_ids) or len(set(trace_ids)) != len(
        trace_ids
    ):
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
    witnesses = _trace_filter_witnesses_from_rows(
        buffered_rows,
        ui_filters=ui_filters,
        project_id=project_id,
    )
    if {(witness.trace_id, witness.filter_ordinal) for witness in witnesses} != {
        (trace_id, ordinal)
        for trace_id in trace_ids
        for ordinal in range(len(descriptors))
    }:
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
    return witnesses


def _normalized_trace_replay_identity(
    row: dict[str, Any], *, project_id: str
) -> tuple[str, str, str, datetime] | None:
    """Return the immutable canonical-root identity used across both phases."""

    trace_id = str(row.get("trace_id") or "")
    root_span_id = str(row.get("root_span_id") or "")
    start_time = row.get("start_time")
    if not trace_id or not root_span_id or not isinstance(start_time, datetime):
        return None
    normalized_start = (
        start_time.astimezone(UTC).replace(tzinfo=None)
        if start_time.tzinfo is not None
        else start_time
    )
    return (
        str(row.get("project_id") or project_id),
        trace_id,
        root_span_id,
        normalized_start,
    )


def _replay_historical_trace_filter_witnesses(
    analytics,
    *,
    builder,
    rows: list[dict[str, Any]],
    phase_one_query_count: int,
    read_started: float,
    ui_filters: list[dict[str, Any]],
    project_id: str,
) -> list[dict[str, Any]]:
    """Replay only proven traces to attach exact any-span filter witnesses.

    The first phase proves membership/order without expensive ``argMinIf``
    witness projections. This phase replays the final matched prefix in the
    production-qualified 100-trace batches. It is fully buffered and compares the
    immutable project/trace/root/start identity before returning anything, so a
    concurrent latest-state change fails closed rather than targeting a stale
    span in the evaluation mapping.
    """

    descriptors = _trace_any_span_filter_descriptors(ui_filters)
    if not rows or not descriptors:
        return rows

    required_queries = ceil(len(rows) / _EVAL_TASK_TRACE_WITNESS_BATCH_SIZE)
    if phase_one_query_count + required_queries > _EVAL_TASK_MAX_READ_ATTEMPTS:
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)

    expected_by_trace: dict[str, tuple[str, str, str, datetime]] = {}
    for row in rows:
        identity = _normalized_trace_replay_identity(row, project_id=project_id)
        if identity is None or identity[1] in expected_by_trace:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
        expected_by_trace[identity[1]] = identity

    total_deadline = read_started + _EVAL_TASK_HISTORICAL_TRACE_READ_SECONDS
    # The largest production tenant completed a 100-trace exact replay in
    # 429 ms. Reserve 500 ms per batch before the first replay read, so the
    # default 1,000-row task has a realistic 5 s preflight inside its separately
    # padded 10 s wall budget and no partial witness set is ever produced.
    if int((total_deadline - time.monotonic()) * 1000) < (
        required_queries * _EVAL_TASK_TRACE_WITNESS_PREFLIGHT_MS_PER_BATCH
    ):
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)

    replayed_rows: list[dict[str, Any]] = []
    trace_ids = list(expected_by_trace)
    for offset in range(0, len(trace_ids), _EVAL_TASK_TRACE_WITNESS_BATCH_SIZE):
        remaining_ms = int((total_deadline - time.monotonic()) * 1000)
        if remaining_ms < 25:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
        batch = trace_ids[offset : offset + _EVAL_TASK_TRACE_WITNESS_BATCH_SIZE]
        try:
            query, query_params = builder.build_filter_match_query(
                batch,
                include_filter_witnesses=True,
            )
            if not query:
                raise ValueError("trace witness replay query is empty")
            result = analytics.execute_ch_query(
                query,
                query_params,
                timeout_ms=min(
                    _EVAL_TASK_TRACE_WITNESS_QUERY_TIMEOUT_MS,
                    remaining_ms,
                ),
                settings={
                    **_EVAL_TASK_TRACE_WITNESS_READ_SETTINGS,
                    "max_result_rows": len(batch),
                },
            )
        except Exception as exc:
            if (
                not isinstance(exc, ValueError)
                and not isinstance(exc, TimeoutError)
                and not is_read_budget_error(exc)
                and not is_clickhouse_query_error(exc)
            ):
                raise
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
        replayed_rows.extend(result.data or [])

    replayed_by_trace: dict[str, dict[str, Any]] = {}
    for row in replayed_rows:
        identity = _normalized_trace_replay_identity(row, project_id=project_id)
        if identity is None or identity[1] in replayed_by_trace:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
        if expected_by_trace.get(identity[1]) != identity:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
        replayed_by_trace[identity[1]] = row
    if set(replayed_by_trace) != set(expected_by_trace):
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)

    _validated_trace_filter_witnesses(
        replayed_rows,
        ui_filters=ui_filters,
        project_id=project_id,
    )

    return [replayed_by_trace[trace_id] for trace_id in expected_by_trace]


def iter_desired_rows(
    task: EvalTask, *, batch_size: int = 10_000, ceiling: datetime | None = None
) -> Iterator[list[str]]:
    """Compatibility iterator over one already-buffered membership proof."""

    resolved = resolve_desired_rows(task, ceiling=ceiling)
    yield from _iter_id_batches(list(resolved.matched_ids), batch_size=batch_size)


def resolve_desired_rows(
    task: EvalTask, *, ceiling: datetime | None = None
) -> ResolvedRowSet:
    """Resolve C and M once, without yielding or writing partial results."""

    # Row limit applies to historical tasks only; continuous runs forever.
    limit = task.spans_limit if task.run_type == RunType.HISTORICAL else None
    sampling_rate = task.sampling_rate if task.sampling_rate is not None else 100.0

    if task.run_type == RunType.CONTINUOUS:
        return _resolve_continuous_rows(
            task,
            ceiling=ceiling,
            sampling_rate=float(sampling_rate),
        )

    # A strict datetime complement can make the requested interval provably
    # empty (for example ``is_null`` on non-nullable start_time). Preserve the
    # compatibility SQL for non-empty requests, but do zero CH reads when the
    # filter algebra already proves an empty result.
    from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder

    historical_ui_filters = _task_ui_filters(
        task.filters or {}, row_type=task.row_type, bounded_trace_root=True
    )
    if BaseQueryBuilder.analyze_bounded_datetime_filters(
        historical_ui_filters, strict=True
    ).empty:
        return ResolvedRowSet((), (), True)

    if (
        task.row_type in (RowType.SPANS, RowType.TRACES, RowType.SESSIONS)
        and task.run_type == RunType.HISTORICAL
        and limit is not None
    ):
        if float(sampling_rate) <= 0:
            return ResolvedRowSet((), (), True)

        from tracer.services.clickhouse.v2.query_service import (
            V2AnalyticsQueryService,
        )

        bounded_result = _resolve_bounded_historical_span_ids(
            V2AnalyticsQueryService(),
            sql=None,
            params=None,
            project_id=str(task.project_id),
            salt=str(task.id),
            sampling_rate=float(sampling_rate),
            filters=task.filters or {},
            limit=int(limit),
            batch_size=_EVAL_TASK_BUFFERED_ID_LIMIT,
            row_type=task.row_type,
            include_trace_filter_witnesses=True,
        )
        if not isinstance(bounded_result, _BoundedHistoricalResult):
            # Preserve compatibility with test/fallback selectors that return
            # the historical list shape and therefore cannot supply witnesses.
            bounded_result = _BoundedHistoricalResult(
                tuple(str(value) for value in bounded_result)
            )
        return ResolvedRowSet(
            bounded_result.ids,
            bounded_result.ids,
            True,
            bounded_result.trace_filter_witnesses,
        )

    sql, params = _build_sample_query(
        project_id=str(task.project_id),
        row_type=task.row_type,
        salt=str(task.id),
        sampling_rate=float(sampling_rate),
        filters=task.filters or {},
        limit=limit,
    )
    resolved_ids = _resolve_buffered_legacy_ids(
        sql,
        params,
        batch_size=min(_EVAL_TASK_BUFFERED_ID_LIMIT, 10_000),
        limit=int(limit or _EVAL_TASK_BUFFERED_ID_LIMIT),
    )
    resolved = tuple(resolved_ids)
    return ResolvedRowSet(resolved, resolved, True)


def _resolve_continuous_rows(
    task: EvalTask,
    *,
    ceiling: datetime | None,
    sampling_rate: float,
) -> ResolvedRowSet:
    """Resolve an arrival delta C, then classify C against complete latest M."""

    from tracer.selectors.eval_tasks.continuous_candidates import (
        ContinuousCandidateOverflow,
        ContinuousCandidateReadError,
        discover_continuous_candidates,
        sample_public_ids,
    )
    from tracer.services.clickhouse.v2.dispatch import get_v2_class
    from tracer.services.clickhouse.v2.query_service import V2AnalyticsQueryService

    floor = _continuous_floor(task)
    if floor is None:
        raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE)
    frozen_ceiling = ceiling or datetime.now(UTC)
    if frozen_ceiling.tzinfo is None:
        frozen_ceiling = frozen_ceiling.replace(tzinfo=UTC)
    if floor.tzinfo is None:
        floor = floor.replace(tzinfo=UTC)
    full_state = task.continuous_cursor is None
    ui_filters = _task_ui_filters(
        task.filters or {},
        row_type=task.row_type,
        bounded_trace_root=True,
        include_legacy_date_range=False,
    )
    from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder

    if BaseQueryBuilder.analyze_bounded_datetime_filters(ui_filters, strict=True).empty:
        # This is a global predicate proof, not merely an empty arrival delta.
        # Mark it FULL so reconciliation can remove every pending stale row
        # without issuing any ClickHouse query.
        return ResolvedRowSet((), (), True)
    analytics = V2AnalyticsQueryService()
    try:
        candidates = discover_continuous_candidates(
            analytics,
            project_id=str(task.project_id),
            row_type=task.row_type,
            filters=ui_filters,
            floor=floor,
            ceiling=frozen_ceiling,
            salt=str(task.id),
            sampling_rate=sampling_rate,
            deadline_seconds=_EVAL_TASK_TOTAL_READ_SECONDS * 0.4,
        )
        if not candidates.classifier_ids:
            return ResolvedRowSet(candidates.public_ids, (), full_state)

        query_type, key_field = _BUILDER_BY_ROW_TYPE[task.row_type]
        builder_kwargs: dict[str, Any] = {
            "project_id": str(task.project_id),
            "filters": ui_filters,
            "bounded_internal_scan": True,
        }
        if task.row_type in (RowType.SPANS, RowType.TRACES):
            builder_kwargs["bounded_identity_only"] = True
        if task.row_type == RowType.TRACES:
            builder_kwargs["bounded_bulk_scan"] = True
        builder = get_v2_class(query_type)(**builder_kwargs)
        if not builder.supports_bounded_filter_scan():
            raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE)

        classify_size = _EVAL_TASK_CLASSIFY_BATCH_SIZE
        recommended = getattr(builder, "recommended_filter_classify_batch_size", None)
        if callable(recommended):
            recommended_size = recommended()
            if recommended_size is not None:
                classify_size = min(classify_size, max(1, int(recommended_size)))
        deadline = time.monotonic() + _EVAL_TASK_TOTAL_READ_SECONDS * 0.5
        matched: list[str] = []
        matched_rows: list[dict[str, Any]] = []
        for offset in range(0, len(candidates.classifier_ids), classify_size):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
            batch = list(candidates.classifier_ids[offset : offset + classify_size])
            query, params = builder.build_filter_match_query(
                batch,
                candidate_full_state=True,
            )
            if not query:
                continue
            try:
                result = analytics.execute_ch_query(
                    query,
                    params,
                    timeout_ms=max(1, int(remaining * 1000)),
                    settings=_EVAL_TASK_STREAM_READ_SETTINGS,
                )
            except Exception as exc:
                if (
                    not isinstance(exc, TimeoutError)
                    and not is_read_budget_error(exc)
                    and not is_clickhouse_query_error(exc)
                ):
                    raise
                raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
            result_key = (
                "root_span_id" if task.row_type == RowType.VOICE_CALLS else key_field
            )
            matched_rows.extend(result.data)
            matched.extend(
                str(row[result_key])
                for row in result.data
                if row.get(result_key) not in (None, "")
            )

        matched_ids = tuple(sorted(dict.fromkeys(matched)))
        if task.row_type == RowType.VOICE_CALLS:
            matched_ids = sample_public_ids(
                analytics,
                matched_ids,
                salt=str(task.id),
                sampling_rate=sampling_rate,
                deadline_seconds=_EVAL_TASK_TOTAL_READ_SECONDS * 0.1,
            )
        # A classifier may only admit public identities proved by C. This also
        # fences malformed/multi-root output before reconciliation can write.
        candidate_public = set(candidates.public_ids)
        matched_ids = tuple(value for value in matched_ids if value in candidate_public)
        witnesses: tuple[TraceFilterWitness, ...] = ()
        if task.row_type == RowType.TRACES:
            matched_set = set(matched_ids)
            witnesses = tuple(
                witness
                for witness in _trace_filter_witnesses_from_rows(
                    matched_rows,
                    ui_filters=ui_filters,
                    project_id=str(task.project_id),
                )
                if witness.trace_id in matched_set
            )
        return ResolvedRowSet(
            candidates.public_ids,
            matched_ids,
            full_state,
            witnesses,
        )
    except EvalTaskReadBudgetExceeded:
        raise
    except ContinuousCandidateOverflow:
        raise EvalTaskReadBudgetExceeded(_SAFE_ROW_LIMIT_MESSAGE) from None
    except ContinuousCandidateReadError:
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
    except (TypeError, ValueError):
        raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE) from None


def _resolve_bounded_historical_span_ids(
    analytics,
    *,
    sql: str | None,
    params: dict[str, Any] | None,
    project_id: str,
    salt: str,
    sampling_rate: float,
    filters: dict,
    limit: int,
    batch_size: int,
    row_type: str = RowType.SPANS,
    include_trace_filter_witnesses: bool = False,
) -> list[str] | _BoundedHistoricalResult:
    """Resolve a complete historical span/trace/session set with bounded CH reads.

    Adjacent time slices produce only candidate identities. Every candidate is
    then reclassified against global latest state before it can be returned.
    The complete prefix stays buffered until the reader proves it; a timeout,
    query cap, unsupported filter, or incomplete scan raises a sanitized error
    and never leaks a partial task row set.
    """

    def resolved_result(
        ids: Iterable[str],
        rows: Iterable[dict[str, Any]] = (),
        *,
        ui_filters: list[dict[str, Any]] | None = None,
    ) -> list[str] | _BoundedHistoricalResult:
        normalized_ids = tuple(str(value) for value in ids)
        if not include_trace_filter_witnesses:
            return list(normalized_ids)
        witnesses: tuple[TraceFilterWitness, ...] = ()
        if row_type == RowType.TRACES and ui_filters is not None:
            witnesses = _validated_trace_filter_witnesses(
                rows,
                ui_filters=ui_filters,
                project_id=str(project_id),
            )
        return _BoundedHistoricalResult(normalized_ids, witnesses)

    if limit <= 0:
        return resolved_result(())
    if row_type not in (RowType.SPANS, RowType.TRACES, RowType.SESSIONS):
        raise ValueError(
            "Bounded historical resolution supports spans, traces, and sessions"
        )
    if not 0 <= float(sampling_rate) <= 100:
        raise ValueError("sampling_rate must be between 0 and 100")

    from tracer.selectors.trace_filter_reads import read_bounded_filter_page
    from tracer.services.clickhouse.v2.dispatch import get_v2_class

    ui_filters = _task_ui_filters(
        filters,
        row_type=row_type,
        bounded_trace_root=True,
    )
    time_columns = {"created_at", "start_time"}
    has_time_filter = any(
        (item.get("column_id") or item.get("columnId")) in time_columns
        for item in ui_filters
    )
    has_non_time_filter = any(
        (item.get("column_id") or item.get("columnId")) not in time_columns
        for item in ui_filters
    )
    bounded_limit = min(limit, _EVAL_TASK_BUFFERED_ID_LIMIT)
    # Historical session tasks have always selected a deterministic ID-sorted
    # population before LIMIT.  The bounded reader is newest-first, so a
    # session task may use it only as a complete-population proof: exhaust the
    # sampled set, sort the IDs below, and fail closed when the sentinel proves
    # that the configured limit would truncate it.
    requires_population_proof = (
        row_type == RowType.SESSIONS or limit > _EVAL_TASK_BUFFERED_ID_LIMIT
    )
    if not has_non_time_filter and not requires_population_proof:
        # The historical contract caps an ID-ascending deterministic prefix.
        # The bounded reader is newest-first so it can prove a filtered UI
        # page without scanning the whole window.  Keep unfiltered/time-only
        # tasks on the compatibility query; otherwise changing the row limit
        # across the bounded threshold would change the selected population.
        compatibility_sql, compatibility_params = (
            (sql, params)
            if sql is not None and params is not None
            else _build_sample_query(
                project_id=str(project_id),
                row_type=row_type,
                salt=str(salt),
                sampling_rate=float(sampling_rate),
                filters=filters,
                limit=limit,
            )
        )
        return resolved_result(
            _resolve_buffered_legacy_ids(
                compatibility_sql,
                compatibility_params,
                batch_size=batch_size,
                limit=limit,
            )
        )

    query_type, key_field = _BUILDER_BY_ROW_TYPE[row_type]
    trace_any_span_witnesses = bool(
        row_type == RowType.TRACES
        and include_trace_filter_witnesses
        and _trace_any_span_filter_descriptors(ui_filters)
    )
    trace_population_proof = bool(
        trace_any_span_witnesses and requires_population_proof
    )
    trace_witness_replay = bool(trace_any_span_witnesses and not trace_population_proof)
    builder = get_v2_class(query_type)(
        project_id=str(project_id),
        filters=ui_filters,
        bounded_internal_scan=True,
        bounded_identity_only=True,
        # Trace evaluation projects only identity + order. Any-span tasks use
        # the two-phase membership/witness contract below; other trace filters
        # retain the established bounded bulk envelope.
        bounded_bulk_scan=row_type == RowType.TRACES,
        # Normal historical trace tasks first prove membership/order in 100-ID
        # batches, then replay witnesses only for the final matched prefix.
        # High-limit population proofs attach witnesses in one pass below.
        **(
            {
                # Attach witness projections in phase one only when a high-limit
                # population proof must finish in one pass.  Callers that do not
                # request witnesses and normal two-phase tasks both need the
                # lightweight membership classifier; treating "no replay" as
                # "include witnesses" shrank those batches from 100 traces to 20
                # and repeated the expensive argMinIf projections unnecessarily.
                "bounded_include_filter_witnesses": trace_population_proof
            }
            if row_type == RowType.TRACES
            else {}
        ),
        # A configured limit above the 10k executable buffer needs a population
        # proof, not a newest-first trace page. For any-span trace filters this
        # lets the bounded reader exhaust the directly filtered physical seed
        # stream (or stop at the exact 10k+1 sentinel) instead of scanning every
        # tenant root. Accepted sets are still fully exhausted and ID-sorted.
        **({"bounded_population_proof": True} if trace_population_proof else {}),
        bounded_sampling_salt=str(salt),
        bounded_sampling_rate=float(sampling_rate),
    )
    if not builder.supports_bounded_filter_scan():
        # Never fall back to the broad compatibility statement for a filter
        # the latest-state candidate classifier cannot prove. In particular,
        # eval/annotation predicates must remain candidate-scoped.
        raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE)

    try:
        start_date, end_date = builder.parse_time_range(ui_filters)
    except (TypeError, ValueError):
        raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE) from None
    if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
        raise EvalTaskReadBudgetExceeded(_SAFE_UNSUPPORTED_FILTER_MESSAGE)
    if start_date >= end_date:
        return resolved_result(())
    if not has_time_filter:
        # BaseQueryBuilder's default window is relative to ``utcnow()``. Pin
        # that one resolved window before adjacent seed pages are built;
        # otherwise every builder call advances the lower bound by a few
        # microseconds and the final slice can fall just outside its own
        # request window.
        ui_filters = [
            *ui_filters,
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [start_date, end_date],
                },
            },
        ]
        builder.filters = ui_filters

    classify_batch_size = _EVAL_TASK_CLASSIFY_BATCH_SIZE
    batch_recommendation = getattr(
        builder, "recommended_filter_classify_batch_size", None
    )
    if callable(batch_recommendation):
        recommended = batch_recommendation()
        if recommended is not None:
            classify_batch_size = min(
                _EVAL_TASK_CLASSIFY_BATCH_SIZE,
                max(1, int(recommended)),
            )
    if trace_witness_replay:
        classify_batch_size = min(
            classify_batch_size,
            _EVAL_TASK_TRACE_MEMBERSHIP_BATCH_SIZE,
        )

    read_started = time.monotonic()
    phase_one_deadline_seconds = _EVAL_TASK_TOTAL_READ_SECONDS
    phase_one_query_count = _EVAL_TASK_MAX_READ_ATTEMPTS
    phase_one_seed_attempts = _EVAL_TASK_MAX_READ_ATTEMPTS
    if trace_witness_replay:
        phase_one_deadline_seconds = (
            _EVAL_TASK_HISTORICAL_TRACE_READ_SECONDS
            - _EVAL_TASK_TRACE_WITNESS_SECONDS_RESERVE
        )
        # Normal trace tasks reserve the last ten seconds for exact witness
        # replay. The replay preflights the actual phase-one query count plus
        # its exact number of 100-trace batches before reading.
        phase_one_query_count -= _EVAL_TASK_TRACE_WITNESS_QUERY_RESERVE
    elif trace_population_proof:
        # Population-proof classification attaches exact witnesses in the same
        # production-qualified 100-trace query. There is no second pass to
        # reserve. At the production-qualified 522 ms ceiling,
        # all 101 classifier batches for a complete 10k set need about 53 s;
        # this workflow-only path therefore has its own finite 60 s wall cap.
        phase_one_deadline_seconds = _EVAL_TASK_HISTORICAL_TRACE_POPULATION_SECONDS
        # Reserve the worst-case exact classifiers before scheduling adjacent
        # time slices. With a 10k+1 sentinel and 100-trace batches this leaves
        # 27 physical-seed attempts, so the adaptive scheduler covers even a
        # year-long request with broad bounded slices instead of planning 128
        # narrow seeds that later classifier queries could crowd out.
        phase_one_seed_attempts = _EVAL_TASK_MAX_READ_ATTEMPTS - ceil(
            (bounded_limit + 1) / classify_batch_size
        )

    try:
        page = read_bounded_filter_page(
            builder=builder,
            analytics=analytics,
            filters=ui_filters,
            key_field=key_field,
            page_number=0,
            page_size=bounded_limit,
            deadline_ms=int(phase_one_deadline_seconds * 1000),
            max_seed_attempts=phase_one_seed_attempts,
            max_candidates=_EVAL_TASK_MAX_CANDIDATES,
            max_query_count=phase_one_query_count,
            classify_batch_size=classify_batch_size,
            retry_wide_read_budget=True,
            read_settings=(
                _EVAL_TASK_TRACE_WITNESS_READ_SETTINGS
                if trace_population_proof
                else None
            ),
        )
    except Exception as exc:
        if not is_read_budget_error(exc) and not isinstance(
            exc, (TimeoutError, ValueError)
        ):
            raise
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None

    if not page.complete:
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
    if requires_population_proof and page.has_more:
        # The configured public limit may pre-date the executable safety cap.
        # A bounded 10k+sentinel read proves whether the complete sampled set
        # fits. Never materialize a partial prefix when it does not.
        raise EvalTaskReadBudgetExceeded(_SAFE_ROW_LIMIT_MESSAGE)
    resolved_rows = page.rows
    if trace_witness_replay:
        resolved_rows = _replay_historical_trace_filter_witnesses(
            analytics,
            builder=builder,
            rows=page.rows,
            phase_one_query_count=page.query_count,
            read_started=read_started,
            ui_filters=ui_filters,
            project_id=str(project_id),
        )
    if row_type == RowType.SPANS:
        identities_by_span_id: dict[str, set[tuple[str, Any]]] = {}
        for row in resolved_rows:
            span_id = str(row.get("id") or "")
            trace_id = str(row.get("trace_id") or "")
            start_time = row.get("start_time")
            if not span_id or not trace_id or start_time is None:
                continue
            identities = identities_by_span_id.setdefault(span_id, set())
            identities.add((trace_id, start_time))
            if len(identities) > 1:
                # EvalTaskEntry's public/storage contract carries only the
                # span ID. It cannot represent two distinct physical spans
                # that reuse that ID, so never silently target one.
                raise EvalTaskReadBudgetExceeded(_SAFE_AMBIGUOUS_SPAN_MESSAGE)
        resolved = list(
            dict.fromkeys(
                str(row[key_field])
                for row in resolved_rows
                if row.get(key_field) not in (None, "")
            )
        )
        final_ids = sorted(resolved) if requires_population_proof else resolved
        return resolved_result(final_ids, resolved_rows, ui_filters=ui_filters)
    resolved = [
        str(row[key_field])
        for row in resolved_rows
        if row.get(key_field) not in (None, "")
    ]
    final_ids = sorted(resolved) if requires_population_proof else resolved
    return resolved_result(final_ids, resolved_rows, ui_filters=ui_filters)


def _resolve_buffered_legacy_ids(
    sql: str,
    params: dict[str, Any],
    *,
    batch_size: int,
    limit: int,
) -> list[str]:
    """Run the compatibility selector without ever exposing a partial prefix."""

    if limit <= 0:
        return []
    resolved: list[str] = []
    reader = get_reader()
    try:
        for batch in reader.stream_query(
            sql,
            params,
            batch_size=batch_size,
            settings=_EVAL_TASK_STREAM_READ_SETTINGS,
        ):
            resolved.extend(str(value) for value in batch)
            if len(resolved) > limit:
                raise EvalTaskReadBudgetExceeded(_SAFE_ROW_LIMIT_MESSAGE)
    except Exception as exc:
        if isinstance(exc, EvalTaskReadBudgetExceeded):
            raise
        if (
            not isinstance(exc, TimeoutError)
            and not is_read_budget_error(exc)
            and not is_clickhouse_query_error(exc)
        ):
            raise
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
    finally:
        reader.close()
    return resolved


def _iter_id_batches(row_ids: list[str], *, batch_size: int) -> Iterator[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    for offset in range(0, len(row_ids), batch_size):
        yield row_ids[offset : offset + batch_size]


def _validated_task_filter_items(
    task_filters: dict[str, Any], key: str
) -> list[dict[str, Any]]:
    if key not in task_filters or task_filters[key] is None:
        return []
    values = task_filters[key]
    if not isinstance(values, list):
        raise ValueError(f"{key} must be a list")
    if any(not isinstance(item, dict) for item in values):
        raise ValueError(f"{key} entries must be objects")
    return list(values)


def _validated_task_filters(filters: dict | None) -> dict[str, Any]:
    if filters is None:
        return {}
    if not isinstance(filters, dict):
        raise ValueError("task filters must be an object")
    return filters


def _validated_date_range(task_filters: dict[str, Any]) -> list[Any] | None:
    if "date_range" not in task_filters or task_filters["date_range"] is None:
        return None
    date_range = task_filters["date_range"]
    if not isinstance(date_range, list | tuple) or len(date_range) != 2:
        raise ValueError("date_range must contain exactly two values")
    if any(value in (None, "") for value in date_range):
        raise ValueError("date_range values must be non-empty")
    return list(date_range)


def _task_ui_filters(
    filters: dict | None,
    *,
    row_type: str | None = None,
    bounded_trace_root: bool = False,
    include_legacy_date_range: bool = True,
) -> list[dict[str, Any]]:
    """Normalize persisted task filters to the list-builder filter contract."""

    task_filters = _validated_task_filters(filters)

    ui_filters: list[dict[str, Any]] = []
    for key in ("filters", "span_attributes_filters"):
        ui_filters.extend(_validated_task_filter_items(task_filters, key))

    date_range = (
        _validated_date_range(task_filters) if include_legacy_date_range else None
    )
    if date_range is not None:
        ui_filters.append(
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [date_range[0], date_range[1]],
                },
            }
        )

    for task_key, column_id in (
        ("span_id", "span_id"),
        ("trace_id", "trace_id"),
        ("session_id", "session_id"),
        ("observation_type", "observation_type"),
    ):
        raw_values = task_filters.get(task_key)
        if raw_values is None:
            continue
        values = (
            [str(value) for value in raw_values if value not in (None, "")]
            if isinstance(raw_values, list | tuple | set)
            else [str(raw_values)]
        )
        if not values:
            continue
        internal_trace_root = (
            bounded_trace_root
            and row_type == RowType.TRACES
            and task_key == "observation_type"
        )
        item = {
            "column_id": column_id,
            "filter_config": {
                "col_type": (
                    "INTERNAL_ROOT_METRIC" if internal_trace_root else "SYSTEM_METRIC"
                ),
                "filter_type": "text",
                "filter_op": "in",
                "filter_value": values,
            },
        }
        if internal_trace_root:
            # FilterItemField rejects unknown keys on external requests.  This
            # second marker keeps the internal col_type from becoming a
            # user-selectable semantic switch if a caller guesses its string.
            item["_eval_task_trace_root"] = True
        ui_filters.append(item)

    if task_filters.get("created_at"):
        ui_filters.append(
            {
                "column_id": "created_at",
                "filter_config": {
                    "col_type": "SYSTEM_METRIC",
                    "filter_type": "datetime",
                    "filter_op": "greater_than",
                    "filter_value": task_filters["created_at"],
                },
            }
        )
    return ui_filters


def _continuous_floor(task: EvalTask) -> datetime | None:
    """Lower arrival/change watermark for a continuous task's desired set.

    A continuous task only evaluates rows that arrive after it starts — it must
    never backfill the project history that pre-dates it. The floor is the
    forward watermark once the reconciler has advanced it, falling back to the
    task's start (then creation) on the first pass. Historical tasks have no
    floor here (they carve their window from ``filters`` + ``spans_limit``).
    """
    if task.run_type != RunType.CONTINUOUS:
        return None
    return task.continuous_cursor or task.start_time or task.created_at


def _build_sample_query(
    *,
    project_id: str,
    row_type: str,
    salt: str,
    sampling_rate: float,
    filters: dict | None,
    limit: int | None,
    created_at_floor: datetime | None = None,
    created_at_ceiling: datetime | None = None,
) -> tuple[str, dict[str, Any]]:
    """Sampled-row-ids SQL for the row_type: take the UI list builder's filtered
    id set and wrap it with deterministic hash sampling, a stable order, and the
    row limit."""
    from tracer.services.clickhouse.v2.dispatch import get_v2_class

    try:
        query_type, id_col = _BUILDER_BY_ROW_TYPE[row_type]
    except KeyError:
        raise ValueError(f"Unsupported row_type: {row_type!r}") from None

    # Reshape the eval task's stored filters into the frontend filter list the UI
    # builder consumes; the date range is read via parse_time_range.
    f = _validated_task_filters(filters)
    ui_filters = _validated_task_filter_items(f, "filters")
    ui_filters.extend(_validated_task_filter_items(f, "span_attributes_filters"))
    dr = _validated_date_range(f)
    if dr is not None:
        ui_filters.append(
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [dr[0], dr[1]],
                },
            }
        )

    for task_key, column_id in (
        ("span_id", "span_id"),
        ("trace_id", "trace_id"),
        ("session_id", "session_id"),
    ):
        raw_values = f.get(task_key)
        if raw_values is None:
            continue
        values = (
            [str(value) for value in raw_values if value not in (None, "")]
            if isinstance(raw_values, list | tuple | set)
            else [str(raw_values)]
        )
        if values:
            ui_filters.append(
                {
                    "column_id": column_id,
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "in",
                        "filter_value": values,
                    },
                }
            )

    if f.get("created_at"):
        ui_filters.append(
            {
                "column_id": "created_at",
                "filter_config": {
                    "col_type": "SYSTEM_METRIC",
                    "filter_type": "datetime",
                    "filter_op": "greater_than",
                    "filter_value": f["created_at"],
                },
            }
        )

    # Continuous bounds are passed directly to ``build_id_query`` so they bind
    # on arrival time (``created_at``). Injecting the floor into ``ui_filters``
    # would make ``parse_time_range`` apply it to historical ``start_time``.
    builder = get_v2_class(query_type)(project_id=str(project_id), filters=ui_filters)
    inner_sql, params = builder.build_id_query(
        created_at_floor=created_at_floor, created_at_ceiling=created_at_ceiling
    )
    params = {**params, "salt": str(salt), "rate": float(sampling_rate)}

    # observation_type is a legacy top-level key, not a filter-builder column;
    # constrain the id set against spans directly.
    ot_pred = ""
    ot = f.get("observation_type")
    if ot:
        params["otypes"] = tuple(
            str(o) for o in (ot if isinstance(ot, list | tuple | set) else [ot])
        )
        params["ot_project_id"] = str(project_id)
        src = "toString(trace_session_id)" if row_type == "sessions" else id_col
        # For traces, the trace list derives observation_type from the ROOT span
        # (it scans parent_span_id IS NULL), so match root spans only for parity.
        root_pred = (
            " AND (parent_span_id IS NULL OR parent_span_id = '')"
            if row_type == "traces"
            else ""
        )
        # Scope the subquery like the outer scan (project + not-deleted) so it
        # can't match ids from another project or soft-deleted rows.
        ot_pred = (
            f"AND {id_col} IN "
            f"(SELECT {src} FROM spans "
            f"WHERE observation_type IN %(otypes)s "
            f"AND project_id = %(ot_project_id)s AND is_deleted = 0"
            f"{root_pred})"
        )

    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %(lim)s"
        params["lim"] = int(limit)

    # modulo() not `%` — clickhouse-connect treats a literal `%` as a
    # parameter-format marker. Order by the id for a stable limit prefix.
    sql = (
        f"SELECT {id_col} FROM ({inner_sql}) "
        f"WHERE modulo(cityHash64(%(salt)s, toString({id_col})), 100) < %(rate)s "
        f"{ot_pred} "
        f"ORDER BY {id_col} {limit_sql}"
    )
    return sql, params
