"""Resolve an eval task's desired (in-scope) row set, deterministically.

The "did the row set change?" axis of the reconciler — the counterpart to the
config hash. Streams the in-scope identity ids (span / trace / session ids, per
the task's row_type) in deterministic order, in batches, so a large historical
task never holds its whole row set in memory.

Selection reuses the UI list builders' filter compilation (the same builders
``list_spans_observe`` / ``list_voice_calls`` / ``list_traces_of_session`` /
``list_sessions`` use) so the eval set matches the list endpoints for the same
filters; on top of that filtered id set we apply deterministic hash sampling and
the row limit. The entry FKs are batch-resolved by the materializer later.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any

from django.utils import timezone

from tracer.models.eval_task import RowType, RunType
from tracer.services.clickhouse.read_budget import (
    FUTURE_TAIL_PROBE_SETTINGS,
    build_future_tail_probe,
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

# ReplacingMergeTree duplicate span/trace versions are rare, but a capped query
# must leave room for them before Python de-duplicates the result. This keeps
# the CH top-K bounded while still filling the requested task row limit in the
# normal case (up to one duplicate candidate per desired id).
_DEDUP_ID_CANDIDATE_MULTIPLIER = 2

_EVAL_TASK_READ_SETTINGS = {
    "max_execution_time": 0.75,
    "timeout_overflow_mode": "throw",
    "max_threads": 2,
    "max_memory_usage": 256 * 1024 * 1024,
    # The activity-wide eval guardrail uses a 2 GiB external-sort threshold
    # under its ordinary 4 GiB memory cap. This selector intentionally tightens
    # the cap to 256 MiB, so it must also lower the spill threshold; otherwise
    # the bounded top-K reaches code 241 before external sorting can start.
    "max_bytes_before_external_sort": 128 * 1024 * 1024,
    "max_bytes_to_read": 1024 * 1024 * 1024,
    "read_overflow_mode": "throw",
}

# A historical task is bounded by ``spans_limit``, so resolving its ids in
# memory is bounded too.  The wall-clock deadline is shared by the initial
# whole-window attempt and every fallback slice: splitting one unsafe query
# must not accidentally turn it into an unbounded sequence of individually
# bounded queries.
_EVAL_TASK_TOTAL_READ_SECONDS = 10.0
_EVAL_TASK_SLICE = timedelta(minutes=1)
_EVAL_TASK_MAX_FUTURE_SKEW = timedelta(minutes=5)
_EVAL_TASK_SLICE_PAGE_SIZE = 50
_EVAL_TASK_TRACE_CANDIDATE_PAGE_SIZE = 50
# Exact fallback resolution buffers ids until the whole requested set is
# proven. Keep that safety property for ordinary task sizes without allowing a
# valid million-row task to allocate multiple million-entry Python containers.
# Larger tasks retain the established one-query streaming path.
_EVAL_TASK_BUFFERED_ID_LIMIT = 100_000
_SAFE_READ_BUDGET_MESSAGE = (
    "Evaluation task row selection exceeded its read budget. "
    "Narrow the time range and retry."
)


class EvalTaskReadBudgetExceeded(RuntimeError):
    """Safe, non-ClickHouse error exposed when exact id resolution cannot finish."""


def iter_desired_rows(
    task: EvalTask, *, batch_size: int = 10_000
) -> Iterator[list[str]]:
    # Row limit applies to historical tasks only; continuous runs forever.
    limit = task.spans_limit if task.run_type == RunType.HISTORICAL else None
    sampling_rate = task.sampling_rate if task.sampling_rate is not None else 100.0

    sql, params = _build_sample_query(
        project_id=str(task.project_id),
        row_type=task.row_type,
        salt=str(task.id),
        sampling_rate=float(sampling_rate),
        filters=task.filters or {},
        limit=limit,
        created_at_floor=_continuous_floor(task),
    )
    reader = get_reader()
    try:
        # US-scale span-attribute filters can exceed the per-query CH budget
        # even though one minute of the same exact predicate is cheap. Keep the
        # established query for small/empty tenants (where it proves the whole
        # result set in one read), then fall back to deterministic, adjacent
        # newest-to-oldest slices only for a bounded historical span task.
        #
        # Continuous tasks deliberately retain their streaming forward-window
        # query: buffering/slicing them would change cursor semantics. Session
        # and trace builders can aggregate/match across many spans, so slicing
        # those identities by individual span time is not generally exact.
        if (
            task.row_type in (RowType.SPANS, RowType.TRACES)
            and task.run_type == RunType.HISTORICAL
            and limit is not None
            and int(limit) <= _EVAL_TASK_BUFFERED_ID_LIMIT
        ):
            resolved_ids = _resolve_bounded_historical_span_ids(
                reader,
                sql=sql,
                params=params,
                project_id=str(task.project_id),
                salt=str(task.id),
                sampling_rate=float(sampling_rate),
                filters=task.filters or {},
                limit=int(limit),
                batch_size=batch_size,
                row_type=task.row_type,
            )
            yield from _iter_id_batches(resolved_ids, batch_size=batch_size)
            return

        batches = reader.stream_query(
            sql,
            params,
            batch_size=batch_size,
            settings=_EVAL_TASK_READ_SETTINGS,
        )
        if task.row_type in (RowType.SPANS, RowType.TRACES) and limit is not None:
            yield from _iter_unique_id_batches(
                batches,
                batch_size=batch_size,
                max_rows=int(limit),
            )
        else:
            yield from batches
    finally:
        reader.close()


def _resolve_bounded_historical_span_ids(
    reader,
    *,
    sql: str,
    params: dict[str, Any],
    project_id: str,
    salt: str,
    sampling_rate: float,
    filters: dict,
    limit: int,
    batch_size: int,
    row_type: str = RowType.SPANS,
) -> list[str]:
    """Resolve an exact bounded historical span/trace set without a wide scan.

    Both the whole-window fast path and fallback use the same canonical
    ``(start minute DESC, id ASC)`` order. This makes the selected ids
    independent of whether the first query happens to finish under load.
    If the whole-window query hits a CH read limit (or duplicate versions fill
    its candidate margin), scan adjacent one-minute windows newest-to-oldest;
    each minute is keyset-paged by distinct span id.

    If the shared deadline expires before either the requested cap is filled or
    the full time window is exhausted, raise a safe explicit failure. Returning
    a partial/empty set there would silently create the wrong evaluation task.
    """
    if limit <= 0:
        return []
    if row_type not in (RowType.SPANS, RowType.TRACES):
        raise ValueError("Bounded historical resolution supports spans and traces")

    deadline = monotonic() + _EVAL_TASK_TOTAL_READ_SECONDS
    try:
        whole_window_prefix, whole_window_raw_count = _collect_unique_query_ids(
            reader,
            sql,
            params,
            batch_size=batch_size,
            settings=_read_settings_before(deadline),
            max_unique=limit,
        )
    except Exception as exc:
        if not is_read_budget_error(exc):
            raise
    else:
        candidate_limit = int(params.get("lim") or params.get("id_limit") or 0)
        # Reaching the requested unique-id cap proves the canonical prefix.
        # Duplicate-margin exhaustion matters only when it did not.
        if len(whole_window_prefix) >= limit:
            return whole_window_prefix
        # A short bounded result proves CH exhausted the whole filtered window.
        if candidate_limit and whole_window_raw_count < candidate_limit:
            return whole_window_prefix

    # A trace is ordered/sliced by its root span, but TRACE_LIST filters may
    # intentionally match any span in that trace. Recompiling such a filter
    # against each one-minute root window would also narrow the child-span
    # membership subquery (currently to the minute ± one day). For those
    # filters, page unfiltered root IDs from each minute and verify each small
    # candidate set against the original full window. Root-only filters keep
    # the cheaper direct sliced path.
    trace_candidate_verification = (
        row_type == RowType.TRACES and not _trace_slice_fallback_is_exact(sql)
    )

    start_date = params.get("start_date")
    end_date = params.get("end_date")
    if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
        raise ValueError("Historical span query did not bind a valid time window")
    if start_date >= end_date:
        return []

    selected: list[str] = []
    seen: set[str] = set()
    scan_now = timezone.now()
    if timezone.is_naive(end_date):
        scan_now = scan_now.replace(tzinfo=None)
    slice_end = min(end_date, scan_now + _EVAL_TASK_MAX_FUTURE_SKEW)

    if slice_end < end_date:
        tail_query, tail_params = build_future_tail_probe(
            start=slice_end,
            end=end_date,
            root_only=row_type == RowType.TRACES,
            project_id=project_id,
        )
        try:
            remaining_settings = _read_settings_before(deadline)
            tail_settings = {
                **FUTURE_TAIL_PROBE_SETTINGS,
                "max_execution_time": min(
                    FUTURE_TAIL_PROBE_SETTINGS["max_execution_time"],
                    remaining_settings["max_execution_time"],
                ),
            }
            tail_has_rows = False
            for batch in reader.stream_query(
                tail_query,
                tail_params,
                batch_size=1,
                settings=tail_settings,
            ):
                if not isinstance(batch, list) or not batch:
                    raise ValueError("Malformed future-tail probe response")
                tail_has_rows = True
        except Exception:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
        if tail_has_rows:
            raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)

    while slice_end > start_date and len(selected) < limit:
        slice_start = max(start_date, slice_end - _EVAL_TASK_SLICE)
        after_id: str | None = None

        while len(selected) < limit:
            if trace_candidate_verification:
                page_size = _EVAL_TASK_TRACE_CANDIDATE_PAGE_SIZE
                page_sql, page_params = _build_trace_candidate_seed_query(
                    project_id=project_id,
                    salt=salt,
                    sampling_rate=sampling_rate,
                    start=slice_start,
                    end=slice_end,
                    after_id=after_id,
                    limit=page_size,
                )
            else:
                page_size = min(_EVAL_TASK_SLICE_PAGE_SIZE, limit - len(selected))
                page_sql, page_params = _build_sample_query(
                    project_id=project_id,
                    row_type=row_type,
                    salt=salt,
                    sampling_rate=sampling_rate,
                    filters=filters,
                    limit=page_size,
                    time_window=(slice_start, slice_end),
                    after_id=after_id,
                )
            try:
                page_unique_ids, _ = _collect_unique_query_ids(
                    reader,
                    page_sql,
                    page_params,
                    batch_size=batch_size,
                    settings=(
                        _bounded_result_settings(
                            deadline,
                            max_result_rows=page_size,
                        )
                        if trace_candidate_verification
                        else _read_settings_before(deadline)
                    ),
                    max_unique=page_size,
                )
            except Exception as exc:
                if not is_read_budget_error(exc):
                    raise
                raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None

            if not page_unique_ids:
                break

            matching_ids = (
                _verify_trace_candidates(
                    reader,
                    candidate_ids=page_unique_ids,
                    project_id=project_id,
                    salt=salt,
                    sampling_rate=sampling_rate,
                    filters=filters,
                    batch_size=batch_size,
                    deadline=deadline,
                )
                if trace_candidate_verification
                else set(page_unique_ids)
            )
            for row_id in page_unique_ids:
                if row_id in seen:
                    continue
                seen.add(row_id)
                if row_id in matching_ids:
                    selected.append(row_id)
                    if len(selected) >= limit:
                        return selected

            # Slice SQL selects DISTINCT ids. A short unique-id page proves the
            # minute is exhausted; a full page advances by id and reads its next
            # exact segment.
            unique_page_limit = int(
                page_params[
                    "candidate_limit" if trace_candidate_verification else "lim"
                ]
            )
            if len(page_unique_ids) < unique_page_limit:
                break
            next_after_id = max(page_unique_ids)
            if after_id is not None and next_after_id <= after_id:
                raise RuntimeError("Evaluation task id keyset did not advance")
            after_id = next_after_id

        slice_end = slice_start

    return selected


def _trace_slice_fallback_is_exact(sql: str) -> bool:
    """Whether root-time slicing preserves a TRACE_LIST query's semantics."""

    return (
        re.search(
            r"\btrace_id\s+(?:NOT\s+)?IN\s*\(\s*SELECT\b",
            sql,
            flags=re.IGNORECASE,
        )
        is None
    )


def _build_trace_candidate_seed_query(
    *,
    project_id: str,
    salt: str,
    sampling_rate: float,
    start: datetime,
    end: datetime,
    after_id: str | None,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    """Page current root trace IDs in one minute without user predicates.

    User predicates are intentionally deferred to
    :func:`_verify_trace_candidates`, which evaluates them against the original
    task window. Sampling is safe to push into this seed because it is a pure,
    deterministic function of ``trace_id``.
    """
    if limit <= 0:
        raise ValueError("Trace candidate limit must be greater than zero")
    if not 0 <= sampling_rate <= 100:
        raise ValueError("sampling_rate must be between 0 and 100")

    params: dict[str, Any] = {
        "candidate_project_id": str(project_id),
        "candidate_start": start,
        "candidate_end": end,
        "candidate_salt": str(salt),
        "candidate_rate": float(sampling_rate),
        "candidate_limit": int(limit),
    }
    after_fragment = ""
    if after_id is not None:
        params["candidate_after_id"] = str(after_id)
        after_fragment = "AND trace_id > %(candidate_after_id)s"

    sql = f"""
    SELECT DISTINCT trace_id
    FROM spans FINAL
    PREWHERE project_id = %(candidate_project_id)s
      AND start_time >= %(candidate_start)s
      AND start_time < %(candidate_end)s
    WHERE is_deleted = 0
      AND (parent_span_id IS NULL OR parent_span_id = '')
      AND modulo(
        cityHash64(%(candidate_salt)s, toString(trace_id)),
        100
      ) < %(candidate_rate)s
      {after_fragment}
    ORDER BY trace_id
    LIMIT %(candidate_limit)s
    """
    return sql, params


def _verify_trace_candidates(
    reader,
    *,
    candidate_ids: list[str],
    project_id: str,
    salt: str,
    sampling_rate: float,
    filters: dict,
    batch_size: int,
    deadline: float,
) -> set[str]:
    """Return candidates satisfying the original whole-window trace query.

    A timed-out candidate batch is split recursively under the same shared
    deadline. A single-candidate timeout cannot be proven complete and fails
    closed; rows yielded before any driver error are never accepted.
    """
    if not candidate_ids:
        return set()

    probe_sql, probe_params = _build_sample_query(
        project_id=project_id,
        row_type=RowType.TRACES,
        salt=salt,
        sampling_rate=sampling_rate,
        filters=filters,
        # Candidate scope bounds this query already. The uncapped builder form
        # uses LIMIT 1 BY trace_id, proving every candidate without relying on
        # the ordinary whole-window duplicate-version margin.
        limit=None,
        candidate_trace_ids=candidate_ids,
    )
    try:
        matching_ids, _ = _collect_unique_query_ids(
            reader,
            probe_sql,
            probe_params,
            batch_size=batch_size,
            settings=_bounded_result_settings(
                deadline,
                max_result_rows=len(candidate_ids),
            ),
            max_unique=len(candidate_ids),
        )
    except Exception as exc:
        if not is_read_budget_error(exc):
            raise
        if len(candidate_ids) > 1:
            midpoint = len(candidate_ids) // 2
            return _verify_trace_candidates(
                reader,
                candidate_ids=candidate_ids[:midpoint],
                project_id=project_id,
                salt=salt,
                sampling_rate=sampling_rate,
                filters=filters,
                batch_size=batch_size,
                deadline=deadline,
            ) | _verify_trace_candidates(
                reader,
                candidate_ids=candidate_ids[midpoint:],
                project_id=project_id,
                salt=salt,
                sampling_rate=sampling_rate,
                filters=filters,
                batch_size=batch_size,
                deadline=deadline,
            )
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE) from None
    return set(matching_ids)


def _collect_unique_query_ids(
    reader,
    sql: str,
    params: dict[str, Any],
    *,
    batch_size: int,
    settings: dict[str, Any],
    max_unique: int,
) -> tuple[list[str], int]:
    """Collect only the ordered unique prefix and raw row count.

    The old implementation first materialized up to ``2 * spans_limit`` raw
    ids and then built a second list/set. Folding both operations into the
    stream keeps the buffered fallback proportional to the requested result,
    capped by ``_EVAL_TASK_BUFFERED_ID_LIMIT``. The stream is still drained
    after the prefix fills: a driver error after yielding partial rows must
    propagate instead of turning those rows into a false-complete selection.
    """
    unique: list[str] = []
    seen: set[str] = set()
    raw_count = 0
    for batch in reader.stream_query(
        sql,
        params,
        batch_size=batch_size,
        settings=settings,
    ):
        raw_count += len(batch)
        for row_id in batch:
            normalized_id = str(row_id)
            if normalized_id in seen:
                continue
            if len(unique) < max_unique:
                seen.add(normalized_id)
                unique.append(normalized_id)
    return unique, raw_count


def _read_settings_before(deadline: float) -> dict[str, Any]:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise EvalTaskReadBudgetExceeded(_SAFE_READ_BUDGET_MESSAGE)
    settings = dict(_EVAL_TASK_READ_SETTINGS)
    settings["max_execution_time"] = min(
        float(settings["max_execution_time"]),
        remaining,
    )
    return settings


def _bounded_result_settings(
    deadline: float,
    *,
    max_result_rows: int,
) -> dict[str, Any]:
    """Clamp one read to both the shared deadline and its expected row cap."""
    settings = _read_settings_before(deadline)
    settings["max_result_rows"] = max(int(max_result_rows), 1)
    settings["result_overflow_mode"] = "throw"
    settings["use_skip_indexes_if_final"] = 1
    settings["optimize_use_projections"] = 1
    return settings


def _iter_id_batches(
    row_ids: list[str],
    *,
    batch_size: int,
) -> Iterator[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    for offset in range(0, len(row_ids), batch_size):
        yield row_ids[offset : offset + batch_size]


def _iter_unique_id_batches(
    batches: Iterable[list[str]],
    *,
    batch_size: int,
    max_rows: int,
) -> Iterator[list[str]]:
    """De-duplicate a bounded span/trace candidate stream, preserving order."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if max_rows <= 0:
        return

    seen: set[str] = set()
    output: list[str] = []
    for batch in batches:
        for row_id in batch:
            if row_id in seen:
                continue
            seen.add(row_id)
            output.append(row_id)
            if len(output) >= batch_size:
                yield output
                output = []
            if len(seen) >= max_rows:
                if output:
                    yield output
                return
    if output:
        yield output


def _continuous_floor(task: EvalTask) -> datetime | None:
    """Lower ``created_at`` bound for a continuous task's desired set.

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
    time_window: tuple[datetime, datetime] | None = None,
    after_id: str | None = None,
    candidate_trace_ids: list[str] | None = None,
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
    f = filters or {}
    ui_filters = list(f.get("filters") or [])
    dr = f.get("date_range")
    if isinstance(dr, list | tuple) and len(dr) == 2:
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

    # Legacy sibling constraints are still part of the typed task contract.
    # Compile them through the same physical-column filter path as the list
    # APIs instead of silently dropping them from task materialization.
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
        if not values:
            continue
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

    # Continuous forward floor. Appended last so it wins the lower bound in
    # parse_time_range over any date_range start above (a continuous task is
    # anchored at its own start, not an earlier configured window) — and so the
    # set isn't silently capped at parse_time_range's now-30d default.
    if created_at_floor is not None:
        ui_filters.append(
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "greater_than",
                    "filter_value": created_at_floor,
                },
            }
        )

    # observation_type is a legacy top-level task key. For spans, push it
    # through the normal filter compiler so sampling and bounded top-K happen
    # *after* the type restriction. Other row types retain their established
    # identity-subquery semantics below.
    ot = f.get("observation_type")
    ot_values = (
        [str(value) for value in ot]
        if isinstance(ot, list | tuple | set)
        else ([str(ot)] if ot else [])
    )
    if row_type == RowType.SPANS and ot_values:
        ui_filters.append(
            {
                "column_id": "observation_type",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": ot_values,
                },
            }
        )

    # The internal id keyset is compiled through the same physical-column
    # filter path as user filters. Slice time is added after builder compilation
    # below: making it another UI time filter would replace the saved window's
    # shared ``start_date`` parameter, incorrectly tightening the builder's
    # ingestion-time guard as well as its span-time predicate.
    if time_window is not None:
        if row_type not in (RowType.SPANS, RowType.TRACES):
            raise ValueError(
                "Time-sliced eval resolution currently supports spans and traces"
            )
        if row_type == RowType.TRACES:
            # For a trace slice, replace the saved whole-window time predicate
            # before compilation. This scopes both the root row and every
            # any-span membership subquery, while the caller retains the
            # original start/end solely to drive adjacent-slice iteration.
            ui_filters = [
                item
                for item in ui_filters
                if (item.get("column_id") or item.get("columnId"))
                not in {"created_at", "start_time"}
            ]
            ui_filters.append(
                {
                    "column_id": "start_time",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "datetime",
                        "filter_op": "between",
                        "filter_value": [time_window[0], time_window[1]],
                    },
                }
            )
    if after_id is not None:
        if row_type not in (RowType.SPANS, RowType.TRACES):
            raise ValueError(
                "Keyset eval resolution currently supports spans and traces"
            )
        ui_filters.append(
            {
                "column_id": ("span_id" if row_type == RowType.SPANS else "trace_id"),
                "filter_config": {
                    "col_type": "SYSTEM_METRIC",
                    "filter_type": "text",
                    "filter_op": "greater_than",
                    "filter_value": after_id,
                },
            }
        )

    if candidate_trace_ids and row_type != RowType.TRACES:
        raise ValueError("Candidate trace IDs are supported only for trace queries")
    builder_kwargs: dict[str, Any] = {
        "project_id": str(project_id),
        "filters": ui_filters,
    }
    if candidate_trace_ids:
        builder_kwargs["candidate_trace_ids"] = [
            str(trace_id) for trace_id in candidate_trace_ids if trace_id
        ]
    builder = get_v2_class(query_type)(**builder_kwargs)
    is_span_query = row_type == RowType.SPANS
    is_trace_query = row_type == RowType.TRACES
    is_session_query = row_type == RowType.SESSIONS
    sampling_pushed_down = is_span_query or is_trace_query or is_session_query
    distinct_slice_ids = (is_span_query or is_trace_query) and time_window is not None
    recent_minute_order = (
        (is_span_query or is_trace_query) and limit is not None and time_window is None
    )
    candidate_limit = None
    if limit is not None:
        candidate_limit = int(limit)
        if (is_span_query or is_trace_query) and not distinct_slice_ids:
            candidate_limit *= _DEDUP_ID_CANDIDATE_MULTIPLIER

    if sampling_pushed_down:
        build_id_kwargs = {
            "limit": None if distinct_slice_ids else candidate_limit,
            "sampling_salt": str(salt),
            "sampling_rate": float(sampling_rate),
        }
        if is_span_query or is_trace_query:
            build_id_kwargs["order_by_recent_minute"] = recent_minute_order
            build_id_kwargs["latest_state"] = (
                time_window is not None or limit is not None
            )
        inner_sql, params = builder.build_id_query(
            # For one bounded minute, de-duplicate before the outer top-K. This
            # makes slice completion/keyset pagination operate on unique ids,
            # rather than relying on a duplicate-version margin.
            **build_id_kwargs,
        )
        params = dict(params)
    else:
        inner_sql, params = builder.build_id_query()
        params = {**params, "salt": str(salt), "rate": float(sampling_rate)}

    if time_window is not None and is_span_query:
        inner_sql, params = _add_span_slice_bounds(
            inner_sql,
            params,
            start=time_window[0],
            end=time_window[1],
        )

    # observation_type is a legacy top-level key, not a filter-builder column;
    # constrain the id set against spans directly.
    ot_pred = ""
    if ot_values and not is_span_query:
        params["otypes"] = tuple(ot_values)
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

    # For a bounded historical span/trace selector, sampling, canonical
    # newest-minute ordering, and the duplicate-margin LIMIT are already pushed
    # into build_id_query(). With no legacy outer predicate left to apply, the
    # wrapper below would perform the same ORDER BY + LIMIT a second time over
    # as many as 2 * spans_limit rows. Besides being redundant, that second
    # top-K can hold enough String ids to trip the selector's 256 MiB cap.
    #
    # Return the builder query directly. stream_query() deliberately consumes
    # only its first column, so the private eval_order_start_time ordering
    # column remains invisible to callers. Slice queries still need DISTINCT,
    # and trace/session/voice queries with an outer predicate keep the wrapper.
    if recent_minute_order and not ot_pred:
        return inner_sql, params

    result_limit = (
        limit
        if distinct_slice_ids
        else (candidate_limit if sampling_pushed_down else limit)
    )
    limit_sql = ""
    if result_limit is not None:
        limit_sql = "LIMIT %(lim)s"
        params["lim"] = int(result_limit)

    # modulo() not `%` — clickhouse-connect treats a literal `%` as a
    # parameter-format marker. Span/trace/session sampling is pushed into the
    # bounded inner query; voice calls retain their established outer sampling.
    sample_predicate = (
        "1 = 1"
        if sampling_pushed_down
        else f"modulo(cityHash64(%(salt)s, toString({id_col})), 100) < %(rate)s"
    )
    # A capped inner query is already bounded; sorting that candidate set by id
    # gives stable task materialization order. An unbounded continuous scan
    # intentionally stays unordered so it can stream without a full-window sort.
    if recent_minute_order:
        order_sql = f"ORDER BY toStartOfMinute(eval_order_start_time) DESC, {id_col}"
    else:
        order_sql = (
            ""
            if sampling_pushed_down and result_limit is None
            else f"ORDER BY {id_col}"
        )
    select_modifier = "DISTINCT " if distinct_slice_ids else ""
    sql = (
        f"SELECT {select_modifier}{id_col} FROM ({inner_sql}) "
        f"WHERE {sample_predicate} "
        f"{ot_pred} "
        f"{order_sql} {limit_sql}"
    )
    return sql, params


def _add_span_slice_bounds(
    sql: str,
    params: dict[str, Any],
    *,
    start: datetime,
    end: datetime,
) -> tuple[str, dict[str, Any]]:
    """Add a span-time slice without replacing the saved full-window params."""
    params = {
        **params,
        "eval_slice_start": start,
        "eval_slice_end": end,
    }
    predicate = (
        "\nAND start_time >= %(eval_slice_start)s\nAND start_time < %(eval_slice_end)s"
    )

    # V2 builders append their required SETTINGS at the query tail. Predicates
    # belong before that clause; rpartition avoids touching SETTINGS that might
    # appear inside a nested filter subquery.
    head, marker, tail = sql.rpartition("\nSETTINGS ")
    if marker:
        return f"{head.rstrip()}{predicate}\nSETTINGS {tail}", params
    return f"{sql.rstrip()}{predicate}", params
