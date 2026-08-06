from __future__ import annotations

import json
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, NoReturn

from tracer.services.clickhouse.client import get_clickhouse_client
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.services.clickhouse.trace_project_scope import (
    latest_live_trace_projects_sql,
)

READ_TIMEOUT_MS = 55 * 60 * 1000
QUERY_TIMEOUT_MS = 120_000
MAX_PAGE_SIZE = 100
_USAGE_TABLE = "usage_apicalllog"
_PARTITION_DAYS = 31
_MIN_PARTITION_MINUTES = 60

_READ_SETTINGS = {
    "max_threads": 2,
    "max_rows_to_read": 25_000_000,
    "read_overflow_mode": "throw",
    "max_bytes_to_read": 4 * 1024 * 1024 * 1024,
    "max_memory_usage": 256 * 1024 * 1024,
    "timeout_overflow_mode": "throw",
}


@dataclass(frozen=True)
class EvalUsageLog:
    log_id: str
    config: dict[str, Any]
    status: str
    created_at: datetime | None


@dataclass(frozen=True)
class EvalUsageChartBucket:
    bucket: datetime
    calls: int
    avg_duration: float | None
    avg_score: float | None
    pass_count: int
    fail_count: int


class EvalUsageReadCompleteness(StrEnum):
    COMPLETE = "complete"
    DEGRADED = "degraded"


class EvalUsageReadErrorCode(StrEnum):
    DEADLINE_EXCEEDED = "deadline_exceeded"
    QUERY_FAILED = "query_failed"


class EvalUsageReadError(RuntimeError):
    """Sanitized, typed failure for a bounded eval-usage CH read."""

    def __init__(
        self,
        code: EvalUsageReadErrorCode,
        *,
        operations: tuple[str, ...] = (),
    ) -> None:
        self.code = code
        self.operations = operations
        super().__init__(code.value)


@dataclass(frozen=True)
class EvalUsageRead:
    total_runs: int
    runs_period: int
    success_count: int
    error_count: int
    chart: list[EvalUsageChartBucket]
    logs: list[EvalUsageLog]
    completeness: EvalUsageReadCompleteness
    unavailable_fields: tuple[str, ...]


def _decode_config(value: Any) -> dict[str, Any]:
    decoded = value
    # Historical rows include both normal JSON objects and JSONB values mirrored
    # as a JSON string. Decode at most twice; anything else is malformed input,
    # not a reason to fail the whole usage page.
    for _ in range(2):
        if not isinstance(decoded, str):
            break
        try:
            decoded = json.loads(decoded)
        except (TypeError, ValueError):
            return {}
    return decoded if isinstance(decoded, dict) else {}


def _finite_float_or_none(value: Any) -> float | None:
    """Normalize ClickHouse empty-aggregate sentinels at the read boundary.

    ClickHouse returns ``NaN`` for an empty ``avgIf`` over non-nullable
    floating-point inputs.  Non-finite values are not meaningful usage
    metrics and must not reach response formatting, where rounding ``NaN``
    raises for integer precision.
    """

    if value is None:
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if math.isfinite(normalized) else None


def _scope(
    *,
    organization_id: str,
    workspace_id: str | None,
) -> tuple[list[str], dict[str, Any]]:
    predicates = ["organization_id = toUUID(%(organization_id)s)"]
    params: dict[str, Any] = {"organization_id": organization_id}
    if workspace_id is not None:
        predicates.append("workspace_id = toUUID(%(workspace_id)s)")
        params["workspace_id"] = workspace_id
    return predicates, params


def _latest_usage_slice(
    *,
    projection: str,
    scope: list[str],
    source_id_param: str = "template_id",
    start_param: str | None,
    end_param: str | None,
    project_scoped: bool = True,
) -> str:
    candidate_scope = [*scope, f"source_id = %({source_id_param})s"]
    if start_param:
        candidate_scope.append(f"created_at >= %({start_param})s")
    if end_param:
        candidate_scope.append(f"created_at < %({end_param})s")
    project_join = ""
    project_scope = ""
    if project_scoped:
        trace_candidates = f"""
            SELECT DISTINCT toUUIDOrZero(eval_trace_id) AS trace_id
            FROM {_USAGE_TABLE}
            PREWHERE {" AND ".join(candidate_scope)}
            WHERE eval_trace_id != ''
        """
        trace_projects = latest_live_trace_projects_sql(
            candidate_trace_ids_sql=trace_candidates
        )
        project_join = (
            f"LEFT JOIN ({trace_projects}) AS allowed_trace_projects "
            "ON allowed_trace_projects.trace_id = toUUIDOrZero(eval_trace_id)"
        )
        project_scope = (
            "WHERE (eval_trace_id = '' OR "
            "allowed_trace_projects.project_id IN %(project_ids)s)"
        )
    return f"""
        SELECT {projection}
        FROM {_USAGE_TABLE}
        {project_join}
        PREWHERE {" AND ".join(candidate_scope)}
        {project_scope}
        ORDER BY _peerdb_version DESC
        LIMIT 1 BY id
    """


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _partition_windows(
    start: datetime,
    end: datetime,
    *,
    days: int = _PARTITION_DAYS,
) -> tuple[tuple[datetime, datetime], ...]:
    """Return deterministic half-open windows with no gaps or overlaps."""

    current = _utc(start)
    stop = _utc(end)
    if current >= stop:
        return ()
    width = timedelta(days=max(1, days))
    windows: list[tuple[datetime, datetime]] = []
    while current < stop:
        partition_end = min(current + width, stop)
        windows.append((current, partition_end))
        current = partition_end
    return tuple(windows)


def _split_window(
    start: datetime,
    end: datetime,
    *,
    minimum_minutes: int = _MIN_PARTITION_MINUTES,
) -> tuple[tuple[datetime, datetime], tuple[datetime, datetime]] | None:
    minimum = timedelta(minutes=max(1, minimum_minutes))
    if end - start <= minimum:
        return None
    midpoint = start + (end - start) / 2
    # Microsecond precision matches usage_apicalllog.created_at. Keep both
    # children non-empty even for odd-width ranges.
    if midpoint <= start or midpoint >= end:
        return None
    return (start, midpoint), (midpoint, end)


def read_eval_usage(
    *,
    organization_id: str,
    workspace_id: str | None,
    project_ids: list[str] | tuple[str, ...],
    template_id: str,
    start_date: datetime,
    end_date: datetime,
    bucket_minutes: int,
    page: int,
    page_size: int,
) -> EvalUsageRead:
    """Read one eval template's stats, chart, and page from ClickHouse.

    Every query bounds work and narrows to the tenant/template candidate slice
    before collapsing physical versions. Production's historical table is
    ordered by ``id`` (new installs may use a tenant/time-aware key), so this
    path must remain safe without assuming tenant predicates are primary-key
    pruning. Tombstone predicates are deliberately outside the collapse so a
    deleted newest version cannot resurrect an older row.
    """

    if page < 0:
        raise ValueError("page must be non-negative")
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")

    scope, params = _scope(
        organization_id=organization_id,
        workspace_id=workspace_id,
    )
    params.update(
        {
            "template_id": template_id,
            # Keep IN syntactically valid and fail closed for trace-attached
            # rows when this template has no project config in the workspace.
            "project_ids": tuple(project_ids)
            or ("00000000-0000-0000-0000-000000000000",),
            "start_date": start_date,
            "end_date": end_date,
            "bucket_minutes": bucket_minutes,
            "success_status": "success",
            "error_status": "error",
        }
    )

    live = "_peerdb_is_deleted = 0 AND deleted = 0"
    # Preserve the pre-CH contract exactly: total_runs is all live runs for the
    # organization/workspace/template, independent of the requested period.
    # Unlike period rendering, that contract never had project membership in
    # its scope. Avoiding the trace dictionary here keeps this exact count on
    # the table's organization/source ordering while the shared finite budget
    # turns an unprovable count into a typed failure, never a partial success.
    total_slice = _latest_usage_slice(
        projection="id, deleted, _peerdb_is_deleted",
        scope=scope,
        start_param="partition_start",
        end_param="partition_end",
        project_scoped=False,
    )
    total_query = f"""
        SELECT count() AS total_runs
        FROM ({total_slice}) AS latest_usage
        WHERE {live}
    """

    period_projection = (
        "id, log_id, config, status, created_at, deleted, _peerdb_is_deleted"
    )
    period_slice = _latest_usage_slice(
        projection=period_projection,
        scope=scope,
        start_param="partition_start",
        end_param="partition_end",
    )

    # ``config`` has existed in two encodings: a JSON object and a JSON string
    # containing that object. Normalize once in SQL before extracting chart
    # values. Page rows still return the original config for lossless details.
    config_expr = (
        "if(isValidJSON(config) AND JSONType(config) = 'String', "
        "JSONExtractString(config), config)"
    )
    output_raw = f"JSONExtractRaw({config_expr}, 'output', 'output')"
    output_type = f"JSONType({output_raw})"
    output_text = f"lowerUTF8(JSONExtractString({config_expr}, 'output', 'output'))"
    output_label = (
        f"lowerUTF8(JSONExtractString({config_expr}, 'output', 'output', 'label'))"
    )
    score_expr = (
        "multiIf("
        f"{output_type} IN ('Int64', 'UInt64', 'Float64'), "
        f"toFloat64OrNull({output_raw}), "
        f"{output_type} = 'Object' AND "
        f"JSONHas({config_expr}, 'output', 'output', 'score'), "
        f"toNullable(JSONExtractFloat({config_expr}, 'output', 'output', 'score')), "
        f"{output_text} IN ('passed', 'pass'), toNullable(1.0), "
        f"{output_text} IN ('failed', 'fail'), toNullable(0.0), "
        "CAST(NULL, 'Nullable(Float64)'))"
    )
    duration_present = (
        f"JSONHas({config_expr}, 'duration') OR JSONHas({config_expr}, 'response_time')"
    )
    duration_expr = (
        f"if(JSONHas({config_expr}, 'duration'), "
        f"JSONExtractFloat({config_expr}, 'duration'), "
        f"JSONExtractFloat({config_expr}, 'response_time'))"
    )
    aggregate_pass_present = f"JSONHas({config_expr}, 'output', 'aggregate_pass')"
    aggregate_pass = f"JSONExtractBool({config_expr}, 'output', 'aggregate_pass')"
    pass_expr = (
        f"({aggregate_pass_present} AND {aggregate_pass} = 1) "
        f"OR {output_label} IN ('passed', 'pass') "
        f"OR {output_text} IN ('passed', 'pass')"
    )
    fail_expr = (
        f"({aggregate_pass_present} AND {aggregate_pass} = 0) "
        f"OR {output_label} IN ('failed', 'fail') "
        f"OR {output_text} IN ('failed', 'fail')"
    )
    chart_query = f"""
        SELECT
            toStartOfInterval(
                created_at,
                toIntervalMinute(%(bucket_minutes)s),
                'UTC'
            ) AS bucket,
            count() AS calls,
            sumKahanIf({duration_expr}, {duration_present}) AS duration_sum,
            countIf({duration_present}) AS duration_count,
            sumKahanIf({score_expr}, {score_expr} IS NOT NULL) AS score_sum,
            countIf({score_expr} IS NOT NULL) AS score_count,
            countIf({pass_expr}) AS pass_count,
            countIf({fail_expr}) AS fail_count,
            countIf(status = %(success_status)s) AS success_count,
            countIf(status = %(error_status)s) AS error_count
        FROM ({period_slice}) AS latest_usage
        WHERE {live}
        GROUP BY bucket
        ORDER BY bucket
    """

    page_query = f"""
        SELECT
            toString(log_id) AS log_id,
            config,
            status,
            created_at
        FROM ({period_slice}) AS latest_usage
        WHERE {live}
        ORDER BY created_at DESC, id DESC
        LIMIT %(partition_limit)s OFFSET %(partition_offset)s
    """

    deadline_at = time.monotonic() + (READ_TIMEOUT_MS / 1000.0)

    def remaining_ms(operation: str, *, cap_ms: int = QUERY_TIMEOUT_MS) -> int:
        remaining = int((deadline_at - time.monotonic()) * 1000)
        if remaining <= 0:
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                operations=(operation,),
            )
        return min(cap_ms, remaining)

    def raise_typed(operation: str, exc: Exception) -> NoReturn:
        if isinstance(exc, EvalUsageReadError):
            raise exc
        if is_read_budget_error(exc):
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                operations=(operation,),
            ) from exc
        if is_clickhouse_query_error(exc):
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.QUERY_FAILED,
                operations=(operation,),
            ) from exc
        raise exc

    worker_pool = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="eval-usage-exact-ch",
    )

    def execute_read(
        operation: str,
        query: str,
        query_params: dict[str, Any],
        query_settings: dict[str, Any],
    ) -> list[tuple]:
        def read():
            client = get_clickhouse_client()
            rows, _columns, _elapsed = client.execute_read(
                query,
                query_params,
                timeout_ms=remaining_ms(operation),
                settings=query_settings,
            )
            return rows

        future = worker_pool.submit(read)
        try:
            return future.result(timeout=max(deadline_at - time.monotonic(), 0))
        except FutureTimeoutError as exc:
            future.cancel()
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                operations=(operation,),
            ) from exc

    def execute_typed(
        operation: str,
        query: str,
        query_params: dict[str, Any],
        query_settings: dict[str, Any],
    ) -> list[tuple]:
        try:
            return execute_read(operation, query, query_params, query_settings)
        except Exception as exc:
            raise_typed(operation, exc)

    # Freeze physical visibility in O(1). The legacy CDC table is fenced by
    # its sync timestamp; direct-write traces use epoch-nanosecond versions.
    # This avoids the previous max(version) full scans while every later
    # partition still observes one immutable snapshot.
    ceiling_query = """
        SELECT
            toUnixTimestamp64Micro(now64(6, 'UTC')) AS usage_version_ceiling,
            toUnixTimestamp64Nano(now64(9, 'UTC')) AS trace_version_ceiling
    """

    try:
        ceiling_rows = execute_typed(
            "snapshot",
            ceiling_query,
            params,
            {**_READ_SETTINGS, "max_result_rows": 1},
        )
        if not ceiling_rows:
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.QUERY_FAILED,
                operations=("snapshot",),
            )
        usage_version_ceiling = max(int(ceiling_rows[0][0] or 0), 1)
        trace_version_ceiling = max(int(ceiling_rows[0][1] or 0), 1)
        snapshot_settings = {
            **_READ_SETTINGS,
            "additional_table_filters": {
                _USAGE_TABLE: (
                    "_peerdb_synced_at < "
                    f"fromUnixTimestamp64Micro({usage_version_ceiling}, 'UTC')"
                ),
                "traces": f"_version < {trace_version_ceiling}",
            },
        }

        bound_scope = [*scope, "source_id = %(template_id)s"]

        def bound_query(direction: str) -> str:
            if direction not in {"ASC", "DESC"}:
                raise ValueError("invalid usage-bound direction")
            return f"""
                SELECT created_at
                FROM {_USAGE_TABLE}
                PREWHERE {" AND ".join(bound_scope)}
                ORDER BY created_at {direction}, id {direction}
                LIMIT 1
            """

        earliest_rows = execute_typed(
            "total_bounds",
            bound_query("ASC"),
            params,
            {**snapshot_settings, "max_result_rows": 1},
        )
        if not earliest_rows:
            return EvalUsageRead(
                total_runs=0,
                runs_period=0,
                success_count=0,
                error_count=0,
                chart=[],
                logs=[],
                completeness=EvalUsageReadCompleteness.COMPLETE,
                unavailable_fields=(),
            )
        latest_rows = execute_typed(
            "total_bounds",
            bound_query("DESC"),
            params,
            {**snapshot_settings, "max_result_rows": 1},
        )
        if not latest_rows:
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.QUERY_FAILED,
                operations=("total_bounds",),
            )
        total_start = _utc(earliest_rows[0][0])
        total_end = _utc(latest_rows[0][0]) + timedelta(microseconds=1)
        period_start = _utc(start_date)
        period_end = _utc(end_date) + timedelta(microseconds=1)

        def partition_params(start: datetime, end: datetime) -> dict[str, Any]:
            return {
                **params,
                "partition_start": start,
                "partition_end": end,
            }

        def execute_partitions(
            operation: str,
            query: str,
            windows: tuple[tuple[datetime, datetime], ...],
            *,
            settings: dict[str, Any],
        ) -> list[tuple[datetime, datetime, list[tuple]]]:
            completed: list[tuple[datetime, datetime, list[tuple]]] = []

            def visit(start: datetime, end: datetime) -> None:
                try:
                    rows = execute_read(
                        operation,
                        query,
                        partition_params(start, end),
                        {**snapshot_settings, **settings},
                    )
                except Exception as exc:
                    split = _split_window(start, end)
                    if is_read_budget_error(exc) and split is not None:
                        visit(*split[0])
                        visit(*split[1])
                        return
                    raise_typed(operation, exc)
                    return
                completed.append((start, end, rows))

            for window_start, window_end in windows:
                visit(window_start, window_end)
            return completed

        total_partitions = execute_partitions(
            "total",
            total_query,
            _partition_windows(total_start, total_end),
            settings={"max_result_rows": 1},
        )
        total_runs = 0
        for _start, _end, rows in total_partitions:
            if len(rows) != 1:
                raise EvalUsageReadError(
                    EvalUsageReadErrorCode.QUERY_FAILED,
                    operations=("total",),
                )
            total_runs += int(rows[0][0] or 0)

        chart_partitions = execute_partitions(
            "chart",
            chart_query,
            _partition_windows(period_start, period_end),
            settings={
                "max_result_rows": 50_000,
                "max_result_bytes": 16 * 1024 * 1024,
                "result_overflow_mode": "throw",
            },
        )

        bucket_state: dict[datetime, dict[str, Any]] = defaultdict(
            lambda: {
                "calls": 0,
                "duration_sums": [],
                "duration_count": 0,
                "duration_valid": True,
                "score_sums": [],
                "score_count": 0,
                "score_valid": True,
                "pass_count": 0,
                "fail_count": 0,
            }
        )
        partition_counts: list[tuple[datetime, datetime, int]] = []
        runs_period = 0
        success_count = 0
        error_count = 0
        for window_start, window_end, rows in chart_partitions:
            window_calls = 0
            for row in rows:
                bucket = row[0]
                state = bucket_state[bucket]
                calls = int(row[1] or 0)
                duration_count = int(row[3] or 0)
                score_count = int(row[5] or 0)
                duration_sum = _finite_float_or_none(row[2])
                score_sum = _finite_float_or_none(row[4])
                state["calls"] += calls
                state["duration_count"] += duration_count
                state["score_count"] += score_count
                if duration_count:
                    if duration_sum is None:
                        state["duration_valid"] = False
                    else:
                        state["duration_sums"].append(duration_sum)
                if score_count:
                    if score_sum is None:
                        state["score_valid"] = False
                    else:
                        state["score_sums"].append(score_sum)
                state["pass_count"] += int(row[6] or 0)
                state["fail_count"] += int(row[7] or 0)
                success_count += int(row[8] or 0)
                error_count += int(row[9] or 0)
                window_calls += calls
                runs_period += calls
            partition_counts.append((window_start, window_end, window_calls))

        chart: list[EvalUsageChartBucket] = []
        for bucket in sorted(bucket_state):
            state = bucket_state[bucket]
            duration_count = state["duration_count"]
            score_count = state["score_count"]
            avg_duration = (
                math.fsum(state["duration_sums"]) / duration_count
                if duration_count and state["duration_valid"]
                else None
            )
            avg_score = (
                math.fsum(state["score_sums"]) / score_count
                if score_count and state["score_valid"]
                else None
            )
            chart.append(
                EvalUsageChartBucket(
                    bucket=bucket,
                    calls=state["calls"],
                    avg_duration=avg_duration,
                    avg_score=avg_score,
                    pass_count=state["pass_count"],
                    fail_count=state["fail_count"],
                )
            )

        page_rows: list[tuple] = []
        page_offset = page * page_size
        for window_start, window_end, window_calls in reversed(partition_counts):
            if len(page_rows) >= page_size:
                break
            if page_offset >= window_calls:
                page_offset -= window_calls
                continue
            partition_limit = page_size - len(page_rows)
            expected_rows = min(partition_limit, window_calls - page_offset)
            page_params = {
                **partition_params(window_start, window_end),
                "partition_limit": partition_limit,
                "partition_offset": page_offset,
            }
            try:
                rows = execute_read(
                    "page",
                    page_query,
                    page_params,
                    {
                        **snapshot_settings,
                        "max_result_rows": partition_limit,
                        "max_result_bytes": 16 * 1024 * 1024,
                        "result_overflow_mode": "throw",
                    },
                )
            except Exception as exc:
                raise_typed("page", exc)
            if len(rows) != expected_rows:
                raise EvalUsageReadError(
                    EvalUsageReadErrorCode.QUERY_FAILED,
                    operations=("page",),
                )
            page_rows.extend(rows)
            page_offset = 0

        logs = [
            EvalUsageLog(
                log_id=str(row[0]),
                config=_decode_config(row[1]),
                status=str(row[2] or ""),
                created_at=row[3],
            )
            for row in page_rows
        ]
        return EvalUsageRead(
            total_runs=total_runs,
            runs_period=runs_period,
            success_count=success_count,
            error_count=error_count,
            chart=chart,
            logs=logs,
            completeness=EvalUsageReadCompleteness.COMPLETE,
            unavailable_fields=(),
        )
    except Exception as exc:
        if isinstance(exc, EvalUsageReadError):
            raise
        raise_typed("eval_usage", exc)
    finally:
        worker_pool.shutdown(wait=False, cancel_futures=True)


__all__ = [
    "EvalUsageChartBucket",
    "EvalUsageLog",
    "EvalUsageRead",
    "EvalUsageReadCompleteness",
    "EvalUsageReadError",
    "EvalUsageReadErrorCode",
    "read_eval_usage",
]
