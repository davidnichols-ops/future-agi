from __future__ import annotations

import json
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from tracer.services.clickhouse.client import get_clickhouse_client
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)

READ_TIMEOUT_MS = 4_000
MAX_PAGE_SIZE = 100
_USAGE_TABLE = "usage_apicalllog"
_TRACE_PROJECT_DICT = "trace_dict"

_READ_SETTINGS = {
    "max_threads": 2,
    "max_rows_to_read": 8_000_000,
    "read_overflow_mode": "throw",
    "max_bytes_to_read": 768 * 1024 * 1024,
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
    time_predicate: bool,
    project_scoped: bool = True,
) -> str:
    candidate_scope = [*scope, f"source_id = %({source_id_param})s"]
    if time_predicate:
        candidate_scope.extend(
            [
                "created_at >= %(start_date)s",
                "created_at <= %(end_date)s",
            ]
        )
    project_scope = ""
    if project_scoped:
        project_scope = (
            "WHERE (eval_trace_id = '' OR dictGetOrDefault("
            f"'{_TRACE_PROJECT_DICT}', 'project_id', toUUIDOrZero(eval_trace_id), "
            "toUUID('00000000-0000-0000-0000-000000000000')) "
            "IN %(project_ids)s)"
        )
    return f"""
        SELECT {projection}
        FROM {_USAGE_TABLE}
        PREWHERE {" AND ".join(candidate_scope)}
        {project_scope}
        ORDER BY _peerdb_version DESC
        LIMIT 1 BY id
    """


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
            "limit": page_size,
            "offset": page * page_size,
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
        time_predicate=False,
        project_scoped=False,
    )
    total_query = f"""
        SELECT count() AS total_runs
        FROM ({total_slice}) AS latest_usage
        WHERE {live}
    """

    # Period stats remain exact and partition-bounded.
    stats_slice = _latest_usage_slice(
        projection="id, created_at, status, deleted, _peerdb_is_deleted",
        scope=scope,
        time_predicate=True,
    )
    stats_query = f"""
        SELECT
            count() AS runs_period,
            countIf(
                status = %(success_status)s
            ) AS success_count,
            countIf(
                status = %(error_status)s
            ) AS error_count
        FROM ({stats_slice}) AS latest_usage
        WHERE {live}
    """

    period_projection = (
        "id, log_id, config, status, created_at, deleted, _peerdb_is_deleted"
    )
    period_slice = _latest_usage_slice(
        projection=period_projection,
        scope=scope,
        time_predicate=True,
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
            avgIf({duration_expr}, {duration_present}) AS avg_duration,
            avgIf({score_expr}, {score_expr} IS NOT NULL) AS avg_score,
            countIf({pass_expr}) AS pass_count,
            countIf({fail_expr}) AS fail_count
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
        LIMIT %(limit)s OFFSET %(offset)s
    """

    deadline_at = time.monotonic() + (READ_TIMEOUT_MS / 1000.0)

    def remaining_ms(operation: str) -> int:
        remaining = int((deadline_at - time.monotonic()) * 1000)
        if remaining <= 0:
            raise EvalUsageReadError(
                EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                operations=(operation,),
            )
        return min(READ_TIMEOUT_MS, remaining)

    def execute(
        operation: str,
        query: str,
        *,
        settings: dict[str, Any] | None = None,
    ):
        # Client acquisition is inside the future.  The first pool checkout may
        # establish a network connection, and that connect time must consume the
        # same request-owned wall deadline as query execution.
        client = get_clickhouse_client()
        rows, _columns, _elapsed = client.execute_read(
            query,
            params,
            timeout_ms=remaining_ms(operation),
            settings={**_READ_SETTINGS, **(settings or {})},
        )
        return rows

    # Independent reads share one real monotonic wall deadline.  Do not use the
    # executor context manager here: its implicit shutdown(wait=True) would make
    # a connect stall block the request after our timeout had already expired.
    pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="eval-usage-ch")
    futures: dict[Future, str] = {}
    try:
        futures[pool.submit(execute, "total", total_query)] = "total"
        futures[pool.submit(execute, "stats", stats_query)] = "stats"
        futures[
            pool.submit(
                execute,
                "chart",
                chart_query,
                settings={
                    "max_result_rows": 550,
                    "max_result_bytes": 4 * 1024 * 1024,
                    "result_overflow_mode": "throw",
                },
            )
        ] = "chart"
        futures[
            pool.submit(
                execute,
                "page",
                page_query,
                settings={
                    "max_result_rows": page_size,
                    "max_result_bytes": 16 * 1024 * 1024,
                    "result_overflow_mode": "throw",
                },
            )
        ] = "page"

        completed_rows: dict[str, list[tuple]] = {}
        pending = set(futures)
        while pending:
            remaining_seconds = deadline_at - time.monotonic()
            if remaining_seconds <= 0:
                raise EvalUsageReadError(
                    EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                    operations=tuple(sorted(futures[future] for future in pending)),
                )
            done, pending = wait(
                pending,
                timeout=remaining_seconds,
                return_when=FIRST_COMPLETED,
            )
            if not done:
                raise EvalUsageReadError(
                    EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
                    operations=tuple(sorted(futures[future] for future in pending)),
                )
            for future in done:
                operation = futures[future]
                try:
                    completed_rows[operation] = future.result()
                except EvalUsageReadError:
                    raise
                except Exception as exc:
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
                    # Python/application defects are not database degradation.
                    # Preserve their original type and traceback.
                    raise

        total_rows = completed_rows["total"]
        stats_rows = completed_rows["stats"]
        chart_rows = completed_rows["chart"]
        page_rows = completed_rows["page"]
    finally:
        for future in futures:
            future.cancel()
        pool.shutdown(wait=False, cancel_futures=True)

    stats = stats_rows[0] if stats_rows else (0, 0, 0)
    chart = [
        EvalUsageChartBucket(
            bucket=row[0],
            calls=int(row[1] or 0),
            avg_duration=float(row[2]) if row[2] is not None else None,
            avg_score=float(row[3]) if row[3] is not None else None,
            pass_count=int(row[4] or 0),
            fail_count=int(row[5] or 0),
        )
        for row in chart_rows
    ]
    logs = [
        EvalUsageLog(
            log_id=str(row[0]),
            config=_decode_config(row[1]),
            status=str(row[2] or ""),
            created_at=row[3],
        )
        for row in page_rows
    ]
    total_runs = int(total_rows[0][0] or 0) if total_rows else 0
    runs_period = int(stats[0] or 0)
    return EvalUsageRead(
        total_runs=total_runs,
        runs_period=runs_period,
        success_count=int(stats[1] or 0),
        error_count=int(stats[2] or 0),
        chart=chart,
        logs=logs,
        completeness=EvalUsageReadCompleteness.COMPLETE,
        unavailable_fields=(),
    )


__all__ = [
    "EvalUsageChartBucket",
    "EvalUsageLog",
    "EvalUsageRead",
    "EvalUsageReadCompleteness",
    "EvalUsageReadError",
    "EvalUsageReadErrorCode",
    "read_eval_usage",
]
