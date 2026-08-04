"""Business logic for the Observe Users list and CSV export.

HTTP-free layer between the request boundary and the response: scope resolution,
ClickHouse query/execute, row formatting, span-attribute enrichment, and CSV
serialization. ``UsersView`` keeps only (de)serialization and response building.
"""

import csv
import io
import json
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import Any

import structlog

from tracer.services.clickhouse.read_budget import (
    ReadDeadline,
    ReadDeadlineExceeded,
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.services.clickhouse.v2.query_builders.user_list import (
    UserListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_service import V2AnalyticsQueryService

logger = structlog.get_logger(__name__)


# (header, source field) — column order is the frontend export contract.
USERS_EXPORT_COLUMNS = [
    ("User ID", "user_id"),
    ("User ID Type", "user_id_type"),
    ("User ID Hash", "user_id_hash"),
    ("First Active", "activated_at"),
    ("Last Active", "last_active"),
    ("No. of Traces", "num_traces"),
    ("No. of Sessions", "num_sessions"),
    ("Avg Session Duration (s)", "avg_session_duration"),
    ("Total Tokens", "total_tokens"),
    ("Total Cost ($)", "total_cost"),
    ("Avg Latency / Trace (ms)", "avg_trace_latency"),
    ("No. of LLM Calls", "num_llm_calls"),
    ("Guardrails Triggered", "num_guardrails_triggered"),
    ("Evals Pass Rate (%)", "bool_eval_pass_rate"),
    ("Input Tokens", "input_tokens"),
    ("Output Tokens", "output_tokens"),
]


# CSV-injection guard: a cell starting with one of these executes as a formula
# in Excel/Sheets, so customer-controlled strings get a leading quote prefixed.
_CSV_FORMULA_TRIGGERS = ("=", "+", "-", "@", "\t", "\r")

_SKIP_ATTR_PREFIXES = (
    "raw.",
    "llm.input_messages",
    "llm.output_messages",
    "input.value",
    "output.value",
)

USER_LIST_WALL_DEADLINE_MS = 3_000
USER_LIST_PRESENCE_TIMEOUT_MS = 1_500
USER_LIST_QUERY_TIMEOUT_MS = 2_200
USER_LIST_ENRICHMENT_TIMEOUT_MS = 900
USER_EXPORT_WALL_DEADLINE_MS = 30_000

_USER_LIST_READ_SETTINGS = {
    "max_threads": 2,
    "max_block_size": 8192,
    "max_rows_to_read": 10_000_000,
    "read_overflow_mode": "throw",
    "max_bytes_to_read": 512 * 1024 * 1024,
    "max_memory_usage": 256 * 1024 * 1024,
    "timeout_overflow_mode": "throw",
}
_USER_LIST_RESULT_BYTES = 32 * 1024 * 1024
_USER_LIST_ATTR_RESULT_ROWS = 50_000

# Hard cap on export rows. Bounds worker memory + latency for the large-workspace
# case this feature targets (matches agentcc's MAX_EXPORT_ROWS); a hit is logged
# and signalled in-band rather than silently truncating the download.
MAX_EXPORT_ROWS = 10_000


def _read_settings(*, max_result_rows: int) -> dict[str, int | str]:
    """Return hard server-side bounds for one user-list ClickHouse read."""

    if max_result_rows <= 0:
        raise ValueError("max_result_rows must be positive")
    return {
        **_USER_LIST_READ_SETTINGS,
        "max_result_rows": int(max_result_rows),
        "max_result_bytes": _USER_LIST_RESULT_BYTES,
        "result_overflow_mode": "throw",
    }


def _log_user_read_failure(event: str, exc: Exception, **context: object) -> None:
    """Log operational reads compactly and programming defects with a stack."""

    if is_read_budget_error(exc) or is_clickhouse_query_error(exc):
        logger.warning(event, error_type=type(exc).__name__, **context)
        return
    logger.exception(event, error_type=type(exc).__name__, **context)


def _users_attr_enrichment_query(project_id=None, project_ids=None):
    """Build the Observe-Users span-attribute enrichment query (DESIGN §3).

    P3b step1.5 DUAL id-remap so a cross-cutover straddler's attributes unify
    under the OLD curated id: resolve each span's ``end_user_id`` new→old via
    ``end_user_id_remap``, then both filter AND re-project on the resolved id so
    the caller buckets new-id spans under the old id. Resolve+filter lives in a
    wrapped ``WHERE`` (not ``PREWHERE``, which can't see the joined column).

    Returns ``(sql, params)``; the caller binds ``%(eu_ids)s``.
    """
    from tracer.services.clickhouse.v2.id_remap_sql import (
        resolved_id_expr,
        survivor_map_subquery,
    )

    params: dict = {}
    project_clause = ""
    if project_id:
        params["attr_pid"] = str(project_id)
        project_clause = "AND spans.project_id = toUUID(%(attr_pid)s)"
    elif project_ids:
        params["attr_pids"] = tuple(str(value) for value in project_ids)
        project_clause = "AND spans.project_id IN %(attr_pids)s"

    eu_map = survivor_map_subquery("end_user_id_remap")
    resolved = resolved_id_expr("latest_end_user_id", "eu_remap")
    sql = f"""
    WITH
    eu_survivor_map AS ({eu_map}),
    candidate_span_identities AS (
        SELECT DISTINCT project_id, trace_id, id, start_time
        FROM spans
        PREWHERE 1 = 1
          {project_clause}
          AND (
              end_user_id IN %(eu_ids)s
              OR end_user_id IN (
                  SELECT any_id
                  FROM eu_survivor_map
                  WHERE survivor_id IN %(eu_ids)s
              )
          )
    ),
    latest_candidate_spans AS (
        SELECT
            project_id,
            trace_id,
            id,
            start_time,
            argMax(tuple(end_user_id), _version).1 AS latest_end_user_id,
            argMax(tuple(attributes_extra), _version).1 AS latest_attributes_extra,
            argMax(attrs_string, _version) AS latest_attrs_string,
            argMax(attrs_number, _version) AS latest_attrs_number,
            argMax(is_deleted, _version) AS latest_is_deleted
        FROM spans
        PREWHERE 1 = 1
          {project_clause}
          AND (project_id, trace_id, id, start_time) IN (
              SELECT project_id, trace_id, id, start_time
              FROM candidate_span_identities
          )
        GROUP BY project_id, trace_id, id, start_time
    )
    SELECT
        {resolved} AS end_user_id,
        latest_attributes_extra AS attributes_extra,
        latest_attrs_string AS attrs_string,
        latest_attrs_number AS attrs_number
    FROM latest_candidate_spans
    LEFT JOIN eu_survivor_map AS eu_remap
        ON latest_end_user_id = eu_remap.any_id
    WHERE latest_is_deleted = 0
      AND {resolved} IN %(eu_ids)s
      AND (
        (latest_attributes_extra != '{{}}' AND latest_attributes_extra != '')
        OR length(mapKeys(latest_attrs_string)) > 0
        OR length(mapKeys(latest_attrs_number)) > 0
      )
    """
    from tracer.services.clickhouse.v2.query_builders.filters import (
        _append_v2_settings,
    )

    return _append_v2_settings(sql), params


class UsersListManager:
    """Owns the Observe Users list + CSV export business logic."""

    def __init__(
        self,
        *,
        organization_id: str,
        allowed_project_ids: list[str],
        project_id: str | None = None,
        search: str | None = None,
        filters: list[dict] | None = None,
        sort_params: list[dict] | None = None,
    ):
        self.organization_id = str(organization_id)
        self.project_id = str(project_id) if project_id else None
        self.search = search
        self.filters = filters or []
        self.sort_params = sort_params or []
        self.scoped_project_ids, self.empty_scope = self._resolve_scope(
            self.project_id, allowed_project_ids
        )

    @staticmethod
    def _resolve_scope(
        project_id: str | None, allowed_project_ids: list[str]
    ) -> tuple[list[str], bool]:
        """Intersect the requested project with the caller's allowed projects.

        An out-of-scope project collapses to ``empty_scope`` — never an org-wide
        scan (CH25: the curated source has no ``workspace_id`` column to filter).
        """
        allowed_strs = {str(p) for p in allowed_project_ids}
        if project_id:
            if project_id in allowed_strs:
                return [project_id], False
            return [], True
        scoped = [str(p) for p in allowed_project_ids]
        return scoped, not scoped

    def _fetch_rows(
        self,
        *,
        limit: int | None,
        offset: int | None,
        deadline: ReadDeadline,
        max_rows: int | None = None,
    ) -> tuple[list[dict], int, UserListQueryBuilderV2]:
        analytics = V2AnalyticsQueryService()
        builder = UserListQueryBuilderV2(
            organization_id=self.organization_id,
            project_ids=self.scoped_project_ids,
            search=self.search,
            limit=limit,
            offset=offset,
            max_rows=max_rows,
            filters=self.filters,
            sort_params=self.sort_params,
            empty_scope=self.empty_scope,
        )
        if self.empty_scope:
            return [], 0, builder
        physical_query, physical_params = builder.build_physical_user_presence_query()
        physical_presence = analytics.execute_ch_query(
            physical_query,
            physical_params,
            timeout_ms=deadline.remaining_ms(USER_LIST_PRESENCE_TIMEOUT_MS),
            settings=_read_settings(max_result_rows=1),
        )
        if not physical_presence.data:
            return [], 0, builder
        query, params = builder.build_candidate_page_query()
        result_row_cap = max_rows or limit or 1
        result = analytics.execute_ch_query(
            query,
            params,
            timeout_ms=deadline.remaining_ms(USER_LIST_QUERY_TIMEOUT_MS),
            settings=_read_settings(max_result_rows=result_row_cap),
        )
        formatted = builder.format_rows(result.data)
        return formatted["table"], formatted["total_count"], builder

    def _read_page_metrics(
        self,
        rows: list[dict],
        builder: UserListQueryBuilderV2,
        deadline: ReadDeadline,
        *,
        timeout_cap_ms: int | None = USER_LIST_ENRICHMENT_TIMEOUT_MS,
    ) -> dict[str, dict]:
        """Return latest-row raw metrics for the already finite user page."""

        end_user_ids = [r.get("end_user_id") for r in rows if r.get("end_user_id")]
        if not end_user_ids:
            return {}
        query, params = builder.build_page_metrics_query(
            [str(value) for value in end_user_ids]
        )
        analytics = V2AnalyticsQueryService()
        result = analytics.execute_ch_query(
            query,
            params,
            timeout_ms=deadline.remaining_ms(timeout_cap_ms),
            settings=_read_settings(max_result_rows=max(1, len(end_user_ids))),
        )
        return {str(row.get("end_user_id", "")): row for row in result.data}

    @staticmethod
    def _apply_page_metrics(rows: list[dict], metrics: dict[str, dict]) -> None:
        fields = (
            "num_sessions",
            "avg_session_duration",
            "avg_trace_latency",
            "num_llm_calls",
            "num_guardrails_triggered",
            "num_active_days",
            "num_traces_with_errors",
        )
        for entry in rows:
            metric_row = metrics.get(str(entry.get("end_user_id", "")), {})
            for field in fields:
                entry[field] = metric_row.get(field, 0) or 0

    def _read_span_attributes(
        self,
        rows: list[dict],
        deadline: ReadDeadline,
    ) -> dict[str, dict[str, object]]:
        """Return page-user attributes under the request-owned wall deadline."""

        end_user_ids = [r.get("end_user_id") for r in rows if r.get("end_user_id")]
        if not end_user_ids:
            return {}
        analytics = V2AnalyticsQueryService()
        attr_query, attr_params = _users_attr_enrichment_query(
            project_id=self.project_id,
            project_ids=self.scoped_project_ids,
        )
        attr_params["eu_ids"] = tuple(str(e) for e in end_user_ids)
        attr_result = analytics.execute_ch_query(
            attr_query,
            attr_params,
            timeout_ms=deadline.remaining_ms(USER_LIST_ENRICHMENT_TIMEOUT_MS),
            settings=_read_settings(max_result_rows=_USER_LIST_ATTR_RESULT_ROWS),
        )
        user_attrs: dict[str, dict[str, object]] = {}
        for attr_row in attr_result.data:
            uid = str(attr_row.get("end_user_id", ""))
            raw = attr_row.get("attributes_extra", "{}")
            try:
                attrs = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except (json.JSONDecodeError, TypeError):
                attrs = {}
            if not isinstance(attrs, dict):
                attrs = {}
            # Fallback: merge from typed Map columns when raw is empty.
            if not attrs:
                str_map = attr_row.get("attrs_string") or {}
                num_map = attr_row.get("attrs_number") or {}
                if isinstance(str_map, dict):
                    attrs.update(str_map)
                if isinstance(num_map, dict):
                    for key, value in num_map.items():
                        if key not in attrs:
                            attrs[key] = value
            if uid not in user_attrs:
                user_attrs[uid] = {}
            for key, value in attrs.items():
                if key.startswith(_SKIP_ATTR_PREFIXES):
                    continue
                if isinstance(value, str) and len(value) > 500:
                    continue
                if key not in user_attrs[uid]:
                    user_attrs[uid][key] = (
                        set() if isinstance(value, (str, int, float, bool)) else []
                    )
                if isinstance(value, (str, int, float, bool)):
                    values = user_attrs[uid][key]
                    assert isinstance(values, set)
                    values.add(
                        value if not isinstance(value, bool) else str(value).lower()
                    )
        return user_attrs

    @staticmethod
    def _apply_span_attributes(
        rows: list[dict],
        user_attrs: dict[str, dict[str, object]],
    ) -> None:
        for entry in rows:
            end_user_id = str(entry.get("end_user_id", ""))
            for key, values in user_attrs.get(end_user_id, {}).items():
                if key in entry:
                    continue
                if isinstance(values, set):
                    sorted_values = sorted(values, key=str)
                    entry[key] = (
                        sorted_values[0] if len(sorted_values) == 1 else sorted_values
                    )
                else:
                    entry[key] = values

    def _read_evals(
        self,
        rows: list[dict],
        builder: UserListQueryBuilderV2,
        deadline: ReadDeadline,
    ) -> dict[str, dict]:
        """Return page-user eval metrics under the shared request deadline."""

        end_user_ids = [r.get("end_user_id") for r in rows if r.get("end_user_id")]
        if not end_user_ids:
            return {}
        eval_query, eval_params = builder.build_eval_query(
            [str(e) for e in end_user_ids]
        )
        if not eval_query:
            return {}
        analytics = V2AnalyticsQueryService()
        eval_result = analytics.execute_ch_query(
            eval_query,
            eval_params,
            timeout_ms=deadline.remaining_ms(USER_LIST_ENRICHMENT_TIMEOUT_MS),
            settings=_read_settings(max_result_rows=max(1, len(end_user_ids))),
        )
        return {str(row.get("end_user_id", "")): row for row in eval_result.data}

    @staticmethod
    def _apply_evals(rows: list[dict], eval_map: dict[str, dict]) -> None:
        for entry in rows:
            end_user_id = str(entry.get("end_user_id", ""))
            eval_row = eval_map.get(end_user_id, {})
            entry["bool_eval_pass_rate"] = eval_row.get("bool_eval_pass_rate", 0)
            entry["avg_output_float"] = eval_row.get("avg_output_float", 0)

    def list_payload(self, *, page_size: int, current_page: int) -> dict:
        """Paginated list response: rows + span/eval enrichment + page totals."""
        deadline = ReadDeadline.start(USER_LIST_WALL_DEADLINE_MS)
        try:
            rows, count, builder = self._fetch_rows(
                limit=page_size,
                offset=current_page * page_size,
                deadline=deadline,
            )
            if rows:
                pool = ThreadPoolExecutor(max_workers=3)
                futures = {
                    pool.submit(
                        self._read_page_metrics, rows, builder, deadline
                    ): "metrics",
                    pool.submit(
                        self._read_span_attributes, rows, deadline
                    ): "attributes",
                    pool.submit(self._read_evals, rows, builder, deadline): "evals",
                }
                completed: dict[str, dict] = {}
                try:
                    for future, phase in futures.items():
                        completed[phase] = future.result(
                            timeout=deadline.remaining_ms() / 1000
                        )
                    deadline.remaining_ms()
                finally:
                    pool.shutdown(wait=False, cancel_futures=True)
                self._apply_page_metrics(rows, completed["metrics"])
                self._apply_span_attributes(rows, completed["attributes"])
                self._apply_evals(rows, completed["evals"])
        except (FuturesTimeoutError, ReadDeadlineExceeded) as exc:
            _log_user_read_failure(
                "users_list_deadline_exceeded",
                exc,
                organization_id=self.organization_id,
                project_id=self.project_id,
            )
            raise
        except Exception as exc:
            _log_user_read_failure(
                "users_list_read_failed",
                exc,
                organization_id=self.organization_id,
                project_id=self.project_id,
            )
            # The HTTP boundary emits the sanitized retryable response.  Never
            # turn an arbitrary programming defect into a successful empty or
            # partially enriched user page.
            raise
        total_pages = (count // page_size) + (1 if count % page_size > 0 else 0)
        return {"table": rows, "total_count": count, "total_pages": total_pages}

    @classmethod
    def _format_export_cell(cls, value: Any):
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str) and value.startswith(_CSV_FORMULA_TRIGGERS):
            return "'" + value
        return value

    def iter_export_csv(self) -> Iterator[str]:
        """Stream the export as CSV text, header row first.

        The header is yielded BEFORE the ClickHouse fetch so the socket stays
        warm while the (slow) query runs — a buffered response would leave it
        idle past the LB read timeout. Rows are hard-capped at
        ``MAX_EXPORT_ROWS``; a cap hit or a mid-stream failure is logged and
        signalled in-band, since headers are already sent and the status can no
        longer change (otherwise a partial body reads as a clean 200).
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        def _drain() -> str:
            chunk = buffer.getvalue()
            buffer.seek(0)
            buffer.truncate()
            return chunk

        writer.writerow([header for header, _ in USERS_EXPORT_COLUMNS])
        yield _drain()

        try:
            # Fetch cap + 1 so a full page can be distinguished from a truncation.
            deadline = ReadDeadline.start(USER_EXPORT_WALL_DEADLINE_MS)
            rows, _, builder = self._fetch_rows(
                limit=None,
                offset=None,
                max_rows=MAX_EXPORT_ROWS + 1,
                deadline=deadline,
            )
            if rows:
                metrics = self._read_page_metrics(
                    rows,
                    builder,
                    deadline,
                    timeout_cap_ms=None,
                )
                self._apply_page_metrics(rows, metrics)
        except Exception as exc:
            _log_user_read_failure(
                "users_export_failed",
                exc,
                organization_id=self.organization_id,
                project_id=self.project_id,
            )
            writer.writerow(
                ["# export failed before completion; data may be incomplete"]
            )
            yield _drain()
            return

        truncated = len(rows) > MAX_EXPORT_ROWS
        if truncated:
            rows = rows[:MAX_EXPORT_ROWS]
            logger.warning(
                "users_export_truncated",
                organization_id=self.organization_id,
                project_id=self.project_id,
                max_rows=MAX_EXPORT_ROWS,
            )

        for row in rows:
            writer.writerow(
                [
                    self._format_export_cell(row.get(field))
                    for _, field in USERS_EXPORT_COLUMNS
                ]
            )
            yield _drain()

        if truncated:
            writer.writerow([f"# export truncated at {MAX_EXPORT_ROWS} rows"])
            yield _drain()
