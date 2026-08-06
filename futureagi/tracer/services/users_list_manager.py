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
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import structlog

from tracer.services.clickhouse.list_cursor import ListCursor
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

USER_LIST_WALL_DEADLINE_MS = 12_000
USER_LIST_PRESENCE_TIMEOUT_MS = 1_500
USER_LIST_QUERY_TIMEOUT_MS = 8_000
USER_LIST_ENRICHMENT_TIMEOUT_MS = 5_000
USER_EXPORT_WALL_DEADLINE_MS = 30_000
USER_LIST_CANDIDATE_BATCH_SIZE = 100
USER_LIST_MAX_CANDIDATE_BATCHES = 8

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


@dataclass(frozen=True)
class UserCursorRead:
    """One exact bounded Users page plus opaque transport state."""

    payload: dict[str, Any]
    window_start: datetime
    window_end: datetime
    checkpoint_order: tuple[Any, ...] | None
    seen_rows: int
    has_more: bool
    unseen_row_proven: bool


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


def _page_read_settings(*, max_result_rows: int) -> dict[str, Any]:
    """Return finite settings for one current-latest user-list statement."""

    return _read_settings(max_result_rows=max_result_rows)


def _log_user_read_failure(event: str, exc: Exception, **context: object) -> None:
    """Log operational reads compactly and programming defects with a stack."""

    if is_read_budget_error(exc) or is_clickhouse_query_error(exc):
        logger.warning(event, error_type=type(exc).__name__, **context)
        return
    logger.exception(event, error_type=type(exc).__name__, **context)


def _users_attr_enrichment_query(
    project_id=None,
    project_ids=None,
    *,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
):
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
    if (start_date is None) != (end_date is None):
        raise ValueError("attribute enrichment window must be provided together")
    time_filter = ""
    if start_date is not None:
        params["attr_start_date"] = start_date
        params["attr_end_date"] = end_date
        time_filter = """
          AND start_time >= %(attr_start_date)s
          AND start_time < %(attr_end_date)s
        """
    sql = f"""
    WITH
    eu_survivor_map AS ({eu_map}),
    candidate_span_identities AS (
        SELECT DISTINCT project_id, trace_id, id, start_time
        FROM spans
        PREWHERE 1 = 1
          {project_clause}
          {time_filter}
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
            argMax(attrs_bool, _version) AS latest_attrs_bool,
            argMax(is_deleted, _version) AS latest_is_deleted
        FROM spans
        PREWHERE 1 = 1
          {project_clause}
          {time_filter}
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
        latest_attrs_number AS attrs_number,
        latest_attrs_bool AS attrs_bool
    FROM latest_candidate_spans
    LEFT JOIN eu_survivor_map AS eu_remap
        ON latest_end_user_id = eu_remap.any_id
    WHERE latest_is_deleted = 0
      AND {resolved} IN %(eu_ids)s
      AND (
        (latest_attributes_extra != '{{}}' AND latest_attributes_extra != '')
        OR length(mapKeys(latest_attrs_string)) > 0
        OR length(mapKeys(latest_attrs_number)) > 0
        OR length(mapKeys(latest_attrs_bool)) > 0
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
            settings=_page_read_settings(max_result_rows=max(1, len(end_user_ids))),
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
        *,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, dict[str, object]]:
        """Return page-user attributes under the request-owned wall deadline."""

        end_user_ids = [r.get("end_user_id") for r in rows if r.get("end_user_id")]
        if not end_user_ids:
            return {}
        analytics = V2AnalyticsQueryService()
        attr_query, attr_params = _users_attr_enrichment_query(
            project_id=self.project_id,
            project_ids=self.scoped_project_ids,
            start_date=start_date,
            end_date=end_date,
        )
        attr_params["eu_ids"] = tuple(str(e) for e in end_user_ids)
        attr_result = analytics.execute_ch_query(
            attr_query,
            attr_params,
            timeout_ms=deadline.remaining_ms(USER_LIST_ENRICHMENT_TIMEOUT_MS),
            settings=_page_read_settings(max_result_rows=_USER_LIST_ATTR_RESULT_ROWS),
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
            # Direct writes split scalar attributes across typed Maps while
            # structured values remain in attributes_extra. Merge every source
            # on every row; structured/raw values win if a malformed producer
            # writes the same key to more than one physical column.
            for typed_map_name in ("attrs_string", "attrs_number", "attrs_bool"):
                typed_map = attr_row.get(typed_map_name) or {}
                if not isinstance(typed_map, dict):
                    continue
                for key, value in typed_map.items():
                    attrs.setdefault(key, value)
            if uid not in user_attrs:
                user_attrs[uid] = {}
            for key, value in attrs.items():
                if key.startswith(_SKIP_ATTR_PREFIXES):
                    continue
                if isinstance(value, str) and len(value) > 500:
                    continue
                if key not in user_attrs[uid]:
                    user_attrs[uid][key] = (
                        set()
                        if value is None or isinstance(value, (str, int, float, bool))
                        else []
                    )
                if value is None or isinstance(value, (str, int, float, bool)):
                    values = user_attrs[uid][key]
                    normalized = (
                        value if not isinstance(value, bool) else str(value).lower()
                    )
                    if isinstance(values, set):
                        values.add(normalized)
                    elif normalized not in values:
                        # A custom attribute can legitimately change type across
                        # spans. Keep both exact representations rather than
                        # crashing because an earlier row happened to be JSON.
                        values.append(normalized)
                elif isinstance(value, (dict, list)):
                    values = user_attrs[uid][key]
                    if isinstance(values, set):
                        # Promote a scalar bucket when this key later carries a
                        # structured value. The output becomes a deterministic
                        # multi-value attribute and no exact value is discarded.
                        values = sorted(values, key=str)
                        user_attrs[uid][key] = values
                    canonical = json.dumps(
                        value,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    )
                    if canonical not in values:
                        values.append(canonical)
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
                    entry[key] = sorted(
                        values,
                        key=lambda value: (
                            type(value).__name__,
                            UsersListManager._canonical_filter_value(value),
                        ),
                    )

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
            settings=_page_read_settings(max_result_rows=max(1, len(end_user_ids))),
        )
        return {str(row.get("end_user_id", "")): row for row in eval_result.data}

    @staticmethod
    def _apply_evals(rows: list[dict], eval_map: dict[str, dict]) -> None:
        for entry in rows:
            end_user_id = str(entry.get("end_user_id", ""))
            eval_row = eval_map.get(end_user_id, {})
            entry["bool_eval_pass_rate"] = eval_row.get("bool_eval_pass_rate", 0)
            entry["avg_output_float"] = eval_row.get("avg_output_float", 0)

    @staticmethod
    def _frozen_filters(
        filters: list[dict],
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict]:
        return [
            *[
                item
                for item in filters
                if not UserListQueryBuilderV2._is_date_filter(item)
            ],
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [window_start, window_end],
                },
            },
        ]

    def _read_dimension_candidates(
        self,
        *,
        deadline: ReadDeadline,
        limit: int,
        before_first_seen: datetime | None,
        before_end_user_id: str | None,
    ) -> list[dict]:
        builder = UserListQueryBuilderV2(
            organization_id=self.organization_id,
            project_ids=self.scoped_project_ids,
            search=self.search,
            empty_scope=self.empty_scope,
        )
        query, params = builder.build_dimension_candidate_query(
            limit=limit,
            before_first_seen=before_first_seen,
            before_end_user_id=before_end_user_id,
        )
        result = V2AnalyticsQueryService().execute_ch_query(
            query,
            params,
            timeout_ms=deadline.remaining_ms(USER_LIST_QUERY_TIMEOUT_MS),
            settings=_page_read_settings(max_result_rows=limit),
        )
        return list(result.data or [])

    def _read_exact_candidate_rows(
        self,
        *,
        candidate_ids: list[str],
        frozen_filters: list[dict],
        window_start: datetime,
        window_end: datetime,
        deadline: ReadDeadline,
    ) -> list[dict]:
        if not candidate_ids:
            return []
        date_filters = [
            item
            for item in frozen_filters
            if UserListQueryBuilderV2._is_date_filter(item)
        ]
        builder = UserListQueryBuilderV2(
            organization_id=self.organization_id,
            project_ids=self.scoped_project_ids,
            filters=date_filters,
            limit=len(candidate_ids),
            offset=0,
            candidate_end_user_ids=candidate_ids,
            empty_scope=self.empty_scope,
        )
        query, params = builder.build_candidate_page_query()
        result = V2AnalyticsQueryService().execute_ch_query(
            query,
            params,
            timeout_ms=deadline.remaining_ms(USER_LIST_QUERY_TIMEOUT_MS),
            settings=_page_read_settings(max_result_rows=max(1, len(candidate_ids))),
        )
        rows = builder.format_rows(result.data)["table"]
        if not rows:
            return []

        pool = ThreadPoolExecutor(max_workers=3)
        futures = {
            pool.submit(
                self._read_page_metrics,
                rows,
                builder,
                deadline,
            ): "metrics",
            pool.submit(
                self._read_span_attributes,
                rows,
                deadline,
                start_date=window_start,
                end_date=window_end,
            ): "attributes",
            pool.submit(
                self._read_evals,
                rows,
                builder,
                deadline,
            ): "evals",
        }
        completed: dict[str, dict] = {}
        try:
            for future, phase in futures.items():
                completed[phase] = future.result(timeout=deadline.remaining_ms() / 1000)
            deadline.remaining_ms()
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        self._apply_page_metrics(rows, completed["metrics"])
        self._apply_span_attributes(rows, completed["attributes"])
        self._apply_evals(rows, completed["evals"])
        return rows

    @staticmethod
    def _candidate_value_matches(candidate: Any, op: str | None, expected: Any) -> bool:
        if isinstance(candidate, (list, tuple, set)):
            values = list(candidate)
            if op in {"not_equals", "not_in", "not_contains"}:
                positive_op = {
                    "not_equals": "equals",
                    "not_in": "in",
                    "not_contains": "contains",
                }[str(op)]
                return all(
                    not UsersListManager._candidate_value_matches(
                        value, positive_op, expected
                    )
                    for value in values
                )
            return any(
                UsersListManager._candidate_value_matches(value, op, expected)
                for value in values
            )
        if op == "is_null":
            return candidate is None
        if op == "is_not_null":
            return candidate is not None
        if op in {"in", "not_in"}:
            expected_values = expected if isinstance(expected, list) else [expected]
            left = UsersListManager._canonical_filter_value(candidate)
            matched = any(
                left == UsersListManager._canonical_filter_value(value)
                for value in expected_values
            )
            return not matched if op == "not_in" else matched
        if op in {"equals", "not_equals"}:
            left = UsersListManager._canonical_filter_value(candidate)
            right = UsersListManager._canonical_filter_value(expected)
            matched = left == right
            return not matched if op == "not_equals" else matched
        if op in {"contains", "not_contains", "starts_with", "ends_with"}:
            left = UsersListManager._canonical_filter_value(candidate or "").lower()
            right = UsersListManager._canonical_filter_value(expected or "").lower()
            if op == "starts_with":
                return left.startswith(right)
            if op == "ends_with":
                return left.endswith(right)
            matched = right in left
            return not matched if op == "not_contains" else matched
        if op in {
            "greater_than",
            "greater_than_or_equal",
            "less_than",
            "less_than_or_equal",
        }:
            try:
                left = float(candidate)
                right = float(expected)
            except (TypeError, ValueError):
                return False
            if op == "greater_than":
                return left > right
            if op == "greater_than_or_equal":
                return left >= right
            if op == "less_than":
                return left < right
            return left <= right
        if op in {"between", "not_between"}:
            if not isinstance(expected, (list, tuple)) or len(expected) != 2:
                return False
            try:
                matched = expected[0] <= candidate < expected[1]
            except TypeError:
                left = str(candidate)
                matched = str(expected[0]) <= left < str(expected[1])
            return not matched if op == "not_between" else matched
        return True

    @staticmethod
    def _canonical_filter_value(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (int, float, Decimal)):
            numeric = Decimal(str(value))
            if numeric.is_finite():
                if numeric == 0:
                    return "0"
                return format(numeric.normalize(), "f")
            return str(value).lower()
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.lower() in {"true", "false"}:
                return stripped.lower()
            if stripped.startswith(("{", "[")):
                try:
                    structured = json.loads(stripped)
                except (json.JSONDecodeError, TypeError):
                    pass
                else:
                    if isinstance(structured, (dict, list)):
                        return json.dumps(
                            structured,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )
        return str(value)

    def _row_matches_filters(self, row: dict[str, Any]) -> bool:
        for item in self.filters:
            if UserListQueryBuilderV2._is_date_filter(item):
                continue
            config = item.get("filter_config") or {}
            column_id = item.get("column_id") or item.get("columnId")
            if not column_id:
                continue
            if column_id == "eval_score":
                key = "bool_eval_pass_rate"
            else:
                key = UserListQueryBuilderV2.OUTPUT_FILTER_MAP.get(column_id, column_id)
            if not self._candidate_value_matches(
                row.get(key),
                config.get("filter_op") or config.get("filterOp"),
                config.get("filter_value", config.get("filterValue")),
            ):
                return False
        return True

    def list_cursor_payload(
        self,
        *,
        page_size: int,
        cursor: ListCursor | None = None,
    ) -> UserCursorRead:
        """Return exact rows from a bounded, signed dimension continuation.

        The list is intentionally candidate ordered.  It never samples or
        publishes a partially hydrated user; an unfinished dimension scan is
        represented only by ``has_more`` plus the next opaque cursor.
        """

        deadline = ReadDeadline.start(USER_LIST_WALL_DEADLINE_MS)
        base_builder = UserListQueryBuilderV2(
            organization_id=self.organization_id,
            project_ids=self.scoped_project_ids,
            filters=self.filters,
            empty_scope=self.empty_scope,
        )
        if cursor is None:
            window_start, window_end = base_builder.parse_time_range(self.filters)
            frozen_filters = self._frozen_filters(
                self.filters,
                window_start=window_start,
                window_end=window_end,
            )
            seen_before = 0
            before_first_seen = None
            before_end_user_id = None
        else:
            window_start, window_end = cursor.window_start, cursor.window_end
            frozen_filters = self._frozen_filters(
                self.filters,
                window_start=window_start,
                window_end=window_end,
            )
            seen_before = cursor.seen_rows
            if len(cursor.order) != 2:
                raise ValueError("user list cursor order is invalid")
            before_first_seen = cursor.order[0]
            before_end_user_id = str(cursor.order[1])

        published: list[dict] = []
        checkpoint: tuple[Any, ...] | None = None
        has_more = False
        unseen_row_proven = False

        for _ in range(USER_LIST_MAX_CANDIDATE_BATCHES):
            try:
                candidate_rows = self._read_dimension_candidates(
                    deadline=deadline,
                    limit=USER_LIST_CANDIDATE_BATCH_SIZE + 1,
                    before_first_seen=before_first_seen,
                    before_end_user_id=before_end_user_id,
                )
                if not candidate_rows:
                    has_more = False
                    break

                batch = candidate_rows[:USER_LIST_CANDIDATE_BATCH_SIZE]
                dimension_has_more = len(candidate_rows) > len(batch)
                candidate_ids = [str(row["end_user_id"]) for row in batch]
                exact_rows = self._read_exact_candidate_rows(
                    candidate_ids=candidate_ids,
                    frozen_filters=frozen_filters,
                    window_start=window_start,
                    window_end=window_end,
                    deadline=deadline,
                )
                exact_by_id = {
                    str(row.get("end_user_id")): row
                    for row in exact_rows
                    if row.get("end_user_id")
                }
                consumed = 0
                for candidate in batch:
                    consumed += 1
                    row = exact_by_id.get(str(candidate.get("end_user_id")))
                    if row is None or not self._row_matches_filters(row):
                        continue
                    published.append(row)
                    if len(published) == page_size:
                        unseen_row_proven = any(
                            (
                                exact_by_id.get(str(later.get("end_user_id")))
                                is not None
                                and self._row_matches_filters(
                                    exact_by_id[str(later.get("end_user_id"))]
                                )
                            )
                            for later in batch[consumed:]
                        )
                        break

                consumed_row = batch[consumed - 1]
                checkpoint = (
                    consumed_row["first_seen"],
                    str(consumed_row["end_user_id"]),
                )
                before_first_seen = checkpoint[0]
                before_end_user_id = checkpoint[1]
                unconsumed_candidates = consumed < len(batch)
                has_more = bool(
                    unconsumed_candidates
                    or dimension_has_more
                    or len(batch) == USER_LIST_CANDIDATE_BATCH_SIZE
                )
                if len(published) == page_size:
                    break
                if (
                    not dimension_has_more
                    and len(batch) < USER_LIST_CANDIDATE_BATCH_SIZE
                ):
                    has_more = False
                    break
            except (FuturesTimeoutError, ReadDeadlineExceeded):
                if checkpoint is None:
                    raise
                has_more = True
                break
            except Exception as exc:
                if checkpoint is None or not is_read_budget_error(exc):
                    raise
                has_more = True
                break
        else:
            has_more = checkpoint is not None

        seen_rows = seen_before + len(published)
        lower_bound = seen_rows + (1 if has_more and unseen_row_proven else 0)
        total_pages = (lower_bound + page_size - 1) // page_size
        payload = {
            "table": published,
            "total_count": lower_bound,
            "total_pages": total_pages,
            "count_is_lower_bound": has_more,
            "has_more": has_more,
            # Every published row completed exact latest-state hydration and
            # every requested predicate. ``has_more`` describes only the
            # dimension traversal; it must not relabel an exact list page as an
            # incomplete/sampled result in shared UI state handling.
            "query_complete": True,
            "query_status": "complete",
        }
        return UserCursorRead(
            payload=payload,
            window_start=window_start,
            window_end=window_end,
            checkpoint_order=checkpoint,
            seen_rows=seen_rows,
            has_more=has_more,
            unseen_row_proven=unseen_row_proven,
        )

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
