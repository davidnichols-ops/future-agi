"""
Monitor Metrics Query Builder for ClickHouse.

Replaces the PostgreSQL ORM queries in ``tracer.utils.monitor`` and
``tracer.utils.monitor_graphs`` with ClickHouse-native SQL.

Supports all metric types defined in ``MonitorMetricTypeChoices``:
- COUNT_OF_ERRORS
- ERROR_RATES_FOR_FUNCTION_CALLING
- ERROR_FREE_SESSION_RATES
- SERVICE_PROVIDER_ERROR_RATES
- LLM_API_FAILURE_RATES
- SPAN_RESPONSE_TIME
- LLM_RESPONSE_TIME
- TOKEN_USAGE
- DAILY_TOKENS_SPENT
- MONTHLY_TOKENS_SPENT
- EVALUATION_METRICS

Three query modes:
- ``build_metric_value_query`` -- returns a single scalar value
- ``build_historical_stats_query`` -- returns mean/stddev for a window
- ``build_time_series_query`` -- returns time-bucketed series
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import structlog

from tracer.services.clickhouse.eval_logger_table import eval_logger_source
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder, _parse_dt
from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder
from tracer.models.monitor import MonitorMetricTypeChoices
from tracer.services.clickhouse.v2.id_remap_sql import (
    remap_left_join,
    resolved_id_expr,
)

logger = structlog.get_logger(__name__)

# Mirror of MonitorMetricTypeChoices values
COUNT_OF_ERRORS = "count_of_errors"
ERROR_RATES_FOR_FUNCTION_CALLING = "error_rates_for_function_calling"
ERROR_FREE_SESSION_RATES = "error_free_session_rates"
SERVICE_PROVIDER_ERROR_RATES = "service_provider_error_rates"
LLM_API_FAILURE_RATES = "llm_api_failure_rates"
SPAN_RESPONSE_TIME = "span_response_time"
LLM_RESPONSE_TIME = "llm_response_time"
TOKEN_USAGE = "token_usage"
DAILY_TOKENS_SPENT = "daily_tokens_spent"
MONTHLY_TOKENS_SPENT = "monthly_tokens_spent"
EVALUATION_METRICS = "evaluation_metrics"

SPANS_TABLE = "spans"
SESSION_REMAP_TABLE = "trace_session_id_remap"

# Time-series bucket: floor created_at to the frequency window (parity with the
# pre-migration ORM bucketing; pruning comes from the WHERE, not the bucket).
_TIME_BUCKET_EXPR = (
    "toDateTime(intDiv(toUInt32(created_at), %(freq_seconds)s) * %(freq_seconds)s)"
)



def _pruned_window(start_param: str, end_param: str) -> str:
    """Half-open exact created_at window ``[start, end)`` + padded start_time bounds.

    Half-open (``>= start AND < end``) so a span on the boundary is never claimed
    by two adjacent windows (matters for trailing spend sums; harmless elsewhere).
    Partitioning is by toDate(start_time), so the start_time pads (±1 day
    ingest-lag allowance) drive partition pruning; the created_at minmax skip
    index (v2 schema 024) only helps at the part level.
    """
    return (
        f"created_at >= %({start_param})s AND created_at < %({end_param})s "
        f"AND start_time >= %({start_param})s - INTERVAL 1 DAY "
        f"AND start_time < %({end_param})s + INTERVAL 1 DAY"
    )


class MonitorMetricsQueryBuilder(BaseQueryBuilder):
    """Build ClickHouse queries for monitor metric evaluation and graphing.

    Args:
        project_id: Project UUID string.
        filters: Raw monitor filters dict (the same JSON stored on the
            ``UserAlertMonitor.filters`` field).  These are translated to
            ClickHouse WHERE clauses via :class:`ClickHouseFilterBuilder`.
        eval_config_id: UUID string of the eval config (only needed for
            ``EVALUATION_METRICS``).
        eval_output_type: One of ``"SCORE"``, ``"PASS_FAIL"``, ``"CHOICES"``
            (only needed for ``EVALUATION_METRICS``).
        threshold_metric_value: The threshold metric value from the monitor
            (used for PASS_FAIL and CHOICES eval types).
    """

    def __init__(
        self,
        project_id: str,
        filters: Optional[Dict] = None,
        eval_config_id: Optional[str] = None,
        eval_output_type: Optional[str] = None,
        threshold_metric_value: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(project_id, **kwargs)
        self.raw_filters = filters or {}
        self.eval_config_id = eval_config_id
        self.eval_output_type = eval_output_type
        self.threshold_metric_value = threshold_metric_value

        # Translate monitor filters to CH WHERE fragments
        self._filter_clause = ""
        self._filter_params: Dict[str, Any] = {}
        self._translate_filters()

    @staticmethod
    def _eval_choice_match_expr(param_name: str = "choice_val") -> str:
        """Choice membership in the JSON list (PG parity: list containment only)."""
        return f"has(JSONExtract(output_str_list, 'Array(String)'), %({param_name})s)"

    def _translate_filters(self) -> None:
        """Translate raw monitor filter JSON into CH WHERE clause fragments."""
        ch_conditions: List[str] = []
        params: Dict[str, Any] = {}

        if not self.raw_filters:
            self._filter_clause = ""
            self._filter_params = {}
            return

        # Every build method binds ``%(start_date)s`` (= start_time), so opt in
        # to the shared date-scoping seams. ``span_date_scope`` bounds the
        # ``trace_id IN (SELECT … FROM spans)`` membership subqueries emitted
        # for span-attribute filters — without it they scan the project's
        # ENTIRE span history (24-47s at 241M spans, at/over the 30s monitor
        # timeout; sub-second bounded). ``score_date_scope`` does the same for
        # annotation/score subqueries. Both fragments are opt-in on the shared
        # builder, so dashboard SQL is untouched.
        fb = ClickHouseFilterBuilder(
            table=SPANS_TABLE,
            score_date_scope=True,
            span_date_scope=True,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )

        for key, value in self.raw_filters.items():
            if key == "span_attributes_filters" and isinstance(value, list):
                clause, p = fb.translate(value)
                if clause:
                    ch_conditions.append(clause)
                    params.update(p)
            elif key == "observation_type":
                pname = "mf_obs_type"
                if isinstance(value, list):
                    if value:
                        params[pname] = tuple(value)
                        ch_conditions.append(f"observation_type IN %({pname})s")
                    else:
                        # PG Q(observation_type__in=[]) was always-false.
                        ch_conditions.append("1 = 0")
                elif isinstance(value, str):
                    params[pname] = value
                    ch_conditions.append(f"observation_type = %({pname})s")
            elif key == "project_id":
                # Already handled by project_where()
                pass

        self._filter_clause = " AND ".join(ch_conditions) if ch_conditions else ""
        self._filter_params = params

    def build(self) -> Tuple[str, Dict[str, Any]]:
        """Not used directly -- use build_metric_value_query or build_time_series_query."""
        raise NotImplementedError(
            "Use build_metric_value_query() or build_time_series_query() instead."
        )

    # ------------------------------------------------------------------
    # Metric value query (single scalar)
    # ------------------------------------------------------------------

    def build_metric_value_query(
        self,
        metric_type: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build a query that returns a single metric value for the time window.

        Returns:
            A ``(query_string, params_dict)`` tuple. The query returns a single
            row with a ``value`` column.
        """
        params = dict(self.params)
        params.update(self._filter_params)
        params["start_time"] = _parse_dt(start_time)
        params["end_time"] = _parse_dt(end_time)
        # Bound for the filter builder's date-scoped subqueries (span/score
        # membership) — see _translate_filters.
        params["start_date"] = params["start_time"]

        base_where = self._spans_base_where()
        time_win = f"AND {_pruned_window('start_time', 'end_time')}"

        if metric_type == COUNT_OF_ERRORS:
            query = f"""
                SELECT count() AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND status = 'ERROR'
            """

        elif metric_type == ERROR_RATES_FOR_FUNCTION_CALLING:
            query = f"""
                SELECT
                    CASE WHEN count() = 0 THEN NULL
                         ELSE countIf(status = 'ERROR') / count()
                    END AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'tool'
            """

        elif metric_type == ERROR_FREE_SESSION_RATES:
            # Resolve session ids through the remap before grouping so old/new
            # aliases of one logical session count once (see session_analytics).
            remap_join = remap_left_join("rs.trace_session_id", SESSION_REMAP_TABLE)
            resolved_ts = resolved_id_expr("rs.trace_session_id")
            query = f"""
                SELECT
                    CASE WHEN uniq(trace_session_id) = 0 THEN NULL
                         ELSE uniqIf(trace_session_id, error_count = 0) / uniq(trace_session_id)
                    END AS value
                FROM (
                    SELECT
                        {resolved_ts} AS trace_session_id,
                        countIf(rs.status = 'ERROR') AS error_count
                    FROM (
                        SELECT trace_session_id, status
                        FROM {SPANS_TABLE}
                        {base_where}
                          {time_win}
                          AND trace_session_id IS NOT NULL
                    ) AS rs
                    {remap_join}
                    GROUP BY {resolved_ts}
                )
            """

        elif metric_type == SERVICE_PROVIDER_ERROR_RATES:
            query = f"""
                SELECT
                    CASE WHEN uniq(provider) = 0 THEN NULL
                         ELSE uniqIf(provider, error_count = 0) / uniq(provider)
                    END AS value
                FROM (
                    SELECT
                        provider,
                        countIf(status = 'ERROR') AS error_count
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND provider != ''
                    GROUP BY provider
                )
            """

        elif metric_type == LLM_API_FAILURE_RATES:
            query = f"""
                SELECT
                    CASE WHEN count() = 0 THEN NULL
                         ELSE countIf(status = 'ERROR') / count()
                    END AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'llm'
            """

        elif metric_type == SPAN_RESPONSE_TIME:
            query = f"""
                SELECT ifNotFinite(avg(latency_ms), NULL) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == LLM_RESPONSE_TIME:
            query = f"""
                SELECT ifNotFinite(avg(latency_ms), NULL) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'llm'
            """

        elif metric_type in (TOKEN_USAGE, DAILY_TOKENS_SPENT, MONTHLY_TOKENS_SPENT):
            # No token data must yield NULL (PG Sum parity), not 0 — a 0 would
            # falsely fire LESS_THAN spend monitors. v2 total_tokens is
            # non-Nullable (PG NULL → 0), so "no data" = no nonzero rows.
            # DAILY/MONTHLY differ only by the trailing window (ch_start override
            # in the evaluator), not the SQL.
            query = f"""
                SELECT
                    CASE WHEN countIf(total_tokens != 0) = 0 THEN NULL
                         ELSE sum(total_tokens)
                    END AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == MonitorMetricTypeChoices.EVALUATION_METRICS:
            query, params = self._build_eval_metric_value_query(params)

        else:
            query = "SELECT NULL AS value"

        return query, params

    # Eval SQL reads the eval-logger table AND embeds a spans membership
    # subquery whose monitor-filter fragment carries v1 map tokens (span_attr_*),
    # so it must go THROUGH the v2 rewrite; the not-deleted predicate uses the
    # rewrite-safe (deleted-based) form, so no exclusion is needed. These are
    # reached via the EVALUATION_METRICS branch of the public build_* dispatch.

    def _build_eval_metric_value_query(
        self, params: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build the eval metric value query against the configured eval-logger table."""
        if not self.eval_config_id:
            return "SELECT NULL AS value", params

        params["eval_config_id"] = self.eval_config_id

        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_base_where(eval_nd)

        if self.eval_output_type == "SCORE":
            query = f"""
                SELECT ifNotFinite(avg(output_float), NULL) AS value
                FROM {eval_table} FINAL
                {eval_where}
            """
        elif self.eval_output_type == "PASS_FAIL":
            output_bool_val = 1 if self.threshold_metric_value == "Passed" else 0
            params["output_bool_val"] = output_bool_val
            query = f"""
                SELECT ifNotFinite(avg(
                    CASE WHEN output_bool = %(output_bool_val)s THEN 1.0 ELSE 0.0 END
                ), NULL) AS value
                FROM {eval_table} FINAL
                {eval_where}
            """
        elif self.eval_output_type == "CHOICES":
            if not self.threshold_metric_value:
                return "SELECT NULL AS value", params
            params["choice_val"] = self.threshold_metric_value
            choice_match = self._eval_choice_match_expr()
            query = f"""
                SELECT ifNotFinite(avg(
                    CASE WHEN {choice_match} THEN 1.0 ELSE 0.0 END
                ), NULL) AS value
                FROM {eval_table} FINAL
                {eval_where}
            """
        else:
            query = "SELECT NULL AS value"

        return query, params

    # ------------------------------------------------------------------
    # Historical stats query (mean + stddev)
    # ------------------------------------------------------------------

    def build_historical_stats_query(
        self,
        metric_type: str,
        start_time: datetime,
        end_time: datetime,
        interval_kind: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build a query that returns mean and stddev for historical analysis.

        Per-row stats for rate/latency metrics (population stddev, PG parity);
        calendar-bucketed stats for count/token metrics (``interval_kind`` =
        minute/hour/day/month, sample stddev — parity with the old
        ``Trunc`` + ``statistics.stdev`` path).

        Returns:
            A ``(query_string, params_dict)`` tuple with ``mean`` and ``stddev`` columns.
        """
        params = dict(self.params)
        params.update(self._filter_params)
        params["start_time"] = _parse_dt(start_time)
        params["end_time"] = _parse_dt(end_time)
        # Bound for the filter builder's date-scoped subqueries (span/score
        # membership) — see _translate_filters.
        params["start_date"] = params["start_time"]

        base_where = self._spans_base_where()
        time_win = f"AND {_pruned_window('start_time', 'end_time')}"

        if metric_type == ERROR_RATES_FOR_FUNCTION_CALLING:
            query = f"""
                SELECT
                    ifNotFinite(avg(is_error), NULL) AS mean,
                    ifNotFinite(stddevPop(is_error), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN status = 'ERROR' THEN 1.0 ELSE 0.0 END AS is_error
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND observation_type = 'tool'
                )
            """

        elif metric_type == ERROR_FREE_SESSION_RATES:
            remap_join = remap_left_join("rs.trace_session_id", SESSION_REMAP_TABLE)
            resolved_ts = resolved_id_expr("rs.trace_session_id")
            query = f"""
                SELECT
                    ifNotFinite(avg(is_error_free), NULL) AS mean,
                    ifNotFinite(stddevPop(is_error_free), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN countIf(rs.status = 'ERROR') > 0 THEN 0.0 ELSE 1.0 END AS is_error_free
                    FROM (
                        SELECT trace_session_id, status
                        FROM {SPANS_TABLE}
                        {base_where}
                          {time_win}
                          AND trace_session_id IS NOT NULL
                    ) AS rs
                    {remap_join}
                    GROUP BY {resolved_ts}
                )
            """

        elif metric_type == SERVICE_PROVIDER_ERROR_RATES:
            query = f"""
                SELECT
                    ifNotFinite(avg(is_error_free), NULL) AS mean,
                    ifNotFinite(stddevPop(is_error_free), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN countIf(status = 'ERROR') > 0 THEN 0.0 ELSE 1.0 END AS is_error_free
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND provider != ''
                    GROUP BY provider
                )
            """

        elif metric_type == LLM_API_FAILURE_RATES:
            query = f"""
                SELECT
                    ifNotFinite(avg(is_error), NULL) AS mean,
                    ifNotFinite(stddevPop(is_error), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN status = 'ERROR' THEN 1.0 ELSE 0.0 END AS is_error
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND observation_type = 'llm'
                )
            """

        elif metric_type == SPAN_RESPONSE_TIME:
            query = f"""
                SELECT
                    ifNotFinite(avg(latency_ms), NULL) AS mean,
                    ifNotFinite(stddevPop(latency_ms), NULL) AS stddev
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == LLM_RESPONSE_TIME:
            query = f"""
                SELECT
                    ifNotFinite(avg(latency_ms), NULL) AS mean,
                    ifNotFinite(stddevPop(latency_ms), NULL) AS stddev
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'llm'
            """

        elif metric_type == MonitorMetricTypeChoices.EVALUATION_METRICS:
            query, params = self._build_eval_stats_query(params)

        elif metric_type in (
            COUNT_OF_ERRORS,
            TOKEN_USAGE,
            DAILY_TOKENS_SPENT,
            MONTHLY_TOKENS_SPENT,
        ):
            # Stats over calendar-aligned buckets. Empty result collapses to
            # (0, 0), a single bucket to (value, 0), and no-token buckets are
            # skipped via nullIf (v2 total_tokens is non-Nullable) — matching
            # the old Python path.
            bucket_fn = self.time_bucket_expr(interval_kind or "hour")
            agg = (
                "countIf(status = 'ERROR')"
                if metric_type == COUNT_OF_ERRORS
                else "nullIf(sum(total_tokens), 0)"
            )
            query = f"""
                SELECT
                    coalesce(ifNotFinite(avg(bucket_value), 0), 0) AS mean,
                    coalesce(ifNotFinite(stddevSamp(bucket_value), 0), 0) AS stddev
                FROM (
                    SELECT
                        {bucket_fn}(created_at) AS bucket_ts,
                        {agg} AS bucket_value
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                    GROUP BY bucket_ts
                )
            """

        else:
            query = "SELECT NULL AS mean, NULL AS stddev"

        return query, params

    def _build_eval_stats_query(
        self, params: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build eval metric stats (mean/stddev) query."""
        if not self.eval_config_id:
            return "SELECT NULL AS mean, NULL AS stddev", params

        params["eval_config_id"] = self.eval_config_id
        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_base_where(eval_nd)

        if self.eval_output_type == "SCORE":
            query = f"""
                SELECT
                    ifNotFinite(avg(output_float), NULL) AS mean,
                    ifNotFinite(stddevPop(output_float), NULL) AS stddev
                FROM {eval_table} FINAL
                {eval_where}
            """
        elif self.eval_output_type == "PASS_FAIL":
            output_bool_val = 1 if self.threshold_metric_value == "Passed" else 0
            params["output_bool_val"] = output_bool_val
            query = f"""
                SELECT
                    ifNotFinite(avg(pass_value), NULL) AS mean,
                    ifNotFinite(stddevPop(pass_value), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN output_bool = %(output_bool_val)s THEN 1.0 ELSE 0.0 END AS pass_value
                    FROM {eval_table} FINAL
                    {eval_where}
                )
            """
        elif self.eval_output_type == "CHOICES":
            if not self.threshold_metric_value:
                return "SELECT NULL AS mean, NULL AS stddev", params
            params["choice_val"] = self.threshold_metric_value
            choice_match = self._eval_choice_match_expr()
            query = f"""
                SELECT
                    ifNotFinite(avg(choice_value), NULL) AS mean,
                    ifNotFinite(stddevPop(choice_value), NULL) AS stddev
                FROM (
                    SELECT
                        CASE WHEN {choice_match} THEN 1.0 ELSE 0.0 END AS choice_value
                    FROM {eval_table} FINAL
                    {eval_where}
                )
            """
        else:
            query = "SELECT NULL AS mean, NULL AS stddev"

        return query, params

    # ------------------------------------------------------------------
    # Time series query (bucketed)
    # ------------------------------------------------------------------

    def build_time_series_query(
        self,
        metric_type: str,
        start_time: datetime,
        end_time: datetime,
        frequency_seconds: int,
    ) -> Tuple[str, Dict[str, Any]]:
        """Build a time-bucketed query for graph data.

        Returns:
            A ``(query_string, params_dict)`` tuple. The query returns rows with
            ``timestamp`` and ``value`` columns, ordered by timestamp.
        """
        params = dict(self.params)
        params.update(self._filter_params)
        params["start_time"] = _parse_dt(start_time)
        params["end_time"] = _parse_dt(end_time)
        # Bound for the filter builder's date-scoped subqueries (span/score
        # membership) — see _translate_filters.
        params["start_date"] = params["start_time"]
        params["freq_seconds"] = frequency_seconds

        bucket_expr = _TIME_BUCKET_EXPR

        base_where = self._spans_base_where()
        time_filter = f"AND {_pruned_window('start_time', 'end_time')}"

        if metric_type in (TOKEN_USAGE, DAILY_TOKENS_SPENT, MONTHLY_TOKENS_SPENT):
            query = f"""
                SELECT
                    {bucket_expr} AS timestamp,
                    sum(total_tokens) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_filter}
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == COUNT_OF_ERRORS:
            query = f"""
                SELECT
                    {bucket_expr} AS timestamp,
                    countIf(status = 'ERROR') AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_filter}
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == SPAN_RESPONSE_TIME:
            query = f"""
                SELECT
                    {bucket_expr} AS timestamp,
                    avg(latency_ms) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_filter}
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == LLM_RESPONSE_TIME:
            query = f"""
                SELECT
                    {bucket_expr} AS timestamp,
                    avg(latency_ms) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_filter}
                  AND observation_type = 'llm'
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type in (ERROR_RATES_FOR_FUNCTION_CALLING, LLM_API_FAILURE_RATES):
            obs_type = (
                "tool" if metric_type == ERROR_RATES_FOR_FUNCTION_CALLING else "llm"
            )
            params["obs_type_ts"] = obs_type
            query = f"""
                SELECT
                    {bucket_expr} AS timestamp,
                    CASE WHEN count() = 0 THEN 0
                         ELSE countIf(status = 'ERROR') / count()
                    END AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_filter}
                  AND observation_type = %(obs_type_ts)s
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == ERROR_FREE_SESSION_RATES:
            remap_join = remap_left_join("rs.trace_session_id", SESSION_REMAP_TABLE)
            resolved_ts = resolved_id_expr("rs.trace_session_id")
            query = f"""
                SELECT
                    timestamp,
                    CASE WHEN uniq(trace_session_id) = 0 THEN 0
                         ELSE uniqIf(trace_session_id, error_count = 0) / uniq(trace_session_id)
                    END AS value
                FROM (
                    SELECT
                        {bucket_expr} AS timestamp,
                        {resolved_ts} AS trace_session_id,
                        countIf(rs.status = 'ERROR') AS error_count
                    FROM (
                        SELECT trace_session_id, status, created_at
                        FROM {SPANS_TABLE}
                        {base_where}
                          {time_filter}
                          AND trace_session_id IS NOT NULL
                    ) AS rs
                    {remap_join}
                    GROUP BY timestamp, {resolved_ts}
                )
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == SERVICE_PROVIDER_ERROR_RATES:
            query = f"""
                SELECT
                    timestamp,
                    CASE WHEN uniq(provider) = 0 THEN 0
                         ELSE uniqIf(provider, error_count = 0) / uniq(provider)
                    END AS value
                FROM (
                    SELECT
                        {bucket_expr} AS timestamp,
                        provider,
                        countIf(status = 'ERROR') AS error_count
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_filter}
                      AND provider != ''
                    GROUP BY timestamp, provider
                )
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == MonitorMetricTypeChoices.EVALUATION_METRICS:
            query, params = self._build_eval_time_series_query(params)

        else:
            query = "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0"

        return query, params

    def _build_eval_time_series_query(
        self,
        params: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        """Build eval metric time-series query.

        Buckets on the eval row's ``created_at`` (the table has no span-time
        column). Window membership comes from the span subquery, so a
        late-computed eval for an in-window span lands in the bucket of its
        computation time.
        # TODO: bucket by span time (needs a spans join) for graph fidelity.
        """
        if not self.eval_config_id:
            return "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0", params

        params["eval_config_id"] = self.eval_config_id
        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_base_where(eval_nd)
        bucket_expr = _TIME_BUCKET_EXPR

        if self.eval_output_type == "SCORE":
            agg = "avg(output_float)"
        elif self.eval_output_type == "PASS_FAIL":
            output_bool_val = 1 if self.threshold_metric_value == "Passed" else 0
            params["output_bool_val"] = output_bool_val
            agg = (
                "avg(CASE WHEN output_bool = %(output_bool_val)s THEN 1.0 ELSE 0.0 END)"
            )
        elif self.eval_output_type == "CHOICES":
            if not self.threshold_metric_value:
                return "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0", params
            params["choice_val"] = self.threshold_metric_value
            choice_match = self._eval_choice_match_expr()
            agg = f"avg(CASE WHEN {choice_match} THEN 1.0 ELSE 0.0 END)"
        else:
            return "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0", params

        query = f"""
            SELECT
                {bucket_expr} AS timestamp,
                ifNotFinite({agg}, NULL) AS value
            FROM {eval_table} FINAL
            {eval_where}
            GROUP BY timestamp
            ORDER BY timestamp
        """

        return query, params

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _spans_base_where(self) -> str:
        """Return the base WHERE clause for spans table queries."""
        clause = self.project_where()
        if self._filter_clause:
            clause += f" AND {self._filter_clause}"
        return clause

    def _eval_base_where(self, not_deleted: str) -> str:
        """Eval-config scope + span-time window via bounded span membership.

        ``not_deleted`` comes from the same ``eval_logger_source()`` call that
        resolved the table, so the pair stays consistent.

        The metric window is applied to the SPAN's ``created_at`` (when the
        activity happened), NOT the eval row's ``created_at`` (when the score
        was computed) — evals run asynchronously after their spans, so a
        window on eval time measures the wrong thing (proven at prod scale:
        8,577 vs 400 evals for the same busy hour). Windowing the span
        subquery also keeps the IN-set a window's worth of ids instead of the
        whole project (an unbounded set hit 105M ids / 1.69 GB →
        SET_SIZE_LIMIT_EXCEEDED at 241M spans).

        The eval-side ``created_at`` keeps only a loose lower bound: an eval
        row is written after its span, so eval time >= span time >= window
        start (1-day pad for skew). It is correctness-neutral (the span
        subquery alone determines the result) but it is the eval table's ONLY
        prune — the table is PARTITION BY toYYYYMM(created_at) with sort key
        (trace_id, config_id, id), so without it every tick full-scans all
        partitions + FINAL-merges them (51K vs 1.55M rows read at prod today;
        the gap grows with table age). Drop it only if evals are ever
        backfilled with created_at stamps predating their spans by >1 day.
        """
        filter_extra = ""
        if self._filter_clause:
            filter_extra = f" AND {self._filter_clause}"

        return (
            f"WHERE custom_eval_config_id = toUUID(%(eval_config_id)s) "
            f"AND {not_deleted} "
            f"AND created_at >= %(start_time)s - INTERVAL 1 DAY "
            f"AND observation_span_id IN ("
            f"  SELECT id FROM {SPANS_TABLE} "
            f"  WHERE {self.project_filter_sql()} "
            f"  AND is_deleted = 0 "
            f"  AND created_at >= %(start_time)s AND created_at < %(end_time)s "
            f"  AND start_time >= %(start_time)s - INTERVAL 1 DAY "
            f"  AND start_time < %(end_time)s + INTERVAL 1 DAY"
            f"  {filter_extra}"
            f")"
        )
