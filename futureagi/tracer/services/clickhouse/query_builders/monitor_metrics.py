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

Two main query modes:
- ``build_metric_value_query`` -- returns a single scalar value
- ``build_time_series_query`` -- returns time-bucketed series
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import structlog

from tracer.models.observation_span import EvalEntryStatus
from tracer.services.clickhouse.eval_logger_table import eval_logger_source
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder, _parse_dt
from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder

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

# Time-series buckets: to epoch seconds, integer-divide by the frequency to
# floor to the bucket boundary, back to DateTime (300s: 12:07:43 -> 12:05:00).
# Works for any frequency, unlike toStartOfHour/-FiveMinutes. Both bucket on
# span ``start_time`` (event time — the user's timeline); the eval table has
# no span-time column, so its series joins spans (alias ``sp``).
_SPANS_BUCKET_EXPR = (
    "toDateTime(intDiv(toUInt32(start_time), %(freq_seconds)s) * %(freq_seconds)s)"
)
_EVAL_SPAN_BUCKET_EXPR = (
    "toDateTime(intDiv(toUInt32(sp.start_time), %(freq_seconds)s) * %(freq_seconds)s)"
)

# Statuses whose rows carry no real value (NULL outputs). NOT IN keeps legacy
# empty/NULL-status rows counted as completed (mirrors span_list.py).
_EVAL_NON_VALUE_STATUSES = ", ".join(
    f"'{s.value}'" for s in EvalEntryStatus if s is not EvalEntryStatus.COMPLETED
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
        """Choice membership in the JSON list (choice evals only write output_str_list)."""
        return f"has(JSONExtract(output_str_list, 'Array(String)'), %({param_name})s)"

    def _translate_filters(self) -> None:
        """Translate raw monitor filter JSON into CH WHERE clause fragments."""
        ch_conditions: List[str] = []
        params: Dict[str, Any] = {}

        if not self.raw_filters:
            self._filter_clause = ""
            self._filter_params = {}
            return

        # Monitor queries bind start_time/end_time per query method, not
        # start_date/end_date. Disable score date pruning here so annotation
        # filters do not emit an unbound ``%(start_date)s`` placeholder.
        fb = ClickHouseFilterBuilder(table=SPANS_TABLE, score_date_scope=False)

        for key, value in self.raw_filters.items():
            if key == "span_attributes_filters" and isinstance(value, list):
                clause, p = fb.translate(value)
                if clause:
                    ch_conditions.append(clause)
                    params.update(p)
            elif key == "observation_type":
                pname = f"mf_obs_type"
                if isinstance(value, list):
                    params[pname] = tuple(value)
                    ch_conditions.append(f"observation_type IN %({pname})s")
                elif isinstance(value, str):
                    params[pname] = value
                    ch_conditions.append(f"observation_type = %({pname})s")
            elif key == "session_id" and value:
                pname = "mf_session_id"
                params[pname] = str(value)
                ch_conditions.append(f"trace_session_id = toUUID(%({pname})s)")
            elif key == "date_range" and isinstance(value, list) and len(value) == 2:
                params["mf_dr_start"] = _parse_dt(value[0])
                params["mf_dr_end"] = _parse_dt(value[1])
                ch_conditions.append(
                    "start_time BETWEEN %(mf_dr_start)s AND %(mf_dr_end)s "
                    "AND created_at >= %(mf_dr_start)s - INTERVAL 1 DAY"
                )
            elif key == "created_at" and value:
                params["mf_created_at"] = _parse_dt(value)
                ch_conditions.append(
                    "start_time >= %(mf_created_at)s "
                    "AND created_at >= %(mf_created_at)s - INTERVAL 1 DAY"
                )
            elif key == "project_id":
                # Already handled by project_where()
                pass

        self._filter_clause = " AND ".join(ch_conditions) if ch_conditions else ""
        self._filter_params = params

    def _spans_time_window(self) -> str:
        """Spans time-window predicate — event-time semantics.

        Deliberate change from pre-CH behavior: windows evaluate on
        ``start_time`` (event time, the partition key, so CH prunes), not
        ``created_at`` (ingest time, unindexed, full scan). Blind spot: spans
        ingested after their window was evaluated are never counted.
        TODO: offset evaluation windows by ingest lag. The padded
        ``created_at`` companion only guards against clock-skewed future
        ``start_time``. Mirrors ``DashboardQueryBuilder``.
        """
        return (
            "start_time BETWEEN %(start_time)s AND %(end_time)s "
            "AND created_at >= %(start_time)s - INTERVAL 1 DAY"
        )

    def _spans_time_window_half_open(self) -> str:
        """Half-open (``>= start AND < end``) variant for daily/monthly sums."""
        return (
            "start_time >= %(start_time)s AND start_time < %(end_time)s "
            "AND created_at >= %(start_time)s - INTERVAL 1 DAY"
        )

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

        base_where = self._spans_base_where()
        time_win = f"AND {self._spans_time_window()}"
        time_win_ho = f"AND {self._spans_time_window_half_open()}"

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
            query = f"""
                SELECT
                    CASE WHEN uniq(trace_session_id) = 0 THEN NULL
                         ELSE uniqIf(trace_session_id, error_count = 0) / uniq(trace_session_id)
                    END AS value
                FROM (
                    SELECT
                        trace_session_id,
                        countIf(status = 'ERROR') AS error_count
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND trace_session_id IS NOT NULL
                    GROUP BY trace_session_id
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
                      AND provider IS NOT NULL
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
                SELECT avg(latency_ms) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == LLM_RESPONSE_TIME:
            query = f"""
                SELECT avg(latency_ms) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'llm'
            """

        elif metric_type == TOKEN_USAGE:
            query = f"""
                SELECT sum(total_tokens) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == DAILY_TOKENS_SPENT:
            query = f"""
                SELECT sum(total_tokens) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win_ho}
            """

        elif metric_type == MONTHLY_TOKENS_SPENT:
            query = f"""
                SELECT sum(total_tokens) AS value
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win_ho}
            """

        elif metric_type == EVALUATION_METRICS:
            query, params = self._build_eval_metric_value_query(params)

        else:
            query = "SELECT NULL AS value"

        return query, params

    def _build_eval_metric_value_query(
        self, params: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build the eval metric value query against the configured eval-logger table."""
        if not self.eval_config_id:
            return "SELECT NULL AS value", params

        params["eval_config_id"] = self.eval_config_id

        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_membership(eval_nd)

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

        For rate-based and latency metrics, computes per-row stats.
        For aggregated metrics (token usage, error counts), computes
        stats over time-bucketed values.

        Returns:
            A ``(query_string, params_dict)`` tuple with ``mean`` and ``stddev`` columns.
        """
        params = dict(self.params)
        params.update(self._filter_params)
        params["start_time"] = _parse_dt(start_time)
        params["end_time"] = _parse_dt(end_time)

        base_where = self._spans_base_where()
        time_win = f"AND {self._spans_time_window()}"

        if metric_type == ERROR_RATES_FOR_FUNCTION_CALLING:
            query = f"""
                SELECT
                    avg(is_error) AS mean,
                    stddevSamp(is_error) AS stddev
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
            query = f"""
                SELECT
                    avg(is_error_free) AS mean,
                    stddevSamp(is_error_free) AS stddev
                FROM (
                    SELECT
                        CASE WHEN countIf(status = 'ERROR') > 0 THEN 0.0 ELSE 1.0 END AS is_error_free
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND trace_session_id IS NOT NULL
                    GROUP BY trace_session_id
                )
            """

        elif metric_type == SERVICE_PROVIDER_ERROR_RATES:
            query = f"""
                SELECT
                    avg(is_error_free) AS mean,
                    stddevSamp(is_error_free) AS stddev
                FROM (
                    SELECT
                        CASE WHEN countIf(status = 'ERROR') > 0 THEN 0.0 ELSE 1.0 END AS is_error_free
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_win}
                      AND provider != ''
                      AND provider IS NOT NULL
                    GROUP BY provider
                )
            """

        elif metric_type == LLM_API_FAILURE_RATES:
            query = f"""
                SELECT
                    avg(is_error) AS mean,
                    stddevSamp(is_error) AS stddev
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
                    avg(latency_ms) AS mean,
                    stddevSamp(latency_ms) AS stddev
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
            """

        elif metric_type == LLM_RESPONSE_TIME:
            query = f"""
                SELECT
                    avg(latency_ms) AS mean,
                    stddevSamp(latency_ms) AS stddev
                FROM {SPANS_TABLE}
                {base_where}
                  {time_win}
                  AND observation_type = 'llm'
            """

        elif metric_type == EVALUATION_METRICS:
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
            # the old Python path. Buckets are on created_at, so cap it at
            # end_time: a late-ingested in-window span would otherwise create
            # a sparse trailing bucket that skews mean/stddev every tick.
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
                      AND created_at <= %(end_time)s
                    GROUP BY bucket_ts
                )
            """

        else:
            query = "SELECT NULL AS mean, NULL AS stddev"

        return query, params

    def _build_eval_stats_query(
        self, params: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """Build eval metric stats (mean/stddev) query.

        ``stddevPop`` matches the pre-migration PG ``StdDev`` default.
        """
        if not self.eval_config_id:
            return "SELECT NULL AS mean, NULL AS stddev", params

        params["eval_config_id"] = self.eval_config_id
        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_membership(eval_nd)

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
        params["freq_seconds"] = frequency_seconds

        bucket_expr = _SPANS_BUCKET_EXPR

        base_where = self._spans_base_where()
        time_filter = f"AND {self._spans_time_window()}"

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
            query = f"""
                SELECT
                    timestamp,
                    CASE WHEN uniq(trace_session_id) = 0 THEN 0
                         ELSE uniqIf(trace_session_id, error_count = 0) / uniq(trace_session_id)
                    END AS value
                FROM (
                    SELECT
                        {bucket_expr} AS timestamp,
                        trace_session_id,
                        countIf(status = 'ERROR') AS error_count
                    FROM {SPANS_TABLE}
                    {base_where}
                      {time_filter}
                      AND trace_session_id IS NOT NULL
                    GROUP BY timestamp, trace_session_id
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
                      AND provider IS NOT NULL
                    GROUP BY timestamp, provider
                )
                GROUP BY timestamp
                ORDER BY timestamp
            """

        elif metric_type == EVALUATION_METRICS:
            query, params = self._build_eval_time_series_query(params)

        else:
            query = "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0"

        return query, params

    def _build_eval_time_series_query(
        self,
        params: Dict[str, Any],
    ) -> Tuple[str, Dict[str, Any]]:
        """Build eval metric time-series query.

        Buckets on the joined SPAN's ``start_time`` (the user's application
        timeline): a low score must chart when the activity happened, not
        when the eval was computed — and late-computed evals must not emit
        buckets past the requested window.
        """
        if not self.eval_config_id:
            return "SELECT NULL AS timestamp, NULL AS value WHERE 1 = 0", params

        params["eval_config_id"] = self.eval_config_id
        eval_table, eval_nd = eval_logger_source()
        eval_where = self._eval_membership(eval_nd, "id, start_time")
        bucket_expr = _EVAL_SPAN_BUCKET_EXPR

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

    def _eval_membership(self, not_deleted: str, span_cols: str = "id") -> str:
        """Span-windowed membership (JOIN) + eval-row guards.

        ``not_deleted`` comes from the same ``eval_logger_source()`` call that
        resolved the table, so the pair stays consistent.

        The window lives on the SPAN's ``created_at`` (ingest time — late
        spans land in the current tick), not the eval row's — evals run async
        after their spans. A JOIN (vs ``IN``) streams the span set instead of
        materializing it in memory: a 30-day graph window on a busy project
        overflows ``max_bytes_in_set`` as an IN-set (~100M ids).
        The eval-side ``created_at`` lower bound (1-day skew pad) is
        correctness-neutral but is the eval table's only partition prune
        (toYYYYMM(created_at)); the v2 sort key also prunes granules with it.
        Only completed, non-errored rows count — pending/errored rows carry
        NULL outputs that would read as failures.
        """
        return (
            f"INNER JOIN ({self._bounded_spans_subquery(span_cols)}) AS sp "
            f"ON observation_span_id = sp.id "
            f"WHERE custom_eval_config_id = toUUID(%(eval_config_id)s) "
            f"AND {not_deleted} "
            f"AND error = 0 "
            f"AND ifNull(output_str, '') != 'ERROR' "
            f"AND status NOT IN ({_EVAL_NON_VALUE_STATUSES}) "
            f"AND created_at >= %(start_time)s - INTERVAL 1 DAY"
        )

    def _bounded_spans_subquery(self, select_cols: str = "id") -> str:
        """Metric-window-bounded spans subquery for eval membership."""
        filter_extra = f" AND {self._filter_clause}" if self._filter_clause else ""
        return (
            f"SELECT {select_cols} FROM {SPANS_TABLE} "
            f"WHERE {self.project_filter_sql()} "
            f"AND is_deleted = 0 "
            f"AND created_at >= %(start_time)s AND created_at < %(end_time)s "
            f"AND start_time >= %(start_time)s - INTERVAL 1 DAY "
            f"AND start_time < %(end_time)s + INTERVAL 1 DAY"
            f"{filter_extra}"
        )
