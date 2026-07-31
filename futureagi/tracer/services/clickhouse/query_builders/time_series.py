"""
Time-Series Query Builder for ClickHouse.

Replaces ``get_all_system_metrics()`` and ``get_system_metric_data()`` from
``tracer.utils.graphs_optimized`` with ClickHouse-native queries.

Strategy:
- Unfiltered dashboard queries read from the ``spans_hourly_rollup``
  pre-aggregated AggregatingMergeTree (v2 schema 010) using ``countMerge`` /
  ``sumMerge`` / ``quantilesTDigestMerge`` combinators. The rollup is fed
  directly from the v2 typed-JSON ``spans`` table via an incremental MV.
- When attribute filters are present, falls back to scanning the v2
  ``spans`` table directly.

CH25 close-out (2026-05-28): cut over from the legacy ``span_metrics_hourly``
(fed by ``spans_mv`` ← ``tracer_observation_span`` CDC mirror) to
``spans_hourly_rollup``. Removes the last dashboard read-path dependency on
the legacy CDC-based aggregate.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from django.conf import settings

from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder


class TimeSeriesQueryBuilder(BaseQueryBuilder):
    """Build time-series metric queries for the dashboard.

    Returns all four metrics in a single query: latency, tokens, cost,
    and traffic.  The output format matches the dict returned by
    ``get_all_system_metrics()``::

        {
            "latency": [{"timestamp": "...", "value": 0, "latency": 0}, ...],
            "tokens":  [{"timestamp": "...", "value": 0, "tokens": 0}, ...],
            "cost":    [{"timestamp": "...", "value": 0, "cost": 0}, ...],
            "traffic": [{"timestamp": "...", "traffic": 0}, ...],
        }

    Args:
        project_id: Project UUID string.
        filters: Frontend filter list (may be empty).
        interval: Time bucket interval (``"hour"``, ``"day"``, ``"week"``,
            ``"month"``).
        system_metric_filters: Additional keyword filters (currently unused;
            reserved for future per-model breakdowns).
    """

    # Pre-aggregated table (AggregatingMergeTree)
    # CH25 close-out (2026-05-28): switched from the legacy
    # `span_metrics_hourly` (fed by `spans_mv` ← `tracer_observation_span` CDC
    # mirror) to the v2 `spans_hourly_rollup` (fed directly from the v2 typed-
    # JSON `spans` table — no CDC). The v2 rollup uses AggregateFunction
    # columns + `*Merge()` combinators (real AggregatingMergeTree pattern)
    # whereas the legacy table stored already-summed Int64s.
    AGG_TABLE = "spans_hourly_rollup"
    # Denormalized raw table (for filtered queries)
    RAW_TABLE = "spans"
    ATTR_ROLLUP_TABLE = "dashboard_attr_rollup"
    ATTR_ROLLUP_KEYS = frozenset({"final_status"})
    ATTR_ROLLUP_INTERVALS = frozenset({"hour", "day", "week", "month"})

    def __init__(
        self,
        project_id: str,
        filters: list[dict] | None = None,
        interval: str = "hour",
        system_metric_filters: dict[str, Any] | None = None,
        observe_type: str = "trace",
        metric_id: str = "latency",
        **kwargs: Any,
    ) -> None:
        super().__init__(project_id, **kwargs)
        self.filters = filters or []
        self.interval = interval
        self.system_metric_filters = system_metric_filters or {}
        self.observe_type = str(observe_type or "trace").strip().lower()
        self.metric_id = metric_id
        self.start_date: datetime | None = None
        self.end_date: datetime | None = None
        self.rollup_window_adjusted = False
        self.rollup_window_start: datetime | None = None
        self.rollup_window_end: datetime | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(self) -> tuple[str, dict[str, Any]]:
        """Build the time-series query.

        Returns:
            A ``(query_string, params)`` tuple.
        """
        # Lazy import: a module-level import would form a v1↔v2 circular import.
        from tracer.services.clickhouse.v2.query_builders.filters import (
            ClickHouseFilterBuilderV2 as ClickHouseFilterBuilder,
        )

        self.start_date, self.end_date = self._parse_time_range_utc()
        self.params["start_date"] = self.start_date
        self.params["end_date"] = self.end_date

        rollup_filter = self._safe_attr_rollup_filter()
        if rollup_filter is not None and self._attr_rollup_window_covered():
            return self._build_attr_rollup_query(*rollup_filter)

        attribute_filters, remaining_filters = self._partition_attribute_filters()
        if attribute_filters:
            return self._build_attribute_filtered_query(
                ClickHouseFilterBuilder,
                attribute_filters=attribute_filters,
                remaining_filters=remaining_filters,
            )

        # Determine if we have attribute filters that prevent using the
        # pre-aggregated table.
        # Project + window scope must reach the filter compiler: without them
        # the trace-membership subqueries it emits for SPAN_ATTRIBUTE /
        # SYSTEM_METRIC filters scan every tenant's spans for all time.
        filter_builder = ClickHouseFilterBuilder(
            table=self.RAW_TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
            span_date_scope=True,
            query_mode=(
                ClickHouseFilterBuilder.QUERY_MODE_SPAN
                if self.observe_type == "span"
                else ClickHouseFilterBuilder.QUERY_MODE_TRACE
            ),
        )
        extra_where, extra_params = filter_builder.translate(self.filters)
        self.params.update(extra_params)

        if extra_where:
            return self._build_raw_query(extra_where)
        else:
            return self._build_agg_query()

    def _partition_attribute_filters(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Split explicit span-attribute predicates from all other filters.

        Trace graphs need a different SQL shape for arbitrary attributes:
        filtering the root-row aggregate with ``trace_id IN (SELECT …)`` makes
        ClickHouse materialize a potentially project-wide Set before reading
        the outer table.  On high-volume projects that Set construction is the
        source of the 30-second ``FutureSetFromSubquery`` timeout.  Keeping the
        split here lets :meth:`_build_attribute_filtered_query` use a
        filter-first candidate relation while the canonical filter compiler
        still owns every operator/value rule.
        """
        attribute_filters: list[dict[str, Any]] = []
        remaining_filters: list[dict[str, Any]] = []
        for filter_item in self.filters:
            column_id, config = self._filter_parts(filter_item)
            col_type = str(
                config.get("col_type") or config.get("colType") or ""
            ).upper()
            if (
                column_id not in ("created_at", "start_time")
                and col_type == "SPAN_ATTRIBUTE"
            ):
                attribute_filters.append(filter_item)
            else:
                remaining_filters.append(filter_item)
        return attribute_filters, remaining_filters

    @staticmethod
    def _prefix_condition_params(
        condition: str,
        params: dict[str, Any],
        *,
        prefix: str,
    ) -> tuple[str, dict[str, Any]]:
        """Namespace one independently compiled filter's bound parameters."""
        prefixed: dict[str, Any] = {}
        for name, value in params.items():
            prefixed_name = f"{prefix}_{name}"
            condition = condition.replace(
                f"%({name})s",
                f"%({prefixed_name})s",
            )
            prefixed[prefixed_name] = value
        return condition, prefixed

    def _compile_attribute_condition(
        self,
        filter_builder_cls,
        filters: list[dict[str, Any]],
        *,
        prefix: str,
    ) -> str:
        """Compile row-level span attributes through the canonical contract."""
        filter_builder = filter_builder_cls(
            table=self.RAW_TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
            query_mode=filter_builder_cls.QUERY_MODE_SPAN,
        )
        condition, params = filter_builder.translate(filters)
        condition, params = self._prefix_condition_params(
            condition,
            params,
            prefix=prefix,
        )
        self.params.update(params)
        return condition

    def _build_trace_attribute_candidate_join(
        self,
        filter_builder_cls,
        attribute_filters: list[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        """Build one exact filter-first trace candidate relation.

        Every arbitrary attribute filter is compiled independently, matching
        the existing trace semantics: filter A and filter B may be satisfied
        by different spans in the same trace.  The grouped relation intersects
        those per-filter matches with ``countIf`` and exposes each trace ID
        once, so the outer root aggregate cannot be multiplied by child rows.

        Attributes guaranteed to live on the trace root are returned as direct
        outer predicates.  This avoids both the candidate scan and the old
        project-wide Set for common root attributes, while retaining generic
        any-span behavior for every other key.
        """
        root_keys = frozenset(filter_builder_cls._TRACE_ROOT_ATTRIBUTE_KEYS)
        root_filters: list[dict[str, Any]] = []
        any_span_filters: list[dict[str, Any]] = []
        for filter_item in attribute_filters:
            column_id, _config = self._filter_parts(filter_item)
            if column_id in root_keys:
                root_filters.append(filter_item)
            else:
                any_span_filters.append(filter_item)

        outer_conditions: list[str] = []
        if root_filters:
            root_condition = self._compile_attribute_condition(
                filter_builder_cls,
                root_filters,
                prefix="graph_root_attr",
            )
            if root_condition:
                outer_conditions.append(root_condition)

        if not any_span_filters:
            return "", outer_conditions

        candidate_conditions: list[str] = []
        for index, filter_item in enumerate(any_span_filters):
            condition = self._compile_attribute_condition(
                filter_builder_cls,
                [filter_item],
                prefix=f"graph_candidate_attr_{index}",
            )
            if condition:
                candidate_conditions.append(condition)

        if not candidate_conditions:
            return "", outer_conditions

        any_match = " OR ".join(f"({condition})" for condition in candidate_conditions)
        having = ""
        if len(candidate_conditions) > 1:
            all_matches = " AND ".join(
                f"countIf({condition}) > 0" for condition in candidate_conditions
            )
            having = f"HAVING {all_matches}"

        # The candidate scan is project- and time-pruned before reading Map
        # values.  The one-day skew buffer exactly matches the prior
        # trace-membership subquery contract, so this is a SQL-shape change,
        # not a result-set change.
        candidate_join = f"""
        INNER JOIN (
            SELECT trace_id
            FROM {self.RAW_TABLE}
            PREWHERE {self.project_filter_sql()}
              AND start_time >= %(start_date)s - INTERVAL 1 DAY
              AND start_time < %(end_date)s + INTERVAL 1 DAY
            WHERE is_deleted = 0
              AND ({any_match})
            GROUP BY trace_id
            {having}
        ) AS graph_attr_candidates USING (trace_id)
        """
        return candidate_join, outer_conditions

    def _build_attribute_filtered_query(
        self,
        filter_builder_cls,
        *,
        attribute_filters: list[dict[str, Any]],
        remaining_filters: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        """Build an exact raw graph query without an unbounded attribute Set."""
        remaining_builder = filter_builder_cls(
            table=self.RAW_TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
            span_date_scope=True,
            query_mode=(
                filter_builder_cls.QUERY_MODE_SPAN
                if self.observe_type == "span"
                else filter_builder_cls.QUERY_MODE_TRACE
            ),
        )
        remaining_where, remaining_params = remaining_builder.translate(
            remaining_filters
        )
        self.params.update(remaining_params)

        candidate_join = ""
        outer_conditions: list[str] = []
        if remaining_where:
            outer_conditions.append(remaining_where)

        if self.observe_type == "span":
            attribute_condition = self._compile_attribute_condition(
                filter_builder_cls,
                attribute_filters,
                prefix="graph_span_attr",
            )
            if attribute_condition:
                outer_conditions.append(attribute_condition)
        else:
            candidate_join, root_conditions = (
                self._build_trace_attribute_candidate_join(
                    filter_builder_cls,
                    attribute_filters,
                )
            )
            outer_conditions.extend(root_conditions)

        return self._build_raw_query(
            " AND ".join(f"({condition})" for condition in outer_conditions)
            if outer_conditions
            else "1 = 1",
            candidate_join=candidate_join,
        )

    @staticmethod
    def _filter_parts(filter_item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        column_id = filter_item.get("column_id") or filter_item.get("columnId") or ""
        config = (
            filter_item.get("filter_config") or filter_item.get("filterConfig") or {}
        )
        return str(column_id), config

    @staticmethod
    def _utc_naive_datetime(value: Any) -> datetime | None:
        """Parse an ISO boundary and normalize offsets to naive UTC for CH."""
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            cleaned = f"{value[:-1]}+00:00" if value.endswith("Z") else value
            try:
                parsed = datetime.fromisoformat(cleaned)
            except ValueError:
                return None
        else:
            return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed

    def _parse_time_range_utc(self) -> tuple[datetime, datetime]:
        """Preserve base defaults while correcting explicit offset boundaries."""
        start_date, end_date = self.parse_time_range(self.filters)
        assert start_date is not None and end_date is not None
        for filter_item in self.filters:
            column_id, config = self._filter_parts(filter_item)
            if column_id not in ("created_at", "start_time"):
                continue
            filter_op = config.get("filter_op") or config.get("filterOp")
            filter_value = config.get("filter_value", config.get("filterValue"))
            if filter_op == "greater_than":
                parsed = self._utc_naive_datetime(filter_value)
                if parsed is not None:
                    start_date = parsed
            elif filter_op == "less_than":
                parsed = self._utc_naive_datetime(filter_value)
                if parsed is not None:
                    end_date = parsed
            elif (
                filter_op == "between"
                and isinstance(filter_value, list)
                and len(filter_value) == 2
            ):
                parsed_start = self._utc_naive_datetime(filter_value[0])
                parsed_end = self._utc_naive_datetime(filter_value[1])
                if parsed_start is not None:
                    start_date = parsed_start
                if parsed_end is not None:
                    end_date = parsed_end
        return start_date, end_date

    def _safe_attr_rollup_filter(self) -> tuple[str, tuple[str, ...]] | None:
        """Return the one root-attribute predicate the rollup can answer exactly.

        Only ``final_status`` has positive production parity evidence for this
        incident path. It is safe for the trace latency graph only. Date filters
        are handled by :meth:`parse_time_range`; any other filter forces the
        raw spans path. Empty values are rejected because the rollup cannot
        distinguish an absent Map key from a present key whose value is empty.
        """
        if (
            self.observe_type != "trace"
            or self.metric_id != "latency"
            or self.interval not in self.ATTR_ROLLUP_INTERVALS
        ):
            return None

        attr_filter: tuple[str, tuple[str, ...]] | None = None
        for filter_item in self.filters:
            column_id, config = self._filter_parts(filter_item)
            filter_type = str(
                config.get("filter_type") or config.get("filterType") or ""
            ).lower()
            filter_op = str(
                config.get("filter_op") or config.get("filterOp") or ""
            ).lower()
            filter_value = config.get("filter_value", config.get("filterValue"))
            col_type = str(
                config.get("col_type") or config.get("colType") or ""
            ).upper()

            if column_id in ("created_at", "start_time"):
                if filter_type not in ("date", "datetime") or filter_op not in (
                    "between",
                    "greater_than",
                    "less_than",
                ):
                    return None
                continue

            if (
                attr_filter is not None
                or column_id not in self.ATTR_ROLLUP_KEYS
                or col_type != "SPAN_ATTRIBUTE"
                or filter_type not in ("text", "string")
                or filter_op not in ("equals", "in")
            ):
                return None

            if filter_op == "equals":
                if isinstance(filter_value, list):
                    return None
                values = (filter_value,)
            else:
                if not isinstance(filter_value, list) or not filter_value:
                    return None
                values = tuple(filter_value)

            if any(not isinstance(value, str) or not value for value in values):
                return None
            attr_filter = (
                column_id,
                tuple(value.lower() for value in values),
            )

        return attr_filter

    def _attr_rollup_window_covered(self) -> bool:
        """Fail closed until the existing dashboard rollup is backfilled."""
        if not getattr(settings, "TRACE_GRAPH_ATTR_ROLLUP_ENABLED", False):
            return False
        covered_since = getattr(settings, "DASHBOARD_ATTR_ROLLUP_COVERED_SINCE", None)
        if not isinstance(covered_since, datetime) or self.start_date is None:
            return False

        # Compare the first complete rollup hour, not the raw request start.
        # Explicit request offsets were normalized to naive UTC above; retain
        # defensive handling for direct builder callers that inject datetimes.
        effective_start = self._ceil_hour(self.start_date)
        if effective_start.tzinfo is not None:
            effective_start = effective_start.astimezone(UTC).replace(tzinfo=None)
        if covered_since.tzinfo is not None:
            covered_since = covered_since.astimezone(UTC).replace(tzinfo=None)
        return effective_start >= covered_since

    @staticmethod
    def _floor_hour(value: datetime) -> datetime:
        return value.replace(minute=0, second=0, microsecond=0)

    @classmethod
    def _ceil_hour(cls, value: datetime) -> datetime:
        floored = cls._floor_hour(value)
        return floored if floored == value else floored + timedelta(hours=1)

    def _build_attr_rollup_query(
        self,
        attr_key: str,
        attr_values: tuple[str, ...],
    ) -> tuple[str, dict[str, Any]]:
        """Read the existing root-span rollup using its hourly window policy.

        Only complete hourly buckets are included. A raw boundary-hour union is
        unsafe on whale tenants (one hour can exceed the 1 GiB read budget), so
        an adjusted window is exposed to the caller instead of silently
        presenting boundary data as exact.
        """
        assert self.start_date is not None and self.end_date is not None
        # Ceil the start so the result never includes data preceding the
        # requested range; floor the end to omit the incomplete trailing hour.
        rollup_start = self._ceil_hour(self.start_date)
        rollup_end = self._floor_hour(self.end_date)
        if rollup_start >= rollup_end:
            # A sub-hour range has no complete rollup bucket.
            from tracer.services.clickhouse.v2.query_builders.filters import (
                ClickHouseFilterBuilderV2,
            )

            filter_builder = ClickHouseFilterBuilderV2(
                table=self.RAW_TABLE,
                project_id=self.project_id,
                project_ids=self.project_ids,
                span_date_scope=True,
                query_mode=ClickHouseFilterBuilderV2.QUERY_MODE_TRACE,
            )
            extra_where, extra_params = filter_builder.translate(self.filters)
            self.params.update(extra_params)
            return self._build_raw_query(extra_where)

        self.rollup_window_adjusted = (
            rollup_start != self.start_date or rollup_end != self.end_date
        )
        self.rollup_window_start = rollup_start
        self.rollup_window_end = rollup_end
        self.params.update(
            {
                "attr_key": attr_key,
                "attr_values": attr_values,
                "rollup_start": rollup_start,
                "rollup_end": rollup_end,
            }
        )
        bucket_fn = self.time_bucket_expr(self.interval)

        query = f"""
        SELECT
            {bucket_fn}(hour) AS time_bucket,
            sumMerge(latency_sum) /
                greatest(countMerge(n), 1) AS avg_latency,
            0 AS total_tokens,
            0 AS avg_cost,
            countMerge(n) AS traffic_count,
            0 AS prompt_tokens,
            0 AS completion_tokens,
            0 AS error_rate
        FROM {self.ATTR_ROLLUP_TABLE}
        WHERE project_id = %(project_id)s
          AND attr_key = %(attr_key)s
          AND hour >= %(rollup_start)s
          AND hour < %(rollup_end)s
          AND lowerUTF8(attr_value) IN %(attr_values)s
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        return query, self.params

    def format_result(
        self,
        rows: list[tuple],
        columns: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Post-process raw ClickHouse rows into the standard response dict.

        Expected columns from the query:
        ``time_bucket, avg_latency, total_tokens, avg_cost, traffic_count``

        Args:
            rows: Rows returned by ClickHouse.
            columns: Column name list.

        Returns:
            Dict with keys ``latency``, ``tokens``, ``cost``, ``traffic``.
        """
        assert self.start_date is not None and self.end_date is not None
        series_start_date = self.rollup_window_start or self.start_date
        # The SQL end bound is exclusive. Base zero-filling is inclusive, so
        # keep it inside the disclosed rollup window instead of appending a
        # synthetic zero point exactly at ``rollup_window_end``.
        series_end_date = (
            self.rollup_window_end - timedelta(microseconds=1)
            if self.rollup_window_end is not None
            else self.end_date
        )

        # Build per-metric data lists
        latency_data: list[dict[str, Any]] = []
        tokens_data: list[dict[str, Any]] = []
        cost_data: list[dict[str, Any]] = []
        traffic_data: list[dict[str, Any]] = []

        for row in rows:
            # Support both dict rows (from execute_ch_query) and tuple rows
            if isinstance(row, dict):
                ts = row.get(
                    "time_bucket", row.get(columns[0] if columns else "time_bucket")
                )
                avg_lat = row.get("avg_latency", 0)
                total_tok = row.get("total_tokens", 0)
                avg_cst = row.get("avg_cost", 0)
                count = row.get("traffic_count", 0)
            else:
                ts = row[0]
                avg_lat = row[1] if len(row) > 1 else 0
                total_tok = row[2] if len(row) > 2 else 0
                avg_cst = row[3] if len(row) > 3 else 0
                count = row[4] if len(row) > 4 else 0
            ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

            latency_data.append(
                {
                    "timestamp": ts_str,
                    "value": round(avg_lat, 2) if avg_lat else 0,
                    "latency": round(avg_lat, 2) if avg_lat else 0,
                }
            )
            tokens_data.append(
                {
                    "timestamp": ts_str,
                    "value": round(total_tok, 2) if total_tok else 0,
                    "tokens": round(total_tok, 2) if total_tok else 0,
                }
            )
            cost_data.append(
                {
                    "timestamp": ts_str,
                    "value": round(avg_cst, 9) if avg_cst else 0,
                    "cost": round(avg_cst, 9) if avg_cst else 0,
                }
            )
            traffic_data.append(
                {
                    "timestamp": ts_str,
                    "traffic": count or 0,
                }
            )

        # Helper to extract values from dict or tuple rows
        def _get(r, key, idx, default=0):
            if isinstance(r, dict):
                return r.get(key, default)
            return r[idx] if len(r) > idx else default

        # Zero-fill missing buckets for each metric
        latency_data = self.format_time_series(
            rows=[(_get(r, "time_bucket", 0), _get(r, "avg_latency", 1)) for r in rows],
            columns=["time_bucket", "value", "latency"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value", "latency"],
        )
        tokens_data = self.format_time_series(
            rows=[
                (_get(r, "time_bucket", 0), _get(r, "total_tokens", 2)) for r in rows
            ],
            columns=["time_bucket", "value", "tokens"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value", "tokens"],
        )
        cost_data = self.format_time_series(
            rows=[(_get(r, "time_bucket", 0), _get(r, "avg_cost", 3)) for r in rows],
            columns=["time_bucket", "value", "cost"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value", "cost"],
        )
        traffic_data = self.format_time_series(
            rows=[
                (_get(r, "time_bucket", 0), _get(r, "traffic_count", 4)) for r in rows
            ],
            columns=["time_bucket", "traffic"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["traffic"],
        )

        # Additional metrics: prompt_tokens, completion_tokens, error_rate
        prompt_tokens_data = self.format_time_series(
            rows=[
                (_get(r, "time_bucket", 0), _get(r, "prompt_tokens", 5)) for r in rows
            ],
            columns=["time_bucket", "value"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value"],
        )
        completion_tokens_data = self.format_time_series(
            rows=[
                (_get(r, "time_bucket", 0), _get(r, "completion_tokens", 6))
                for r in rows
            ],
            columns=["time_bucket", "value"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value"],
        )
        error_rate_data = self.format_time_series(
            rows=[(_get(r, "time_bucket", 0), _get(r, "error_rate", 7)) for r in rows],
            columns=["time_bucket", "value"],
            interval=self.interval,
            start_date=series_start_date,
            end_date=series_end_date,
            value_keys=["value"],
        )

        return {
            "latency": latency_data,
            "tokens": tokens_data,
            "cost": cost_data,
            "traffic": traffic_data,
            "prompt_tokens": prompt_tokens_data,
            "completion_tokens": completion_tokens_data,
            "input_tokens": prompt_tokens_data,
            "output_tokens": completion_tokens_data,
            "total_tokens": tokens_data,
            "error_rate": error_rate_data,
        }

    # ------------------------------------------------------------------
    # Private query builders
    # ------------------------------------------------------------------

    def _build_agg_query(self) -> tuple[str, dict[str, Any]]:
        """Build a query against the pre-aggregated ``spans_hourly_rollup`` table.

        Uses ``*Merge()`` aggregate combinators (``countMerge``,
        ``sumMerge``, ``quantilesTDigestMerge``) to reconstruct metrics
        from the ``AggregatingMergeTree`` state columns. See
        ``tracer/services/clickhouse/v2/schema/010_hourly_downsample.sql``
        for the rollup table definition.
        """
        bucket_fn = self.time_bucket_expr(self.interval)

        # quantilesTDigestMerge returns a Tuple; index [1] is the 0.5 (median).
        # The v2 rollup stores 3 quantiles (0.5, 0.95, 0.99) vs the legacy 4
        # (0.5, 0.9, 0.95, 0.99) — we still surface the median as avg_latency
        # to preserve the dashboard contract.
        query = f"""
        SELECT
            {bucket_fn}(hour) AS time_bucket,
            (quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_q))[1]
                AS avg_latency,
            sumMerge(total_tokens_sum) AS total_tokens,
            sumMerge(cost_sum) / greatest(countMerge(n), 1)
                AS avg_cost,
            countMerge(n) AS traffic_count,
            sumMerge(prompt_tokens_sum) AS prompt_tokens,
            sumMerge(completion_tokens_sum) AS completion_tokens,
            countMerge(error_count) * 100.0 / greatest(countMerge(n), 1)
                AS error_rate
        FROM {self.AGG_TABLE}
        WHERE project_id = %(project_id)s
          AND hour >= %(start_date)s
          AND hour < %(end_date)s
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        return query, self.params

    def _build_raw_query(
        self,
        extra_where: str,
        *,
        candidate_join: str = "",
    ) -> tuple[str, dict[str, Any]]:
        """Build a query against the raw ``spans`` table with filters applied."""
        bucket_fn = self.time_bucket_expr(self.interval)
        entity_scope = (
            "AND (parent_span_id IS NULL OR parent_span_id = '')"
            if self.observe_type == "trace"
            else ""
        )

        query = f"""
        SELECT
            {bucket_fn}(start_time) AS time_bucket,
            avg(latency_ms) AS avg_latency,
            sum(total_tokens) AS total_tokens,
            avg(cost) AS avg_cost,
            count() AS traffic_count,
            sum(prompt_tokens) AS prompt_tokens,
            sum(completion_tokens) AS completion_tokens,
            countIf(status = 'ERROR') * 100.0 / greatest(count(), 1)
                AS error_rate
        FROM {self.RAW_TABLE}
        {candidate_join}
        PREWHERE {self.project_filter_sql()}
          AND start_time >= %(start_date)s
          AND start_time < %(end_date)s
        WHERE is_deleted = 0
          {entity_scope}
          AND {extra_where}
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        return query, self.params
