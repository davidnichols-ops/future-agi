"""
v2 Dashboard query builder — targets the CH 25.3 spans schema.

Subclass + post-rewrite. The v1 dashboard builder emits 1 SQL query per
dashboard metric (latency, p95, model breakdown, custom-attribute pivots,
etc.). Each metric type goes through `build_metric_query()`; `build_all_queries`
fans out over it and returns `[(sql, params, meta), …]`.

Unlike the list builders, the dashboard builder dispatches EVERY metric type
through that ONE polymorphic method. A metric may target the migrated `spans`
schema (system_metric / custom_attribute) OR a non-migrated legacy table
(eval_metric → `usage_apicalllog`, annotation_metric → `model_hub_score`, both
still on `_peerdb_is_deleted` / `deleted`). `V2RewriteMixin`'s blanket auto-wrap
cannot distinguish aliases by physical table. Both dispatch methods are
therefore excluded from the mixin and the rewrite is applied here after
protecting/restoring every legacy-table alias. That matters for mixed queries
too: a system metric can JOIN `model_hub_score` for an annotation breakdown
while its spans columns still need the v2 rewrite.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta

from tracer.services.clickhouse.query_builders.dashboard import (
    SYSTEM_METRICS,
    DashboardQueryBuilder,
    InvalidMetricCombinationError,
)
from tracer.services.clickhouse.v2.query_builders._rewrite import V2RewriteMixin
from tracer.services.clickhouse.v2.query_builders.filters import (
    rewrite_and_apply_v2_settings,
)
from tracer.utils.filter_operators import normalize_span_attribute_filter_type

# Tables whose columns must NOT be rewritten (they keep `_peerdb_is_deleted`).
_LEGACY_TABLE_RE = re.compile(
    r"(?:usage_apicalllog|model_hub_score)\s+AS\s+(\w+)", re.IGNORECASE
)

# The eval-metric builder uses candidate-scoped subqueries over the legacy
# usage table. Their outer aliases no longer appear immediately after the
# table token, so `_LEGACY_TABLE_RE` cannot discover them. Protect only the
# explicitly generated usage aliases while the spans portion is rewritten.
_USAGE_CDC_COLUMN_RE = re.compile(
    r"\b(?P<alias>e|ev_(?:bd|f)\d+|usage_[A-Za-z0-9_]+)\."
    r"(?P<column>_peerdb_is_deleted|_peerdb_version)\b"
)


# Raw arbitrary-Map dashboard aggregates cannot be exact at unbounded tenant
# scale under the production read profile's byte ceiling.  Keep the sampled
# input small enough that even unusually wide attribute Maps are hydrated only
# for a finite physical-row set. Independently limited, contiguous time slices
# cover the complete requested window; the final LIMIT is a second hard bound.
_RAW_ATTRIBUTE_CANDIDATES_PER_BUCKET = 128
_RAW_ATTRIBUTE_CANDIDATE_LIMIT = 8_192
_RAW_ATTRIBUTE_MAX_STRATA = 64
_RAW_ATTRIBUTE_SAMPLING_STRATEGY = "bounded_physical_rows_per_time_bucket"
_RAW_ATTRIBUTE_GRANULARITY_SECONDS = {
    "minute": 60,
    "hour": 60 * 60,
    "day": 24 * 60 * 60,
    "week": 7 * 24 * 60 * 60,
    "month": 28 * 24 * 60 * 60,
    "year": 365 * 24 * 60 * 60,
}


def _protect_usage_cdc_columns(sql: str) -> str:
    return _USAGE_CDC_COLUMN_RE.sub(
        lambda match: (
            f"{match.group('alias')}.__usage_legacy_"
            f"{match.group('column').removeprefix('_peerdb_')}__"
        ),
        sql,
    )


def _restore_usage_cdc_columns(sql: str) -> str:
    return sql.replace(".__usage_legacy_is_deleted__", "._peerdb_is_deleted").replace(
        ".__usage_legacy_version__", "._peerdb_version"
    )


class DashboardQueryBuilderV2(V2RewriteMixin, DashboardQueryBuilder):
    """Drop-in v2 Dashboard builder.

    Both `build_metric_query` and `build_all_queries` are excluded from the
    mixin's blanket rewrite because they are polymorphic over metric type (see
    module docstring). `build_metric_query` applies the rewrite itself, then
    restores protected legacy aliases. This covers both legacy metrics and
    mixed queries such as a system metric with an annotation/eval breakdown.
    """

    # dashboard_attr_rollup ships only in the v2 schema, so the fast-path is safe only here.
    _attr_rollup_available: bool = True

    # Product reads use the direct-write curated dimension. This avoids a
    # runtime dependency on the optional ClickHouse dictionary (the locked
    # read-only production identity is intentionally not granted dictionary
    # access) while preserving latest-live + id-remap semantics.
    _direct_end_users_available: bool = True

    # Project-scope trace-attached annotations through the direct-write traces
    # table. The locked production read-only identity has no dictionary grants.
    _direct_trace_project_scope_available: bool = True

    # CH25 spans is partitioned by toDate(start_time). Do not inherit the
    # legacy created_at partition hint: it is redundant for correctness and
    # makes root metric queries ineligible for proj_root_spans.
    _spans_partitioned_by_created_at: bool = False

    _v2_rewrite_exclude = frozenset({"build_metric_query", "build_all_queries"})

    def __init__(self, query_config: dict) -> None:
        super().__init__(query_config)
        # A preset range is relative to ``now``. Freeze it once per request so
        # metric metadata, candidate slices, and the outer aggregate all use
        # the identical endpoints—even across midnight or concurrent metrics.
        self._resolved_time_range = super().parse_time_range()

    def parse_time_range(self) -> tuple[datetime, datetime]:
        return self._resolved_time_range

    def _uses_bounded_raw_attribute_input(self, metric: dict) -> bool:
        """Whether *metric* must read an arbitrary typed-Map value.

        Keep this deliberately V2- and shape-local.  Eval/annotation metrics,
        ordinary system metrics, the V1 builder, and the covered rollup path
        retain their existing exact SQL.
        """

        metric_type = metric.get("type", "system_metric")
        if metric_type == "custom_attribute":
            return metric.get("attribute_type", "number") in {
                "text",
                "string",
                "number",
                "boolean",
            }
        if metric_type != "system_metric":
            return False

        metric_name = (metric.get("id") or metric.get("name") or "").lower()
        # Saved widgets can carry a span attribute under the old
        # ``system_metric`` type; the base builder intentionally falls back to
        # its numeric Map. ``time_to_first_token`` is also a named Map metric.
        if metric_name not in SYSTEM_METRICS or metric_name == "time_to_first_token":
            return True

        custom_breakdowns = [
            breakdown
            for breakdown in self.breakdowns
            if breakdown.get("type", "system_metric") == "custom_attribute"
            and breakdown.get("source", "traces") not in {"datasets", "simulation"}
            and breakdown.get("attribute_type", "string")
            in {"text", "string", "number", "boolean"}
        ]
        per_metric_filters = metric.get("filters", [])
        has_custom_filter = any(
            item.get("metric_type") == "custom_attribute"
            and item.get("source", "traces") in {"traces", ""}
            for item in self.global_filters + per_metric_filters
        )
        if not custom_breakdowns and not has_custom_filter:
            return False

        aggregation = metric.get("aggregation", "avg")
        single_breakdown = self.breakdowns[0] if len(self.breakdowns) == 1 else None
        if self._should_use_rollup(
            metric_name,
            aggregation,
            single_breakdown,
            per_metric_filters,
            self.parse_time_range()[0],
        ):
            return False
        return True

    def _raw_attribute_sampling_plan(self) -> tuple[int, int]:
        """Return a full-window stratum width and a per-stratum row cap."""

        start_date, end_date = self.parse_time_range()
        duration_seconds = max(
            1,
            math.ceil((end_date - start_date).total_seconds()),
        )
        requested_interval = _RAW_ATTRIBUTE_GRANULARITY_SECONDS.get(
            self.granularity,
            _RAW_ATTRIBUTE_GRANULARITY_SECONDS["day"],
        )
        # Use at most 64 explicit time slices. Each slice has its own LIMIT, so
        # dense slices terminate independently instead of relying on LIMIT BY,
        # which limits emitted rows but may still consume the full source.
        interval_seconds = max(
            requested_interval,
            math.ceil(duration_seconds / _RAW_ATTRIBUTE_MAX_STRATA),
        )
        stratum_upper_bound = max(
            1,
            math.ceil(duration_seconds / interval_seconds),
        )
        candidates_per_stratum = max(
            1,
            min(
                _RAW_ATTRIBUTE_CANDIDATES_PER_BUCKET,
                _RAW_ATTRIBUTE_CANDIDATE_LIMIT // stratum_upper_bound,
            ),
        )
        return interval_seconds, candidates_per_stratum

    @staticmethod
    def _raw_attribute_map(attribute_type: str) -> str | None:
        return {
            "text": "attrs_string",
            "string": "attrs_string",
            "number": "attrs_number",
            "boolean": "attrs_bool",
        }.get(attribute_type)

    def _candidate_key_predicates(
        self, metric: dict, bounded_params: dict
    ) -> list[str]:
        """Return key-only predicates implied by the aggregate/filter."""

        predicates: list[str] = []
        seen: set[tuple[str, str]] = set()

        def add(attribute_key: str, attribute_type: str) -> None:
            map_column = self._raw_attribute_map(attribute_type)
            key = str(attribute_key or "")
            if not map_column or not key or (map_column, key) in seen:
                return
            seen.add((map_column, key))
            param = f"_raw_attr_presence_key_{len(seen) - 1}"
            bounded_params[param] = key
            predicates.append(
                f"(indexHint(has(mapKeys({map_column}), %({param})s)) "
                f"AND has({map_column}.keys, %({param})s))"
            )

        if metric.get("type", "system_metric") == "custom_attribute":
            add(
                metric.get("attribute_key") or metric.get("id") or metric.get("name"),
                metric.get("attribute_type", "number"),
            )
        elif metric.get("type", "system_metric") == "system_metric":
            metric_name = (metric.get("id") or metric.get("name") or "").lower()
            if metric_name not in SYSTEM_METRICS:
                add(metric_name, "number")
            elif metric_name == "time_to_first_token":
                add("gen_ai.server.time_to_first_token", "number")
        for breakdown in self.breakdowns:
            if breakdown.get(
                "type", "system_metric"
            ) == "custom_attribute" and breakdown.get("source", "traces") not in {
                "datasets",
                "simulation",
            }:
                add(
                    breakdown.get("attribute_key")
                    or breakdown.get("id")
                    or breakdown.get("name"),
                    breakdown.get("attribute_type", "string"),
                )
        for item in self.global_filters + metric.get("filters", []):
            if item.get("metric_type") != "custom_attribute" or item.get(
                "source", "traces"
            ) not in {"traces", ""}:
                continue
            canonical = item.get("canonical_filter") or {}
            config = canonical.get("filter_config") or {}
            operation = config.get("filter_op") or item.get("operator")
            if operation in {"is_null", "is_not_set"}:
                continue
            attribute_type = normalize_span_attribute_filter_type(
                config.get("filter_type") or item.get("attribute_type", "string"),
                config.get("filter_value"),
            )
            add(
                canonical.get("column_id") or item.get("metric_name"),
                attribute_type,
            )
        return predicates

    def _bound_raw_attribute_input(
        self, sql: str, params: dict, metric: dict
    ) -> tuple[str, dict]:
        """Replace the physical spans source with a finite semantic barrier.

        The identity Set scans only narrow columns plus optional key-only Map
        subcolumns. The surrounding ``SELECT * ... LIMIT`` is intentional: a
        Map-value predicate from the aggregate cannot be pushed through that
        limit, even when a read-only transport strips optimization settings.
        Consequently no raw Map value is hydrated outside the finite Set.
        """

        metric_name = (metric.get("id") or metric.get("name") or "").lower()
        candidate_live_predicate = "is_deleted = 0"
        if (
            metric.get("type", "system_metric") == "system_metric"
            and metric_name == "latency"
        ):
            # The outer latency metric is root-only. Seed roots here too;
            # sampling arbitrary children first could otherwise produce an
            # empty breakdown even when every root carries the attribute.
            candidate_live_predicate += (
                "\n      AND (parent_span_id IS NULL OR parent_span_id = '')"
            )
        bounded_params = dict(params)
        interval_seconds, candidates_per_stratum = self._raw_attribute_sampling_plan()
        bounded_params["_raw_attr_candidates_per_bucket"] = candidates_per_stratum
        bounded_params["_raw_attr_candidate_limit"] = _RAW_ATTRIBUTE_CANDIDATE_LIMIT
        key_predicates = self._candidate_key_predicates(metric, bounded_params)
        if key_predicates:
            candidate_live_predicate += "\n      AND " + "\n      AND ".join(
                key_predicates
            )

        start_date, end_date = self.parse_time_range()
        slice_queries: list[str] = []
        slice_start = start_date
        slice_index = 0
        while slice_start < end_date:
            slice_end = min(
                slice_start + timedelta(seconds=interval_seconds),
                end_date,
            )
            start_param = f"_raw_attr_slice_start_{slice_index}"
            end_param = f"_raw_attr_slice_end_{slice_index}"
            bounded_params[start_param] = slice_start
            bounded_params[end_param] = slice_end
            slice_queries.append(
                f"""SELECT
                project_id,
                trace_id,
                id,
                toUnixTimestamp64Micro(start_time) AS start_time_us,
                _version
            FROM spans
            PREWHERE project_id IN %(project_ids)s
              AND start_time >= %({start_param})s
              AND start_time < %({end_param})s
            WHERE {candidate_live_predicate}
            LIMIT %(_raw_attr_candidates_per_bucket)s"""
            )
            slice_start = slice_end
            slice_index += 1
        if not slice_queries or slice_index > _RAW_ATTRIBUTE_MAX_STRATA:
            raise RuntimeError("raw dashboard sampling slice plan escaped its bound")
        candidate_union = "\n        UNION ALL\n        ".join(
            f"SELECT * FROM ({query}) AS raw_slice_{index}"
            for index, query in enumerate(slice_queries)
        )

        bounded_source = f"""(
    SELECT *
    FROM spans
    PREWHERE (
        project_id,
        trace_id,
        id,
        toUnixTimestamp64Micro(start_time),
        _version
    ) IN (
        SELECT
            project_id,
            trace_id,
            id,
            start_time_us,
            _version
        FROM (
        {candidate_union}
        ) AS raw_candidates
        LIMIT %(_raw_attr_candidate_limit)s
    )
    LIMIT %(_raw_attr_candidate_limit)s
)"""

        if "FROM (SELECT sp.* EXCEPT" in sql:
            source_match = re.search(r"\bFROM\s+spans\s+AS\s+sp\b", sql)
            alias = "sp"
        else:
            source_match = re.search(
                r"\bFROM\s+spans(?:\s+AS\s+(?P<alias>\w+))?\b",
                sql,
            )
            alias = (
                source_match.group("alias")
                if source_match and source_match.group("alias")
                else "spans"
            )
        if source_match is None:
            raise RuntimeError("bounded raw dashboard query has no spans source")
        replacement = f"FROM {bounded_source} AS {alias}"
        bounded_sql = (
            sql[: source_match.start()] + replacement + sql[source_match.end() :]
        )
        return bounded_sql, bounded_params

    def build_metric_query(self, metric: dict) -> tuple[str, dict]:
        uses_sample = self._uses_bounded_raw_attribute_input(metric)
        if uses_sample and self.config.get("allow_sampled") is not True:
            raise InvalidMetricCombinationError(
                "This metric requires an explicitly sampled dashboard read. "
                "Retry with allow_sampled=true."
            )
        sql, params = super().build_metric_query(metric)
        sql = _protect_usage_cdc_columns(sql)
        sql = rewrite_and_apply_v2_settings(sql)
        sql = _restore_usage_cdc_columns(sql)
        # Mixed-table query: rewrite already fixed spans refs, now restore
        # _peerdb_is_deleted for every legacy-table alias.
        for alias in _LEGACY_TABLE_RE.findall(sql):
            sql = sql.replace(f"{alias}.is_deleted", f"{alias}._peerdb_is_deleted")
        if uses_sample:
            sql, params = self._bound_raw_attribute_input(sql, params, metric)
        return sql, params

    def metric_info(self, metric: dict) -> dict:
        info = super().metric_info(metric)
        if self._uses_bounded_raw_attribute_input(metric):
            if self.config.get("allow_sampled") is not True:
                info.update(
                    {
                        "query_complete": False,
                        "query_status": "degraded",
                        "query_error_code": "query_failed",
                    }
                )
                return info
            interval_seconds, candidates_per_stratum = (
                self._raw_attribute_sampling_plan()
            )
            info.update(
                {
                    "query_complete": False,
                    "query_status": "sampled",
                    "query_error_code": "sample_limit",
                    "query_sampling_strategy": _RAW_ATTRIBUTE_SAMPLING_STRATEGY,
                    "query_sampling_interval_seconds": interval_seconds,
                    "query_sample_limit": _RAW_ATTRIBUTE_CANDIDATE_LIMIT,
                    "query_sample_per_bucket": candidates_per_stratum,
                }
            )
        return info


__all__ = ["DashboardQueryBuilderV2"]
