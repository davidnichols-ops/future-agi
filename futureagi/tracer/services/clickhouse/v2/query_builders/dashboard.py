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
can't make that per-metric distinction — it would rename `_peerdb_is_deleted` →
`is_deleted` on the legacy tables too. So both dispatch methods are excluded
from the mixin and the rewrite is applied here, per metric. For legacy-table
metrics that JOIN onto spans (breakdowns/filters by trace dimensions), the
full rewrite is applied first (fixing spans refs like `s._peerdb_is_deleted` →
`s.is_deleted`, `s.span_attr_str` → `s.attrs_string`), then the legacy-table
aliases are restored (e.g. `e.is_deleted` → `e._peerdb_is_deleted`).
"""

from __future__ import annotations

import re

from tracer.services.clickhouse.query_builders.dashboard import DashboardQueryBuilder
from tracer.services.clickhouse.v2.query_builders._rewrite import V2RewriteMixin
from tracer.services.clickhouse.v2.query_builders.filters import (
    rewrite_and_apply_v2_settings,
)

# Metric types whose SQL reads tables NOT migrated to the CH 25.3 spans schema.
_LEGACY_TABLE_METRIC_TYPES = frozenset({"eval_metric", "annotation_metric"})

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
    module docstring). `build_metric_query` applies the rewrite itself, per
    metric:

    * Non-legacy metrics (system_metric, custom_attribute): full rewrite.
    * Legacy metrics (eval_metric, annotation_metric): full rewrite first
      (so spans-JOINed refs like ``s._peerdb_is_deleted`` become
      ``s.is_deleted``), then legacy-table aliases are restored
      (``e.is_deleted`` → ``e._peerdb_is_deleted``).
    """

    # dashboard_attr_rollup ships only in the v2 schema, so the fast-path is safe only here.
    _attr_rollup_available: bool = True

    _v2_rewrite_exclude = frozenset({"build_metric_query", "build_all_queries"})

    def build_metric_query(self, metric: dict) -> tuple[str, dict]:
        sql, params = super().build_metric_query(metric)
        if metric.get("type") not in _LEGACY_TABLE_METRIC_TYPES:
            return rewrite_and_apply_v2_settings(sql), params
        sql = _protect_usage_cdc_columns(sql)
        sql = rewrite_and_apply_v2_settings(sql)
        sql = _restore_usage_cdc_columns(sql)
        # Mixed-table query: rewrite already fixed spans refs, now restore
        # _peerdb_is_deleted for every legacy-table alias.
        for alias in _LEGACY_TABLE_RE.findall(sql):
            sql = sql.replace(f"{alias}.is_deleted", f"{alias}._peerdb_is_deleted")
        return sql, params


__all__ = ["DashboardQueryBuilderV2"]
