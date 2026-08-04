"""
v2 EvalMetrics query builder — targets the CH 25.3 spans schema.

Subclass + post-rewrite. EvalMetrics powers the eval scoreboard panels
(pass-rate by config, by span type, etc.). It JOINs spans to
tracer_eval_logger. `V2RewriteMixin` routes the inherited `build()` SQL through
the v2 rewriter at one boundary.
"""

from __future__ import annotations

from tracer.services.clickhouse.eval_logger_table import eval_logger_source
from tracer.services.clickhouse.query_builders.eval_metrics import (
    EvalMetricsQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders._rewrite import V2RewriteMixin


class EvalMetricsQueryBuilderV2(V2RewriteMixin, EvalMetricsQueryBuilder):
    """Direct-write eval metrics builder for the CH25 topology.

    The legacy ``eval_metrics_hourly`` rollup is fed from the PeerDB eval
    logger and is therefore not authoritative after direct-write cutover.
    Keep the graph on the config/time-pruned authoritative raw table selected
    by ``CH25_EVAL_LOGGER_TABLE`` until a schema-compatible rollup can preserve
    the same average-score contract. The surrounding spans read and connection
    still use the CH25/V2 path. This is a read-routing change only; it requires
    no DDL.
    """

    _EVAL_LOGGER_SOURCE = staticmethod(eval_logger_source)
    # Eval logger trace IDs are UUID-typed, while the direct spans table stores
    # dashed UUIDs as String. Compare their textual forms so filtered eval
    # graphs do not fail with NO_COMMON_TYPE (Code 386).
    _EVAL_TRACE_ID_EXPR = "toString(raw_eval_logger.trace_id)"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_preaggregated = False

    def _filter_fragment(self) -> str:
        """Fail closed if a caller bypasses bounded filtered graph dispatch."""
        if not self.filters:
            return ""
        raise ValueError("Filtered eval graphs must use the bounded graph dispatcher")


__all__ = ["EvalMetricsQueryBuilderV2"]
