"""
v2 VoiceCallList query builder — targets the CH 25.3 spans schema.

Subclass + post-rewrite. Voice calls are LLM agent calls with a specific
attribute shape (call.total_turns, call.talk_ratio, etc.) — these live in
`attrs_number` in v2 (was `span_attr_num` in v1) and are queried heavily
by the voice observability surface. `V2RewriteMixin` routes every inherited
`build*` method's SQL through the v2 rewriter at one boundary.

`build_eval_query` resolves the configured legacy-or-direct-write eval table
itself; `build_annotation_query` reads `model_hub_score`. Both are excluded
from the span-schema token rewrite because neither query targets `spans`.
"""

from __future__ import annotations

from tracer.services.clickhouse.query_builders.voice_call_list import (
    VoiceCallListQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders._rewrite import V2RewriteMixin
from tracer.services.clickhouse.v2.query_builders.filters import (
    ClickHouseFilterBuilderV2,
)


class VoiceCallListQueryBuilderV2(V2RewriteMixin, VoiceCallListQueryBuilder):
    """Drop-in v2 VoiceCallList builder."""

    _v2_rewrite_exclude = frozenset({"build_eval_query", "build_annotation_query"})
    _FILTER_BUILDER_CLS = ClickHouseFilterBuilderV2


__all__ = ["VoiceCallListQueryBuilderV2"]
