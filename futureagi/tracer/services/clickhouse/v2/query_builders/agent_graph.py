"""Direct-write CH25 agent-graph query builder."""

from tracer.services.clickhouse.query_builders.agent_graph import (
    AgentGraphQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders._rewrite import V2RewriteMixin
from tracer.services.clickhouse.v2.query_builders.filters import (
    ClickHouseFilterBuilderV2,
)


class AgentGraphQueryBuilderV2(V2RewriteMixin, AgentGraphQueryBuilder):
    """Compile graph topology and attribute filters for the CH25 schema."""

    _FILTER_BUILDER_CLS = ClickHouseFilterBuilderV2


__all__ = ["AgentGraphQueryBuilderV2"]
