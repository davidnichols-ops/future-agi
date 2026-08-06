"""One-snapshot aggregate Agent Graph/Path query for direct-write ClickHouse.

The graph and path are two views of the same current span set:

* ``edges`` are exact parent -> child topology transitions;
* ``path_edges`` are exact adjacent chronological transitions inside each trace.

Both, together with node metrics, are produced by one ClickHouse statement and
one physical ``spans`` reference.  This matters on ClickHouse 25.3: named CTEs
are expanded at every use, so separate edge/node statements (or a CTE reused by
three UNION branches) can observe different ReplacingMergeTree part snapshots.
The query below collapses every physical identity with ``argMax(_version)``,
applies mutable filters only after that collapse, packs each accepted trace into
one compact array, and emits node/hierarchy/path events through one final
``arrayJoin``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.query_builders.exact_graph_predicates import (
    compile_exact_graph_row_predicates,
)


class AgentGraphQueryBuilder(BaseQueryBuilder):
    """Build one exact latest-state Agent Graph/Path statement."""

    TABLE = "spans"
    VERSION_COLUMN = "_version"
    DELETED_COLUMN = "is_deleted"

    def __init__(
        self,
        project_id: str,
        filters: list[dict] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(project_id, **kwargs)
        self.filters = list(filters or [])
        analyzed = self.analyze_bounded_datetime_filters(self.filters, strict=True)
        self.start_date = analyzed.start
        self.end_date = analyzed.end
        self.empty_window = analyzed.empty
        self.params.update(
            {
                "start_date": self.start_date,
                "end_date": self.end_date,
                # Preserve the established trace-filter contract: ordinary
                # scalar witnesses may sit in the adjacent ingest window,
                # while contribution rows remain inside the selected window.
                "graph_witness_start_date": self.start_date - timedelta(days=1),
                "graph_witness_end_date": self.end_date + timedelta(days=1),
            }
        )

    @staticmethod
    def _make_node_id(name: str, node_type: str) -> str:
        return f"{node_type}:{name}"

    @staticmethod
    def _latest_projection(
        row_predicates: tuple[str, ...],
        contribution_predicates: tuple[str, ...],
    ) -> tuple[str, str, int]:
        """Return argMax tuple expressions, aliases, and tombstone position."""

        expressions = [
            "id",
            "parent_span_id",
            "name",
            "observation_type",
            "start_time",
            "toFloat64(latency_ms)",
            "toInt64(total_tokens)",
            "toFloat64(cost)",
            "status",
            "toUInt8(is_deleted)",
        ]
        aliases = [
            "id",
            "parent_span_id",
            "name",
            "observation_type",
            "start_time",
            "latency_ms",
            "total_tokens",
            "cost",
            "status",
            "is_deleted",
        ]
        for index, predicate in enumerate(row_predicates):
            expressions.append(f"toUInt8(ifNull(({predicate}), 0))")
            aliases.append(f"graph_row_match_{index}")
        for index, predicate in enumerate(contribution_predicates):
            expressions.append(f"toUInt8(ifNull(({predicate}), 0))")
            aliases.append(f"graph_contribution_match_{index}")

        tuple_sql = ",\n".join(
            f"                        {item}" for item in expressions
        )
        projection_sql = ",\n".join(
            f"            tupleElement(graph_latest_row, {index}) AS {alias}"
            for index, alias in enumerate(aliases, start=1)
        )
        return tuple_sql, projection_sql, aliases.index("is_deleted") + 1

    @staticmethod
    def _span_tuple(prefix: str) -> str:
        """Return the scalar tuple used after the wide latest-state collapse."""

        return (
            f"tuple({prefix}id, {prefix}parent_span_id, {prefix}name, "
            f"{prefix}observation_type, toUnixTimestamp64Micro({prefix}start_time), "
            f"toFloat64({prefix}latency_ms), toInt64({prefix}total_tokens), "
            f"toFloat64({prefix}cost), {prefix}status)"
        )

    def build(self) -> tuple[str, dict[str, Any]]:
        """Return one exact node/hierarchy/path aggregation statement."""

        plan = compile_exact_graph_row_predicates(
            self.filters,
            project_id=str(self.project_id),
            # The historical Agent Graph endpoint filters traces, then graphs
            # every contributing child of each matched trace.
            observe_type="trace",
        )
        self.params.update(plan.params)

        tuple_sql, projection_sql, tombstone_index = self._latest_projection(
            plan.predicates,
            plan.contribution_predicates,
        )
        output_window = "start_time >= %(start_date)s AND start_time < %(end_date)s"
        contribution_terms = [
            output_window,
            *(
                f"graph_contribution_match_{index} = 1"
                for index in range(len(plan.contribution_predicates))
            ),
        ]
        contribution_condition = " AND ".join(
            f"({term})" for term in contribution_terms
        )
        match_columns = []
        match_having = []
        for index, required in enumerate(plan.required_matches):
            match_condition = f"graph_row_match_{index} = 1"
            if plan.output_window_only[index]:
                match_condition = f"({match_condition}) AND ({output_window})"
            match_columns.append(
                "            max(toUInt8(ifNull(("
                f"{match_condition}), 0))) AS graph_match_{index}"
            )
            match_having.append(f"graph_match_{index} = {1 if required else 0}")

        trace_projection = ""
        if match_columns:
            trace_projection = ",\n" + ",\n".join(match_columns)
        trace_having = ["length(graph_spans) > 0", *match_having]
        if self.empty_window:
            trace_having.append("0 = 1")
        trace_having_sql = " AND ".join(f"({item})" for item in trace_having)

        span_tuple = self._span_tuple("")
        # Tuple indexes in ``graph_spans``:
        #   1 id, 2 parent id, 3 name, 4 type, 5 start-us,
        #   6 latency, 7 tokens, 8 cost, 9 status.
        node_events = """arrayMap(
                    graph_span -> tuple(
                        'node',
                        tupleElement(graph_span, 3),
                        tupleElement(graph_span, 4),
                        '',
                        '',
                        tupleElement(graph_span, 6),
                        tupleElement(graph_span, 7),
                        tupleElement(graph_span, 8),
                        toUInt8(upper(tupleElement(graph_span, 9)) IN
                            ('ERROR', 'ERRORED', 'FAILED'))
                    ),
                    graph_spans
                )"""
        hierarchy_events = """arrayMap(
                    graph_child -> tuple(
                        'hierarchy',
                        tupleElement(arrayFirst(
                            graph_parent -> tupleElement(graph_parent, 1)
                                = tupleElement(graph_child, 2),
                            graph_spans
                        ), 3),
                        tupleElement(arrayFirst(
                            graph_parent -> tupleElement(graph_parent, 1)
                                = tupleElement(graph_child, 2),
                            graph_spans
                        ), 4),
                        tupleElement(graph_child, 3),
                        tupleElement(graph_child, 4),
                        tupleElement(graph_child, 6),
                        tupleElement(graph_child, 7),
                        tupleElement(graph_child, 8),
                        toUInt8(upper(tupleElement(graph_child, 9)) IN
                            ('ERROR', 'ERRORED', 'FAILED'))
                    ),
                    arrayFilter(
                        graph_child -> tupleElement(graph_child, 2) != ''
                            AND arrayExists(
                                graph_parent -> tupleElement(graph_parent, 1)
                                    = tupleElement(graph_child, 2),
                                graph_spans
                            ),
                        graph_spans
                    )
                )"""
        path_events = """arrayMap(
                    graph_index -> tuple(
                        'path',
                        tupleElement(graph_ordered_spans[graph_index], 3),
                        tupleElement(graph_ordered_spans[graph_index], 4),
                        tupleElement(graph_ordered_spans[graph_index + 1], 3),
                        tupleElement(graph_ordered_spans[graph_index + 1], 4),
                        tupleElement(graph_ordered_spans[graph_index + 1], 6),
                        tupleElement(graph_ordered_spans[graph_index + 1], 7),
                        tupleElement(graph_ordered_spans[graph_index + 1], 8),
                        toUInt8(upper(tupleElement(
                            graph_ordered_spans[graph_index + 1], 9
                        )) IN ('ERROR', 'ERRORED', 'FAILED'))
                    ),
                    range(1, length(graph_ordered_spans))
                )"""

        query = f"""
        WITH graph_latest_spans AS (
            SELECT
                trace_id,
{projection_sql}
            FROM (
                SELECT
                    trace_id,
                    argMax(
                        tuple(
{tuple_sql}
                        ),
                        {self.VERSION_COLUMN}
                    ) AS graph_latest_row
                FROM {self.TABLE}
                PREWHERE {self.project_filter_sql()}
                  AND start_time >= %(graph_witness_start_date)s
                  AND start_time < %(graph_witness_end_date)s
                GROUP BY
                    project_id,
                    observation_type,
                    service_name,
                    toStartOfHour(start_time),
                    trace_id,
                    id
            ) AS graph_physical_versions
            WHERE tupleElement(graph_latest_row, {tombstone_index}) = 0
        ),
        graph_traces AS (
            SELECT
                trace_id,
                groupArrayIf(
                    {span_tuple},
                    {contribution_condition}
                ) AS graph_spans
{trace_projection}
            FROM graph_latest_spans
            GROUP BY trace_id
            HAVING {trace_having_sql}
        ),
        graph_ordered_traces AS (
            SELECT
                trace_id,
                graph_spans,
                arraySort(
                    graph_span -> tuple(
                        tupleElement(graph_span, 5),
                        tupleElement(graph_span, 1)
                    ),
                    graph_spans
                ) AS graph_ordered_spans
            FROM graph_traces
        ),
        graph_events AS (
            SELECT
                trace_id,
                arrayJoin(arrayConcat(
                    {node_events},
                    {hierarchy_events},
                    {path_events}
                )) AS graph_event
            FROM graph_ordered_traces
        )
        SELECT
            tupleElement(graph_event, 1) AS row_kind,
            tupleElement(graph_event, 2) AS source_node,
            tupleElement(graph_event, 3) AS source_type,
            tupleElement(graph_event, 4) AS target_node,
            tupleElement(graph_event, 5) AS target_type,
            count() AS item_count,
            avg(tupleElement(graph_event, 6)) AS avg_latency_ms,
            sum(tupleElement(graph_event, 7)) AS total_tokens,
            sum(tupleElement(graph_event, 8)) AS total_cost,
            sum(tupleElement(graph_event, 9)) AS error_count,
            uniqExact(trace_id) AS trace_count
        FROM graph_events
        GROUP BY
            row_kind,
            source_node,
            source_type,
            target_node,
            target_type
        ORDER BY row_kind, item_count DESC, source_type, source_node,
                 target_type, target_node
        SETTINGS
            max_threads = 1,
            optimize_aggregation_in_order = 1,
            max_bytes_before_external_group_by = 33554432,
            max_bytes_before_external_sort = 33554432
        """
        return query, self.params

    # Kept as a hard failure so a future call site cannot silently reintroduce
    # the old independently-snapshotted second statement.
    def build_node_metrics(self) -> tuple[str, dict[str, Any]]:
        raise RuntimeError("agent graph nodes and edges must use build() together")

    def format_result(
        self,
        rows: list[Any],
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Split the single statement's tagged rows into wire graph payloads."""

        names = list(columns or [])

        def value(row: Any, key: str, index: int, default: Any = 0) -> Any:
            if isinstance(row, dict):
                return row.get(key, default)
            if names and key in names:
                position = names.index(key)
                return row[position] if len(row) > position else default
            return row[index] if len(row) > index else default

        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        path_edges: list[dict[str, Any]] = []
        for row in rows or []:
            kind = str(value(row, "row_kind", 0, ""))
            source_name = str(value(row, "source_node", 1, ""))
            source_type = str(value(row, "source_type", 2, "unknown"))
            target_name = str(value(row, "target_node", 3, ""))
            target_type = str(value(row, "target_type", 4, "unknown"))
            count = int(value(row, "item_count", 5, 0) or 0)
            avg_latency = float(value(row, "avg_latency_ms", 6, 0) or 0)
            total_tokens = int(value(row, "total_tokens", 7, 0) or 0)
            total_cost = float(value(row, "total_cost", 8, 0) or 0)
            error_count = int(value(row, "error_count", 9, 0) or 0)
            trace_count = int(value(row, "trace_count", 10, 0) or 0)

            source_id = self._make_node_id(source_name, source_type)
            if kind == "node":
                nodes.append(
                    {
                        "id": source_id,
                        "name": source_name,
                        "type": source_type,
                        "span_count": count,
                        "avg_latency_ms": round(avg_latency, 2),
                        "total_tokens": total_tokens,
                        "total_cost": round(total_cost, 6),
                        "error_count": error_count,
                        "trace_count": trace_count,
                    }
                )
                continue
            if kind not in {"hierarchy", "path"}:
                continue
            target_id = self._make_node_id(target_name, target_type)
            edge = {
                "source": source_id,
                "target": target_id,
                "transition_count": count,
                "avg_latency_ms": round(avg_latency, 2),
                "total_tokens": total_tokens,
                "total_cost": round(total_cost, 6),
                "error_count": error_count,
                "trace_count": trace_count,
                "is_self_loop": source_id == target_id,
            }
            (edges if kind == "hierarchy" else path_edges).append(edge)

        return {"nodes": nodes, "edges": edges, "path_edges": path_edges}
