"""Exact direct-write and async-snapshot contracts for Agent Graph/Path."""

from inspect import unwrap
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from clickhouse_driver.errors import ServerException

from tracer.services.clickhouse.v2.query_builders.agent_graph import (
    AgentGraphQueryBuilderV2,
)

PROJECT_ID = "11111111-1111-4111-8111-111111111111"
FINAL_STATUS_FILTER = {
    "column_id": "final_status",
    "display_name": "final_status",
    "filter_config": {
        "filter_type": "text",
        "filter_op": "in",
        "filter_value": ["Rechazado"],
        "col_type": "SPAN_ATTRIBUTE",
    },
}
PROFILE_FILTER = {
    "column_id": "profile",
    "filter_config": {
        "filter_type": "map",
        "filter_op": "contains",
        "filter_value": {"tier": "gold", "enabled": True},
        "col_type": "SPAN_ATTRIBUTE",
    },
}


def _complete_payload():
    return {
        "nodes": [],
        "edges": [],
        "path_edges": [],
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }


def _call_agent_graph(monkeypatch, *, side_effect=None, refresh=False):
    from tracer.views import trace as trace_view

    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = object()
    fetch = MagicMock(return_value=_complete_payload())
    if side_effect is not None:
        fetch.side_effect = side_effect
    monkeypatch.setattr(
        trace_view, "_project_queryset_for_request", lambda _request: project_scope
    )
    monkeypatch.setattr(trace_view, "fetch_agent_graph_ch", fetch)
    monkeypatch.setattr(
        trace_view,
        "bind_request_my_annotations_principal",
        lambda filters, *, request: filters,
    )

    request = SimpleNamespace(
        validated_query_data={
            "project_id": PROJECT_ID,
            "filters": [FINAL_STATUS_FILTER],
            "refresh": refresh,
        }
    )
    view = trace_view.TraceView()
    view.request = request
    response = unwrap(trace_view.TraceView.agent_graph)(view, request)
    return response, fetch


@pytest.mark.unit
def test_agent_graph_http_path_schedules_exact_snapshot_without_sync_ch(monkeypatch):
    response, fetch = _call_agent_graph(monkeypatch, refresh=True)

    assert response.status_code == 200
    assert response.data["result"] == _complete_payload()
    fetch.assert_called_once_with(
        project_id=PROJECT_ID,
        filters=[FINAL_STATUS_FILTER],
        refresh=True,
    )


@pytest.mark.unit
def test_agent_graph_is_one_latest_state_v2_statement_for_all_outputs():
    builder = AgentGraphQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[FINAL_STATUS_FILTER, PROFILE_FILTER],
    )
    query, params = builder.build()

    assert query.count("FROM spans") == 1
    assert "argMax(" in query
    assert "_version" in query
    assert "AS graph_physical_versions" in query
    assert "WHERE tupleElement(graph_latest_row," in query
    assert "FROM spans FINAL" not in query
    assert "attrs_string" in query
    assert "attributes_extra" in query
    assert "span_attr_str" not in query
    assert "span_attributes_raw" not in query
    assert "arrayJoin(arrayConcat(" in query
    assert "'node'" in query
    assert "'hierarchy'" in query
    assert "'path'" in query
    assert "max_threads = 1" in query
    assert params["project_id"] == PROJECT_ID

    # Mutable Map/JSON values are consumed inside argMax. They must not be
    # applied as PREWHERE predicates where an old matching version could hide
    # a newer correction or tombstone.
    prewhere = query.split("PREWHERE", 1)[1].split("GROUP BY", 1)[0]
    assert "attrs_string" not in prewhere
    assert "attributes_extra" not in prewhere
    collapse_suffix = query.split(") AS graph_physical_versions", 1)[1]
    assert "attrs_string" not in collapse_suffix
    assert "attributes_extra" not in collapse_suffix


@pytest.mark.unit
def test_agent_graph_formatter_separates_topology_and_chronological_path():
    builder = AgentGraphQueryBuilderV2(project_id=PROJECT_ID, filters=[])
    payload = builder.format_result(
        [
            {
                "row_kind": "node",
                "source_node": "agent",
                "source_type": "agent",
                "target_node": "",
                "target_type": "",
                "item_count": 4,
                "avg_latency_ms": 12.5,
                "total_tokens": 8,
                "total_cost": 0.25,
                "error_count": 1,
                "trace_count": 3,
            },
            {
                "row_kind": "hierarchy",
                "source_node": "agent",
                "source_type": "agent",
                "target_node": "lookup",
                "target_type": "tool",
                "item_count": 2,
                "avg_latency_ms": 5,
                "total_tokens": 0,
                "total_cost": 0,
                "error_count": 0,
                "trace_count": 2,
            },
            {
                "row_kind": "path",
                "source_node": "lookup",
                "source_type": "tool",
                "target_node": "answer",
                "target_type": "llm",
                "item_count": 2,
                "avg_latency_ms": 10,
                "total_tokens": 8,
                "total_cost": 0.25,
                "error_count": 1,
                "trace_count": 2,
            },
        ],
        [],
    )

    assert payload["nodes"] == [
        {
            "id": "agent:agent",
            "name": "agent",
            "type": "agent",
            "span_count": 4,
            "avg_latency_ms": 12.5,
            "total_tokens": 8,
            "total_cost": 0.25,
            "error_count": 1,
            "trace_count": 3,
        }
    ]
    assert payload["edges"][0]["source"] == "agent:agent"
    assert payload["edges"][0]["target"] == "tool:lookup"
    assert payload["path_edges"][0]["source"] == "tool:lookup"
    assert payload["path_edges"][0]["target"] == "llm:answer"


@pytest.mark.unit
def test_exact_agent_graph_reader_executes_only_one_statement():
    from tracer.services.clickhouse.exact_graph_reads import read_exact_agent_graph

    analytics = MagicMock()
    analytics.execute_ch_query.return_value = SimpleNamespace(
        data=[], columns=[], row_count=0
    )
    result = read_exact_agent_graph(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[FINAL_STATUS_FILTER],
    )

    assert analytics.execute_ch_query.call_count == 1
    call = analytics.execute_ch_query.call_args
    assert call.kwargs["settings"]["max_threads"] == 1
    assert call.args[0].count("FROM spans") == 1
    assert result["query_complete"] is True
    assert result["query_status"] == "complete"
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_exact_aggregation_worker_routes_agent_graph_without_interval(monkeypatch):
    from tracer.services.clickhouse import exact_graph_reads
    from tracer.services.clickhouse.v2 import query_service
    from tracer.tasks import exact_aggregation

    analytics = object()
    reader = MagicMock(return_value=_complete_payload())
    monkeypatch.setattr(query_service, "V2AnalyticsQueryService", lambda: analytics)
    monkeypatch.setattr(exact_graph_reads, "read_exact_agent_graph", reader)

    payload = exact_aggregation._observe_payload(
        "observe-agent-graph",
        {"project_id": PROJECT_ID, "filters": [FINAL_STATUS_FILTER]},
    )

    assert payload == _complete_payload()
    reader.assert_called_once_with(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[FINAL_STATUS_FILTER],
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (ServerException("private timeout SQL", code=159), 503),
        (ServerException("private memory SQL", code=241), 503),
        (ServerException("private byte-limit SQL", code=307), 503),
        (ServerException("private heterogeneous SQL", code=386), 503),
        (ServerException("private unknown-column SQL", code=47), 500),
    ],
    ids=["code-159", "code-241", "code-307", "code-386", "code-47"],
)
def test_agent_graph_error_boundary_is_typed_and_sanitized(
    monkeypatch, failure, expected_status
):
    response, _fetch = _call_agent_graph(monkeypatch, side_effect=failure)

    assert response.status_code == expected_status
    assert "private" not in str(response.data)
    if expected_status == 503:
        assert response.data["code"] == "service_unavailable"
    else:
        assert response.data["code"] == "server_error"
