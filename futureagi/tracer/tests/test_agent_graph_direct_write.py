"""Direct-write and filtered-SQL contracts for the agent graph endpoint."""

from inspect import unwrap
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from clickhouse_driver.errors import ServerException

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


def _call_agent_graph(monkeypatch, *, side_effect=None):
    from tracer.views import trace as trace_view

    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = object()
    analytics = MagicMock()
    if side_effect is None:
        analytics.execute_ch_query.return_value = SimpleNamespace(data=[], columns=[])
    else:
        analytics.execute_ch_query.side_effect = side_effect

    v2_factory = MagicMock(return_value=analytics)
    legacy_factory = MagicMock(
        side_effect=AssertionError("legacy ClickHouse service must not be used")
    )
    monkeypatch.setattr(
        trace_view, "_project_queryset_for_request", lambda _request: project_scope
    )
    monkeypatch.setattr(trace_view, "V2AnalyticsQueryService", v2_factory)
    monkeypatch.setattr(trace_view, "AnalyticsQueryService", legacy_factory)

    request = SimpleNamespace(
        validated_query_data={
            "project_id": PROJECT_ID,
            "filters": [FINAL_STATUS_FILTER],
        }
    )
    view = trace_view.TraceView()
    view.request = request
    response = unwrap(trace_view.TraceView.agent_graph)(view, request)
    return response, analytics, v2_factory, legacy_factory


@pytest.mark.unit
def test_agent_graph_filter_uses_physical_v2_table_and_one_tenant_scope(monkeypatch):
    response, analytics, v2_factory, legacy_factory = _call_agent_graph(monkeypatch)

    assert response.status_code == 200
    v2_factory.assert_called_once_with()
    legacy_factory.assert_not_called()
    assert analytics.execute_ch_query.call_count == 2

    edge_sql = analytics.execute_ch_query.call_args_list[0].args[0]
    node_sql = analytics.execute_ch_query.call_args_list[1].args[0]
    for sql in (edge_sql, node_sql):
        assert "WITH filtered_trace_ids AS" in sql
        assert "FROM spans" in sql
        assert "project_id = %(project_id)s" in sql
        assert "attrs_string" in sql
        assert "span_attr_str" not in sql
        assert "created_at < %(end_date)s + INTERVAL 1 DAY" in sql

    # Regression guard for the customer-visible filtered graph failure: the
    # old builder emitted an impossible subquery against the edge alias.
    assert "FROM child" not in edge_sql
    assert "child.trace_id IN (SELECT trace_id FROM filtered_trace_ids)" in edge_sql
    assert "spans.trace_id IN (SELECT trace_id FROM filtered_trace_ids)" in node_sql


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
    response, _analytics, _v2_factory, legacy_factory = _call_agent_graph(
        monkeypatch, side_effect=failure
    )

    assert response.status_code == expected_status
    legacy_factory.assert_not_called()
    assert "private" not in str(response.data)
    if expected_status == 503:
        assert response.data["code"] == "service_unavailable"
    else:
        assert response.data["code"] == "server_error"
