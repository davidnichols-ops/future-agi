"""Regression coverage for dashboard ClickHouse error boundaries."""

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver.errors import ServerException

from tracer.models.dashboard import Dashboard, DashboardWidget
from tracer.views.dashboard import DashboardViewSet, DashboardWidgetViewSet


@pytest.fixture
def dashboard(db, workspace, user):
    return Dashboard.objects.create(
        workspace=workspace,
        name="Boundary Dashboard",
        created_by=user,
        updated_by=user,
    )


@pytest.fixture
def dashboard_widget(db, dashboard, user):
    return DashboardWidget.objects.create(
        dashboard=dashboard,
        name="Boundary Widget",
        position=0,
        width=6,
        height=4,
        query_config={
            "project_ids": [str(uuid.uuid4())],
            "granularity": "day",
            "time_range": {"preset": "7D"},
            "metrics": [
                {
                    "id": "latency",
                    "name": "latency",
                    "type": "system_metric",
                    "aggregation": "avg",
                }
            ],
        },
        chart_config={"chart_type": "line"},
        created_by=user,
    )


def _trace_query(project_id):
    return {
        "project_ids": [str(project_id)],
        "granularity": "day",
        "time_range": {"preset": "7D"},
        "metrics": [
            {
                "id": "latency",
                "name": "latency",
                "type": "system_metric",
                "aggregation": "avg",
            }
        ],
    }


@pytest.mark.django_db
@pytest.mark.parametrize(
    "failure",
    [
        ServerException("private missing-column query", code=47),
        RuntimeError("private dashboard compiler invariant"),
    ],
)
@patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True)
@patch("tracer.views.dashboard.V2AnalyticsQueryService")
def test_system_filter_values_programming_defects_preserve_sanitized_400(
    mock_analytics_cls,
    _mock_ch_enabled,
    failure,
    auth_client,
    observe_project,
):
    mock_analytics_cls.return_value.execute_ch_query.side_effect = failure

    response = auth_client.get(
        "/tracer/dashboard/filter_values/"
        "?metric_name=model&metric_type=system_metric"
        f"&project_ids={observe_project.id}&source=traces"
    )

    assert response.status_code == 400
    payload = json.dumps(response.json())
    assert "private" not in payload
    assert "missing-column" not in payload
    assert "compiler invariant" not in payload


@pytest.mark.django_db
@patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True)
@patch("tracer.views.dashboard.V2AnalyticsQueryService")
def test_system_filter_values_read_budget_is_explicit_degraded_200(
    mock_analytics_cls,
    _mock_ch_enabled,
    auth_client,
    observe_project,
):
    mock_analytics_cls.return_value.execute_ch_query.side_effect = ServerException(
        "private timeout query", code=159
    )

    response = auth_client.get(
        "/tracer/dashboard/filter_values/"
        "?metric_name=model&metric_type=system_metric"
        f"&project_ids={observe_project.id}&source=traces"
    )

    assert response.status_code == 200
    payload = response.json()["result"]
    assert payload == {
        "values": [],
        "query_complete": False,
        "query_status": "degraded",
        "query_error_code": "read_budget_exceeded",
    }
    assert "private" not in json.dumps(response.json())


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (ServerException("private missing-column query", code=47), 400),
        (RuntimeError("private dashboard compiler invariant"), 400),
        (ServerException("private timeout query", code=159), 503),
    ],
)
@patch("tracer.views.dashboard.V2AnalyticsQueryService")
def test_dashboard_query_does_not_mask_clickhouse_failures(
    mock_analytics_cls,
    failure,
    expected_status,
    auth_client,
    observe_project,
):
    mock_analytics_cls.return_value.execute_ch_query.side_effect = failure

    response = auth_client.post(
        "/tracer/dashboard/query/",
        _trace_query(observe_project.id),
        format="json",
    )

    assert response.status_code == expected_status
    payload = json.dumps(response.json())
    assert "private" not in payload
    assert "missing-column" not in payload
    assert "compiler invariant" not in payload
    assert "timeout query" not in payload


def test_metric_query_programming_defect_propagates():
    builder = MagicMock()
    metric = {"name": "latency"}
    builder.metrics = [metric]
    builder.metric_info.return_value = {"name": "latency"}
    builder.build_metric_query.return_value = ("SELECT broken", {})

    def fail(_sql, _params):
        raise RuntimeError("dashboard compiler invariant")

    with pytest.raises(RuntimeError, match="compiler invariant"):
        DashboardViewSet._run_metric_queries(builder, "traces", fail)


@pytest.mark.django_db
@patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True)
@patch.object(DashboardWidgetViewSet, "_execute_ch_query_config")
def test_widget_query_programming_defect_preserves_sanitized_400(
    mock_execute,
    _mock_ch_enabled,
    auth_client,
    dashboard,
    dashboard_widget,
):
    mock_execute.side_effect = RuntimeError("private widget compiler invariant")

    response = auth_client.post(
        f"/tracer/dashboard/{dashboard.id}/widgets/{dashboard_widget.id}/query/"
    )

    assert response.status_code == 400
    assert "private" not in json.dumps(response.json())


@pytest.mark.django_db
@patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True)
@patch.object(DashboardWidgetViewSet, "_execute_ch_query_config")
def test_widget_preview_programming_defect_preserves_sanitized_400(
    mock_execute,
    _mock_ch_enabled,
    auth_client,
    dashboard,
    observe_project,
):
    mock_execute.side_effect = RuntimeError("private preview compiler invariant")

    response = auth_client.post(
        f"/tracer/dashboard/{dashboard.id}/widgets/preview/",
        {"query_config": _trace_query(observe_project.id)},
        format="json",
    )

    assert response.status_code == 400
    assert "private" not in json.dumps(response.json())
