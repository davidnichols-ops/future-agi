"""Regression coverage for dashboard ClickHouse error boundaries."""

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver.errors import ServerException

from tracer.models.dashboard import Dashboard, DashboardWidget
from tracer.services.clickhouse.v2.query_builders.dashboard import (
    DashboardQueryBuilderV2,
)
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


DIRECT_WRITE_ROUTING_CONFIGS = (
    pytest.param({}, id="routing-missing"),
    pytest.param(
        {"QUERY_TYPES_DISABLED": "dashboard"},
        id="routing-disabled",
    ),
    pytest.param(
        {
            "QUERY_TYPES_V2_ONLY": "trace_list",
            "QUERY_TYPES_SHADOW": "dashboard",
        },
        id="routing-misconfigured-shadow",
    ),
)


@pytest.mark.django_db
@pytest.mark.parametrize("routing_config", DIRECT_WRITE_ROUTING_CONFIGS)
def test_dashboard_query_uses_direct_write_backend_independent_of_routing(
    routing_config,
    settings,
    auth_client,
    observe_project,
):
    settings.CLICKHOUSE_V2 = routing_config
    v2_client = MagicMock()
    v2_client.execute_read.return_value = ([], [], 1.0)

    with (
        patch(
            "tracer.services.clickhouse.v2.query_service.get_v2_query_client",
            return_value=v2_client,
        ),
        patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            side_effect=AssertionError("dashboard dispatch must not be consulted"),
        ) as dispatch,
        patch(
            "tracer.views.dashboard.AnalyticsQueryService",
            side_effect=AssertionError("legacy analytics must not be constructed"),
        ) as legacy_analytics,
        patch(
            "tracer.views.dashboard.DashboardQueryBuilderV2",
            wraps=DashboardQueryBuilderV2,
        ) as v2_builder,
        patch(
            "tracer.views.dashboard.read_or_schedule_exact_snapshot",
            side_effect=lambda _namespace, _identity, **kwargs: kwargs[
                "pending_payload"
            ],
        ) as exact_snapshot,
    ):
        response = auth_client.post(
            "/tracer/dashboard/query/",
            _trace_query(observe_project.id),
            format="json",
        )

    assert response.status_code == 200
    assert response.json()["result"]["query_status"] == "pending"
    assert not v2_client.execute_read.called
    v2_builder.assert_not_called()
    exact_snapshot.assert_called_once()
    dispatch.assert_not_called()
    legacy_analytics.assert_not_called()


@pytest.mark.django_db
@pytest.mark.parametrize("routing_config", DIRECT_WRITE_ROUTING_CONFIGS)
@pytest.mark.parametrize("action", ("execute", "preview"))
def test_widget_trace_queries_use_direct_write_backend_independent_of_routing(
    action,
    routing_config,
    settings,
    auth_client,
    dashboard,
    dashboard_widget,
    observe_project,
):
    settings.CLICKHOUSE_V2 = routing_config
    query_config = _trace_query(observe_project.id)
    dashboard_widget.query_config = query_config
    dashboard_widget.save(update_fields=["query_config"])

    v2_client = MagicMock()
    v2_client.execute_read.return_value = ([], [], 1.0)

    with (
        patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True),
        patch(
            "tracer.services.clickhouse.v2.query_service.get_v2_query_client",
            return_value=v2_client,
        ),
        patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            side_effect=AssertionError("dashboard dispatch must not be consulted"),
        ) as dispatch,
        patch(
            "tracer.views.dashboard.AnalyticsQueryService",
            side_effect=AssertionError("legacy analytics must not be constructed"),
        ) as legacy_analytics,
        patch(
            "tracer.views.dashboard.get_clickhouse_client",
            side_effect=AssertionError("legacy client must not be constructed"),
        ) as legacy_client,
        patch(
            "tracer.views.dashboard.DashboardQueryBuilderV2",
            wraps=DashboardQueryBuilderV2,
        ) as v2_builder,
        patch(
            "tracer.views.dashboard.read_or_schedule_exact_snapshot",
            side_effect=lambda _namespace, _identity, **kwargs: kwargs[
                "pending_payload"
            ],
        ) as exact_snapshot,
        patch("tracer.views.dashboard.read_exact_snapshot", return_value=None),
    ):
        if action == "execute":
            response = auth_client.post(
                f"/tracer/dashboard/{dashboard.id}/widgets/{dashboard_widget.id}/query/"
            )
        else:
            response = auth_client.post(
                f"/tracer/dashboard/{dashboard.id}/widgets/preview/",
                {"query_config": query_config},
                format="json",
            )

    assert response.status_code == 200
    assert response.json()["result"]["query_status"] == "pending"
    assert not v2_client.execute_read.called
    v2_builder.assert_not_called()
    exact_snapshot.assert_called_once()
    dispatch.assert_not_called()
    legacy_analytics.assert_not_called()
    legacy_client.assert_not_called()


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
def test_system_filter_values_programming_defects_preserve_sanitized_500(
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

    assert response.status_code == 500
    payload = json.dumps(response.json())
    assert "private" not in payload
    assert "missing-column" not in payload
    assert "compiler invariant" not in payload


@pytest.mark.django_db
@patch("tracer.views.dashboard.is_clickhouse_enabled", return_value=True)
@patch("tracer.views.dashboard.V2AnalyticsQueryService")
def test_system_filter_values_read_budget_is_sanitized_503(
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

    assert response.status_code == 503
    payload = json.dumps(response.json())
    assert "temporarily unavailable" in payload
    assert "private" not in payload
    assert "timeout query" not in payload


@pytest.mark.django_db
@pytest.mark.parametrize(
    "failure",
    [
        ServerException("private missing-column query", code=47),
        RuntimeError("private dashboard compiler invariant"),
        ServerException("private timeout query", code=159),
    ],
)
@patch("tracer.views.dashboard.V2AnalyticsQueryService")
def test_dashboard_poll_defers_clickhouse_failures_to_exact_worker(
    mock_analytics_cls,
    failure,
    auth_client,
    observe_project,
):
    mock_analytics_cls.return_value.execute_ch_query.side_effect = failure

    with patch(
        "tracer.views.dashboard.read_or_schedule_exact_snapshot",
        side_effect=lambda _namespace, _identity, **kwargs: kwargs["pending_payload"],
    ):
        response = auth_client.post(
            "/tracer/dashboard/query/",
            _trace_query(observe_project.id),
            format="json",
        )

    assert response.status_code == 200
    assert response.json()["result"]["query_status"] == "pending"
    mock_analytics_cls.assert_not_called()
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
