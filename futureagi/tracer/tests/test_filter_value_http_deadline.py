"""Unit wiring for the request-owned system filter-value deadline."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

from tracer.services.clickhouse.filter_value_reads import FilterValueCursorPageRead
from tracer.views import dashboard as dashboard_view


def test_system_filter_value_cursor_receives_view_owned_deadline(monkeypatch):
    project_id = "00000000-0000-4000-8000-000000000001"
    events = []
    request_deadline = object()
    deadline_start = Mock(
        side_effect=lambda _total_ms: events.append("deadline") or request_deadline
    )

    class ProjectScope:
        def values_list(self, *_args, **_kwargs):
            events.append("project_scope")
            return [project_id]

    retained_start = datetime(2026, 8, 1, 11, 55, tzinfo=UTC)
    selector = Mock()
    selector.retained_window_start.return_value = retained_start
    page_read = FilterValueCursorPageRead(
        values=("customer-ended-call",),
        query_window_start=retained_start,
        query_window_end=retained_start + timedelta(minutes=5),
        has_more=False,
        next_segment_end=retained_start,
        next_segment_start=None,
        next_value_after=None,
        seen_value_digests=(),
        browse_status="exhausted",
    )

    def read_cursor_page(*_args, **kwargs):
        events.append("selector")
        assert kwargs["deadline"] is request_deadline
        return page_read

    monkeypatch.setattr(
        dashboard_view,
        "ReadDeadline",
        SimpleNamespace(start=deadline_start),
    )
    monkeypatch.setattr(
        dashboard_view,
        "project_queryset_for_request",
        lambda _request: ProjectScope(),
    )
    monkeypatch.setattr(
        dashboard_view,
        "cursor_scope_for_request",
        lambda *_args, **_kwargs: {"principal": "unit"},
    )
    monkeypatch.setattr(
        dashboard_view,
        "AttributeReadSelector",
        lambda **_kwargs: selector,
    )
    monkeypatch.setattr(
        dashboard_view,
        "V2AnalyticsQueryService",
        lambda: object(),
    )
    monkeypatch.setattr(
        dashboard_view,
        "read_span_system_filter_value_cursor_page",
        read_cursor_page,
    )

    request = SimpleNamespace(
        validated_query_data={
            "metric_name": "ended_reason",
            "metric_type": "system_metric",
            "source": "traces",
            "project_ids": [project_id],
            "search": "",
            "page_size": 20,
        },
        workspace=SimpleNamespace(id="00000000-0000-4000-8000-000000000002"),
    )
    response = dashboard_view.DashboardViewSet.filter_values.__wrapped__(
        dashboard_view.DashboardViewSet(), request
    )

    assert response.status_code == 200
    assert response.data["result"]["values"] == [
        {"value": "customer-ended-call", "label": "customer-ended-call"}
    ]
    deadline_start.assert_called_once_with(
        dashboard_view._FILTER_VALUES_INTERACTIVE_TIMEOUT_MS
    )
    assert dashboard_view._FILTER_VALUES_INTERACTIVE_TIMEOUT_MS == 6_000
    assert events == ["deadline", "project_scope", "selector"]
