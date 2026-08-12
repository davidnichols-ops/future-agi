import csv
import io
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from rest_framework import status

from tracer.views.observation_span import ObservationSpanView
from tracer.views.trace import TraceView
from tracer.views.trace_session import TraceSessionView

pytestmark = pytest.mark.unit


def _request(query_params):
    request = MagicMock()
    request.query_params = query_params
    request.validated_query_data = {}
    request.user.organization = SimpleNamespace(id="org-1")
    return request


def _rows(response):
    body = (
        b"".join(response.streaming_content)
        if getattr(response, "streaming", False)
        else response.content
    )
    return list(csv.reader(io.StringIO(body.decode())))


def test_trace_export_returns_bounded_csv_and_marks_partial_page():
    request = _request({"project_id": "00000000-0000-0000-0000-000000000001"})
    project = SimpleNamespace(name="Observe")
    page = SimpleNamespace(
        status_code=status.HTTP_200_OK,
        data={
            "result": {
                "table": [{"trace_id": "trace-1", "cost": 1.25}],
                "metadata": {"has_more": True, "total_rows_is_lower_bound": True},
            }
        },
    )

    with (
        patch("tracer.views.trace._project_queryset_for_request") as projects,
        patch.object(TraceView, "list_traces_of_session", return_value=page) as listing,
    ):
        projects.return_value.filter.return_value.first.return_value = project
        response = TraceView().get_trace_export_data(request)

    assert response.status_code == status.HTTP_200_OK
    assert (
        response["Content-Disposition"] == 'attachment; filename="Observe_traces.csv"'
    )
    assert _rows(response) == [
        ["trace_id", "cost"],
        ["trace-1", "1.25"],
        [
            "# export truncated after 1 rows; refine filters to export a complete bounded page"
        ],
    ]
    list_request = listing.call_args.args[0]
    assert listing.call_args.kwargs == {"bounded_export": True}
    assert list_request is request


def test_bounded_csv_cells_are_stable_and_formula_safe():
    from tracer.utils.bounded_csv import bounded_page_csv_response

    response = bounded_page_csv_response(
        rows=[
            {
                "payload": {"z": 2, "a": [1, True]},
                "created_at": datetime(2026, 8, 11, 12, 30, tzinfo=UTC),
                "customer": "=SUM(1,1)",
                "empty": None,
            }
        ],
        filename="safe.csv",
    )

    assert _rows(response) == [
        ["payload", "created_at", "customer", "empty"],
        [
            '{"a":[1,true],"z":2}',
            "2026-08-11T12:30:00+00:00",
            "'=SUM(1,1)",
            "",
        ],
    ]


def test_trace_list_forces_export_bound_after_request_revalidation():
    request = _request({})
    request.validated_query_data = {
        "project_id": "00000000-0000-0000-0000-000000000001",
        "filters": [],
        "page_number": 9,
        "page_size": 1,
    }
    project = SimpleNamespace(trace_type="observe")
    sentinel = object()

    with (
        patch("tracer.views.trace._project_queryset_for_request") as projects,
        patch("tracer.views.trace.V2AnalyticsQueryService"),
        patch.object(
            TraceView,
            "_list_traces_of_session_clickhouse",
            return_value=sentinel,
        ) as internal_list,
    ):
        projects.return_value.filter.return_value.first.return_value = project
        response = TraceView.list_traces_of_session.__wrapped__(
            TraceView(), request, bounded_export=True
        )

    assert response is sentinel
    internal_data = internal_list.call_args.args[2]
    assert internal_data["page_number"] == 0
    assert internal_data["page_size"] == 500


def test_span_export_propagates_list_failure_before_starting_csv():
    request = _request({"project_id": "00000000-0000-0000-0000-000000000001"})
    failure = SimpleNamespace(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    project = SimpleNamespace(name="Observe")
    with (
        patch("tracer.views.observation_span.Project.objects.filter") as projects,
        patch.object(
            ObservationSpanView, "list_spans_observe", return_value=failure
        ) as listing,
    ):
        projects.return_value.first.return_value = project
        response = ObservationSpanView().get_spans_export_data(request)

    assert response is failure
    assert listing.call_args.kwargs == {"bounded_export": True}


def test_session_export_marks_lower_bound_page_in_band():
    request = _request({"project_id": "00000000-0000-0000-0000-000000000001"})
    project = SimpleNamespace(name="Observe")
    page = SimpleNamespace(
        status_code=status.HTTP_200_OK,
        data={
            "result": {
                "table": [{"session_id": "session-1"}],
                "metadata": {"total_rows_is_lower_bound": True},
            }
        },
    )

    with (
        patch.object(TraceSessionView, "list_sessions", return_value=page) as listing,
        patch("tracer.views.trace_session._project_queryset_for_request") as projects,
    ):
        projects.return_value.get.return_value = project
        response = TraceSessionView().get_trace_session_export_data(request)

    assert response.status_code == status.HTTP_200_OK
    assert _rows(response)[-1][0].startswith("# export truncated after 1 rows")
    assert listing.call_args.kwargs == {"bounded_export": True}
