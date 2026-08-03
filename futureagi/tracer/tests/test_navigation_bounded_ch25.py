"""Exact, fail-closed navigation over the direct-write ClickHouse lists."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver.errors import ServerException

from tracer.selectors.trace_filter_reads import BoundedFilterPage


def _complete_page(rows, *, has_more=False):
    return BoundedFilterPage(
        rows=list(rows),
        has_more=has_more,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=len(rows),
        elapsed_ms=0.0,
        query_count=1,
        rows_returned=len(rows),
        result_payload_bytes=0,
        attempts=(),
    )


def _ordered_rows(*, span=False):
    started = datetime(2026, 7, 31, tzinfo=UTC)
    rows = []
    for offset, label in enumerate(("newer", "current", "older")):
        row = {
            "trace_id": f"trace-{label}",
            "start_time": started - timedelta(seconds=offset),
        }
        if span:
            row.update(
                {
                    "project_id": "project-1",
                    "id": f"span-{label}",
                }
            )
        rows.append(row)
    return rows


@pytest.mark.unit
def test_trace_navigation_preserves_newest_first_list_direction():
    from tracer.views.trace import TraceView

    page = _complete_page(_ordered_rows())
    with patch(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
        return_value=page,
    ) as selector:
        response = TraceView()._get_trace_id_by_index_observe_clickhouse(
            MagicMock(),
            "trace-current",
            "project-1",
            [],
            MagicMock(),
        )

    assert response.status_code == 200
    assert response.data["result"] == {
        "next_trace_id": "trace-older",
        "previous_trace_id": "trace-newer",
    }
    assert selector.call_args.kwargs["page_number"] == 0
    assert selector.call_args.kwargs["page_size"] == 4095


@pytest.mark.unit
def test_span_navigation_preserves_newest_first_list_direction():
    from tracer.views.observation_span import ObservationSpanView

    page = _complete_page(_ordered_rows(span=True))
    with (
        patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=page,
        ) as selector,
        patch("tracer.views.observation_span.V2AnalyticsQueryService"),
    ):
        response = ObservationSpanView()._bounded_span_navigation_response(
            project_id="project-1",
            span_id="span-current",
            filters=[],
        )

    assert response.status_code == 200
    assert response.data["result"] == {
        "next_trace_id": "trace-older",
        "previous_trace_id": "trace-newer",
    }
    assert selector.call_args.kwargs["page_number"] == 0
    assert selector.call_args.kwargs["page_size"] == 4095


@pytest.mark.unit
@pytest.mark.parametrize("kind", ["trace", "span"])
def test_navigation_never_guesses_across_an_unread_page_boundary(kind):
    from tracer.views.observation_span import (
        ObservationSpanView,
        SpanNavigationReadUnavailable,
    )
    from tracer.views.trace import TraceNavigationReadUnavailable, TraceView

    rows = _ordered_rows(span=kind == "span")[:2]
    page = _complete_page(rows, has_more=True)
    with (
        patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=page,
        ),
        patch("tracer.views.observation_span.V2AnalyticsQueryService"),
    ):
        if kind == "trace":
            with pytest.raises(
                TraceNavigationReadUnavailable, match="page_depth_exceeded"
            ):
                TraceView()._get_trace_id_by_index_observe_clickhouse(
                    MagicMock(),
                    "trace-current",
                    "project-1",
                    [],
                    MagicMock(),
                )
        else:
            with pytest.raises(
                SpanNavigationReadUnavailable, match="page_depth_exceeded"
            ):
                ObservationSpanView()._bounded_span_navigation_response(
                    project_id="project-1",
                    span_id="span-current",
                    filters=[],
                )


@pytest.mark.unit
def test_trace_navigation_redacts_clickhouse_timeout():
    from tracer.views.trace import TraceView

    project_id = "00000000-0000-0000-0000-000000000001"
    trace_id = "00000000-0000-0000-0000-000000000002"
    request = SimpleNamespace(
        validated_query_data={
            "trace_id": trace_id,
            "project_id": project_id,
            "filters": [],
        }
    )
    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = SimpleNamespace(
        trace_type="observe"
    )
    private_error = "secret SQL and internal ClickHouse stack"

    with (
        patch(
            "tracer.views.trace._project_queryset_for_request",
            return_value=project_scope,
        ),
        patch("tracer.views.trace.V2AnalyticsQueryService"),
        patch.object(
            TraceView,
            "_get_trace_id_by_index_observe_clickhouse",
            side_effect=ServerException(private_error, code=159),
        ),
    ):
        response = TraceView.get_trace_id_by_index_observe.__wrapped__(
            TraceView(), request
        )

    assert response.status_code == 503
    assert response.data["result"] == (
        "Trace navigation is temporarily unavailable. Please retry."
    )
    assert private_error not in str(response.data)
    assert "DB::Exception" not in str(response.data)


@pytest.mark.unit
def test_trace_navigation_completed_miss_is_not_reported_as_unavailable():
    from tracer.views.trace import TraceNavigationReadUnavailable, TraceView

    project_id = "00000000-0000-0000-0000-000000000001"
    trace_id = "00000000-0000-0000-0000-000000000002"
    request = SimpleNamespace(
        validated_query_data={
            "trace_id": trace_id,
            "project_id": project_id,
            "filters": [],
        }
    )
    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = SimpleNamespace(
        trace_type="observe"
    )

    with (
        patch(
            "tracer.views.trace._project_queryset_for_request",
            return_value=project_scope,
        ),
        patch("tracer.views.trace.V2AnalyticsQueryService"),
        patch.object(
            TraceView,
            "_get_trace_id_by_index_observe_clickhouse",
            side_effect=TraceNavigationReadUnavailable("trace_not_in_list"),
        ),
    ):
        response = TraceView.get_trace_id_by_index_observe.__wrapped__(
            TraceView(), request
        )

    assert response.status_code == 400
    assert response.data["result"] == "Trace not found"


@pytest.mark.unit
def test_non_observe_trace_navigation_redacts_database_failure():
    from tracer.views.trace import TraceView

    request = SimpleNamespace(
        validated_query_data={
            "trace_id": "00000000-0000-0000-0000-000000000001",
            "project_version_id": "00000000-0000-0000-0000-000000000002",
            "filters": [],
        }
    )
    private_error = "secret SQL and internal database stack"

    with patch(
        "tracer.views.trace._project_version_queryset_for_request",
        side_effect=ServerException(private_error, code=159),
    ):
        response = TraceView.get_trace_id_by_index.__wrapped__(TraceView(), request)

    assert response.status_code == 400
    assert response.data["result"] == "Trace navigation could not be loaded"
    assert private_error not in str(response.data)


@pytest.mark.unit
def test_observation_span_fields_redact_internal_failure():
    from tracer.models.observation_span import ObservationSpan
    from tracer.views.observation_span import ObservationSpanView

    private_error = "secret metadata and internal database stack"
    with patch.object(
        ObservationSpan._meta,
        "get_fields",
        side_effect=ServerException(private_error, code=159),
    ):
        response = ObservationSpanView().get_observation_span_fields(SimpleNamespace())

    assert response.status_code == 400
    assert response.data["result"] == "Observation span fields could not be loaded"
    assert private_error not in str(response.data)


@pytest.mark.unit
def test_observation_span_export_redacts_clickhouse_failure():
    from tracer.views.observation_span import ObservationSpanView

    private_error = "secret SQL and internal ClickHouse stack"
    request = SimpleNamespace(query_params={})
    view = ObservationSpanView()
    view.request = request
    serializer = MagicMock()
    serializer.is_valid.return_value = True
    serializer.validated_data = {"project_id": "project-1"}

    with (
        patch(
            "tracer.views.observation_span.SpanExportQuerySerializer",
            return_value=serializer,
        ),
        patch.object(
            view,
            "list_spans_observe",
            side_effect=ServerException(private_error, code=159),
        ),
    ):
        response = view.get_spans_export_data(request)

    assert response.status_code == 400
    assert response.data["result"] == "Span export could not be generated"
    assert private_error not in str(response.data)
