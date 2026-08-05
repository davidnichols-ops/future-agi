"""Session-list transport coverage for bounded scalar-attribute filters."""

from __future__ import annotations

import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

import pytest

from tracer.selectors.trace_filter_reads import BoundedFilterPage


def _attribute_filter() -> dict:
    return {
        "column_id": "final_status",
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": "text",
            "filter_op": "in",
            "filter_value": ["Rejected"],
        },
    }


def _bounded_page(
    *,
    rows: list[dict] | None = None,
    has_more: bool = False,
    complete: bool = True,
    error_code: str | None = None,
    total_rows_lower_bound: int = 0,
) -> BoundedFilterPage:
    return BoundedFilterPage(
        rows=list(rows or []),
        has_more=has_more,
        complete=complete,
        status="complete" if complete else "degraded",
        error_code=error_code,
        total_rows_lower_bound=total_rows_lower_bound,
        elapsed_ms=12.5,
        query_count=2,
        rows_returned=len(rows or []),
        result_payload_bytes=128,
        attempts=(),
    )


def _view_and_request():
    from tracer.views.trace_session import TraceSessionView

    view = TraceSessionView.__new__(TraceSessionView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda status, message, code: (
            "error",
            status,
            message,
            code,
        ),
        bad_request=lambda message: ("bad_request", message),
    )
    organization = SimpleNamespace(id=uuid.uuid4())
    request = SimpleNamespace(
        query_params={},
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    return view, request


@pytest.mark.unit
def test_attribute_session_list_uses_bounded_protocol_and_page_scoped_hydration():
    from tracer.views.trace_session import TraceSessionView

    view, request = _view_and_request()
    project_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    start_time = datetime(2026, 7, 31, 12, 0)

    builder = mock.MagicMock()
    builder.supports_candidate_first_page.return_value = False
    builder.supports_bounded_filter_scan.return_value = True
    builder.recommended_filter_classify_batch_size.return_value = 50
    builder.build_page_metrics_query.return_value = ("page metrics", {})
    builder.build_content_query.return_value = ("page content", {})
    builder.build_span_attributes_query.return_value = ("page attributes", {})
    builder.format_sessions.side_effect = lambda rows, columns: [
        dict(zip(columns, row, strict=True)) for row in rows
    ]
    builder_cls = mock.MagicMock(return_value=builder)

    analytics = mock.MagicMock()

    def _execute(query, _params, **_kwargs):
        if query == "page metrics":
            return SimpleNamespace(
                data=[
                    {
                        "session_id": session_id,
                        "session_start": start_time,
                        "session_end": start_time,
                        "duration": 0,
                        "total_cost": 0,
                        "total_tokens": 0,
                        "traces_count": 1,
                    }
                ]
            )
        if query == "page content":
            return SimpleNamespace(
                data=[
                    {
                        "session_id": session_id,
                        "first_message": "first",
                        "last_message": "last",
                    }
                ]
            )
        if query == "page attributes":
            return SimpleNamespace(data=[])
        raise AssertionError(f"unexpected broad ClickHouse query: {query}")

    analytics.execute_ch_query.side_effect = _execute
    view._fetch_session_names = mock.MagicMock(return_value={})
    view._fetch_end_user_info = mock.MagicMock(return_value={})

    bounded = _bounded_page(
        rows=[{"session_id": session_id, "start_time": start_time}],
        has_more=True,
        total_rows_lower_bound=6,
    )
    filters = [_attribute_filter()]
    with (
        mock.patch(
            "tracer.views.trace_session.SessionListQueryBuilderV2",
            builder_cls,
        ),
        mock.patch(
            "tracer.views.trace_session.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
        mock.patch(
            "tracer.views.trace_session.AnnotationsLabels.objects.filter",
            return_value=[],
        ),
    ):
        omitted_status, omitted_payload = TraceSessionView._list_sessions_clickhouse(
            view,
            request,
            project_id=project_id,
            project=None,
            analytics=analytics,
            validated_data={
                "filters": filters,
                "sort_params": [],
                "page_number": 4,
                "page_size": 1,
            },
        )
        request.query_params = {"allow_sampled": "false"}
        explicit_false_response = TraceSessionView._list_sessions_clickhouse(
            view,
            request,
            project_id=project_id,
            project=None,
            analytics=analytics,
            validated_data={
                "filters": filters,
                "sort_params": [],
                "page_number": 4,
                "page_size": 1,
                "allow_sampled": False,
            },
        )
        request.query_params = {"allow_sampled": "true"}
        status, payload = TraceSessionView._list_sessions_clickhouse(
            view,
            request,
            project_id=project_id,
            project=None,
            analytics=analytics,
            validated_data={
                "filters": filters,
                "sort_params": [],
                "page_number": 4,
                "page_size": 1,
                "allow_sampled": True,
            },
        )

    assert omitted_status == "ok"
    assert omitted_payload["metadata"]["total_rows_is_lower_bound"] is True
    assert explicit_false_response[0] == "error"
    assert explicit_false_response[1] == 503
    assert status == "ok"
    assert payload["metadata"] == {
        "total_rows": 6,
        "total_rows_is_lower_bound": True,
        "has_more": True,
        "query_complete": True,
        "query_status": "complete",
        "query_error_code": None,
    }
    assert payload["table"][0]["first_message"] == "first"
    assert payload["table"][0]["last_message"] == "last"
    assert bounded_read.call_count == 3
    bounded_kwargs = bounded_read.call_args.kwargs
    assert bounded_kwargs["key_field"] == "session_id"
    assert bounded_kwargs["page_number"] == 4
    assert bounded_kwargs["page_size"] == 1
    assert bounded_kwargs["max_candidates"] == 200
    assert bounded_kwargs["classify_batch_size"] == 50
    builder.build_candidate_page_query.assert_not_called()
    builder.build.assert_not_called()
    assert builder.build_page_metrics_query.call_count == 3
    assert builder.build_content_query.call_count == 3
    assert builder.build_span_attributes_query.call_count == 3


@pytest.mark.unit
def test_candidate_first_session_list_keeps_exact_metadata():
    from tracer.views.trace_session import TraceSessionView

    view, request = _view_and_request()
    builder = mock.MagicMock()
    builder.supports_candidate_first_page.return_value = True
    builder.build_candidate_page_query.return_value = ("candidate page", {})
    builder.build_candidate_count_query.return_value = ("candidate count", {})
    builder_cls = mock.MagicMock(return_value=builder)
    analytics = mock.MagicMock()

    def _execute(query, _params, **_kwargs):
        if query == "candidate page":
            return SimpleNamespace(data=[])
        if query == "candidate count":
            return SimpleNamespace(data=[{"total": 0}])
        raise AssertionError(f"unexpected ClickHouse query: {query}")

    analytics.execute_ch_query.side_effect = _execute
    with (
        mock.patch(
            "tracer.views.trace_session.SessionListQueryBuilderV2",
            builder_cls,
        ),
        mock.patch(
            "tracer.views.trace_session.read_bounded_filter_page"
        ) as bounded_read,
        mock.patch(
            "tracer.views.trace_session.AnnotationsLabels.objects.filter",
            return_value=[],
        ),
    ):
        status, payload = TraceSessionView._list_sessions_clickhouse(
            view,
            request,
            project_id=str(uuid.uuid4()),
            project=None,
            analytics=analytics,
            validated_data={
                "filters": [],
                "sort_params": [],
                "page_number": 4,
                "page_size": 30,
            },
        )

    assert status == "ok"
    assert payload["metadata"] == {"total_rows": 0}
    bounded_read.assert_not_called()
    builder.build_candidate_page_query.assert_called_once_with()
    builder.build_candidate_count_query.assert_called_once_with()


@pytest.mark.unit
def test_incomplete_bounded_session_list_returns_sanitized_503_without_hydration():
    from tracer.views.trace_session import TraceSessionView

    view, request = _view_and_request()
    builder = mock.MagicMock()
    builder.supports_candidate_first_page.return_value = False
    builder.supports_bounded_filter_scan.return_value = True
    builder_cls = mock.MagicMock(return_value=builder)
    analytics = mock.MagicMock()

    with (
        mock.patch(
            "tracer.views.trace_session.SessionListQueryBuilderV2",
            builder_cls,
        ),
        mock.patch(
            "tracer.views.trace_session.read_bounded_filter_page",
            return_value=_bounded_page(
                complete=False,
                error_code="deadline_exceeded",
            ),
        ),
    ):
        response = TraceSessionView._list_sessions_clickhouse(
            view,
            request,
            project_id=str(uuid.uuid4()),
            project=None,
            analytics=analytics,
            validated_data={
                "filters": [_attribute_filter()],
                "sort_params": [],
                "page_number": 0,
                "page_size": 30,
            },
        )

    assert response == (
        "error",
        503,
        "Filtered session data is temporarily unavailable. Please retry.",
        "service_unavailable",
    )
    analytics.execute_ch_query.assert_not_called()
    builder.build_candidate_page_query.assert_not_called()
    builder.build.assert_not_called()
    builder.build_page_metrics_query.assert_not_called()
