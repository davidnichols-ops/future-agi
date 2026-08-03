"""Failure-semantics guards for tracing list, graph, and detail boundaries."""

from inspect import unwrap
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from clickhouse_driver.errors import ServerException

from tracer.models.project import Project
from tracer.models.trace import Trace
from tracer.services.clickhouse.list_cursor import ListCursorError
from tracer.services.clickhouse.v2.trace_detail_reads import (
    TraceDetailNotFound,
    TraceDetailReadUnavailable,
)

PROJECT_ID = "11111111-1111-4111-8111-111111111111"


def _raise(exc):
    def fail(*_args, **_kwargs):
        raise exc

    return fail


def _result(response):
    return response.data.get("result", response.data)


def _assert_sanitized_400(response):
    assert response.status_code == 400
    assert "could not be loaded" in str(response.data)
    assert "private" not in str(response.data)


def _trace_list_call(monkeypatch, exc):
    from tracer.views import trace as trace_view

    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = SimpleNamespace(
        trace_type="observe"
    )
    monkeypatch.setattr(
        trace_view, "_project_queryset_for_request", lambda _request: project_scope
    )
    monkeypatch.setattr(
        trace_view, "_get_request_organization", lambda _request: object()
    )
    monkeypatch.setattr(trace_view, "AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        trace_view.TraceView,
        "_list_traces_of_session_clickhouse",
        _raise(exc),
    )
    request = SimpleNamespace(
        validated_query_data={"project_id": PROJECT_ID},
    )
    view = trace_view.TraceView()
    view.request = request
    return unwrap(trace_view.TraceView.list_traces_of_session)(view, request)


def _trace_version_list_call(monkeypatch, exc):
    from tracer.views import trace as trace_view

    project_version_scope = MagicMock()
    project_version_scope.filter.return_value.first.return_value = object()
    monkeypatch.setattr(
        trace_view,
        "_project_version_queryset_for_request",
        lambda _request: project_version_scope,
    )
    monkeypatch.setattr(trace_view, "AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        trace_view.TraceView,
        "_list_traces_clickhouse",
        _raise(exc),
    )
    request = SimpleNamespace(
        validated_query_data={"project_version_id": "project-version-1"}
    )
    view = trace_view.TraceView()
    view.request = request
    return unwrap(trace_view.TraceView.list_traces)(view, request)


def _span_list_call(monkeypatch, exc):
    from tracer.views import observation_span as span_view

    monkeypatch.setattr(
        span_view.Project.objects, "get", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        span_view, "_get_request_organization", lambda _request: object()
    )
    monkeypatch.setattr(span_view, "AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        span_view.ObservationSpanView,
        "_list_spans_clickhouse",
        _raise(exc),
    )
    request = SimpleNamespace(validated_query_data={"project_id": PROJECT_ID})
    view = span_view.ObservationSpanView()
    view.request = request
    return unwrap(span_view.ObservationSpanView.list_spans_observe)(view, request)


def _span_version_list_call(monkeypatch, exc):
    from tracer.services.clickhouse import query_service
    from tracer.views import observation_span as span_view

    serializer = MagicMock()
    serializer.is_valid.return_value = True
    serializer.validated_data = {
        "project_version_id": "project-version-1",
        "filters": [],
    }
    monkeypatch.setattr(
        span_view, "SpanListQuerySerializer", lambda **_kwargs: serializer
    )
    monkeypatch.setattr(
        span_view.ProjectVersion.objects,
        "get",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        span_view, "_project_workspace_scope_q", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        span_view, "_get_request_organization", lambda _request: object()
    )
    monkeypatch.setattr(query_service, "AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        span_view.ObservationSpanView,
        "_list_spans_non_observe_clickhouse",
        _raise(exc),
    )
    request = SimpleNamespace(query_params={})
    view = span_view.ObservationSpanView()
    view.request = request
    return unwrap(span_view.ObservationSpanView.list_spans)(view, request)


def _voice_list_call(monkeypatch, exc):
    from tracer.views import trace as trace_view

    serializer = MagicMock()
    serializer.is_valid.return_value = True
    serializer.validated_data = {
        "project_id": PROJECT_ID,
        "filters": [],
        "page": 1,
        "page_size": 25,
    }
    monkeypatch.setattr(
        trace_view, "TraceVoiceCallListQuerySerializer", lambda **_kwargs: serializer
    )
    monkeypatch.setattr(
        trace_view.Project.objects, "get", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(trace_view, "AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        trace_view.TraceView,
        "_list_voice_calls_clickhouse",
        _raise(exc),
    )
    organization = object()
    request = SimpleNamespace(
        query_params={},
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view = trace_view.TraceView()
    view.request = request
    return unwrap(trace_view.TraceView.list_voice_calls)(view, request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary",
    [
        _trace_version_list_call,
        _trace_list_call,
        _span_version_list_call,
        _span_list_call,
        _voice_list_call,
    ],
    ids=["version-traces", "traces", "version-spans", "spans", "voice-calls"],
)
def test_list_boundaries_preserve_typed_timeout_response(monkeypatch, call_boundary):
    private_error = "private ClickHouse timeout and SQL"

    response = call_boundary(monkeypatch, ServerException(private_error, code=159))

    assert response.status_code == 503
    assert _result(response) in {
        "Trace data is temporarily unavailable. Please retry.",
        "Span data is temporarily unavailable. Please retry.",
        "Voice call data is temporarily unavailable. Please retry.",
    }
    assert private_error not in str(response.data)


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary",
    [
        _trace_version_list_call,
        _trace_list_call,
        _span_version_list_call,
        _span_list_call,
        _voice_list_call,
    ],
    ids=["version-traces", "traces", "version-spans", "spans", "voice-calls"],
)
@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("private query compiler invariant"),
        ValueError("private query compiler contract"),
        ServerException("private unknown-column SQL", code=47),
    ],
    ids=["runtime", "value-error", "code-47"],
)
def test_list_boundaries_preserve_sanitized_400_for_query_defects(
    monkeypatch, call_boundary, exc
):
    response = call_boundary(monkeypatch, exc)

    _assert_sanitized_400(response)


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary", [_trace_list_call, _span_list_call], ids=["traces", "spans"]
)
@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("invalid_cursor", "The continuation cursor is invalid."),
        ("cursor_mismatch", "The continuation cursor does not match this request."),
        ("cursor_expired", "The continuation cursor has expired."),
    ],
)
def test_observe_list_boundaries_return_typed_sanitized_cursor_400(
    monkeypatch, call_boundary, code, message
):
    response = call_boundary(monkeypatch, ListCursorError(code, message))

    assert response.status_code == 400
    assert code in str(response.data)
    assert message in str(response.data)
    assert "BadSignature" not in str(response.data)


def _graph_call(monkeypatch, view_kind, outcome):
    if view_kind == "trace":
        from tracer.views import trace as graph_view

        project_scope = MagicMock()
        project_scope.filter.return_value.first.return_value = SimpleNamespace(
            trace_type="observe"
        )
        monkeypatch.setattr(
            graph_view,
            "_project_queryset_for_request",
            lambda _request: project_scope,
        )
        view_cls = graph_view.TraceView
    else:
        from tracer.views import observation_span as graph_view

        monkeypatch.setattr(
            graph_view.Project.objects,
            "get",
            lambda *_args, **_kwargs: SimpleNamespace(trace_type="observe"),
        )
        monkeypatch.setattr(
            graph_view, "_project_workspace_scope_q", lambda *_args, **_kwargs: object()
        )
        monkeypatch.setattr(
            graph_view, "_get_request_organization", lambda _request: object()
        )
        view_cls = graph_view.ObservationSpanView

    monkeypatch.setattr(graph_view, "V2AnalyticsQueryService", MagicMock)
    graph_fetch = (
        _raise(outcome)
        if isinstance(outcome, BaseException)
        else lambda **_kwargs: outcome
    )
    monkeypatch.setattr(graph_view, "fetch_system_metric_graph_ch", graph_fetch)
    request = SimpleNamespace(
        validated_data={
            "project_id": PROJECT_ID,
            "filters": [],
            "property": "average",
            "interval": "day",
            "req_data_config": {"id": "latency", "type": "SYSTEM_METRIC"},
        }
    )
    view = view_cls()
    view.request = request
    return unwrap(view_cls.get_graph_methods)(view, request)


@pytest.mark.unit
@pytest.mark.parametrize("view_kind", ["trace", "span"])
def test_graph_boundaries_degrade_code_159(monkeypatch, view_kind):
    private_error = "private ClickHouse timeout and stack"

    response = _graph_call(
        monkeypatch,
        view_kind,
        ServerException(private_error, code=159),
    )

    assert response.status_code == 200
    result = _result(response)
    assert result["data"] == []
    assert result["query_complete"] is False
    assert result["query_status"] == "degraded"
    assert result["query_error_code"] == "read_budget_exceeded"
    assert private_error not in str(response.data)


@pytest.mark.unit
@pytest.mark.parametrize("view_kind", ["trace", "span"])
def test_graph_boundaries_never_publish_degraded_points(monkeypatch, view_kind):
    response = _graph_call(
        monkeypatch,
        view_kind,
        {
            "metric_name": "latency",
            "data": [
                {
                    "timestamp": "2026-08-03T00:00:00Z",
                    "value": 999,
                    "primary_traffic": 999,
                }
            ],
            "query_complete": False,
            "query_status": "degraded",
            "query_error_code": "sample_limit",
        },
    )

    assert response.status_code == 200
    result = _result(response)
    assert result["data"] == []
    assert result["query_complete"] is False
    assert result["query_status"] == "degraded"
    assert result["query_error_code"] == "sample_limit"


@pytest.mark.unit
@pytest.mark.parametrize("view_kind", ["trace", "span"])
@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("private graph compiler invariant"),
        ServerException("private graph unknown-column SQL", code=47),
    ],
    ids=["runtime", "code-47"],
)
def test_graph_boundaries_preserve_sanitized_400_for_query_defects(
    monkeypatch, view_kind, exc
):
    response = _graph_call(monkeypatch, view_kind, exc)

    _assert_sanitized_400(response)


def _trace_detail_call(monkeypatch, exc):
    from tracer.services.clickhouse.v2 import dispatch, query_service
    from tracer.views.trace import TraceView

    class Handler:
        def __init__(self, **_kwargs):
            pass

        def fetch(self):
            raise exc

    monkeypatch.setattr(
        dispatch, "get_query_builder_class", lambda _query_type: Handler
    )
    monkeypatch.setattr(
        query_service,
        "query_service_for_builder",
        lambda *_args, **_kwargs: object(),
    )
    request = SimpleNamespace()
    view = TraceView()
    view.request = request
    return view.retrieve(request, pk="trace-1")


def _span_detail_call(monkeypatch, exc):
    from tracer.views import observation_span as span_view

    manager = MagicMock()
    manager.filter.return_value.values_list.return_value.__getitem__.return_value = []
    project_model = SimpleNamespace(
        no_workspace_objects=manager,
        objects=manager,
        DoesNotExist=Project.DoesNotExist,
    )
    monkeypatch.setattr(span_view, "Project", project_model)
    monkeypatch.setattr(
        span_view, "_project_workspace_scope_q", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        span_view, "_get_request_organization", lambda _request: object()
    )
    monkeypatch.setattr(span_view, "V2AnalyticsQueryService", MagicMock)
    monkeypatch.setattr(
        span_view.ObservationSpanView,
        "_retrieve_clickhouse",
        _raise(exc),
    )
    request = SimpleNamespace()
    view = span_view.ObservationSpanView()
    view.request = request
    return unwrap(span_view.ObservationSpanView.retrieve)(view, request, pk="span-1")


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary", [_trace_detail_call, _span_detail_call], ids=["trace", "span"]
)
def test_detail_boundaries_preserve_typed_unavailable_response(
    monkeypatch, call_boundary
):
    response = call_boundary(
        monkeypatch, TraceDetailReadUnavailable("read_budget_exceeded")
    )

    assert response.status_code == 503
    assert "temporarily unavailable" in str(response.data)
    assert "read_budget_exceeded" not in str(response.data)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("call_boundary", "exc"),
    [
        (_trace_detail_call, Trace.DoesNotExist()),
        (_span_detail_call, TraceDetailNotFound()),
    ],
    ids=["trace", "span"],
)
def test_detail_boundaries_preserve_not_found_bad_request(
    monkeypatch, call_boundary, exc
):
    response = call_boundary(monkeypatch, exc)

    assert response.status_code == 400


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary", [_trace_detail_call, _span_detail_call], ids=["trace", "span"]
)
@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("private detail compiler invariant"),
        ServerException("private detail unknown-column SQL", code=47),
    ],
    ids=["runtime", "code-47"],
)
def test_detail_boundaries_preserve_sanitized_400_for_query_defects(
    monkeypatch, call_boundary, exc
):
    response = call_boundary(monkeypatch, exc)

    _assert_sanitized_400(response)


def _eval_name_call(monkeypatch, exc):
    from tracer.views import trace as trace_view

    project_scope = MagicMock()
    project_scope.filter.return_value.first.return_value = SimpleNamespace(
        trace_type="observe"
    )
    config_manager = MagicMock()
    config_manager.filter.return_value.values_list.return_value = ["config-1"]
    analytics = MagicMock()
    analytics.get_eval_config_ids_with_data_ch.side_effect = exc
    monkeypatch.setattr(
        trace_view, "_project_queryset_for_request", lambda _request: project_scope
    )
    monkeypatch.setattr(trace_view.CustomEvalConfig, "objects", config_manager)
    monkeypatch.setattr(trace_view, "AnalyticsQueryService", lambda: analytics)
    request = SimpleNamespace(query_params={"project_id": PROJECT_ID})
    view = trace_view.TraceView()
    view.request = request
    return unwrap(trace_view.TraceView.get_eval_names)(view, request)


def _eval_detail_call(monkeypatch, exc):
    from tracer.services.clickhouse import query_service
    from tracer.views import observation_span as span_view

    config_manager = MagicMock()
    config_manager.filter.return_value.values.return_value.first.return_value = {
        "project_id": PROJECT_ID
    }
    analytics = MagicMock()
    analytics.get_eval_detail_ch.side_effect = exc
    monkeypatch.setattr(
        span_view.CustomEvalConfig, "no_workspace_objects", config_manager
    )
    monkeypatch.setattr(
        span_view, "_project_workspace_scope_q", lambda *_args, **_kwargs: object()
    )
    monkeypatch.setattr(
        span_view, "_get_request_organization", lambda _request: object()
    )
    monkeypatch.setattr(query_service, "AnalyticsQueryService", lambda: analytics)
    request = SimpleNamespace(
        query_params={
            "observation_span_id": "span-1",
            "custom_eval_config_id": "config-1",
        }
    )
    view = span_view.ObservationSpanView()
    view.request = request
    return unwrap(span_view.ObservationSpanView.get_evaluation_details)(view, request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary", [_eval_name_call, _eval_detail_call], ids=["names", "detail"]
)
def test_eval_read_boundaries_preserve_typed_timeout_response(
    monkeypatch, call_boundary
):
    private_error = "private eval timeout and SQL"

    response = call_boundary(monkeypatch, ServerException(private_error, code=159))

    assert response.status_code == 503
    assert "temporarily unavailable" in str(response.data)
    assert private_error not in str(response.data)


@pytest.mark.unit
@pytest.mark.parametrize(
    "call_boundary", [_eval_name_call, _eval_detail_call], ids=["names", "detail"]
)
@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("private eval compiler invariant"),
        ServerException("private eval unknown-column SQL", code=47),
    ],
    ids=["runtime", "code-47"],
)
def test_eval_read_boundaries_preserve_sanitized_400_for_query_defects(
    monkeypatch, call_boundary, exc
):
    response = call_boundary(monkeypatch, exc)

    _assert_sanitized_400(response)
