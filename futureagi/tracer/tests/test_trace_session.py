"""
TraceSession API Tests

Tests for /tracer/trace-session/ endpoints.
"""

import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from clickhouse_driver.errors import ServerException
from django.utils import timezone
from rest_framework import status

from tracer.models.observation_span import EvalLogger, EvalTargetType, ObservationSpan
from tracer.models.trace import Trace
from tracer.models.trace_session import TraceSession, TraceSessionOverlay
from tracer.services.clickhouse.bounded_graph_reads import (
    BoundedGraphReadError,
    GraphCandidateSample,
)
from tracer.services.clickhouse.read_budget import ReadDeadlineExceeded
from tracer.services.clickhouse.session_graph import fetch_session_graph_ch
from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)
from tracer.views.trace_session import TraceSessionView


def _create_session_with_span(project, name, created_at=None):
    """Helper to create a session with a trace and span so get_session_navigation can find it."""
    session = TraceSession.objects.create(project=project, name=name)
    if created_at:
        TraceSession.objects.filter(id=session.id).update(created_at=created_at)
        session.refresh_from_db()
    trace = Trace.objects.create(
        project=project,
        session=session,
        name=f"Trace for {name}",
        input={"prompt": "test"},
        output={"response": "test"},
    )
    ObservationSpan.objects.create(
        id=f"span_{uuid.uuid4().hex[:16]}",
        project=project,
        trace=trace,
        name="ChatCompletion",
        observation_type="llm",
        start_time=session.created_at or timezone.now(),
        end_time=(session.created_at or timezone.now()) + timedelta(seconds=1),
        input="test",
        output="test",
        total_tokens=10,
        prompt_tokens=5,
        completion_tokens=5,
        cost=0.0001,
        latency_ms=500,
        status="OK",
    )
    return session


def _create_other_workspace_session(organization, user):
    from accounts.models.workspace import Workspace
    from model_hub.models.ai_model import AIModel
    from tracer.models.project import Project

    other_workspace = Workspace.objects.create(
        name=f"Other Workspace {uuid.uuid4()}",
        organization=organization,
        is_default=False,
        is_active=True,
        created_by=user,
    )
    other_project = Project.objects.create(
        name=f"Other Workspace Observe {uuid.uuid4()}",
        organization=organization,
        workspace=other_workspace,
        model_type=AIModel.ModelTypes.GENERATIVE_LLM,
        trace_type="observe",
    )
    session = _create_session_with_span(other_project, "Other Workspace Session")
    EvalLogger.objects.create(
        trace_session=session,
        target_type=EvalTargetType.SESSION,
        output_bool=True,
        eval_explanation="other workspace session eval",
    )
    return other_project, session


def get_result(response):
    """Extract result from API response wrapper."""
    data = response.json()
    return data.get("result", data)


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionRetrieveAPI:
    """Tests for GET /tracer/trace-session/{id}/ endpoint."""

    def test_retrieve_session_unauthenticated(self, api_client, trace_session):
        """Unauthenticated requests should be rejected."""
        response = api_client.get(f"/tracer/trace-session/{trace_session.id}/")
        assert response.status_code in (
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        )

    def test_retrieve_session_success(self, auth_client, trace_session):
        """Retrieve a trace session by ID."""
        response = auth_client.get(f"/tracer/trace-session/{trace_session.id}/")
        assert response.status_code == status.HTTP_200_OK
        data = get_result(response)
        assert "session_metadata" in data
        assert data["session_metadata"]["session_id"] == str(trace_session.id)

    def test_retrieve_session_not_found(self, auth_client):
        """Retrieve non-existent session returns error."""
        fake_id = uuid.uuid4()
        response = auth_client.get(f"/tracer/trace-session/{fake_id}/")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_retrieve_session_from_different_org(self, auth_client, organization):
        """
        Test retrieving session from different organization.

        The API now enforces organization-level access control on session
        retrieval and rejects sessions outside the request organization.
        """
        from accounts.models.organization import Organization
        from model_hub.models.ai_model import AIModel
        from tracer.models.project import Project

        # Create another organization and session
        other_org = Organization.objects.create(name="Other Org")
        other_project = Project.objects.create(
            name="Other Project",
            organization=other_org,
            model_type=AIModel.ModelTypes.GENERATIVE_LLM,
            trace_type="observe",
        )
        other_session = TraceSession.objects.create(
            project=other_project,
            name="Other Session",
        )

        response = auth_client.get(f"/tracer/trace-session/{other_session.id}/")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_retrieve_ch_only_session_requires_accessible_project(
        self, auth_client, observe_project
    ):
        session_id = uuid.uuid4()
        inaccessible_project_id = uuid.uuid4()

        with mock.patch(
            "tracer.services.clickhouse.v2.trace_session_dict_reader."
            "resolve_session_fields",
            return_value={
                str(session_id): {"project_id": str(inaccessible_project_id)}
            },
        ):
            response = auth_client.get(f"/tracer/trace-session/{session_id}/")

        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_retrieve_session_has_navigation_fields(self, auth_client, trace_session):
        """Session detail response includes previous/next session IDs in session_metadata."""
        response = auth_client.get(f"/tracer/trace-session/{trace_session.id}/")
        assert response.status_code == status.HTTP_200_OK
        data = get_result(response)
        metadata = data["session_metadata"]
        assert "previous_session_id" in metadata
        assert "next_session_id" in metadata

    def test_retrieve_session_navigation_single_session(
        self, auth_client, observe_project, trace_session
    ):
        """With only one session, both prev and next should be None."""
        TraceSession.objects.filter(project=observe_project).exclude(
            id=trace_session.id
        ).delete()

        response = auth_client.get(f"/tracer/trace-session/{trace_session.id}/")
        assert response.status_code == status.HTTP_200_OK
        metadata = get_result(response)["session_metadata"]
        assert metadata["previous_session_id"] is None
        assert metadata["next_session_id"] is None

    # The test env routes CH_ROUTE_SESSION_ANALYTICS to postgres and does
    # not seed ClickHouse, so the navigation tests below monkeypatch
    # _try_session_navigation_ch to simulate CH returning known
    # neighbours.

    def test_retrieve_session_navigation_middle_session(
        self, auth_client, observe_project, monkeypatch
    ):
        """Middle session should have both prev and next."""
        base = timezone.now()
        s1 = _create_session_with_span(
            observe_project, "First", base - timedelta(minutes=2)
        )
        s2 = _create_session_with_span(
            observe_project, "Middle", base - timedelta(minutes=1)
        )
        s3 = _create_session_with_span(observe_project, "Last", base)

        from tracer.utils import session as session_utils

        monkeypatch.setattr(
            session_utils,
            "_try_session_navigation_ch",
            lambda req, pid, sid: (str(s1.id), str(s3.id)),
        )

        response = auth_client.get(f"/tracer/trace-session/{s2.id}/")
        assert response.status_code == status.HTTP_200_OK
        metadata = get_result(response)["session_metadata"]
        assert metadata["previous_session_id"] == str(s3.id)
        assert metadata["next_session_id"] == str(s1.id)

    def test_retrieve_session_navigation_first_session(
        self, auth_client, observe_project, monkeypatch
    ):
        """First session (newest) should have next but no previous."""
        base = timezone.now()
        s1 = _create_session_with_span(
            observe_project, "Older", base - timedelta(minutes=1)
        )
        s2 = _create_session_with_span(observe_project, "Newest", base)

        from tracer.utils import session as session_utils

        monkeypatch.setattr(
            session_utils,
            "_try_session_navigation_ch",
            lambda req, pid, sid: (str(s1.id), None),
        )

        response = auth_client.get(f"/tracer/trace-session/{s2.id}/")
        assert response.status_code == status.HTTP_200_OK
        metadata = get_result(response)["session_metadata"]
        assert metadata["previous_session_id"] is None
        assert metadata["next_session_id"] == str(s1.id)

    def test_retrieve_session_navigation_last_session(
        self, auth_client, observe_project, monkeypatch
    ):
        """Last session (oldest) should have previous but no next."""
        base = timezone.now()
        s1 = _create_session_with_span(
            observe_project, "Oldest", base - timedelta(minutes=1)
        )
        s2 = _create_session_with_span(observe_project, "Newer", base)

        from tracer.utils import session as session_utils

        monkeypatch.setattr(
            session_utils,
            "_try_session_navigation_ch",
            lambda req, pid, sid: (None, str(s2.id)),
        )

        response = auth_client.get(f"/tracer/trace-session/{s1.id}/")
        assert response.status_code == status.HTTP_200_OK
        metadata = get_result(response)["session_metadata"]
        assert metadata["previous_session_id"] == str(s2.id)
        assert metadata["next_session_id"] is None

    def test_retrieve_session_rejects_legacy_navigation_aliases(
        self, auth_client, trace_session
    ):
        response = auth_client.get(
            f"/tracer/trace-session/{trace_session.id}/",
            {
                "userId": "customer-1",
                "sortParams": "[]",
                "pageNumber": "1",
                "pageSize": "10",
            },
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionListAPI:
    """Tests for GET /tracer/trace-session/list_sessions/ endpoint."""

    def test_list_sessions_unauthenticated(self, api_client, observe_project):
        """Unauthenticated requests should be rejected."""
        response = api_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(observe_project.id)},
        )
        assert response.status_code in (
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        )

    def test_list_sessions_missing_project(self, auth_client):
        """List sessions supports org-scoped listing without project ID."""
        with mock.patch(
            "tracer.services.clickhouse.query_service.AnalyticsQueryService"
        ) as analytics_cls:
            response = auth_client.get("/tracer/trace-session/list_sessions/")

        assert response.status_code == status.HTTP_200_OK
        assert get_result(response)["metadata"]["total_rows"] == 0
        analytics_cls.assert_not_called()

    def test_list_sessions_success(
        self, auth_client, observe_project, trace_session, session_trace
    ):
        """List sessions for a project."""
        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(observe_project.id)},
        )
        assert response.status_code == status.HTTP_200_OK
        data = get_result(response)
        assert "metadata" in data or "table" in data

    def test_list_sessions_with_pagination(self, auth_client, observe_project):
        """List sessions with pagination."""
        # Create multiple sessions
        for i in range(15):
            TraceSession.objects.create(
                project=observe_project,
                name=f"Session {i}",
            )

        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {
                "project_id": str(observe_project.id),
                "page_number": 0,
                "page_size": 10,
            },
        )
        assert response.status_code == status.HTTP_200_OK
        data = get_result(response)
        assert "metadata" in data

    def test_deep_filtered_page_is_non_retryable_422_before_clickhouse(
        self, auth_client, observe_project
    ):
        filters = [
            {
                "column_id": "final_status",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "Rechazado",
                },
            }
        ]

        with mock.patch(
            "tracer.services.clickhouse.query_service.AnalyticsQueryService"
        ) as analytics_cls:
            response = auth_client.get(
                "/tracer/trace-session/list_sessions/",
                {
                    "project_id": str(observe_project.id),
                    "filters": json.dumps(filters),
                    "page_number": 159,
                    "page_size": 30,
                },
            )

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert response.data["type"] == "client_error"
        assert response.data["code"] == "page_depth_exceeded"
        assert "earlier page" in response.data["message"]
        analytics_cls.assert_not_called()

    def test_list_sessions_empty(self, auth_client, observe_project):
        """List returns empty when no sessions exist."""
        # Delete existing sessions
        TraceSession.objects.filter(project=observe_project).delete()

        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(observe_project.id)},
        )
        assert response.status_code == status.HTTP_200_OK

    def test_list_sessions_filter_bookmarked(self, auth_client, observe_project):
        """Filter sessions by bookmarked status."""
        # Create bookmarked session
        TraceSession.objects.create(
            project=observe_project,
            name="Bookmarked Session",
            bookmarked=True,
        )

        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {
                "project_id": str(observe_project.id),
                "bookmarked": "true",
            },
        )
        assert response.status_code == status.HTTP_200_OK


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionExportAPI:
    """Tests for GET /tracer/trace-session/get_trace_session_export_data/ endpoint."""

    def test_export_sessions_unauthenticated(self, api_client, observe_project):
        """Unauthenticated requests should be rejected."""
        response = api_client.get(
            "/tracer/trace-session/get_trace_session_export_data/",
            {"project_id": str(observe_project.id)},
        )
        assert response.status_code in (
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        )

    def test_export_sessions_missing_project(self, auth_client):
        """Export sessions fails without project ID."""
        response = auth_client.get(
            "/tracer/trace-session/get_trace_session_export_data/"
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_export_sessions_success(
        self, auth_client, observe_project, trace_session, session_trace
    ):
        """Export sessions for a project."""
        response = auth_client.get(
            "/tracer/trace-session/get_trace_session_export_data/",
            {"project_id": str(observe_project.id)},
        )
        assert response.status_code == status.HTTP_200_OK


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionGraphAPI:
    """Tests for POST /tracer/trace-session/get_session_graph_data/ endpoint."""

    def test_session_filter_uses_clickhouse_graph(self, auth_client, observe_project):
        session_id = "003b76f1-2b4a-4af5-b0dc-224d687374d4"
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(data=[], columns=[])

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ) as v2_service,
            mock.patch(
                "tracer.services.clickhouse.query_service.AnalyticsQueryService"
            ) as legacy_service,
            mock.patch.object(Trace, "objects") as pg_trace_manager,
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "session_count",
                        "type": "SYSTEM_METRIC",
                    },
                    "filters": [
                        {
                            "column_id": "created_at",
                            "filter_config": {
                                "filter_type": "datetime",
                                "filter_op": "between",
                                "filter_value": [
                                    "2026-06-18T00:00:00Z",
                                    "2026-06-19T00:00:00Z",
                                ],
                            },
                        },
                        {
                            "column_id": "session",
                            "display_name": "Session",
                            "filter_config": {
                                "filter_type": "text",
                                "filter_op": "in",
                                "filter_value": [session_id],
                                "col_type": "SYSTEM_METRIC",
                            },
                        },
                    ],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_200_OK
        payload = get_result(response)
        assert payload["metric_name"] == "session_count"
        assert payload["query_complete"] is True
        assert payload["query_status"] == "complete"
        assert payload["data"]
        assert all(point["value"] == 0 for point in payload["data"])
        query_text = "\n".join(
            call.args[0] for call in analytics.execute_ch_query.call_args_list
        )
        assert "trace_session_id" in query_text
        assert "SessionTimeSeriesQueryBuilder" not in query_text
        for query_call in analytics.execute_ch_query.call_args_list:
            assert 0 < query_call.kwargs["timeout_ms"] <= 1_200
            settings = query_call.kwargs["settings"]
            assert settings["max_threads"] == 1
            assert settings["max_rows_to_read"] == 10_000_000
            assert settings["max_bytes_to_read"] == 512 * 1024 * 1024
            assert settings["max_memory_usage"] == 256 * 1024 * 1024
            assert settings["timeout_overflow_mode"] == "throw"
        v2_service.assert_called_once_with()
        legacy_service.assert_not_called()
        pg_trace_manager.assert_not_called()

    def test_session_system_graph_groups_only_proven_latest_trace_candidates(self):
        window_start = datetime(2026, 7, 24, tzinfo=UTC)
        window_end = window_start + timedelta(days=1)
        survivor = "10000000-0000-0000-0000-000000000000"
        duplicate_old = "20000000-0000-0000-0000-000000000000"
        consolidated_new = "30000000-0000-0000-0000-000000000000"
        other_session = "40000000-0000-0000-0000-000000000000"
        rows = (
            {
                "trace_id": "trace-corrected",
                "trace_session_id": consolidated_new,
                "start_time": window_start + timedelta(hours=1),
                "end_time": window_start + timedelta(hours=1, seconds=4),
                "latency_ms": 10,
                "cost": 2,
                "total_tokens": 10,
                "prompt_tokens": 6,
                "completion_tokens": 4,
                "status": "OK",
            },
            {
                "trace_id": "trace-same-session",
                "trace_session_id": duplicate_old,
                "start_time": window_start + timedelta(hours=2),
                "end_time": window_start + timedelta(hours=2, seconds=8),
                "latency_ms": 30,
                "cost": 3,
                "total_tokens": 20,
                "prompt_tokens": 12,
                "completion_tokens": 8,
                "status": "ERROR",
            },
            {
                "trace_id": "trace-other-session",
                "trace_session_id": other_session,
                "start_time": window_start + timedelta(hours=3),
                "end_time": window_start + timedelta(hours=3, seconds=2),
                "latency_ms": 50,
                "cost": 7,
                "total_tokens": 5,
                "prompt_tokens": 3,
                "completion_tokens": 2,
                "status": "OK",
            },
        )
        sample = GraphCandidateSample(
            rows=rows,
            query_complete=True,
            query_status="complete",
            query_error_code=None,
            window_start=window_start,
            window_end=window_end,
            elapsed_ms=1,
            query_count=2,
            rows_returned=3,
            result_payload_bytes=500,
            total_rows_lower_bound=3,
        )
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[
                {"old_id": survivor, "new_id": consolidated_new},
                {"old_id": duplicate_old, "new_id": consolidated_new},
            ],
            columns=["old_id", "new_id"],
        )

        with mock.patch(
            "tracer.services.clickhouse.session_graph.read_graph_candidates",
            return_value=sample,
        ) as candidate_read:
            graph = fetch_session_graph_ch(
                analytics=analytics,
                project_id=str(uuid.uuid4()),
                filters=[],
                interval="day",
                req_data_config={"id": "cost", "type": "SYSTEM_METRIC"},
            )

        assert graph["query_complete"] is True
        assert graph["query_status"] == "complete"
        # The two old/new IDs are one canonical session: its corrected latest
        # traces sum to 5. The other session sums to 7; cost is their average.
        assert graph["data"][0]["value"] == 6
        assert graph["data"][0]["primary_traffic"] == 2
        candidate_kwargs = candidate_read.call_args.kwargs
        assert candidate_kwargs["observe_type"] == "trace"
        assert candidate_kwargs["filters"][-1]["column_id"] == "trace_session_id"
        remap_sql = analytics.execute_ch_query.call_args.args[0]
        assert "trace_session_id_remap FINAL" in remap_sql
        assert "relevant_new_ids" in remap_sql

    def test_session_system_candidate_sql_replays_v2_updates_and_tombstones(self):
        builder = TraceListQueryBuilderV2(
            project_id=str(uuid.uuid4()),
            filters=[
                {
                    "column_id": "created_at",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "datetime",
                        "filter_op": "between",
                        "filter_value": [
                            "2026-07-24T00:00:00Z",
                            "2026-07-25T00:00:00Z",
                        ],
                    },
                },
                {
                    "column_id": "trace_session_id",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "is_not_null",
                        "filter_value": None,
                    },
                },
            ],
        )

        query, _ = builder.build_filter_match_query(["trace-1"])

        assert "argMax(is_deleted, _version) AS latest_is_deleted" in query
        assert "argMax(tuple(trace_session_id), _version).1" in query
        assert "WHERE latest_is_deleted = 0" in query
        assert "HAVING countIf" in query

    def test_session_system_graph_incomplete_candidate_set_is_typed_503(
        self,
        auth_client,
        observe_project,
    ):
        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=mock.Mock(),
            ),
            mock.patch(
                "tracer.services.clickhouse.session_graph.read_graph_candidates",
                side_effect=BoundedGraphReadError("sample_limit"),
            ),
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "session_count",
                        "type": "SYSTEM_METRIC",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        payload = get_result(response)
        assert payload["query_complete"] is False
        assert payload["query_error_code"] == "sample_limit"

    def test_session_system_graph_rejects_returned_candidate_sample_before_remap(
        self,
    ):
        window_start = datetime(2026, 7, 24, tzinfo=UTC)
        sample = GraphCandidateSample(
            rows=(
                {
                    "trace_id": "trace-proven-match",
                    "trace_session_id": "003b76f1-2b4a-4af5-b0dc-224d687374d4",
                    "start_time": window_start,
                },
            ),
            query_complete=False,
            query_status="degraded",
            query_error_code="sample_limit",
            window_start=window_start,
            window_end=window_start + timedelta(days=1),
            elapsed_ms=1,
            query_count=2,
            rows_returned=50,
            result_payload_bytes=100,
            total_rows_lower_bound=51,
        )
        analytics = mock.Mock()

        with (
            mock.patch(
                "tracer.services.clickhouse.session_graph.read_graph_candidates",
                return_value=sample,
            ),
            pytest.raises(BoundedGraphReadError) as caught,
        ):
            fetch_session_graph_ch(
                analytics=analytics,
                project_id=str(uuid.uuid4()),
                filters=[],
                interval="day",
                req_data_config={"id": "session_count", "type": "SYSTEM_METRIC"},
            )

        assert caught.value.error_code == "sample_limit"
        analytics.execute_ch_query.assert_not_called()

    @pytest.mark.parametrize(
        ("failure", "error_code"),
        [
            (
                ServerException("secret SQL and internal CH stack", 159),
                "read_budget_exceeded",
            ),
            (ReadDeadlineExceeded("secret deadline context"), "read_budget_exceeded"),
        ],
    )
    def test_session_graph_failure_is_sanitized_typed_503_without_pg_fallback(
        self,
        auth_client,
        observe_project,
        failure,
        error_code,
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = failure

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ),
            mock.patch.object(Trace, "objects") as pg_trace_manager,
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "session_count",
                        "type": "SYSTEM_METRIC",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        assert response.data["type"] == "service_unavailable"
        assert response.data["code"] == "service_unavailable"
        payload = get_result(response)
        assert payload["query_complete"] is False
        assert payload["query_status"] == "degraded"
        assert payload["query_error_code"] == error_code
        rendered = str(response.data)
        assert "secret SQL" not in rendered
        assert "internal CH stack" not in rendered
        assert "secret deadline context" not in rendered
        pg_trace_manager.assert_not_called()

    @pytest.mark.parametrize(
        "failure",
        [
            ServerException("secret unknown identifier", 47),
            ServerException("secret unknown table", 60),
            ServerException("secret syntax error", 62),
            RuntimeError("secret compiler state"),
        ],
    )
    def test_session_graph_query_defects_are_sanitized_400_without_pg_fallback(
        self,
        auth_client,
        observe_project,
        failure,
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = failure

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ),
            mock.patch.object(Trace, "objects") as pg_trace_manager,
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "session_count",
                        "type": "SYSTEM_METRIC",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        rendered = str(response.data)
        assert "secret" not in rendered
        assert "unknown identifier" not in rendered
        assert "unknown table" not in rendered
        assert "syntax error" not in rendered
        assert "compiler state" not in rendered
        pg_trace_manager.assert_not_called()

    def test_session_eval_graph_uses_same_deadline_caps_and_session_scope(
        self,
        auth_client,
        observe_project,
        eval_template,
    ):
        from tracer.models.custom_eval_config import CustomEvalConfig

        eval_config = CustomEvalConfig.objects.create(
            name=f"Session graph eval {uuid.uuid4()}",
            project=observe_project,
            eval_template=eval_template,
        )
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(data=[], columns=[])
        helper_calls = []

        def fake_eval_graph(**kwargs):
            helper_calls.append(kwargs)
            kwargs["analytics"].execute_ch_query(
                "SELECT 1",
                {},
                timeout_ms=9_000,
                settings={
                    "max_threads": 8,
                    "max_rows_to_read": 100_000_000,
                    "max_bytes_to_read": 2 * 1024 * 1024 * 1024,
                },
            )
            return {
                "metric_name": str(eval_config.id),
                "data": [],
                "query_complete": True,
                "query_status": "complete",
            }

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ),
            mock.patch(
                "tracer.services.clickhouse.session_graph.fetch_eval_graph_ch",
                side_effect=fake_eval_graph,
            ),
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": str(eval_config.id),
                        "type": "EVAL",
                        "output_type": "SCORE",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_200_OK
        assert get_result(response)["query_complete"] is True
        session_filter = helper_calls[0]["filters"][-1]
        assert session_filter["column_id"] == "trace_session_id"
        assert session_filter["filter_config"]["filter_op"] == "is_not_null"
        query_call = analytics.execute_ch_query.call_args
        assert 0 < query_call.kwargs["timeout_ms"] <= 1_200
        assert query_call.kwargs["settings"]["max_threads"] == 1
        assert query_call.kwargs["settings"]["max_rows_to_read"] == 10_000_000
        assert query_call.kwargs["settings"]["max_bytes_to_read"] == 512 * 1024 * 1024

    def test_session_annotation_incomplete_graph_is_typed_503(
        self,
        auth_client,
        observe_project,
    ):
        helper_calls = []

        def incomplete_annotation(**kwargs):
            helper_calls.append(kwargs)
            return {
                "metric_name": "annotation-id",
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
            }

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=mock.Mock(),
            ),
            mock.patch(
                "tracer.services.clickhouse.session_graph.fetch_annotation_graph_ch",
                side_effect=incomplete_annotation,
            ),
        ):
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "annotation-id",
                        "type": "ANNOTATION",
                        "output_type": "SCORE",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        payload = get_result(response)
        assert payload["data"] == []
        assert payload["query_complete"] is False
        assert payload["query_error_code"] == "sample_limit"
        assert helper_calls[0]["filters"][-1]["column_id"] == "trace_session_id"

    def test_session_graph_rejects_unsupported_type_without_any_ch_read(
        self,
        auth_client,
        observe_project,
    ):
        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService"
        ) as v2_service:
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "session_count",
                        "type": "UNSUPPORTED",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        v2_service.assert_not_called()

    def test_session_graph_rejects_unknown_system_metric_without_any_ch_read(
        self,
        auth_client,
        observe_project,
    ):
        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService"
        ) as v2_service:
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": "not_a_session_metric",
                        "type": "SYSTEM_METRIC",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        v2_service.assert_not_called()

    def test_session_graph_rejects_foreign_eval_config_before_ch_read(
        self,
        auth_client,
        observe_project,
        project,
        eval_template,
    ):
        from tracer.models.custom_eval_config import CustomEvalConfig

        foreign_config = CustomEvalConfig.objects.create(
            name=f"Foreign session eval {uuid.uuid4()}",
            project=project,
            eval_template=eval_template,
        )
        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService"
        ) as v2_service:
            response = auth_client.post(
                "/tracer/trace-session/get_session_graph_data/",
                {
                    "project_id": str(observe_project.id),
                    "interval": "day",
                    "property": "average",
                    "req_data_config": {
                        "id": str(foreign_config.id),
                        "type": "EVAL",
                        "output_type": "SCORE",
                    },
                    "filters": [],
                },
                format="json",
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        v2_service.assert_not_called()


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionWorkspaceScopeAPI:
    def test_create_rejects_same_org_other_workspace_project(
        self, auth_client, organization, user
    ):
        other_project, _session = _create_other_workspace_session(organization, user)

        response = auth_client.post(
            "/tracer/trace-session/",
            {
                "project": str(other_project.id),
                "name": "Forbidden Session",
                "bookmarked": False,
            },
            format="json",
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert not TraceSession.all_objects.filter(
            project=other_project,
            name="Forbidden Session",
        ).exists()

    def test_patch_rejects_same_org_other_workspace_project(
        self, auth_client, trace_session, organization, user
    ):
        other_project, _session = _create_other_workspace_session(organization, user)

        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"project": str(other_project.id)},
            format="json",
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        trace_session.refresh_from_db()
        assert trace_session.project_id != other_project.id

    def test_custom_actions_reject_same_org_other_workspace_project_or_session(
        self, auth_client, organization, user
    ):
        other_project, other_session = _create_other_workspace_session(
            organization,
            user,
        )

        detail = auth_client.get(f"/tracer/trace-session/{other_session.id}/")
        assert detail.status_code in (
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        )

        eval_logs = auth_client.get(
            f"/tracer/trace-session/{other_session.id}/eval_logs/"
        )
        assert eval_logs.status_code in (
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        )

        list_response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(other_project.id)},
        )
        assert list_response.status_code == status.HTTP_400_BAD_REQUEST

        export = auth_client.get(
            "/tracer/trace-session/get_trace_session_export_data/",
            {"project_id": str(other_project.id)},
        )
        assert export.status_code == status.HTTP_400_BAD_REQUEST

        filter_values = auth_client.get(
            "/tracer/trace-session/get_session_filter_values/",
            {"project_id": str(other_project.id), "column": "session_id"},
        )
        assert filter_values.status_code == status.HTTP_400_BAD_REQUEST

        graph = auth_client.post(
            "/tracer/trace-session/get_session_graph_data/",
            {
                "project_id": str(other_project.id),
                "interval": "day",
                "property": "average",
                "req_data_config": {"id": "session_count", "type": "SYSTEM_METRIC"},
                "filters": [],
            },
            format="json",
        )
        assert graph.status_code == status.HTTP_400_BAD_REQUEST

    def test_user_filter_values_return_external_user_ids(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"val": "alice"}, {"val": "bob"}]
        )

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ) as v2_service,
            mock.patch(
                "tracer.services.clickhouse.query_service.AnalyticsQueryService"
            ) as legacy_service,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {"project_id": str(observe_project.id), "column": "user_id"},
            )

        assert response.status_code == status.HTTP_200_OK
        v2_service.assert_called_once_with()
        legacy_service.assert_not_called()
        assert get_result(response)["values"] == ["alice", "bob"]
        query = analytics.execute_ch_query.call_args.args[0]
        assert "FROM end_users FINAL" in query
        assert "user_id AS val" in query

    @pytest.mark.parametrize(
        ("column", "expected_aggregate"),
        [
            ("first_message", "argMin(latest_input, start_time) AS val"),
            ("last_message", "argMax(latest_input, start_time) AS val"),
        ],
    )
    def test_session_message_filter_values_are_finite_and_latest_state_safe(
        self,
        auth_client,
        observe_project,
        column,
        expected_aggregate,
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"val": "Needle message"}]
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ) as v2_service:
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {
                    "project_id": str(observe_project.id),
                    "column": column,
                    "search": "Needle",
                },
            )

        assert response.status_code == status.HTTP_200_OK
        v2_service.assert_called_once_with()
        payload = get_result(response)
        assert payload["values"] == ["Needle message"]
        assert payload["next"] is False
        assert payload["query_complete"] is True
        assert payload["query_status"] == "complete"

        call = analytics.execute_ch_query.call_args
        query, params = call.args[:2]
        assert "latest_roots AS" in query
        assert "start_time >= %(window_start)s" in query
        assert "start_time < %(window_end)s" in query
        assert params["window_end"] - params["window_start"] == timedelta(days=30)
        assert "argMax(is_deleted, _version) AS latest_is_deleted" in query
        assert (
            "argMax(tuple(parent_span_id), _version).1 "
            "AS latest_parent_span_id" in query
        )
        assert "argMax(tuple(trace_session_id), _version).1" in query
        assert "AS latest_trace_session_id" in query
        assert "argMax(tuple(input), _version).1 AS latest_input" in query
        assert "WHERE latest_is_deleted = 0" in query
        assert expected_aggregate in query
        assert "trace_session_id_remap" in query
        assert "positionCaseInsensitiveUTF8(val, %(filter_value_search)s)" in query
        assert params["filter_value_search"] == "Needle"
        assert params["result_limit"] == 51
        assert call.kwargs["timeout_ms"] == 4_000
        settings = call.kwargs["settings"]
        assert settings["max_rows_to_read"] == 2_000_000
        assert settings["max_bytes_to_read"] == 512 * 1024 * 1024
        assert settings["timeout_overflow_mode"] == "throw"
        assert settings["read_overflow_mode"] == "throw"

    def test_dense_session_message_page_uses_exact_has_more_sentinel(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"val": f"message-{index:03d}"} for index in range(501)]
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {
                    "project_id": str(observe_project.id),
                    "column": "first_message",
                    "page": 2,
                    "page_size": 500,
                },
            )

        assert response.status_code == status.HTTP_200_OK
        payload = get_result(response)
        assert len(payload["values"]) == 500
        assert payload["next"] is True
        # A numbered page with a sentinel is exact even though another page exists.
        assert payload["query_complete"] is True
        assert payload["query_status"] == "complete"
        _query, params = analytics.execute_ch_query.call_args.args[:2]
        assert params["result_limit"] == 501
        assert params["result_offset"] == 1_000
        settings = analytics.execute_ch_query.call_args.kwargs["settings"]
        assert settings["max_result_rows"] == 501

    def test_session_message_filter_budget_is_explicit_degraded_and_sanitized(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = ServerException(
            "DB::Exception private dense-project query", 159
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {
                    "project_id": str(observe_project.id),
                    "column": "last_message",
                },
            )

        assert response.status_code == status.HTTP_200_OK
        assert get_result(response) == {
            "values": [],
            "next": False,
            "query_complete": False,
            "query_status": "degraded",
            "query_error_code": "read_budget_exceeded",
        }
        assert "private dense-project query" not in str(response.data)
        settings = analytics.execute_ch_query.call_args.kwargs["settings"]
        assert settings["timeout_overflow_mode"] == "throw"
        assert settings["read_overflow_mode"] == "throw"

    def test_session_message_filter_query_defect_is_sanitized_400(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = ServerException(
            "DB::Exception secret unknown identifier", 47
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {
                    "project_id": str(observe_project.id),
                    "column": "first_message",
                },
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        rendered = str(response.data)
        assert "secret unknown identifier" not in rendered
        assert "DB::Exception" not in rendered

    def test_session_filter_values_sanitize_clickhouse_failure(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = ServerException(
            "DB::Exception secret-internal-query", 159
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {"project_id": str(observe_project.id), "column": "user_id"},
            )

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        payload = str(get_result(response))
        assert "secret-internal-query" not in payload
        assert "DB::Exception" not in payload
        settings = analytics.execute_ch_query.call_args.kwargs["settings"]
        assert settings["max_rows_to_read"] == 500_000
        assert settings["max_bytes_to_read"] == 256 * 1024 * 1024
        assert settings["timeout_overflow_mode"] == "throw"

    @pytest.mark.parametrize(
        "failure",
        [
            ServerException("secret unknown identifier", 47),
            ServerException("secret unknown table", 60),
            ServerException("secret syntax error", 62),
            RuntimeError("secret session filter compiler state"),
        ],
    )
    def test_session_filter_values_query_defects_are_sanitized_400(
        self,
        auth_client,
        observe_project,
        failure,
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = failure

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {"project_id": str(observe_project.id), "column": "user_id"},
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        rendered = str(response.data)
        assert "secret" not in rendered
        assert "unknown identifier" not in rendered
        assert "unknown table" not in rendered
        assert "syntax error" not in rendered
        assert "compiler state" not in rendered

    def test_session_metric_filter_uses_narrow_candidate_and_exact_count(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = [
            mock.Mock(data=[]),
            mock.Mock(data=[{"total": 0}]),
        ]
        filters = json.dumps(
            [
                {
                    "column_id": "total_tokens",
                    "filter_config": {
                        "filter_type": "number",
                        "filter_op": "greater_than",
                        "filter_value": 10,
                    },
                }
            ]
        )

        with mock.patch(
            "tracer.services.clickhouse.query_service.AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/list_sessions/",
                {
                    "project_id": str(observe_project.id),
                    "filters": filters,
                },
            )

        assert response.status_code == status.HTTP_200_OK
        assert analytics.execute_ch_query.call_count == 2
        page_sql = analytics.execute_ch_query.call_args_list[0].args[0]
        count_sql = analytics.execute_ch_query.call_args_list[1].args[0]
        for sql in (page_sql, count_sql):
            assert "candidate_root_identities AS" in sql
            assert "latest_roots AS" in sql
            assert "sum(total_tokens) AS total_tokens" in sql
            assert "HAVING total_tokens >" in sql
        for call in analytics.execute_ch_query.call_args_list:
            assert 0 < call.kwargs["timeout_ms"] <= 1_800
            settings = call.kwargs["settings"]
            assert settings["max_rows_to_read"] == 10_000_000
            assert settings["max_bytes_to_read"] == 512 * 1024 * 1024
            assert settings["max_memory_usage"] == 256 * 1024 * 1024
            assert settings["max_result_rows"] > 0
            assert settings["max_result_bytes"] == 32 * 1024 * 1024
            assert settings["result_overflow_mode"] == "throw"

    def test_session_list_sanitizes_typed_clickhouse_failure(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = ServerException(
            "secret SQL and internal stack", 159
        )

        with mock.patch(
            "tracer.services.clickhouse.query_service.AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/list_sessions/",
                {"project_id": str(observe_project.id)},
            )

        assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
        payload = str(get_result(response))
        assert "secret SQL" not in payload
        assert "internal stack" not in payload

    @pytest.mark.parametrize(
        "failure",
        [
            ServerException("secret unknown identifier", 47),
            ServerException("secret unknown table", 60),
            ServerException("secret syntax error", 62),
            RuntimeError("secret session compiler invariant failed"),
        ],
    )
    def test_session_list_query_defects_are_sanitized_400(
        self,
        auth_client,
        observe_project,
        failure,
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.side_effect = failure

        with mock.patch(
            "tracer.services.clickhouse.query_service.AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/list_sessions/",
                {"project_id": str(observe_project.id)},
            )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        rendered = str(response.data)
        assert "secret" not in rendered
        assert "unknown identifier" not in rendered
        assert "unknown table" not in rendered
        assert "syntax error" not in rendered
        assert "compiler invariant" not in rendered

    def test_session_filter_values_use_external_id_as_label(
        self, auth_client, observe_project
    ):
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"val": str(uuid.uuid4()), "label": "session-alpha"}]
        )
        session_id = analytics.execute_ch_query.return_value.data[0]["val"]

        with (
            mock.patch(
                "tracer.views.trace_session.V2AnalyticsQueryService",
                return_value=analytics,
            ),
            mock.patch(
                "tracer.services.clickhouse.v2.trace_session_dict_reader."
                "resolve_session_fields",
                return_value={
                    session_id: {
                        "external_session_id": "session-alpha",
                        "display_name": None,
                    }
                },
            ),
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {"project_id": str(observe_project.id), "column": "session_id"},
            )

        assert response.status_code == status.HTTP_200_OK
        assert get_result(response)["values"] == [
            {"value": session_id, "label": "session-alpha"}
        ]

    def test_session_filter_values_dedupe_straddlers_through_remap(
        self, auth_client, observe_project
    ):
        survivor_id = str(uuid.uuid4())
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"val": survivor_id, "label": "session-alpha"}]
        )

        with mock.patch(
            "tracer.views.trace_session.V2AnalyticsQueryService",
            return_value=analytics,
        ):
            response = auth_client.get(
                "/tracer/trace-session/get_session_filter_values/",
                {"project_id": str(observe_project.id), "column": "session_id"},
            )

        assert response.status_code == status.HTTP_200_OK
        assert get_result(response)["values"] == [
            {"value": survivor_id, "label": "session-alpha"}
        ]
        query = analytics.execute_ch_query.call_args.args[0]
        assert "trace_session_id_remap" in query
        assert "GROUP BY val_id" in query
        assert "toString(val_id) AS val" in query

    def test_generic_delete_cascades_session_traces_spans_and_eval_logs(
        self, auth_client, observe_project
    ):
        session = _create_session_with_span(observe_project, "Delete Cascade Session")
        trace = Trace.objects.get(session=session)
        span = ObservationSpan.objects.get(trace=trace)
        session_eval_log = EvalLogger.objects.create(
            trace_session=session,
            target_type=EvalTargetType.SESSION,
            output_bool=True,
            eval_explanation="session eval",
        )
        trace_eval_log = EvalLogger.objects.create(
            trace=trace,
            observation_span=span,
            target_type=EvalTargetType.TRACE,
            output_bool=True,
            eval_explanation="trace eval",
        )

        response = auth_client.delete(f"/tracer/trace-session/{session.id}/")

        assert response.status_code == status.HTTP_204_NO_CONTENT
        assert TraceSession.all_objects.get(id=session.id).deleted is True
        assert Trace.all_objects.get(id=trace.id).deleted is True
        assert ObservationSpan.all_objects.get(id=span.id).deleted is True
        assert EvalLogger.all_objects.get(id=session_eval_log.id).deleted is True
        assert EvalLogger.all_objects.get(id=trace_eval_log.id).deleted is True


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionOverlayWritePath:
    """Slice 2b — the bookmark/rename WRITE path mirrors the PG overlay.

    Drives the REAL update path (DRF ``PATCH`` → ``TraceSessionView.perform_update``
    → ``TraceSessionSerializer.save``) and asserts the PG ``TraceSessionOverlay``
    is upserted so slice 2a's overlay-backed reads stay fresh, the legacy
    ``TraceSession`` write is preserved, and the two PG writes share one
    transaction (DESIGN §5 / §5.1).
    """

    def test_patch_bookmark_upserts_overlay(self, auth_client, trace_session):
        """PATCH bookmarked=True → overlay row created with bookmarked=True.

        Also asserts the legacy ``TraceSession.bookmarked`` write is preserved
        (additive cutover) and the overlay carries project_id + the current name
        as display_name.
        """
        assert not TraceSessionOverlay.objects.filter(
            trace_session_id=trace_session.id
        ).exists()

        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"bookmarked": True},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK

        # Legacy TraceSession write preserved (PG-fallback path still reads it).
        trace_session.refresh_from_db()
        assert trace_session.bookmarked is True

        # Overlay upserted, mirroring the post-save instance state.
        overlay = TraceSessionOverlay.objects.get(trace_session_id=trace_session.id)
        assert overlay.bookmarked is True
        assert overlay.project_id == trace_session.project_id
        # display_name mirrors current name (the fixture session's name).
        assert overlay.display_name == trace_session.name

    def test_patch_bookmark_writes_overlay_for_ch_only_session(
        self, auth_client, observe_project
    ):
        session_id = uuid.uuid4()
        assert not TraceSession.objects.filter(id=session_id).exists()

        with mock.patch(
            "tracer.views.trace_session._resolve_ch_session_fields",
            return_value={
                "project_id": observe_project.id,
                "bookmarked": False,
                "display_name": "collector-session",
                "first_seen": None,
            },
        ):
            response = auth_client.patch(
                f"/tracer/trace-session/{session_id}/",
                {"bookmarked": True},
                format="json",
            )

        assert response.status_code == status.HTTP_200_OK
        assert not TraceSession.objects.filter(id=session_id).exists()
        overlay = TraceSessionOverlay.objects.get(trace_session_id=session_id)
        assert overlay.project_id == observe_project.id
        assert overlay.bookmarked is True
        assert overlay.display_name == "collector-session"

    def test_patch_rename_sets_overlay_display_name(self, auth_client, trace_session):
        """PATCH name='renamed-via-2b' → overlay.display_name reflects the rename."""
        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"name": "renamed-via-2b"},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK

        # Legacy name write preserved.
        trace_session.refresh_from_db()
        assert trace_session.name == "renamed-via-2b"

        # Overlay carries the new label as the display_name override.
        overlay = TraceSessionOverlay.objects.get(trace_session_id=trace_session.id)
        assert overlay.display_name == "renamed-via-2b"

    def test_partial_patch_does_not_clobber_other_overlay_field(
        self, auth_client, trace_session
    ):
        """A later partial PATCH must keep the previously-set overlay field.

        Reading the overlay defaults from the POST-SAVE instance (not
        ``validated_data``) means a ``{"bookmarked": ...}``-only PATCH still
        carries the existing ``name`` into ``display_name`` (and vice-versa).
        """
        # 1) rename
        auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"name": "renamed-via-2b"},
            format="json",
        )
        # 2) bookmark-only PATCH (no name in the body)
        auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"bookmarked": True},
            format="json",
        )

        overlay = TraceSessionOverlay.objects.get(trace_session_id=trace_session.id)
        # bookmarked applied AND the earlier rename survived (not clobbered to None).
        assert overlay.bookmarked is True
        assert overlay.display_name == "renamed-via-2b"

    def test_overlay_write_composes_with_slice_2a_bookmark_read(
        self, auth_client, observe_project, trace_session
    ):
        """End-to-end: the 2b write makes slice 2a's bookmark filter include it.

        ``_build_bookmark_filter`` is pure PG (overlay → ids), so it is exercised
        for real. Before the write the new session must NOT be in the bookmarked
        id set; after the PATCH it MUST be.
        """
        sid = str(trace_session.id)
        proj_ids = [str(observe_project.id)]

        before = TraceSessionView._build_bookmark_filter(True, proj_ids)
        assert sid not in before["filter_config"]["filter_value"]

        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"bookmarked": True},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK

        after = TraceSessionView._build_bookmark_filter(True, proj_ids)
        assert after["filter_config"]["filter_op"] == "in"
        assert sid in after["filter_config"]["filter_value"]

    def test_bookmark_filter_canonicalizes_overlay_ids_for_clickhouse(
        self, observe_project
    ):
        raw_id = str(uuid.uuid4())
        survivor_id = str(uuid.uuid4())
        TraceSessionOverlay.objects.create(
            trace_session_id=raw_id,
            project_id=observe_project.id,
            bookmarked=True,
        )
        analytics = mock.Mock()
        analytics.execute_ch_query.return_value = mock.Mock(
            data=[{"any_id": raw_id, "survivor_id": survivor_id}]
        )

        bookmark_filter = TraceSessionView._build_bookmark_filter(
            True, [str(observe_project.id)], analytics=analytics
        )

        assert bookmark_filter["filter_config"]["filter_op"] == "in"
        assert bookmark_filter["filter_config"]["filter_value"] == [survivor_id]
        sql = analytics.execute_ch_query.call_args.args[0]
        assert "trace_session_id_remap" in sql
        assert "WHERE any_id IN %(ids)s" in sql

    def test_retrieve_clickhouse_binds_canonical_requested_session_id(
        self, observe_project
    ):
        requested_id = str(uuid.uuid4())
        survivor_id = str(uuid.uuid4())
        bound_session_ids = []
        analytics = mock.Mock()

        def execute_ch_query(query, params, timeout_ms):
            if "WHERE any_id IN %(ids)s" in query:
                return mock.Mock(
                    data=[{"any_id": requested_id, "survivor_id": survivor_id}]
                )
            if "trace_session_id_remap" in query:
                return mock.Mock(data=[])
            if "count(DISTINCT trace_id)" in query:
                bound_session_ids.append(params.get("session_group_ids"))
                return mock.Mock(
                    data=[
                        {
                            "session_start": None,
                            "session_end": None,
                            "total_cost": 0,
                            "total_tokens": 0,
                            "total_traces": 0,
                        }
                    ]
                )
            if "GROUP BY trace_id" in query:
                bound_session_ids.append(params.get("session_group_ids"))
                return mock.Mock(data=[])
            raise AssertionError(f"unexpected ClickHouse query: {query}")

        analytics.execute_ch_query.side_effect = execute_ch_query

        with mock.patch(
            "tracer.views.trace_session.get_session_navigation",
            return_value=(None, None),
        ):
            response = TraceSessionView()._retrieve_clickhouse(
                mock.Mock(),
                requested_id,
                observe_project.id,
                analytics,
                {"page_number": 0, "page_size": 10},
            )

        assert response.status_code == status.HTTP_200_OK
        expected_group = (survivor_id,)
        assert bound_session_ids == [expected_group, expected_group]

    def test_retrieve_clickhouse_scans_spans_scoped_to_session_group(
        self, observe_project
    ):
        """A by-id session-detail retrieve scans spans scoped to the session
        group (``trace_session_id IN session_group_ids``), not clipped to the
        request's created_at window — the detail view shows the whole session.
        """
        requested_id = str(uuid.uuid4())
        captured_span_scan_params = []
        analytics = mock.Mock()

        def execute_ch_query(query, params, timeout_ms):
            if "WHERE any_id IN %(ids)s" in query:
                return mock.Mock(data=[])
            # Session-id canonicalization scan (_expand_session_group): no
            # aliases, so the requested id stands alone.
            if "FROM trace_session_id_remap" in query:
                return mock.Mock(data=[])
            if "FROM spans" in query and (
                "count(DISTINCT trace_id)" in query or "GROUP BY trace_id" in query
            ):
                # Both span scans are scoped to the session group
                # (partition/bloom-pruned by the id set), not the request's
                # created_at window — the detail view shows the whole session.
                # NB: we pin the positive scoping only. We deliberately do NOT
                # assert the *absence* of a time bound: `spans` is partitioned
                # by month, so a future fix that re-adds a date bound for
                # partition pruning (cf. TH-6237's 3.6 GiB OOM) must not be
                # forced red by this test.
                assert "trace_session_id IN %(session_group_ids)s" in query
                captured_span_scan_params.append(params)
                if "count(DISTINCT trace_id)" in query:
                    return mock.Mock(
                        data=[
                            {
                                "session_start": None,
                                "session_end": None,
                                "total_cost": 0,
                                "total_tokens": 0,
                                "total_traces": 0,
                                "end_user_id": "",
                            }
                        ]
                    )
                return mock.Mock(data=[])
            raise AssertionError(f"unexpected ClickHouse query: {query}")

        analytics.execute_ch_query.side_effect = execute_ch_query

        query_data = {
            "page_number": 0,
            "page_size": 10,
            "filters": [
                {
                    "column_id": "created_at",
                    "filter_config": {
                        "filter_type": "datetime",
                        "filter_op": "between",
                        "filter_value": [
                            "2026-01-01T00:00:00Z",
                            "2026-01-31T23:59:59Z",
                        ],
                    },
                }
            ],
        }

        with mock.patch(
            "tracer.views.trace_session.get_session_navigation",
            return_value=(None, None),
        ):
            response = TraceSessionView()._retrieve_clickhouse(
                mock.Mock(),
                requested_id,
                observe_project.id,
                analytics,
                query_data,
            )

        assert response.status_code == status.HTTP_200_OK
        # Both the session-aggregate and the paginated-trace span scans ran.
        assert len(captured_span_scan_params) == 2
        for params in captured_span_scan_params:
            assert params["session_group_ids"] == (requested_id,)

    def test_overlay_write_composes_with_slice_2a_name_read(
        self, auth_client, observe_project, trace_session
    ):
        """End-to-end: the 2b rename surfaces through slice 2a's name COALESCE.

        ``_fetch_session_names`` resolves ``external_session_id`` from CH FIRST
        (``resolve_external_session_ids``, which re-raises on error and is
        unreachable from host pytest), THEN overlays ``display_name``. We mock the
        CH half to ``{}`` and assert the PG overlay override wins:
        ``COALESCE(overlay.display_name, external)`` → the new label.
        """
        sid = str(trace_session.id)
        proj_ids = [str(observe_project.id)]

        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"name": "renamed-via-2b"},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK

        with mock.patch(
            "tracer.services.clickhouse.v2.trace_session_dict_reader."
            "resolve_external_session_ids",
            return_value={},
        ):
            name_map = TraceSessionView._fetch_session_names([sid], proj_ids)

        assert name_map[sid] == "renamed-via-2b"

    def test_overlay_upsert_failure_rolls_back_trace_session(self, trace_session):
        """Atomicity: a failing overlay upsert rolls back the TraceSession write.

        The overlay is PG (same DB as TraceSession), so ``perform_update`` wraps
        ``save()`` + the overlay ``update_or_create`` in ONE
        ``transaction.atomic()`` — both-or-neither. We drive ``perform_update``
        directly (the exact code under test) so the assertion targets the
        transactional guarantee, not DRF's exception-to-status mapping: a raw
        ``RuntimeError`` is not an ``APIException`` and would otherwise propagate
        through the test client unconverted. Force the overlay upsert to raise,
        then assert the legacy TraceSession row is UNCHANGED (the save did not
        stick) and NO overlay row was created.
        """
        from tracer.serializers.trace_session import TraceSessionSerializer

        original_name = trace_session.name
        assert original_name != "renamed-via-2b"

        view = TraceSessionView()
        serializer = TraceSessionSerializer(
            instance=trace_session,
            data={"name": "renamed-via-2b"},
            partial=True,
        )
        assert serializer.is_valid(), serializer.errors

        with mock.patch.object(
            TraceSessionOverlay.objects,
            "update_or_create",
            side_effect=RuntimeError("overlay write boom"),
        ):
            with pytest.raises(RuntimeError, match="overlay write boom"):
                view.perform_update(serializer)

        # The atomic() block rolled the TraceSession save back with the failed
        # overlay upsert (shared transaction) — re-read straight from the DB.
        fresh = TraceSession.objects.get(id=trace_session.id)
        assert fresh.name == original_name
        # No overlay row leaked.
        assert not TraceSessionOverlay.objects.filter(
            trace_session_id=trace_session.id
        ).exists()


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionCHOnlyDestroyPath:
    """CH-only sessions (no PG row) must be deletable via the same endpoint."""

    def test_delete_ch_only_session_returns_204(self, auth_client, observe_project):
        session_id = uuid.uuid4()
        assert not TraceSession.objects.filter(id=session_id).exists()

        with (
            mock.patch(
                "tracer.views.trace_session._resolve_ch_session_fields",
                return_value={
                    "project_id": observe_project.id,
                    "external_session_id": "ext-session-1",
                    "first_seen": timezone.now(),
                    "bookmarked": False,
                    "display_name": None,
                },
            ),
            mock.patch(
                "tracer.services.clickhouse.v2.curated_writer._get_client"
            ) as mock_ch_client,
        ):
            mock_ch_client.return_value = mock.Mock()
            response = auth_client.delete(
                f"/tracer/trace-session/{session_id}/",
            )

        assert response.status_code == status.HTTP_204_NO_CONTENT

    def test_delete_ch_only_session_removes_overlay(self, auth_client, observe_project):
        session_id = uuid.uuid4()
        TraceSessionOverlay.objects.create(
            trace_session_id=session_id,
            project_id=observe_project.id,
            bookmarked=True,
            display_name="bookmarked-session",
        )

        with (
            mock.patch(
                "tracer.views.trace_session._resolve_ch_session_fields",
                return_value={
                    "project_id": observe_project.id,
                    "external_session_id": "ext-session-1",
                    "first_seen": timezone.now(),
                    "bookmarked": True,
                    "display_name": "bookmarked-session",
                },
            ),
            mock.patch(
                "tracer.services.clickhouse.v2.curated_writer._get_client"
            ) as mock_ch_client,
        ):
            mock_ch_client.return_value = mock.Mock()
            response = auth_client.delete(
                f"/tracer/trace-session/{session_id}/",
            )

        assert response.status_code == status.HTTP_204_NO_CONTENT
        assert not TraceSessionOverlay.objects.filter(
            trace_session_id=session_id
        ).exists()

    def test_delete_ch_only_session_inserts_deletion_marker(
        self, auth_client, observe_project
    ):
        session_id = uuid.uuid4()

        with (
            mock.patch(
                "tracer.views.trace_session._resolve_ch_session_fields",
                return_value={
                    "project_id": observe_project.id,
                    "external_session_id": "ext-session-1",
                    "first_seen": timezone.now(),
                    "bookmarked": False,
                    "display_name": None,
                },
            ),
            mock.patch(
                "tracer.services.clickhouse.v2.curated_writer._get_client"
            ) as mock_ch_client,
        ):
            ch_client = mock.Mock()
            mock_ch_client.return_value = ch_client
            auth_client.delete(f"/tracer/trace-session/{session_id}/")

        ch_client.insert.assert_called_once()
        call_args = ch_client.insert.call_args
        assert call_args.args[0] == "trace_sessions"
        row = call_args.args[1][0]
        is_deleted_col_idx = 5
        assert row[is_deleted_col_idx] == 1

    def test_delete_ch_only_session_not_found_returns_404(
        self, auth_client, observe_project
    ):
        session_id = uuid.uuid4()

        with mock.patch(
            "tracer.views.trace_session._resolve_ch_session_fields",
            return_value=None,
        ):
            response = auth_client.delete(
                f"/tracer/trace-session/{session_id}/",
            )

        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_delete_pg_session_still_works(self, auth_client, trace_session):
        response = auth_client.delete(
            f"/tracer/trace-session/{trace_session.id}/",
        )
        assert response.status_code == status.HTTP_204_NO_CONTENT
        trace_session.refresh_from_db()
        assert trace_session.deleted is True


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionResponseContract:
    """Both PG and CH-only PATCH paths must return the same response shape."""

    EXPECTED_KEYS = {"id", "project", "bookmarked", "name", "created_at"}

    def test_pg_patch_response_shape(self, auth_client, trace_session):
        response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"bookmarked": True},
            format="json",
        )
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert self.EXPECTED_KEYS == set(data.keys())

    def test_ch_only_patch_response_shape(self, auth_client, observe_project):
        session_id = uuid.uuid4()

        with mock.patch(
            "tracer.views.trace_session._resolve_ch_session_fields",
            return_value={
                "project_id": observe_project.id,
                "bookmarked": False,
                "display_name": "ch-session",
                "first_seen": timezone.now(),
            },
        ):
            response = auth_client.patch(
                f"/tracer/trace-session/{session_id}/",
                {"bookmarked": True},
                format="json",
            )

        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert self.EXPECTED_KEYS == set(data.keys())
        assert data["id"] == str(session_id)
        assert data["project"] == str(observe_project.id)
        assert data["bookmarked"] is True
        assert data["name"] == "ch-session"
        assert data["created_at"] is not None

    def test_pg_and_ch_created_at_use_same_format(
        self, auth_client, observe_project, trace_session
    ):
        """Both paths must serialize created_at to the same ISO format (Z suffix)."""
        first_seen = trace_session.created_at

        pg_response = auth_client.patch(
            f"/tracer/trace-session/{trace_session.id}/",
            {"bookmarked": True},
            format="json",
        )
        pg_created_at = pg_response.json()["created_at"]

        ch_session_id = uuid.uuid4()
        with mock.patch(
            "tracer.views.trace_session._resolve_ch_session_fields",
            return_value={
                "project_id": observe_project.id,
                "bookmarked": False,
                "display_name": "ch-session",
                "first_seen": first_seen,
            },
        ):
            ch_response = auth_client.patch(
                f"/tracer/trace-session/{ch_session_id}/",
                {"bookmarked": True},
                format="json",
            )
        ch_created_at = ch_response.json()["created_at"]

        assert pg_created_at == ch_created_at


@pytest.mark.integration
@pytest.mark.api
class TestTraceSessionUserIdFilterValidation:
    """Unsupported user_id filter operators must be rejected, not silently matched."""

    def test_contains_op_rejected(self, auth_client, observe_project):
        import json

        filters = json.dumps(
            [
                {
                    "column_id": "user_id",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "contains",
                        "filter_value": "alice",
                    },
                }
            ]
        )
        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(observe_project.id), "filters": filters},
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_starts_with_op_rejected(self, auth_client, observe_project):
        import json

        filters = json.dumps(
            [
                {
                    "column_id": "user_id",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "starts_with",
                        "filter_value": "ali",
                    },
                }
            ]
        )
        response = auth_client.get(
            "/tracer/trace-session/list_sessions/",
            {"project_id": str(observe_project.id), "filters": filters},
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_equals_op_accepted(self, auth_client, observe_project):
        import json

        filters = json.dumps(
            [
                {
                    "column_id": "user_id",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "alice",
                    },
                }
            ]
        )
        with mock.patch(
            "tracer.views.trace_session._resolve_end_user_ids_for_user_id",
            return_value=([], None),
        ):
            response = auth_client.get(
                "/tracer/trace-session/list_sessions/",
                {"project_id": str(observe_project.id), "filters": filters},
            )
        assert response.status_code != status.HTTP_400_BAD_REQUEST


# ===========================================================================
# Benchmark: Session List Query Latency (requires running ClickHouse)
# ===========================================================================


@pytest.mark.benchmark
class TestSessionListLatency:
    """Wall-time benchmarks for /tracer/trace-session/list_sessions/.

    These tests hit the real ClickHouse instance and measure end-to-end
    latency for common filter combinations. They skip automatically when
    CH is not reachable.

    Run with: pytest -m benchmark futureagi/tracer/tests/test_trace_session.py -v
    """

    @staticmethod
    def _ch_available():
        try:
            from tracer.services.clickhouse.client import (
                ClickHouseClient,
                is_clickhouse_enabled,
            )

            if not is_clickhouse_enabled():
                return False
            client = ClickHouseClient()
            client.execute_read("SELECT 1", {})
            return True
        except Exception:
            return False

    @staticmethod
    def _get_test_project_id():
        from tracer.services.clickhouse.client import ClickHouseClient

        client = ClickHouseClient()
        rows, _, _ = client.execute_read(
            "SELECT toString(project_id), count() AS n "
            "FROM spans WHERE is_deleted = 0 "
            "GROUP BY project_id ORDER BY n DESC LIMIT 1",
            {},
        )
        if rows:
            return (
                rows[0][0]
                if isinstance(rows[0], (list, tuple))
                else rows[0].get("project_id")
            )
        return None

    @pytest.fixture(autouse=True)
    def skip_if_no_ch(self):
        if not self._ch_available():
            pytest.skip("ClickHouse not reachable for benchmark")
        self.project_id = self._get_test_project_id()
        if not self.project_id:
            pytest.skip("No spans data in ClickHouse for benchmark")

    @pytest.fixture(autouse=True)
    def seed_benchmark_spans(self, skip_if_no_ch):
        """Seed 1000 sessions × 5 spans = 5000 spans, 30-day spread, 200 end_users."""
        from datetime import timedelta

        from tracer.services.clickhouse.client import ClickHouseClient

        client = ClickHouseClient()
        rows, _, _ = client.execute_read(
            "SELECT count() FROM spans WHERE is_deleted = 0 "
            "AND project_id = %(pid)s "
            "AND trace_session_id IS NOT NULL "
            "AND (parent_span_id IS NULL OR parent_span_id = '')",
            {"pid": self.project_id},
        )
        row_count = (
            rows[0][0]
            if rows and isinstance(rows[0], (list, tuple))
            else (rows[0].get("count()", 0) if rows else 0)
        )

        eu_rows, _, _ = client.execute_read(
            "SELECT count() FROM end_users WHERE project_id = %(pid)s AND is_deleted = 0",
            {"pid": self.project_id},
        )
        eu_count = (
            eu_rows[0][0]
            if eu_rows and isinstance(eu_rows[0], (list, tuple))
            else (eu_rows[0].get("count()", 0) if eu_rows else 0)
        )

        if row_count >= 1000 and eu_count >= 100:
            return

        now = (
            datetime.now(timezone.utc)
            if hasattr(timezone, "utc")
            else datetime.utcnow()
        )
        import os

        import clickhouse_connect
        from django.conf import settings

        ch_settings = getattr(settings, "CLICKHOUSE", {})
        ch = clickhouse_connect.get_client(
            host=ch_settings.get("CH_HOST", "localhost"),
            port=int(os.environ.get("CH_HTTP_PORT", 8123)),
            username=ch_settings.get("CH_USERNAME", "default"),
            password=ch_settings.get("CH_PASSWORD", "") or "",
            database=ch_settings.get("CH_DATABASE", "test_tfc"),
        )

        num_sessions = 1000
        spans_per_session = 5
        session_ids = [str(uuid.uuid4()) for _ in range(num_sessions)]
        end_user_ids = [str(uuid.uuid4()) for _ in range(200)]

        batch_values = []
        for s_idx, sid in enumerate(session_ids):
            trace_id = str(uuid.uuid4())
            session_start = now - timedelta(days=s_idx % 30, hours=s_idx % 24)

            for sp_idx in range(spans_per_session):
                span_id = f"bench_{uuid.uuid4().hex[:16]}"
                start = session_start + timedelta(seconds=sp_idx * 3)
                end = start + timedelta(seconds=2)
                is_root = sp_idx == 0
                euid = end_user_ids[s_idx % len(end_user_ids)]

                batch_values.append(
                    f"('{span_id}', '{trace_id}', '{self.project_id}', '{sid}', "
                    f"'{euid}', 'llm', "
                    f"'{start.strftime('%Y-%m-%d %H:%M:%S')}', "
                    f"'{end.strftime('%Y-%m-%d %H:%M:%S')}', "
                    f"{100 + sp_idx * 50}, {0.001 * (sp_idx + 1)}, "
                    f"{10 * (sp_idx + 1)}, {5 * (sp_idx + 1)}, {5 * (sp_idx + 1)}, "
                    f"'{'ERROR' if s_idx % 20 == 0 else 'OK'}', 0, "
                    f"{'NULL' if is_root else repr(span_id + '_parent')}, "
                    f"'bench_span_{s_idx}_{sp_idx}', "
                    f"'hello session {s_idx}', 'response {s_idx}')"
                )

                if len(batch_values) >= 500:
                    ch.command(
                        "INSERT INTO spans "
                        "(id, trace_id, project_id, trace_session_id, end_user_id, "
                        "observation_type, start_time, end_time, latency_ms, cost, "
                        "total_tokens, prompt_tokens, completion_tokens, status, "
                        "is_deleted, parent_span_id, name, input, output) VALUES "
                        + ", ".join(batch_values)
                    )
                    batch_values = []

        if batch_values:
            ch.command(
                "INSERT INTO spans "
                "(id, trace_id, project_id, trace_session_id, end_user_id, "
                "observation_type, start_time, end_time, latency_ms, cost, "
                "total_tokens, prompt_tokens, completion_tokens, status, "
                "is_deleted, parent_span_id, name, input, output) VALUES "
                + ", ".join(batch_values)
            )

        org_id = "00000000-0000-0000-0000-000000000001"
        eu_values = []
        for i, euid in enumerate(end_user_ids):
            eu_values.append(
                f"('{self.project_id}', '{euid}', '{org_id}', "
                f"'bench_user_{i}', 'email', '', '{{}}', "
                f"'{now.strftime('%Y-%m-%d %H:%M:%S')}', "
                f"'{now.strftime('%Y-%m-%d %H:%M:%S')}', 0)"
            )
        ch.command(
            "INSERT INTO end_users "
            "(project_id, end_user_id, organization_id, user_id, "
            "user_id_type, user_id_hash, metadata, first_seen, version, is_deleted) "
            "VALUES " + ", ".join(eu_values)
        )

    def _run_session_list_query(
        self, filters, project_id=None, sort_params=None, page_number=0, page_size=30
    ):
        import time

        from tracer.services.clickhouse.query_service import AnalyticsQueryService
        from tracer.services.clickhouse.v2.dispatch import get_query_builder_class

        _Cls = get_query_builder_class("SESSION_LIST")
        builder = _Cls(
            project_id=project_id or self.project_id,
            page_number=page_number,
            page_size=page_size,
            filters=filters,
            sort_params=sort_params or [],
        )
        analytics = AnalyticsQueryService()
        query, params = builder.build()

        t0 = time.time()
        result = analytics.execute_ch_query(query, params, timeout_ms=15000)
        main_ms = (time.time() - t0) * 1000

        session_ids = [str(row.get("session_id", "")) for row in result.data[:30]]

        enrichment_ms = 0
        if session_ids:
            from concurrent.futures import ThreadPoolExecutor

            def _content():
                cq, cp = builder.build_content_query(session_ids)
                if cq:
                    analytics.execute_ch_query(cq, cp, timeout_ms=10000)

            def _attrs():
                aq, ap = builder.build_span_attributes_query(session_ids)
                if aq:
                    analytics.execute_ch_query(aq, ap, timeout_ms=5000)

            t1 = time.time()
            with ThreadPoolExecutor(max_workers=3) as pool:
                pool.submit(_content)
                pool.submit(_attrs)
            enrichment_ms = (time.time() - t1) * 1000

        return main_ms, enrichment_ms, len(session_ids)

    def test_latency_with_project_id_and_time_filter(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(filters)
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] project_id + time: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert count >= 30, (
            f"Benchmark should find seeded sessions (got {count}, expected >=30)"
        )
        assert total < 3000, (
            f"Session list with project_id took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_with_project_id_and_cost_filter(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            },
            {
                "column_id": "total_cost",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 0,
                },
            },
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(filters)
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] project_id + time + cost>0: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert total < 3000, (
            f"Session list with cost filter took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_without_project_id(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(
            filters, project_id=None
        )
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] no project_id + time: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert total < 3000, (
            f"Session list without project_id took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_with_sort_by_duration(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(
            filters, sort_params=[{"column_id": "duration", "direction": "desc"}]
        )
        total = main_ms + enrich_ms
        print(f"\n  [BENCHMARK] sort by duration DESC: {total:.0f}ms sessions={count}")
        assert total < 2000, (
            f"Session list sorted by duration took {total:.0f}ms (threshold: 2000ms)"
        )

    def test_latency_with_tokens_having_filter(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            },
            {
                "column_id": "total_tokens",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 0,
                },
            },
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(filters)
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] tokens>0 HAVING: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert total < 3000, (
            f"Session list with tokens HAVING took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_with_traces_count_filter(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            },
            {
                "column_id": "traces_count",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than_or_equal",
                    "filter_value": 1,
                },
            },
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(filters)
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] traces_count>=1 HAVING: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert total < 3000, (
            f"Session list with traces_count HAVING took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_sort_by_cost_asc(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(
            filters, sort_params=[{"column_id": "total_cost", "direction": "asc"}]
        )
        total = main_ms + enrich_ms
        print(f"\n  [BENCHMARK] sort by cost ASC: {total:.0f}ms sessions={count}")
        assert total < 2000, (
            f"Session list sorted by cost took {total:.0f}ms (threshold: 2000ms)"
        )

    def test_latency_narrow_time_range_24h(self):
        from datetime import timedelta

        now = datetime.utcnow()
        start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [start, end],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(filters)
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] 24h window: main={main_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total:.0f}ms sessions={count}"
        )
        assert total < 1500, (
            f"Session list with 24h window took {total:.0f}ms (threshold: 1500ms)"
        )

    def test_latency_combined_filters_and_sort(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            },
            {
                "column_id": "total_tokens",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 5,
                },
            },
            {
                "column_id": "total_cost",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 0,
                },
            },
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(
            filters, sort_params=[{"column_id": "duration", "direction": "desc"}]
        )
        total = main_ms + enrich_ms
        print(
            f"\n  [BENCHMARK] tokens+cost+sort_duration: {total:.0f}ms sessions={count}"
        )
        assert total < 3000, (
            f"Combined filters + sort took {total:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_page_2(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        main_ms, enrich_ms, count = self._run_session_list_query(
            filters, page_number=1, page_size=10
        )
        total = main_ms + enrich_ms
        print(f"\n  [BENCHMARK] page 2 (offset 10): {total:.0f}ms sessions={count}")
        assert total < 2000, (
            f"Session list page 2 took {total:.0f}ms (threshold: 2000ms)"
        )


@pytest.mark.benchmark
class TestUserListLatency:
    @staticmethod
    def _ch_available():
        try:
            from tracer.services.clickhouse.client import (
                ClickHouseClient,
                is_clickhouse_enabled,
            )

            if not is_clickhouse_enabled():
                return False
            client = ClickHouseClient()
            client.execute_read("SELECT 1", {})
            return True
        except Exception:
            return False

    @staticmethod
    def _get_test_project_id():
        from tracer.services.clickhouse.client import ClickHouseClient

        client = ClickHouseClient()
        rows, _, _ = client.execute_read(
            "SELECT toString(project_id), count() AS n "
            "FROM spans WHERE is_deleted = 0 "
            "AND end_user_id IS NOT NULL "
            "GROUP BY project_id ORDER BY n DESC LIMIT 1",
            {},
        )
        if rows:
            return (
                rows[0][0]
                if isinstance(rows[0], (list, tuple))
                else rows[0].get("project_id")
            )
        return None

    @pytest.fixture(autouse=True)
    def skip_if_no_ch(self):
        if not self._ch_available():
            pytest.skip("ClickHouse not reachable for benchmark")
        self.project_id = self._get_test_project_id()
        if not self.project_id:
            pytest.skip("No spans with end_user_id in ClickHouse for benchmark")

    def _run_user_list_query(self, filters, sort_params=None):
        import time

        from tracer.services.clickhouse.query_service import AnalyticsQueryService
        from tracer.services.clickhouse.v2.query_builders.user_list import (
            UserListQueryBuilderV2,
        )

        builder = UserListQueryBuilderV2(
            organization_id="00000000-0000-0000-0000-000000000001",
            project_ids=[self.project_id],
            filters=filters,
            sort_params=sort_params or [],
            limit=30,
            offset=0,
        )
        analytics = AnalyticsQueryService()
        query, params = builder.build()

        t0 = time.time()
        result = analytics.execute_ch_query(query, params, timeout_ms=15000)
        total_ms = (time.time() - t0) * 1000

        return total_ms, len(result.data)

    def test_latency_default_time_range(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        ms, count = self._run_user_list_query(filters)
        print(f"\n  [BENCHMARK] users default: {ms:.0f}ms users={count}")
        assert count > 0, f"Expected users, got {count}"
        assert ms < 3000, f"User list took {ms:.0f}ms (threshold: 3000ms)"

    def test_latency_sort_by_cost(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        ms, count = self._run_user_list_query(
            filters, sort_params=[{"column_id": "total_cost", "direction": "desc"}]
        )
        print(f"\n  [BENCHMARK] users sort by cost: {ms:.0f}ms users={count}")
        assert ms < 3000, (
            f"User list sorted by cost took {ms:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_sort_by_tokens(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        ms, count = self._run_user_list_query(
            filters, sort_params=[{"column_id": "total_tokens", "direction": "desc"}]
        )
        print(f"\n  [BENCHMARK] users sort by tokens: {ms:.0f}ms users={count}")
        assert ms < 3000, (
            f"User list sorted by tokens took {ms:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_sort_by_trace_count(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        ms, count = self._run_user_list_query(
            filters, sort_params=[{"column_id": "trace_count", "direction": "desc"}]
        )
        print(f"\n  [BENCHMARK] users sort by trace_count: {ms:.0f}ms users={count}")
        assert ms < 3000, (
            f"User list sorted by trace_count took {ms:.0f}ms (threshold: 3000ms)"
        )

    def test_latency_narrow_24h_window(self):
        from datetime import timedelta

        now = datetime.utcnow()
        start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [start, end],
                },
            }
        ]
        ms, count = self._run_user_list_query(filters)
        print(f"\n  [BENCHMARK] users 24h window: {ms:.0f}ms users={count}")
        assert ms < 2000, f"User list 24h took {ms:.0f}ms (threshold: 2000ms)"

    def test_latency_page_2(self):
        import time

        from tracer.services.clickhouse.query_service import AnalyticsQueryService
        from tracer.services.clickhouse.v2.query_builders.user_list import (
            UserListQueryBuilderV2,
        )

        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-01-01T00:00:00.000Z",
                        "2026-12-31T23:59:59.000Z",
                    ],
                },
            }
        ]
        builder = UserListQueryBuilderV2(
            organization_id="00000000-0000-0000-0000-000000000001",
            project_ids=[self.project_id],
            filters=filters,
            sort_params=[],
            limit=10,
            offset=10,
        )
        analytics = AnalyticsQueryService()
        query, params = builder.build()

        t0 = time.time()
        result = analytics.execute_ch_query(query, params, timeout_ms=15000)
        ms = (time.time() - t0) * 1000
        print(f"\n  [BENCHMARK] users page 2: {ms:.0f}ms users={len(result.data)}")
        assert ms < 3000, f"User list page 2 took {ms:.0f}ms (threshold: 3000ms)"

    def test_latency_wide_6_month_range(self):
        filters = [
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        "2025-12-01T00:00:00.000Z",
                        "2026-06-30T23:59:59.000Z",
                    ],
                },
            }
        ]
        ms, count = self._run_user_list_query(filters)
        print(f"\n  [BENCHMARK] users 6-month range: {ms:.0f}ms users={count}")
        assert ms < 5000, f"User list 6-month took {ms:.0f}ms (threshold: 5000ms)"
