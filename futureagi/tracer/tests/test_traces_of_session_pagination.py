"""Regression test for TH-5574 — Trace View selection counts off by one.

The bounded direct-write reader fetches a ``page_size + 1`` has-more sentinel,
then returns exactly ``page_size`` rows plus explicit ``has_more`` metadata.
The consuming view must preserve that contract so "select all on this page"
never reports 26 selections for a 25-row page.

This pins the handoff in ``TraceView._list_traces_of_session_clickhouse`` (the
``list_traces_of_session`` endpoint named in the ticket).
"""

import uuid
from types import SimpleNamespace
from unittest import mock

import pytest

from tracer.selectors.trace_filter_reads import BoundedFilterPage


@pytest.mark.unit
class TestTracesOfSessionPagination:
    def _make_view(self):
        from tracer.views.trace import TraceView

        view = TraceView.__new__(TraceView)
        view._gm = SimpleNamespace(
            success_response=lambda payload: ("ok", payload),
            bad_request=lambda msg: ("bad_request", msg),
            custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
        )
        return view

    def _make_request(self, *, page_size):
        org = SimpleNamespace(id=uuid.uuid4())
        return SimpleNamespace(
            query_params={"page_number": "0", "page_size": str(page_size)},
            organization=org,
            user=SimpleNamespace(organization=org),
        )

    def _routing_analytics(self, *, trace_rows, content_complete=True):
        """Return exact page-scoped enrichment rows without a ClickHouse hit."""

        def _side_effect(query, params=None, **kwargs):
            if content_complete and params and params.get("content_trace_ids"):
                return SimpleNamespace(
                    data=[
                        {
                            "trace_id": str(trace_id),
                            "input": None,
                            "output": None,
                            "attrs_string": {},
                            "attrs_number": {},
                            "attrs_bool": {},
                            "attributes_extra": "{}",
                            "metadata": "{}",
                            "trace_tags": [],
                        }
                        for trace_id in params["content_trace_ids"]
                    ]
                )
            return SimpleNamespace(data=[])

        analytics = mock.MagicMock()
        analytics.execute_ch_query.side_effect = _side_effect
        return analytics

    @staticmethod
    def _bounded_page(trace_rows, *, total, has_more=False):
        return BoundedFilterPage(
            rows=list(trace_rows),
            has_more=has_more,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=total,
            elapsed_ms=1.0,
            query_count=1,
            rows_returned=len(trace_rows),
            result_payload_bytes=1,
            attempts=(),
        )

    def test_page_trimmed_to_page_size(self):
        """A page that fetched page_size + 1 rows returns exactly page_size."""
        page_size = 25
        view = self._make_view()
        request = self._make_request(page_size=page_size)

        # The bounded V2 reader consumes its sentinel internally and exposes it
        # as ``has_more``; the view receives exactly one public page.
        trace_rows = [{"trace_id": str(uuid.uuid4())} for _ in range(page_size)]
        total = 40
        analytics = self._routing_analytics(trace_rows=trace_rows)

        with (
            mock.patch("tracer.views.trace.CustomEvalConfig") as mock_cfg,
            mock.patch(
                "tracer.views.trace.get_annotation_labels_for_project",
                return_value=[],
            ),
            mock.patch(
                "tracer.views.trace._build_annotation_map_from_scores",
                return_value={},
            ),
            mock.patch(
                "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
                return_value=self._bounded_page(trace_rows, total=total, has_more=True),
            ),
        ):
            # No eval configs for this project → discovery short-circuits with
            # candidate_ids == [] (no PG/CH eval round-trip). This test pins the
            # pagination trim, not eval columns.
            mock_cfg.objects.filter.return_value.select_related.return_value = []
            status, payload = view._list_traces_of_session_clickhouse(
                request,
                project_id=str(uuid.uuid4()),
                # Pagination now comes from the serializer-validated query data
                # (request.validated_query_data), not request.query_params.
                validated_data={
                    "filters": [],
                    "page_number": 0,
                    "page_size": page_size,
                    "allow_sampled": True,
                },
                analytics=analytics,
                org_project_ids=None,
                org=request.organization,
            )

        assert status == "ok"
        # The sentinel row must be trimmed — exactly page_size, not page_size + 1.
        assert len(payload["table"]) == page_size
        # total_rows comes from the (correct) uniq() count, unchanged by the trim.
        assert payload["metadata"]["total_rows"] == total

    def test_span_trace_map_skipped_without_annotation_labels(self):
        """No annotation labels -> the annotation map is a guaranteed no-op,
        so the span->trace map query must not run at all."""
        view = self._make_view()
        request = self._make_request(page_size=5)
        trace_rows = [{"trace_id": str(uuid.uuid4())} for _ in range(3)]
        analytics = self._routing_analytics(trace_rows=trace_rows)

        with (
            mock.patch("tracer.views.trace.CustomEvalConfig") as mock_cfg,
            mock.patch(
                "tracer.views.trace.get_annotation_labels_for_project",
                return_value=[],
            ),
            mock.patch(
                "tracer.views.trace._build_annotation_map_from_scores",
                return_value={},
            ),
            mock.patch(
                "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
                return_value=self._bounded_page(trace_rows, total=3),
            ),
        ):
            mock_cfg.objects.filter.return_value.select_related.return_value = []
            status, _ = view._list_traces_of_session_clickhouse(
                request,
                project_id=str(uuid.uuid4()),
                validated_data={
                    "filters": [],
                    "page_number": 0,
                    "page_size": 5,
                    "allow_sampled": True,
                },
                analytics=analytics,
                org_project_ids=None,
                org=request.organization,
            )

        assert status == "ok"
        analytics.get_span_trace_map.assert_not_called()

    @pytest.mark.django_db
    def test_span_trace_map_runs_with_annotation_labels(self):
        """With labels present the span->trace map runs, scoped to project + window."""
        view = self._make_view()
        request = self._make_request(page_size=5)
        trace_rows = [{"trace_id": str(uuid.uuid4())} for _ in range(3)]
        analytics = self._routing_analytics(trace_rows=trace_rows)
        label = mock.Mock()
        label.id = uuid.uuid4()
        label.type = "text"
        label.name = "Quality"
        label.settings = {}
        project_id = str(uuid.uuid4())

        with (
            mock.patch("tracer.views.trace.CustomEvalConfig") as mock_cfg,
            mock.patch(
                "tracer.views.trace.get_annotation_labels_for_project",
                return_value=[label],
            ),
            mock.patch(
                "tracer.views.trace._build_annotation_map_from_scores",
                return_value={},
            ),
            mock.patch(
                "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
                return_value=self._bounded_page(trace_rows, total=3),
            ),
        ):
            mock_cfg.objects.filter.return_value.select_related.return_value = []
            status, _ = view._list_traces_of_session_clickhouse(
                request,
                project_id=project_id,
                validated_data={
                    "filters": [],
                    "page_number": 0,
                    "page_size": 5,
                    "allow_sampled": True,
                },
                analytics=analytics,
                org_project_ids=None,
                org=request.organization,
            )

        assert status == "ok"
        analytics.get_span_trace_map.assert_called_once()
        assert analytics.get_span_trace_map.call_args.kwargs["project_id"] == project_id

    def test_content_shortfall_returns_retryable_error(self):
        """A latest-state content replay shortfall must fail closed."""
        view = self._make_view()
        request = self._make_request(page_size=5)
        trace_rows = [{"trace_id": str(uuid.uuid4())} for _ in range(3)]
        analytics = self._routing_analytics(
            trace_rows=trace_rows,
            content_complete=False,
        )

        with (
            mock.patch("tracer.views.trace.CustomEvalConfig") as mock_cfg,
            mock.patch(
                "tracer.views.trace.get_annotation_labels_for_project",
                return_value=[],
            ),
            mock.patch(
                "tracer.views.trace._build_annotation_map_from_scores",
                return_value={},
            ),
            mock.patch(
                "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
                return_value=self._bounded_page(trace_rows, total=3),
            ),
            mock.patch("tracer.views.trace.logger") as mock_logger,
        ):
            mock_cfg.objects.filter.return_value.select_related.return_value = []
            response = view._list_traces_of_session_clickhouse(
                request,
                project_id=str(uuid.uuid4()),
                validated_data={"filters": [], "page_number": 0, "page_size": 5},
                analytics=analytics,
                org_project_ids=None,
                org=request.organization,
            )

        assert response[0] == "error"
        assert response[1][0] == 503
        assert response[2]["code"] == "service_unavailable"
        warning_events = [
            c.args[0] for c in mock_logger.warning.call_args_list if c.args
        ]
        assert "trace_list_content_replay_incomplete" in warning_events
