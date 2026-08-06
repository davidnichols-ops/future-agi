"""Fail-closed snapshot and partitioning tests for exact dashboards."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from clickhouse_driver.errors import ServerException

from tracer.services.clickhouse.dashboard_snapshot import (
    DashboardRelationSnapshotError,
    capture_dashboard_relation_snapshot,
    dashboard_physical_relations,
)
from tracer.services.clickhouse.query_builders.dataset_dashboard import (
    DatasetQueryBuilder,
)
from tracer.services.clickhouse.query_builders.simulation_dashboard import (
    SimulationQueryBuilder,
)
from tracer.services.clickhouse.query_service import AnalyticsQueryService
from tracer.views.dashboard import (
    _DASHBOARD_TRACE_READ_SETTINGS,
    DashboardExactReadError,
    DashboardViewSet,
    DashboardWidgetViewSet,
    _fetch_exact_dashboard_rows,
)


class _SnapshotAnalytics:
    supports_per_query_read_settings = True

    def __init__(self, *, failure: Exception | None = None):
        self.failure = failure
        self.calls: list[tuple[str, dict, int, dict]] = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, dict(params), timeout_ms, dict(settings)))
        if self.failure is not None:
            raise self.failure
        if "now64" in query:
            ceiling = 900
        else:
            table_match = re.search(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)", query)
            assert table_match is not None
            ceiling = {
                "model_hub_cell": 101,
                "model_hub_dataset": 202,
                "model_hub_score": 303,
                "usage_apicalllog": 404,
                "end_users": 505,
            }.get(table_match.group(1), 606)
        return SimpleNamespace(
            data=[{"version_ceiling": ceiling}],
            columns=["version_ceiling"],
        )


class _LegacyBudgetClient:
    server_enforced_readonly = False

    def __init__(self, *, failing_leaf_hours=()):
        self.failing_leaf_hours = set(failing_leaf_hours)
        self.calls: list[tuple[str, dict, int, dict]] = []

    def execute_read(self, query, params, *, timeout_ms, settings):
        copied = dict(params)
        self.calls.append((query, copied, timeout_ms, dict(settings)))
        duration = params["end_date"] - params["start_date"]
        if duration > timedelta(hours=1):
            raise ServerException("private read budget detail", code=159)
        if params["start_date"].hour in self.failing_leaf_hours:
            raise ServerException("private leaf budget detail", code=159)
        return (
            [(params["start_date"], 1)],
            [("time_bucket", "DateTime"), ("value", "UInt64")],
            1.0,
        )


def _legacy_analytics(client):
    analytics = AnalyticsQueryService()
    analytics._ch_client = client
    return analytics


def _partition_params(hours: int):
    return {
        "start_date": datetime(2026, 8, 1, tzinfo=UTC),
        "end_date": datetime(2026, 8, 1, tzinfo=UTC) + timedelta(hours=hours),
        "workspace_id": "00000000-0000-0000-0000-000000000001",
    }


@pytest.mark.unit
def test_relation_parser_ignores_literals_comments_and_cte_aliases():
    sql = """
    WITH recent AS (
      SELECT * FROM `spans` FINAL
      WHERE marker = 'FROM fake_table JOIN dashboard_attr_rollup'
    )
    SELECT * FROM recent
    -- JOIN unsupported_comment
    JOIN traces FINAL USING (trace_id)
    """

    assert dashboard_physical_relations(sql) == frozenset({"spans", "traces"})


@pytest.mark.unit
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM unsupported_relation",
        "SELECT * FROM dashboard_attr_rollup",
        "SELECT 1",
    ],
)
def test_unsupported_or_unversioned_relation_fails_closed(sql):
    analytics = _SnapshotAnalytics()

    with pytest.raises(DashboardRelationSnapshotError):
        capture_dashboard_relation_snapshot(
            analytics=analytics,
            sql_statements=[sql],
            base_settings={},
            timeout_ms=300_000,
        )

    assert analytics.calls == []


@pytest.mark.unit
def test_two_metrics_share_one_ceiling_per_relation_and_scope_is_frozen():
    analytics = _SnapshotAnalytics()
    statements = [
        "SELECT count() FROM model_hub_cell FINAL "
        "JOIN model_hub_dataset FINAL USING (dataset_id)",
        "SELECT avg(response_time) FROM model_hub_cell FINAL "
        "JOIN model_hub_dataset FINAL USING (dataset_id)",
    ]

    snapshot = capture_dashboard_relation_snapshot(
        analytics=analytics,
        sql_statements=statements,
        base_settings={"max_threads": 2},
        timeout_ms=300_000,
    )

    assert snapshot.tables == ("model_hub_cell", "model_hub_dataset")
    assert snapshot.snapshot_query_count == 2
    assert len(analytics.calls) == 2
    filters = snapshot.settings["additional_table_filters"]
    assert filters["model_hub_cell"] == "_peerdb_version < 101"
    assert filters["model_hub_dataset"] == "_peerdb_version < 202"


@pytest.mark.unit
def test_existing_frozen_ceiling_is_reused_without_recapture():
    analytics = _SnapshotAnalytics()

    snapshot = capture_dashboard_relation_snapshot(
        analytics=analytics,
        sql_statements=["SELECT * FROM model_hub_score FINAL"],
        base_settings={
            "additional_table_filters": {"model_hub_score": "_peerdb_version < 77"}
        },
        timeout_ms=300_000,
    )

    assert snapshot.version_ceilings == {"model_hub_score": 77}
    assert snapshot.snapshot_query_count == 0
    assert analytics.calls == []


@pytest.mark.unit
def test_combined_direct_eval_annotation_and_dimension_relations_are_all_frozen():
    analytics = _SnapshotAnalytics()
    sql = """
    SELECT * FROM spans FINAL
    JOIN traces FINAL USING (trace_id)
    JOIN tracer_eval_logger_v2 FINAL USING (trace_id)
    JOIN model_hub_score FINAL USING (trace_id)
    JOIN usage_apicalllog FINAL USING (trace_id)
    JOIN end_users FINAL USING (project_id)
    """

    snapshot = capture_dashboard_relation_snapshot(
        analytics=analytics,
        sql_statements=[sql],
        base_settings={},
        timeout_ms=300_000,
    )

    assert snapshot.snapshot_query_count == 4
    assert snapshot.version_ceilings["spans"] == 900
    assert snapshot.version_ceilings["traces"] == 900
    assert snapshot.version_ceilings["tracer_eval_logger_v2"] == 900
    assert snapshot.version_ceilings["model_hub_score"] == 303
    assert snapshot.version_ceilings["usage_apicalllog"] == 404
    assert snapshot.version_ceilings["end_users"] == 505


@pytest.mark.unit
def test_concurrent_metrics_cannot_mix_a_later_mutation_after_capture():
    analytics = _SnapshotAnalytics()
    snapshot = capture_dashboard_relation_snapshot(
        analytics=analytics,
        sql_statements=[
            "SELECT count() FROM model_hub_cell FINAL",
            "SELECT avg(response_time) FROM model_hub_cell FINAL",
        ],
        base_settings={},
        timeout_ms=300_000,
    )
    # Model a write arriving after the plan is frozen. Both concurrent metric
    # reads must continue to carry the captured 101 boundary.
    analytics.calls.clear()
    observed_filters = []
    builder = MagicMock()
    builder.metrics = [{"id": "count"}, {"id": "latency"}]
    builder.metric_info.side_effect = lambda metric: dict(metric)
    prepared = (
        (
            builder.metrics[0],
            "SELECT count() FROM model_hub_cell",
            _partition_params(1),
        ),
        (builder.metrics[1], "SELECT avg(x) FROM model_hub_cell", _partition_params(1)),
    )

    def fetch_rows(_sql, _params):
        observed_filters.append(dict(snapshot.settings["additional_table_filters"]))
        return []

    results = DashboardViewSet._run_metric_queries(
        builder,
        "datasets",
        fetch_rows,
        prepared_queries=prepared,
    )

    assert len(results) == 2
    assert observed_filters == [
        {"model_hub_cell": "_peerdb_version < 101"},
        {"model_hub_cell": "_peerdb_version < 101"},
    ]


@pytest.mark.unit
def test_capture_failure_and_locked_settings_fail_without_complete_snapshot():
    with pytest.raises(DashboardRelationSnapshotError):
        capture_dashboard_relation_snapshot(
            analytics=_SnapshotAnalytics(failure=TimeoutError("private timeout")),
            sql_statements=["SELECT * FROM spans FINAL"],
            base_settings={},
            timeout_ms=300_000,
        )

    locked = _SnapshotAnalytics()
    locked.supports_per_query_read_settings = False
    with pytest.raises(DashboardRelationSnapshotError):
        capture_dashboard_relation_snapshot(
            analytics=locked,
            sql_statements=["SELECT * FROM spans FINAL"],
            base_settings={},
            timeout_ms=300_000,
        )
    assert locked.calls == []


@pytest.mark.unit
def test_dashboard_worker_capture_failure_executes_no_metric_and_returns_no_payload():
    project_id = "00000000-0000-0000-0000-000000000010"
    workspace = SimpleNamespace(
        id="00000000-0000-0000-0000-000000000020",
        organization_id="00000000-0000-0000-0000-000000000030",
    )
    query_config = {
        "project_ids": [project_id],
        "granularity": "day",
        "time_range": {"preset": "7D"},
        "metrics": [
            {
                "id": "latency",
                "name": "latency",
                "type": "system_metric",
                "aggregation": "avg",
                "source": "traces",
            }
        ],
        "filters": [],
        "breakdowns": [],
    }
    project_queryset = MagicMock()
    project_queryset.count.return_value = 1
    metric_runner = MagicMock()

    with (
        patch(
            "tracer.views.dashboard._materialize_dashboard_query_scope",
            side_effect=lambda config, *_args, **_kwargs: config,
        ),
        patch(
            "tracer.views.dashboard.Project.objects.filter",
            return_value=project_queryset,
        ),
        patch(
            "tracer.views.dashboard.V2AnalyticsQueryService", return_value=MagicMock()
        ),
        patch(
            "tracer.views.dashboard.capture_dashboard_relation_snapshot",
            side_effect=DashboardRelationSnapshotError("private failure"),
        ),
        patch.object(DashboardViewSet, "_run_metric_queries", metric_runner),
    ):
        with pytest.raises(DashboardExactReadError):
            DashboardWidgetViewSet()._execute_ch_query_config(
                query_config,
                workspace,
                _exact_worker=True,
                cache_identity_override={
                    "workspace_id": workspace.id,
                    "query_config": query_config,
                },
            )

    metric_runner.assert_not_called()


@pytest.mark.unit
def test_dataset_exact_builder_exposes_source_and_scope_relations():
    metric = {
        "id": "column_name",
        "name": "column_name",
        "type": "system_metric",
        "aggregation": "count_distinct",
    }
    builder = DatasetQueryBuilder(
        {
            "workspace_id": "00000000-0000-0000-0000-000000000001",
            "granularity": "day",
            "time_range": {"preset": "12M"},
            "metrics": [metric],
            "filters": [],
            "breakdowns": [],
            "exact_snapshot_dimensions": True,
        }
    )

    sql, _params = builder.build_metric_query(metric)

    assert "dictGet" not in sql
    assert dashboard_physical_relations(sql) == frozenset(
        {"model_hub_cell", "model_hub_column", "model_hub_dataset"}
    )


@pytest.mark.unit
def test_simulation_exact_builder_exposes_source_and_dimension_relations():
    metric = {
        "id": "duration",
        "name": "duration",
        "type": "system_metric",
        "aggregation": "avg",
    }
    builder = SimulationQueryBuilder(
        {
            "workspace_id": "00000000-0000-0000-0000-000000000001",
            "granularity": "day",
            "time_range": {"preset": "12M"},
            "metrics": [metric],
            "filters": [],
            "breakdowns": [],
            "exact_snapshot_dimensions": True,
        }
    )

    sql, _params = builder.build_metric_query(metric)

    assert "dictGet" not in sql
    assert dashboard_physical_relations(sql) == frozenset(
        {
            "simulate_agent_definition",
            "simulate_agent_version",
            "simulate_call_execution",
            "simulate_run_test",
            "simulate_scenarios",
            "simulate_test_execution",
        }
    )


@pytest.mark.unit
def test_dataset_legacy_executor_adaptively_splits_with_one_frozen_snapshot():
    client = _LegacyBudgetClient()
    settings = {
        **_DASHBOARD_TRACE_READ_SETTINGS,
        "additional_table_filters": {
            "model_hub_cell": "_peerdb_version < 101",
            "model_hub_dataset": "_peerdb_version < 202",
        },
    }

    rows = _fetch_exact_dashboard_rows(
        analytics=_legacy_analytics(client),
        sql="SELECT exact dataset metric FROM model_hub_cell FINAL",
        params=_partition_params(4),
        granularity="hour",
        timeout_ms=300_000,
        settings=settings,
    )

    assert [row["time_bucket"].hour for row in rows] == [0, 1, 2, 3]
    assert {call[2] for call in client.calls} == {30_000, 300_000}
    assert all(call[3] == settings for call in client.calls)


@pytest.mark.unit
def test_simulation_indivisible_budget_failure_returns_no_partial_rows():
    client = _LegacyBudgetClient(failing_leaf_hours={0})
    published = []

    with pytest.raises(ServerException):
        rows = _fetch_exact_dashboard_rows(
            analytics=_legacy_analytics(client),
            sql="SELECT exact simulation metric FROM simulate_call_execution FINAL",
            params=_partition_params(1),
            granularity="hour",
            timeout_ms=300_000,
            settings={
                **_DASHBOARD_TRACE_READ_SETTINGS,
                "additional_table_filters": {
                    "simulate_call_execution": "_peerdb_version < 606"
                },
            },
        )
        published.extend(rows)

    assert published == []
    assert len(client.calls) == 1
    assert client.calls[0][2] == 300_000
