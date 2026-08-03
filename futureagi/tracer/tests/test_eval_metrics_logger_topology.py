"""Regression coverage for raw eval-metric graph logger reads.

The eval logger intentionally has no ``project_id`` column in either the
legacy PeerDB or direct-write topology. Tenant authorization happens against
``CustomEvalConfig`` before graph dispatch; the ClickHouse read must then stay
bound to that globally unique config (and to project-owned trace candidates
when row filters are present).
"""

from __future__ import annotations

from datetime import datetime
from unittest import mock

import pytest

from tracer.models.custom_eval_config import CustomEvalConfig
from tracer.services.clickhouse.query_builders.eval_metrics import (
    EvalMetricsQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders.eval_metrics import (
    EvalMetricsQueryBuilderV2,
)
from tracer.utils.graphs_optimized import get_eval_graph_data

PROJECT_ID = "11111111-1111-4111-8111-111111111111"
EVAL_CONFIG_ID = "22222222-2222-4222-8222-222222222222"
START = datetime(2026, 7, 20)
END = datetime(2026, 8, 3)


@pytest.mark.unit
def test_eval_graph_common_boundary_scopes_config_to_request_project():
    """Reject a foreign config before a project-less raw logger read can run."""

    with mock.patch.object(
        CustomEvalConfig.objects, "select_related"
    ) as select_related:
        scoped_configs = select_related.return_value
        scoped_configs.get.side_effect = CustomEvalConfig.DoesNotExist

        with pytest.raises(ValueError, match="Custom eval config does not exist"):
            get_eval_graph_data(
                interval="day",
                filters=[],
                property="average",
                observe_type="charts",
                req_data_config={"id": EVAL_CONFIG_ID, "type": "EVAL"},
                eval_logger_filters={"project_id": PROJECT_ID},
            )

    select_related.assert_called_once_with("eval_template")
    scoped_configs.get.assert_called_once_with(
        id=EVAL_CONFIG_ID,
        project_id=PROJECT_ID,
        deleted=False,
    )


def _raw_builder(
    output_type: str,
    *,
    filters: list[dict] | None = None,
) -> EvalMetricsQueryBuilderV2:
    return EvalMetricsQueryBuilderV2(
        custom_eval_config_id=EVAL_CONFIG_ID,
        project_id=PROJECT_ID,
        start_date=START,
        end_date=END,
        interval="day",
        eval_output_type=output_type,
        choices=["accepted", "rejected"] if output_type == "CHOICES" else None,
        use_preaggregated=False,
        filters=filters or [],
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("output_type", "metric_expression"),
    [
        ("SCORE", "avg(output_float)"),
        ("PASS_FAIL", "output_bool = 1"),
        ("CHOICES", "JSONExtract(output_str_list, 'Array(String)')"),
    ],
)
@pytest.mark.parametrize(
    ("logger_table", "live_predicate", "foreign_live_column"),
    [
        (
            "tracer_eval_logger",
            "(deleted = 0 OR deleted IS NULL)",
            "is_deleted = 0",
        ),
        (
            "tracer_eval_logger_v2",
            "is_deleted = 0",
            "deleted = 0 OR deleted IS NULL",
        ),
    ],
)
def test_raw_terminal_graph_uses_configured_logger_without_project_column(
    settings,
    output_type,
    metric_expression,
    logger_table,
    live_predicate,
    foreign_live_column,
):
    """Reproduce the three production Code 47 terminal graph failures."""

    settings.CH25_EVAL_LOGGER_TABLE = logger_table

    query, params = _raw_builder(output_type).build()
    normalized = " ".join(query.split())

    assert f"FROM {logger_table} FINAL" in normalized
    assert live_predicate in normalized
    assert foreign_live_column not in normalized
    assert metric_expression in normalized
    # Neither physical logger has project_id. The config UUID is the authorized
    # tenant anchor for this unfiltered raw query.
    assert "project_id" not in normalized
    assert "custom_eval_config_id = toUUID(%(eval_config_id)s)" in normalized
    assert "created_at >= %(start_date)s" in normalized
    assert "created_at < %(end_date)s" in normalized
    assert params["eval_config_id"] == EVAL_CONFIG_ID
    assert params["start_date"] == START
    assert params["end_date"] == END
    assert EVAL_CONFIG_ID not in query


@pytest.mark.unit
def test_filtered_raw_graph_keeps_project_scope_on_span_candidates(settings):
    """Removing the nonexistent logger column must not remove tenant scope."""

    settings.CH25_EVAL_LOGGER_TABLE = "tracer_eval_logger"
    filters = [
        {
            "column_id": "status",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "OK",
            },
        }
    ]

    query, params = _raw_builder("SCORE", filters=filters).build()
    normalized = " ".join(query.split())
    logger_scope, candidate_scope = normalized.split(
        "AND trace_id IN (SELECT DISTINCT trace_id FROM spans", 1
    )

    assert "project_id" not in logger_scope
    assert "custom_eval_config_id = toUUID(%(eval_config_id)s)" in logger_scope
    assert "WHERE project_id = %(project_id)s AND is_deleted = 0" in candidate_scope
    assert "lowerUTF8(toString(status)) =" in candidate_scope
    assert params["project_id"] == PROJECT_ID
    assert params["eval_config_id"] == EVAL_CONFIG_ID
    assert "ok" in params.values()


@pytest.mark.unit
def test_preaggregated_eval_graph_remains_directly_project_scoped(settings):
    """Only raw logger reads omit project_id; the rollup owns that column."""

    settings.CH25_EVAL_LOGGER_TABLE = "tracer_eval_logger_v2"
    builder = EvalMetricsQueryBuilder(
        custom_eval_config_id=EVAL_CONFIG_ID,
        project_id=PROJECT_ID,
        start_date=START,
        end_date=END,
        interval="day",
        eval_output_type="SCORE",
        use_preaggregated=True,
    )

    query, params = builder.build()
    normalized = " ".join(query.split())

    assert "FROM eval_metrics_hourly" in normalized
    assert "WHERE project_id = %(project_id)s" in normalized
    assert "tracer_eval_logger" not in normalized
    assert params["project_id"] == PROJECT_ID
