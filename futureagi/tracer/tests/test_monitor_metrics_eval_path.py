"""Monitor builder EVALUATION_METRICS: table + not-deleted predicate from
eval_logger_source(); span membership bounded; the eval SQL goes THROUGH the v2
rewrite so a spliced span-attribute filter fragment is translated to v2 columns.
Pure SQL-string assertions, no ClickHouse."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from django.test import override_settings

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilderV2,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
EVAL_CONFIG_ID = "22222222-2222-2222-2222-222222222222"
START = datetime(2026, 8, 1, tzinfo=UTC)
END = datetime(2026, 8, 8, tzinfo=UTC)

LEGACY_ND = "(deleted = 0 OR deleted IS NULL)"
ATTR_FILTER = {
    "span_attributes_filters": [
        {
            "column_id": "my.attr",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "x",
            },
        }
    ]
}


def _builder(
    cls: type[MonitorMetricsQueryBuilder] = MonitorMetricsQueryBuilder,
    output_type: str = "SCORE",
    filters=None,
) -> MonitorMetricsQueryBuilder:
    return cls(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID,
        eval_output_type=output_type,
        filters=filters,
    )


def _eval_sqls(
    cls: type[MonitorMetricsQueryBuilder] = MonitorMetricsQueryBuilder,
    filters=None,
) -> list[str]:
    b = _builder(cls, filters=filters)
    return [
        b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)[0],
        b.build_historical_stats_query(mm.EVALUATION_METRICS, START, END)[0],
        b.build_time_series_query(mm.EVALUATION_METRICS, START, END, 3600)[0],
    ]


def test_eval_legacy_table_default_predicate() -> None:
    with override_settings(CH25_EVAL_LOGGER_TABLE="tracer_eval_logger"):
        for sql in _eval_sqls():
            assert "FROM tracer_eval_logger FINAL" in sql
            assert "tracer_eval_logger_v2" not in sql
            assert (
                f"custom_eval_config_id = toUUID(%(eval_config_id)s) AND {LEGACY_ND}"
                in sql
            )
            # Rewrite-safe: no _peerdb token that the v2 rewriter would mangle.
            assert "_peerdb" not in sql


def test_eval_v2_table_uses_is_deleted() -> None:
    with override_settings(CH25_EVAL_LOGGER_TABLE="tracer_eval_logger_v2"):
        for sql in _eval_sqls():
            assert "FROM tracer_eval_logger_v2 FINAL" in sql
            assert (
                "custom_eval_config_id = toUUID(%(eval_config_id)s) AND is_deleted = 0"
                in sql
            )


def test_eval_span_subquery_windowed_on_span_time() -> None:
    # The metric window lives on the SPAN's created_at inside the membership
    # subquery (evals run async after their spans), with the same ±1-day
    # start_time pruning pads as every other spans query. An unbounded (or
    # 30-day) span set exploded to 105M ids at prod scale.
    for sql in _eval_sqls():
        subq = sql.split("observation_span_id IN (", 1)[1]
        assert "SELECT id FROM spans" in subq
        assert "created_at >= %(start_time)s AND created_at < %(end_time)s" in subq
        assert "start_time >= %(start_time)s - INTERVAL 1 DAY" in subq
        assert "start_time < %(end_time)s + INTERVAL 1 DAY" in subq
        assert "project_id = %(project_id)s" in subq
        assert "INTERVAL 30 DAY" not in sql


def test_eval_table_window_is_loose_lower_bound_only() -> None:
    # The eval row's own created_at gets only a skew-padded lower bound
    # (eval time >= span time >= window start): prunes the eval table
    # without dropping late-computed evals for in-window spans. No exact or
    # upper eval-time window — that measured "evals computed recently", not
    # "quality of recent activity" (8,577 vs 400 evals for the same hour).
    for sql in _eval_sqls():
        head = sql.split("observation_span_id IN (", 1)[0]
        assert "created_at >= %(start_time)s - INTERVAL 1 DAY" in head
        assert "created_at < %(end_time)s" not in head
        assert "created_at BETWEEN" not in sql


def test_eval_rows_exclude_non_completed_statuses() -> None:
    # Pending/running/skipped/errored work items carry NULL outputs that
    # would read as failures (a burst of newly-enqueued evals must not
    # depress the pass rate). Mirrors span_list.py / filters.py.
    for sql in _eval_sqls():
        head = sql.split("observation_span_id IN (", 1)[0]
        assert "error = 0" in head
        assert "ifNull(output_str, '') != 'ERROR'" in head
        for status in ("pending", "running", "skipped", "errored"):
            assert status in head
        assert "'completed'" not in head  # NOT-IN keeps empty/NULL status rows


def test_v1_eval_filter_emits_legacy_span_attr_token() -> None:
    # Sanity: the spliced filter fragment uses v1 map columns pre-rewrite.
    b = _builder(filters=ATTR_FILTER)
    assert b._filter_clause, "attr filter should compile to a clause"
    assert (
        "span_attr" in b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)[0]
    )


def test_v2_eval_filter_fragment_is_rewritten() -> None:
    # The regression: eval SQL must pass through the v2 rewrite so the spliced
    # span-attribute filter tokens become v2 columns (no CH Code 47).
    for sql in _eval_sqls(MonitorMetricsQueryBuilderV2, filters=ATTR_FILTER):
        assert "span_attr_str" not in sql
        assert "span_attr_num" not in sql
        assert "span_attr_bool" not in sql
        assert "attrs_" in sql


def test_eval_empty_window_yields_null_for_all_output_types() -> None:
    # avg over zero rows is NaN in CH; every output type must collapse to NULL
    # so the evaluator's no-data skip works.
    for output_type in ("SCORE", "PASS_FAIL", "CHOICES"):
        b = MonitorMetricsQueryBuilder(
            project_id=PROJECT_ID,
            eval_config_id=EVAL_CONFIG_ID,
            eval_output_type=output_type,
            threshold_metric_value="Passed" if output_type != "SCORE" else None,
        )
        assert (
            "ifNotFinite("
            in b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)[0]
        )
        assert (
            b.build_historical_stats_query(mm.EVALUATION_METRICS, START, END)[0].count(
                "ifNotFinite("
            )
            >= 2
        )


@pytest.mark.parametrize("output_type", ["SCORE", "PASS_FAIL", "CHOICES"])
def test_all_eval_output_types_build(output_type: str) -> None:
    b = MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID,
        eval_output_type=output_type,
        threshold_metric_value="Passed" if output_type != "SCORE" else None,
    )
    sql, _ = b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)
    assert "FROM " in sql and "custom_eval_config_id" in sql


def test_choices_without_threshold_value_returns_null() -> None:
    # A CHOICES monitor with no selected choice can't compute anything — all
    # three query families must return the NULL/no-data shape, not broken SQL.
    b = MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID,
        eval_output_type="CHOICES",
        threshold_metric_value=None,
    )
    value_sql, _ = b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)
    stats_sql, _ = b.build_historical_stats_query(mm.EVALUATION_METRICS, START, END)
    ts_sql, _ = b.build_time_series_query(mm.EVALUATION_METRICS, START, END, 3600)
    assert "NULL" in value_sql and "output_str_list" not in value_sql
    assert "NULL" in stats_sql and "output_str_list" not in stats_sql
    assert "output_str_list" not in ts_sql


def test_pass_fail_time_series_shape() -> None:
    b = MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID,
        eval_output_type="PASS_FAIL",
        threshold_metric_value="Passed",
    )
    sql, params = b.build_time_series_query(mm.EVALUATION_METRICS, START, END, 3600)
    assert "output_bool = %(output_bool_val)s" in sql
    assert params["output_bool_val"] == 1
    assert "GROUP BY timestamp" in sql


def test_choices_time_series_shape() -> None:
    b = MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID,
        eval_output_type="CHOICES",
        threshold_metric_value="Good",
    )
    sql, params = b.build_time_series_query(mm.EVALUATION_METRICS, START, END, 3600)
    assert "has(JSONExtract(output_str_list, 'Array(String)'), %(choice_val)s)" in sql
    assert params["choice_val"] == "Good"
    assert "GROUP BY timestamp" in sql
