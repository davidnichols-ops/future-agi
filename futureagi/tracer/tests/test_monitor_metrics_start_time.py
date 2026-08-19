"""Monitor builder time windows: exact created_at filter + padded start_time
bounds for partition pruning (spans); eval table stays on created_at.
Pure SQL-string assertions, no ClickHouse."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
EVAL_CONFIG_ID = "22222222-2222-2222-2222-222222222222"
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 8, tzinfo=timezone.utc)

# One unified half-open created_at window for spans AND the eval table.
EXACT_HALF_OPEN = "created_at >= %(start_time)s AND created_at < %(end_time)s"
PAD_LOWER = "start_time >= %(start_time)s - INTERVAL 1 DAY"
PAD_UPPER = "start_time < %(end_time)s + INTERVAL 1 DAY"

SPANS_METRICS = [
    mm.COUNT_OF_ERRORS,
    mm.ERROR_RATES_FOR_FUNCTION_CALLING,
    mm.ERROR_FREE_SESSION_RATES,
    mm.SERVICE_PROVIDER_ERROR_RATES,
    mm.LLM_API_FAILURE_RATES,
    mm.SPAN_RESPONSE_TIME,
    mm.LLM_RESPONSE_TIME,
    mm.TOKEN_USAGE,
    mm.DAILY_TOKENS_SPENT,
    mm.MONTHLY_TOKENS_SPENT,
]
HISTORICAL_SPANS = [
    mm.ERROR_RATES_FOR_FUNCTION_CALLING,
    mm.ERROR_FREE_SESSION_RATES,
    mm.SERVICE_PROVIDER_ERROR_RATES,
    mm.LLM_API_FAILURE_RATES,
    mm.SPAN_RESPONSE_TIME,
    mm.LLM_RESPONSE_TIME,
]


def _builder(
    filters: Optional[Dict[str, Any]] = None,
    eval_output_type: Optional[str] = None,
) -> MonitorMetricsQueryBuilder:
    return MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        eval_config_id=EVAL_CONFIG_ID if eval_output_type else None,
        eval_output_type=eval_output_type,
        threshold_metric_value="Passed" if eval_output_type == "PASS_FAIL" else None,
    )


def _assert_pruned(sql: str) -> None:
    assert PAD_LOWER in sql, "missing start_time lower pruning bound"
    assert PAD_UPPER in sql, "missing start_time upper pruning bound"


@pytest.mark.parametrize("metric_type", SPANS_METRICS)
def test_metric_value_half_open_exact_plus_pruning(metric_type: str) -> None:
    sql, _ = _builder().build_metric_value_query(metric_type, START, END)
    assert EXACT_HALF_OPEN in sql
    assert "created_at BETWEEN" not in sql
    _assert_pruned(sql)


@pytest.mark.parametrize("metric_type", HISTORICAL_SPANS)
def test_historical_stats_exact_plus_pruning(metric_type: str) -> None:
    sql, _ = _builder().build_historical_stats_query(metric_type, START, END)
    assert EXACT_HALF_OPEN in sql
    _assert_pruned(sql)


@pytest.mark.parametrize("metric_type", SPANS_METRICS)
def test_time_series_buckets_created_at_and_prunes(metric_type: str) -> None:
    sql, _ = _builder().build_time_series_query(metric_type, START, END, 3600)
    assert "toUInt32(created_at)" in sql
    assert "toUInt32(start_time)" not in sql
    _assert_pruned(sql)


def test_eval_value_query_windows_span_time() -> None:
    # The half-open window lives on the SPAN membership subquery (span time);
    # the eval table itself keeps only a loose created_at lower bound.
    sql, _ = _builder(eval_output_type="SCORE").build_metric_value_query(
        mm.EVALUATION_METRICS, START, END
    )
    assert EXACT_HALF_OPEN in sql.split("observation_span_id IN (", 1)[1]
    # No spans-style bucket on the eval table itself.
    assert "start_time BETWEEN" not in sql
    assert "toUInt32(start_time)" not in sql


def test_eval_time_series_buckets_created_at() -> None:
    sql, _ = _builder(eval_output_type="SCORE").build_time_series_query(
        mm.EVALUATION_METRICS, START, END, 3600
    )
    assert "toUInt32(created_at)" in sql
    assert "toUInt32(start_time)" not in sql
