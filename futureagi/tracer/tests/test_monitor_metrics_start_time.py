"""
Pin the monitor builder's time-window predicates to ``start_time``.

The v2 spans table is ``PARTITION BY toDate(start_time)`` with
``toStartOfHour(start_time)`` in the primary key and no index on
``created_at``. Filtering ``created_at`` prunes nothing → full-history scans.
These tests assert the COMPILED SQL filters/buckets spans on ``start_time``
(with a padded ``created_at`` companion bound), while the eval-table queries
stay on ``created_at`` (that table has no ``start_time`` column).

No real ClickHouse is hit — only the generated SQL string is asserted.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
EVAL_CONFIG_ID = "22222222-2222-2222-2222-222222222222"
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 8, tzinfo=timezone.utc)

COMPANION = "created_at >= %(start_time)s - INTERVAL 1 DAY"
BETWEEN_START = "start_time BETWEEN %(start_time)s AND %(end_time)s"
HALF_OPEN_START = "start_time >= %(start_time)s AND start_time < %(end_time)s"

# Spans metric types whose time window is a BETWEEN on start_time.
SPANS_BETWEEN = [
    mm.COUNT_OF_ERRORS,
    mm.ERROR_RATES_FOR_FUNCTION_CALLING,
    mm.ERROR_FREE_SESSION_RATES,
    mm.SERVICE_PROVIDER_ERROR_RATES,
    mm.LLM_API_FAILURE_RATES,
    mm.SPAN_RESPONSE_TIME,
    mm.LLM_RESPONSE_TIME,
    mm.TOKEN_USAGE,
]
SPANS_HALF_OPEN = [mm.DAILY_TOKENS_SPENT, mm.MONTHLY_TOKENS_SPENT]
# Per-row metrics that have a historical-stats branch (excludes the
# time-aggregated ones handled in Python and eval).
HISTORICAL_SPANS = [
    mm.ERROR_RATES_FOR_FUNCTION_CALLING,
    mm.ERROR_FREE_SESSION_RATES,
    mm.SERVICE_PROVIDER_ERROR_RATES,
    mm.LLM_API_FAILURE_RATES,
    mm.SPAN_RESPONSE_TIME,
    mm.LLM_RESPONSE_TIME,
]


def _builder(filters=None, eval_output_type=None):
    return MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        eval_config_id=EVAL_CONFIG_ID if eval_output_type else None,
        eval_output_type=eval_output_type,
        threshold_metric_value="Passed" if eval_output_type == "PASS_FAIL" else None,
    )


def _assert_spans_pruned(sql: str) -> None:
    """A spans query must prune on start_time and never bind bare created_at."""
    assert COMPANION in sql, "missing padded created_at companion bound"
    assert "created_at BETWEEN" not in sql, "spans query still filters created_at"


@pytest.mark.parametrize("metric_type", SPANS_BETWEEN)
def test_metric_value_spans_filter_start_time(metric_type):
    sql, _ = _builder().build_metric_value_query(metric_type, START, END)
    assert BETWEEN_START in sql
    _assert_spans_pruned(sql)


@pytest.mark.parametrize("metric_type", SPANS_HALF_OPEN)
def test_metric_value_half_open_filters_start_time(metric_type):
    sql, _ = _builder().build_metric_value_query(metric_type, START, END)
    assert HALF_OPEN_START in sql
    _assert_spans_pruned(sql)


@pytest.mark.parametrize("metric_type", HISTORICAL_SPANS)
def test_historical_stats_filter_start_time(metric_type):
    sql, _ = _builder().build_historical_stats_query(metric_type, START, END)
    assert BETWEEN_START in sql
    _assert_spans_pruned(sql)


@pytest.mark.parametrize("metric_type", SPANS_BETWEEN)
def test_time_series_buckets_and_filters_start_time(metric_type):
    sql, _ = _builder().build_time_series_query(metric_type, START, END, 3600)
    assert "toUInt32(start_time)" in sql, "spans bucket must floor start_time"
    assert "toUInt32(created_at)" not in sql
    _assert_spans_pruned(sql)


def test_session_and_date_range_filters_use_start_time():
    filters = {
        "date_range": [START.isoformat(), END.isoformat()],
        "created_at": START.isoformat(),
    }
    sql, _ = _builder(filters=filters).build_metric_value_query(
        mm.COUNT_OF_ERRORS, START, END
    )
    assert "start_time BETWEEN %(mf_dr_start)s AND %(mf_dr_end)s" in sql
    assert "start_time >= %(mf_created_at)s" in sql
    assert "created_at BETWEEN %(mf_dr_start)s" not in sql


# --- Eval-table queries must stay on created_at (no start_time column) --------


def test_eval_value_query_keeps_created_at():
    sql, _ = _builder(eval_output_type="SCORE").build_metric_value_query(
        mm.EVALUATION_METRICS, START, END
    )
    assert "created_at BETWEEN %(start_time)s AND %(end_time)s" in sql
    # ``start_time`` is only ever a param name here, never a column reference —
    # the eval table has no start_time column.
    for col_use in ("start_time BETWEEN", "start_time >=", "toUInt32(start_time)"):
        assert col_use not in sql, f"eval query references start_time column: {col_use}"


def test_eval_time_series_buckets_created_at():
    sql, _ = _builder(eval_output_type="SCORE").build_time_series_query(
        mm.EVALUATION_METRICS, START, END, 3600
    )
    assert "toUInt32(created_at)" in sql
    assert "toUInt32(start_time)" not in sql
