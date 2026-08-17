"""Monitor builder parity: calendar-bucketed historical stats for count/token
metrics, stddevPop vs stddevSamp split, NULL-on-empty token windows, provider
guard, and eval CHOICES containment. Pure SQL-string assertions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
EVAL_CONFIG_ID = "22222222-2222-2222-2222-222222222222"
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 8, tzinfo=timezone.utc)

TIME_AGGREGATED = [
    mm.COUNT_OF_ERRORS,
    mm.TOKEN_USAGE,
    mm.DAILY_TOKENS_SPENT,
    mm.MONTHLY_TOKENS_SPENT,
]
PER_ROW_HISTORICAL = [
    mm.ERROR_RATES_FOR_FUNCTION_CALLING,
    mm.ERROR_FREE_SESSION_RATES,
    mm.SERVICE_PROVIDER_ERROR_RATES,
    mm.LLM_API_FAILURE_RATES,
    mm.SPAN_RESPONSE_TIME,
    mm.LLM_RESPONSE_TIME,
]


def _builder(eval_output_type: Optional[str] = None) -> MonitorMetricsQueryBuilder:
    return MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        eval_config_id=EVAL_CONFIG_ID if eval_output_type else None,
        eval_output_type=eval_output_type,
        threshold_metric_value="Good" if eval_output_type == "CHOICES" else None,
    )


# --- Calendar-bucketed historical stats for count/token metrics ---------------


@pytest.mark.parametrize(
    "interval_kind,bucket_fn",
    [
        ("minute", "toStartOfMinute"),
        ("hour", "toStartOfHour"),
        ("day", "toStartOfDay"),
        ("month", "toStartOfMonth"),
    ],
)
@pytest.mark.parametrize("metric_type", TIME_AGGREGATED)
def test_time_aggregated_historical_buckets_calendar(
    metric_type: str, interval_kind: str, bucket_fn: str
) -> None:
    sql, _ = _builder().build_historical_stats_query(
        metric_type, START, END, interval_kind=interval_kind
    )
    assert f"{bucket_fn}(created_at) AS bucket_ts" in sql
    assert "GROUP BY bucket_ts" in sql
    # Sample stddev here (old path used statistics.stdev), collapsed to 0.
    assert "stddevSamp(bucket_value)" in sql
    assert "coalesce(ifNotFinite(avg(bucket_value), 0), 0)" in sql


def test_time_aggregated_historical_agg_per_metric() -> None:
    sql_err, _ = _builder().build_historical_stats_query(
        mm.COUNT_OF_ERRORS, START, END, interval_kind="hour"
    )
    assert "countIf(status = 'ERROR') AS bucket_value" in sql_err
    sql_tok, _ = _builder().build_historical_stats_query(
        mm.TOKEN_USAGE, START, END, interval_kind="hour"
    )
    # No-token buckets excluded (v2 total_tokens is non-Nullable, PG NULL -> 0).
    assert "nullIf(sum(total_tokens), 0) AS bucket_value" in sql_tok


def test_time_aggregated_historical_defaults_to_hour() -> None:
    sql, _ = _builder().build_historical_stats_query(mm.COUNT_OF_ERRORS, START, END)
    assert "toStartOfHour(created_at) AS bucket_ts" in sql


# --- stddevPop for per-row + eval stats (PG StdDev is population) -------------


@pytest.mark.parametrize("metric_type", PER_ROW_HISTORICAL)
def test_per_row_historical_uses_population_stddev(metric_type: str) -> None:
    sql, _ = _builder().build_historical_stats_query(metric_type, START, END)
    assert "stddevPop(" in sql
    assert "stddevSamp" not in sql


def test_eval_stats_use_population_stddev() -> None:
    sql, _ = _builder(eval_output_type="SCORE").build_historical_stats_query(
        mm.EVALUATION_METRICS, START, END
    )
    assert "stddevPop(" in sql
    assert "stddevSamp" not in sql


# --- No-token window yields NULL, not 0 ---------------------------------------


@pytest.mark.parametrize(
    "metric_type",
    [mm.TOKEN_USAGE, mm.DAILY_TOKENS_SPENT, mm.MONTHLY_TOKENS_SPENT],
)
def test_token_value_null_on_empty_window(metric_type: str) -> None:
    sql, _ = _builder().build_metric_value_query(metric_type, START, END)
    assert "CASE WHEN countIf(total_tokens != 0) = 0 THEN NULL" in sql
    assert "sum(total_tokens)" in sql


# --- Provider guard (v2 provider is non-Nullable; '' means no provider) -------


def test_provider_guard_excludes_empty_string_only() -> None:
    for sql, _ in (
        _builder().build_metric_value_query(
            mm.SERVICE_PROVIDER_ERROR_RATES, START, END
        ),
        _builder().build_historical_stats_query(
            mm.SERVICE_PROVIDER_ERROR_RATES, START, END
        ),
        _builder().build_time_series_query(
            mm.SERVICE_PROVIDER_ERROR_RATES, START, END, 3600
        ),
    ):
        assert "provider != ''" in sql
        assert "provider IS NOT NULL" not in sql


# --- Eval CHOICES list containment only (PG parity) ---------------------------


def test_eval_choices_no_output_str_fallback() -> None:
    sql, _ = _builder(eval_output_type="CHOICES").build_metric_value_query(
        mm.EVALUATION_METRICS, START, END
    )
    assert "has(JSONExtract(output_str_list, 'Array(String)'), %(choice_val)s)" in sql
    assert "OR output_str" not in sql
