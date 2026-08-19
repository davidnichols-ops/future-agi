"""Monitor builder parity: calendar-bucketed historical stats for count/token
metrics (matches the old Python statistics path). Pure SQL-string assertions."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
START = datetime(2026, 8, 1, tzinfo=UTC)
END = datetime(2026, 8, 8, tzinfo=UTC)

TIME_AGGREGATED = [
    mm.COUNT_OF_ERRORS,
    mm.TOKEN_USAGE,
    mm.DAILY_TOKENS_SPENT,
    mm.MONTHLY_TOKENS_SPENT,
]


def _builder() -> MonitorMetricsQueryBuilder:
    return MonitorMetricsQueryBuilder(project_id=PROJECT_ID)


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
    # Bucket axis capped at the window end — no sparse trailing bucket from
    # late-ingested spans deflating the mean / inflating the stddev.
    assert "created_at <= %(end_time)s" in sql
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


# --- Evaluator routing: CH serves these metrics, PG is never touched ----------


class TestHistoricalStatsRouting:
    """Pin the _get_historical_stats routing hunk: the four time-aggregated
    metrics are served by the CH builder with the monitor's interval_kind,
    and ObservationSpan (the dropped span table) is never queried."""

    @pytest.mark.parametrize(
        ("metric_type", "frequency", "bucket_fn"),
        [
            (mm.COUNT_OF_ERRORS, 60, "toStartOfHour"),
            (mm.TOKEN_USAGE, 5, "toStartOfMinute"),
            (mm.DAILY_TOKENS_SPENT, 60, "toStartOfDay"),
            (mm.MONTHLY_TOKENS_SPENT, 60, "toStartOfMonth"),
        ],
    )
    def test_ch_route_passes_interval_kind_and_skips_pg(
        self, metric_type: str, frequency: int, bucket_fn: str
    ) -> None:
        from types import SimpleNamespace
        from unittest import mock

        from tracer.models.monitor import UserAlertMonitor
        from tracer.utils import monitor as monitor_utils

        monitor = UserAlertMonitor(
            project_id=PROJECT_ID,
            metric_type=metric_type,
            alert_frequency=frequency,
            filters={},
        )
        captured: dict[str, str] = {}

        class _Svc:
            def execute_ch_query(self, query, params, **kwargs):
                captured["query"] = query
                return SimpleNamespace(data=[{"mean": 4.2, "stddev": 1.1}])

        with mock.patch.object(monitor_utils, "AnalyticsQueryService", _Svc):
            mean, stddev = monitor_utils._get_historical_stats(monitor, START, END)

        assert (mean, stddev) == (4.2, 1.1)
        # interval_kind derived from the monitor reaches the bucket function.
        assert f"{bucket_fn}(created_at)" in captured["query"]
        # PG path is gone structurally: the module no longer imports the
        # dropped span table at all.
        assert not hasattr(monitor_utils, "ObservationSpan")
