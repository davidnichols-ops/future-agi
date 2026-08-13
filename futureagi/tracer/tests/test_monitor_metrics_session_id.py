"""
Pin the monitor builder's session grouping/filter to ``trace_session_id``.

The live v2 spans table has no ``session_id`` column — only
``trace_session_id`` (Nullable(UUID)) — and the v2 rewriter does not translate
the name. These tests assert the compiled SQL uses ``trace_session_id``, drops
the ``!= ''`` guard (invalid on a UUID column), and that the session filter is a
direct equality (not the old malformed ``trace_id IN (SELECT id ...)`` subquery).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
SESSION_ID = "33333333-3333-3333-3333-333333333333"
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 8, tzinfo=timezone.utc)


def _builder(filters=None):
    return MonitorMetricsQueryBuilder(project_id=PROJECT_ID, filters=filters)


def _assert_session_ok(sql: str) -> None:
    assert "trace_session_id" in sql
    # Bare ``session_id`` column must never appear, nor the invalid UUID guard.
    assert "session_id != ''" not in sql
    # Catch a bare ``session_id`` not prefixed by ``trace_``.
    assert " session_id" not in sql.replace("trace_session_id", "")


def test_error_free_session_rates_value_uses_trace_session_id():
    sql, _ = _builder().build_metric_value_query(
        mm.ERROR_FREE_SESSION_RATES, START, END
    )
    _assert_session_ok(sql)
    assert "GROUP BY trace_session_id" in sql


def test_error_free_session_rates_historical_uses_trace_session_id():
    sql, _ = _builder().build_historical_stats_query(
        mm.ERROR_FREE_SESSION_RATES, START, END
    )
    _assert_session_ok(sql)
    assert "GROUP BY trace_session_id" in sql


def test_error_free_session_rates_time_series_uses_trace_session_id():
    sql, _ = _builder().build_time_series_query(
        mm.ERROR_FREE_SESSION_RATES, START, END, 3600
    )
    _assert_session_ok(sql)
    assert "GROUP BY timestamp, trace_session_id" in sql


def test_session_id_filter_is_direct_equality():
    sql, params = _builder(filters={"session_id": SESSION_ID}).build_metric_value_query(
        mm.COUNT_OF_ERRORS, START, END
    )
    assert "trace_session_id = toUUID(%(mf_session_id)s)" in sql
    assert params["mf_session_id"] == SESSION_ID
    # The old malformed span-id/trace-id subquery is gone.
    assert "SELECT DISTINCT id FROM spans" not in sql


@pytest.mark.parametrize(
    "metric_type",
    [mm.COUNT_OF_ERRORS, mm.TOKEN_USAGE, mm.SPAN_RESPONSE_TIME],
)
def test_session_filter_does_not_break_other_metrics(metric_type):
    # A session_id filter is spliced into the shared base WHERE, so it must
    # compile for every metric type, not just error_free_session_rates.
    sql, _ = _builder(filters={"session_id": SESSION_ID}).build_metric_value_query(
        metric_type, START, END
    )
    assert "trace_session_id = toUUID(%(mf_session_id)s)" in sql
