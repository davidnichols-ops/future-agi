"""Monitor builder session metrics + monitor filters: trace_session_id column,
remap-resolved grouping, and the stable content-class filters (observation_type,
span_attributes_filters). Pure SQL-string assertions, no ClickHouse.

Entity/time-scoping filter keys (session_id/trace_id/span_id/date_range/
created_at) are intentionally not honored on monitors — a continuous alert owns
its own rolling window, so pinning it to momentary ids or an absolute range would
make the metric age out and the alert silently go dark."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from tracer.services.clickhouse.query_builders import monitor_metrics as mm
from tracer.services.clickhouse.query_builders.monitor_metrics import (
    MonitorMetricsQueryBuilder,
)

PROJECT_ID = "11111111-1111-1111-1111-111111111111"
START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 8, tzinfo=timezone.utc)


def _builder(
    filters: Optional[Dict[str, Any]] = None,
) -> MonitorMetricsQueryBuilder:
    return MonitorMetricsQueryBuilder(project_id=PROJECT_ID, filters=filters)


def _assert_session_ok(sql: str) -> None:
    assert "trace_session_id" in sql
    assert "session_id != ''" not in sql, "invalid String guard on a UUID column"
    # No bare session_id column ref (strip the trace_session_id token first).
    assert " session_id" not in sql.replace("trace_session_id", "")


def test_session_rates_value_resolves_remap() -> None:
    sql, _ = _builder().build_metric_value_query(
        mm.ERROR_FREE_SESSION_RATES, START, END
    )
    _assert_session_ok(sql)
    assert "trace_session_id_remap" in sql, "missing session id remap resolution"


def test_session_rates_historical_resolves_remap() -> None:
    sql, _ = _builder().build_historical_stats_query(
        mm.ERROR_FREE_SESSION_RATES, START, END
    )
    _assert_session_ok(sql)
    assert "trace_session_id_remap" in sql


def test_session_rates_time_series_resolves_remap() -> None:
    sql, _ = _builder().build_time_series_query(
        mm.ERROR_FREE_SESSION_RATES, START, END, 3600
    )
    _assert_session_ok(sql)
    assert "trace_session_id_remap" in sql
    assert "GROUP BY timestamp, " in sql


@pytest.mark.parametrize("key", ["session_id", "trace_id", "span_id", "created_at"])
def test_entity_and_time_scoping_keys_are_ignored(key: str) -> None:
    # These keys don't make sense on a continuous alert and must not compile to
    # any predicate — no id match, no 1 = 0, no created_at bound.
    sql, params = _builder(filters={key: "irrelevant"}).build_metric_value_query(
        mm.COUNT_OF_ERRORS, START, END
    )
    assert "mf_session_ids" not in sql
    assert "mf_trace_ids" not in sql
    assert "mf_span_ids" not in sql
    assert "mf_created_at" not in sql
    assert not any(k.startswith("mf_") for k in params)


def test_date_range_filter_is_ignored() -> None:
    # A stray absolute window (e.g. copied from a dashboard filter) is dropped,
    # not parsed — so it neither raises nor pins the rolling window.
    sql, params = _builder(
        filters={"date_range": ["not-a-date", "2026-08-01"]}
    ).build_metric_value_query(mm.COUNT_OF_ERRORS, START, END)
    assert "mf_dr_start" not in sql
    assert not any(k.startswith("mf_") for k in params)


def test_empty_observation_type_list_is_always_false() -> None:
    sql, _ = _builder(filters={"observation_type": []}).build_metric_value_query(
        mm.COUNT_OF_ERRORS, START, END
    )
    assert "1 = 0" in sql
    assert "IN %(mf_obs_type)s" not in sql


def test_span_attr_filter_subquery_is_project_scoped() -> None:
    filters = {
        "span_attributes_filters": [
            {
                "column_id": "session.id",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "x",
                },
            }
        ]
    }
    b = _builder(filters=filters)
    assert b._filter_clause, "attr filter must compile to a non-empty clause"
    sql, _ = b.build_metric_value_query(mm.COUNT_OF_ERRORS, START, END)
    # The membership subquery must be tenant-scoped, not a full-table scan.
    assert "WHERE 1 = 1" not in sql
    assert "project_id = %(project_id)s" in b._filter_clause


@pytest.mark.parametrize("key", ["session_id", "trace_id", "span_id"])
def test_empty_ignored_key_does_not_emit_always_false(key: str) -> None:
    # Removed keys are ignored, so even an empty selection must not inject a
    # 1 = 0 that would silently zero out the metric.
    sql, _ = _builder(filters={key: []}).build_metric_value_query(
        mm.COUNT_OF_ERRORS, START, END
    )
    assert "1 = 0" not in sql


# --- span-attribute filter date scoping (prod scale fix) ----------------------

ATTR_FILTER = {
    "span_attributes_filters": [
        {
            "column_id": "llm.model_name",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "gpt-4o",
            },
        }
    ]
}


def test_attr_filter_membership_subquery_is_date_scoped() -> None:
    # Without span_date_scope the trace-membership subquery scans the
    # project's ENTIRE span history (24-47s at 241M spans, at/over the 30s
    # monitor timeout). The monitor opts in and binds %(start_date)s.
    b = _builder(filters=ATTR_FILTER)
    assert "created_at >= %(start_date)s - INTERVAL 1 DAY" in b._filter_clause
    assert "trace_id IN (" in b._filter_clause


@pytest.mark.parametrize("family", ["value", "historical", "time_series"])
def test_attr_filter_start_date_bound_in_all_families(family: str) -> None:
    # Every build method must bind the %(start_date)s the scoped subquery
    # references — an unbound placeholder throws at execution time.
    b = _builder(filters=ATTR_FILTER)
    if family == "value":
        sql, params = b.build_metric_value_query(mm.COUNT_OF_ERRORS, START, END)
    elif family == "historical":
        sql, params = b.build_historical_stats_query(mm.COUNT_OF_ERRORS, START, END)
    else:
        sql, params = b.build_time_series_query(mm.COUNT_OF_ERRORS, START, END, 3600)
    assert "%(start_date)s" in sql
    assert params["start_date"] == params["start_time"]


def test_attr_filter_date_scope_reaches_eval_span_subquery() -> None:
    # Eval monitors splice the filter fragment into their span-membership
    # subquery; the date scope must survive that path too.
    b = MonitorMetricsQueryBuilder(
        project_id=PROJECT_ID,
        filters=ATTR_FILTER,
        eval_config_id="22222222-2222-2222-2222-222222222222",
        eval_output_type="SCORE",
    )
    sql, params = b.build_metric_value_query(mm.EVALUATION_METRICS, START, END)
    assert "created_at >= %(start_date)s - INTERVAL 1 DAY" in sql
    assert params["start_date"] == params["start_time"]


def test_dashboard_default_stays_unscoped() -> None:
    # The shared builder must stay byte-identical for callers that don't opt
    # in (dashboards): no %(start_date)s fragment in membership subqueries.
    from tracer.services.clickhouse.query_builders.filters import (
        ClickHouseFilterBuilder,
    )

    fb = ClickHouseFilterBuilder(table="spans", project_id=PROJECT_ID)
    clause, _ = fb.translate(ATTR_FILTER["span_attributes_filters"])
    assert "%(start_date)s" not in clause
