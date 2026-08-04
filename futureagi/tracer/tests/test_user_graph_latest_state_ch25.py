"""Real-ClickHouse proof that Project user graphs replay corrections/tombstones."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from clickhouse_driver import Client

from tracer.services.clickhouse.v2.query_builders.user_time_series import (
    UserDetailTimeSeriesQueryBuilderV2,
    UserTimeSeriesQueryBuilderV2,
)

pytestmark = pytest.mark.integration

CH_HOST = os.environ.get("CH25_HOST", "127.0.0.1")
CH_NATIVE_PORT = int(os.environ.get("CH25_NATIVE_PORT", "19000"))


@pytest.fixture(scope="module")
def ch_client():
    client = Client(host=CH_HOST, port=CH_NATIVE_PORT, connect_timeout=3)
    try:
        client.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"CH25 is not reachable on {CH_HOST}:{CH_NATIVE_PORT} ({exc!r})")
    return client


@pytest.fixture()
def user_graph_tables(ch_client):
    suffix = uuid.uuid4().hex[:8]
    spans = f"_test_user_graph_spans_{suffix}"
    end_users = f"_test_user_graph_end_users_{suffix}"
    end_user_remap = f"_test_user_graph_eu_remap_{suffix}"
    trace_session_remap = f"_test_user_graph_ts_remap_{suffix}"

    ch_client.execute(
        f"""
        CREATE TABLE {spans} (
            project_id UUID,
            observation_type String,
            service_name String,
            start_time DateTime64(6, 'UTC'),
            trace_id String,
            id String,
            parent_span_id String,
            end_user_id Nullable(UUID),
            trace_session_id Nullable(UUID),
            latency_ms Int32,
            total_tokens Int32,
            prompt_tokens Int32,
            completion_tokens Int32,
            cost Float64,
            status String,
            created_at DateTime64(6, 'UTC'),
            is_deleted UInt8,
            _version UInt64
        ) ENGINE = MergeTree
        ORDER BY (project_id, observation_type, service_name,
                  toStartOfHour(start_time), trace_id, id, _version)
        """
    )
    ch_client.execute(
        f"""
        CREATE TABLE {end_users} (
            project_id UUID,
            end_user_id UUID,
            organization_id UUID,
            version UInt64,
            is_deleted UInt8
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY (project_id, end_user_id)
        """
    )
    for table in (end_user_remap, trace_session_remap):
        ch_client.execute(
            f"""
            CREATE TABLE {table} (
                old_id UUID,
                new_id UUID,
                version UInt64
            ) ENGINE = ReplacingMergeTree(version)
            ORDER BY old_id
            """
        )
    try:
        yield spans, end_users, end_user_remap, trace_session_remap
    finally:
        for table in (spans, end_users, end_user_remap, trace_session_remap):
            ch_client.execute(f"DROP TABLE {table}")


def _execute(ch_client, query, params):
    rows, columns = ch_client.execute(
        query,
        params,
        with_column_types=True,
        settings={"max_threads": 1},
    )
    names = [name for name, _type in columns]
    return [dict(zip(names, row, strict=True)) for row in rows]


def test_user_graphs_count_only_latest_live_span_rows(ch_client, user_graph_tables):
    spans, end_users, end_user_remap, trace_session_remap = user_graph_tables
    project_id = "00000000-0000-4000-8000-000000000071"
    organization_id = "00000000-0000-4000-8000-000000000072"
    end_user_id = "00000000-0000-4000-8000-000000000073"
    trace_session_id = "00000000-0000-4000-8000-000000000074"
    started_at = datetime(2026, 7, 15, 10, 0, tzinfo=UTC)

    # One corrected span plus one tombstoned span. A raw physical-row aggregate
    # would report cost=169 and tokens=169; exact latest-live truth is 10/10.
    rows = [
        (
            project_id,
            "llm",
            "svc",
            started_at,
            "trace-live",
            "span-live",
            "",
            end_user_id,
            trace_session_id,
            900,
            100,
            40,
            60,
            100.0,
            "OK",
            started_at,
            0,
            1,
        ),
        (
            project_id,
            "llm",
            "svc",
            started_at,
            "trace-live",
            "span-live",
            "",
            end_user_id,
            trace_session_id,
            90,
            10,
            4,
            6,
            10.0,
            "OK",
            started_at,
            0,
            2,
        ),
        (
            project_id,
            "llm",
            "svc",
            started_at + timedelta(minutes=1),
            "trace-deleted",
            "span-deleted",
            "",
            end_user_id,
            trace_session_id,
            500,
            50,
            20,
            30,
            50.0,
            "ERROR",
            started_at + timedelta(minutes=1),
            0,
            1,
        ),
        (
            project_id,
            "llm",
            "svc",
            started_at + timedelta(minutes=1),
            "trace-deleted",
            "span-deleted",
            "",
            end_user_id,
            trace_session_id,
            500,
            50,
            20,
            30,
            50.0,
            "ERROR",
            started_at + timedelta(minutes=1),
            1,
            2,
        ),
    ]
    ch_client.execute(f"INSERT INTO {spans} VALUES", rows)
    ch_client.execute(
        f"INSERT INTO {end_users} VALUES",
        [(project_id, end_user_id, organization_id, 1, 0)],
    )

    date_filter = {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [
                started_at - timedelta(hours=1),
                started_at + timedelta(hours=1),
            ],
        },
    }

    aggregate = UserTimeSeriesQueryBuilderV2(
        project_id=project_id,
        filters=[date_filter],
        interval="hour",
    )
    aggregate.TABLE = spans
    aggregate.END_USER_REMAP_TABLE = end_user_remap
    aggregate_query, aggregate_params = aggregate.build()
    aggregate_rows = _execute(ch_client, aggregate_query, aggregate_params)

    assert len(aggregate_rows) == 1
    assert aggregate_rows[0]["active_users"] == 1
    assert aggregate_rows[0]["total_cost_sum"] == 10.0
    assert aggregate_rows[0]["total_tokens"] == 10
    assert aggregate_rows[0]["error_rate"] == 0

    old_value_filter = {
        "column_id": "cost",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "number",
            "filter_op": "greater_than",
            "filter_value": 20,
        },
    }
    corrected_filter_builder = UserTimeSeriesQueryBuilderV2(
        project_id=project_id,
        filters=[date_filter, old_value_filter],
        interval="hour",
    )
    corrected_filter_builder.TABLE = spans
    corrected_filter_builder.END_USER_REMAP_TABLE = end_user_remap
    corrected_query, corrected_params = corrected_filter_builder.build()
    # Neither the corrected old cost=100 row nor the tombstoned cost=50 row
    # may satisfy a filter compiled against latest state.
    assert _execute(ch_client, corrected_query, corrected_params) == []

    detail = UserDetailTimeSeriesQueryBuilderV2(
        project_id=project_id,
        organization_id=organization_id,
        end_user_id=end_user_id,
        filters=[date_filter],
        interval="hour",
    )
    detail.TABLE = spans
    detail.END_USERS_TABLE = end_users
    detail.END_USER_REMAP_TABLE = end_user_remap
    detail.TRACE_SESSION_REMAP_TABLE = trace_session_remap
    detail_query, detail_params = detail.build()
    detail_rows = _execute(ch_client, detail_query, detail_params)

    assert len(detail_rows) == 1
    assert detail_rows[0]["session_count"] == 1
    assert detail_rows[0]["trace_count"] == 1
    assert detail_rows[0]["cost"] == 10.0
    assert detail_rows[0]["input_tokens"] == 4
    assert detail_rows[0]["output_tokens"] == 6

    filtered_detail = UserDetailTimeSeriesQueryBuilderV2(
        project_id=project_id,
        organization_id=organization_id,
        end_user_id=end_user_id,
        filters=[date_filter, old_value_filter],
        interval="hour",
    )
    filtered_detail.TABLE = spans
    filtered_detail.END_USERS_TABLE = end_users
    filtered_detail.END_USER_REMAP_TABLE = end_user_remap
    filtered_detail.TRACE_SESSION_REMAP_TABLE = trace_session_remap
    filtered_query, filtered_params = filtered_detail.build()
    assert _execute(ch_client, filtered_query, filtered_params) == []
