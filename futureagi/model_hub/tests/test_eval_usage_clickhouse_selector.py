from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import NetworkError, ServerException

from model_hub.selectors import eval_usage
from model_hub.selectors.eval_usage import read_eval_usage
from tracer.services.clickhouse.client import ClickHouseClient


class _FakeClient:
    def __init__(self, *, total_runs=9):
        self.calls = []
        self.lock = threading.Lock()
        self.total_runs = total_runs

    def execute_read(self, query, params, *, timeout_ms, settings):
        with self.lock:
            self.calls.append((query, params, timeout_ms, settings))
        if "AS total_runs" in query:
            return [(self.total_runs,)], [], 1.0
        if "AS runs_period" in query and "toStartOfInterval" not in query:
            return [(3, 2, 1)], [], 1.0
        if "toStartOfInterval" in query:
            return (
                [
                    (
                        datetime(2026, 8, 1, tzinfo=UTC),
                        3,
                        0.25,
                        0.75,
                        2,
                        1,
                    )
                ],
                [],
                1.0,
            )
        return (
            [
                (
                    str(uuid.uuid4()),
                    '"{\\"output\\":{\\"output\\":0.75}}"',
                    "success",
                    datetime(2026, 8, 1, tzinfo=UTC),
                )
            ],
            [],
            1.0,
        )


@pytest.mark.unit
def test_eval_usage_queries_are_project_scoped_bounded_and_page_only(monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(eval_usage, "get_clickhouse_client", lambda: fake)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    result = read_eval_usage(
        organization_id=str(uuid.uuid4()),
        workspace_id=str(uuid.uuid4()),
        project_ids=[str(uuid.uuid4())],
        template_id=str(uuid.uuid4()),
        start_date=now - timedelta(days=30),
        end_date=now,
        bucket_minutes=1440,
        page=2,
        page_size=25,
    )

    assert result.total_runs == 9
    assert result.completeness == eval_usage.EvalUsageReadCompleteness.COMPLETE
    assert result.unavailable_fields == ()
    assert result.runs_period == 3
    assert result.logs[0].config == {"output": {"output": 0.75}}
    assert len(fake.calls) == 4
    for query, params, timeout_ms, settings in fake.calls:
        assert "usage_apicalllog FINAL" not in query
        assert "PREWHERE organization_id = toUUID" in query
        assert "workspace_id = toUUID" in query
        assert "source_id = %(template_id)s" in query
        assert "ORDER BY _peerdb_version DESC" in query
        assert "LIMIT 1 BY id" in query
        assert "WHERE _peerdb_is_deleted = 0 AND deleted = 0" in query
        assert params["project_ids"]
        assert 0 < timeout_ms <= eval_usage.READ_TIMEOUT_MS
        assert settings["readonly"] if "readonly" in settings else True
    page_query = next(query for query, *_ in fake.calls if "toString(log_id)" in query)
    assert "LIMIT %(limit)s OFFSET %(offset)s" in page_query
    total_query = next(query for query, *_ in fake.calls if "AS total_runs" in query)
    assert "dictGetOrDefault('trace_dict', 'project_id'" not in total_query
    assert "IN %(project_ids)s" not in total_query
    assert "created_at >= %(start_date)s" not in total_query
    assert "created_at <= %(end_date)s" not in total_query
    period_queries = [query for query, *_ in fake.calls if "AS total_runs" not in query]
    assert all(
        "dictGetOrDefault('trace_dict', 'project_id'" in query
        and "IN %(project_ids)s" in query
        and "created_at >= %(start_date)s" in query
        and "created_at <= %(end_date)s" in query
        for query in period_queries
    )


@pytest.mark.unit
def test_eval_usage_exact_total_can_be_zero(monkeypatch):
    fake = _FakeClient(total_runs=0)
    monkeypatch.setattr(eval_usage, "get_clickhouse_client", lambda: fake)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    result = read_eval_usage(
        organization_id=str(uuid.uuid4()),
        workspace_id=str(uuid.uuid4()),
        project_ids=[str(uuid.uuid4())],
        template_id=str(uuid.uuid4()),
        start_date=now - timedelta(days=1),
        end_date=now,
        bucket_minutes=60,
        page=0,
        page_size=25,
    )

    assert result.total_runs == 0
    assert result.completeness == eval_usage.EvalUsageReadCompleteness.COMPLETE
    assert result.unavailable_fields == ()


@pytest.mark.unit
def test_eval_usage_connect_stall_returns_within_one_wall_deadline(monkeypatch):
    fake = _FakeClient()
    release = threading.Event()
    lock = threading.Lock()
    acquisitions = 0

    def acquire_client():
        nonlocal acquisitions
        with lock:
            acquisition = acquisitions
            acquisitions += 1
        if acquisition == 0:
            release.wait(timeout=5)
        return fake

    monkeypatch.setattr(eval_usage, "READ_TIMEOUT_MS", 75)
    monkeypatch.setattr(eval_usage, "get_clickhouse_client", acquire_client)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    started = time.monotonic()
    try:
        with pytest.raises(eval_usage.EvalUsageReadError) as raised:
            read_eval_usage(
                organization_id=str(uuid.uuid4()),
                workspace_id=str(uuid.uuid4()),
                project_ids=[str(uuid.uuid4())],
                template_id=str(uuid.uuid4()),
                start_date=now - timedelta(days=1),
                end_date=now,
                bucket_minutes=60,
                page=0,
                page_size=25,
            )
    finally:
        release.set()

    assert raised.value.code == eval_usage.EvalUsageReadErrorCode.DEADLINE_EXCEEDED
    assert time.monotonic() - started < 0.5


@pytest.mark.unit
@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        (
            ServerException("private timeout query", code=159),
            eval_usage.EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
        ),
        (
            NetworkError("private network detail"),
            eval_usage.EvalUsageReadErrorCode.QUERY_FAILED,
        ),
    ],
)
def test_eval_usage_clickhouse_failures_are_typed(monkeypatch, failure, expected_code):
    class FailingClient:
        def execute_read(self, *_args, **_kwargs):
            raise failure

    monkeypatch.setattr(eval_usage, "get_clickhouse_client", FailingClient)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    with pytest.raises(eval_usage.EvalUsageReadError) as raised:
        read_eval_usage(
            organization_id=str(uuid.uuid4()),
            workspace_id=str(uuid.uuid4()),
            project_ids=[str(uuid.uuid4())],
            template_id=str(uuid.uuid4()),
            start_date=now - timedelta(days=1),
            end_date=now,
            bucket_minutes=60,
            page=0,
            page_size=25,
        )

    assert raised.value.code == expected_code
    assert raised.value.operations[0] in {"chart", "page", "stats", "total"}


@pytest.mark.unit
def test_eval_usage_programming_defect_re_raises_original_type(monkeypatch):
    class BuggyClient:
        def execute_read(self, *_args, **_kwargs):
            raise KeyError("application bug")

    monkeypatch.setattr(eval_usage, "get_clickhouse_client", BuggyClient)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    with pytest.raises(KeyError, match="application bug"):
        read_eval_usage(
            organization_id=str(uuid.uuid4()),
            workspace_id=str(uuid.uuid4()),
            project_ids=[str(uuid.uuid4())],
            template_id=str(uuid.uuid4()),
            start_date=now - timedelta(days=1),
            end_date=now,
            bucket_minutes=60,
            page=0,
            page_size=25,
        )


@pytest.mark.unit
def test_eval_usage_empty_project_set_fails_closed_for_trace_rows(monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(eval_usage, "get_clickhouse_client", lambda: fake)
    now = datetime(2026, 8, 2, tzinfo=UTC)

    read_eval_usage(
        organization_id=str(uuid.uuid4()),
        workspace_id=None,
        project_ids=[],
        template_id=str(uuid.uuid4()),
        start_date=now - timedelta(days=1),
        end_date=now,
        bucket_minutes=60,
        page=0,
        page_size=25,
    )

    assert all(
        params["project_ids"] == ("00000000-0000-0000-0000-000000000000",)
        for _query, params, _timeout, _settings in fake.calls
    )


@pytest.fixture(scope="module")
def ch_client():
    host = os.environ.get("CH25_HOST", "127.0.0.1")
    port = int(os.environ.get("CH25_NATIVE_PORT", "19000"))
    client = Client(host=host, port=port, connect_timeout=3)
    try:
        client.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"CH25 unavailable on {host}:{port}: {exc!r}")
    try:
        yield client
    finally:
        client.disconnect_connection()


@pytest.mark.integration
def test_eval_usage_real_ch25_latest_tombstone_and_project_scope(
    ch_client,
    monkeypatch,
):
    suffix = uuid.uuid4().hex[:10]
    usage_table = f"_test_eval_usage_{suffix}"
    trace_source = f"_test_eval_usage_trace_{suffix}"
    trace_dictionary = f"_test_eval_usage_dict_{suffix}"
    organization_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    other_project_id = uuid.uuid4()
    template_id = str(uuid.uuid4())
    trace_id = uuid.uuid4()
    other_trace_id = uuid.uuid4()
    now = datetime.now(UTC).replace(microsecond=0)

    ch_client.execute(
        f"""
        CREATE TABLE {usage_table} (
            id Int64,
            log_id UUID,
            organization_id UUID,
            workspace_id Nullable(UUID),
            source_id String,
            status String,
            config String,
            eval_trace_id String,
            deleted UInt8,
            created_at DateTime64(6, 'UTC'),
            _peerdb_is_deleted UInt8,
            _peerdb_version Int64
        ) ENGINE = MergeTree
        -- Match the live historical table layout. The selector must not rely
        -- on tenant/source/time being part of the primary key.
        ORDER BY id
        """
    )
    ch_client.execute(
        f"""
        CREATE TABLE {trace_source} (
            id UUID,
            project_id UUID
        ) ENGINE = MergeTree ORDER BY id
        """
    )
    ch_client.execute(
        f"""
        CREATE DICTIONARY {trace_dictionary} (
            id UUID,
            project_id UUID
        )
        PRIMARY KEY id
        SOURCE(CLICKHOUSE(
            HOST '127.0.0.1' PORT 9000 USER 'default'
            DB 'default' TABLE '{trace_source}'
        ))
        LIFETIME(0)
        LAYOUT(HASHED())
        """
    )
    try:
        ch_client.execute(
            f"INSERT INTO {trace_source} VALUES",
            [(trace_id, project_id), (other_trace_id, other_project_id)],
        )
        rows = [
            # Live selected-project trace row.
            (
                1,
                uuid.uuid4(),
                organization_id,
                workspace_id,
                template_id,
                "success",
                '{"duration":0.25,"output":{"output":0.85}}',
                str(trace_id),
                0,
                now - timedelta(hours=2),
                0,
                1,
            ),
            # Older live version followed by a tombstone: must not resurrect.
            (
                2,
                uuid.uuid4(),
                organization_id,
                workspace_id,
                template_id,
                "success",
                '{"output":{"output":"Passed"}}',
                str(trace_id),
                0,
                now - timedelta(hours=1),
                0,
                1,
            ),
            (
                2,
                uuid.uuid4(),
                organization_id,
                workspace_id,
                template_id,
                "success",
                '{"output":{"output":"Passed"}}',
                str(trace_id),
                1,
                now - timedelta(hours=1),
                1,
                2,
            ),
            # Sibling-project trace row: project dictionary must exclude it.
            (
                3,
                uuid.uuid4(),
                organization_id,
                workspace_id,
                template_id,
                "error",
                '{"output":{"output":"Failed"}}',
                str(other_trace_id),
                0,
                now - timedelta(minutes=30),
                0,
                1,
            ),
            # Non-trace playground row retains historical usage semantics.
            (
                4,
                uuid.uuid4(),
                organization_id,
                workspace_id,
                template_id,
                "success",
                '{"response_time":250,"output":{"output":{"label":"Passed","score":1.0}}}',
                "",
                0,
                now - timedelta(minutes=15),
                0,
                1,
            ),
        ]
        ch_client.execute(f"INSERT INTO {usage_table} VALUES", rows)
        monkeypatch.setattr(eval_usage, "_USAGE_TABLE", usage_table)
        monkeypatch.setattr(eval_usage, "_TRACE_PROJECT_DICT", trace_dictionary)
        read_client = ClickHouseClient(
            host=os.environ.get("CH25_HOST", "127.0.0.1"),
            port=int(os.environ.get("CH25_NATIVE_PORT", "19000")),
            database="default",
        )
        monkeypatch.setattr(
            eval_usage,
            "get_clickhouse_client",
            lambda: read_client,
        )

        result = read_eval_usage(
            organization_id=str(organization_id),
            workspace_id=str(workspace_id),
            project_ids=[str(project_id)],
            template_id=template_id,
            start_date=now - timedelta(days=1),
            end_date=now,
            bucket_minutes=60,
            page=0,
            page_size=25,
        )

        # total_runs preserves the original org/workspace/template contract,
        # so it includes the live sibling-project row as well as the two rows
        # rendered by the project-scoped selected-period response.
        assert result.total_runs == 3
        assert result.completeness == eval_usage.EvalUsageReadCompleteness.COMPLETE
        assert result.unavailable_fields == ()
        assert result.runs_period == 2
        assert result.success_count == 2
        assert result.error_count == 0
        assert len(result.logs) == 2
        assert sum(bucket.calls for bucket in result.chart) == 2
    finally:
        if "read_client" in locals():
            read_client.close()
        ch_client.execute(f"DROP DICTIONARY IF EXISTS {trace_dictionary}")
        ch_client.execute(f"DROP TABLE IF EXISTS {trace_source}")
        ch_client.execute(f"DROP TABLE IF EXISTS {usage_table}")
