"""Focused regressions for latest-state trace/span list queries."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tracer.services.clickhouse.v2.query_builders.span_list import (
    SpanListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.span_reader import merge_span_attributes

_START = datetime(2026, 7, 30, 11, tzinfo=UTC)
_END = datetime(2026, 7, 30, 13, tzinfo=UTC)


def _time_filter():
    return {
        "column_id": "start_time",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [_START, _END],
        },
    }


def _attr(key, value):
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": "text",
            "filter_op": "equals",
            "filter_value": value,
        },
    }


def _session_filter(value):
    return {
        "column_id": "trace_session_id",
        "filter_config": {
            "col_type": "NORMAL",
            "filter_type": "text",
            "filter_op": "equals",
            "filter_value": str(value),
        },
    }


def test_span_latest_page_compiles_same_row_and_tombstone_predicates():
    builder = SpanListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter(), _attr("a", "x"), _attr("b", "y")],
        page_size=25,
    )

    sql, params = builder.build_latest_attribute_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
        before_start_time=_END - timedelta(minutes=1),
        before_id="span-b",
    )

    assert "FINAL" not in sql
    assert "GROUP BY id" in sql
    assert "latest_attr_exists_0" in sql
    assert "latest_attr_exists_1" in sql
    assert "latest_is_deleted = 0" in sql
    assert "latest_start_time = %(keyset_start_time)s" in sql
    assert "grouped_id < %(keyset_id)s" in sql
    assert "project_id = %(project_id)s" in sql
    assert params["project_id"] == "11111111-1111-1111-1111-111111111111"


def test_span_latest_id_page_applies_sampling_exclusion_and_keyset_before_limit():
    builder = SpanListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter(), _attr("final_status", "approved")],
    )

    sql, params = builder.build_latest_attribute_id_page(
        slice_start=_START,
        slice_end=_START + timedelta(minutes=1),
        limit=25,
        sampling_salt="task-1",
        sampling_rate=50,
        exclude_span_ids={"span-a"},
        after_span_id="span-b",
    )

    assert "FINAL" not in sql
    assert "GROUP BY id" in sql
    assert "latest_is_deleted = 0" in sql
    assert "final_status" in sql
    assert "grouped_id NOT IN %(latest_span_excluded_ids)s" in sql
    assert "grouped_id > %(latest_span_after_id)s" in sql
    assert sql.index("latest_is_deleted = 0") < sql.index("LIMIT %(latest_span_limit)s")
    assert params["latest_span_sampling_salt"] == "task-1"
    assert params["latest_span_sampling_rate"] == 50.0
    assert params["latest_span_excluded_ids"] == ("span-a",)
    assert params["latest_span_after_id"] == "span-b"


def test_trace_latest_root_id_page_selects_canonical_root_before_final_status():
    builder = TraceListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter(), _attr("final_status", "approved")],
    )

    sql, params = builder.build_latest_root_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
        sampling_salt="task-1",
        sampling_rate=100,
    )

    assert "FINAL" not in sql
    assert "LIMIT 1 BY grouped_trace_id" in sql
    assert sql.index("LIMIT 1 BY grouped_trace_id") < sql.rindex("latest_attr_exists_0")
    assert "argMax(tuple(parent_span_id), _version).1" in sql
    assert "argMax(tuple(start_time), _version).1" in sql
    assert params["latest_root_limit"] == 25


def test_trace_matcher_keeps_root_and_any_span_semantics_separate():
    builder = TraceListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter(), _attr("final_status", "approved"), _attr("a", "x")],
    )

    sql, params = builder.build_latest_filter_match_query(
        ["trace-a", "trace-b"],
        filters=[_attr("final_status", "approved"), _attr("a", "x")],
    )

    assert "FINAL" not in sql
    assert "LIMIT 1 BY grouped_trace_id" in sql
    assert sql.index("LIMIT 1 BY grouped_trace_id") < sql.rindex("latest_attr_exists_0")
    assert "trace_id IN (" in sql
    assert "GROUP BY trace_id, id" in sql
    assert "created_at >= %(candidate_start_date)s - INTERVAL 1 DAY" in sql
    assert "start_time >= %(candidate_start_date)s - INTERVAL 1 DAY" in sql
    assert "start_time < %(candidate_end_date)s + INTERVAL 1 DAY" in sql
    assert params["candidate_trace_ids"] == ("trace-a", "trace-b")
    assert params["project_id"] == "11111111-1111-1111-1111-111111111111"


def test_trace_matcher_revalidates_session_on_canonical_root():
    session_id = uuid.uuid4()
    builder = TraceListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter(), _session_filter(session_id), _attr("a", "x")],
    )

    sql, params = builder.build_latest_filter_match_query(
        ["trace-a"],
        filters=[_session_filter(session_id), _attr("a", "x")],
    )

    assert "FINAL" not in sql
    assert "argMax(tuple(trace_session_id), _version).1 AS latest_root_value_0" in sql
    assert "LIMIT 1 BY grouped_trace_id" in sql
    assert params["latest_root_value_param_0"] == str(session_id)


def test_trace_hydration_selects_canonical_latest_live_root():
    builder = TraceListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter()],
    )
    sql, _ = builder.build_candidate_hydration_query(["trace-a"])

    assert "FINAL" not in sql
    assert "GROUP BY trace_id, id" in sql
    assert "latest_is_deleted = 0" in sql
    assert "LIMIT 1 BY grouped_trace_id" in sql
    assert "ORDER BY latest_start_time DESC" in sql


def test_span_latest_page_keeps_project_version_in_scalar_latest_state():
    project_version_id = "22222222-2222-2222-2222-222222222222"
    builder = SpanListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        project_version_id=project_version_id,
        filters=[_time_filter(), _attr("final_status", "approved")],
        page_size=25,
    )

    sql, params = builder.build_latest_attribute_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
    )

    assert "FINAL" not in sql
    assert "argMax(tuple(project_version_id), _version).1" in sql
    assert "latest_project_version_id = %(project_version_id)s" in sql
    assert params["project_version_id"] == project_version_id


def test_trace_attribute_hydration_keeps_request_time_pruning():
    builder = TraceListQueryBuilderV2(
        project_id="11111111-1111-1111-1111-111111111111",
        filters=[_time_filter()],
    )
    builder.build()

    sql, params = builder.build_span_attributes_query(["trace-a"])

    assert "start_time >= %(start_date)s - INTERVAL 1 DAY" in sql
    assert "start_time < %(end_date)s + INTERVAL 1 DAY" in sql
    assert params["start_date"] == _START.replace(tzinfo=None)
    assert params["end_date"] == _END.replace(tzinfo=None)


def test_mixed_overflow_typed_and_bool_attributes_have_explicit_precedence():
    merged = merge_span_attributes(
        {"typed_string": "yes", "shared": "typed"},
        {"typed_number": 3.5},
        {"typed_bool": 1},
        '{"overflow": "yes", "shared": "overflow"}',
    )

    assert merged == {
        "typed_string": "yes",
        "typed_number": 3.5,
        "typed_bool": True,
        "overflow": "yes",
        "shared": "overflow",
    }


pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def latest_query_ch():
    """Isolated CH25 table for semantic parity; skip on CH-less lanes."""

    if os.environ.get("FUTUREAGI_TEST_ALLOW_LOCAL_CH_DDL") != "1":
        pytest.skip("local ClickHouse DDL integration test requires explicit opt-in")
    clickhouse_connect = pytest.importorskip("clickhouse_connect")
    host = os.environ.get("CH25_HOST") or os.environ.get("CH_HOST") or "localhost"
    if host not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("local ClickHouse DDL test refuses a non-loopback host")
    port = int(
        os.environ.get("CH25_HTTP_PORT") or os.environ.get("CH_HTTP_PORT") or 18124
    )
    database = f"test_latest_lists_{uuid.uuid4().hex[:8]}"
    try:
        admin = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=os.environ.get("CH_USER", "default"),
            password=os.environ.get("CH_PASSWORD", ""),
        )
        admin.command("SELECT 1")
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"ClickHouse not available: {exc!r}")

    admin.command(f"CREATE DATABASE {database}")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=os.environ.get("CH_USER", "default"),
        password=os.environ.get("CH_PASSWORD", ""),
        database=database,
    )
    client.command(
        """
        CREATE TABLE spans (
            project_id UUID,
            trace_id String,
            id String,
            parent_span_id String DEFAULT '',
            trace_name String DEFAULT '',
            name String DEFAULT '',
            observation_type String DEFAULT '',
            status String DEFAULT '',
            start_time DateTime64(6, 'UTC'),
            end_time Nullable(DateTime64(6, 'UTC')),
            latency_ms Int32 DEFAULT 0,
            cost Float64 DEFAULT 0,
            total_tokens Int32 DEFAULT 0,
            prompt_tokens Int32 DEFAULT 0,
            completion_tokens Int32 DEFAULT 0,
            model String DEFAULT '',
            provider String DEFAULT '',
            end_user_id Nullable(UUID),
            trace_session_id Nullable(UUID),
            project_version_id Nullable(UUID),
            created_at DateTime64(6, 'UTC'),
            input String DEFAULT '',
            output String DEFAULT '',
            attributes_extra String DEFAULT '{}',
            attrs_string Map(String, String),
            attrs_number Map(String, Float64),
            attrs_bool Map(String, UInt8),
            is_deleted UInt8 DEFAULT 0,
            _version UInt64
        ) ENGINE = MergeTree
        ORDER BY (project_id, start_time, trace_id, id, _version)
        """
    )
    try:
        yield client
    finally:
        client.close()
        admin.command(f"DROP DATABASE IF EXISTS {database}")
        admin.close()


def _insert(client, rows):
    columns = [
        "project_id",
        "trace_id",
        "id",
        "parent_span_id",
        "trace_name",
        "name",
        "observation_type",
        "status",
        "start_time",
        "end_time",
        "created_at",
        "trace_session_id",
        "project_version_id",
        "attrs_string",
        "attrs_number",
        "attrs_bool",
        "is_deleted",
        "_version",
    ]
    client.insert("spans", rows, column_names=columns)


def _query(client, sql, params):
    result = client.query(sql, parameters=params)
    return [
        dict(zip(result.column_names, row, strict=False)) for row in result.result_rows
    ]


@pytest.mark.integration
def test_latest_list_queries_real_ch_semantics(latest_query_ch):
    client = latest_query_ch
    project = uuid.uuid4()
    other_project = uuid.uuid4()
    root_old = _START + timedelta(minutes=10)
    root_new = _START + timedelta(minutes=20)
    same_time = _START + timedelta(minutes=30)
    stale_session = uuid.uuid4()
    current_session = uuid.uuid4()
    selected_project_version = uuid.uuid4()
    other_project_version = uuid.uuid4()

    def row(
        pid,
        trace_id,
        span_id,
        when,
        attrs,
        *,
        parent="",
        deleted=0,
        version=1,
        name="span",
        session=None,
        project_version=None,
    ):
        return [
            pid,
            trace_id,
            span_id,
            parent,
            f"trace-{trace_id}",
            name,
            "llm",
            "OK",
            when,
            when,
            when,
            session,
            project_version,
            attrs,
            {},
            {},
            deleted,
            version,
        ]

    _insert(
        client,
        [
            # Canonical-root contract: the newer root is approved. The older
            # rejected root must not make final_status=rejected pass.
            row(
                project,
                "multi-root",
                "root-old",
                root_old,
                {"final_status": "rejected"},
                name="old",
            ),
            row(
                project,
                "multi-root",
                "root-new",
                root_new,
                {"final_status": "approved"},
                name="new",
            ),
            # Generic attributes may live on independent child spans.
            row(project, "generic", "generic-root", root_old, {}, name="generic-root"),
            row(
                project,
                "generic",
                "child-a",
                root_old,
                {"a": "x"},
                parent="generic-root",
            ),
            row(
                project,
                "generic",
                "child-b",
                root_old,
                {"b": "y"},
                parent="generic-root",
            ),
            # Span same-row AND and equal-timestamp keyset fixtures.
            row(project, "span-a", "span-a", same_time, {"a": "x", "b": "y"}),
            row(project, "span-b", "span-b", same_time, {"a": "x", "b": "no"}),
            row(project, "span-c", "span-c", same_time, {"a": "no", "b": "y"}),
            row(
                project,
                "versioned-a",
                "versioned-a",
                same_time,
                {"a": "x"},
                project_version=selected_project_version,
            ),
            row(
                project,
                "versioned-b",
                "versioned-b",
                same_time,
                {"a": "x"},
                project_version=other_project_version,
            ),
            # argMax over Nullable skips NULL unless the value is tuple-wrapped.
            # The latest version clearing project_version must not resurrect v1.
            row(
                project,
                "version-cleared",
                "version-cleared",
                same_time,
                {"a": "x"},
                version=1,
                project_version=selected_project_version,
            ),
            row(
                project,
                "version-cleared",
                "version-cleared",
                same_time,
                {"a": "x"},
                version=2,
                project_version=None,
            ),
            # Latest tombstone wins over an older live matching version.
            row(
                project,
                "deleted",
                "span-deleted",
                same_time,
                {"a": "x", "b": "y"},
                version=1,
            ),
            row(
                project,
                "deleted",
                "span-deleted",
                same_time,
                {"a": "x", "b": "y"},
                deleted=1,
                version=2,
            ),
            # A historical seed can carry the old session. The matcher must
            # resolve the canonical root's latest session before accepting it.
            row(
                project,
                "session-change",
                "session-root",
                root_old,
                {"a": "x"},
                version=1,
                session=stale_session,
            ),
            row(
                project,
                "session-change",
                "session-root",
                root_old,
                {"a": "x"},
                version=2,
                session=current_session,
            ),
            row(
                project,
                "session-cleared",
                "session-cleared-root",
                root_old,
                {"a": "x"},
                version=1,
                session=stale_session,
            ),
            row(
                project,
                "session-cleared",
                "session-cleared-root",
                root_old,
                {"a": "x"},
                version=2,
                session=None,
            ),
            # Same values in another tenant must never leak.
            row(other_project, "other", "other-span", same_time, {"a": "x", "b": "y"}),
        ],
    )

    span_builder = SpanListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("a", "x"), _attr("b", "y")],
        page_size=25,
    )
    span_sql, span_params = span_builder.build_latest_attribute_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
    )
    span_rows = _query(client, span_sql, span_params)
    assert {row["id"] for row in span_rows} == {"span-a"}

    span_id_sql, span_id_params = span_builder.build_latest_attribute_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
        sampling_salt="task-real-ch",
        sampling_rate=100,
    )
    assert {row["id"] for row in _query(client, span_id_sql, span_id_params)} == {
        "span-a"
    }

    keyset_sql, keyset_params = span_builder.build_latest_attribute_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
        before_start_time=same_time,
        before_id="span-b",
    )
    assert [row["id"] for row in _query(client, keyset_sql, keyset_params)] == [
        "span-a"
    ]

    version_builder = SpanListQueryBuilderV2(
        project_id=str(project),
        project_version_id=str(selected_project_version),
        filters=[_time_filter(), _attr("a", "x")],
        page_size=25,
    )
    version_sql, version_params = version_builder.build_latest_attribute_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
    )
    assert {row["id"] for row in _query(client, version_sql, version_params)} == {
        "versioned-a"
    }

    approved_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("final_status", "approved")],
    )
    approved_sql, approved_params = approved_builder.build_latest_filter_match_query(
        ["multi-root"],
        filters=[_attr("final_status", "approved")],
    )
    assert _query(client, approved_sql, approved_params) == [{"trace_id": "multi-root"}]
    approved_id_sql, approved_id_params = approved_builder.build_latest_root_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
    )
    assert _query(client, approved_id_sql, approved_id_params) == [
        {
            "trace_id": "multi-root",
            "eval_order_start_time": root_new.replace(tzinfo=None),
        }
    ]

    rejected_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("final_status", "rejected")],
    )
    rejected_sql, rejected_params = rejected_builder.build_latest_filter_match_query(
        ["multi-root"],
        filters=[_attr("final_status", "rejected")],
    )
    assert _query(client, rejected_sql, rejected_params) == []
    rejected_id_sql, rejected_id_params = rejected_builder.build_latest_root_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=25,
    )
    assert _query(client, rejected_id_sql, rejected_id_params) == []

    generic_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("a", "x"), _attr("b", "y")],
    )
    generic_sql, generic_params = generic_builder.build_latest_filter_match_query(
        ["generic"],
        filters=[_attr("a", "x"), _attr("b", "y")],
    )
    assert _query(client, generic_sql, generic_params) == [{"trace_id": "generic"}]

    stale_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _session_filter(stale_session), _attr("a", "x")],
    )
    stale_sql, stale_params = stale_builder.build_latest_filter_match_query(
        ["session-change"],
        filters=[_session_filter(stale_session), _attr("a", "x")],
    )
    assert _query(client, stale_sql, stale_params) == []

    cleared_sql, cleared_params = stale_builder.build_latest_filter_match_query(
        ["session-cleared"],
        filters=[_session_filter(stale_session), _attr("a", "x")],
    )
    assert _query(client, cleared_sql, cleared_params) == []

    current_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _session_filter(current_session), _attr("a", "x")],
    )
    current_sql, current_params = current_builder.build_latest_filter_match_query(
        ["session-change"],
        filters=[_session_filter(current_session), _attr("a", "x")],
    )
    assert _query(client, current_sql, current_params) == [
        {"trace_id": "session-change"}
    ]

    hydration_sql, hydration_params = approved_builder.build_candidate_hydration_query(
        ["multi-root"]
    )
    hydrated = _query(client, hydration_sql, hydration_params)
    assert len(hydrated) == 1
    assert hydrated[0]["span_name"] == "new"
    assert hydrated[0]["start_time"].replace(tzinfo=UTC) == root_new


@pytest.mark.integration
def test_continuous_final_status_real_ch_uses_changed_ids_and_latest_state(
    latest_query_ch,
):
    """A cursor finds writes, while argMax classifies their complete current row."""

    client = latest_query_ch
    project = uuid.uuid4()
    other_project = uuid.uuid4()
    old_root_time = _START + timedelta(minutes=10)
    new_root_time = _START + timedelta(minutes=20)
    span_time = _START + timedelta(minutes=30)
    cursor_version = 100

    def row(
        pid,
        trace_id,
        span_id,
        when,
        final_status,
        *,
        parent="",
        deleted=0,
        version=90,
    ):
        return [
            pid,
            trace_id,
            span_id,
            parent,
            f"trace-{trace_id}",
            "root",
            "llm",
            "OK",
            when,
            when,
            when,
            None,
            None,
            ({"final_status": final_status} if final_status is not None else {}),
            {},
            {},
            deleted,
            version,
        ]

    _insert(
        client,
        [
            # Span transitions after the cursor: only the latest matching live
            # value is selected. An unchanged old match is not a tail candidate.
            row(
                project,
                "span-match",
                "span-match",
                span_time,
                "rejected",
                parent="span-parent",
            ),
            row(
                project,
                "span-match",
                "span-match",
                span_time,
                "approved",
                parent="span-parent",
                version=110,
            ),
            row(
                project,
                "span-stale",
                "span-stale",
                span_time,
                "approved",
                parent="span-parent",
            ),
            row(
                project,
                "span-stale",
                "span-stale",
                span_time,
                "rejected",
                parent="span-parent",
                version=110,
            ),
            row(
                project,
                "span-deleted",
                "span-deleted",
                span_time,
                "approved",
                parent="span-parent",
            ),
            row(
                project,
                "span-deleted",
                "span-deleted",
                span_time,
                "approved",
                parent="span-parent",
                deleted=1,
                version=110,
            ),
            row(
                project,
                "span-key-cleared",
                "span-key-cleared",
                span_time,
                "approved",
                parent="span-parent",
            ),
            row(
                project,
                "span-key-cleared",
                "span-key-cleared",
                span_time,
                None,
                parent="span-parent",
                version=110,
            ),
            row(
                project,
                "span-unchanged",
                "span-unchanged",
                span_time,
                "approved",
                parent="span-parent",
            ),
            row(
                project,
                "span-before-task",
                "span-before-task",
                _START - timedelta(minutes=1),
                "approved",
                parent="span-parent",
                version=110,
            ),
            row(
                other_project,
                "span-other",
                "span-other",
                span_time,
                "approved",
                parent="span-parent",
                version=110,
            ),
            # A changed older matching root makes the trace a candidate, but the
            # newer unchanged non-matching root remains canonical.
            row(project, "trace-multi", "root-old", old_root_time, "approved"),
            row(
                project,
                "trace-multi",
                "root-old",
                old_root_time,
                "rejected",
                version=110,
            ),
            row(project, "trace-multi", "root-new", new_root_time, "approved"),
            # A changed canonical root can enter the task; a latest tombstone
            # cannot resurrect its older matching version.
            row(project, "trace-match", "trace-match-root", new_root_time, "approved"),
            row(
                project,
                "trace-match",
                "trace-match-root",
                new_root_time,
                "rejected",
                version=110,
            ),
            row(
                project,
                "trace-deleted",
                "trace-deleted-root",
                new_root_time,
                "rejected",
            ),
            row(
                project,
                "trace-deleted",
                "trace-deleted-root",
                new_root_time,
                "rejected",
                deleted=1,
                version=110,
            ),
            # Deleting the newest root promotes the previous live root. The
            # promoted root is then the canonical row whose status is tested.
            row(
                project,
                "trace-promoted",
                "trace-promoted-old",
                old_root_time,
                "rejected",
            ),
            row(
                project,
                "trace-promoted",
                "trace-promoted-new",
                new_root_time,
                "approved",
            ),
            row(
                project,
                "trace-promoted",
                "trace-promoted-new",
                new_root_time,
                "approved",
                deleted=1,
                version=110,
            ),
            # Equal-time roots use the same deterministic id-desc tie-break as
            # trace hydration. Updating the non-canonical root must not win.
            row(
                project,
                "trace-equal-time",
                "root-z",
                new_root_time,
                "approved",
            ),
            row(
                project,
                "trace-equal-time",
                "root-a",
                new_root_time,
                "rejected",
                version=110,
            ),
            row(
                project,
                "trace-key-cleared",
                "trace-key-cleared-root",
                new_root_time,
                "rejected",
            ),
            row(
                project,
                "trace-key-cleared",
                "trace-key-cleared-root",
                new_root_time,
                None,
                version=110,
            ),
            row(
                project,
                "trace-before-task",
                "trace-before-task-root",
                _START - timedelta(minutes=1),
                "rejected",
                version=110,
            ),
            row(
                project,
                "trace-unchanged",
                "trace-unchanged-root",
                new_root_time,
                "rejected",
            ),
            row(
                other_project,
                "trace-other",
                "trace-other-root",
                new_root_time,
                "rejected",
                version=110,
            ),
        ],
    )

    span_builder = SpanListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("final_status", "approved")],
    )
    span_sql, span_params = span_builder.build_latest_attribute_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=None,
        sampling_salt="continuous-span",
        sampling_rate=100,
        changed_since_version=cursor_version,
    )
    assert "FINAL" not in span_sql
    assert "_version >= %(latest_span_changed_since_version)s" in span_sql
    assert "latest_span_limit" not in span_params
    assert {row["id"] for row in _query(client, span_sql, span_params)} == {
        "span-match"
    }

    trace_builder = TraceListQueryBuilderV2(
        project_id=str(project),
        filters=[_time_filter(), _attr("final_status", "rejected")],
    )
    trace_sql, trace_params = trace_builder.build_latest_root_id_page(
        slice_start=_START,
        slice_end=_END,
        limit=None,
        sampling_salt="continuous-trace",
        sampling_rate=100,
        changed_since_version=cursor_version,
    )
    assert "FINAL" not in trace_sql
    assert "_version >= %(latest_root_changed_since_version)s" in trace_sql
    assert "latest_root_limit" not in trace_params
    expected_trace_ids = {"trace-match", "trace-promoted"}
    assert {
        row["trace_id"] for row in _query(client, trace_sql, trace_params)
    } == expected_trace_ids
    # Re-reading the overlap is idempotent: the same write-version candidates
    # classify to the same current trace set.
    assert {
        row["trace_id"] for row in _query(client, trace_sql, trace_params)
    } == expected_trace_ids
