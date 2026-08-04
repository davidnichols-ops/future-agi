"""Candidate-first session/user list regression and CH25 execution coverage."""

from __future__ import annotations

import inspect
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from tracer.selectors.trace_filter_reads import read_bounded_filter_page
from tracer.services.clickhouse.query_builders.user_list import (
    UnsupportedBoundedUserListQuery,
    UserListQueryBuilder,
)
from tracer.services.clickhouse.v2.query_builders.session_list import (
    SessionListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_builders.user_list import (
    UserListQueryBuilderV2,
)


def _window(now: datetime) -> list[dict]:
    return [
        {
            "column_id": "created_at",
            "filter_config": {
                "filter_type": "datetime",
                "filter_op": "between",
                "filter_value": [
                    (now - timedelta(days=1)).isoformat(),
                    (now + timedelta(days=1)).isoformat(),
                ],
            },
        }
    ]


@pytest.mark.unit
def test_user_default_page_replays_latest_state_before_pagination():
    builder = UserListQueryBuilderV2(
        organization_id=str(uuid.uuid4()),
        project_id=str(uuid.uuid4()),
        limit=25,
        offset=50,
        filters=[],
    )

    page_sql, params = builder.build_candidate_page_query()
    physical_sql, physical_params = builder.build_physical_user_presence_query()
    metrics_sql, metrics_params = builder.build_page_metrics_query([str(uuid.uuid4())])
    combined_sql, _ = builder.build()

    assert builder.supports_candidate_first_page() is True
    assert "FROM spans" in physical_sql
    assert "argMax" not in physical_sql
    assert "LIMIT 1" in physical_sql
    assert physical_params["project_id"] == builder.project_id
    assert "candidate_users AS" in page_sql
    assert "latest_candidate_spans AS" in page_sql
    assert "argMax(is_deleted, _version) AS latest_is_deleted" in page_sql
    assert "latest_is_deleted = 0" in page_sql
    assert "span_user_rollup" not in page_sql
    assert "span_user_rollup" not in combined_sql
    assert "FROM span_user_rollup" not in inspect.getsource(UserListQueryBuilder)
    assert "LIMIT %(limit)s OFFSET %(offset)s" in page_sql
    assert params["limit"] == 25
    assert params["offset"] == 50
    assert "argMax(is_deleted, _version) AS latest_is_deleted" in metrics_sql
    assert "(project_id, trace_id, id, start_time) IN" in metrics_sql
    assert "eu_survivor_map" in metrics_sql
    assert "ts_survivor_map" in metrics_sql
    assert len(metrics_params["candidate_end_user_ids"]) == 1


@pytest.mark.unit
def test_user_raw_metric_sort_fails_closed_instead_of_running_legacy_scan():
    builder = UserListQueryBuilderV2(
        organization_id=str(uuid.uuid4()),
        project_id=str(uuid.uuid4()),
        limit=25,
        offset=0,
        sort_params=[{"column_id": "num_sessions", "direction": "desc"}],
    )

    assert builder.supports_candidate_first_page() is False
    with pytest.raises(UnsupportedBoundedUserListQuery, match="bounded query path"):
        builder.build()


@pytest.mark.unit
def test_session_candidate_page_is_physical_latest_and_page_metrics_are_scoped():
    project_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=project_id,
        page_number=3,
        page_size=25,
        filters=[],
    )

    page_sql, page_params = builder.build_candidate_page_query()
    metrics_sql, metrics_params = builder.build_page_metrics_query([session_id])

    assert builder.supports_candidate_first_page() is True
    assert "argMax(is_deleted, _version) AS latest_is_deleted" in page_sql
    assert "count() OVER() AS total_count" in page_sql
    assert "ORDER BY session_start DESC, session_id DESC" in page_sql
    assert page_params["limit"] == 26
    assert page_params["offset"] == 75
    assert metrics_params["candidate_session_ids"] == (session_id,)
    assert "candidate_root_identities AS" in metrics_sql
    assert "(project_id, trace_id, id, start_time) IN" in metrics_sql
    assert "trace_session_id_remap" in metrics_sql

    count_sql, _ = builder.build_candidate_count_query()
    assert "SELECT count() AS total" in count_sql
    assert "sum(cost)" not in count_sql


@pytest.mark.unit
def test_session_aggregate_filter_and_sort_use_narrow_exact_candidate_shape():
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "total_tokens",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 10,
                },
            }
        ],
        sort_params=[{"column_id": "total_tokens", "direction": "desc"}],
    )

    sql, params = builder.build_candidate_page_query()

    assert builder.supports_candidate_first_page() is True
    assert "argMax(tuple(total_tokens), _version).1 AS latest_total_tokens" in sql
    assert "sum(total_tokens) AS total_tokens" in sql
    assert "HAVING total_tokens > %(having_" in sql
    assert "ORDER BY total_tokens DESC, session_id DESC" in sql
    assert 10 in params.values()


@pytest.mark.unit
def test_arbitrary_span_filter_remains_controlled_unsupported():
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "model",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "gpt-5",
                },
            }
        ],
    )

    assert builder.supports_candidate_first_page() is False
    with pytest.raises(ValueError, match="not candidate-page safe"):
        builder.build_candidate_page_query()


@pytest.mark.unit
def test_attribute_bulk_filter_uses_bounded_seed_and_latest_candidate_classifier():
    now = datetime(2026, 7, 31, 12, 0)
    session_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            *_window(now),
            {
                "column_id": "final_status",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": ["Rejected"],
                },
            },
        ],
        bounded_internal_scan=True,
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=now - timedelta(minutes=5),
        slice_end=now,
        limit=200,
    )
    match_sql, match_params = builder.build_filter_match_query([session_id])

    assert builder.supports_bounded_filter_scan() is True
    assert seed_params["filter_seed_limit"] == 200
    assert "SELECT session_id, start_time" in seed_sql
    assert "LIMIT %(filter_seed_limit)s" in seed_sql
    assert "trace_session_id_remap" not in seed_sql
    assert " FINAL" not in seed_sql
    assert "argMax(is_deleted, _version) AS latest_is_deleted" in match_sql
    assert "argMax(mapContains(attrs_string" in match_sql
    assert "latest_attr_value_0" in match_sql
    assert "indexHint(has(mapKeys(attrs_string)" in seed_sql
    assert "has(attrs_string.keys" in seed_sql
    candidate_roots = match_sql.split("candidate_root_identities AS (", 1)[1].split(
        "latest_roots AS (", 1
    )[0]
    assert "indexHint(has(mapKeys(attrs_string)" in candidate_roots
    assert "has(attrs_string.keys" in candidate_roots
    # The witness only narrows physical identities. Exact latest-state replay
    # and matching-root ordering retain the existing classifier semantics.
    assert "latest_attr_exists_0 AND" in match_sql
    assert "min(start_time) AS session_start" in match_sql
    assert "candidate_filter_session_id_array" in match_sql
    assert match_params["candidate_filter_session_ids"] == (session_id,)
    assert match_params["candidate_filter_session_id_array"] == [session_id]
    assert "candidate_filter_sessions AS" in match_sql
    assert "candidate_raw_session_id = candidate_ts_remap.any_id" in match_sql
    assert "SELECT session_id FROM candidate_filter_sessions" in match_sql
    assert "WHERE survivor_id IN (" in match_sql
    assert "rejected" in match_params.values()


@pytest.mark.unit
def test_raw_new_session_seed_classifier_expands_group_and_keeps_all_filters():
    now = datetime(2026, 7, 31, 12, 0)
    new_session_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            *_window(now),
            {
                "column_id": "final_status",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "Rejected",
                },
            },
            {
                "column_id": "customer.region",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "US",
                },
            },
        ],
        bounded_internal_scan=True,
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=now - timedelta(minutes=5),
        slice_end=now,
        limit=200,
    )
    match_sql, match_params = builder.build_filter_match_query([new_session_id])

    # The per-slice query is a raw superset: no remap FINAL can repeat for
    # every empty/adjacent slice. Only one safe witness narrows the seed.
    assert "trace_session_id_remap" not in seed_sql
    assert "has(attrs_string.keys, %(latest_filter_key_0)s)" in seed_sql
    assert "latest_filter_key_1" not in seed_params

    # Exact classification resolves a raw new/old candidate to its survivor,
    # expands that survivor back to every group member, and evaluates both
    # customer filters against latest state before returning the canonical ID.
    assert "trace_session_id_remap FINAL" in match_sql
    assert "argMin(old_id, toString(old_id)) OVER" not in match_sql
    assert match_sql.count("FROM trace_session_id_remap FINAL") == 2
    assert "FROM trace_sessions FINAL" not in match_sql
    assert "candidate_target_new_ids AS" in match_sql
    assert "PREWHERE old_id IN (" in match_sql
    assert "WHERE new_id IN (" in match_sql
    assert "arrayConcat(groupArray(old_id), [new_id])" in match_sql
    assert "SELECT arrayJoin(group_ids) AS any_id" in match_sql
    assert "AS candidate_session_pairs" in match_sql
    assert "SELECT arrayJoin(candidate_session_pairs) AS pair" in match_sql
    assert "AS Array(UUID)" in match_sql
    assert "candidate_filter_sessions AS" in match_sql
    assert "candidate_raw_session_id = candidate_ts_remap.any_id" in match_sql
    assert "SELECT any_id" in match_sql
    assert "SELECT session_id FROM candidate_filter_sessions" in match_sql
    assert "latest_attr_exists_0 AND" in match_sql
    assert "latest_attr_exists_1 AND" in match_sql
    assert match_params["candidate_filter_session_id_array"] == [new_session_id]
    assert match_params["latest_filter_param_0"] == "rejected"
    assert match_params["latest_filter_param_1"] == "us"


class _RawAliasSessionBuilder:
    def __init__(self, rows, canonical_rows, *, start, end):
        self.rows = rows
        self.canonical_rows = canonical_rows
        self.start = start
        self.end = end

    def parse_time_range(self, _filters):
        return self.start, self.end

    @staticmethod
    def filter_seed_proves_result_order():
        return True

    @staticmethod
    def recommended_filter_classify_batch_size():
        return 2

    def build_filter_seed_page(
        self,
        *,
        slice_start,
        slice_end,
        limit,
        before_start_time=None,
        before_id=None,
    ):
        return "seed", {
            "slice_start": slice_start,
            "slice_end": slice_end,
            "limit": limit,
            "before_start_time": before_start_time,
            "before_id": before_id,
        }

    @staticmethod
    def build_filter_match_query(candidate_ids):
        return "match", {"candidate_ids": tuple(candidate_ids)}


class _RawAliasSessionExecutor:
    def __init__(self, builder):
        self.builder = builder
        self.calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params))
        if query == "seed":
            rows = [
                row
                for row in self.builder.rows
                if params["slice_start"] <= row["start_time"] < params["slice_end"]
            ]
            before = params["before_start_time"]
            if before is not None:
                boundary = (before, str(params["before_id"]))
                rows = [
                    row
                    for row in rows
                    if (row["start_time"], str(row["session_id"])) < boundary
                ]
            rows = sorted(
                rows,
                key=lambda row: (row["start_time"], str(row["session_id"])),
                reverse=True,
            )[: params["limit"]]
        else:
            rows_by_canonical = {}
            for raw_id in params["candidate_ids"]:
                row = self.builder.canonical_rows.get(raw_id)
                if row is not None:
                    rows_by_canonical[row["session_id"]] = row
            rows = list(rows_by_canonical.values())
        return SimpleNamespace(data=rows)


@pytest.mark.unit
def test_raw_alias_duplicates_keep_page_one_and_page_n_disjoint():
    end = datetime(2026, 7, 31, 12, 0)
    start = end - timedelta(hours=1)
    canonical = {
        "new-a": {"session_id": "old-a", "start_time": end - timedelta(minutes=1)},
        "old-a": {"session_id": "old-a", "start_time": end - timedelta(minutes=1)},
        "session-b": {
            "session_id": "session-b",
            "start_time": end - timedelta(minutes=2),
        },
        "session-c": {
            "session_id": "session-c",
            "start_time": end - timedelta(minutes=3),
        },
        "session-d": {
            "session_id": "session-d",
            "start_time": end - timedelta(minutes=4),
        },
    }
    builder = _RawAliasSessionBuilder(
        [
            {"session_id": "new-a", "start_time": end - timedelta(seconds=30)},
            *canonical.values(),
        ],
        canonical,
        start=start,
        end=end,
    )

    pages = []
    for page_number in (0, 1):
        page = read_bounded_filter_page(
            builder=builder,
            analytics=_RawAliasSessionExecutor(builder),
            filters=_window(end),
            key_field="session_id",
            page_number=page_number,
            page_size=2,
            deadline_ms=5_000,
            max_candidates=200,
        )
        assert page.complete is True
        pages.append([row["session_id"] for row in page.rows])

    assert pages == [["old-a", "session-b"], ["session-c", "session-d"]]
    assert set(pages[0]).isdisjoint(pages[1])


@pytest.mark.unit
def test_raw_session_seed_crosses_empty_recent_slices_to_late_match():
    end = datetime(2026, 7, 31, 12, 0)
    start = end - timedelta(days=365)
    late = {"session_id": "late-session", "start_time": start + timedelta(days=20)}
    builder = _RawAliasSessionBuilder(
        [late],
        {"late-session": late},
        start=start,
        end=end,
    )
    executor = _RawAliasSessionExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=_window(end),
        key_field="session_id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
        max_candidates=200,
    )

    seed_calls = [params for query, params in executor.calls if query == "seed"]
    assert page.complete is True
    assert [row["session_id"] for row in page.rows] == ["late-session"]
    assert len(seed_calls) > 1
    assert seed_calls[-1]["slice_start"] == start
    assert all(
        newer["slice_start"] == older["slice_end"]
        for newer, older in zip(seed_calls, seed_calls[1:], strict=False)
    )


@pytest.mark.unit
def test_session_eval_seed_allows_shared_512_rows_but_classifier_stays_at_200():
    now = datetime(2026, 7, 31, 12, 0)
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[*_window(now)],
        bounded_internal_scan=True,
    )

    _, seed_params = builder.build_filter_seed_page(
        slice_start=now - timedelta(minutes=5),
        slice_end=now,
        limit=512,
    )

    assert seed_params["filter_seed_limit"] == 512
    with pytest.raises(ValueError, match="between 1 and 512"):
        builder.build_filter_seed_page(
            slice_start=now - timedelta(minutes=5),
            slice_end=now,
            limit=513,
        )
    with pytest.raises(ValueError, match="exceeds bounded limit"):
        builder.build_filter_match_query([str(uuid.uuid4()) for _ in range(201)])


@pytest.mark.unit
def test_negated_end_user_bulk_filter_is_candidate_session_scoped():
    session_id = str(uuid.uuid4())
    end_user_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "end_user_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "not_in",
                    "filter_value": [end_user_id],
                },
            }
        ],
        bounded_internal_scan=True,
    )

    sql, params = builder.build_filter_match_query([session_id])

    assert builder.supports_bounded_filter_scan() is True
    assert params["candidate_filter_session_ids"] == (session_id,)
    assert params["candidate_filter_session_id_array"] == [session_id]
    assert params["eu_remap_1"] == (end_user_id,)
    assert "end_user_id NOT IN %(eu_remap_1)s" in sql
    assert "candidate_filter_sessions AS" in sql
    assert sql.count("SELECT session_id FROM candidate_filter_sessions") >= 2
    assert "WHERE survivor_id IN (" in sql
    assert "session_id IN (SELECT session_id FROM matching_user_sessions)" in sql


@pytest.mark.unit
def test_session_message_filter_and_sort_are_applied_before_page():
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "first_message",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "contains",
                    "filter_value": "needle",
                },
            }
        ],
        sort_params=[{"column_id": "first_message", "direction": "asc"}],
    )

    sql, params = builder.build_candidate_page_query()

    assert "argMax(tuple(input), _version).1 AS latest_input" in sql
    assert "argMin(input, start_time) AS first_message" in sql
    assert "HAVING first_message ILIKE %(having_" in sql
    assert "ORDER BY first_message ASC, session_id ASC" in sql
    assert "%needle%" in params.values()


@pytest.mark.unit
def test_session_candidate_page_preserves_ascending_time_sort():
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        sort_params=[{"column_id": "created_at", "direction": "asc"}],
    )

    sql, _ = builder.build_candidate_page_query()

    assert "ORDER BY session_start ASC, session_id ASC" in sql


@pytest.mark.unit
def test_session_identity_filters_stay_on_bounded_candidate_path():
    session_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "trace_session_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": [session_id],
                },
            }
        ],
    )

    sql, params = builder.build_candidate_page_query()

    assert builder.supports_candidate_first_page() is True
    assert params["candidate_filter_session_ids"] == (session_id,)
    assert params["candidate_sess_1"] == (session_id,)
    assert "candidate_root_identities AS" in sql
    assert "session_id IN %(candidate_sess_1)s" in sql


@pytest.mark.unit
def test_positive_end_user_filter_uses_candidate_scoped_membership():
    end_user_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "end_user_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": [end_user_id],
                },
            }
        ],
    )

    sql, params = builder.build_candidate_page_query()

    assert builder.supports_candidate_first_page() is True
    assert params["candidate_filter_user_ids"] == (end_user_id,)
    assert params["eu_remap_1"] == (end_user_id,)
    assert "candidate_user_span_identities AS" in sql
    assert "latest_user_spans AS" in sql
    assert "matching_user_sessions AS" in sql
    assert "session_id IN (SELECT session_id FROM matching_user_sessions)" in sql


@pytest.mark.unit
def test_negated_end_user_filter_uses_exact_time_scoped_membership():
    excluded_id = str(uuid.uuid4())
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[
            {
                "column_id": "end_user_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "not_in",
                    "filter_value": [excluded_id],
                },
            }
        ],
    )

    sql, params = builder.build_candidate_page_query()

    assert builder.supports_candidate_first_page() is True
    assert params["eu_remap_1"] == (excluded_id,)
    assert "end_user_id NOT IN %(eu_remap_1)s" in sql
    assert "session_id IN (SELECT session_id FROM matching_user_sessions)" in sql
    # A negated predicate must not preseed only the excluded user IDs.
    assert "candidate_filter_user_ids" not in params


@pytest.mark.unit
def test_session_page_enrichments_replay_tombstones_and_resolve_remaps():
    builder = SessionListQueryBuilderV2(
        project_id=str(uuid.uuid4()),
        filters=[],
    )
    session_id = str(uuid.uuid4())

    metrics_sql, _ = builder.build_page_metrics_query([session_id])
    content_sql, content_params = builder.build_content_query([session_id])
    attrs_sql, _ = builder.build_span_attributes_query([session_id])

    assert content_params["candidate_filter_session_id_array"] == [session_id]
    # One primary-key old-ID probe plus one authoritative reverse new-ID pass.
    # The scalar tuple-array wrapper executes those source arms once even though
    # content hydration consumes the tiny map in multiple CTE stages.
    assert content_sql.count("FROM trace_session_id_remap FINAL") == 2
    assert "WHERE new_id IN (" in content_sql
    assert "candidate_target_new_ids AS" in content_sql
    assert "PREWHERE old_id IN (" in content_sql
    assert "AS candidate_session_pairs" in content_sql
    assert "SELECT arrayJoin(candidate_session_pairs) AS pair" in content_sql
    assert "trace_session_id IN %(content_session_ids)s" in content_sql
    assert "if(ts_remap.survivor_id IS NULL OR ts_remap.survivor_id = " in content_sql

    for sql in (metrics_sql, content_sql, attrs_sql):
        candidate_sql = sql.split("candidate_root_identities AS (", 1)[1].split(
            "),\n        latest_roots AS (", 1
        )[0]
        assert "candidate_root_identities AS" in sql
        # A latest live root always has at least its latest raw root row, so
        # this is a safe candidate witness. Root-to-child corrections and
        # tombstones are still rejected by the exact latest-state phase below.
        assert "(parent_span_id IS NULL OR parent_span_id = '')" in candidate_sql
        assert "trace_session_id_remap" in sql
        assert (
            "argMax(tuple(parent_span_id), _version).1 AS latest_parent_span_id" in sql
        )
        assert "argMax(is_deleted, _version) AS latest_is_deleted" in sql
        assert "latest_is_deleted = 0" in sql
        assert "(latest_parent_span_id IS NULL OR latest_parent_span_id = '')" in sql


def _ch25_client():
    host = os.getenv("CH25_HOST")
    port = int(
        os.getenv("CH25_NATIVE_PORT")
        or os.getenv("CH25_TCP_PORT")
        or os.getenv("CH_PORT")
        or "9000"
    )
    database = os.getenv("CH25_DATABASE") or os.getenv("CH_DATABASE") or "test_tfc"
    if not host:
        pytest.skip("CH25_HOST is not configured")
    try:
        from clickhouse_driver import Client

        client = Client(host=host, port=port, database=database)
        client.execute("SELECT 1")
        return client
    except Exception as exc:
        pytest.skip(f"disposable ClickHouse 25 is unavailable: {type(exc).__name__}")


def _dict_rows(rows, columns):
    names = [column[0] for column in columns]
    return [dict(zip(names, row, strict=True)) for row in rows]


@pytest.mark.integration
def test_user_pages_ignore_stale_rollup_tombstones_updates_and_reassignments():
    """Page membership/order comes from latest spans, never insert-only states."""

    client = _ch25_client()
    project_id = str(uuid.uuid4())
    organization_id = str(uuid.uuid4())
    user_ids = {name: str(uuid.uuid4()) for name in ("a", "b", "c", "d", "e")}
    trace_ids = {name: str(uuid.uuid4()) for name in ("a", "b", "c", "move")}
    now = datetime.now(UTC).replace(tzinfo=None)

    client.execute(
        "INSERT INTO end_users "
        "(project_id, end_user_id, organization_id, user_id, user_id_type, "
        "user_id_hash, metadata, first_seen, version, is_deleted) VALUES",
        [
            (
                project_id,
                end_user_id,
                organization_id,
                f"exact-user-{name}",
                "custom",
                f"hash-{name}",
                "{}",
                now - timedelta(days=1),
                now,
                0,
            )
            for name, end_user_id in user_ids.items()
        ],
    )

    columns = [
        "project_id",
        "observation_type",
        "service_name",
        "start_time",
        "trace_id",
        "id",
        "parent_span_id",
        "name",
        "end_time",
        "latency_ms",
        "org_id",
        "end_user_id",
        "trace_session_id",
        "status",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cost",
        "attrs_string",
        "attrs_number",
        "attrs_bool",
        "attributes_extra",
        "input",
        "output",
        "is_deleted",
        "_version",
    ]

    def _span(
        *,
        key: str,
        user: str,
        start_minutes: int,
        end_minutes: int,
        cost: float,
        deleted: int,
        version: int,
    ):
        start = now - timedelta(minutes=start_minutes)
        return (
            project_id,
            "llm",
            "user-exactness-test",
            start,
            trace_ids[key],
            f"span-{key}",
            "",
            f"span-{key}",
            now - timedelta(minutes=end_minutes),
            100,
            organization_id,
            user_ids[user],
            None,
            "OK",
            4,
            6,
            10,
            cost,
            {},
            {},
            {},
            "{}",
            "",
            "",
            deleted,
            version,
        )

    rows = [
        # A has only a tombstoned identity. Its insert-only rollup still has the
        # newest last_active value and would incorrectly put it on page 1.
        _span(
            key="a",
            user="a",
            start_minutes=20,
            end_minutes=1,
            cost=50,
            deleted=0,
            version=1,
        ),
        _span(
            key="a",
            user="a",
            start_minutes=20,
            end_minutes=1,
            cost=50,
            deleted=1,
            version=2,
        ),
        # B's corrected latest row reduces both cost and last_active.
        _span(
            key="b",
            user="b",
            start_minutes=10,
            end_minutes=2,
            cost=100,
            deleted=0,
            version=1,
        ),
        _span(
            key="b",
            user="b",
            start_minutes=10,
            end_minutes=8,
            cost=1,
            deleted=0,
            version=2,
        ),
        # One physical identity moves D -> E. D must disappear and E must own
        # exactly the latest contribution.
        _span(
            key="move",
            user="d",
            start_minutes=7,
            end_minutes=6,
            cost=20,
            deleted=0,
            version=1,
        ),
        _span(
            key="move",
            user="e",
            start_minutes=7,
            end_minutes=5,
            cost=2,
            deleted=0,
            version=2,
        ),
        _span(
            key="c",
            user="c",
            start_minutes=4,
            end_minutes=3,
            cost=5,
            deleted=0,
            version=1,
        ),
    ]
    client.execute(
        f"INSERT INTO spans ({', '.join(columns)}) VALUES",
        rows,
        types_check=True,
    )

    # Prove the fixture is adversarial: the append-only source is stale for A,
    # D, and B's corrected cost. A selector that still reads this table fails.
    stale_rollup = dict(
        client.execute(
            "SELECT toString(end_user_id), sumMerge(cost_sum) "
            "FROM span_user_rollup "
            "WHERE project_id = toUUID(%(project_id)s) GROUP BY end_user_id",
            {"project_id": project_id},
        )
    )
    assert stale_rollup[user_ids["a"]] == 50
    assert stale_rollup[user_ids["b"]] == 101
    assert stale_rollup[user_ids["d"]] == 20

    filters = _window(now)
    pages = []
    elapsed_ms = []
    for offset in range(3):
        builder = UserListQueryBuilderV2(
            organization_id=organization_id,
            project_id=project_id,
            limit=1,
            offset=offset,
            filters=filters,
        )
        query, params = builder.build_candidate_page_query()
        started = time.monotonic()
        raw, returned_columns = client.execute(query, params, with_column_types=True)
        elapsed_ms.append((time.monotonic() - started) * 1000)
        pages.extend(_dict_rows(raw, returned_columns))

    assert [str(row["end_user_id"]) for row in pages] == [
        user_ids["c"],
        user_ids["e"],
        user_ids["b"],
    ]
    assert [row["total_count"] for row in pages] == [3, 3, 3]
    assert [row["total_cost"] for row in pages] == [5, 2, 1]
    assert user_ids["a"] not in {str(row["end_user_id"]) for row in pages}
    assert user_ids["d"] not in {str(row["end_user_id"]) for row in pages}
    # This is a disposable correctness ceiling, not a production performance
    # claim. The API's tighter read deadline fails closed when exact replay is
    # not affordable on a heavy workspace.
    assert max(elapsed_ms) < 5_000


@pytest.mark.integration
def test_candidate_reads_on_ch25_preserve_remap_and_tombstone_semantics():
    """Execute every new query against the disposable CH25 schema.

    The old physical root is tombstoned after insert; the live root carries the
    deterministic ids. All APIs must return the one canonical old session/user,
    and deleted content/attributes must stay absent.
    """

    client = _ch25_client()
    project_id = str(uuid.uuid4())
    organization_id = str(uuid.uuid4())
    old_user_id, new_user_id = str(uuid.uuid4()), str(uuid.uuid4())
    old_session_id, new_session_id = str(uuid.uuid4()), str(uuid.uuid4())
    now = datetime.now(UTC).replace(tzinfo=None)

    client.execute(
        "INSERT INTO end_users "
        "(project_id, end_user_id, organization_id, user_id, user_id_type, "
        "user_id_hash, metadata, first_seen, version, is_deleted) VALUES",
        [
            (
                project_id,
                old_user_id,
                organization_id,
                "candidate-user",
                "email",
                "candidate-hash",
                "{}",
                now - timedelta(hours=1),
                now,
                0,
            )
        ],
    )
    client.execute(
        "INSERT INTO end_user_id_remap (old_id, new_id, version) VALUES",
        [(old_user_id, new_user_id, now)],
    )
    client.execute(
        "INSERT INTO trace_session_id_remap (old_id, new_id, version) VALUES",
        [(old_session_id, new_session_id, now)],
    )

    columns = [
        "project_id",
        "observation_type",
        "service_name",
        "start_time",
        "trace_id",
        "id",
        "parent_span_id",
        "name",
        "end_time",
        "latency_ms",
        "org_id",
        "project_version_id",
        "end_user_id",
        "trace_session_id",
        "status",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cost",
        "attrs_string",
        "attrs_number",
        "attrs_bool",
        "attributes_extra",
        "input",
        "output",
        "is_deleted",
        "_version",
    ]
    old_start = now - timedelta(minutes=10)
    live_start = now - timedelta(minutes=5)
    old_row = (
        project_id,
        "llm",
        "candidate-test",
        old_start,
        str(uuid.uuid4()),
        "candidate-root-old",
        "",
        "old-root",
        old_start + timedelta(seconds=2),
        100,
        organization_id,
        None,
        old_user_id,
        old_session_id,
        "OK",
        4,
        6,
        10,
        1.0,
        {"deleted_key": "gone"},
        {},
        {},
        '{"deleted_key":"gone"}',
        "deleted-message",
        "old-output",
        0,
        1,
    )
    live_row = (
        project_id,
        "llm",
        "candidate-test",
        live_start,
        str(uuid.uuid4()),
        "candidate-root-new",
        "",
        "live-root",
        live_start + timedelta(seconds=3),
        200,
        organization_id,
        None,
        new_user_id,
        new_session_id,
        "ERROR",
        8,
        12,
        20,
        2.0,
        {"live_key": "yes", "final_status": "Rejected"},
        {"score": 2.0},
        {},
        '{"live_key":"yes","final_status":"Rejected"}',
        "live-message",
        "live-output",
        0,
        1,
    )
    client.execute(
        f"INSERT INTO spans ({', '.join(columns)}) VALUES",
        [old_row, live_row],
        types_check=True,
    )
    tombstone = list(old_row)
    tombstone[-2] = 1
    tombstone[-1] = 2
    client.execute(
        f"INSERT INTO spans ({', '.join(columns)}) VALUES",
        [tuple(tombstone)],
        types_check=True,
    )

    filters = _window(now)
    user_builder = UserListQueryBuilderV2(
        organization_id=organization_id,
        project_id=project_id,
        limit=25,
        offset=0,
        filters=filters,
    )
    user_sql, user_params = user_builder.build_candidate_page_query()
    started = time.monotonic()
    user_raw, user_columns = client.execute(
        user_sql, user_params, with_column_types=True
    )
    user_page_elapsed_ms = (time.monotonic() - started) * 1000
    users = _dict_rows(user_raw, user_columns)

    assert len(users) == 1
    assert str(users[0]["end_user_id"]) == old_user_id
    user_metrics_sql, user_metrics_params = user_builder.build_page_metrics_query(
        [old_user_id]
    )
    started = time.monotonic()
    user_metrics_raw, user_metrics_columns = client.execute(
        user_metrics_sql,
        user_metrics_params,
        with_column_types=True,
    )
    user_metrics_elapsed_ms = (time.monotonic() - started) * 1000
    user_metrics = _dict_rows(user_metrics_raw, user_metrics_columns)
    assert len(user_metrics) == 1
    assert str(user_metrics[0]["end_user_id"]) == old_user_id
    assert user_metrics[0]["num_llm_calls"] == 1
    assert user_metrics[0]["num_sessions"] == 1
    assert user_metrics[0]["num_traces_with_errors"] == 1

    session_builder = SessionListQueryBuilderV2(
        project_id=project_id,
        page_number=0,
        page_size=25,
        filters=filters,
    )
    page_sql, page_params = session_builder.build_candidate_page_query()
    page_raw, page_columns = client.execute(
        page_sql, page_params, with_column_types=True
    )
    page = _dict_rows(page_raw, page_columns)
    assert len(page) == 1
    assert str(page[0]["session_id"]) == old_session_id
    assert page[0]["total_count"] == 1

    count_sql, count_params = session_builder.build_candidate_count_query()
    count_raw = client.execute(count_sql, count_params)
    assert count_raw[0][0] == 1

    structural_filters = {
        "session_in": [
            {
                "column_id": "trace_session_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": [old_session_id],
                },
            }
        ],
        "user_in": [
            {
                "column_id": "end_user_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": [old_user_id],
                },
            }
        ],
    }
    for structural_filter in structural_filters.values():
        filtered_builder = SessionListQueryBuilderV2(
            project_id=project_id,
            page_number=0,
            page_size=25,
            filters=[*filters, *structural_filter],
        )
        filtered_sql, filtered_params = filtered_builder.build_candidate_page_query()
        filtered_raw = client.execute(filtered_sql, filtered_params)
        assert len(filtered_raw) == 1
        assert str(filtered_raw[0][0]) == old_session_id
        filtered_count_sql, filtered_count_params = (
            filtered_builder.build_candidate_count_query()
        )
        assert client.execute(filtered_count_sql, filtered_count_params)[0][0] == 1

    excluded_builder = SessionListQueryBuilderV2(
        project_id=project_id,
        page_number=0,
        page_size=25,
        filters=[
            *filters,
            {
                "column_id": "trace_session_id",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "not_in",
                    "filter_value": [old_session_id],
                },
            },
        ],
    )
    excluded_sql, excluded_params = excluded_builder.build_candidate_page_query()
    assert client.execute(excluded_sql, excluded_params) == []
    excluded_count_sql, excluded_count_params = (
        excluded_builder.build_candidate_count_query()
    )
    assert client.execute(excluded_count_sql, excluded_count_params)[0][0] == 0

    attribute_builder = SessionListQueryBuilderV2(
        project_id=project_id,
        filters=[
            *filters,
            {
                "column_id": "final_status",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "in",
                    "filter_value": ["Rejected"],
                },
            },
        ],
        bounded_internal_scan=True,
    )
    seed_sql, seed_params = attribute_builder.build_filter_seed_page(
        slice_start=now - timedelta(minutes=15),
        slice_end=now + timedelta(seconds=1),
        limit=200,
    )
    seed_raw, seed_columns = client.execute(
        seed_sql, seed_params, with_column_types=True
    )
    seed_rows = _dict_rows(seed_raw, seed_columns)
    # The old physical root is tombstoned, so the raw live-row seed exposes
    # only the deterministic new alias. The finite classifier below must still
    # expand that alias's remap group and return the canonical old survivor.
    assert {str(row["session_id"]) for row in seed_rows} == {new_session_id}
    match_sql, match_params = attribute_builder.build_filter_match_query(
        [new_session_id]
    )
    match_raw = client.execute(match_sql, match_params)
    assert len(match_raw) == 1
    assert str(match_raw[0][0]) == old_session_id

    class _Executor:
        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            raw, returned_columns = client.execute(
                query,
                params,
                with_column_types=True,
                settings={
                    **settings,
                    "max_execution_time": max(1, timeout_ms // 1000),
                },
            )
            return SimpleNamespace(data=_dict_rows(raw, returned_columns))

    bounded_page = read_bounded_filter_page(
        builder=attribute_builder,
        analytics=_Executor(),
        filters=attribute_builder.filters,
        key_field="session_id",
        page_number=0,
        page_size=25,
        deadline_ms=5000,
    )
    assert bounded_page.complete is True
    assert [str(row["session_id"]) for row in bounded_page.rows] == [old_session_id]

    tombstoned_attribute_builder = SessionListQueryBuilderV2(
        project_id=project_id,
        filters=[
            *filters,
            {
                "column_id": "deleted_key",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "gone",
                },
            },
        ],
        bounded_internal_scan=True,
    )
    deleted_sql, deleted_params = tombstoned_attribute_builder.build_filter_match_query(
        [old_session_id]
    )
    assert client.execute(deleted_sql, deleted_params) == []

    derived_filters = {
        "tokens": (
            {
                "column_id": "total_tokens",
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 15,
                },
            },
            {"column_id": "total_tokens", "direction": "desc"},
        ),
        "message": (
            {
                "column_id": "first_message",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "contains",
                    "filter_value": "live-message",
                },
            },
            {"column_id": "first_message", "direction": "asc"},
        ),
    }
    for derived_filter, derived_sort in derived_filters.values():
        derived_builder = SessionListQueryBuilderV2(
            project_id=project_id,
            page_number=0,
            page_size=25,
            filters=[*filters, derived_filter],
            sort_params=[derived_sort],
        )
        derived_sql, derived_params = derived_builder.build_candidate_page_query()
        derived_raw = client.execute(derived_sql, derived_params)
        assert len(derived_raw) == 1
        assert str(derived_raw[0][0]) == old_session_id
        derived_count_sql, derived_count_params = (
            derived_builder.build_candidate_count_query()
        )
        assert client.execute(derived_count_sql, derived_count_params)[0][0] == 1

    phase_timings = []
    results = {}
    for name, (sql, params) in {
        "metrics": session_builder.build_page_metrics_query([old_session_id]),
        "content": session_builder.build_content_query([old_session_id]),
        "attributes": session_builder.build_span_attributes_query([old_session_id]),
    }.items():
        started = time.monotonic()
        raw, returned_columns = client.execute(sql, params, with_column_types=True)
        phase_timings.append((time.monotonic() - started) * 1000)
        results[name] = _dict_rows(raw, returned_columns)

    assert results["metrics"][0]["total_cost"] == 2.0
    assert results["metrics"][0]["total_tokens"] == 20
    assert results["metrics"][0]["traces_count"] == 1
    assert results["content"][0]["first_message"] == "live-message"
    assert results["content"][0]["last_message"] == "live-message"
    assert results["attributes"][0]["attrs_string"] == {
        "live_key": "yes",
        "final_status": "Rejected",
    }
    assert "deleted_key" not in results["attributes"][0]["span_attributes_raw"]

    # Generous CI ceilings; local disposable runs are normally <1s for Users
    # and <100ms per Session phase. Production A/B remains a separate sealed
    # read-only gate and is not inferred from these local ceilings.
    assert user_page_elapsed_ms < 2000
    assert user_metrics_elapsed_ms < 2000
    assert sum(phase_timings) < 2000
