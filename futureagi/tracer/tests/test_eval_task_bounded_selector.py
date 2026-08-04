from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from tracer.models.eval_task import RowType, RunType
from tracer.selectors.eval_tasks import row_resolver
from tracer.selectors.trace_filter_reads import (
    BoundedFilterPage,
    read_bounded_filter_page,
)
from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
    supports_span_filters,
    supports_trace_filters,
    targets_span_filter_domain,
    targets_trace_filter_domain,
)
from tracer.services.clickhouse.query_builders.session_list import (
    SessionListQueryBuilder,
)
from tracer.services.clickhouse.query_builders.span_list import SpanListQueryBuilder
from tracer.services.clickhouse.query_builders.trace_list import TraceListQueryBuilder
from tracer.services.clickhouse.query_service import QueryResult

PROJECT_ID = "00000000-0000-4000-8000-000000000001"
START = datetime(2026, 1, 1)
END = START + timedelta(days=365)


def _time_filter() -> dict:
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [START.isoformat(), END.isoformat()],
        },
    }


def _attribute_filter(key: str, value: str) -> dict:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": "text",
            "filter_op": "equals",
            "filter_value": value,
        },
    }


@pytest.mark.parametrize(
    ("builder_class", "identity"),
    [
        (SpanListQueryBuilder, "id"),
        (TraceListQueryBuilder, "trace_id"),
        (SessionListQueryBuilder, "session_id"),
    ],
)
def test_internal_bounded_seed_pushes_sampling_before_limit(
    builder_class, identity: str
) -> None:
    builder = builder_class(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
        bounded_internal_scan=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=25.0,
    )

    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=25,
    )

    assert builder.supports_bounded_filter_scan() is True
    if builder_class is SpanListQueryBuilder:
        assert "toString(trace_id)" in sql
        assert "toString(id)" in sql
    else:
        assert f"cityHash64(%(bounded_sampling_salt)s, toString({identity}))" in sql
    assert sql.index("cityHash64") < sql.index("LIMIT %(filter_seed_limit)s")
    assert params["bounded_sampling_salt"] == "task-salt"
    assert params["bounded_sampling_rate"] == 25.0


def test_internal_bounded_span_scan_supports_time_only_tasks() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter()],
        bounded_internal_scan=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=100.0,
    )

    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=25,
    )
    match_sql, _ = builder.build_filter_match_query(["span-a"])

    assert "WHERE 1 = 1" in seed_sql
    assert "AND 1 = 1" in match_sql


@pytest.mark.parametrize("row_type", [RowType.SPANS, RowType.TRACES])
def test_time_only_eval_resolution_preserves_legacy_id_prefix(
    monkeypatch: pytest.MonkeyPatch,
    row_type: str,
) -> None:
    class CompatibilityReader:
        closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            assert sql == "SELECT exact_id_prefix"
            assert batch_size == 11
            assert settings["max_execution_time"] == 10
            yield ["id-a", "id-b"]

        def close(self):
            self.closed = True

    reader = CompatibilityReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql="SELECT exact_id_prefix",
        params={"start_date": START, "end_date": END},
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={"date_range": [START, END]},
        limit=25,
        batch_size=11,
        row_type=row_type,
    )

    assert ids == ["id-a", "id-b"]
    assert reader.closed is True


def test_task_filters_merge_legacy_and_canonical_lists() -> None:
    canonical = _attribute_filter("prompt_slug", "agent_2_identity_disclosure")
    legacy = _attribute_filter("final_status", "Rejected")

    normalized = row_resolver._task_ui_filters(
        {
            "filters": [canonical],
            "span_attributes_filters": [legacy],
            "date_range": [START, END],
            "trace_id": ["trace-a"],
            "observation_type": ["llm"],
        }
    )

    assert canonical in normalized
    assert legacy in normalized
    assert {item["column_id"] for item in normalized} >= {
        "created_at",
        "trace_id",
        "observation_type",
    }


@pytest.mark.parametrize(
    ("row_type", "identity", "expected_classify_batch"),
    [
        (RowType.SPANS, "id", 200),
        (RowType.TRACES, "trace_id", 100),
    ],
)
def test_bounded_resolver_returns_only_a_complete_latest_state_page(
    monkeypatch: pytest.MonkeyPatch,
    row_type: str,
    identity: str,
    expected_classify_batch: int,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        return BoundedFilterPage(
            rows=[
                {identity: "row-a", "start_time": END - timedelta(minutes=1)},
                {identity: "row-b", "start_time": END - timedelta(minutes=2)},
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=10,
            query_count=2,
            rows_returned=4,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )
    filters = {
        "filters": [_attribute_filter("prompt_slug", "agent_2_identity_disclosure")],
        "span_attributes_filters": [_attribute_filter("final_status", "Rejected")],
        "date_range": [START, END],
    }

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql="baseline-protocol-sql",
        params={"start_date": START, "end_date": END},
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters=filters,
        limit=25,
        batch_size=256,
        row_type=row_type,
    )

    assert ids == ["row-a", "row-b"]
    assert captured["deadline_ms"] == 10_000
    assert captured["max_query_count"] == 128
    assert captured["max_candidates"] == 512
    assert captured["classify_batch_size"] == expected_classify_batch
    assert captured["retry_wide_read_budget"] is True
    assert captured["builder"].supports_bounded_filter_scan() is True
    assert captured["builder"]._bounded_identity_only is True
    if row_type == RowType.TRACES:
        trace_builder = captured["builder"]
        assert trace_builder._bounded_bulk_scan is True
        assert trace_builder._bounded_include_filter_witnesses is False
        assert trace_builder.skip_full_window_filter_anchor_probe() is True
        membership_sql, _ = trace_builder.build_filter_match_query_from_seed_rows(
            [
                {
                    "trace_id": "trace-a",
                    "root_span_id": "root-a",
                    "start_time": END - timedelta(minutes=1),
                }
            ]
        )
        assert "filter_witness_0" not in membership_sql
        assert "argMinIf(tuple(grouped_id, latest_start_time)" not in membership_sql


def test_bounded_historical_session_selector_proves_and_sorts_full_population(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        return BoundedFilterPage(
            rows=[
                {
                    "session_id": "session-b",
                    "start_time": END - timedelta(minutes=1),
                },
                {
                    "session_id": "session-a",
                    "start_time": END - timedelta(minutes=2),
                },
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=10,
            query_count=2,
            rows_returned=4,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql=None,
        params=None,
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=25.0,
        filters={
            "filters": [_attribute_filter("final_status", "Rejected")],
            "date_range": [START, END],
        },
        limit=25,
        batch_size=256,
        row_type=RowType.SESSIONS,
    )

    assert ids == ["session-a", "session-b"]
    assert captured["key_field"] == "session_id"
    assert captured["page_size"] == 25
    assert captured["deadline_ms"] == 10_000
    assert captured["max_query_count"] == 128
    assert captured["max_candidates"] == 512
    assert captured["classify_batch_size"] == 50
    assert captured["builder"]._bounded_internal_scan is True
    assert captured["builder"]._bounded_sampling_salt == "task-salt"
    assert captured["builder"]._bounded_sampling_rate == 25.0


def test_bounded_historical_session_selector_rejects_capped_population(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[{"session_id": "must-not-escape", "start_time": END}],
            has_more=True,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=26,
            elapsed_ms=10,
            query_count=2,
            rows_returned=26,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="too large",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            object(),
            sql=None,
            params=None,
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=256,
            row_type=RowType.SESSIONS,
        )


@pytest.mark.parametrize(
    ("row_type", "identity"),
    [(RowType.SPANS, "id"), (RowType.TRACES, "trace_id")],
)
def test_eval_task_reuses_candidate_scoped_map_filter(
    monkeypatch: pytest.MonkeyPatch,
    row_type: str,
    identity: str,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        row = {identity: "selected-id", "start_time": END - timedelta(minutes=1)}
        if row_type == RowType.SPANS:
            row.update({"project_id": PROJECT_ID, "trace_id": "trace-a"})
        return BoundedFilterPage(
            rows=[row],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )
    filters = {
        "span_attributes_filters": [
            {
                "column_id": "customer.context",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": "map",
                    "filter_op": "contains",
                    "filter_value": {"tier": "vip", "attempt": 2},
                },
            }
        ],
        "date_range": [START, END],
    }

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql="must-not-run",
        params={"start_date": START, "end_date": END},
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters=filters,
        limit=25,
        batch_size=17,
        row_type=row_type,
    )

    assert ids == ["selected-id"]
    builder = captured["builder"]
    assert builder.supports_bounded_filter_scan() is True
    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=25,
    )
    match_sql, match_params = builder.build_filter_match_query(["selected-id"])
    assert "attributes_extra" not in seed_sql
    assert "latest_filter_key_0" not in seed_params
    assert "JSONExtractRaw(attributes_extra" in match_sql
    assert "vip" not in match_sql
    assert "vip" in match_params.values()


@pytest.mark.parametrize(
    ("builder_class", "identity"),
    [(SpanListQueryBuilder, "id"), (TraceListQueryBuilder, "trace_id")],
)
def test_eval_internal_classifier_projects_only_identity_and_order(
    builder_class, identity: str
) -> None:
    builder = builder_class(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
        bounded_internal_scan=True,
        bounded_identity_only=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=100.0,
    )

    sql, _ = builder.build_filter_match_query([f"{identity}-a"])

    assert f"AS {identity}" in sql
    assert "AS start_time" in sql
    if builder_class is SpanListQueryBuilder:
        assert "latest_trace_id AS trace_id" in sql
    assert "latest_cost AS cost" not in sql
    assert "latest_total_tokens AS total_tokens" not in sql


def test_trace_eval_classifier_projects_one_physical_witness_per_any_span_leaf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}
    filters = [
        _time_filter(),
        _attribute_filter("final_status", "Rejected"),
        _attribute_filter("customer_tier", "vip"),
    ]
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        bounded_internal_scan=True,
        bounded_identity_only=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=100.0,
    )

    sql, _ = builder.build_filter_match_query(["trace-a"])

    assert "filter_witness_0" in sql
    assert "filter_witness_1" in sql
    assert sql.count("argMinIf(tuple(grouped_id, latest_start_time)") == 2
    assert "tuple(latest_start_time, grouped_id)" in sql

    witness_start = END - timedelta(minutes=1)

    def fake_read(**kwargs):
        captured.update(kwargs)
        return BoundedFilterPage(
            rows=[
                {
                    "trace_id": "trace-a",
                    "root_span_id": "root-a",
                    "start_time": witness_start,
                }
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    class ReplayAnalytics:
        def execute_ch_query(self, _query, params, *, timeout_ms, settings):
            assert params["candidate_trace_ids"] == ("trace-a",)
            assert timeout_ms <= 1_500
            assert settings["max_execution_time"] == 2
            assert settings["max_threads"] == 1
            assert settings["max_rows_to_read"] == 5_000_000
            assert settings["max_memory_usage"] == 256 * 1024 * 1024
            assert settings["max_bytes_to_read"] == 512 * 1024 * 1024
            assert settings["max_result_rows"] == 1
            return QueryResult(
                [
                    {
                        "trace_id": "trace-a",
                        "root_span_id": "root-a",
                        "start_time": witness_start,
                        "filter_witness_0": ("span-status", witness_start),
                        "filter_witness_1": ("span-tier", witness_start),
                    }
                ],
                1,
                "clickhouse",
                1.0,
            )

    result = row_resolver._resolve_bounded_historical_span_ids(
        ReplayAnalytics(),
        sql=None,
        params=None,
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={
            "filters": filters,
            "date_range": [START, END],
        },
        limit=25,
        batch_size=25,
        row_type=RowType.TRACES,
        include_trace_filter_witnesses=True,
    )

    assert result.ids == ("trace-a",)
    assert [witness.span_id for witness in result.trace_filter_witnesses] == [
        "span-status",
        "span-tier",
    ]
    assert [witness.column_id for witness in result.trace_filter_witnesses] == [
        "final_status",
        "customer_tier",
    ]
    assert captured["deadline_ms"] == 12_000
    assert captured["max_query_count"] == 112
    assert captured["classify_batch_size"] == 100
    phase_one_builder = captured["builder"]
    assert phase_one_builder._bounded_include_filter_witnesses is False
    membership_sql, _ = phase_one_builder.build_filter_match_query_from_seed_rows(
        [
            {
                "trace_id": "trace-a",
                "root_span_id": "root-a",
                "start_time": witness_start,
            }
        ]
    )
    assert "filter_witness_0" not in membership_sql
    assert "argMinIf(tuple(grouped_id, latest_start_time)" not in membership_sql


def test_ui_default_100k_trace_task_accepts_a_complete_sparse_population(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the real 100k wire limit through the real bounded reader.

    The population is deliberately smaller than the 10k executable buffer. The
    direct child-span seed is unordered, so success also proves the reader
    exhausted the complete request window before returning the ID-sorted set.
    """

    window_start = END - timedelta(minutes=10)
    root_start = END - timedelta(minutes=1)
    source_rows = {
        "trace-b": {
            "trace_id": "trace-b",
            "root_span_id": "root-b",
            "start_time": root_start,
            "matched_span_id": "status-b",
        },
        "trace-a": {
            "trace_id": "trace-a",
            "root_span_id": "root-a",
            "start_time": root_start - timedelta(seconds=1),
            "matched_span_id": "status-a",
        },
    }

    class SparsePopulationAnalytics:
        calls: list[tuple[str, dict]] = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((query, params))
            assert timeout_ms <= 1_500
            assert settings["max_threads"] == 1
            assert settings["max_rows_to_read"] == 5_000_000
            assert settings["max_memory_usage"] == 256 * 1024 * 1024
            assert settings["max_bytes_to_read"] == 512 * 1024 * 1024

            candidate_ids = params.get("candidate_trace_ids")
            if candidate_ids is not None:
                # Population-proof mode is one phase: each exact membership
                # classifier carries the physical witness and there is no
                # post-page replay query.
                assert "filter_witness_0" in query
                rows = [
                    {
                        **source_rows[trace_id],
                        "filter_witness_0": (
                            source_rows[trace_id]["matched_span_id"],
                            source_rows[trace_id]["start_time"],
                        ),
                    }
                    for trace_id in candidate_ids
                ]
                return QueryResult(rows, len(rows), "clickhouse", 1.0)

            assert "id AS matched_span_id" in query
            assert "parent_span_id IS NULL" not in query
            rows = [
                {
                    "project_id": PROJECT_ID,
                    "trace_id": row["trace_id"],
                    "matched_span_id": row["matched_span_id"],
                    "start_time": row["start_time"],
                }
                for row in source_rows.values()
                if params["filter_slice_start"]
                <= row["start_time"]
                < params["filter_slice_end"]
            ]
            return QueryResult(rows, len(rows), "clickhouse", 1.0)

    captured: dict = {}
    real_read = read_bounded_filter_page

    def capture_read(**kwargs):
        captured.update(kwargs)
        return real_read(**kwargs)

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", capture_read
    )

    analytics = SparsePopulationAnalytics()
    result = row_resolver._resolve_bounded_historical_span_ids(
        analytics,
        sql=None,
        params=None,
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={
            "filters": [_attribute_filter("final_status", "Rejected")],
            "date_range": [window_start, END],
        },
        limit=100_000,
        batch_size=10_000,
        row_type=RowType.TRACES,
        include_trace_filter_witnesses=True,
    )

    assert result.ids == ("trace-a", "trace-b")
    assert {
        (witness.trace_id, witness.span_id) for witness in result.trace_filter_witnesses
    } == {("trace-a", "status-a"), ("trace-b", "status-b")}
    seed_queries = [
        query
        for query, params in analytics.calls
        if "candidate_trace_ids" not in params
    ]
    classifier_queries = [
        query for query, params in analytics.calls if "candidate_trace_ids" in params
    ]
    assert len(seed_queries) == 2
    assert len(classifier_queries) == 1
    assert captured["classify_batch_size"] == 100
    assert captured["builder"]._bounded_include_filter_witnesses is True


def test_trace_eval_witness_replay_uses_hundred_id_batches_with_hard_caps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    witness_start = END - timedelta(minutes=1)
    rows = [
        {
            "trace_id": f"trace-{index:02d}",
            "root_span_id": f"root-{index:02d}",
            "start_time": witness_start - timedelta(seconds=index),
        }
        for index in range(205)
    ]

    def fake_read(**kwargs):
        assert kwargs["max_query_count"] == 112
        return BoundedFilterPage(
            rows=rows,
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=len(rows),
            elapsed_ms=1,
            query_count=12,
            rows_returned=len(rows),
            result_payload_bytes=1_000,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    class ReplayAnalytics:
        batch_sizes: list[int] = []

        def execute_ch_query(self, _query, params, *, timeout_ms, settings):
            trace_ids = params["candidate_trace_ids"]
            self.batch_sizes.append(len(trace_ids))
            assert len(trace_ids) <= 100
            assert timeout_ms <= 1_500
            assert settings == {
                "max_execution_time": 2,
                "max_threads": 1,
                "max_block_size": 8192,
                "max_rows_to_read": 5_000_000,
                "max_memory_usage": 256 * 1024 * 1024,
                "max_bytes_to_read": 512 * 1024 * 1024,
                "read_overflow_mode": "throw",
                "result_overflow_mode": "throw",
                "timeout_overflow_mode": "throw",
                "max_result_rows": len(trace_ids),
            }
            by_trace = {row["trace_id"]: row for row in rows}
            replayed = [
                {
                    **by_trace[trace_id],
                    "filter_witness_0": (f"span-{trace_id}", witness_start),
                }
                for trace_id in trace_ids
            ]
            return QueryResult(replayed, len(replayed), "clickhouse", 1.0)

    analytics = ReplayAnalytics()
    result = row_resolver._resolve_bounded_historical_span_ids(
        analytics,
        sql=None,
        params=None,
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={
            "filters": [_attribute_filter("final_status", "Rejected")],
            "date_range": [START, END],
        },
        limit=250,
        batch_size=250,
        row_type=RowType.TRACES,
        include_trace_filter_witnesses=True,
    )

    assert result.ids == tuple(row["trace_id"] for row in rows)
    assert analytics.batch_sizes == [100, 100, 5]
    assert len(result.trace_filter_witnesses) == len(rows)


def test_trace_eval_witness_replay_preflights_total_query_cap_without_partial_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "trace_id": f"trace-{index:04d}",
            "root_span_id": f"root-{index:04d}",
            "start_time": END - timedelta(seconds=index),
        }
        for index in range(1_601)
    ]

    def fake_read(**kwargs):
        assert kwargs["max_query_count"] == 112
        return BoundedFilterPage(
            rows=rows,
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=len(rows),
            elapsed_ms=1,
            query_count=112,
            rows_returned=len(rows),
            result_payload_bytes=10_000,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    class MustNotReplay:
        def execute_ch_query(self, *_args, **_kwargs):
            raise AssertionError("replay must be rejected before the first CH read")

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="Narrow the time range",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            MustNotReplay(),
            sql=None,
            params=None,
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=2_000,
            batch_size=2_000,
            row_type=RowType.TRACES,
            include_trace_filter_witnesses=True,
        )


def test_trace_eval_witness_replay_never_returns_a_partial_second_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "trace_id": f"trace-{index:02d}",
            "root_span_id": f"root-{index:02d}",
            "start_time": END - timedelta(seconds=index),
        }
        for index in range(101)
    ]

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
        lambda **_kwargs: BoundedFilterPage(
            rows=rows,
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=len(rows),
            elapsed_ms=1,
            query_count=2,
            rows_returned=len(rows),
            result_payload_bytes=500,
            attempts=(),
        ),
    )

    class SecondBatchFails:
        calls = 0

        def execute_ch_query(self, _query, params, **_kwargs):
            self.calls += 1
            if self.calls == 2:
                raise ValueError("simulated bounded replay failure")
            replayed = [
                {
                    **rows[index],
                    "filter_witness_0": (
                        f"span-{trace_id}",
                        rows[index]["start_time"],
                    ),
                }
                for index, trace_id in enumerate(params["candidate_trace_ids"])
            ]
            return QueryResult(replayed, len(replayed), "clickhouse", 1.0)

    analytics = SecondBatchFails()
    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="Narrow the time range",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            analytics,
            sql=None,
            params=None,
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=25,
            row_type=RowType.TRACES,
            include_trace_filter_witnesses=True,
        )
    assert analytics.calls == 2


@pytest.mark.parametrize("drift_field", ["root_span_id", "start_time"])
def test_trace_eval_witness_replay_fails_closed_on_canonical_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
    drift_field: str,
) -> None:
    membership_row = {
        "trace_id": "trace-a",
        "root_span_id": "root-a",
        "start_time": END - timedelta(minutes=1),
    }

    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[membership_row],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=1,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    class DriftAnalytics:
        def execute_ch_query(self, _query, _params, **_kwargs):
            replayed = {
                **membership_row,
                "filter_witness_0": ("span-status", membership_row["start_time"]),
            }
            replayed[drift_field] = (
                "replacement-root"
                if drift_field == "root_span_id"
                else membership_row["start_time"] - timedelta(microseconds=1)
            )
            return QueryResult([replayed], 1, "clickhouse", 1.0)

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="Narrow the time range",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            DriftAnalytics(),
            sql=None,
            params=None,
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=25,
            row_type=RowType.TRACES,
            include_trace_filter_witnesses=True,
        )


def test_trace_legacy_observation_type_is_root_scoped_before_cap() -> None:
    filters = row_resolver._task_ui_filters(
        {
            "date_range": [START, END],
            "observation_type": ["llm"],
        },
        row_type=RowType.TRACES,
        bounded_trace_root=True,
    )
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        bounded_internal_scan=True,
        bounded_identity_only=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=100.0,
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=25,
    )
    match_sql, _ = builder.build_filter_match_query(["trace-a"])

    assert "observation_type" in seed_sql
    assert seed_params["latest_filter_param_0"] == ("llm",)
    assert "argMax(observation_type" in match_sql
    assert "SELECT latest_trace_id" not in match_sql


def test_public_internal_root_col_type_cannot_change_trace_semantics() -> None:
    filters = [
        _time_filter(),
        {
            "column_id": "observation_type",
            "filter_config": {
                "col_type": "INTERNAL_ROOT_METRIC",
                "filter_type": "text",
                "filter_op": "in",
                "filter_value": ["llm"],
            },
        },
    ]

    sql, _ = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
    ).build_filter_match_query(["trace-a"])

    # Without the private marker created only by the eval-task normalizer, the
    # regular public ANY-SPAN observation_type contract remains in force.
    assert "countIf(" in sql
    assert "latest_column_value_0" in sql
    assert "SELECT latest_trace_id" not in sql


@pytest.mark.parametrize("key", ["filters", "span_attributes_filters"])
def test_malformed_saved_filter_entries_fail_closed(key: str) -> None:
    with pytest.raises(ValueError, match="entries must be objects"):
        row_resolver._task_ui_filters({key: ["not-an-object"]})


@pytest.mark.parametrize("filters", ["", [], 0, False])
def test_malformed_falsy_task_filter_wrapper_fails_closed(filters) -> None:
    with pytest.raises(ValueError, match="task filters must be an object"):
        row_resolver._task_ui_filters(filters)
    with pytest.raises(ValueError, match="task filters must be an object"):
        row_resolver._build_sample_query(
            project_id=PROJECT_ID,
            row_type=RowType.SPANS,
            salt="task-salt",
            sampling_rate=100.0,
            filters=filters,
            limit=25,
        )


@pytest.mark.parametrize(
    "filters",
    [
        {"filters": ""},
        {"span_attributes_filters": {}},
        {"date_range": []},
        {"date_range": [START, ""]},
    ],
)
def test_malformed_falsy_saved_filter_fields_fail_closed(filters: dict) -> None:
    with pytest.raises(ValueError):
        row_resolver._task_ui_filters(filters)
    with pytest.raises(ValueError):
        row_resolver._build_sample_query(
            project_id=PROJECT_ID,
            row_type=RowType.SPANS,
            salt="task-salt",
            sampling_rate=100.0,
            filters=filters,
            limit=25,
        )


def test_task_over_10k_proves_population_without_legacy_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def must_not_compile(**_kwargs):
        raise AssertionError("legacy selector must not be compiled")

    def fake_resolve(_analytics, **kwargs):
        captured.update(kwargs)
        return ["selected-id"]

    monkeypatch.setattr(row_resolver, "_build_sample_query", must_not_compile)
    monkeypatch.setattr(
        row_resolver,
        "_resolve_bounded_historical_span_ids",
        fake_resolve,
    )
    task = SimpleNamespace(
        spans_limit=10_001,
        run_type=RunType.HISTORICAL,
        row_type=RowType.SPANS,
        sampling_rate=100.0,
        project_id=PROJECT_ID,
        id="task-id",
        filters={},
        continuous_cursor=None,
        start_time=None,
        created_at=START,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["selected-id"]]
    assert captured["sql"] is None
    assert captured["params"] is None
    assert captured["limit"] == 10_001


def test_task_over_10k_rejects_actual_oversized_population_without_partial_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        return BoundedFilterPage(
            rows=[
                {
                    "id": "must-not-escape",
                    "project_id": PROJECT_ID,
                    "trace_id": "trace-a",
                    "start_time": END,
                }
            ],
            has_more=True,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=10_001,
            elapsed_ms=10,
            query_count=80,
            rows_returned=10_001,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="too large",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            object(),
            sql=None,
            params=None,
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={"date_range": [START, END]},
            limit=10_001,
            batch_size=256,
            row_type=RowType.SPANS,
        )

    assert captured["page_size"] == 10_000


def test_configured_1m_task_returns_complete_small_population_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        return BoundedFilterPage(
            rows=[
                {
                    "id": "span-b",
                    "project_id": PROJECT_ID,
                    "trace_id": "trace-b",
                    "start_time": END,
                },
                {
                    "id": "span-a",
                    "project_id": PROJECT_ID,
                    "trace_id": "trace-a",
                    "start_time": END - timedelta(seconds=1),
                },
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=10,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql=None,
        params=None,
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={"date_range": [START, END]},
        limit=1_000_000,
        batch_size=256,
        row_type=RowType.SPANS,
    )

    assert ids == ["span-a", "span-b"]
    assert captured["page_size"] == 10_000


@pytest.mark.parametrize("row_type", [RowType.SPANS, RowType.TRACES, RowType.SESSIONS])
def test_task_at_10k_routes_directly_to_bounded_selector_without_legacy_sql(
    monkeypatch: pytest.MonkeyPatch,
    row_type: str,
) -> None:
    captured: dict = {}

    def must_not_compile(**_kwargs):
        raise AssertionError("legacy selector must not be compiled")

    def fake_resolve(_analytics, **kwargs):
        captured.update(kwargs)
        return ["selected-id"]

    monkeypatch.setattr(row_resolver, "_build_sample_query", must_not_compile)
    monkeypatch.setattr(
        row_resolver,
        "_resolve_bounded_historical_span_ids",
        fake_resolve,
    )
    task = SimpleNamespace(
        spans_limit=10_000,
        run_type=RunType.HISTORICAL,
        row_type=row_type,
        sampling_rate=100.0,
        project_id=PROJECT_ID,
        id="task-id",
        filters={
            "date_range": [START, END],
            "filters": [_attribute_filter("final_status", "Rejected")],
        },
        continuous_cursor=None,
        start_time=None,
        created_at=START,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["selected-id"]]
    assert captured["sql"] is None
    assert captured["params"] is None
    assert captured["limit"] == 10_000


def test_bounded_resolver_rejects_incomplete_page_without_partial_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[{"id": "must-not-escape", "start_time": END}],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="deadline_exceeded",
            total_rows_lower_bound=1,
            elapsed_ms=4500,
            query_count=12,
            rows_returned=1,
            result_payload_bytes=10,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="Narrow the time range",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            object(),
            sql="baseline-protocol-sql",
            params={"start_date": START, "end_date": END},
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=256,
            row_type=RowType.SPANS,
        )


def test_bounded_span_resolver_rejects_cross_trace_id_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[
                {"id": "shared", "trace_id": "trace-a", "start_time": END},
                {
                    "id": "shared",
                    "trace_id": "trace-b",
                    "start_time": END - timedelta(seconds=1),
                },
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=10,
            query_count=2,
            rows_returned=4,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="could not safely distinguish",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            object(),
            sql="baseline-protocol-sql",
            params={"start_date": START, "end_date": END},
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=256,
            row_type=RowType.SPANS,
        )


def test_bounded_span_resolver_rejects_same_trace_distinct_physical_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[
                {"id": "shared", "trace_id": "trace-a", "start_time": END},
                {
                    "id": "shared",
                    "trace_id": "trace-a",
                    "start_time": END - timedelta(seconds=1),
                },
            ],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=10,
            query_count=2,
            rows_returned=4,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="could not safely distinguish",
    ):
        row_resolver._resolve_bounded_historical_span_ids(
            object(),
            sql="baseline-protocol-sql",
            params={"start_date": START, "end_date": END},
            project_id=PROJECT_ID,
            salt="task-salt",
            sampling_rate=100.0,
            filters={
                "filters": [_attribute_filter("final_status", "Rejected")],
                "date_range": [START, END],
            },
            limit=25,
            batch_size=256,
            row_type=RowType.SPANS,
        )


def test_bounded_span_resolver_dedupes_duplicate_exact_physical_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_read(**_kwargs):
        duplicate = {"id": "shared", "trace_id": "trace-a", "start_time": END}
        return BoundedFilterPage(
            rows=[duplicate, dict(duplicate)],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=1,
            elapsed_ms=10,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=100,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql="baseline-protocol-sql",
        params={"start_date": START, "end_date": END},
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters={
            "filters": [_attribute_filter("final_status", "Rejected")],
            "date_range": [START, END],
        },
        limit=25,
        batch_size=256,
        row_type=RowType.SPANS,
    )

    assert ids == ["shared"]


@pytest.mark.parametrize("row_type", [RowType.SPANS, RowType.TRACES])
@pytest.mark.parametrize("col_type", ["EVAL_METRIC", "ANNOTATION"])
def test_eval_and_annotation_filters_use_candidate_scoped_bounded_reader(
    monkeypatch: pytest.MonkeyPatch,
    row_type: str,
    col_type: str,
) -> None:
    captured: dict = {}

    def fake_read(**kwargs):
        captured.update(kwargs)
        identity = "id" if row_type == RowType.SPANS else "trace_id"
        row = {identity: "selected-id", "start_time": END}
        if row_type == RowType.SPANS:
            row.update({"project_id": PROJECT_ID, "trace_id": "trace-a"})
        return BoundedFilterPage(
            rows=[row],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(
        "tracer.selectors.trace_filter_reads.read_bounded_filter_page", fake_read
    )
    filters = {
        "filters": [
            {
                "column_id": "00000000-0000-4000-8000-000000000099",
                "filter_config": {
                    "col_type": col_type,
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 0.5,
                },
            }
        ],
        "date_range": [START, END],
    }

    ids = row_resolver._resolve_bounded_historical_span_ids(
        object(),
        sql="SELECT exact_legacy_id",
        params={"start_date": START, "end_date": END},
        project_id=PROJECT_ID,
        salt="task-salt",
        sampling_rate=100.0,
        filters=filters,
        limit=25,
        batch_size=17,
        row_type=row_type,
    )

    assert ids == ["selected-id"]
    builder = captured["builder"]
    assert builder.supports_bounded_filter_scan() is True
    assert captured["max_candidates"] == 512
    assert captured["classify_batch_size"] == 200


@pytest.mark.parametrize("row_type", [RowType.SPANS, RowType.TRACES])
@pytest.mark.parametrize("limit", [5_100, 10_000])
def test_shared_candidate_reader_proves_large_eval_prefix_within_query_cap(
    row_type: str,
    limit: int,
) -> None:
    identity_key = "id" if row_type == RowType.SPANS else "trace_id"
    started_at = END - timedelta(minutes=1)
    rows = []
    for index in range(limit + 1):
        identity = f"row-{index:05d}"
        row = {identity_key: identity, "start_time": started_at}
        if row_type == RowType.SPANS:
            row.update(
                {
                    "project_id": PROJECT_ID,
                    "trace_id": f"trace-{index:05d}",
                }
            )
        rows.append(row)

    class SyntheticBuilder:
        def parse_time_range(self, _filters):
            return START, END

        @staticmethod
        def filter_seed_proves_result_order():
            return True

        @staticmethod
        def recommended_filter_classify_batch_size():
            return 200

        @staticmethod
        def bounded_filter_row_identity(row):
            if row_type == RowType.SPANS:
                return (
                    row["project_id"],
                    row["trace_id"],
                    row["id"],
                    row["start_time"],
                )
            return row["trace_id"]

        @staticmethod
        def bounded_filter_row_order_token(row):
            if row_type == RowType.SPANS:
                return (row["id"], row["trace_id"], row["project_id"])
            return row["trace_id"]

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
        def build_filter_match_query_from_seed_rows(candidate_rows):
            return "classify", {"candidate_rows": candidate_rows}

    def row_key(row):
        return row["start_time"], SyntheticBuilder.bounded_filter_row_order_token(row)

    class SyntheticAnalytics:
        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            assert timeout_ms <= 1_500
            if query == "classify":
                assert settings["max_result_rows"] == 200
                result_rows = list(params["candidate_rows"])
            else:
                assert 200 <= settings["max_result_rows"] <= 512
                result_rows = [
                    row
                    for row in rows
                    if params["slice_start"] <= row["start_time"] < params["slice_end"]
                ]
                before_start = params["before_start_time"]
                if before_start is not None:
                    result_rows = [
                        row
                        for row in result_rows
                        if row_key(row) < (before_start, params["before_id"])
                    ]
                result_rows = sorted(result_rows, key=row_key, reverse=True)[
                    : params["limit"]
                ]
            return QueryResult(result_rows, len(result_rows), "clickhouse", 0.0)

    page = read_bounded_filter_page(
        builder=SyntheticBuilder(),
        analytics=SyntheticAnalytics(),
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
        key_field=identity_key,
        page_number=0,
        page_size=limit,
        deadline_ms=10_000,
        max_seed_attempts=128,
        max_candidates=200,
        max_query_count=128,
        classify_batch_size=200,
    )

    assert page.complete is True
    assert len(page.rows) == limit
    assert page.has_more is True
    assert page.query_count <= 102


def test_population_proof_buffers_dense_10k_sentinel_within_128_queries() -> None:
    started_at = END - timedelta(minutes=1)
    rows = [
        {"trace_id": f"trace-{index:05d}", "start_time": started_at}
        for index in range(10_001)
    ]

    class PopulationBuilder:
        @staticmethod
        def parse_time_range(_filters):
            return END - timedelta(minutes=5), END

        @staticmethod
        def filter_seed_proves_result_order():
            return False

        @staticmethod
        def filter_seed_proves_population_bound():
            return True

        @staticmethod
        def recommended_filter_classify_batch_size():
            return 100

        @staticmethod
        def recommended_filter_seed_batch_size():
            return 512

        @staticmethod
        def bounded_filter_row_identity(row):
            return row["trace_id"]

        @staticmethod
        def bounded_filter_row_order_token(row):
            return row["trace_id"]

        bounded_filter_seed_identity = bounded_filter_row_identity
        bounded_filter_seed_order_token = bounded_filter_row_order_token

        @staticmethod
        def build_filter_seed_page(
            *,
            slice_start,
            slice_end,
            limit,
            before_start_time=None,
            before_id=None,
        ):
            return "direct_seed", {
                "slice_start": slice_start,
                "slice_end": slice_end,
                "limit": limit,
                "before_start_time": before_start_time,
                "before_id": before_id,
            }

        @staticmethod
        def build_filter_match_query_from_seed_rows(candidate_rows):
            return "classify_with_witness", {"candidate_rows": candidate_rows}

    class PopulationAnalytics:
        calls: list[str] = []

        def execute_ch_query(self, query, params, **_kwargs):
            self.calls.append(query)
            if query == "classify_with_witness":
                result_rows = list(params["candidate_rows"])
            else:
                result_rows = sorted(
                    rows,
                    key=lambda row: (row["start_time"], row["trace_id"]),
                    reverse=True,
                )
                if params["before_start_time"] is not None:
                    boundary = params["before_start_time"], params["before_id"]
                    result_rows = [
                        row
                        for row in result_rows
                        if (row["start_time"], row["trace_id"]) < boundary
                    ]
                result_rows = result_rows[: params["limit"]]
            return QueryResult(
                result_rows,
                len(result_rows),
                "clickhouse",
                0.0,
            )

    analytics = PopulationAnalytics()
    page = read_bounded_filter_page(
        builder=PopulationBuilder(),
        analytics=analytics,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
        ],
        key_field="trace_id",
        page_number=0,
        page_size=10_000,
        deadline_ms=60_000,
        max_seed_attempts=128,
        max_candidates=512,
        max_query_count=128,
        classify_batch_size=100,
    )

    assert page.complete is True
    assert page.has_more is True
    assert len(page.rows) == 10_000
    assert analytics.calls.count("direct_seed") == 20
    assert analytics.calls.count("classify_with_witness") == 101
    assert page.query_count == 121


@pytest.mark.parametrize(
    ("builder_class", "supports", "targets"),
    [
        (SpanListQueryBuilder, supports_span_filters, targets_span_filter_domain),
        (TraceListQueryBuilder, supports_trace_filters, targets_trace_filter_domain),
    ],
)
def test_legacy_system_metric_alias_uses_its_denormalized_latest_column(
    builder_class,
    supports,
    targets,
) -> None:
    # ``tokens`` is a legacy SYSTEM_METRIC alias for the total_tokens column.
    # Candidate classification must keep that mapping rather than reading a
    # same-named custom attribute or forcing a broad compatibility scan.
    filters = [
        _time_filter(),
        {
            "column_id": "tokens",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "number",
                "filter_op": "greater_than",
                "filter_value": 10,
            },
        },
    ]

    assert supports(filters) is True
    assert targets(filters) is True
    builder = builder_class(project_id=PROJECT_ID, filters=filters)
    assert builder.bounded_filter_degraded_error_code() is None
    assert builder.supports_bounded_filter_scan() is True
    sql, _ = builder.build_filter_match_query(["candidate-id"])
    assert "argMax(tuple(total_tokens), _peerdb_version).1" in sql
    assert "latest_filter_key" not in sql
