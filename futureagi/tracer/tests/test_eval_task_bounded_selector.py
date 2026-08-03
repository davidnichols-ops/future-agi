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
    [(SpanListQueryBuilder, "id"), (TraceListQueryBuilder, "trace_id")],
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
        (RowType.TRACES, "trace_id", 200),
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
        assert captured["builder"]._bounded_bulk_scan is True


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

    def fake_read(**_kwargs):
        return BoundedFilterPage(
            rows=[
                {
                    "trace_id": "trace-a",
                    "start_time": witness_start,
                    "filter_witness_0": ("span-status", witness_start),
                    "filter_witness_1": ("span-tier", witness_start),
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
    result = row_resolver._resolve_bounded_historical_span_ids(
        object(),
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


@pytest.mark.parametrize("row_type", [RowType.SPANS, RowType.TRACES])
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
            assert timeout_ms <= 750
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
