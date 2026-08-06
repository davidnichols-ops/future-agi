"""Exact attribute-detail latest-state and result-shape regressions."""

from __future__ import annotations

from datetime import UTC, datetime

from tracer.serializers.dashboard import DashboardFilterValuesQuerySerializer
from tracer.serializers.span_attributes import SpanAttributeDetailResponseSerializer
from tracer.services.clickhouse.attribute_reads import AttributeQueryPage
from tracer.services.clickhouse.exact_attribute_detail import (
    EXACT_ATTRIBUTE_DETAIL_SQL,
    read_exact_attribute_detail,
)

PROJECT_ID = "c4de3065-12b5-488c-a814-aa1c8e3f856f"
NOW = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


class _Executor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, query, params, **kwargs):
        self.calls.append((query, params, kwargs))
        return AttributeQueryPage(list(self.rows), 1.0)


def test_exact_sql_applies_mutable_predicates_only_after_latest_state_replay():
    latest_source = EXACT_ATTRIBUTE_DETAIL_SQL.split("active_values AS", 1)[0]

    assert "argMax(" in latest_source
    assert "GROUP BY project_id, trace_id, id, start_time" in latest_source
    assert "mapContains" not in latest_source
    assert "JSONHas" not in latest_source
    assert "is_deleted = 0" not in latest_source
    assert EXACT_ATTRIBUTE_DETAIL_SQL.index(
        "argMax("
    ) < EXACT_ATTRIBUTE_DETAIL_SQL.index("mapContains")
    assert EXACT_ATTRIBUTE_DETAIL_SQL.index(
        "argMax("
    ) < EXACT_ATTRIBUTE_DETAIL_SQL.index("tupleElement(latest_state, 1) = 0")


def test_exact_detail_parses_full_distribution_and_weighted_numeric_stats():
    executor = _Executor(
        [
            {
                "attribute_type": "number",
                "value_json": "10.0",
                "value_count": 3,
                "type_count": 5,
                "unique_values": 2,
                "numeric_min": 10.0,
                "numeric_max": 20.0,
                "numeric_avg": 14.0,
                "numeric_p50": 10.0,
                "numeric_p95": 20.0,
            },
            {
                "attribute_type": "number",
                "value_json": "20.0",
                "value_count": 2,
                "type_count": 5,
                "unique_values": 2,
                "numeric_min": 10.0,
                "numeric_max": 20.0,
                "numeric_avg": 14.0,
                "numeric_p50": 10.0,
                "numeric_p95": 20.0,
            },
        ]
    )

    payload = read_exact_attribute_detail(
        project_id=PROJECT_ID,
        attribute_key="latency.score",
        executor=executor,
        window_end=NOW,
    )

    assert payload["query_complete"] is True
    assert payload["query_status"] == "complete"
    assert payload["query_sampled"] is False
    assert payload["type"] == "number"
    assert payload["count"] == 5
    assert payload["unique_values"] == 2
    assert payload["top_values"] == [
        {"value": 10.0, "count": 3, "percentage": 60.0},
        {"value": 20.0, "count": 2, "percentage": 40.0},
    ]
    assert payload["stats"] == {
        "min": 10.0,
        "max": 20.0,
        "avg": 14.0,
        "p50": 10.0,
        "p95": 20.0,
    }
    query, params, kwargs = executor.calls[0]
    assert query == EXACT_ATTRIBUTE_DETAIL_SQL
    assert params["attribute_key"] == "latency.score"
    assert kwargs["settings"]["max_rows_to_read"] == 0
    assert kwargs["settings"]["max_bytes_to_read"] == 0


def test_exact_detail_empty_result_is_complete_and_not_sampled():
    payload = read_exact_attribute_detail(
        project_id=PROJECT_ID,
        attribute_key="removed.by.latest.version",
        executor=_Executor([]),
        window_end=NOW,
    )

    assert payload["type"] is None
    assert payload["count"] == 0
    assert payload["unique_values"] == 0
    assert payload["top_values"] == []
    assert payload["query_complete"] is True
    assert payload["query_status"] == "complete"
    assert payload["query_sampled"] is False


def test_exact_detail_dominant_type_is_deterministic():
    shared = {
        "value_count": 2,
        "type_count": 2,
        "unique_values": 1,
        "numeric_min": None,
        "numeric_max": None,
        "numeric_avg": None,
        "numeric_p50": None,
        "numeric_p95": None,
    }
    payload = read_exact_attribute_detail(
        project_id=PROJECT_ID,
        attribute_key="mixed",
        executor=_Executor(
            [
                {**shared, "attribute_type": "boolean", "value_json": "true"},
                {**shared, "attribute_type": "string", "value_json": '"true"'},
            ]
        ),
        window_end=NOW,
    )

    assert payload["type"] == "string"
    assert payload["top_values"][0]["value"] == "true"


def test_attribute_detail_exact_worker_namespace_dispatches(monkeypatch):
    from tracer.tasks import exact_aggregation

    identity = {
        "workspace_id": "workspace-a",
        "project_id": PROJECT_ID,
        "attribute_key": "final_status",
        "horizon_days": 365,
    }
    expected = {
        "key": "final_status",
        "type": "string",
        "count": 0,
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }
    captured = []

    def load(received_identity):
        captured.append(received_identity)
        return expected

    monkeypatch.setattr(exact_aggregation, "_attribute_detail_payload", load)

    assert (
        exact_aggregation._load_exact_payload("attribute-detail", identity) == expected
    )
    assert captured == [identity]


def test_attribute_contracts_accept_all_types_and_pending_exact_response():
    for attribute_type in ("string", "number", "boolean", "array", "map", "json"):
        query = DashboardFilterValuesQuerySerializer(
            data={
                "metric_name": "metadata",
                "metric_type": "custom_attribute",
                "source": "traces",
                "page_size": 10,
                "attribute_type": attribute_type,
            }
        )
        assert query.is_valid(), query.errors
        assert query.validated_data["attribute_type"] == attribute_type

    pending = SpanAttributeDetailResponseSerializer(
        data={
            "key": "metadata",
            "type": None,
            "count": 0,
            "unique_values": 0,
            "top_values": [],
            "query_complete": False,
            "query_status": "pending",
            "query_sampled": False,
            "query_refreshing": True,
        }
    )
    assert pending.is_valid(), pending.errors
