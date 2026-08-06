"""Regression contracts for bounded session attribute witness reads."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from tracer.services.clickhouse.v2.query_builders.session_list import (
    SessionListQueryBuilderV2,
)

PROJECT_ID = "00000000-0000-4000-8000-000000000001"
END = datetime(2026, 7, 31, 7, 0)
START = END - timedelta(days=7)
CANDIDATE_SESSION_ID = "00000000-0000-4000-8000-000000000002"


def _time_filter() -> dict:
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [START.isoformat(), END.isoformat()],
        },
    }


def _attribute_filter(
    key: str,
    value: object,
    *,
    filter_type: str = "text",
    operation: str = "equals",
) -> dict:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": filter_type,
            "filter_op": operation,
            "filter_value": value,
        },
    }


def _builder(*attribute_filters: dict) -> SessionListQueryBuilderV2:
    return SessionListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[_time_filter(), *attribute_filters],
        bounded_internal_scan=True,
    )


@pytest.mark.unit
def test_session_seed_pushes_indexed_scalar_witness_before_group_and_limit() -> None:
    builder = _builder(_attribute_filter("final_status", ["Rechazado"], operation="in"))

    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=200,
    )

    witness = "has(attrs_string.keys, %(latest_filter_key_0)s)"
    index_companion = "indexHint(has(mapKeys(attrs_string), %(latest_filter_key_0)s))"
    value_bloom = "hasAny(arrayMap(x -> lower(x), mapValues(attrs_string))"
    assert witness in sql
    assert index_companion in sql
    assert value_bloom not in sql
    assert sql.index(witness) < sql.index("GROUP BY seed_spans.trace_session_id")
    assert sql.index(witness) < sql.index("LIMIT %(filter_seed_limit)s")
    assert params["latest_filter_key_0"] == "final_status"
    assert params["latest_filter_param_0"] == ("rechazado",)
    assert "latest_filter_index_0_0" not in params

    match_sql, match_params = builder.build_filter_match_query([CANDIDATE_SESSION_ID])
    assert "lowerUTF8(toString(latest_attr_value_0)) IN" in match_sql
    assert match_params["latest_filter_param_0"] == ("rechazado",)


@pytest.mark.unit
def test_session_seed_prefers_indexed_scalar_when_json_filter_comes_first() -> None:
    builder = _builder(
        _attribute_filter(
            "customer_context",
            {"country": "CO"},
            filter_type="map",
            operation="contains",
        ),
        _attribute_filter("final_status", ["Rechazado"], operation="in"),
    )

    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=200,
    )

    assert "has(attrs_string.keys, %(latest_filter_key_1)s)" in sql
    assert "mapValues(attrs_string)" not in sql
    assert "%(latest_filter_key_0)s" not in sql
    assert params["latest_filter_key_1"] == "final_status"
    assert params["latest_filter_param_1"] == ("rechazado",)
    assert "latest_filter_index_1_0" not in params
    assert "latest_filter_key_0" not in params

    match_sql, match_params = builder.build_filter_match_query([CANDIDATE_SESSION_ID])
    assert "latest_json_map_exists_0" in match_sql
    assert "latest_attr_exists_1" in match_sql
    assert match_params["latest_filter_key_0"] == "customer_context"
    assert match_params["latest_filter_key_1"] == "final_status"


@pytest.mark.unit
def test_one_year_multi_attribute_session_filter_stays_candidate_scoped() -> None:
    """Long windows change the number of bounded slices, not classifier width."""

    long_start = END - timedelta(days=365)
    filters = [
        {
            "column_id": "created_at",
            "filter_config": {
                "filter_type": "datetime",
                "filter_op": "between",
                "filter_value": [long_start.isoformat(), END.isoformat()],
            },
        },
        _attribute_filter("final_status", ["Rechazado"], operation="in"),
        _attribute_filter(
            "customer_context",
            {"country": "CO"},
            filter_type="map",
            operation="contains",
        ),
        _attribute_filter(
            "quality_score",
            0.75,
            filter_type="number",
            operation="greater_than_or_equal",
        ),
        _attribute_filter(
            "reviewed",
            True,
            filter_type="boolean",
            operation="equals",
        ),
    ]
    builder = SessionListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=filters,
        bounded_internal_scan=True,
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=200,
    )
    match_sql, match_params = builder.build_filter_match_query([CANDIDATE_SESSION_ID])

    assert builder.parse_time_range(filters) == (long_start, END)
    assert seed_params["filter_slice_start"] == END - timedelta(minutes=5)
    assert seed_params["filter_slice_end"] == END
    assert "LIMIT %(filter_seed_limit)s" in seed_sql
    assert "JSONExtract" not in seed_sql
    assert "candidate_filter_session_ids" in match_params
    assert match_params["candidate_filter_session_ids"] == (CANDIDATE_SESSION_ID,)
    assert "latest_json_map_exists_1" in match_sql
    assert "latest_attr_exists_0" in match_sql
    assert "latest_attr_exists_2" in match_sql
    assert "latest_attr_exists_3" in match_sql
    assert "LIMIT %(bounded_match_limit)s" in match_sql


@pytest.mark.unit
@pytest.mark.parametrize(
    ("filter_type", "value", "raw_expression"),
    [
        (
            "map",
            {"country": "CO"},
            "JSONExtractRaw(attributes_extra, %(latest_filter_key_0)s)",
        ),
        (
            "array",
            ["vip", 3, True],
            "JSONExtractArrayRaw(attributes_extra, %(latest_filter_key_0)s)",
        ),
    ],
)
def test_positive_json_only_filter_defers_parsing_to_exact_classifier(
    filter_type: str,
    value: object,
    raw_expression: str,
) -> None:
    builder = _builder(
        _attribute_filter(
            "customer_context",
            value,
            filter_type=filter_type,
            operation="contains",
        )
    )

    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=200,
    )

    assert "JSONHas(attributes_extra, %(latest_filter_key_0)s)" not in sql
    assert raw_expression not in sql
    assert "latest_filter_key_0" not in params

    plans, residual = builder._bounded_span_filter_parts()
    assert residual == []
    assert plans[0].raw_witness_predicate is None
    match_sql, match_params = builder.build_filter_match_query([CANDIDATE_SESSION_ID])
    assert raw_expression in match_sql
    assert match_params["latest_filter_key_0"] == "customer_context"


@pytest.mark.unit
def test_negative_only_session_filter_does_not_claim_a_raw_witness() -> None:
    builder = _builder(
        _attribute_filter(
            "final_status",
            "Rechazado",
            operation="not_equals",
        )
    )

    plans, residual = builder._bounded_span_filter_parts()
    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=200,
    )

    assert residual == []
    assert len(plans) == 1
    assert plans[0].raw_witness_predicate is None
    assert "mapContains(attrs_string" not in sql
    assert "latest_filter_key_0" not in params


@pytest.mark.unit
def test_session_match_keeps_exact_filter_on_roots_before_session_grouping() -> None:
    builder = _builder(_attribute_filter("final_status", ["Rechazado"], operation="in"))

    sql, _ = builder.build_filter_match_query([CANDIDATE_SESSION_ID])

    resolved_roots = sql.split("resolved_root_sessions AS (", 1)[1].split(
        "\n        )", 1
    )[0]
    sessions = sql.split("sessions AS (", 1)[1]
    assert "latest_attr_exists_0" in resolved_roots
    assert "lowerUTF8(toString(latest_attr_value_0)) IN" in resolved_roots
    assert "FROM resolved_root_sessions" in sessions
    assert "session_id IN (SELECT session_id FROM matching_root_sessions)" not in sql
