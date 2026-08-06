from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)
from tracer.views.trace import (
    _append_trace_attribute_value,
    _trace_attribute_value_token,
)

pytestmark = pytest.mark.unit


def test_trace_attribute_hydration_replays_all_direct_write_columns():
    builder = TraceListQueryBuilderV2(
        project_ids=["00000000-0000-4000-8000-000000000001"],
        filters=[
            {
                "column_id": "created_at",
                "filter_config": {
                    "filter_type": "datetime",
                    "filter_op": "between",
                    "filter_value": [
                        datetime(2026, 7, 1, tzinfo=UTC),
                        datetime(2026, 8, 1, tzinfo=UTC),
                    ],
                },
            }
        ],
    )
    builder.build()

    sql, params = builder.build_span_attributes_query(["trace-1"])

    assert "argMax(attrs_string, _version)" in sql
    assert "argMax(attrs_number, _version)" in sql
    assert "argMax(attrs_bool, _version)" in sql
    assert "argMax(tuple(attributes_extra), _version).1" in sql
    assert "argMax(is_deleted, _version) AS latest_is_deleted" in sql
    assert "WHERE latest_is_deleted = 0" in sql
    assert "GROUP BY project_id, trace_id, id, start_time" in sql
    assert "length(mapKeys(latest_attrs_bool)) > 0" in sql
    assert params["attr_trace_ids"] == ("trace-1",)


@pytest.mark.parametrize("structured_first", [False, True])
def test_trace_attribute_accumulator_preserves_mixed_values_in_both_orders(
    structured_first,
):
    values = []
    ordered = (
        [{"attempt": 2}, "Rechazado"]
        if structured_first
        else ["Rechazado", {"attempt": 2}]
    )

    for value in ordered:
        _append_trace_attribute_value(values, value)
    _append_trace_attribute_value(values, {"attempt": 2})

    assert sorted(values, key=_trace_attribute_value_token) == [
        "Rechazado",
        {"attempt": 2},
    ]


def test_trace_attribute_accumulator_normalizes_direct_write_boolean_once():
    values = []

    _append_trace_attribute_value(values, True)
    _append_trace_attribute_value(values, "true")

    assert values == ["true"]
