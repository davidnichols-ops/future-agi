"""Voice filter aliases: the FE metrics picker sends these column_ids, but the
values are stored under different CH attribute keys. Each must resolve to the
stored key via VOICE_SYSTEM_METRIC_EXPRS, not fall through to a span-attr lookup
on the (non-existent) FE name."""

from datetime import datetime, timedelta

import pytest

from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder
from tracer.services.clickhouse.query_builders.voice_call_list import (
    VoiceCallFilterBuilder,
)
from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_builders.voice_call_list import (
    VoiceCallFilterBuilderV2,
    VoiceCallListQueryBuilderV2,
)

PROJECT_ID = "00000000-0000-4000-8000-000000000001"
WINDOW_END = datetime(2026, 8, 5, 12, 0)
WINDOW_START = WINDOW_END - timedelta(hours=1)

# FE column_id (from /tracer/dashboard/metrics/) -> stored CH attr key it must read.
VOICE_ALIASES = {
    "talk_ratio": "call.talk_ratio",
    "agent_latency": "avg_agent_latency_ms",
    "ai_interruptions": "ai_interruption_count",
    "user_interruptions": "user_interruption_count",
    "stop_time_after_interruption": "avg_stop_time_after_interruption_ms",
    "llm_cost": "cost_breakdown.llm",
    "stt_cost": "cost_breakdown.stt",
    "tts_cost": "cost_breakdown.tts",
    "total_cost": "cost_breakdown.total",
    "customer_cost": "cost_breakdown.total",
    "llm_latency": "modelLatencyAverage",
    "stt_latency": "transcriberLatencyAverage",
    "tts_latency": "voiceLatencyAverage",
    "response_time": "turnLatencyAverage",
}


@pytest.mark.unit
@pytest.mark.parametrize("col_id,stored_key", list(VOICE_ALIASES.items()))
def test_voice_filter_alias_resolves_to_stored_key(col_id, stored_key):
    where, _ = ClickHouseFilterBuilder().translate(
        [
            {
                "column_id": col_id,
                "filter_config": {
                    "filter_type": "number",
                    "filter_op": "greater_than",
                    "filter_value": 0,
                    "col_type": "SYSTEM_METRIC",
                },
            }
        ]
    )
    assert stored_key in where, f"{col_id} must read '{stored_key}', got: {where[:200]}"


@pytest.mark.unit
def test_call_type_filter_excludes_missing_or_unknown_raw_type():
    where, _ = ClickHouseFilterBuilder().translate(
        [
            {
                "column_id": "call_type",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "outbound",
                    "col_type": "SYSTEM_METRIC",
                },
            }
        ]
    )

    assert "multiIf(" in where
    assert "= 'inboundPhoneCall', 'inbound'" in where
    assert "= 'outboundPhoneCall', 'outbound', null)" in where
    assert "'inbound', 'outbound')" not in where


def _system_filter(column_id, filter_type, filter_op, filter_value):
    return {
        "column_id": column_id,
        "filter_config": {
            "filter_type": filter_type,
            "filter_op": filter_op,
            "filter_value": filter_value,
            "col_type": "SYSTEM_METRIC",
        },
    }


def _time_filter():
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [WINDOW_START, WINDOW_END],
        },
    }


def _voice_builder(*filters):
    return VoiceCallListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[_time_filter(), *filters],
        page_size=15,
    )


@pytest.mark.unit
def test_voice_call_status_alias_matches_normalized_list_semantics_only():
    where, _ = VoiceCallFilterBuilder().translate(
        [_system_filter("call_status", "text", "equals", "completed")]
    )

    assert "multiIf(" in where
    assert "'ended', 'completed', 'in-progress'" in where
    assert "IN ('done', 'ended')" in where
    assert "coalesce(" in where

    generic_where, _ = ClickHouseFilterBuilder().translate(
        [_system_filter("call_status", "text", "equals", "ended")]
    )
    assert "span_attr_str['call.status']" in generic_where
    assert "'in-progress'" not in generic_where


@pytest.mark.unit
def test_voice_cost_cents_alias_normalizes_every_supported_provider():
    where, params = VoiceCallFilterBuilder().translate(
        [_system_filter("cost_cents", "number", "equals", 12.2)]
    )

    assert "'call_cost', 'combined_cost'" in where  # Retell: already cents
    assert "'metadata', 'cost'" in where  # ElevenLabs: already cents
    assert "'cost_breakdown.total'" in where  # VAPI dollars -> cents
    assert "'price'" in where  # Bland/Twilio dollars -> cents
    assert "('retell', 'eleven_labs')" in where
    assert "('vapi', 'bland', 'twilio')" in where
    assert 12.2 in params.values()

    generic_where, _ = ClickHouseFilterBuilder().translate(
        [_system_filter("cost_cents", "number", "equals", 12.2)]
    )
    assert "span_attr_num['cost_cents']" in generic_where
    assert "'combined_cost'" not in generic_where


@pytest.mark.unit
def test_voice_normalized_aliases_rewrite_to_ch25_columns():
    where, _ = VoiceCallFilterBuilderV2().translate(
        [
            _system_filter("call_status", "text", "equals", "completed"),
            _system_filter("cost_cents", "number", "greater_than", 1),
        ]
    )

    assert "attrs_string['call.status']" in where
    assert "attrs_number['combined_cost']" in where
    assert "attributes_extra" in where
    assert "span_attr_str" not in where
    assert "span_attr_num" not in where
    assert "span_attributes_raw" not in where


@pytest.mark.unit
@pytest.mark.parametrize(
    ("filter_item", "sql_markers", "forbidden_marker"),
    [
        (
            _system_filter("call_status", "text", "equals", "completed"),
            (
                "'ended', 'completed', 'in-progress'",
                "'done', 'ended'",
                "'retell', 'vapi'",
                "'bland', 'twilio'",
            ),
            "attrs_string['call_status']",
        ),
        (
            _system_filter("cost_cents", "number", "equals", 12.2),
            (
                "'call_cost', 'combined_cost'",
                "'metadata', 'cost'",
                "'cost_breakdown.total'",
                "'vapi', 'bland', 'twilio'",
            ),
            "attrs_number['cost_cents']",
        ),
    ],
)
def test_bounded_voice_seed_uses_normalized_alias_expression(
    filter_item,
    sql_markers,
    forbidden_marker,
):
    query, _ = _voice_builder(filter_item).build_filter_seed_page(
        slice_start=WINDOW_START,
        slice_end=WINDOW_END,
        limit=50,
    )

    for marker in sql_markers:
        assert marker in query
    assert forbidden_marker not in query


@pytest.mark.unit
def test_bounded_voice_match_combines_normalized_status_and_cost():
    query, params = _voice_builder(
        _system_filter("call_status", "text", "equals", "completed"),
        _system_filter("cost_cents", "number", "greater_than", 5),
    ).build_filter_match_query(["trace-a", "trace-b"])

    assert "'ended', 'completed', 'in-progress'" in query
    assert "'call_cost', 'combined_cost'" in query
    assert "'metadata', 'cost'" in query
    assert "'cost_breakdown.total'" in query
    assert "'retell', 'eleven_labs'" in query
    assert "'vapi', 'bland', 'twilio'" in query
    assert "attrs_string['call_status']" not in query
    assert "attrs_number['cost_cents']" not in query
    assert "completed" in params.values()
    assert 5.0 in params.values()


@pytest.mark.unit
def test_generic_bounded_trace_does_not_inherit_voice_normalized_aliases():
    builder = TraceListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _system_filter("call_status", "text", "equals", "completed"),
            _system_filter("cost_cents", "number", "greater_than", 5),
        ],
        page_size=15,
    )

    query, params = builder.build_filter_match_query(["trace-a"])

    assert "'in-progress'" not in query
    assert "'call_cost', 'combined_cost'" not in query
    assert "call.status" in params.values()
    assert "cost_cents" in params.values()
