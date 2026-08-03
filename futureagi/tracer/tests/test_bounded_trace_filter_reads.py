from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from django.test import override_settings

from tracer.selectors.trace_filter_reads import (
    MAX_NUMBERED_PAGE_WORK_ROWS,
    BoundedFilterPage,
    bounded_numbered_page_depth_exceeded,
    numbered_page_depth_exceeded,
    read_bounded_filter_page,
)
from tracer.services.clickhouse.page_dedup import paginate_deduped
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.query_builders.session_list import (
    SessionListQueryBuilder,
)
from tracer.services.clickhouse.query_builders.span_list import SpanListQueryBuilder
from tracer.services.clickhouse.query_builders.trace_list import TraceListQueryBuilder
from tracer.services.clickhouse.query_builders.voice_call_list import (
    VoiceCallListQueryBuilder,
)
from tracer.services.clickhouse.query_service import QueryResult
from tracer.services.clickhouse.read_budget import ReadDeadlineExceeded
from tracer.services.clickhouse.v2.query_builders.span_list import (
    SpanListQueryBuilderV2,
)
from tracer.services.clickhouse.v2.query_builders.trace_list import (
    TraceListQueryBuilderV2,
)

PROJECT_ID = "00000000-0000-4000-8000-000000000001"
START = datetime(2025, 1, 1)
END = START + timedelta(days=365)


def _time_filter(start: datetime = START, end: datetime = END) -> dict[str, Any]:
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [start.isoformat(), end.isoformat()],
        },
    }


def _attribute_filter(
    key: str,
    value: object,
    *,
    filter_type: str = "text",
    operation: str = "equals",
) -> dict[str, Any]:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": filter_type,
            "filter_op": operation,
            "filter_value": value,
        },
    }


def _system_filter(
    key: str,
    value: object,
    *,
    filter_type: str = "text",
    operation: str = "equals",
) -> dict[str, Any]:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": filter_type,
            "filter_op": operation,
            "filter_value": value,
        },
    }


def _annotation_filter(
    label_id: str,
    value: object,
    *,
    filter_type: str = "text",
    operation: str = "equals",
) -> dict[str, Any]:
    return {
        "column_id": label_id,
        "filter_config": {
            "col_type": "ANNOTATION",
            "filter_type": filter_type,
            "filter_op": operation,
            "filter_value": value,
        },
    }


def _eval_filter(
    eval_id: str,
    value: object,
    *,
    filter_type: str = "number",
    operation: str = "greater_than",
) -> dict[str, Any]:
    return {
        "column_id": eval_id,
        "filter_config": {
            "col_type": "EVAL_METRIC",
            "filter_type": filter_type,
            "filter_op": operation,
            "filter_value": value,
        },
    }


@override_settings(
    CLICKHOUSE={
        "CH_HOST": "legacy.invalid",
        "CH_PORT": 9000,
        "CH_USERNAME": "legacy-user",
        "CH_PASSWORD": "legacy-password",
        "CH_DATABASE": "legacy-db",
    },
    CLICKHOUSE_V2={
        "CH25_HOST": "direct-write.invalid",
        "CH25_TCP_PORT": 9440,
        "CH25_USER": "direct-write-user",
        "CH25_PASSWORD": "",
        "CH25_DATABASE": "direct-write-db",
        "QUERY_TYPES_V2_ONLY": "TRACE_LIST",
    },
)
def test_dispatched_v2_query_service_uses_split_host_without_legacy_singleton() -> None:
    from tracer.services.clickhouse.query_service import AnalyticsQueryService
    from tracer.services.clickhouse.v2.dispatch import get_query_builder_class
    from tracer.services.clickhouse.v2.query_service import (
        V2AnalyticsQueryService,
        query_service_for_builder,
        reset_v2_query_client,
    )

    reset_v2_query_client()
    try:
        with mock.patch(
            "tracer.services.clickhouse.query_service.get_clickhouse_client"
        ) as legacy_client:
            builder_class = get_query_builder_class("TRACE_LIST")
            fallback = object.__new__(AnalyticsQueryService)
            service = query_service_for_builder("TRACE_LIST", builder_class, fallback)

        assert isinstance(service, V2AnalyticsQueryService)
        assert service.ch_client.host == "direct-write.invalid"
        assert service.ch_client.port == 9440
        assert service.ch_client.user == "direct-write-user"
        assert service.ch_client.password == ""
        assert service.ch_client.database == "direct-write-db"
        assert V2AnalyticsQueryService().ch_client is service.ch_client
        legacy_client.assert_not_called()
    finally:
        reset_v2_query_client()


def test_customer_final_status_trace_query_uses_indexed_any_span_anchor() -> None:
    filters = [
        _time_filter(),
        _attribute_filter("final_status", ["Rejected"], operation="in"),
    ]
    builder = TraceListQueryBuilder(project_id=PROJECT_ID, filters=filters)

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, match_params = builder.build_filter_match_query(["trace-a"])

    assert "start_time >= %(filter_slice_start)s" in seed_sql
    assert "start_time < %(filter_slice_end)s" in seed_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in seed_sql
    assert "arrayMap(x -> lower(x), mapValues(span_attr_str))" in seed_sql
    assert seed_params["latest_filter_key_0"] == "final_status"
    assert "parent_span_id IS NULL" not in seed_sql
    assert "id AS matched_span_id" in seed_sql
    assert " FINAL" not in seed_sql
    assert seed_params["filter_seed_limit"] == 100
    assert match_params["candidate_trace_ids"] == ("trace-a",)
    assert "argMax(mapContains(span_attr_str, %(latest_filter_key_0)s)" in match_sql
    assert match_params["latest_filter_key_0"] == "final_status"
    assert "argMax(is_deleted, _peerdb_version)" in match_sql
    assert "argMaxIf(tuple(grouped_id)" in match_sql
    assert "GROUP BY trace_id, id, start_time" in match_sql
    assert "SELECT id\n" not in match_sql
    assert "parent_span_id IS NULL" in match_sql
    assert "grouped_trace_id IN %(candidate_trace_ids)s" in match_sql
    assert "latest_attr_exists_0" in match_sql
    assert match_sql.count("FROM spans") == 1
    assert "SELECT latest_trace_id" not in match_sql
    assert "AND trace_id IN %(candidate_trace_ids)s" in match_sql
    assert "%(candidate_start_date)s - INTERVAL 1 DAY" not in match_sql
    assert "%(candidate_end_date)s + INTERVAL 1 DAY" not in match_sql
    assert builder.filter_seed_proves_result_order() is False
    assert builder.recommended_filter_seed_batch_size() == 512
    assert builder.recommended_filter_classify_batch_size() == 512


def test_call_type_trace_filter_skips_unindexed_window_anchor() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _system_filter("call_type", "inbound")],
    )

    ordered_sql, _ = builder.build_filter_ordered_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=50,
    )
    match_sql, _ = builder.build_filter_match_query(["trace-a"])

    assert builder.supports_filter_anchor_probe() is False
    with pytest.raises(ValueError, match="indexed any-span filter"):
        builder.build_filter_anchor_probe(limit=513)
    assert "JSONExtract" not in ordered_sql
    assert "parent_span_id IS NULL" in ordered_sql
    assert "JSONExtract" in match_sql
    assert builder.recommended_filter_seed_batch_size() == 50
    assert builder.recommended_filter_classify_batch_size() == 50


def test_map_plus_json_anchor_uses_only_indexed_map_leaf() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
            _system_filter("call_type", "inbound"),
        ],
    )

    anchor_sql, anchor_params = builder.build_filter_anchor_probe(limit=513)

    assert builder.supports_filter_anchor_probe() is True
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in anchor_sql
    assert "JSONExtract" not in anchor_sql
    assert anchor_params["latest_filter_key_0"] == "final_status"
    assert "latest_filter_param_1" not in anchor_params
    assert builder.recommended_filter_seed_batch_size() == 50
    assert builder.recommended_filter_classify_batch_size() == 50


def test_trace_candidate_classifier_prunes_to_request_partitions() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )

    sql, params = builder.build_filter_match_query(["trace-a"])

    prewhere = sql.split("GROUP BY trace_id, id, start_time", 1)[0]
    assert "toDate(start_time) >= toDate(%(candidate_start_date)s)" in prewhere
    assert "toDate(start_time) <= toDate(%(candidate_end_date)s)" in prewhere
    assert "start_time >= %(candidate_start_date)s" in prewhere
    assert "start_time < %(candidate_end_date)s" in prewhere
    assert params["candidate_start_date"] == START
    assert params["candidate_end_date"] == END


def test_root_seed_replay_does_not_trust_one_raw_physical_root_id() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )

    sql, params = builder.build_filter_match_query_from_seed_rows(
        [
            {
                "trace_id": "trace-a",
                "root_span_id": "root-a",
                "start_time": END - timedelta(minutes=1),
            }
        ]
    )

    assert params["candidate_trace_ids"] == ("trace-a",)
    assert "candidate_root_span_ids" not in params
    assert "id IN %(candidate_root_span_ids)s" not in sql
    assert "trace_id IN %(candidate_trace_ids)s" in sql
    assert "argMaxIf(tuple(grouped_id)" in sql
    assert "SELECT id\n" not in sql
    assert sql.count("FROM spans") == 1


def test_span_match_compiles_typed_map_json_and_multi_filter_at_latest_state() -> None:
    filters = [
        _time_filter(),
        _system_filter("status", ["SUCCESS"], operation="in"),
        _attribute_filter("customer.tier", "enterprise"),
        _attribute_filter(
            "quality", 0.8, filter_type="number", operation="greater_than"
        ),
        _attribute_filter("reviewed", True, filter_type="boolean"),
        _system_filter("call_type", "inbound"),
    ]
    builder = SpanListQueryBuilder(project_id=PROJECT_ID, filters=filters)

    sql, params = builder.build_filter_match_query(["span-a", "span-b"])

    assert params["candidate_span_ids"] == ("span-a", "span-b")
    assert "argMax(tuple(status), _peerdb_version).1" in sql
    assert "argMax(mapContains(span_attr_str, %(latest_filter_key_1)s)" in sql
    assert "argMax(mapContains(span_attr_num, %(latest_filter_key_2)s)" in sql
    assert "argMax(mapContains(span_attr_bool, %(latest_filter_key_3)s)" in sql
    assert params["latest_filter_key_1"] == "customer.tier"
    assert params["latest_filter_key_2"] == "quality"
    assert params["latest_filter_key_3"] == "reviewed"
    assert "JSONExtractString(span_attributes_raw, 'raw_log', 'type')" in sql
    assert "argMax(is_deleted, _peerdb_version)" in sql
    assert "latest_column_value_0" in sql
    assert "latest_attr_exists_1" in sql
    assert "latest_attr_exists_2" in sql
    assert "latest_attr_exists_3" in sql
    assert "latest_json_value_4" in sql
    assert "GROUP BY project_id, trace_id, id, start_time" in sql


def test_span_seed_replay_uses_trace_scoped_otel_identity() -> None:
    project_version_id = "00000000-0000-4000-8000-000000000099"
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        project_version_id=project_version_id,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )
    assert builder.supports_bounded_filter_scan() is True

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, params = builder.build_filter_match_query_from_seed_rows(
        [
            {
                "project_id": PROJECT_ID,
                "id": "shared-span-id",
                "trace_id": "trace-a",
                "start_time": END - timedelta(minutes=1),
            },
            {
                "project_id": PROJECT_ID,
                "id": "shared-span-id",
                "trace_id": "trace-b",
                "start_time": END - timedelta(minutes=2),
            },
        ]
    )

    assert "SELECT project_id, id, trace_id, start_time" in seed_sql
    assert "project_version_id = %(project_version_id)s" in seed_sql
    assert seed_params["project_version_id"] == project_version_id
    assert "LIMIT 1 BY project_id, trace_id, id, start_time" in seed_sql
    assert (
        "ORDER BY start_time DESC, id DESC, trace_id DESC, project_id DESC" in seed_sql
    )
    assert params["candidate_span_ids"] == ("shared-span-id",)
    assert params["candidate_span_trace_ids"] == ("trace-a", "trace-b")
    first_start = END - timedelta(minutes=1)
    second_start = END - timedelta(minutes=2)
    assert params["candidate_span_identities"] == (
        (
            PROJECT_ID,
            "trace-a",
            "shared-span-id",
            int(first_start.replace(tzinfo=UTC).timestamp() * 1_000_000),
        ),
        (
            PROJECT_ID,
            "trace-b",
            "shared-span-id",
            int(second_start.replace(tzinfo=UTC).timestamp() * 1_000_000),
        ),
    )
    assert params["candidate_span_dates"] == (first_start.date(),)
    assert "toUnixTimestamp64Micro(start_time)" in match_sql
    assert "IN %(candidate_span_identities)s" in match_sql
    assert "project_version_id = %(project_version_id)s" in match_sql
    assert params["project_version_id"] == project_version_id
    assert "GROUP BY project_id, trace_id, id, start_time" in match_sql


def test_v2_span_seed_uses_deployed_string_value_bloom_companion() -> None:
    filters = [
        _time_filter(),
        _attribute_filter("final_status", ["Rejected"], operation="in"),
    ]
    builder = SpanListQueryBuilderV2(project_id=PROJECT_ID, filters=filters)

    sql, params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )

    assert (
        "hasAny(arrayMap(x -> lower(x), mapValues(attrs_string)), "
        "[%(latest_filter_index_0_0)s])" in sql
    )
    assert params["latest_filter_index_0_0"] == "rejected"

    prompt_builder = SpanListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("prompt_slug", "agent_2_identity_disclosure"),
        ],
    )
    prompt_sql, prompt_params = prompt_builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    assert (
        "has(arrayMap(x -> lower(x), mapValues(attrs_string)), "
        "%(latest_filter_param_0)s)" in prompt_sql
    )
    assert prompt_params["latest_filter_param_0"] == "agent_2_identity_disclosure"


@pytest.mark.parametrize(
    "filter_type,value", [("text", "x"), ("number", 1.5), ("boolean", True)]
)
def test_native_map_attribute_types_remain_bounded(
    filter_type: str, value: object
) -> None:
    filters = [
        _time_filter(),
        _attribute_filter("typed_key", value, filter_type=filter_type),
    ]
    builder = SpanListQueryBuilderV2(project_id=PROJECT_ID, filters=filters)

    assert builder.supports_bounded_filter_scan() is True
    assert builder.bounded_filter_degraded_error_code() is None


@pytest.mark.parametrize(
    "filters",
    [
        [
            _time_filter(),
            _attribute_filter(
                "overflow_payload", {"nested": [1, 2]}, filter_type="json"
            ),
        ],
        [
            _time_filter(),
            _attribute_filter("typed_key", "x"),
            _attribute_filter(
                "overflow_payload", {"nested": [1, 2]}, filter_type="json"
            ),
        ],
    ],
)
def test_attributes_extra_json_filter_fails_closed_with_explicit_degradation(
    filters: list[dict[str, Any]],
) -> None:
    builder = SpanListQueryBuilderV2(project_id=PROJECT_ID, filters=filters)

    assert builder.supports_bounded_filter_scan() is False
    assert builder.bounded_filter_degraded_error_code() == "unsupported_filter_shape"
    with pytest.raises(ValueError, match="unsupported_filter_shape"):
        builder.build()


def test_text_filter_treats_sql_wildcards_as_literal_user_text() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter(
                "customer.note",
                r"50%_off\\today",
                operation="contains",
            ),
        ],
    )

    sql, params = builder.build_filter_match_query(["span-a"])

    assert "positionUTF8(" in sql
    assert " LIKE " not in sql
    assert r"50%_off\\today" in params.values()


def test_trace_attribute_can_match_only_a_child_span() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("customer.final_status", ["Rejected"], operation="in"),
        ],
    )

    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, _ = builder.build_filter_match_query(["trace-with-child-value"])

    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in seed_sql
    assert "parent_span_id IS NULL" not in seed_sql
    assert "HAVING countIf(" in match_sql
    assert "GROUP BY trace_id, id, start_time" in match_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in match_sql


@pytest.mark.parametrize("key", ["final_status", "country"])
def test_covered_rollup_names_retain_public_any_span_semantics(key: str) -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter(key, "value")],
    )

    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, _ = builder.build_filter_match_query(["trace-a"])

    assert "parent_span_id IS NULL" not in seed_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in seed_sql
    assert "HAVING countIf(" in match_sql
    assert builder.filter_seed_proves_result_order() is False


def test_trace_mixed_root_and_any_span_filters_keep_distinct_scopes() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _system_filter("trace_name", "Café"),
            _attribute_filter("customer.final_status", "Rejected"),
        ],
    )

    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, params = builder.build_filter_match_query(["trace-a"])

    assert "lowerUTF8(toString(trace_name))" not in seed_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_1)s)" in seed_sql
    assert "argMax(trace_name, _peerdb_version)" in match_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_1)s)" in match_sql
    assert params["latest_filter_param_0"] == "café"
    assert params["latest_filter_param_1"] == "rejected"


def test_mixed_attribute_and_annotation_stays_in_one_bounded_trace_classifier() -> None:
    label_id = "00000000-0000-4000-8000-000000000099"
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
            _annotation_filter(label_id, "approved"),
        ],
    )

    assert builder.supports_bounded_filter_scan() is True
    assert builder.bounded_filter_degraded_error_code() is None
    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, params = builder.build_filter_match_query(["trace-a"])

    assert "model_hub_score" not in seed_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in seed_sql
    assert "model_hub_score AS s FINAL" in match_sql
    assert "latest_attr_exists_0" in match_sql
    assert "%(candidate_trace_ids)s" in match_sql
    assert "toString(if(" in match_sql
    assert "toString(s.observation_span_id) IN (" in match_sql
    assert "toString(trace_id) IN %(candidate_trace_ids)s" in match_sql
    assert params["candidate_trace_ids"] == ("trace-a",)
    assert params["ann_label_1"] == label_id


def test_eval_residual_is_candidate_scoped_inside_same_trace_match_query() -> None:
    eval_id = "00000000-0000-4000-8000-000000000088"
    template_id = "00000000-0000-4000-8000-000000000087"

    class _Values(list):
        def first(self):
            return self[0] if self else None

    class _ConfigQuery:
        def filter(self, **_kwargs):
            return self

        def exists(self):
            return True

        def values_list(self, field, **_kwargs):
            return _Values([template_id if field == "eval_template_id" else eval_id])

    class _TemplateQuery:
        def values(self, *_args):
            return self

        def first(self):
            return {"config": {"output": "SCORE"}}

    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _system_filter("status", "SUCCESS"),
            _eval_filter(eval_id, 75),
        ],
    )

    with (
        mock.patch(
            "tracer.models.custom_eval_config.CustomEvalConfig.objects.filter",
            return_value=_ConfigQuery(),
        ),
        mock.patch(
            "model_hub.models.evals_metric.EvalTemplate.no_workspace_objects.filter",
            return_value=_TemplateQuery(),
        ),
    ):
        sql, params = builder.build_filter_match_query(["trace-a", "trace-b"])

    assert builder.supports_bounded_filter_scan() is True
    assert "argMax(tuple(status), _peerdb_version).1" in sql
    assert "custom_eval_config_id IN" in sql
    assert "toString(eval_scan.trace_id) IN %(candidate_trace_ids)s" in sql
    assert params["candidate_trace_ids"] == ("trace-a", "trace-b")
    assert params["eval_cfg_1"] == (eval_id,)


def test_span_annotation_classifier_scopes_score_and_span_sides_to_candidates() -> None:
    label_id = "00000000-0000-4000-8000-000000000077"
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _annotation_filter(label_id, 3, filter_type="number")],
    )

    sql, params = builder.build_filter_match_query_from_seed_rows(
        [
            {
                "project_id": PROJECT_ID,
                "trace_id": "trace-a",
                "id": "span-a",
                "start_time": END - timedelta(minutes=1),
            },
            {
                "project_id": PROJECT_ID,
                "trace_id": "trace-b",
                "id": "span-b",
                "start_time": END - timedelta(minutes=2),
            },
        ]
    )

    assert builder.supports_bounded_filter_scan() is True
    assert "toString(if(" in sql
    assert (
        "(toString(s.trace_id), toString(if(" in sql
        and "IN %(candidate_span_entities)s" in sql
    )
    assert "(toString(trace_id), toString(id)) IN %(candidate_span_entities)s" in sql
    assert params["candidate_span_ids"] == ("span-a", "span-b")
    assert params["candidate_span_entities"] == (
        ("trace-a", "span-a"),
        ("trace-b", "span-b"),
    )
    assert params["ann_label_1"] == label_id


def test_has_eval_span_residual_matches_candidate_span_not_its_whole_trace() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            {
                "column_id": "has_eval",
                "filter_config": {
                    "filter_type": "boolean",
                    "filter_op": "equals",
                    "filter_value": True,
                },
            },
        ],
    )

    sql, params = builder.build_filter_match_query_from_seed_rows(
        [
            {
                "project_id": PROJECT_ID,
                "trace_id": "trace-a",
                "id": "span-a",
                "start_time": END - timedelta(minutes=1),
            }
        ]
    )

    assert "tuple(trace_id, id) IN (" in sql
    assert (
        "SELECT DISTINCT tuple(toString(latest_eval.trace_id), "
        "toString(latest_eval.observation_span_id))" in sql
    )
    assert "sp.trace_id = toString(latest_eval.trace_id)" in sql
    assert "sp.id = toString(latest_eval.observation_span_id)" in sql
    assert (
        "(toString(eval_scan.trace_id), "
        "toString(eval_scan.observation_span_id)) "
        "IN %(candidate_span_entities)s" in sql
    )
    assert "LIMIT 1 BY eval_scan.id" in sql
    assert params["candidate_span_ids"] == ("span-a",)
    assert params["candidate_span_entities"] == (("trace-a", "span-a"),)


def test_legacy_system_aliases_keep_latest_state_without_broad_fallback() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _system_filter(
                "gen_ai.usage.total_tokens",
                500,
                filter_type="number",
                operation="greater_than",
            ),
            _system_filter("legacy.customer.level", "gold"),
        ],
    )

    seed_sql, _ = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, params = builder.build_filter_match_query(["trace-a"])

    assert "total_tokens" not in seed_sql
    assert "mapContains(span_attr_str, %(latest_filter_key_1)s)" in seed_sql
    assert "argMax(tuple(total_tokens), _peerdb_version).1" in match_sql
    assert "argMax(mapContains(span_attr_str" in match_sql
    assert params["latest_filter_key_1"] == "legacy.customer.level"


def test_trace_any_span_root_seed_and_single_latest_state_scan() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("customer.final_status", "Rejected"),
            _attribute_filter("customer.country", "ES"),
        ],
        bounded_internal_scan=True,
        bounded_identity_only=True,
        bounded_sampling_salt="task-salt",
        bounded_sampling_rate=25.0,
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, match_params = builder.build_filter_match_query(["trace-a"])

    first_filter = "mapContains(span_attr_str, %(latest_filter_key_0)s)"
    second_filter = "mapContains(span_attr_str, %(latest_filter_key_1)s)"
    assert first_filter in seed_sql and second_filter not in seed_sql
    assert "parent_span_id IS NULL" not in seed_sql
    assert seed_sql.index("cityHash64") < seed_sql.index("LIMIT %(filter_seed_limit)s")
    assert "toString(trace_id)" in seed_sql
    assert seed_params["latest_filter_key_0"] == "customer.final_status"
    assert "latest_filter_key_1" not in seed_params
    assert seed_params["bounded_sampling_salt"] == "task-salt"
    assert seed_params["bounded_sampling_rate"] == 25.0
    # Both leaves and canonical-root selection share one latest-state scan,
    # while independent countIf leaves allow separate children to satisfy them.
    assert match_sql.count("FROM spans") == 1
    assert match_sql.count("GROUP BY grouped_trace_id") == 1
    assert match_sql.count("countIf(") == 3
    # Canonical root + both independent any-span leaves are constrained to
    # the same half-open request window after latest-version collapse. The
    # root gate is used by argMaxIf and HAVING. Each any-span leaf uses the
    # gate once to project its physical witness and once in its countIf HAVING.
    any_span_leaf_count = 2
    expected_window_gate_uses = 2 + (2 * any_span_leaf_count)
    assert match_sql.count("argMinIf(") == any_span_leaf_count
    assert (
        match_sql.count("latest_start_time >= %(candidate_start_date)s")
        == expected_window_gate_uses
    )
    assert (
        match_sql.count("latest_start_time < %(candidate_end_date)s")
        == expected_window_gate_uses
    )
    assert first_filter in match_sql and second_filter in match_sql
    assert "AND trace_id IN %(candidate_trace_ids)s" in match_sql
    assert "%(candidate_start_date)s - INTERVAL 1 DAY" not in match_sql
    assert "%(candidate_end_date)s + INTERVAL 1 DAY" not in match_sql
    assert "SELECT id\n" not in match_sql
    assert match_params["candidate_trace_ids"] == ("trace-a",)
    assert builder.filter_seed_proves_result_order() is False
    assert builder.recommended_filter_classify_batch_size() == 512


def test_trace_candidate_classifier_enforces_production_proven_512_trace_cap() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )

    with pytest.raises(ValueError, match="candidate trace batch"):
        builder.build_filter_match_query([f"trace-{index:03d}" for index in range(513)])


def test_span_candidate_classifier_enforces_200_identity_hard_cap() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )

    with pytest.raises(ValueError, match="candidate .* batch exceeds bounded limit"):
        builder.build_filter_match_query([f"identity-{index}" for index in range(201)])


def test_unicode_text_equality_and_membership_use_utf8_case_folding() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("customer.tier", "ÉLITE"),
            _system_filter("status", ["ÉXITO"], operation="in"),
        ],
    )

    sql, params = builder.build_filter_match_query(["span-a"])

    assert "lowerUTF8(toString(latest_attr_value_0))" in sql
    assert "lowerUTF8(toString(latest_column_value_1))" in sql
    assert params["latest_filter_param_0"] == "élite"
    assert params["latest_filter_param_1"] == ("éxito",)


def test_attribute_key_is_bound_and_preserved_for_all_map_expressions() -> None:
    key = "café final status '50%_\\path"
    value = "Rejected%_\\literal"
    builder = SpanListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter(key, value)],
    )

    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    match_sql, match_params = builder.build_filter_match_query(["span-a"])

    for sql in (seed_sql, match_sql):
        assert key not in sql
        assert "%(latest_filter_key_0)s" in sql
    assert "mapContains(attrs_string, %(latest_filter_key_0)s)" in seed_sql
    assert "attrs_string[%(latest_filter_key_0)s]" in seed_sql
    assert "argMax(mapContains(attrs_string, %(latest_filter_key_0)s)" in match_sql
    assert "argMax(attrs_string[%(latest_filter_key_0)s], _version)" in match_sql
    assert seed_params["latest_filter_key_0"] == key
    assert match_params["latest_filter_key_0"] == key
    assert match_params["latest_filter_param_0"] == value.lower()


@pytest.mark.parametrize(
    "key",
    ["bad\x00key", "bad\nkey", "x" * 4097, "bad\ud800key"],
)
def test_attribute_key_control_invalid_utf8_and_length_fail_closed(key: str) -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter(key, "value")],
    )

    assert builder.supports_bounded_filter_scan() is False


def test_negative_text_operators_are_literal_utf8_predicates() -> None:
    builder = SpanListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter(
                "customer.note",
                "Café%_\\path",
                operation="not_contains",
            ),
            _system_filter("status", ["ÉCHEC"], operation="not_in"),
        ],
    )

    sql, params = builder.build_filter_match_query(["span-a"])

    assert "positionUTF8(" in sql
    assert ") = 0" in sql
    assert "lowerUTF8(toString(latest_column_value_1)) NOT IN" in sql
    assert " LIKE " not in sql
    assert params["latest_filter_param_0"] == "Café%_\\path"
    assert params["latest_filter_param_1"] == ("échec",)


@pytest.mark.parametrize(
    "bad_filter",
    [
        _attribute_filter("customer.tier", [], operation="in"),
        _attribute_filter("customer.tier", "", operation="contains"),
        _attribute_filter("reviewed", "true", filter_type="boolean"),
        _attribute_filter("quality", "not-a-number", filter_type="number"),
    ],
)
def test_empty_or_malformed_filter_values_emit_no_bounded_query(
    bad_filter: dict[str, Any],
) -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), bad_filter],
    )

    assert builder.supports_bounded_filter_scan() is False
    with pytest.raises(ValueError, match="unsupported bounded span filter scan"):
        builder.build_filter_match_query(["span-a"])


def test_call_type_json_sources_and_unknown_values_fail_closed() -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _system_filter("call_type", "inbound")],
    )

    sql, _ = builder.build_filter_match_query(["span-a"])

    assert "JSONExtractString(span_attributes_raw, 'raw_log', 'type')" in sql
    assert (
        "JSONExtractString(JSONExtractString(span_attributes_raw, 'raw_log'), "
        "'type')" in sql
    )
    assert "JSONExtractString(span_attr_str['raw_log'], 'type')" in sql
    assert "= 'inboundPhoneCall', 'inbound'" in sql
    assert "= 'outboundPhoneCall', 'outbound'" in sql
    assert "'outbound', null)" in sql


def test_v2_bounded_builders_emit_only_ch25_columns() -> None:
    trace_builder = TraceListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
        ],
    )
    trace_seed_sql, _ = trace_builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=100,
    )
    trace_match_sql, _ = trace_builder.build_filter_match_query(["trace-a"])

    span_builder = SpanListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("customer.tier", "Élite"),
            _attribute_filter("quality", 0.8, filter_type="number"),
            _attribute_filter("reviewed", True, filter_type="boolean"),
            _system_filter("call_type", "inbound"),
        ],
    )
    span_match_sql, _ = span_builder.build_filter_match_query(["span-a"])

    assert "start_time >= %(filter_slice_start)s" in trace_seed_sql
    assert "attrs_string" in trace_match_sql
    assert "attrs_string" in span_match_sql
    assert "attrs_number" in span_match_sql
    assert "attrs_bool" in span_match_sql
    assert "JSONExtractString(attributes_extra, 'raw_log', 'type')" in span_match_sql
    assert "_version" in trace_match_sql
    assert "_version" in span_match_sql
    assert "is_deleted" in trace_seed_sql
    for sql in (trace_seed_sql, trace_match_sql, span_match_sql):
        assert "_peerdb_" not in sql
        assert "span_attr_str" not in sql
        assert "span_attr_num" not in sql
        assert "span_attr_bool" not in sql
        assert "span_attributes_raw" not in sql


def test_trace_custom_sort_never_falls_back_to_legacy_query() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
        sort_params=[{"column_id": "latency", "order": "desc"}],
    )

    assert builder.supports_bounded_filter_scan() is False
    assert (
        builder.bounded_filter_degraded_error_code() == "unsupported_filter_modifiers"
    )
    with pytest.raises(ValueError, match="unsafe legacy filtered trace read blocked"):
        builder.build()


def test_trace_search_is_a_literal_latest_root_predicate_in_bounded_reads() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter()],
        search="100%_D",
    )

    assert builder.supports_bounded_filter_scan() is True
    assert builder.bounded_filter_degraded_error_code() is None
    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=50,
    )
    match_sql, match_params = builder.build_filter_match_query(["trace-a"])

    literal_seed = (
        "positionUTF8(lowerUTF8(toString(trace_name)), "
        "lowerUTF8(toString(%(latest_filter_param_0)s))) > 0"
    )
    literal_latest = (
        "positionUTF8(lowerUTF8(toString(latest_column_value_0)), "
        "lowerUTF8(toString(%(latest_filter_param_0)s))) > 0"
    )
    assert literal_seed in seed_sql
    assert literal_latest in match_sql
    assert "argMax(trace_name, _peerdb_version) AS latest_column_value_0" in match_sql
    assert seed_params["latest_filter_param_0"] == "100%_D"
    assert match_params["latest_filter_param_0"] == "100%_D"
    assert "ILIKE" not in seed_sql + match_sql
    assert "100%_D" not in seed_sql + match_sql

    with pytest.raises(ValueError, match="bounded_search_required"):
        builder.build()
    with pytest.raises(ValueError, match="bounded_search_required"):
        builder.build_count_query()


def test_trace_search_and_any_span_filter_share_bounded_classifier() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
        ],
        search="SyntheticAgent",
    )

    anchor_sql, anchor_params = builder.build_filter_anchor_probe(limit=513)
    ordered_sql, ordered_params = builder.build_filter_ordered_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=50,
    )
    match_sql, match_params = builder.build_filter_match_query(["trace-a"])

    # The sparse/common anchor stays on the indexed child attribute. Search is
    # a root predicate, so it belongs in the ordered-root seed and classifier.
    assert "mapContains(span_attr_str, %(latest_filter_key_0)s)" in anchor_sql
    assert "latest_filter_param_1" not in anchor_params
    assert "positionUTF8(lowerUTF8(toString(trace_name))" in ordered_sql
    assert ordered_params["latest_filter_param_1"] == "SyntheticAgent"
    assert "latest_attr_value_0" in match_sql
    assert "latest_column_value_1" in match_sql
    assert match_params["latest_filter_param_0"] == "rejected"
    assert match_params["latest_filter_param_1"] == "SyntheticAgent"
    assert builder.filter_seed_proves_result_order() is False


def test_trace_search_with_custom_sort_remains_fail_closed() -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter()],
        search="needle",
        sort_params=[{"column_id": "latency", "order": "desc"}],
    )

    assert builder.supports_bounded_filter_scan() is False
    assert (
        builder.bounded_filter_degraded_error_code() == "unsupported_filter_modifiers"
    )


@pytest.mark.parametrize("filters", [[], [_time_filter()]])
def test_trace_empty_or_time_only_custom_sort_never_uses_bounded_order(
    filters: list[dict[str, Any]],
) -> None:
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        sort_params=[{"column_id": "latency", "order": "desc"}],
    )

    assert builder.supports_bounded_filter_scan() is False
    assert (
        builder.bounded_filter_degraded_error_code() == "unsupported_filter_modifiers"
    )
    with pytest.raises(ValueError, match="unsupported bounded trace filter scan"):
        builder.build_filter_seed_page(
            slice_start=END - timedelta(minutes=5),
            slice_end=END,
            limit=50,
        )


def test_trace_project_version_filter_is_scoped_in_bounded_seed_and_replay() -> None:
    project_version_id = "00000000-0000-4000-8000-000000000002"
    builder = TraceListQueryBuilder(
        project_id=PROJECT_ID,
        project_version_id=project_version_id,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
    )

    assert builder.supports_bounded_filter_scan() is True
    seed_sql, seed_params = builder.build_filter_seed_page(
        slice_start=END - timedelta(minutes=5),
        slice_end=END,
        limit=50,
    )
    match_sql, match_params = builder.build_filter_match_query(["trace-a"])

    assert "project_version_id = %(project_version_id)s" in seed_sql
    assert "project_version_id = %(project_version_id)s" in match_sql
    assert seed_params["project_version_id"] == project_version_id
    assert match_params["project_version_id"] == project_version_id


@pytest.mark.parametrize(
    "modifier",
    [
        {"sort_params": [{"column_id": "latency", "order": "desc"}]},
        {"end_user_id": "00000000-0000-4000-8000-000000000002"},
    ],
)
def test_span_supported_filter_modifiers_never_fall_back_to_legacy_query(
    modifier: dict[str, Any],
) -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter(), _attribute_filter("final_status", "Rejected")],
        **modifier,
    )

    assert builder.supports_bounded_filter_scan() is False
    assert (
        builder.bounded_filter_degraded_error_code() == "unsupported_filter_modifiers"
    )
    with pytest.raises(ValueError, match="unsafe legacy filtered span read blocked"):
        builder.build()


@pytest.mark.parametrize("filters", [[], [_time_filter()]])
def test_span_empty_or_time_only_custom_sort_uses_project_time_bounded_top_n(
    filters: list[dict[str, Any]],
) -> None:
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        sort_params=[{"column_id": "latency", "order": "desc"}],
    )

    assert builder.supports_bounded_filter_scan() is False
    assert builder.bounded_filter_degraded_error_code() is None
    sql, _ = builder.build()
    assert "ORDER BY latency_ms DESC" in sql
    with pytest.raises(ValueError, match="unsupported bounded span filter scan"):
        builder.build_filter_seed_page(
            slice_start=END - timedelta(minutes=5),
            slice_end=END,
            limit=50,
        )


def test_span_unfiltered_end_user_uses_remap_aware_legacy_path() -> None:
    end_user_id = "00000000-0000-4000-8000-000000000002"
    builder = SpanListQueryBuilder(
        project_id=PROJECT_ID,
        filters=[_time_filter()],
        end_user_id=end_user_id,
    )

    assert builder.supports_bounded_filter_scan() is False
    assert builder.bounded_filter_degraded_error_code() is None
    list_sql, list_params = builder.build()
    count_sql, count_params = builder.build_count_query()
    for sql in (list_sql, count_sql):
        assert "end_user_id_remap" in sql
        assert "resolved_end_user_id = %(end_user_id)s" in sql
    assert list_params["end_user_id"] == end_user_id
    assert count_params["end_user_id"] == end_user_id


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "TRACE_LIST"},
)
def test_trace_list_view_selects_bounded_path_under_v2_only() -> None:
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    bounded = BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=0,
        elapsed_ms=2.0,
        query_count=1,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        status, payload = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert status == "ok"
    assert isinstance(bounded_read.call_args.kwargs["builder"], TraceListQueryBuilderV2)
    assert payload["metadata"]["query_complete"] is True
    assert 0 <= payload["metadata"]["query_elapsed_ms"] < 3_000
    assert payload["metadata"]["query_count"] == 1
    assert payload["metadata"]["query_rows_returned"] == 0
    assert payload["metadata"]["query_result_payload_bytes"] == 0
    assert payload["metadata"]["total_rows_is_lower_bound"] is True
    analytics.execute_ch_query.assert_not_called()


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "TRACE_LIST"},
)
def test_trace_list_nonempty_page_enrichments_share_wall_budget() -> None:
    from tracer.views.trace import TRACE_LIST_READ_SETTINGS, TraceView

    started = END - timedelta(minutes=1)
    row = {
        "project_id": PROJECT_ID,
        "trace_id": "trace-a",
        "root_span_id": "root-a",
        "trace_name": "trace-a",
        "span_name": "root-a",
        "observation_type": "llm",
        "status": "OK",
        "start_time": started,
        "latency_ms": 12.0,
        "cost": 0.001,
    }
    bounded = BoundedFilterPage(
        rows=[row],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=1,
        elapsed_ms=2.0,
        query_count=1,
        rows_returned=1,
        result_payload_bytes=10,
        attempts=(),
    )

    class RecordingAnalytics:
        def __init__(self):
            self.calls = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((params, timeout_ms, settings))
            if "content_trace_ids" in params:
                data = [
                    {
                        "trace_id": "trace-a",
                        "input": "in",
                        "output": "out",
                        "attrs_string": {},
                        "attrs_number": {},
                        "attrs_bool": {},
                        "attributes_extra": "{}",
                        "metadata": "{}",
                        "trace_tags": [],
                    }
                ]
            else:
                data = []
            return QueryResult(data, len(data), "clickhouse", 0.0)

    analytics = RecordingAnalytics()
    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )

    with (
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        status_name, payload = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert status_name == "ok"
    assert payload["table"][0]["trace_id"] == "trace-a"
    assert payload["metadata"]["query_count"] == 4
    assert 0 <= payload["metadata"]["query_elapsed_ms"] < 3_000
    assert bounded_read.call_args.kwargs["deadline_ms"] <= 2_200
    assert len(analytics.calls) == 3
    assert all(0 < timeout_ms <= 900 for _, timeout_ms, _ in analytics.calls)
    assert all(
        settings == TRACE_LIST_READ_SETTINGS for _, _, settings in analytics.calls
    )


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "TRACE_LIST"},
)
def test_trace_list_enrichment_timeout_is_sanitized_503_not_empty_200() -> None:
    from tracer.views.trace import TraceView

    started = END - timedelta(minutes=1)
    bounded = BoundedFilterPage(
        rows=[
            {
                "project_id": PROJECT_ID,
                "trace_id": "trace-a",
                "root_span_id": "root-a",
                "start_time": started,
            }
        ],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=1,
        elapsed_ms=2.0,
        query_count=1,
        rows_returned=1,
        result_payload_bytes=10,
        attempts=(),
    )

    class TimeoutAnalytics:
        def execute_ch_query(self, *args, **kwargs):
            raise ReadDeadlineExceeded("private ClickHouse host and stack")

    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )

    with (
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ),
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=TimeoutAnalytics(),
            org_project_ids=None,
            org=organization,
        )

    assert response[0] == "error"
    assert response[1][0] == 503
    assert response[2]["code"] == "service_unavailable"
    assert "private ClickHouse" not in str(response)


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "TRACE_LIST"},
)
def test_eval_task_trace_list_project_version_selects_bounded_path() -> None:
    """Prototype/eval trace filtering must not call the blocked broad query."""

    from tracer.views.trace import TRACE_LIST_CANDIDATE_DEADLINE_MS, TraceView

    project_version_id = "00000000-0000-4000-8000-000000000099"
    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(success_response=lambda payload: ("ok", payload))
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view.request = request
    bounded = BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=0,
        elapsed_ms=2.0,
        query_count=2,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.ProjectVersion") as project_version,
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
    ):
        project_version.objects.get.return_value = SimpleNamespace(
            project_id=PROJECT_ID
        )
        eval_config.objects.filter.return_value.select_related.return_value = []
        status, payload = view._list_traces_clickhouse(
            request,
            project_version_id,
            analytics,
            {
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "sort_params": [],
                "page_number": 3,
                "page_size": 25,
            },
        )

    assert status == "ok"
    bounded_kwargs = bounded_read.call_args.kwargs
    assert isinstance(bounded_kwargs["builder"], TraceListQueryBuilderV2)
    assert bounded_kwargs["builder"].project_version_id == project_version_id
    assert bounded_kwargs["page_number"] == 3
    assert bounded_kwargs["page_size"] == 25
    assert payload["metadata"]["query_complete"] is True
    assert payload["metadata"]["query_count"] == 2
    assert payload["metadata"]["total_rows_is_lower_bound"] is True
    assert 0 < bounded_kwargs["deadline_ms"] <= TRACE_LIST_CANDIDATE_DEADLINE_MS
    analytics.get_eval_config_ids_with_data_ch.assert_not_called()
    analytics.execute_ch_query.assert_not_called()


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "TRACE_LIST"},
)
def test_eval_task_project_version_enrichments_share_deadline_and_caps() -> None:
    from tracer.views.trace import (
        TRACE_LIST_ENRICHMENT_TIMEOUT_MS,
        TRACE_LIST_READ_SETTINGS,
        TraceView,
    )

    project_version_id = "00000000-0000-4000-8000-000000000099"
    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view.request = request
    bounded = BoundedFilterPage(
        rows=[
            {
                "project_id": PROJECT_ID,
                "trace_id": "trace-a",
                "root_span_id": "root-a",
                "start_time": END - timedelta(minutes=1),
            }
        ],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=1,
        elapsed_ms=2.0,
        query_count=2,
        rows_returned=1,
        result_payload_bytes=64,
        attempts=(),
    )

    class CapturingAnalytics:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def execute_ch_query(self, query, params, timeout_ms, settings=None):
            self.calls.append(
                {
                    "query": query,
                    "timeout_ms": timeout_ms,
                    "settings": settings,
                }
            )
            if " AS user_id" in query:
                rows = [{"trace_id": "trace-a", "user_id": "user-a"}]
            else:
                rows = [
                    {
                        "trace_id": "trace-a",
                        "input": "input-a",
                        "output": "output-a",
                        "trace_tags": [],
                        "attrs_string": {},
                        "attrs_number": {},
                        "attrs_bool": {},
                        "attributes_extra": {},
                    }
                ]
            return QueryResult(
                data=rows,
                row_count=len(rows),
                backend_used="clickhouse",
                query_time_ms=1.0,
            )

    analytics = CapturingAnalytics()
    with (
        mock.patch("tracer.views.trace.ProjectVersion") as project_version,
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ),
    ):
        project_version.objects.get.return_value = SimpleNamespace(
            project_id=PROJECT_ID
        )
        eval_config.objects.filter.return_value.select_related.return_value = []
        status_name, payload = view._list_traces_clickhouse(
            request,
            project_version_id,
            analytics,
            {
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "sort_params": [],
                "page_number": 0,
                "page_size": 25,
            },
        )

    assert status_name == "ok"
    assert len(analytics.calls) == 2
    assert all(
        0 < call["timeout_ms"] <= TRACE_LIST_ENRICHMENT_TIMEOUT_MS
        for call in analytics.calls
    )
    assert all(call["settings"] == TRACE_LIST_READ_SETTINGS for call in analytics.calls)
    assert payload["table"][0]["user_id"] == "user-a"


def test_eval_task_trace_list_incomplete_page_fails_closed_before_enrichment() -> None:
    """Partial selector rows must never escape as a successful task choice."""

    from tracer.views.trace import TraceView

    project_version_id = "00000000-0000-4000-8000-000000000099"
    view = TraceView.__new__(TraceView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 503)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view.request = request
    incomplete = BoundedFilterPage(
        rows=[
            {
                "trace_id": "must-not-escape",
                "start_time": END - timedelta(minutes=1),
            }
        ],
        has_more=False,
        complete=False,
        status="degraded",
        error_code="deadline_exceeded",
        total_rows_lower_bound=1,
        elapsed_ms=4500.0,
        query_count=4,
        rows_returned=1,
        result_payload_bytes=10,
        attempts=(),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.ProjectVersion") as project_version,
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=incomplete,
        ),
    ):
        project_version.objects.get.return_value = SimpleNamespace(
            project_id=PROJECT_ID
        )
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_traces_clickhouse(
            request,
            project_version_id,
            analytics,
            {
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "sort_params": [],
                "page_number": 0,
                "page_size": 25,
            },
        )

    assert response == ("error", 503)
    view._gm.custom_error_response.assert_called_once_with(
        503,
        "Filtered trace data is temporarily unavailable. Please retry.",
        code="service_unavailable",
    )
    view._gm.success_response.assert_not_called()
    analytics.execute_ch_query.assert_not_called()


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "SPAN_LIST"},
)
def test_task_create_prompt_slug_equals_uses_bounded_span_route_contract() -> None:
    from tracer.services.clickhouse.query_service import AnalyticsQueryService
    from tracer.views.observation_span import ObservationSpanView

    view = ObservationSpanView.__new__(ObservationSpanView)
    view._gm = SimpleNamespace(success_response=lambda payload: ("ok", payload))
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    bounded = BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=0,
        elapsed_ms=2.0,
        query_count=1,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )
    legacy_analytics = AnalyticsQueryService()
    v2_analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.observation_span.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.observation_span.get_annotation_labels_for_project",
            return_value=[],
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
        mock.patch(
            "tracer.services.clickhouse.v2.query_service.V2AnalyticsQueryService",
            return_value=v2_analytics,
        ),
        mock.patch(
            "tracer.services.clickhouse.query_service.get_clickhouse_client"
        ) as legacy_client,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        status, payload = view._list_spans_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("prompt_slug", "agent_2_identity_disclosure"),
                ],
                "page_number": 0,
                "page_size": 50,
            },
            analytics=legacy_analytics,
            org_project_ids=None,
            org=organization,
        )

    assert status == "ok"
    bounded_kwargs = bounded_read.call_args.kwargs
    assert isinstance(bounded_kwargs["builder"], SpanListQueryBuilderV2)
    assert bounded_kwargs["page_number"] == 0
    assert bounded_kwargs["page_size"] == 50
    assert bounded_kwargs["analytics"] is v2_analytics
    assert bounded_kwargs["filters"][1] == _attribute_filter(
        "prompt_slug", "agent_2_identity_disclosure"
    )
    assert payload["metadata"]["query_complete"] is True
    assert 0 <= payload["metadata"]["query_elapsed_ms"] < 3_000
    assert payload["metadata"]["query_count"] == 1
    assert payload["metadata"]["query_rows_returned"] == 0
    assert payload["metadata"]["query_result_payload_bytes"] == 0
    assert payload["metadata"]["total_rows_is_lower_bound"] is True
    v2_analytics.execute_ch_query.assert_not_called()
    legacy_client.assert_not_called()


@override_settings(
    CLICKHOUSE_V2={"QUERY_TYPES_V2_ONLY": "SPAN_LIST"},
)
def test_span_list_nonempty_page_content_shares_wall_budget() -> None:
    from tracer.views.observation_span import (
        SPAN_LIST_READ_SETTINGS,
        ObservationSpanView,
    )

    started = END - timedelta(minutes=1)
    row = {
        "project_id": PROJECT_ID,
        "trace_id": "trace-a",
        "id": "span-a",
        "start_time": started,
        "created_at": started,
        "name": "span-a",
        "observation_type": "llm",
        "status": "OK",
        "cost": 0.001,
    }
    bounded = BoundedFilterPage(
        rows=[row],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=1,
        elapsed_ms=2.0,
        query_count=1,
        rows_returned=1,
        result_payload_bytes=10,
        attempts=(),
    )

    class RecordingAnalytics:
        def __init__(self):
            self.calls = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((params, timeout_ms, settings))
            data = [
                {
                    "project_id": PROJECT_ID,
                    "trace_id": "trace-a",
                    "id": "span-a",
                    "start_time": started,
                    "input": "in",
                    "output": "out",
                    "attributes_extra": "{}",
                    "attrs_string": {},
                    "attrs_number": {},
                    "attrs_bool": {},
                }
            ]
            return QueryResult(data, len(data), "clickhouse", 0.0)

    analytics = RecordingAnalytics()
    view = ObservationSpanView.__new__(ObservationSpanView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )

    with (
        mock.patch("tracer.views.observation_span.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.observation_span.get_annotation_labels_for_project",
            return_value=[],
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=bounded,
        ) as bounded_read,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        status_name, payload = view._list_spans_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert status_name == "ok"
    assert payload["table"][0]["span_id"] == "span-a"
    assert payload["metadata"]["query_count"] == 2
    assert 0 <= payload["metadata"]["query_elapsed_ms"] < 3_000
    assert bounded_read.call_args.kwargs["deadline_ms"] <= 2_200
    assert len(analytics.calls) == 1
    assert 0 < analytics.calls[0][1] <= 900
    assert analytics.calls[0][2] == SPAN_LIST_READ_SETTINGS


def test_trace_route_returns_sanitized_degraded_page_for_filtered_sort() -> None:
    from tracer.views.trace import TraceView

    class SortedTraceBuilder(TraceListQueryBuilderV2):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.sort_params = [{"column_id": "latency", "order": "desc"}]

    view = TraceView.__new__(TraceView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            return_value=SortedTraceBuilder,
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page"
        ) as bounded_read,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response[0] == "error"
    assert response[1][0] == 503
    assert response[2]["code"] == "service_unavailable"
    bounded_read.assert_not_called()
    analytics.execute_ch_query.assert_not_called()


def test_span_route_returns_sanitized_degraded_page_for_filtered_end_user() -> None:
    from tracer.views.observation_span import ObservationSpanView

    class EndUserSpanBuilder(SpanListQueryBuilderV2):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.end_user_id = "00000000-0000-4000-8000-000000000002"

    view = ObservationSpanView.__new__(ObservationSpanView)
    view._gm = SimpleNamespace(
        success_response=lambda payload: ("ok", payload),
        custom_error_response=lambda *args, **kwargs: ("error", args, kwargs),
    )
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.observation_span.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.observation_span.get_annotation_labels_for_project",
            return_value=[],
        ),
        mock.patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            return_value=EndUserSpanBuilder,
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page"
        ) as bounded_read,
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_spans_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                ],
                "page_number": 0,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response[0] == "error"
    assert response[1][0] == 503
    assert response[2]["code"] == "service_unavailable"
    bounded_read.assert_not_called()
    analytics.execute_ch_query.assert_not_called()


@dataclass
class _FakeBuilder:
    rows: list[dict[str, Any]]
    start: datetime = START
    end: datetime = END
    key_field: str = "id"
    match_rows: list[dict[str, Any]] | None = None
    seed_proves_order: bool = True
    recommended_batch_size: int | None = None

    def parse_time_range(
        self, _filters: list[dict[str, Any]]
    ) -> tuple[datetime, datetime]:
        return self.start, self.end

    def build_filter_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        return "seed", {
            "slice_start": slice_start,
            "slice_end": slice_end,
            "limit": limit,
            "before_start_time": before_start_time,
            "before_id": before_id,
        }

    def build_filter_match_query(
        self, candidate_ids: list[str]
    ) -> tuple[str, dict[str, Any]]:
        return "match", {"candidate_ids": tuple(candidate_ids)}

    def filter_seed_proves_result_order(self) -> bool:
        return self.seed_proves_order

    def recommended_filter_classify_batch_size(self) -> int | None:
        return self.recommended_batch_size


class _FakeExecutor:
    def __init__(self, builder: _FakeBuilder, *, fail: Exception | None = None):
        self.builder = builder
        self.fail = fail
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> QueryResult:
        self.calls.append((query, params))
        if self.fail is not None:
            raise self.fail
        if query == "match":
            wanted = set(params["candidate_ids"])
            source = (
                self.builder.rows
                if self.builder.match_rows is None
                else self.builder.match_rows
            )
            rows = [row for row in source if row["id"] in wanted]
        else:
            rows = [
                row
                for row in self.builder.rows
                if params["slice_start"] <= row["start_time"] < params["slice_end"]
            ]
            rows.sort(key=lambda row: (row["start_time"], row["id"]), reverse=True)
            before_time = params["before_start_time"]
            before_id = params["before_id"]
            if before_time is not None:
                rows = [
                    row
                    for row in rows
                    if (row["start_time"], row["id"]) < (before_time, before_id)
                ]
            rows = rows[: params["limit"]]
        return QueryResult(rows, len(rows), "clickhouse", 1.0)


class _UnindexedAnySpanFakeBuilder(_FakeBuilder):
    def supports_filter_anchor_probe(self) -> bool:
        return False

    def build_filter_ordered_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        _, params = self.build_filter_seed_page(
            slice_start=slice_start,
            slice_end=slice_end,
            limit=limit,
            before_start_time=before_start_time,
            before_id=before_id,
        )
        return "ordered_seed", params


class _VersionedFakeExecutor(_FakeExecutor):
    """Apply the same raw-version table filter used by CH cursor reads."""

    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> QueryResult:
        expression = (settings.get("additional_table_filters") or {}).get("spans")
        ceiling = int(expression.rsplit(" ", 1)[-1]) if expression else None
        original_rows = self.builder.rows
        original_match_rows = self.builder.match_rows
        try:
            if ceiling is not None:
                self.builder.rows = [
                    row for row in original_rows if row.get("_version", 0) < ceiling
                ]
                if original_match_rows is not None:
                    self.builder.match_rows = [
                        row
                        for row in original_match_rows
                        if row.get("_version", 0) < ceiling
                    ]
            return super().execute_ch_query(
                query,
                params,
                timeout_ms=timeout_ms,
                settings=settings,
            )
        finally:
            self.builder.rows = original_rows
            self.builder.match_rows = original_match_rows


class _PhysicalCursorFakeBuilder(_FakeBuilder):
    """Model the full direct-write span identity and public order tuple."""

    @staticmethod
    def bounded_filter_row_identity(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            row["project_id"],
            row["trace_id"],
            row["id"],
            row["start_time"],
        )

    @staticmethod
    def bounded_filter_row_order_token(row: dict[str, Any]) -> tuple[str, ...]:
        return row["id"], row["trace_id"], row["project_id"]

    bounded_filter_seed_identity = bounded_filter_row_identity
    bounded_filter_seed_order_token = bounded_filter_row_order_token

    def build_filter_match_query_from_seed_rows(
        self, rows: list[dict[str, Any]]
    ) -> tuple[str, dict[str, Any]]:
        identities = tuple(self.bounded_filter_row_identity(row) for row in rows)
        return "match_physical", {"candidate_identities": identities}


class _PhysicalCursorFakeExecutor(_FakeExecutor):
    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> QueryResult:
        del timeout_ms, settings
        self.calls.append((query, params))
        identity = self.builder.bounded_filter_row_identity

        def order(row: dict[str, Any]) -> tuple[Any, ...]:
            return (
                row["start_time"],
                self.builder.bounded_filter_row_order_token(row),
            )

        if query == "match_physical":
            wanted = set(params["candidate_identities"])
            rows = [row for row in self.builder.rows if identity(row) in wanted]
        else:
            rows = [
                row
                for row in self.builder.rows
                if params["slice_start"] <= row["start_time"] < params["slice_end"]
            ]
            rows.sort(key=order, reverse=True)
            if params["before_start_time"] is not None:
                boundary = (
                    params["before_start_time"],
                    params["before_id"],
                )
                rows = [row for row in rows if order(row) < boundary]
            rows = rows[: params["limit"]]
        return QueryResult(rows, len(rows), "clickhouse", 1.0)


class _EmptyExecutor:
    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> QueryResult:
        return QueryResult([], 0, "clickhouse", 0.0)


@pytest.mark.parametrize(
    "builder_cls,key_field",
    [
        (TraceListQueryBuilder, "trace_id"),
        (SpanListQueryBuilder, "id"),
        (SessionListQueryBuilder, "session_id"),
    ],
)
def test_default_window_is_pinned_for_empty_bounded_reads(
    builder_cls, key_field
) -> None:
    first_window = (START, END)
    drifted_window = (
        START + timedelta(microseconds=1),
        END + timedelta(microseconds=1),
    )
    filters = [_attribute_filter("final_status", "Rejected")]

    with mock.patch.object(
        BaseQueryBuilder,
        "parse_time_range",
        side_effect=[first_window, drifted_window],
    ) as parse_time_range:
        builder = builder_cls(project_id=PROJECT_ID, filters=filters)
        page = read_bounded_filter_page(
            builder=builder,
            analytics=_EmptyExecutor(),
            filters=filters,
            key_field=key_field,
            page_number=0,
            page_size=25,
            deadline_ms=5_000,
        )

    assert parse_time_range.call_count == 1
    assert page.complete is True
    assert page.rows == []
    assert page.error_code is None


def test_bounded_reader_rejects_mechanically_impossible_prefix_before_ch() -> None:
    builder = _FakeBuilder([])
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=12_800,
        deadline_ms=10_000,
        max_seed_attempts=128,
        max_candidates=200,
        max_query_count=128,
        classify_batch_size=200,
    )

    assert page.complete is False
    assert page.error_code == "page_depth_exceeded"
    assert page.query_count == 0
    assert executor.calls == []


def test_session_numbered_page_ceiling_is_deterministic() -> None:
    common = {
        "page_size": 30,
        "max_candidates": 200,
        "classify_batch_size": 200,
        "seed_batch_size": 200,
    }

    assert bounded_numbered_page_depth_exceeded(page_number=0, **common) is False
    assert bounded_numbered_page_depth_exceeded(page_number=1, **common) is False
    # Page 158 needs a 4,771-row prefix and exactly 48 seed/classify reads.
    assert bounded_numbered_page_depth_exceeded(page_number=158, **common) is False
    # Page 159 needs 4,801 rows, beyond 24 x 200 finite seed candidates.
    assert bounded_numbered_page_depth_exceeded(page_number=159, **common) is True


def test_voice_numbered_page_ceiling_is_deterministic() -> None:
    classify_batch_size = (
        VoiceCallListQueryBuilder.recommended_filter_classify_batch_size()
    )
    common = {
        "page_size": 30,
        "classify_batch_size": classify_batch_size,
        "seed_batch_size": classify_batch_size,
    }

    # Public voice page 71 needs a 2,131-row prefix and exactly 48 reads.
    assert bounded_numbered_page_depth_exceeded(page_number=70, **common) is False
    # Public voice page 72 needs 49 reads and cannot fit the finite query budget.
    assert bounded_numbered_page_depth_exceeded(page_number=71, **common) is True


@pytest.mark.parametrize(
    ("page_size", "ceiling_page", "first_rejected_page"),
    [
        (1, 4_998, 4_999),
        (500, 8, 9),
    ],
)
def test_global_numbered_page_work_ceiling_scales_with_page_size(
    page_size: int,
    ceiling_page: int,
    first_rejected_page: int,
) -> None:
    assert (ceiling_page + 2) * page_size == MAX_NUMBERED_PAGE_WORK_ROWS
    assert (
        numbered_page_depth_exceeded(
            page_number=ceiling_page,
            page_size=page_size,
        )
        is False
    )
    assert (
        numbered_page_depth_exceeded(
            page_number=first_rejected_page,
            page_size=page_size,
        )
        is True
    )


def test_internal_page_zero_candidate_reads_keep_their_own_finite_budget() -> None:
    assert (
        bounded_numbered_page_depth_exceeded(
            page_number=0,
            page_size=4_095,
        )
        is False
    )


@pytest.mark.parametrize(
    "builder_class",
    [TraceListQueryBuilder, SpanListQueryBuilder],
)
def test_exact_ceiling_preserves_trace_span_prefix_membership(builder_class) -> None:
    page_number = 8
    page_size = 500
    builder = builder_class(
        project_id=PROJECT_ID,
        page_number=page_number,
        page_size=page_size,
    )

    _, params = builder.build()
    rows = [{"id": f"row-{index}"} for index in range(params["limit"])]
    page, has_more = paginate_deduped(rows, "id", page_number, page_size)

    assert params["limit"] == MAX_NUMBERED_PAGE_WORK_ROWS
    assert [row["id"] for row in page] == [
        f"row-{index}" for index in range(4_000, 4_500)
    ]
    assert has_more is True


def test_exact_ceiling_preserves_session_offset_membership() -> None:
    builder = SessionListQueryBuilder(
        project_id=PROJECT_ID,
        page_number=8,
        page_size=500,
    )

    _, params = builder.build()

    assert params["offset"] == 4_000
    assert params["limit"] == 501


def _page_depth_exceeded_page() -> BoundedFilterPage:
    return BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=False,
        status="degraded",
        error_code="page_depth_exceeded",
        total_rows_lower_bound=0,
        elapsed_ms=0.0,
        query_count=0,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )


def _complete_empty_page() -> BoundedFilterPage:
    return BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=True,
        status="complete",
        error_code=None,
        total_rows_lower_bound=0,
        elapsed_ms=0.0,
        query_count=0,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )


def test_voice_last_supported_numbered_page_reaches_bounded_reader() -> None:
    from tracer.serializers.trace import TraceVoiceCallListResponseSerializer
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    analytics = mock.MagicMock()

    with (
        mock.patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            return_value=VoiceCallListQueryBuilder,
        ),
        mock.patch(
            "tracer.services.clickhouse.v2.query_service.query_service_for_builder",
            return_value=analytics,
        ) as query_service,
        mock.patch(
            "tracer.views.trace.get_project_eval_configs", return_value=([], [])
        ),
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.views.trace._build_annotation_map_from_scores", return_value={}
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=_complete_empty_page(),
        ) as bounded_reader,
    ):
        response = view._list_voice_calls_clickhouse(
            SimpleNamespace(),
            project_id=PROJECT_ID,
            validated_data={
                "filters": [_time_filter()],
                "page": 71,
                "page_size": 30,
            },
            remove_simulation_calls=False,
            analytics=analytics,
        )

    assert response.status_code == 200
    assert response.data["current_page"] == 71
    response_serializer = TraceVoiceCallListResponseSerializer(data=response.data)
    assert response_serializer.is_valid(), response_serializer.errors
    query_service.assert_called_once_with(
        "VOICE_CALL_LIST", VoiceCallListQueryBuilder, analytics
    )
    assert bounded_reader.call_args.kwargs["page_number"] == 70
    assert bounded_reader.call_args.kwargs["page_size"] == 30


def test_voice_first_unsupported_numbered_page_is_typed_422_before_ch() -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    analytics = mock.MagicMock()

    with (
        mock.patch(
            "tracer.services.clickhouse.v2.dispatch.get_query_builder_class",
            return_value=VoiceCallListQueryBuilder,
        ),
        mock.patch(
            "tracer.services.clickhouse.v2.query_service.query_service_for_builder"
        ) as query_service,
        mock.patch("tracer.views.trace.get_project_eval_configs") as eval_configs,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project"
        ) as annotation_labels,
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page"
        ) as bounded_reader,
    ):
        response = view._list_voice_calls_clickhouse(
            SimpleNamespace(),
            project_id=PROJECT_ID,
            validated_data={
                "filters": [_time_filter()],
                "page": 72,
                "page_size": 30,
            },
            remove_simulation_calls=False,
            analytics=analytics,
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    query_service.assert_not_called()
    eval_configs.assert_not_called()
    annotation_labels.assert_not_called()
    bounded_reader.assert_not_called()
    analytics.execute_ch_query.assert_not_called()


def test_observe_trace_page_depth_is_typed_422_without_ch_enrichment() -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=_page_depth_exceeded_page(),
        ),
    ):
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data={
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                    _attribute_filter("tenant_tier", "enterprise"),
                ],
                "page_number": 999,
                "page_size": 25,
            },
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics.execute_ch_query.assert_not_called()


def test_prototype_trace_page_depth_is_typed_422_without_ch_enrichment() -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view.request = request
    analytics = mock.MagicMock()

    with (
        mock.patch("tracer.views.trace.ProjectVersion") as project_version,
        mock.patch("tracer.views.trace.CustomEvalConfig") as eval_config,
        mock.patch(
            "tracer.views.trace.get_annotation_labels_for_project", return_value=[]
        ),
        mock.patch(
            "tracer.selectors.trace_filter_reads.read_bounded_filter_page",
            return_value=_page_depth_exceeded_page(),
        ),
    ):
        project_version.objects.get.return_value = SimpleNamespace(
            project_id=PROJECT_ID
        )
        eval_config.objects.filter.return_value.select_related.return_value = []
        response = view._list_traces_clickhouse(
            request,
            "00000000-0000-4000-8000-000000000099",
            analytics,
            {
                "filters": [
                    _time_filter(),
                    _attribute_filter("final_status", "Rejected"),
                    _attribute_filter("tenant_tier", "enterprise"),
                ],
                "sort_params": [],
                "page_number": 999,
                "page_size": 25,
            },
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics.execute_ch_query.assert_not_called()


@pytest.mark.parametrize("prototype", [False, True])
def test_span_deep_filtered_page_preflight_returns_422_before_ch(
    prototype: bool,
) -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.observation_span import ObservationSpanView

    view = ObservationSpanView.__new__(ObservationSpanView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    analytics = mock.MagicMock()
    validated_data = {
        "filters": [
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
            _attribute_filter("tenant_tier", "enterprise"),
        ],
        "page_number": 999,
        "page_size": 25,
    }

    if prototype:
        response = view._list_spans_non_observe_clickhouse(
            request,
            "00000000-0000-4000-8000-000000000099",
            SimpleNamespace(project_id=PROJECT_ID),
            analytics,
            validated_data,
        )
    else:
        response = view._list_spans_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data=validated_data,
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics.execute_ch_query.assert_not_called()
    analytics.get_eval_config_ids_with_data_ch.assert_not_called()


def test_session_deep_filtered_page_preflight_returns_422_before_ch() -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace_session import TraceSessionView

    view = TraceSessionView.__new__(TraceSessionView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    request = SimpleNamespace(
        validated_query_data={
            "project_id": PROJECT_ID,
            "filters": [
                _time_filter(),
                _attribute_filter("final_status", "Rejected"),
                _attribute_filter("tenant_tier", "enterprise"),
            ],
            "sort_params": [],
            "page_number": 159,
            "page_size": 30,
        }
    )

    with mock.patch(
        "tracer.services.clickhouse.query_service.AnalyticsQueryService"
    ) as analytics_cls:
        response = TraceSessionView.list_sessions.__wrapped__(view, request)

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics_cls.assert_not_called()


@pytest.mark.parametrize("prototype", [False, True])
def test_trace_deep_unfiltered_page_preflight_returns_422_before_ch(
    prototype: bool,
) -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace import TraceView

    view = TraceView.__new__(TraceView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    view.request = request
    analytics = mock.MagicMock()
    validated_data = {
        "filters": [],
        "sort_params": [],
        "page_number": 9,
        "page_size": 500,
    }

    if prototype:
        response = view._list_traces_clickhouse(
            request,
            "00000000-0000-4000-8000-000000000099",
            analytics,
            validated_data,
        )
    else:
        response = view._list_traces_of_session_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data=validated_data,
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics.execute_ch_query.assert_not_called()


@pytest.mark.parametrize("prototype", [False, True])
def test_span_deep_unfiltered_page_preflight_returns_422_before_ch(
    prototype: bool,
) -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.observation_span import ObservationSpanView

    view = ObservationSpanView.__new__(ObservationSpanView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    organization = SimpleNamespace(id="org-a")
    request = SimpleNamespace(
        organization=organization,
        user=SimpleNamespace(organization=organization),
    )
    analytics = mock.MagicMock()
    validated_data = {
        "filters": [],
        "page_number": 9,
        "page_size": 500,
    }

    if prototype:
        response = view._list_spans_non_observe_clickhouse(
            request,
            "00000000-0000-4000-8000-000000000099",
            SimpleNamespace(project_id=PROJECT_ID),
            analytics,
            validated_data,
        )
    else:
        response = view._list_spans_clickhouse(
            request,
            project_id=PROJECT_ID,
            validated_data=validated_data,
            analytics=analytics,
            org_project_ids=None,
            org=organization,
        )

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics.execute_ch_query.assert_not_called()


def test_session_deep_unfiltered_page_preflight_returns_422_before_ch() -> None:
    from tracer.selectors.trace_filter_reads import PAGE_DEPTH_EXCEEDED_MESSAGE
    from tracer.views.trace_session import TraceSessionView

    view = TraceSessionView.__new__(TraceSessionView)
    view._gm = mock.MagicMock()
    view._gm.custom_error_response.return_value = ("error", 422)
    request = SimpleNamespace(
        validated_query_data={
            "project_id": PROJECT_ID,
            "filters": [],
            "sort_params": [],
            "page_number": 9,
            "page_size": 500,
        }
    )

    with mock.patch(
        "tracer.services.clickhouse.query_service.AnalyticsQueryService"
    ) as analytics_cls:
        response = TraceSessionView.list_sessions.__wrapped__(view, request)

    assert response == ("error", 422)
    view._gm.custom_error_response.assert_called_once_with(
        422,
        PAGE_DEPTH_EXCEEDED_MESSAGE,
        code="page_depth_exceeded",
    )
    analytics_cls.assert_not_called()


def test_safe_legacy_upper_only_multifilter_can_prove_exact_empty_without_ch() -> None:
    builder = _FakeBuilder([], start=END, end=START)
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[
            _time_filter(),
            _attribute_filter("final_status", "Rejected"),
            _attribute_filter("tenant_tier", "enterprise"),
        ],
        key_field="id",
        page_number=0,
        page_size=25,
    )

    assert page.complete is True
    assert page.rows == []
    assert page.total_rows_lower_bound == 0
    assert page.query_count == 0
    assert executor.calls == []


def _rows(*minute_offsets: int) -> list[dict[str, Any]]:
    return [
        {"id": f"span-{index}", "start_time": END - timedelta(minutes=offset)}
        for index, offset in enumerate(minute_offsets)
    ]


def test_graph_only_incomplete_rows_do_not_change_exact_list_default() -> None:
    rows = _rows(1, 2, 3)
    builder = _FakeBuilder(rows, seed_proves_order=False)
    common = {
        "builder": builder,
        "filters": [_time_filter()],
        "key_field": "id",
        "page_number": 0,
        "page_size": 1,
        "deadline_ms": 5_000,
        "max_seed_attempts": 1,
        "max_candidates": 2,
        "max_query_count": 2,
        "classify_batch_size": 2,
    }

    exact_page = read_bounded_filter_page(
        analytics=_FakeExecutor(builder),
        **common,
    )
    graph_page = read_bounded_filter_page(
        analytics=_FakeExecutor(builder),
        include_incomplete_rows=True,
        **common,
    )

    assert exact_page.complete is False
    assert exact_page.rows == []
    assert graph_page.complete is False
    assert [row["id"] for row in graph_page.rows] == ["span-0"]
    assert graph_page.has_more is True

    with pytest.raises(ValueError, match="only for page zero"):
        read_bounded_filter_page(
            analytics=_FakeExecutor(builder),
            **{**common, "page_number": 1, "include_incomplete_rows": True},
        )


def test_bounded_reader_keeps_page_zero_and_page_n_disjoint() -> None:
    rows = _rows(1, 2, 3, 4, 5, 6, 7)
    builder = _FakeBuilder(rows)

    first = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=2,
        deadline_ms=5_000,
    )
    second = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=1,
        page_size=2,
        deadline_ms=5_000,
    )

    assert [row["id"] for row in first.rows] == ["span-0", "span-1"]
    assert [row["id"] for row in second.rows] == ["span-2", "span-3"]
    assert {row["id"] for row in first.rows}.isdisjoint(
        row["id"] for row in second.rows
    )
    assert first.has_more is True
    assert second.has_more is True


def test_cursor_keyset_handles_equal_timestamps_without_duplicates_or_skips() -> None:
    timestamp = END - timedelta(minutes=1)
    rows = [
        {"id": row_id, "start_time": timestamp}
        for row_id in ("span-d", "span-c", "span-b", "span-a")
    ]
    builder = _FakeBuilder(rows)

    first = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=2,
    )
    second = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=2,
        cursor_start_time=first.rows[-1]["start_time"],
        cursor_order_token=first.rows[-1]["id"],
    )

    assert [row["id"] for row in first.rows] == ["span-d", "span-c"]
    assert [row["id"] for row in second.rows] == ["span-b", "span-a"]
    assert {row["id"] for row in first.rows}.isdisjoint(
        row["id"] for row in second.rows
    )
    assert second.has_more is False


def test_cursor_keyset_preserves_same_identity_rows_one_microsecond_apart() -> None:
    newest = END - timedelta(minutes=1)
    rows = [
        {
            "id": "same-span",
            "trace_id": "same-trace",
            "project_id": "same-project",
            "start_time": newest,
        },
        {
            "id": "same-span",
            "trace_id": "same-trace",
            "project_id": "same-project",
            "start_time": newest - timedelta(microseconds=1),
        },
    ]
    builder = _PhysicalCursorFakeBuilder(rows)

    first = read_bounded_filter_page(
        builder=builder,
        analytics=_PhysicalCursorFakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=1,
    )
    second = read_bounded_filter_page(
        builder=builder,
        analytics=_PhysicalCursorFakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=1,
        cursor_start_time=first.rows[-1]["start_time"],
        cursor_order_token=("same-span", "same-trace", "same-project"),
    )

    assert first.rows[0]["start_time"] == newest
    assert second.rows[0]["start_time"] == newest - timedelta(microseconds=1)
    assert second.has_more is False


def test_cursor_version_ceiling_excludes_concurrent_inserts_and_equal_boundary() -> (
    None
):
    rows = [
        {
            "id": f"span-{row_id}",
            "start_time": END - timedelta(minutes=offset),
            "_version": 9,
        }
        for row_id, offset in (("d", 1), ("c", 2), ("b", 3), ("a", 4))
    ]
    builder = _FakeBuilder(rows)
    settings = {"additional_table_filters": {"spans": "_version < 10"}}

    first = read_bounded_filter_page(
        builder=builder,
        analytics=_VersionedFakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=2,
        read_settings=settings,
    )
    # Simulate live inserts after page one. One is newer than the cursor and
    # one belongs in the remaining tail; neither is in the frozen version set.
    builder.rows.extend(
        [
            {
                "id": "span-live-new",
                "start_time": END - timedelta(seconds=1),
                "_version": 11,
            },
            {
                "id": "span-live-tail",
                "start_time": END - timedelta(minutes=3, seconds=30),
                "_version": 10,
            },
        ]
    )
    second = read_bounded_filter_page(
        builder=builder,
        analytics=_VersionedFakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=2,
        cursor_start_time=first.rows[-1]["start_time"],
        cursor_order_token=first.rows[-1]["id"],
        read_settings=settings,
    )

    assert [row["id"] for row in first.rows] == ["span-d", "span-c"]
    assert [row["id"] for row in second.rows] == ["span-b", "span-a"]
    assert "span-live-tail" not in {row["id"] for row in second.rows}


def test_bounded_reader_crosses_sparse_tail_with_adjacent_slices() -> None:
    rows = _rows(60 * 24 * 200)
    builder = _FakeBuilder(rows)
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    seed_attempts = [attempt for attempt in page.attempts if attempt.kind == "seed"]
    assert [row["id"] for row in page.rows] == ["span-0"]
    assert page.complete is True
    assert len(seed_attempts) > 1
    assert all(
        newer.slice_start == older.slice_end
        for newer, older in zip(seed_attempts, seed_attempts[1:], strict=False)
    )


def test_bounded_reader_covers_a_year_without_a_whole_window_query() -> None:
    builder = _FakeBuilder([])

    page = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    seed_attempts = [attempt for attempt in page.attempts if attempt.kind == "seed"]
    assert page.complete is True
    assert seed_attempts[0].slice_end == END
    assert seed_attempts[-1].slice_start == START
    assert len(seed_attempts) <= 24
    assert all(
        attempt.slice_end - attempt.slice_start < END - START
        for attempt in seed_attempts
    )


def test_bounded_reader_page_n_is_exact_in_a_one_year_sparse_tail() -> None:
    rows = _rows(60 * 24 * 180, 60 * 24 * 240, 60 * 24 * 320)
    builder = _FakeBuilder(rows)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=1,
        page_size=1,
        deadline_ms=5_000,
    )

    assert [row["id"] for row in page.rows] == ["span-1"]
    assert page.complete is True
    assert page.has_more is True


def test_any_span_seed_exhausts_sparse_year_before_root_ordered_page_n() -> None:
    """Child timestamps cannot close a root-ordered trace page prefix.

    The two newest matching children belong to old roots. A much older matching
    child belongs to the newest root. The reader must therefore exhaust every
    adjacent child-match slice before returning either numbered root page.
    """

    window_start = END - timedelta(days=365)
    seed_rows = [
        {"id": "trace-old-a", "start_time": END - timedelta(minutes=1)},
        {"id": "trace-old-b", "start_time": END - timedelta(minutes=2)},
        {
            "id": "trace-newest",
            "start_time": window_start + timedelta(days=200),
        },
    ]
    root_rows = [
        {"id": "trace-old-a", "start_time": window_start + timedelta(days=10)},
        {"id": "trace-old-b", "start_time": window_start + timedelta(days=5)},
        {"id": "trace-newest", "start_time": window_start + timedelta(days=20)},
    ]
    builder = _FakeBuilder(
        seed_rows,
        start=window_start,
        end=END,
        match_rows=root_rows,
        seed_proves_order=False,
    )

    first_executor = _FakeExecutor(builder)
    first = read_bounded_filter_page(
        builder=builder,
        analytics=first_executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=1,
        deadline_ms=5_000,
    )
    second_executor = _FakeExecutor(builder)
    second = read_bounded_filter_page(
        builder=builder,
        analytics=second_executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=1,
        page_size=1,
        deadline_ms=5_000,
    )

    assert [row["id"] for row in first.rows] == ["trace-newest"]
    assert [row["id"] for row in second.rows] == ["trace-old-a"]
    assert first.complete is True and second.complete is True
    for executor in (first_executor, second_executor):
        seed_calls = [params for query, params in executor.calls if query == "seed"]
        assert min(call["slice_start"] for call in seed_calls) == window_start


def test_unindexed_any_span_reader_starts_with_ordered_root_batches() -> None:
    rows = _rows(1, 2, 3)
    builder = _UnindexedAnySpanFakeBuilder(rows, seed_proves_order=False)
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=1,
        deadline_ms=5_000,
    )

    seed_queries = [query for query, _ in executor.calls if query != "match"]
    assert page.complete is True
    assert [row["id"] for row in page.rows] == ["span-0"]
    assert seed_queries == ["ordered_seed"]


def test_any_span_customer_match_set_uses_200_candidate_query_budget() -> None:
    """1,063 matches need six classifier batches, not eleven 100-row batches."""

    window_start = END - timedelta(days=7)
    seed_rows = [
        {
            "id": f"trace-{index:04d}",
            # The reader uses the half-open request window [start, end).
            "start_time": END - timedelta(seconds=(index + 1) / 10),
        }
        for index in range(1_063)
    ]
    builder = _FakeBuilder(
        seed_rows,
        start=window_start,
        end=END,
        seed_proves_order=False,
    )
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter(start=window_start, end=END)],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    seed_calls = [params for query, params in executor.calls if query == "seed"]
    classify_calls = [params for query, params in executor.calls if query == "match"]
    assert page.complete is True
    assert len(page.rows) == 25
    assert all(call["limit"] == 200 for call in seed_calls)
    assert len(classify_calls) == 6
    assert max(len(call["candidate_ids"]) for call in classify_calls) == 200
    assert sum(len(call["candidate_ids"]) for call in classify_calls) == 1_063
    assert page.query_count <= 24


def test_builder_batch_recommendation_caps_seed_and_classifier_working_set() -> None:
    rows = [
        {
            "id": f"trace-{index:04d}",
            "start_time": END - timedelta(seconds=index + 1),
        }
        for index in range(100)
    ]
    builder = _FakeBuilder(rows, recommended_batch_size=50)
    executor = _FakeExecutor(builder)

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    seed_calls = [params for query, params in executor.calls if query == "seed"]
    classify_calls = [params for query, params in executor.calls if query == "match"]
    assert page.complete is True
    assert len(page.rows) == 25
    assert [call["limit"] for call in seed_calls] == [50]
    assert [len(call["candidate_ids"]) for call in classify_calls] == [50]


def test_read_budget_failure_is_degraded_sanitized_and_not_retried() -> None:
    builder = _FakeBuilder([])
    executor = _FakeExecutor(
        builder,
        fail=ReadDeadlineExceeded(
            "Code: 159. Timeout exceeded; secret-host.internal; SELECT customer_payload"
        ),
    )

    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    assert page.rows == []
    assert page.complete is False
    assert page.status == "degraded"
    assert page.error_code == "read_budget_exceeded"
    assert len(executor.calls) == 1
    assert page.query_count == 1
    assert page.attempts[0].error_code == "read_budget_exceeded"
    assert "secret-host" not in repr(page)
    assert "SELECT customer_payload" not in repr(page)


def test_eval_mode_halves_a_wide_timeout_and_still_covers_the_window() -> None:
    start = END - timedelta(hours=2)
    builder = _FakeBuilder([], start=start, end=END)

    class WidthBoundExecutor(_FakeExecutor):
        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            if query == "seed" and params["slice_end"] - params[
                "slice_start"
            ] > timedelta(minutes=30):
                self.calls.append((query, params))
                raise ReadDeadlineExceeded("Code: 159. Timeout exceeded")
            return super().execute_ch_query(
                query, params, timeout_ms=timeout_ms, settings=settings
            )

    executor = WidthBoundExecutor(builder)
    page = read_bounded_filter_page(
        builder=builder,
        analytics=executor,
        filters=[_time_filter(start=start, end=END)],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
        max_seed_attempts=64,
        max_query_count=64,
        retry_wide_read_budget=True,
    )

    assert page.complete is True
    assert page.rows == []
    assert any(
        attempt.error_code == "read_budget_exceeded" for attempt in page.attempts
    )
    successful_seeds = [
        attempt
        for attempt in page.attempts
        if attempt.kind == "seed" and attempt.error_code is None
    ]
    assert successful_seeds[0].slice_end == END
    assert successful_seeds[-1].slice_start == start


def test_programming_errors_are_not_hidden_as_read_budget_failures() -> None:
    builder = _FakeBuilder([])
    executor = _FakeExecutor(builder, fail=KeyError("bad query plan"))

    with pytest.raises(KeyError, match="bad query plan"):
        read_bounded_filter_page(
            builder=builder,
            analytics=executor,
            filters=[_time_filter()],
            key_field="id",
            page_number=0,
            page_size=25,
            deadline_ms=5_000,
        )


def test_attempt_ledger_exposes_separate_timing_query_rows_and_bytes() -> None:
    builder = _FakeBuilder(_rows(1))
    page = read_bounded_filter_page(
        builder=builder,
        analytics=_FakeExecutor(builder),
        filters=[_time_filter()],
        key_field="id",
        page_number=0,
        page_size=25,
        deadline_ms=5_000,
    )

    assert page.query_count == len(page.attempts)
    assert page.elapsed_ms >= 0
    assert all(attempt.elapsed_ms >= 0 for attempt in page.attempts)
    assert all(attempt.query_count == 1 for attempt in page.attempts)
    assert sum(attempt.rows_returned for attempt in page.attempts) == page.rows_returned
    assert (
        sum(attempt.result_payload_bytes for attempt in page.attempts)
        == page.result_payload_bytes
    )
