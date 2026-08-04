from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from django.test import override_settings

from tracer.serializers.observation_span import SpanObserveListQuerySerializer
from tracer.serializers.trace import TraceObserveListQuerySerializer
from tracer.services.clickhouse.list_cursor import (
    ListCursorError,
    capture_snapshot_version_ceiling,
    cursor_page_metadata,
    cursor_scope_for_request,
    decode_list_cursor,
    encode_list_cursor,
    exact_total_explicitly_required,
    normalize_cursor_query,
    snapshot_cursor_supported,
    snapshot_read_settings,
)


def _request(*, user_id="u1", org_id="o1", workspace_id="w1", auth_id="a1"):
    organization = SimpleNamespace(pk=org_id)
    user = SimpleNamespace(
        pk=user_id,
        organization=organization,
        default_workspace_id=workspace_id,
    )
    return SimpleNamespace(
        user=user,
        organization=organization,
        workspace=SimpleNamespace(pk=workspace_id),
        auth=SimpleNamespace(pk=auth_id),
    )


@pytest.mark.parametrize(
    ("query_params", "validated_data", "expected"),
    [
        ({}, {}, False),
        ({"allow_sampled": "false"}, {"allow_sampled": False}, True),
        ({"allow_sampled": "true"}, {"allow_sampled": True}, False),
    ],
)
def test_exact_total_is_required_only_by_explicit_false(
    query_params,
    validated_data,
    expected,
):
    request = SimpleNamespace(query_params=query_params)

    assert exact_total_explicitly_required(request, validated_data) is expected


def _token(**overrides):
    request = overrides.pop("request", _request())
    scope = cursor_scope_for_request(request, project_ids=["p2", "p1"])
    values = {
        "resource": "traces",
        "scope": scope,
        "query": {
            "filters": [
                {
                    "column_id": "status",
                    "filter_config": {
                        "filter_op": "in",
                        "filter_value": ["error", "ok"],
                    },
                }
            ],
            "sort_params": [],
        },
        "page_size": 25,
        "window_start": datetime(2026, 1, 1, tzinfo=UTC),
        "window_end": datetime(2026, 8, 1, tzinfo=UTC),
        "order": (datetime(2026, 7, 1, tzinfo=UTC), "trace-2"),
        "version_ceiling": 1_785_742_808_330_811_452,
        "seen_rows": 25,
    }
    values.update(overrides)
    return encode_list_cursor(**values), values


def test_cursor_round_trip_preserves_datetime_and_complete_order_tuple():
    token, values = _token()
    cursor = decode_list_cursor(
        token,
        resource=values["resource"],
        scope=values["scope"],
        query=values["query"],
        page_size=values["page_size"],
    )
    assert cursor.window_start == values["window_start"]
    assert cursor.window_end == values["window_end"]
    assert cursor.order == values["order"]
    assert cursor.version_ceiling == values["version_ceiling"]
    assert cursor.seen_rows == 25


def test_cursor_normalizes_filter_order_and_in_value_order():
    left = {
        "filters": [
            {
                "column_id": "b",
                "filter_config": {"filter_op": "equals", "filter_value": "2"},
            },
            {
                "column_id": "a",
                "filter_config": {
                    "filter_op": "in",
                    "filter_value": ["z", "a"],
                },
            },
        ],
        "search": " value ",
    }
    right = {
        "search": "value",
        "filters": [
            {
                "column_id": "a",
                "filter_config": {
                    "filter_value": ["a", "z"],
                    "filter_op": "in",
                },
            },
            {
                "filter_config": {"filter_value": "2", "filter_op": "equals"},
                "column_id": "b",
            },
        ],
    }
    assert normalize_cursor_query(left) == normalize_cursor_query(right)


@pytest.mark.parametrize(
    ("change", "expected_code"),
    [
        ({"resource": "spans"}, "cursor_mismatch"),
        ({"page_size": 50}, "cursor_mismatch"),
        ({"query": {"filters": []}}, "cursor_mismatch"),
    ],
)
def test_cursor_rejects_request_mismatch(change, expected_code):
    token, values = _token()
    decode_args = {
        "resource": values["resource"],
        "scope": values["scope"],
        "query": values["query"],
        "page_size": values["page_size"],
        **change,
    }
    with pytest.raises(ListCursorError) as exc_info:
        decode_list_cursor(token, **decode_args)
    assert exc_info.value.code == expected_code
    assert "cursor" in str(exc_info.value).lower()


def test_cursor_rejects_tenant_auth_and_project_replay():
    token, values = _token()
    other_scope = cursor_scope_for_request(
        _request(user_id="u2", org_id="o2", workspace_id="w2", auth_id="a2"),
        project_ids=["p1", "p2"],
    )
    with pytest.raises(ListCursorError, match="does not match") as exc_info:
        decode_list_cursor(
            token,
            resource=values["resource"],
            scope=other_scope,
            query=values["query"],
            page_size=values["page_size"],
        )
    assert exc_info.value.code == "cursor_mismatch"


def test_org_cursor_cannot_be_replayed_in_single_project_scope():
    request = _request()
    org_scope = cursor_scope_for_request(request, project_ids=["p1", "p2"])
    token, values = _token(
        request=request,
        scope=org_scope,
        order=(datetime(2026, 7, 1, tzinfo=UTC), "trace-2", "p2"),
    )
    single_project_scope = cursor_scope_for_request(request, project_ids=["p1"])

    with pytest.raises(ListCursorError) as exc_info:
        decode_list_cursor(
            token,
            resource=values["resource"],
            scope=single_project_scope,
            query=values["query"],
            page_size=values["page_size"],
        )

    assert exc_info.value.code == "cursor_mismatch"


def test_cursor_rejects_tampering_without_exposing_signing_details():
    token, values = _token()
    with pytest.raises(ListCursorError) as exc_info:
        decode_list_cursor(
            f"{token[:-1]}x",
            resource=values["resource"],
            scope=values["scope"],
            query=values["query"],
            page_size=values["page_size"],
        )
    assert exc_info.value.code == "invalid_cursor"
    assert str(exc_info.value) == "The continuation cursor is invalid."


@override_settings(TRACER_LIST_CURSOR_MAX_AGE_SECONDS=1)
def test_cursor_rejects_expired_token(monkeypatch):
    from django.core import signing

    monkeypatch.setattr(signing.time, "time", lambda: 1_000.0)
    token, values = _token()
    monkeypatch.setattr(signing.time, "time", lambda: 1_010.0)
    with pytest.raises(ListCursorError) as exc_info:
        decode_list_cursor(
            token,
            resource=values["resource"],
            scope=values["scope"],
            query=values["query"],
            page_size=values["page_size"],
        )
    assert exc_info.value.code == "cursor_expired"


@override_settings(TRACER_LIST_CURSOR_MAX_AGE_SECONDS=1)
def test_org_composite_cursor_keeps_the_same_ttl(monkeypatch):
    from django.core import signing

    monkeypatch.setattr(signing.time, "time", lambda: 1_000.0)
    token, values = _token(order=(datetime(2026, 7, 1, tzinfo=UTC), "trace-2", "p2"))
    cursor = decode_list_cursor(
        token,
        resource=values["resource"],
        scope=values["scope"],
        query=values["query"],
        page_size=values["page_size"],
    )
    assert cursor.order == values["order"]

    monkeypatch.setattr(signing.time, "time", lambda: 1_010.0)
    with pytest.raises(ListCursorError) as exc_info:
        decode_list_cursor(
            token,
            resource=values["resource"],
            scope=values["scope"],
            query=values["query"],
            page_size=values["page_size"],
        )
    assert exc_info.value.code == "cursor_expired"


@pytest.mark.parametrize(
    "serializer_cls",
    [TraceObserveListQuerySerializer, SpanObserveListQuerySerializer],
)
def test_observe_list_serializers_reject_cursor_with_explicit_numbered_page(
    serializer_cls,
):
    serializer = serializer_cls(
        data={"cursor": "opaque", "cursor_mode": True, "page_number": 1}
    )

    assert not serializer.is_valid()
    assert "cursor" in serializer.errors


@pytest.mark.parametrize(
    "serializer_cls",
    [TraceObserveListQuerySerializer, SpanObserveListQuerySerializer],
)
def test_observe_list_serializers_accept_additive_cursor_mode(serializer_cls):
    serializer = serializer_cls(data={"cursor_mode": True, "page_number": 0})

    assert serializer.is_valid(), serializer.errors
    assert serializer.validated_data["cursor_mode"] is True


@pytest.mark.parametrize(
    "serializer_cls",
    [TraceObserveListQuerySerializer, SpanObserveListQuerySerializer],
)
def test_observe_list_serializers_reject_fresh_cursor_mode_on_deep_page(
    serializer_cls,
):
    serializer = serializer_cls(data={"cursor_mode": True, "page_number": 2})

    assert not serializer.is_valid()
    assert "cursor_mode" in serializer.errors


@pytest.mark.parametrize(
    "serializer_cls",
    [TraceObserveListQuerySerializer, SpanObserveListQuerySerializer],
)
def test_observe_list_serializers_keep_numbered_deep_pages_backward_compatible(
    serializer_cls,
):
    serializer = serializer_cls(data={"page_number": 2})

    assert serializer.is_valid(), serializer.errors
    assert serializer.validated_data["page_number"] == 2
    assert serializer.validated_data["cursor_mode"] is False


def _filter(column_id, *, col_type="SPAN_ATTRIBUTE", filter_type="text"):
    filter_value = {"tier": "value"} if filter_type in {"map", "json"} else "value"
    return {
        "column_id": column_id,
        "filter_config": {
            "col_type": col_type,
            "filter_type": filter_type,
            "filter_op": "equals",
            "filter_value": filter_value,
        },
    }


@pytest.mark.parametrize("resource", ["observe_traces", "observe_spans"])
def test_snapshot_cursor_accepts_span_local_scalar_map_and_json_filters(resource):
    filters = [
        _filter("final_status"),
        _filter("customer_map", filter_type="map"),
        _filter("payload_json", filter_type="json"),
    ]

    assert snapshot_cursor_supported(filters, resource=resource) is True


@pytest.mark.parametrize("resource", ["observe_traces", "observe_spans"])
@pytest.mark.parametrize(
    "filter_item",
    [
        _filter("quality", col_type="EVAL_METRIC"),
        _filter("reviewed", col_type="ANNOTATION"),
        _filter("user_id", col_type="TRACE_END_USER"),
    ],
)
def test_snapshot_cursor_falls_back_for_independently_mutable_relations(
    resource, filter_item
):
    assert snapshot_cursor_supported([filter_item], resource=resource) is False


def test_legacy_fallback_omits_cursor_metadata_even_when_more_rows_exist():
    assert (
        cursor_page_metadata(
            enabled=False,
            has_more=True,
            seen_rows=25,
            next_cursor=None,
        )
        == {}
    )


def test_cursor_metadata_never_claims_terminal_without_a_required_continuation():
    with pytest.raises(RuntimeError, match="requires a continuation"):
        cursor_page_metadata(
            enabled=True,
            has_more=True,
            seen_rows=25,
            next_cursor=None,
        )

    assert cursor_page_metadata(
        enabled=True,
        has_more=True,
        seen_rows=25,
        next_cursor="opaque",
    ) == {
        "total_rows": 26,
        "total_rows_exact": None,
        "total_rows_is_lower_bound": True,
        "has_more": True,
        "next_cursor": "opaque",
    }


def test_terminal_cursor_metadata_reports_the_exact_seen_total():
    assert cursor_page_metadata(
        enabled=True,
        has_more=False,
        seen_rows=42,
        next_cursor=None,
    ) == {
        "total_rows": 42,
        "total_rows_exact": 42,
        "total_rows_is_lower_bound": False,
        "has_more": False,
        "next_cursor": None,
    }


def test_snapshot_settings_use_direct_write_version_and_preserve_existing_filters():
    builder_cls = type(
        "TraceListQueryBuilderV2",
        (),
        {
            "__module__": "tracer.services.clickhouse.v2.query_builders.trace_list",
            "TABLE": "spans",
        },
    )

    result = snapshot_read_settings(
        {"max_threads": 2, "additional_table_filters": {"other": "x = 1"}},
        builder=builder_cls(),
        version_ceiling=42,
    )

    assert result["max_threads"] == 2
    assert result["additional_table_filters"] == {
        "other": "x = 1",
        "spans": "_version < 42",
    }


def test_snapshot_settings_keep_legacy_version_column_for_legacy_builder():
    builder_cls = type(
        "TraceListQueryBuilder",
        (),
        {
            "__module__": "tracer.services.clickhouse.query_builders.trace_list",
            "TABLE": "spans",
        },
    )

    result = snapshot_read_settings({}, builder=builder_cls(), version_ceiling=42)

    assert result["additional_table_filters"] == {"spans": "_peerdb_version < 42"}


def test_snapshot_ceiling_comes_from_clickhouse_server_time():
    class Analytics:
        def __init__(self):
            self.calls = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((query, params, timeout_ms, settings))
            return SimpleNamespace(data=[{"version_ceiling": 123456789}])

    analytics = Analytics()

    assert capture_snapshot_version_ceiling(analytics, timeout_ms=175) == 123456789
    assert len(analytics.calls) == 1
    query, params, timeout_ms, settings = analytics.calls[0]
    assert "now64(9, 'UTC')" in query
    assert params == {}
    assert timeout_ms == 175
    assert settings == {"max_threads": 1, "max_result_rows": 1}


def test_cursor_datetime_precision_matches_canonical_ch25_schema():
    schema = (
        Path(__file__).parents[1]
        / "services"
        / "clickhouse"
        / "v2"
        / "schema"
        / "002_spans_v2.sql"
    ).read_text()

    # Python datetime preserves six fractional digits, so the signed cursor's
    # ordering timestamp is lossless for the canonical direct-write schema.
    assert "start_time          DateTime64(6, 'UTC')" in schema
