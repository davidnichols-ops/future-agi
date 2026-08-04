"""Focused contracts for bounded CH25 attribute discovery/value reads."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from clickhouse_driver.errors import ServerException
from rest_framework.test import APIRequestFactory, force_authenticate

from tracer.serializers.observation_span import (
    ObservationAttributeListQuerySerializer,
    ObservationAttributeListResponseSerializer,
)
from tracer.serializers.span_attributes import (
    SpanAttributeDetailQuerySerializer,
    SpanAttributeDetailResponseSerializer,
    SpanAttributeKeysResponseSerializer,
    SpanAttributeProjectQuerySerializer,
)
from tracer.services.clickhouse.attribute_reads import (
    _LATEST_CARDINALITY_SQL,
    _STRATIFIED_CANDIDATE_SQL,
    ATTRIBUTE_READ_CANDIDATE_LIMIT,
    ATTRIBUTE_READ_MAX_PROJECTS,
    ATTRIBUTE_READ_MAX_QUERY_COUNT,
    ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT,
    ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT,
    ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT,
    AttributeCardinalityRead,
    AttributeDetailRead,
    AttributeKeyRead,
    AttributeKeyRow,
    AttributeQueryPage,
    AttributeReadMetadata,
    AttributeReadSelector,
    AttributeValueRead,
    AttributeValueRow,
    IncompleteLatestStateReplay,
    InvalidAttributeKey,
    V2AttributeQueryExecutor,
    _unix_microseconds,
    adaptive_attribute_windows,
    merge_read_metadata,
    validate_attribute_key,
)
from tracer.services.clickhouse.read_budget import ReadDeadlineExceeded

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
PROJECT_A = "c4de3065-12b5-488c-a814-aa1c8e3f856f"
PROJECT_B = "790063cd-bc6a-4ad0-866b-35f11b5bc29b"


@dataclass(frozen=True)
class QueryCall:
    sql: str
    params: dict[str, Any]
    timeout_ms: int
    settings: dict[str, Any]


class RecordingExecutor:
    def __init__(self, responder=None):
        self.calls: list[QueryCall] = []
        self.responder = responder or (lambda *_: [])

    def execute(self, query, params, *, timeout_ms, settings):
        call = QueryCall(query, dict(params), timeout_ms, dict(settings))
        self.calls.append(call)
        result = self.responder(call, len(self.calls))
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, AttributeQueryPage):
            return result
        return AttributeQueryPage(data=list(result), query_time_ms=1.0)


def _metadata(
    *,
    complete: bool = True,
    error_code: str | None = None,
    sampled: bool = False,
):
    return AttributeReadMetadata(
        query_complete=complete,
        query_status="complete" if complete else "sampled" if sampled else "degraded",
        query_error_code=error_code,
        query_window_start=NOW - timedelta(days=365),
        query_window_end=NOW,
        query_count=2,
    )


def test_attribute_metadata_distinguishes_samples_from_incomplete_reads():
    sampled = _metadata(
        complete=False,
        error_code="sample_limit",
        sampled=True,
    )
    degraded = _metadata(
        complete=False,
        error_code="read_budget_exceeded",
    )

    assert sampled.public_payload()["query_status"] == "sampled"
    assert degraded.public_payload()["query_status"] == "degraded"


def test_merged_attribute_metadata_never_hides_a_degraded_phase():
    exact = _metadata()
    sampled = _metadata(
        complete=False,
        error_code="sample_limit",
        sampled=True,
    )
    degraded = _metadata(
        complete=False,
        error_code="read_budget_exceeded",
    )

    sampled_merge = merge_read_metadata(exact, sampled)
    degraded_merge = merge_read_metadata(sampled, degraded)

    assert sampled_merge.query_status == "sampled"
    assert sampled_merge.query_error_code == "sample_limit"
    assert degraded_merge.query_status == "degraded"
    assert degraded_merge.query_error_code == "read_budget_exceeded"


def test_metadata_merge_never_infers_sampled_from_incomplete_complete_status():
    inconsistent = replace(
        _metadata(complete=False, error_code="query_failed"),
        query_status="complete",
    )

    merged = merge_read_metadata(_metadata(), inconsistent)

    assert merged.query_complete is False
    assert merged.query_status == "degraded"
    assert merged.query_error_code == "query_failed"


def _target_row(
    project_id: str,
    span_id: str,
    *,
    trace_id: str | None = None,
    start_time: datetime | None = None,
    is_deleted: int = 0,
    string: Any = None,
    number: Any = None,
    boolean: Any = None,
    legacy_raw: Any = None,
    latest_version: int = 1,
):
    return {
        "project_id": project_id,
        "id": span_id,
        "start_time": start_time or NOW - timedelta(days=1),
        "is_deleted": is_deleted,
        "trace_id": (
            trace_id if trace_id is not None else f"trace-{project_id}-{span_id}"
        ),
        "trace_session_id": "",
        "parent_span_id": "",
        "string_present": string is not None,
        "string_value": string or "",
        "number_present": number is not None,
        "number_value": number or 0,
        "boolean_present": boolean is not None,
        "boolean_value": boolean or 0,
        "legacy_present": legacy_raw is not None,
        "legacy_value_raw": legacy_raw or "",
        "latest_version": latest_version,
    }


def _candidate(
    project_id: str,
    span_id: str,
    *,
    trace_id: str | None = None,
    start_time: datetime | None = None,
    candidate_version: int = 1,
):
    return {
        "project_id": project_id,
        "trace_id": (
            trace_id if trace_id is not None else f"trace-{project_id}-{span_id}"
        ),
        "id": span_id,
        "start_time": start_time or NOW - timedelta(days=1),
        "candidate_version": candidate_version,
    }


def _candidate_key(row: dict[str, Any]) -> tuple[datetime, str, str, str]:
    return (
        row["start_time"],
        str(row["id"]),
        str(row["trace_id"]),
        str(row["project_id"]),
    )


def _keyset_candidate_page(
    rows: list[dict[str, Any]], call: QueryCall
) -> list[dict[str, Any]]:
    """Apply the candidate SQL's physical-identity order to fixture rows."""

    unique = {
        (
            str(row["project_id"]),
            str(row["trace_id"]),
            str(row["id"]),
            row["start_time"],
        ): row
        for row in rows
    }
    ordered = sorted(unique.values(), key=_candidate_key, reverse=True)
    if "candidate_before_start_us" in call.params:
        before = (
            datetime.fromtimestamp(
                call.params["candidate_before_start_us"] / 1_000_000,
                tz=UTC,
            ),
            call.params["candidate_before_id"],
            call.params["candidate_before_trace_id"],
            call.params["candidate_before_project_id"],
        )
        ordered = [row for row in ordered if _candidate_key(row) < before]
    return ordered[: call.params["candidate_limit"]]


class _ProjectScope:
    def __init__(self, project_ids):
        self.project_ids = project_ids

    def values_list(self, *_args, **_kwargs):
        return self.project_ids


def _authenticated_get(path: str, data: dict[str, Any]):
    request = APIRequestFactory().get(path, data)
    force_authenticate(request, user=SimpleNamespace(is_authenticated=True))
    return request


def test_adaptive_windows_are_adjacent_half_open_7d_14d_30d_6mo_1yr_bands():
    windows = adaptive_attribute_windows(NOW)

    assert windows == (
        (NOW - timedelta(days=7), NOW),
        (NOW - timedelta(days=14), NOW - timedelta(days=7)),
        (NOW - timedelta(days=30), NOW - timedelta(days=14)),
        (NOW - timedelta(days=180), NOW - timedelta(days=30)),
        (NOW - timedelta(days=365), NOW - timedelta(days=180)),
    )
    assert all(
        left[0] == right[1] for left, right in zip(windows, windows[1:], strict=False)
    )


@pytest.mark.parametrize("days", [7, 14])
def test_explicit_dense_windows_are_adjacent_newest_first_day_segments(days: int):
    executor = RecordingExecutor()

    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys(
        [PROJECT_A],
        exact_key="missing",
        window_start=NOW - timedelta(days=days),
        window_end=NOW,
    )

    segments = [
        (call.params["segment_start"], call.params["segment_end"])
        for call in executor.calls
    ]
    assert len(segments) == days
    assert segments[0] == (NOW - timedelta(days=1), NOW)
    assert segments[-1] == (
        NOW - timedelta(days=days),
        NOW - timedelta(days=days - 1),
    )
    assert all(
        newer[0] == older[1]
        for newer, older in zip(segments, segments[1:], strict=False)
    )
    assert read.metadata.query_complete is True


@pytest.mark.parametrize(
    ("horizon_days", "expected_band_count"),
    [(7, 1), (14, 2), (180, 4), (365, 5)],
)
def test_exact_typed_first_probe_covers_each_requested_horizon_band(
    horizon_days: int,
    expected_band_count: int,
) -> None:
    executor = RecordingExecutor()

    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys(
        [PROJECT_A],
        exact_key="final_status",
        horizon_days=horizon_days,
    )

    expected_windows = adaptive_attribute_windows(NOW, horizon_days=horizon_days)
    assert read.rows == ()
    assert read.metadata.query_complete is True
    assert len(expected_windows) == expected_band_count
    assert [
        (call.params["segment_start"], call.params["segment_end"])
        for call in executor.calls
    ] == list(expected_windows)


def test_empty_key_inventory_walks_five_bounded_ch25_segments():
    executor = RecordingExecutor()
    read = AttributeReadSelector(executor, now=NOW).discover_keys([PROJECT_A])

    assert read.rows == ()
    assert read.metadata.query_complete is True
    assert len(executor.calls) == 10
    assert [
        (call.params["segment_start"], call.params["segment_end"])
        for call in executor.calls
    ] == [
        segment
        for segment in adaptive_attribute_windows(NOW)
        for _lane in ("typed", "json")
    ]
    typed_calls = [
        call for call in executor.calls if "length(attrs_string.keys)" in call.sql
    ]
    json_calls = [
        call for call in executor.calls if "attributes_extra NOT IN" in call.sql
    ]
    assert len(typed_calls) == len(json_calls) == 5
    for call in executor.calls:
        upper_sql = call.sql.upper()
        assert "FROM SPANS" in upper_sql
        assert (
            "PREWHERE ATTRIBUTE_SOURCE.PROJECT_ID = "
            "TOUUID(%(SCOPE_PROJECT_ID)S)" in upper_sql
        )
        assert call.params["scope_project_id"] == PROJECT_A
        assert "START_TIME >= %(SEGMENT_START)S" in upper_sql
        assert "START_TIME < %(SEGMENT_END)S" in upper_sql
        assert " FINAL " not in f" {upper_sql} "
        assert "ARRAY JOIN" not in upper_sql
        assert "SELECT DISTINCT" not in upper_sql
        assert "LIMIT 1 BY PROJECT_ID, TRACE_ID, ID, START_TIME" not in upper_sql
        assert "GROUP BY" not in upper_sql
        assert "ATTRIBUTE_SOURCE.PROJECT_ID ASC" in upper_sql
        assert "OBSERVATION_TYPE ASC" in upper_sql
        assert "SERVICE_NAME ASC" in upper_sql
        assert "TOSTARTOFHOUR(ATTRIBUTE_SOURCE.START_TIME) ASC" in upper_sql
        assert call.timeout_ms <= 1_500
        assert call.settings["max_threads"] == 1
        assert call.settings["optimize_use_projections"] == 0
        assert call.settings["allow_experimental_projection_optimization"] == 0
        assert call.settings["use_skip_indexes"] == 0
        assert call.settings["optimize_read_in_order"] == 1
        assert call.settings["max_block_size"] == 8_192
        assert call.settings["max_memory_usage"] <= 512 * 1024 * 1024
        assert call.settings["max_bytes_to_read"] <= 512 * 1024 * 1024
        assert call.settings["max_rows_to_read"] == 500_000
        assert call.settings["max_result_rows"] == ATTRIBUTE_READ_CANDIDATE_LIMIT + 1
        assert call.settings["timeout_overflow_mode"] == "throw"
    assert all("attributes_extra" not in call.sql for call in typed_calls)
    assert all("attrs_string" not in call.sql for call in json_calls)
    assert all(call.timeout_ms <= 750 for call in json_calls)


def test_streaming_candidate_avoids_datetime_bucket_type_coercion():
    """Bound datetimes stay in native range predicates, never dateDiff."""

    assert "start_time >= %(segment_start)s" in _STRATIFIED_CANDIDATE_SQL
    assert "start_time < %(segment_end)s" in _STRATIFIED_CANDIDATE_SQL
    assert "dateDiff(" not in _STRATIFIED_CANDIDATE_SQL


def test_latest_cardinality_replay_has_one_grouping_clause():
    assert (
        _LATEST_CARDINALITY_SQL.count("GROUP BY project_id, trace_id, id, start_time")
        == 1
    )


def test_v2_executor_reuses_the_process_singleton_ch25_pool(monkeypatch):
    class FakeClient:
        def execute_read(self, query, params, *, timeout_ms, settings):
            return [("ok",)], [("value", "String")], 2.5

    client = FakeClient()
    calls = 0

    def get_client():
        nonlocal calls
        calls += 1
        return client

    monkeypatch.setattr(
        "tracer.services.clickhouse.v2.query_service.get_v2_query_client", get_client
    )

    first = V2AttributeQueryExecutor()
    second = V2AttributeQueryExecutor()
    page = first.execute(
        "SELECT 1",
        {},
        timeout_ms=100,
        settings={"max_threads": 1},
    )

    assert calls == 2
    assert first.client is client
    assert second.client is client
    assert page.data == [{"value": "ok"}]


def test_v2_executor_normalizes_builtin_driver_timeout_to_read_deadline():
    class TimeoutClient:
        def execute_read(self, query, params, *, timeout_ms, settings):
            raise TimeoutError("private driver timeout detail")

    executor = V2AttributeQueryExecutor(client=TimeoutClient())

    with pytest.raises(ReadDeadlineExceeded, match="ClickHouse query timed out"):
        executor.execute(
            "SELECT 1",
            {},
            timeout_ms=100,
            settings={"max_threads": 1},
        )


@pytest.mark.parametrize(
    ("candidate_call", "start_days"),
    [(4, 90), (5, 250)],
    ids=["six-month-band", "one-year-band"],
)
def test_general_exact_key_probe_finds_rare_key_in_later_band(
    candidate_call, start_days
):
    candidate_queries = 0

    def respond(call, _):
        nonlocal candidate_queries
        if "segment_start" in call.params:
            candidate_queries += 1
            return (
                [
                    _candidate(
                        PROJECT_A,
                        "rare-span",
                        start_time=NOW - timedelta(days=start_days),
                    )
                ]
                if candidate_queries == candidate_call
                else []
            )
        return [
            _target_row(
                PROJECT_A,
                "rare-span",
                start_time=NOW - timedelta(days=start_days),
                string="Rejected",
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW, typed_only=True).discover_keys(
        [PROJECT_A], exact_key="final_status"
    )

    assert read.rows == (AttributeKeyRow("final_status", "string", 1),)
    assert read.metadata.query_complete is True
    assert (
        read.metadata.query_window_start
        == adaptive_attribute_windows(NOW)[candidate_call - 1][0]
    )
    assert len(executor.calls) == candidate_call + 1
    for call in executor.calls:
        assert call.params["attribute_key"] == "final_status"
        assert "final_status" not in call.sql
    candidate_sql = next(
        call.sql for call in executor.calls if "segment_start" in call.params
    )
    assert "indexHint(has(mapKeys(attrs_string), %(attribute_key)s))" in candidate_sql
    assert "has(attrs_string.keys, %(attribute_key)s)" in candidate_sql
    assert "length(attrs_string.keys)" not in candidate_sql
    assert "argMin(" not in candidate_sql
    assert "LIMIT 1 BY project_id, trace_id, id, start_time" not in candidate_sql
    assert "GROUP BY" not in candidate_sql
    assert "attribute_source.project_id ASC" in candidate_sql
    assert "toStartOfHour(attribute_source.start_time) ASC" in candidate_sql


def test_exact_key_probe_keyset_pages_past_seed_stale_latest_states() -> None:
    key = "final_status"
    first_page = [
        _candidate(
            PROJECT_A,
            f"stale-{index:04d}",
            trace_id=f"trace-stale-{index:04d}",
            start_time=NOW - timedelta(seconds=index + 1),
        )
        for index in range(ATTRIBUTE_READ_CANDIDATE_LIMIT)
    ]
    live_candidate = _candidate(
        PROJECT_A,
        "rare-live",
        trace_id="trace-rare-live",
        start_time=NOW - timedelta(seconds=ATTRIBUTE_READ_CANDIDATE_LIMIT + 1),
    )
    starts_by_id = {
        str(row["id"]): row["start_time"] for row in [*first_page, live_candidate]
    }

    def respond(call, _):
        if "segment_start" in call.params:
            # The +1 row is a truncation sentinel, not part of the replay.
            return _keyset_candidate_page([*first_page, live_candidate], call)
        candidate_ids = call.params["candidate_ids_0"]
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=(
                    "trace-rare-live" if span_id == "rare-live" else f"trace-{span_id}"
                ),
                start_time=starts_by_id[span_id],
                string="Rejected" if span_id == "rare-live" else None,
            )
            for span_id in candidate_ids
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A], exact_key=key, horizon_days=7)

    assert read.rows == (AttributeKeyRow(key, "string", 1),)
    assert read.metadata.query_complete is True
    # One cheap storage-order probe is replayed first. Because it is entirely
    # stale, continuation restarts at ordered page one, then advances from that
    # ordered page's own cursor.
    assert read.metadata.query_count == 6
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert len(candidate_calls) == 3
    assert "attribute_source.project_id ASC" in candidate_calls[0].sql
    assert "LIMIT 1 BY" not in candidate_calls[0].sql
    assert "candidate_before_start_us" not in candidate_calls[0].params
    assert "toString(attribute_source.project_id) DESC" in candidate_calls[1].sql
    assert "candidate_before_start_us" not in candidate_calls[1].params
    cursor = candidate_calls[2].params
    assert cursor["candidate_before_id"] == first_page[-1]["id"]
    assert cursor["candidate_before_trace_id"] == first_page[-1]["trace_id"]
    assert cursor["candidate_before_project_id"] == PROJECT_A
    assert "candidate_before_start_us" in cursor
    assert (
        "toString(attribute_source.project_id) "
        "< %(candidate_before_project_id)s" in candidate_calls[2].sql
    )
    assert "project_id < toUUID(%(candidate_before_project_id)s)" not in (
        candidate_calls[2].sql
    )
    assert "NOT IN" not in candidate_calls[2].sql
    assert all(key not in call.sql for call in executor.calls)
    assert all(call.params["attribute_key"] == key for call in executor.calls)


def test_storage_order_sample_is_never_reused_as_descending_keyset_cursor() -> None:
    """A differently ordered sample cannot skip a newer live fallback row."""

    key = "final_status"
    storage_sample = [
        _candidate(
            PROJECT_A,
            f"storage-stale-{index:04d}",
            trace_id=f"trace-storage-stale-{index:04d}",
            start_time=NOW - timedelta(days=3, seconds=index + 1),
        )
        for index in range(ATTRIBUTE_READ_CANDIDATE_LIMIT + 1)
    ]
    live = _candidate(
        PROJECT_A,
        "newer-live",
        trace_id="trace-newer-live",
        start_time=NOW - timedelta(days=1),
    )
    by_id = {str(row["id"]): row for row in [*storage_sample, live]}

    def respond(call, _):
        if "segment_start" in call.params:
            if "LIMIT 1 BY" not in call.sql:
                # Deliberately unlike the descending fallback order. Seeding a
                # descending keyset from this sample's last row would skip live.
                return storage_sample
            return _keyset_candidate_page([*storage_sample, live], call)
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=by_id[span_id]["trace_id"],
                start_time=by_id[span_id]["start_time"],
                string="Rejected" if span_id == "newer-live" else None,
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A], exact_key=key, horizon_days=7)

    assert read.rows == (AttributeKeyRow(key, "string", 1),)
    assert read.metadata.query_error_code == "sample_limit"
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert len(candidate_calls) == 2
    assert "LIMIT 1 BY" not in candidate_calls[0].sql
    assert "LIMIT 1 BY" in candidate_calls[1].sql
    assert "candidate_before_start_us" not in candidate_calls[1].params


def test_exact_key_keyset_is_bounded_and_lossless_past_page_cap():
    key = "final_status"
    physical_count = 5 * ATTRIBUTE_READ_CANDIDATE_LIMIT + 1
    candidates = [
        _candidate(
            PROJECT_A,
            f"shared-{index // 2:04d}",
            trace_id=f"trace-{index:04d}",
            # Four identities share every timestamp; two also share an id.
            start_time=NOW - timedelta(seconds=index // 4 + 1),
        )
        for index in range(physical_count)
    ]
    ordered = sorted(candidates, key=_candidate_key, reverse=True)
    live_identity = (
        ordered[-1]["trace_id"],
        ordered[-1]["id"],
        _unix_microseconds(ordered[-1]["start_time"]),
    )
    by_identity = {
        (row["trace_id"], row["id"], _unix_microseconds(row["start_time"])): row
        for row in candidates
    }
    replayed: list[tuple[str, str, int]] = []

    def respond(call, _):
        if "segment_start" in call.params:
            # A duplicate raw version of one physical span must not create a
            # second candidate after LIMIT 1 BY.
            return _keyset_candidate_page([*candidates, candidates[17]], call)
        encoded = list(call.params["candidate_physical_identities_0"])
        replayed.extend(encoded)
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=trace_id,
                start_time=by_identity[(trace_id, span_id, start_us)]["start_time"],
                string="Rejected" if identity == live_identity else None,
            )
            for identity in encoded
            for trace_id, span_id, start_us in [identity]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A], exact_key=key, horizon_days=7)

    assert read.rows == (AttributeKeyRow(key, "string", 1),)
    assert read.metadata.query_complete is True
    assert read.metadata.query_count == 14
    # The storage-order sample is intentionally replayed before the ordered
    # restart. Identity-keyed merging prevents duplicate counts.
    assert len(replayed) == physical_count + ATTRIBUTE_READ_CANDIDATE_LIMIT
    assert len(set(replayed)) == physical_count
    assert set(replayed) == set(by_identity)

    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert len(candidate_calls) == 7
    assert "LIMIT 1 BY" not in candidate_calls[0].sql
    assert "candidate_before_start_us" not in candidate_calls[1].params
    assert all(
        "excluded_candidate_identities" not in call.params for call in candidate_calls
    )
    assert all(" NOT IN " not in call.sql for call in candidate_calls)
    assert (
        max(len(call.sql) + len(repr(call.params)) for call in candidate_calls) < 8_192
    )
    continuation_sizes = [
        len(call.sql) + len(repr(call.params)) for call in candidate_calls[2:]
    ]
    assert max(continuation_sizes) - min(continuation_sizes) < 64


def test_exact_key_continuation_stops_at_hard_page_cap_and_degrades() -> None:
    candidate_page = 0
    starts_by_id: dict[str, datetime] = {}

    def respond(call, _):
        nonlocal candidate_page
        if "segment_start" in call.params:
            page = candidate_page
            candidate_page += 1
            rows = [
                _candidate(
                    PROJECT_A,
                    f"stale-{page:02d}-{index:04d}",
                    trace_id=f"trace-stale-{page:02d}-{index:04d}",
                    start_time=NOW - timedelta(seconds=page * 1_000 + index + 1),
                )
                for index in range(ATTRIBUTE_READ_CANDIDATE_LIMIT + 1)
            ]
            starts_by_id.update((str(row["id"]), row["start_time"]) for row in rows)
            return rows
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=f"trace-{span_id}",
                start_time=starts_by_id[span_id],
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A], exact_key="final_status")

    assert read.rows == ()
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "sample_limit"
    assert candidate_page == (
        len(adaptive_attribute_windows(NOW))
        + ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT
    )
    assert read.metadata.query_count == 2 * candidate_page
    final_candidate_call = [
        call for call in executor.calls if "segment_start" in call.params
    ][-1]
    assert set(final_candidate_call.params) >= {
        "candidate_before_start_us",
        "candidate_before_id",
        "candidate_before_trace_id",
        "candidate_before_project_id",
    }
    # Keyset state stays constant-size when the candidate sample size changes.
    assert len(repr(final_candidate_call.params)) < 2_048
    assert "NOT IN" not in final_candidate_call.sql


def test_exact_key_page_cap_is_global_across_horizon_bands() -> None:
    candidate_page = 0
    starts_by_id: dict[str, datetime] = {}

    def respond(call, _):
        nonlocal candidate_page
        if "segment_start" in call.params:
            page = candidate_page
            candidate_page += 1
            segment_end = call.params["segment_end"]
            rows = [
                _candidate(
                    PROJECT_A,
                    f"stale-{page:02d}-{index:04d}",
                    trace_id=f"trace-stale-{page:02d}-{index:04d}",
                    start_time=segment_end - timedelta(seconds=index + 1),
                )
                for index in range(ATTRIBUTE_READ_CANDIDATE_LIMIT + 1)
            ]
            starts_by_id.update((str(row["id"]), row["start_time"]) for row in rows)
            return rows
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=f"trace-{span_id}",
                start_time=starts_by_id[span_id],
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A], exact_key="final_status")

    assert read.rows == ()
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"
    assert candidate_page == (
        len(adaptive_attribute_windows(NOW))
        + ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT
    )
    assert read.metadata.query_count == 2 * candidate_page
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    # Every horizon gets its cheap first probe before the six-page ordered
    # continuation budget is shared round-robin across them.
    assert len({call.params["segment_start"] for call in candidate_calls}) == 5
    assert adaptive_attribute_windows(NOW)[-1][0] in {
        call.params["segment_start"] for call in candidate_calls
    }
    assert sum("LIMIT 1 BY" in call.sql for call in candidate_calls) == (
        ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT
    )


def test_browse_inventory_stops_after_first_verified_dense_sample():
    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]:
                return []
            return [
                _candidate(PROJECT_A, f"sampled-span-{index:04d}")
                for index in range(ATTRIBUTE_READ_CANDIDATE_LIMIT + 1)
            ]
        return [
            {
                "project_id": PROJECT_A,
                "id": span_id,
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "trace_id": f"trace-{PROJECT_A}-{span_id}",
                "trace_session_id": "",
                "parent_span_id": "",
                "string_keys": ["final_status"],
                "number_keys": [],
                "boolean_keys": [],
                "attributes_extra": "{}",
            }
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A])

    assert read.rows == (
        AttributeKeyRow("final_status", "string", ATTRIBUTE_READ_CANDIDATE_LIMIT),
    )
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    assert read.metadata.query_count == 2
    assert read.metadata.query_window_start == NOW - timedelta(days=7)
    candidate_call = next(
        call for call in executor.calls if "segment_start" in call.params
    )
    assert "LIMIT 1 BY project_id, trace_id, id, start_time" not in candidate_call.sql
    assert "GROUP BY" not in candidate_call.sql
    assert "attribute_source.project_id ASC" in candidate_call.sql
    assert candidate_call.settings["optimize_read_in_order"] == 1
    assert candidate_call.params["candidate_limit"] == (
        ATTRIBUTE_READ_CANDIDATE_LIMIT + 1
    )


def test_latest_replay_uses_index_pruning_and_exact_physical_identities():
    candidates = [
        _candidate(PROJECT_A, "duplicate-id"),
        _candidate(PROJECT_B, "duplicate-id"),
        _candidate(PROJECT_A, "string-second"),
        _candidate(PROJECT_A, "number"),
        _candidate(PROJECT_A, "boolean"),
        _candidate(PROJECT_A, "legacy-string"),
        _candidate(PROJECT_A, "legacy-number"),
        _candidate(PROJECT_A, "legacy-boolean"),
        _candidate(PROJECT_A, "cleared"),
        _candidate(PROJECT_A, "legacy-object"),
    ]
    latest = [
        # Same id in two projects: one live value and one opposite tombstone.
        _target_row(PROJECT_A, "duplicate-id", string="Rejected"),
        _target_row(
            PROJECT_B,
            "duplicate-id",
            is_deleted=1,
            string="must-not-resurrect",
        ),
        _target_row(PROJECT_A, "string-second", string="Rejected"),
        _target_row(PROJECT_A, "number", number=42),
        _target_row(PROJECT_A, "boolean", boolean=True),
        _target_row(PROJECT_A, "legacy-string", legacy_raw='"legacy"'),
        _target_row(PROJECT_A, "legacy-number", legacy_raw="7"),
        _target_row(PROJECT_A, "legacy-boolean", legacy_raw="false"),
        _target_row(PROJECT_A, "cleared"),
        _target_row(PROJECT_A, "legacy-object", legacy_raw='{"x": 1}'),
    ]
    typed_candidate_ids = {
        "duplicate-id",
        "string-second",
        "number",
        "boolean",
        "cleared",
    }
    json_candidate_ids = {
        "legacy-string",
        "legacy-number",
        "legacy-boolean",
        "legacy-object",
    }

    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]:
                return []
            selected_ids = (
                json_candidate_ids
                if "JSONHas(attributes_extra" in call.sql
                else typed_candidate_ids
            )
            return [row for row in candidates if row["id"] in selected_ids]

        wanted: set[tuple[str, str, str, int]] = set()
        index = 0
        while f"candidate_project_{index}" in call.params:
            project_id = call.params[f"candidate_project_{index}"]
            wanted.update(
                (project_id, trace_id, span_id, start_us)
                for trace_id, span_id, start_us in call.params[
                    f"candidate_physical_identities_{index}"
                ]
            )
            index += 1
        return [
            row
            for row in latest
            if (
                row["project_id"],
                row["trace_id"],
                row["id"],
                _unix_microseconds(row["start_time"]),
            )
            in wanted
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A, PROJECT_B], "final_status"
    )

    assert read.rows == (
        AttributeValueRow("Rejected", "string", 2),
        AttributeValueRow(42.0, "number", 1),
        AttributeValueRow(True, "boolean", 1),
    )
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    replay = next(call for call in executor.calls if "segment_start" not in call.params)
    replay_prewhere = replay.sql.split("PREWHERE", 1)[1].split("GROUP BY", 1)[0]
    assert "start_time >=" not in replay_prewhere
    assert "start_time <" not in replay_prewhere
    assert "toString(project_id)" not in replay_prewhere
    assert "toString(id)" not in replay_prewhere
    assert "project_id = toUUID(%(candidate_project_0)s)" in replay_prewhere
    assert "project_id = toUUID(%(candidate_project_1)s)" in replay_prewhere
    assert "id IN %(candidate_ids_0)s" in replay_prewhere
    assert "trace_id IN %(candidate_trace_ids_0)s" in replay_prewhere
    assert "toDate(start_time) IN %(candidate_dates_0)s" in replay_prewhere
    assert (
        "(trace_id, id, toUnixTimestamp64Micro(start_time)) "
        "IN %(candidate_physical_identities_0)s" in replay_prewhere
    )
    assert replay.params["candidate_project_0"] == PROJECT_A
    assert replay.params["candidate_project_1"] == PROJECT_B
    assert "duplicate-id" in replay.params["candidate_ids_0"]
    assert replay.params["candidate_ids_1"] == ("duplicate-id",)
    assert replay.params["candidate_trace_ids_1"] == (
        f"trace-{PROJECT_B}-duplicate-id",
    )
    assert replay.params["candidate_dates_1"] == ((NOW - timedelta(days=1)).date(),)
    assert replay.params["candidate_physical_identities_1"] == (
        (
            f"trace-{PROJECT_B}-duplicate-id",
            "duplicate-id",
            _unix_microseconds(NOW - timedelta(days=1)),
        ),
    )
    assert PROJECT_A not in replay.sql
    assert PROJECT_B not in replay.sql
    assert replay.settings["max_threads"] == 1
    assert replay.settings["max_bytes_to_read"] == 512 * 1024 * 1024
    assert replay.settings["max_result_rows"] == 6
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert all(
        not (
            "mapContains(attrs_string" in call.sql
            and "JSONHas(attributes_extra" in call.sql
        )
        for call in candidate_calls
    )


def test_reused_span_ids_keep_trace_and_start_time_scoped_tombstones():
    first = NOW - timedelta(days=1)
    second = first + timedelta(minutes=1)
    candidates = [
        _candidate(
            PROJECT_A,
            "shared",
            trace_id="trace-a",
            start_time=first,
        ),
        _candidate(
            PROJECT_A,
            "shared",
            trace_id="trace-b",
            start_time=first,
        ),
        _candidate(
            PROJECT_A,
            "shared",
            trace_id="trace-a",
            start_time=second,
        ),
        _candidate(
            PROJECT_A,
            "empty-trace",
            trace_id="",
            start_time=first,
        ),
    ]
    latest = [
        _target_row(
            PROJECT_A,
            "shared",
            trace_id="trace-a",
            start_time=first,
            string="Rejected",
        ),
        _target_row(
            PROJECT_A,
            "shared",
            trace_id="trace-b",
            start_time=first,
            is_deleted=1,
            string="must-not-resurrect",
        ),
        _target_row(
            PROJECT_A,
            "shared",
            trace_id="trace-a",
            start_time=second,
            string="Rejected",
        ),
        _target_row(
            PROJECT_A,
            "empty-trace",
            trace_id="",
            start_time=first,
            string="Rejected",
        ),
    ]
    emitted = False

    def respond(call, _):
        nonlocal emitted
        if "segment_start" in call.params:
            if emitted:
                return []
            emitted = True
            return candidates
        wanted = set(call.params["candidate_physical_identities_0"])
        return [
            row
            for row in latest
            if (
                row["trace_id"],
                row["id"],
                _unix_microseconds(row["start_time"]),
            )
            in wanted
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values([PROJECT_A], "final_status")

    assert read.rows == (AttributeValueRow("Rejected", "string", 3),)
    replay = next(call for call in executor.calls if "segment_start" not in call.params)
    assert replay.params["candidate_ids_0"] == ("shared", "empty-trace")
    assert replay.params["candidate_trace_ids_0"] == ("trace-a", "trace-b", "")
    assert replay.params["candidate_physical_identities_0"] == (
        ("trace-a", "shared", _unix_microseconds(first)),
        ("trace-b", "shared", _unix_microseconds(first)),
        ("trace-a", "shared", _unix_microseconds(second)),
        ("", "empty-trace", _unix_microseconds(first)),
    )
    assert "GROUP BY project_id, trace_id, id, start_time" in replay.sql


def test_detail_read_uses_latest_versions_and_does_not_resurrect_tombstones():
    """Candidates can come from old live versions; only replayed state counts."""

    emitted = False

    def respond(call, _):
        nonlocal emitted
        if "segment_start" in call.params:
            if emitted:
                return []
            emitted = True
            return [
                _candidate(PROJECT_A, "later-deleted"),
                _candidate(PROJECT_A, "later-updated"),
                _candidate(PROJECT_A, "still-live"),
            ]
        latest = [
            _target_row(
                PROJECT_A,
                "later-deleted",
                is_deleted=1,
                string="stale-value",
            ),
            _target_row(PROJECT_A, "later-updated", string="new-value"),
            _target_row(PROJECT_A, "still-live", string="new-value"),
        ]
        wanted = set(call.params["candidate_physical_identities_0"])
        return [
            row
            for row in latest
            if (
                row["trace_id"],
                row["id"],
                _unix_microseconds(row["start_time"]),
            )
            in wanted
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_detail([PROJECT_A], "final_status")

    assert read == AttributeDetailRead(
        "string",
        (AttributeValueRow("new-value", "string", 2),),
        read.metadata,
    )
    assert read.metadata.query_complete is True
    replay = next(call for call in executor.calls if "segment_start" not in call.params)
    assert "argMax(" in replay.sql
    assert "_version" in replay.sql
    assert " FINAL " not in f" {replay.sql.upper()} "
    assert replay.settings["max_rows_to_read"] == 500_000
    assert replay.settings["max_bytes_to_read"] == 512 * 1024 * 1024


def test_typed_map_key_browse_and_legacy_json_scalar_precedence():
    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]:
                return []
            return [_candidate(PROJECT_A, "wide")]
        return [
            {
                "project_id": PROJECT_A,
                "id": "wide",
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "trace_id": f"trace-{PROJECT_A}-wide",
                "trace_session_id": "",
                "parent_span_id": "",
                "string_keys": ["alpha", "shared"],
                "number_keys": ["number", "shared"],
                "boolean_keys": ["enabled"],
                "attributes_extra": json.dumps(
                    {
                        "legacy": "x",
                        "legacy_number": 2,
                        "legacy_boolean": True,
                        "object": {"ignored": True},
                        "array": [1],
                        "null": None,
                        "shared": 10,
                    }
                ),
            }
        ]

    read = AttributeReadSelector(RecordingExecutor(respond), now=NOW).discover_keys(
        [PROJECT_A]
    )

    assert {(row.key, row.type) for row in read.rows} == {
        ("alpha", "string"),
        ("shared", "string"),
        ("number", "number"),
        ("enabled", "boolean"),
        ("legacy", "string"),
        ("legacy_number", "number"),
        ("legacy_boolean", "boolean"),
    }
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"


def test_exact_structured_json_key_is_not_reported_as_complete_empty():
    def respond(call, _):
        if "segment_start" in call.params:
            if (
                call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]
                or "JSONHas(attributes_extra" not in call.sql
            ):
                return []
            return [_candidate(PROJECT_A, "structured")]
        return [
            _target_row(
                PROJECT_A,
                "structured",
                legacy_raw='{"nested":true}',
            )
        ]

    selector = AttributeReadSelector(RecordingExecutor(respond), now=NOW)
    key_read = selector.discover_keys([PROJECT_A], exact_key="structured")

    assert key_read.rows == ()
    assert key_read.metadata.query_complete is False
    assert key_read.metadata.query_error_code == "sample_limit"


def test_structured_json_value_picker_is_explicitly_degraded():
    def respond(call, _):
        if "segment_start" in call.params:
            if (
                call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]
                or "candidate_version" in call.sql
            ):
                return []
            return [_candidate(PROJECT_A, "structured")]
        return [
            _target_row(
                PROJECT_A,
                "structured",
                legacy_raw='["one","two"]',
            )
        ]

    read = AttributeReadSelector(RecordingExecutor(respond), now=NOW).read_values(
        [PROJECT_A], "structured"
    )

    assert read.rows == ()
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"


def test_array_filter_picker_surfaces_json_array_and_preserves_typed_maps():
    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]:
                return []
            return [_candidate(PROJECT_A, "array-and-map")]
        return [
            {
                "project_id": PROJECT_A,
                "id": "array-and-map",
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "trace_id": f"trace-{PROJECT_A}-array-and-map",
                "trace_session_id": "",
                "parent_span_id": "",
                "string_keys": ["final_status", "shared"],
                "number_keys": [],
                "boolean_keys": [],
                "attributes_extra": json.dumps(
                    {
                        "json_array": ["one", 2, True],
                        "json_scalar": "not-filterable-from-overflow",
                        "json_object": {"nested": True},
                        "shared": ["typed-map-wins"],
                    }
                ),
            }
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
        json_attribute_mode="arrays",
    ).discover_keys([PROJECT_A])

    assert {(row.key, row.type) for row in read.rows} == {
        ("final_status", "string"),
        ("shared", "string"),
        ("json_array", "array"),
    }
    assert read.metadata.query_complete is True
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert any("attributes_extra NOT IN" in call.sql for call in candidate_calls)
    assert any("length(attrs_string.keys)" in call.sql for call in candidate_calls)
    assert all(
        not (
            "attributes_extra NOT IN" in call.sql
            and "length(attrs_string.keys)" in call.sql
        )
        for call in candidate_calls
    )


def test_array_filter_picker_does_not_advertise_json_object_as_filterable():
    def respond(call, _):
        if "segment_start" in call.params:
            if (
                call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]
                or "JSONHas(attributes_extra" not in call.sql
            ):
                return []
            return [_candidate(PROJECT_A, "object-only")]
        return [
            _target_row(
                PROJECT_A,
                "object-only",
                legacy_raw='{"nested":true}',
            )
        ]

    read = AttributeReadSelector(
        RecordingExecutor(respond),
        now=NOW,
        typed_only=True,
        json_attribute_mode="arrays",
    ).discover_keys([PROJECT_A], exact_key="json_object")

    assert read.rows == ()
    assert read.metadata.query_complete is True


def test_eval_mapping_inventory_includes_all_json_value_families():
    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]:
                return []
            return [_candidate(PROJECT_A, "eval-json")]
        return [
            {
                "project_id": PROJECT_A,
                "id": "eval-json",
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "trace_id": f"trace-{PROJECT_A}-eval-json",
                "trace_session_id": "",
                "parent_span_id": "",
                "string_keys": ["typed_map"],
                "number_keys": [],
                "boolean_keys": [],
                "attributes_extra": json.dumps(
                    {
                        "json_scalar": "value",
                        "json_array": ["one"],
                        "json_object": {"nested": True},
                        "json_null": None,
                    }
                ),
            }
        ]

    read = AttributeReadSelector(
        RecordingExecutor(respond),
        now=NOW,
        typed_only=True,
        json_attribute_mode="all",
    ).discover_keys([PROJECT_A])

    assert {(row.key, row.type) for row in read.rows} == {
        ("typed_map", "string"),
        ("json_scalar", "string"),
        ("json_array", "array"),
        ("json_object", "json"),
        ("json_null", "json"),
    }
    assert read.metadata.query_complete is True


def test_array_value_picker_flattens_supported_json_scalars_type_exactly():
    def respond(call, _):
        if "segment_start" in call.params:
            if (
                call.params["segment_start"] != adaptive_attribute_windows(NOW)[0][0]
                or "candidate_version" in call.sql
            ):
                return []
            return [
                _candidate(PROJECT_A, "array-one"),
                _candidate(PROJECT_A, "array-two"),
            ]
        return [
            _target_row(
                PROJECT_A,
                "array-one",
                legacy_raw='["one",1,1.0,true,"one",null,{"skip":1}]',
            ),
            _target_row(
                PROJECT_A,
                "array-two",
                legacy_raw='["one",18446744073709551615,false,["skip"]]',
            ),
        ]

    read = AttributeReadSelector(
        RecordingExecutor(respond),
        now=NOW,
        typed_only=True,
        json_attribute_mode="arrays",
    ).read_values([PROJECT_A], "json_array")

    by_value = {(type(row.value).__name__, row.value): row.count for row in read.rows}
    assert by_value == {
        ("str", "one"): 2,
        ("int", 1): 1,
        ("float", 1.0): 1,
        ("bool", True): 1,
        ("int", 18446744073709551615): 1,
        ("bool", False): 1,
    }
    assert all(row.type == "array" for row in read.rows)
    assert read.metadata.query_complete is True


def test_native_value_precedence_is_string_then_number_then_boolean_then_json():
    row = _target_row(
        PROJECT_A,
        "precedence",
        string="native-string",
        number=99,
        boolean=True,
        legacy_raw='"legacy"',
    )

    assert AttributeReadSelector._decode_target_value(row) == (
        "string",
        "native-string",
    )


def test_typed_only_picker_never_offers_unfilterable_attributes_extra_scalars():
    emitted = False

    def respond(call, _):
        nonlocal emitted
        if "segment_start" in call.params:
            if emitted:
                return []
            emitted = True
            return [_candidate(PROJECT_A, "typed-and-legacy")]
        return [
            {
                "project_id": PROJECT_A,
                "id": "typed-and-legacy",
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "trace_id": f"trace-{PROJECT_A}-typed-and-legacy",
                "trace_session_id": "",
                "parent_span_id": "",
                "string_keys": ["final_status"],
                "number_keys": [],
                "boolean_keys": [],
                "attributes_extra": json.dumps({"json_only": "hidden"}),
            }
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A])

    assert {(row.key, row.type) for row in read.rows} == {("final_status", "string")}
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert candidate_calls
    assert all("attributes_extra" not in call.sql for call in candidate_calls)
    assert all("JSONHas" not in call.sql for call in candidate_calls)
    replay = next(call for call in executor.calls if "segment_start" not in call.params)
    assert "attributes_extra" not in replay.sql
    assert "JSONHas" not in replay.sql
    assert "trace_session_id" not in replay.sql
    assert "parent_span_id" not in replay.sql


def test_typed_only_value_picker_ignores_legacy_scalar_and_avoids_json_seed():
    emitted = False

    def respond(call, _):
        nonlocal emitted
        if "segment_start" in call.params:
            if emitted:
                return []
            emitted = True
            return [_candidate(PROJECT_A, "legacy-only")]
        return [_target_row(PROJECT_A, "legacy-only", legacy_raw='"hidden"')]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values([PROJECT_A], "json_only")

    assert read.rows == ()
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert candidate_calls
    assert all("JSONHas" not in call.sql for call in candidate_calls)
    assert all("attributes_extra" not in call.sql for call in candidate_calls)
    replay = next(call for call in executor.calls if "segment_start" not in call.params)
    assert "JSONHas" not in replay.sql
    assert "JSONExtractRaw" not in replay.sql
    assert "attributes_extra" not in replay.sql
    assert "trace_session_id" not in replay.sql
    assert "parent_span_id" not in replay.sql


def test_value_search_treats_unicode_like_metacharacters_as_literals():
    emitted = False

    def respond(call, _):
        nonlocal emitted
        if "segment_start" in call.params:
            if emitted:
                return []
            emitted = True
            return [_candidate(PROJECT_A, "literal")]
        return [
            _target_row(
                PROJECT_A,
                "literal",
                string="prefix %_\\路径 suffix",
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A], "customer.quote'key", search="%_\\路径"
    )

    assert [row.value for row in read.rows] == ["prefix %_\\路径 suffix"]
    assert all("LIKE" not in call.sql.upper() for call in executor.calls)
    assert all("%_\\路径" not in call.sql for call in executor.calls)
    assert all("customer.quote'key" not in call.sql for call in executor.calls)
    assert all(
        call.params["attribute_key"] == "customer.quote'key" for call in executor.calls
        if "attribute_key" in call.params
    )
    certificate = next(
        call
        for call in executor.calls
        if "max(_version) AS latest_version" in call.sql
    )
    assert "attribute_key" not in certificate.params
    assert all(
        "attribute_search" not in call.params
        for call in executor.calls
        if "segment_start" in call.params
    )


def test_ascii_value_search_uses_key_only_typed_candidates_and_exact_replay():
    candidate_queries = 0

    def respond(call, _):
        nonlocal candidate_queries
        if "segment_start" in call.params:
            candidate_queries += 1
            return (
                [
                    _candidate(
                        PROJECT_A,
                        "rare-value",
                        start_time=NOW - timedelta(days=250),
                    )
                ]
                if candidate_queries == 5
                else []
            )
        return [
            _target_row(
                PROJECT_A,
                "rare-value",
                start_time=NOW - timedelta(days=250),
                string="prefix NeEdLe%_\\path suffix",
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW, typed_only=True).read_values(
        [PROJECT_A],
        "rare.search.key",
        search="needle%_\\path",
    )

    assert [row.value for row in read.rows] == ["prefix NeEdLe%_\\path suffix"]
    candidates = [call for call in executor.calls if "segment_start" in call.params]
    assert len(candidates) == 5
    assert all("attribute_search" not in call.params for call in candidates)
    assert all("positionCaseInsensitiveUTF8" not in call.sql for call in candidates)
    assert all(
        "indexHint(has(mapKeys(attrs_string), %(attribute_key)s))" in call.sql
        and "has(attrs_string.keys, %(attribute_key)s)" in call.sql
        for call in candidates
    )
    assert all("LIKE" not in call.sql.upper() for call in candidates)
    assert all("needle%_\\path" not in call.sql for call in candidates)


def test_value_read_stops_after_first_verified_dense_sample():
    recent = [
        _candidate(
            PROJECT_A,
            f"recent-{index:04d}",
            trace_id=f"trace-recent-{index:04d}",
            start_time=NOW - timedelta(seconds=index + 1),
        )
        for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1)
    ]
    older = _candidate(
        PROJECT_A,
        "older-distinct",
        trace_id="trace-older-distinct",
        start_time=NOW - timedelta(days=250),
    )
    rows_by_id = {str(row["id"]): row for row in [*recent, older]}
    recent_start = adaptive_attribute_windows(NOW)[0][0]
    oldest_start = adaptive_attribute_windows(NOW)[-1][0]

    def respond(call, _):
        if "segment_start" in call.params:
            segment_start = call.params["segment_start"]
            if segment_start == recent_start:
                return _keyset_candidate_page(recent, call)
            if segment_start == oldest_start:
                return [older]
            return []
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=rows_by_id[span_id]["trace_id"],
                start_time=rows_by_id[span_id]["start_time"],
                string=(
                    "older-value" if span_id == "older-distinct" else "recent-value"
                ),
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values([PROJECT_A], "final_status")

    assert read.rows == (
        AttributeValueRow(
            "recent-value", "string", ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT
        ),
    )
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"
    assert read.metadata.query_window_start == NOW - timedelta(days=7)
    recent_candidate_calls = [
        call
        for call in executor.calls
        if call.params.get("segment_start") == recent_start
    ]
    assert len(recent_candidate_calls) == 1
    assert (
        recent_candidate_calls[0].params["candidate_limit"]
        == ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1
    )
    assert "LIMIT 1 BY" not in recent_candidate_calls[0].sql


def test_dense_typed_value_sample_stops_before_json_lane():
    candidates = [
        _candidate(
            PROJECT_A,
            f"typed-{index:03d}",
            trace_id=f"trace-typed-{index:03d}",
            start_time=NOW - timedelta(hours=1, seconds=index),
        )
        for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1)
    ]
    by_id = {str(row["id"]): row for row in candidates}

    def respond(call, _):
        if "segment_start" in call.params:
            assert "JSONHas(attributes_extra" not in call.sql
            return candidates
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=by_id[span_id]["trace_id"],
                start_time=by_id[span_id]["start_time"],
                string="Rejected",
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
        json_attribute_mode="arrays",
    ).read_values(
        [PROJECT_A],
        "final_status",
        window_start=NOW - timedelta(days=7),
        window_end=NOW,
    )

    assert read.rows == (
        AttributeValueRow(
            "Rejected",
            "string",
            ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT,
        ),
    )
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    assert read.metadata.query_count == 3
    certificate_call = executor.calls[1]
    assert "max(_version) AS latest_version" in certificate_call.sql
    assert "attrs_" not in certificate_call.sql
    assert "attributes_extra" not in certificate_call.sql


def test_typed_value_version_certificate_narrows_hydration_to_current_live_rows():
    candidates = [
        _candidate(
            PROJECT_A,
            span_id,
            trace_id=f"trace-{span_id}",
            start_time=NOW - timedelta(hours=1, seconds=index),
        )
        for index, span_id in enumerate(
            (
                "active-string",
                "active-number",
                "active-boolean",
                "cleared-one",
                "cleared-two",
                "tombstoned-one",
                "tombstoned-two",
                "cleared-three",
                "truncation-sentinel",
            )
        )
    ]
    by_id = {str(row["id"]): row for row in candidates}
    active_ids = {"active-string", "active-number", "active-boolean"}
    tombstoned_ids = {"tombstoned-one", "tombstoned-two"}

    def certificate_row(span_id: str) -> dict[str, Any]:
        candidate = by_id[span_id]
        return {
            "project_id": PROJECT_A,
            "trace_id": candidate["trace_id"],
            "id": span_id,
            "start_time": candidate["start_time"],
            "is_deleted": int(span_id in tombstoned_ids),
            "latest_version": 1 if span_id in active_ids else 2,
        }

    def respond(call, _):
        if "segment_start" in call.params:
            return candidates
        requested_ids = call.params["candidate_ids_0"]
        if "max(_version) AS latest_version" in call.sql:
            return [certificate_row(span_id) for span_id in requested_ids]
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=by_id[span_id]["trace_id"],
                start_time=by_id[span_id]["start_time"],
                string="Rejected" if span_id == "active-string" else None,
                number=42 if span_id == "active-number" else None,
                boolean=True if span_id == "active-boolean" else None,
            )
            for span_id in requested_ids
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values(
        [PROJECT_A],
        "final_status",
        window_start=NOW - timedelta(days=1),
        window_end=NOW,
    )

    assert {(row.value, row.type, row.count) for row in read.rows} == {
        ("Rejected", "string", 1),
        (42.0, "number", 1),
        (True, "boolean", 1),
    }
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    assert read.metadata.query_count == 3

    candidate_call = executor.calls[0]
    certificate_call = executor.calls[1]
    hydration_call = executor.calls[2]
    assert "toUInt64(_version) AS candidate_version" in candidate_call.sql
    for family in ("string", "number", "bool"):
        assert f"attrs_{family}[%(attribute_key)s]" not in candidate_call.sql
        assert f"attrs_{family}" not in certificate_call.sql
        assert f"attrs_{family}[%(attribute_key)s]" in hydration_call.sql
    assert "attributes_extra" not in candidate_call.sql
    assert "attributes_extra" not in certificate_call.sql
    assert "attribute_key" not in certificate_call.params

    expected_active_identities = {
        (
            by_id[span_id]["trace_id"],
            span_id,
            _unix_microseconds(by_id[span_id]["start_time"]),
        )
        for span_id in active_ids
    }
    assert (
        set(hydration_call.params["candidate_physical_identities_0"])
        == expected_active_identities
    )
    assert set(hydration_call.params["candidate_ids_0"]) == active_ids


def test_typed_value_candidate_deduplicates_to_highest_raw_version():
    older = _candidate(
        PROJECT_A,
        "duplicate-version",
        trace_id="trace-duplicate-version",
        start_time=NOW - timedelta(hours=1),
        candidate_version=2,
    )
    newer = {**older, "candidate_version": 7}

    def respond(call, _):
        if "segment_start" in call.params:
            return [older, newer]
        if "max(_version) AS latest_version" in call.sql:
            assert call.params["candidate_ids_0"] == ("duplicate-version",)
            return [
                _target_row(
                    PROJECT_A,
                    "duplicate-version",
                    trace_id="trace-duplicate-version",
                    start_time=older["start_time"],
                    latest_version=7,
                )
            ]
        return [
            _target_row(
                PROJECT_A,
                "duplicate-version",
                trace_id="trace-duplicate-version",
                start_time=older["start_time"],
                string="Rejected",
                latest_version=7,
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values(
        [PROJECT_A],
        "final_status",
        window_start=NOW - timedelta(days=1),
        window_end=NOW,
    )

    assert read.rows == (AttributeValueRow("Rejected", "string", 1),)
    assert read.metadata.query_complete is True
    assert read.metadata.query_count == 3
    assert len(executor.calls) == 3


def test_typed_value_all_stale_versions_skip_value_hydration():
    candidates = [
        _candidate(
            PROJECT_A,
            span_id,
            trace_id=f"trace-{span_id}",
            start_time=NOW - timedelta(hours=1, seconds=index),
        )
        for index, span_id in enumerate(("cleared", "tombstoned"))
    ]
    by_id = {str(row["id"]): row for row in candidates}

    def respond(call, _):
        if "segment_start" in call.params:
            return candidates
        return [
            {
                "project_id": PROJECT_A,
                "trace_id": by_id[span_id]["trace_id"],
                "id": span_id,
                "start_time": by_id[span_id]["start_time"],
                "is_deleted": int(span_id == "tombstoned"),
                "latest_version": 2,
            }
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values(
        [PROJECT_A],
        "final_status",
        window_start=NOW - timedelta(days=1),
        window_end=NOW,
    )

    assert read.rows == ()
    assert read.metadata.query_status == "complete"
    assert read.metadata.query_count == 2
    assert len(executor.calls) == 2
    certificate_call = executor.calls[1]
    assert "max(_version) AS latest_version" in certificate_call.sql
    assert "attrs_" not in certificate_call.sql
    assert "attributes_extra" not in certificate_call.sql


def test_explicit_window_json_value_runs_after_all_typed_bands_are_empty():
    json_emitted = False

    def respond(call, _):
        nonlocal json_emitted
        if "segment_start" in call.params:
            if "candidate_version" in call.sql or json_emitted:
                return []
            json_emitted = True
            return [
                _candidate(
                    PROJECT_A,
                    "json-array",
                    start_time=NOW - timedelta(hours=1),
                )
            ]
        return [
            _target_row(
                PROJECT_A,
                "json-array",
                start_time=NOW - timedelta(hours=1),
                legacy_raw='["accepted"]',
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
        json_attribute_mode="arrays",
    ).read_values(
        [PROJECT_A],
        "json_array",
        search="CEPT",
        window_start=NOW - timedelta(days=7),
        window_end=NOW,
    )

    assert read.rows == (AttributeValueRow("accepted", "array", 1),)
    assert read.metadata.query_complete is True
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert len(candidate_calls) == 14
    assert all(
        "candidate_version" in call.sql for call in candidate_calls[:7]
    )
    assert all(
        "candidate_version" not in call.sql for call in candidate_calls[7:]
    )
    assert all("attributes_extra" not in call.sql for call in candidate_calls)
    assert all("JSONHas(attributes_extra" not in call.sql for call in candidate_calls)
    assert all("attribute_search" not in call.params for call in candidate_calls)
    first_json_call = candidate_calls[7]
    assert first_json_call.settings["max_bytes_to_read"] == 64 * 1024 * 1024
    assert first_json_call.settings["max_rows_to_read"] == 100_000
    assert first_json_call.settings["max_block_size"] == 2_048
    json_hydration = next(
        call
        for call in executor.calls
        if "segment_start" not in call.params
        and "JSONHas(attributes_extra" in call.sql
    )
    assert len(json_hydration.params["candidate_ids_0"]) == 1


def test_older_typed_value_is_verified_before_json_overflow_lane():
    windows = adaptive_attribute_windows(NOW)
    recent = _candidate(
        PROJECT_A,
        "recent-cleared",
        start_time=NOW - timedelta(days=1),
    )
    older = _candidate(
        PROJECT_A,
        "older-live",
        start_time=NOW - timedelta(days=10),
    )
    candidates = {str(row["id"]): row for row in (recent, older)}

    def respond(call, _):
        if "segment_start" in call.params:
            if "JSONHas(attributes_extra" in call.sql:
                pytest.fail("JSON overflow ran before a verified typed sample")
            if call.params["segment_start"] == windows[0][0]:
                return [recent]
            if call.params["segment_start"] == windows[1][0]:
                return [older]
            return []
        requested_ids = call.params["candidate_ids_0"]
        if "max(_version) AS latest_version" in call.sql:
            return [
                _target_row(
                    PROJECT_A,
                    span_id,
                    start_time=candidates[span_id]["start_time"],
                    latest_version=2 if span_id == "recent-cleared" else 1,
                )
                for span_id in requested_ids
            ]
        return [
            _target_row(
                PROJECT_A,
                span_id,
                start_time=candidates[span_id]["start_time"],
                string="Rejected",
            )
            for span_id in requested_ids
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A], "final_status"
    )

    assert read.rows == (AttributeValueRow("Rejected", "string", 1),)
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert [call.params["segment_start"] for call in candidate_calls] == [
        segment[0] for segment in windows
    ]
    assert all("JSONHas(attributes_extra" not in call.sql for call in candidate_calls)


def test_value_search_pages_past_seed_stale_matches_to_live_value():
    stale = [
        _candidate(
            PROJECT_A,
            f"stale-value-{index:04d}",
            trace_id=f"trace-stale-value-{index:04d}",
            start_time=NOW - timedelta(seconds=index + 1),
        )
        for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT)
    ]
    live = _candidate(
        PROJECT_A,
        "live-value",
        trace_id="trace-live-value",
        start_time=NOW - timedelta(seconds=ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1),
    )
    rows_by_id = {str(row["id"]): row for row in [*stale, live]}
    recent_start = adaptive_attribute_windows(NOW)[0][0]

    def respond(call, _):
        if "segment_start" in call.params:
            if call.params["segment_start"] != recent_start:
                return []
            return _keyset_candidate_page([*stale, live], call)
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=rows_by_id[span_id]["trace_id"],
                start_time=rows_by_id[span_id]["start_time"],
                string="Rejected" if span_id == "live-value" else None,
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values([PROJECT_A], "final_status", search="Rejected")

    assert read.rows == (AttributeValueRow("Rejected", "string", 1),)
    assert read.metadata.query_complete is True
    candidates = [call for call in executor.calls if "segment_start" in call.params]
    assert all("attribute_search" not in call.params for call in candidates)
    assert all(
        call.params["candidate_limit"] == ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1
        for call in candidates
    )
    assert all("positionCaseInsensitiveUTF8" not in call.sql for call in candidates)
    assert all(
        "indexHint(has(mapKeys(attrs_string), %(attribute_key)s))" in call.sql
        and "has(attrs_string.keys, %(attribute_key)s)" in call.sql
        for call in candidates
    )
    continuation = next(
        call for call in candidates if "candidate_before_start_us" in call.params
    )
    assert continuation.params["candidate_before_id"] == stale[-1]["id"]
    assert all(
        "toUInt64(_version) AS candidate_version" in call.sql
        for call in candidates
    )
    ordered_call = next(call for call in candidates if "LIMIT 1 BY" in call.sql)
    assert ordered_call.sql.index("_version DESC") < ordered_call.sql.index(
        "LIMIT 1 BY"
    )


def test_incomplete_global_latest_replay_fails_closed_without_retry():
    def respond(call, _):
        if "segment_start" in call.params:
            return [
                _candidate(PROJECT_A, "one"),
                _candidate(PROJECT_A, "two"),
            ]
        return [_target_row(PROJECT_A, "one", string="partial-must-be-discarded")]

    executor = RecordingExecutor(respond)
    selector = AttributeReadSelector(executor, now=NOW)

    with pytest.raises(IncompleteLatestStateReplay):
        selector.read_values([PROJECT_A], "final_status")

    assert len(executor.calls) == 2


def test_global_replay_resource_failure_discards_partial_and_does_not_retry():
    def respond(call, _):
        if "segment_start" in call.params:
            return [_candidate(PROJECT_A, "one")]
        return ServerException("private SQL fragment", 307)

    executor = RecordingExecutor(respond)
    selector = AttributeReadSelector(executor, now=NOW)

    with pytest.raises(ServerException) as raised:
        selector.read_values([PROJECT_A], "final_status")

    assert raised.value.code == 307
    assert len(executor.calls) == 2
    assert selector.query_count == 2


@pytest.mark.parametrize("code", [241, 307])
def test_json_budget_failure_keeps_verified_typed_key_inventory_usable(code: int):
    recent_start = adaptive_attribute_windows(NOW)[0][0]

    def respond(call, _):
        if "segment_start" in call.params:
            if "attributes_extra NOT IN" in call.sql:
                return ServerException("private JSON lane failure", code)
            if call.params["segment_start"] == recent_start:
                return [_candidate(PROJECT_A, "typed-final-status")]
            return []
        return [
            {
                "project_id": PROJECT_A,
                "trace_id": f"trace-{PROJECT_A}-typed-final-status",
                "id": "typed-final-status",
                "start_time": NOW - timedelta(days=1),
                "is_deleted": 0,
                "string_keys": ["final_status"],
                "number_keys": [],
                "boolean_keys": [],
            }
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).discover_keys([PROJECT_A])

    assert read.rows == (AttributeKeyRow("final_status", "string", 1),)
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"
    json_call = next(
        call for call in executor.calls if "attributes_extra NOT IN" in call.sql
    )
    assert json_call.timeout_ms <= 750
    typed_call = next(
        call for call in executor.calls if "length(attrs_string.keys)" in call.sql
    )
    assert "attributes_extra" not in typed_call.sql


def test_verified_typed_searched_value_skips_json_budget_risk():
    recent_start = adaptive_attribute_windows(NOW)[0][0]

    def respond(call, _):
        if "segment_start" in call.params:
            if "JSONHas(attributes_extra" in call.sql:
                pytest.fail("JSON overflow ran after a verified typed sample")
            if call.params["segment_start"] == recent_start:
                return [_candidate(PROJECT_A, "typed-rejected")]
            return []
        return [_target_row(PROJECT_A, "typed-rejected", string="Rejected")]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A],
        "final_status",
        search="Rejected",
    )

    assert read.rows == (AttributeValueRow("Rejected", "string", 1),)
    assert read.metadata.query_complete is False
    assert read.metadata.query_error_code == "sample_limit"
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert all(
        not (
            "mapContains(attrs_string" in call.sql
            and "JSONHas(attributes_extra" in call.sql
        )
        for call in candidate_calls
    )
    assert all("JSONHas(attributes_extra" not in call.sql for call in candidate_calls)


def test_absent_heavy_json_key_uses_only_bounded_identity_seeds():
    starts_by_id: dict[str, datetime] = {}
    json_candidate_page = 0

    def respond(call, _):
        nonlocal json_candidate_page
        if "segment_start" in call.params:
            if "candidate_version" in call.sql:
                return []
            page = json_candidate_page
            json_candidate_page += 1
            rows = [
                _candidate(
                    PROJECT_A,
                    f"raw-json-{page:02d}-{index:02d}",
                    start_time=call.params["segment_start"]
                    + timedelta(seconds=index + 1),
                )
                for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1)
            ]
            starts_by_id.update(
                (str(row["id"]), row["start_time"]) for row in rows
            )
            return rows

        return [
            _target_row(
                PROJECT_A,
                span_id,
                start_time=starts_by_id[span_id],
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A], "absent_heavy_key"
    )

    assert read.rows == ()
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "sample_limit"
    candidate_calls = [
        call for call in executor.calls if "segment_start" in call.params
    ]
    assert len(candidate_calls) == ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT
    assert all("attributes_extra" not in call.sql for call in candidate_calls)
    assert all("JSONHas(attributes_extra" not in call.sql for call in candidate_calls)
    json_calls = [
        call for call in candidate_calls if "candidate_version" not in call.sql
    ]
    assert json_calls
    assert all(call.timeout_ms <= 750 for call in json_calls)
    assert all(
        call.settings["max_bytes_to_read"] == 64 * 1024 * 1024
        and call.settings["max_rows_to_read"] == 100_000
        and call.settings["max_block_size"] == 2_048
        for call in json_calls
    )
    hydration_calls = [
        call
        for call in executor.calls
        if "segment_start" not in call.params
        and "JSONHas(attributes_extra" in call.sql
    ]
    assert hydration_calls
    assert all(
        len(call.params["candidate_ids_0"])
        <= ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT
        for call in hydration_calls
    )


def test_timeout_on_first_segment_has_no_retry():
    executor = RecordingExecutor(
        lambda *_: ReadDeadlineExceeded("private deadline detail")
    )
    selector = AttributeReadSelector(executor, now=NOW)

    with pytest.raises(ReadDeadlineExceeded):
        selector.discover_keys([PROJECT_A])

    assert len(executor.calls) == 1


def test_later_budget_timeout_keeps_replayed_inventory_and_marks_it_degraded(
    monkeypatch,
):
    warning = MagicMock()
    monkeypatch.setattr(
        "tracer.services.clickhouse.attribute_reads.logger",
        SimpleNamespace(warning=warning),
    )

    def respond(call, call_number):
        if call_number == 1:
            return [_candidate(PROJECT_A, "recent")]
        if call_number == 2:
            return [
                {
                    "project_id": PROJECT_A,
                    "id": "recent",
                    "start_time": NOW - timedelta(days=1),
                    "is_deleted": 0,
                    "trace_id": f"trace-{PROJECT_A}-recent",
                    "trace_session_id": "",
                    "parent_span_id": "",
                    "string_keys": ["final_status"],
                    "number_keys": [],
                    "boolean_keys": [],
                    "attributes_extra": "{}",
                }
            ]
        return ReadDeadlineExceeded("private deadline detail")

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).discover_keys([PROJECT_A])

    assert read.rows == (AttributeKeyRow("final_status", "string", 1),)
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "read_budget_exceeded"
    assert read.metadata.query_window_start == NOW - timedelta(days=7)
    assert read.metadata.query_count == 3
    warning.assert_called_once_with(
        "attribute_read_partial_budget_exceeded",
        operation="discover_keys",
        query_count=3,
    )


def test_each_public_operation_starts_fresh_wall_budget_at_call_boundary():
    class ManualClock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = ManualClock()
    executor = RecordingExecutor()
    selector = AttributeReadSelector(
        executor,
        now=NOW,
        clock=clock,
        typed_only=True,
    )

    # Object construction can precede request dispatch without consuming the
    # operation's four-second wall budget.
    clock.value = 100.0
    selector.discover_keys([PROJECT_A], exact_key="first")
    assert selector.query_count == 5

    # A second public operation on the same selector gets a fresh budget and
    # query counter; its own adaptive queries still share that one deadline.
    clock.value = 200.0
    selector.read_values([PROJECT_A], "second")
    assert selector.query_count == 5
    assert len(executor.calls) == 10


def test_candidate_sample_cap_is_explicitly_degraded_and_query_count_bounded():
    starts_by_id: dict[str, datetime] = {}
    recent_start = adaptive_attribute_windows(NOW)[0][0]
    recent_page = 0

    def respond(call, _):
        nonlocal recent_page
        if "segment_start" in call.params:
            if call.params["segment_start"] != recent_start:
                return []
            page = recent_page
            recent_page += 1
            rows = [
                _candidate(
                    PROJECT_A,
                    f"span-{page:02d}-{index:04d}",
                    trace_id=f"trace-span-{page:02d}-{index:04d}",
                    start_time=NOW - timedelta(seconds=page * 1_000 + index + 1),
                )
                for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1)
            ]
            starts_by_id.update((str(row["id"]), row["start_time"]) for row in rows)
            return rows
        return [
            _target_row(
                PROJECT_A,
                span_id,
                trace_id=f"trace-{span_id}",
                start_time=starts_by_id[span_id],
                string="same",
            )
            for span_id in call.params["candidate_ids_0"]
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        typed_only=True,
    ).read_values([PROJECT_A], "sampled")

    assert read.rows == (
        AttributeValueRow(
            "same",
            "string",
            ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT,
        ),
    )
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "sampled"
    assert read.metadata.query_error_code == "sample_limit"
    assert read.metadata.query_count <= (
        2 * ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT
    )


def test_value_search_miss_never_exceeds_hard_query_ceiling():
    starts_by_id: dict[str, datetime] = {}
    typed_candidate_page = 0

    def respond(call, _):
        nonlocal typed_candidate_page
        if "segment_start" in call.params:
            if "JSONHas(attributes_extra" in call.sql:
                return []
            page = typed_candidate_page
            typed_candidate_page += 1
            rows = [
                _candidate(
                    PROJECT_A,
                    f"miss-{page:02d}-{index:02d}",
                    start_time=NOW
                    - timedelta(days=1, seconds=page * 100 + index + 1),
                )
                for index in range(ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT + 1)
            ]
            starts_by_id.update(
                (str(row["id"]), row["start_time"]) for row in rows
            )
            return rows

        requested_ids = call.params["candidate_ids_0"]
        if "max(_version) AS latest_version" in call.sql:
            return [
                _target_row(
                    PROJECT_A,
                    span_id,
                    start_time=starts_by_id[span_id],
                    latest_version=1,
                )
                for span_id in requested_ids
            ]
        return [
            _target_row(
                PROJECT_A,
                span_id,
                start_time=starts_by_id[span_id],
                string="does-not-match",
            )
            for span_id in requested_ids
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(
        executor,
        now=NOW,
        wall_timeout_ms=600_000,
    ).read_values([PROJECT_A], "final_status", search="needle")

    assert read.rows == ()
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "read_budget_exceeded"
    assert read.metadata.query_count == ATTRIBUTE_READ_MAX_QUERY_COUNT
    assert len(executor.calls) == ATTRIBUTE_READ_MAX_QUERY_COUNT


def test_query_ceiling_retains_values_but_never_claims_complete(monkeypatch):
    monkeypatch.setattr(
        "tracer.services.clickhouse.attribute_reads.ATTRIBUTE_READ_MAX_QUERY_COUNT",
        4,
    )
    candidate = _candidate(PROJECT_A, "retained-before-cap")

    def respond(call, call_number):
        if "segment_start" in call.params:
            return [candidate] if call_number == 1 else []
        if "max(_version) AS latest_version" in call.sql:
            return [
                _target_row(
                    PROJECT_A,
                    "retained-before-cap",
                    start_time=candidate["start_time"],
                    latest_version=1,
                )
            ]
        return [
            _target_row(
                PROJECT_A,
                "retained-before-cap",
                start_time=candidate["start_time"],
                string="Rejected",
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A], "final_status"
    )

    assert read.rows == (AttributeValueRow("Rejected", "string", 1),)
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "read_budget_exceeded"
    assert read.metadata.query_count == 4
    assert len(executor.calls) == 4


def test_query_ceiling_without_decoded_values_returns_degraded(monkeypatch):
    monkeypatch.setattr(
        "tracer.services.clickhouse.attribute_reads.ATTRIBUTE_READ_MAX_QUERY_COUNT",
        4,
    )
    candidate = _candidate(PROJECT_A, "empty-before-cap")

    def respond(call, call_number):
        if "segment_start" in call.params:
            return [candidate] if call_number == 1 else []
        return [
            _target_row(
                PROJECT_A,
                "empty-before-cap",
                start_time=candidate["start_time"],
                latest_version=1,
            )
        ]

    executor = RecordingExecutor(respond)
    read = AttributeReadSelector(executor, now=NOW).read_values(
        [PROJECT_A], "final_status"
    )

    assert read.rows == ()
    assert read.metadata.query_complete is False
    assert read.metadata.query_status == "degraded"
    assert read.metadata.query_error_code == "read_budget_exceeded"
    assert read.metadata.query_count == 4
    assert len(executor.calls) == 4


def test_malformed_keys_and_oversized_project_scopes_fail_before_ch():
    executor = RecordingExecutor()
    selector = AttributeReadSelector(executor, now=NOW)

    for key in ("", "contains\x00control", "é" * 257):
        with pytest.raises(InvalidAttributeKey):
            selector.read_values([PROJECT_A], key)
    assert validate_attribute_key("customer.%_status\\路径'quote") == (
        "customer.%_status\\路径'quote"
    )
    too_many_projects = [
        str(uuid.uuid4()) for _ in range(ATTRIBUTE_READ_MAX_PROJECTS + 1)
    ]
    with pytest.raises(IncompleteLatestStateReplay):
        selector.discover_keys(too_many_projects)
    assert executor.calls == []


def test_span_attribute_keys_contract_accepts_exact_probe_and_read_state():
    project_id = uuid.uuid4()
    query = SpanAttributeProjectQuerySerializer(
        data={"project_id": project_id, "q": "final_status"}
    )

    assert query.is_valid(), query.errors
    assert query.validated_data["q"] == "final_status"
    assert {
        "query_complete",
        "query_status",
        "query_error_code",
        "query_window_start",
        "query_window_end",
    } <= set(SpanAttributeKeysResponseSerializer().fields)


def test_span_attribute_detail_contract_validates_key_and_exposes_read_state():
    query = SpanAttributeDetailQuerySerializer(
        data={"project_id": uuid.uuid4(), "key": "customer.%_status\\path"}
    )

    assert query.is_valid(), query.errors
    assert query.validated_data["key"] == "customer.%_status\\path"
    for invalid_key in ("", "contains\x00control", "é" * 257):
        invalid = SpanAttributeDetailQuerySerializer(
            data={"project_id": uuid.uuid4(), "key": invalid_key}
        )
        assert not invalid.is_valid()
        assert "key" in invalid.errors
    assert {
        "query_complete",
        "query_status",
        "query_error_code",
        "query_window_start",
        "query_window_end",
    } <= set(SpanAttributeDetailResponseSerializer().fields)


def test_eval_attribute_picker_contract_accepts_general_exact_key_probe():
    project_id = uuid.uuid4()
    query = ObservationAttributeListQuerySerializer(
        data={
            "filters": {"project_id": str(project_id)},
            "row_type": "spans",
            "q": "customer.%_status\\path",
        }
    )

    assert query.is_valid(), query.errors
    assert query.validated_data["q"] == "customer.%_status\\path"
    assert {
        "query_complete",
        "query_status",
        "query_error_code",
        "query_window_start",
        "query_window_end",
    } <= set(ObservationAttributeListResponseSerializer().fields)


def test_dashboard_final_status_picker_returns_rejected_from_selector(
    monkeypatch,
):
    from tracer.views.dashboard import DashboardViewSet

    captured: dict[str, Any] = {}

    def read_values(self, project_ids, key, **kwargs):
        captured.update(
            project_ids=project_ids,
            key=key,
            typed_only=self._typed_only,
            json_attribute_mode=self._json_attribute_mode,
            kwargs=kwargs,
        )
        return AttributeValueRead(
            (AttributeValueRow("Rejected", "string", 1),),
            _metadata(),
        )

    monkeypatch.setattr(AttributeReadSelector, "read_values", read_values)
    monkeypatch.setattr("tracer.views.dashboard.is_clickhouse_enabled", lambda: False)
    monkeypatch.setattr(
        "tracer.views.dashboard.project_queryset_for_request",
        lambda _request: _ProjectScope([PROJECT_A]),
    )
    monkeypatch.setattr(
        "tracer.views.dashboard.AnalyticsQueryService.execute_ch_query",
        lambda *_args, **_kwargs: pytest.fail("legacy ClickHouse must not be queried"),
    )

    request = _authenticated_get(
        "/tracer/dashboard/filter_values/",
        {
            "metric_name": "final_status",
            "metric_type": "custom_attribute",
            "project_ids": PROJECT_A,
            "source": "traces",
        },
    )
    response = DashboardViewSet.as_view({"get": "filter_values"})(request)

    assert response.status_code == 200
    payload = response.data["result"]
    assert payload["values"] == [{"value": "Rejected", "label": "Rejected"}]
    assert payload["query_complete"] is True
    assert captured["project_ids"] == [PROJECT_A]
    assert captured["key"] == "final_status"
    assert captured["typed_only"] is True
    assert captured["json_attribute_mode"] == "arrays"


def test_dashboard_json_array_value_picker_preserves_scalar_json_types(monkeypatch):
    from tracer.views.dashboard import DashboardViewSet

    def read_values(self, project_ids, key, **kwargs):
        assert self._json_attribute_mode == "arrays"
        return AttributeValueRead(
            (
                AttributeValueRow(True, "array", 2),
                AttributeValueRow(7, "array", 1),
                AttributeValueRow("seven", "array", 1),
            ),
            _metadata(),
        )

    monkeypatch.setattr(AttributeReadSelector, "read_values", read_values)
    monkeypatch.setattr("tracer.views.dashboard.is_clickhouse_enabled", lambda: False)
    monkeypatch.setattr(
        "tracer.views.dashboard.project_queryset_for_request",
        lambda _request: _ProjectScope([PROJECT_A]),
    )

    request = _authenticated_get(
        "/tracer/dashboard/filter_values/",
        {
            "metric_name": "json_choices",
            "metric_type": "custom_attribute",
            "project_ids": PROJECT_A,
            "source": "traces",
        },
    )
    response = DashboardViewSet.as_view({"get": "filter_values"})(request)

    assert response.status_code == 200
    assert response.data["result"]["values"] == [
        {"value": True, "label": "true"},
        {"value": 7, "label": "7"},
        {"value": "seven", "label": "seven"},
    ]


@pytest.mark.parametrize("code", [159, 241, 307])
def test_dashboard_budget_errors_return_sanitized_503_and_are_not_retried(
    code, monkeypatch
):
    from tracer.views.dashboard import DashboardViewSet

    calls = 0

    def fail(self, query, params, *, timeout_ms, settings):
        nonlocal calls
        calls += 1
        raise ServerException("secret SQL and stack detail", code)

    monkeypatch.setattr(V2AttributeQueryExecutor, "execute", fail)
    monkeypatch.setattr("tracer.views.dashboard.is_clickhouse_enabled", lambda: True)
    monkeypatch.setattr(
        "tracer.views.dashboard.project_queryset_for_request",
        lambda _request: _ProjectScope([PROJECT_A]),
    )

    request = _authenticated_get(
        "/tracer/dashboard/filter_values/",
        {
            "metric_name": "final_status",
            "metric_type": "custom_attribute",
            "project_ids": PROJECT_A,
            "source": "traces",
        },
    )
    response = DashboardViewSet.as_view({"get": "filter_values"})(request)

    assert response.status_code == 503
    assert response.data["code"] == "service_unavailable"
    serialized = json.dumps(response.data)
    assert "temporarily unavailable" in serialized
    assert "secret" not in serialized
    assert "SELECT" not in serialized
    assert calls == 1


def test_eval_picker_uses_selector_for_keys_and_cardinality_without_pg_fallback(
    monkeypatch,
):
    from tracer.views.observation_span import ObservationSpanView

    captured: dict[str, Any] = {}

    def discover_keys(self, project_ids, exact_key=None):
        captured.update(
            typed_only=self._typed_only,
            json_attribute_mode=self._json_attribute_mode,
            exact_key=exact_key,
        )
        return AttributeKeyRead(
            (AttributeKeyRow(exact_key or "fallback", "json", 1),),
            _metadata(),
        )

    monkeypatch.setattr(
        AttributeReadSelector,
        "discover_keys",
        discover_keys,
    )
    monkeypatch.setattr(
        AttributeReadSelector,
        "sample_cardinality",
        lambda self, project_ids: AttributeCardinalityRead(1, 1, _metadata()),
    )
    monkeypatch.setattr(
        "tracer.views.observation_span.ObservationSpanView._get_span_attribute_keys",
        lambda *_args, **_kwargs: pytest.fail("PG/legacy inventory fallback used"),
    )
    monkeypatch.setattr(
        "tracer.views.observation_span.ObservationSpanView._max_spans_per_trace",
        lambda *_args, **_kwargs: pytest.fail("PG/legacy cardinality fallback used"),
    )
    monkeypatch.setattr(
        "tracer.views.observation_span.ObservationSpanView._max_traces_per_session",
        lambda *_args, **_kwargs: pytest.fail("PG cardinality fallback used"),
    )
    monkeypatch.setattr(
        ObservationSpanView,
        "_attribute_project_for_request",
        staticmethod(lambda _request, _project_id: True),
    )

    request = _authenticated_get(
        "/tracer/observation-span/get_eval_attributes_list/",
        {
            "filters": json.dumps({"project_id": PROJECT_A}),
            "row_type": "traces",
            "q": "rare.customer.key",
        },
    )
    response = ObservationSpanView.as_view({"get": "get_eval_attributes_list"})(request)

    assert response.status_code == 200
    payload = response.data
    assert "spans.0.rare.customer.key" in payload["result"]
    assert payload["query_complete"] is True
    assert captured == {
        "typed_only": True,
        "json_attribute_mode": "all",
        "exact_key": "rare.customer.key",
    }


@pytest.mark.parametrize(
    ("action_name", "path"),
    [
        (
            "get_span_attributes_list",
            "/tracer/observation-span/get_span_attributes_list/",
        ),
        (
            "get_eval_attributes_list",
            "/tracer/observation-span/get_eval_attributes_list/",
        ),
    ],
)
def test_observation_attribute_pickers_return_sanitized_503_for_typed_ch_failures(
    monkeypatch, action_name, path
):
    from tracer.views.observation_span import ObservationSpanView

    def fail(*_args, **_kwargs):
        raise ServerException("private ClickHouse query detail", 159)

    monkeypatch.setattr(AttributeReadSelector, "discover_keys", fail)
    monkeypatch.setattr(
        ObservationSpanView,
        "_attribute_project_for_request",
        staticmethod(lambda _request, _project_id: True),
    )
    request = _authenticated_get(
        path,
        {"filters": json.dumps({"project_id": PROJECT_A})},
    )

    response = ObservationSpanView.as_view({"get": action_name})(request)

    assert response.status_code == 503
    assert "temporarily unavailable" in json.dumps(response.data)
    assert "private ClickHouse" not in json.dumps(response.data)


@pytest.mark.parametrize(
    ("action_name", "path"),
    [
        (
            "get_span_attributes_list",
            "/tracer/observation-span/get_span_attributes_list/",
        ),
        (
            "get_eval_attributes_list",
            "/tracer/observation-span/get_eval_attributes_list/",
        ),
    ],
)
def test_observation_attribute_pickers_return_sanitized_500_for_programming_defects(
    monkeypatch, action_name, path
):
    from tracer.views.observation_span import ObservationSpanView

    def fail(*_args, **_kwargs):
        raise RuntimeError("private attribute compiler invariant")

    monkeypatch.setattr(AttributeReadSelector, "discover_keys", fail)
    monkeypatch.setattr(
        ObservationSpanView,
        "_attribute_project_for_request",
        staticmethod(lambda _request, _project_id: True),
    )
    request = _authenticated_get(
        path,
        {"filters": json.dumps({"project_id": PROJECT_A})},
    )

    response = ObservationSpanView.as_view({"get": action_name})(request)

    assert response.status_code == 500
    payload = json.dumps(response.data)
    assert "could not be loaded" in payload
    assert "compiler invariant" not in payload


@pytest.mark.parametrize(
    ("action_name", "path"),
    [
        (
            "get_span_attributes_list",
            "/tracer/observation-span/get_span_attributes_list/",
        ),
        (
            "get_eval_attributes_list",
            "/tracer/observation-span/get_eval_attributes_list/",
        ),
    ],
)
@pytest.mark.parametrize(
    ("error_code", "sampled", "expected_status"),
    [
        ("read_budget_exceeded", False, 503),
        ("sample_limit", False, 503),
        ("sample_limit", True, 200),
    ],
)
def test_observation_attribute_pickers_only_publish_labelled_samples(
    monkeypatch, action_name, path, error_code, sampled, expected_status
):
    from tracer.views.observation_span import ObservationSpanView

    monkeypatch.setattr(
        AttributeReadSelector,
        "discover_keys",
        lambda *_args, **_kwargs: AttributeKeyRead(
            (AttributeKeyRow("final_status", "string", 1),),
            _metadata(complete=False, error_code=error_code, sampled=sampled),
        ),
    )
    monkeypatch.setattr(
        ObservationSpanView,
        "_attribute_project_for_request",
        staticmethod(lambda _request, _project_id: True),
    )
    request = _authenticated_get(
        path,
        {"filters": json.dumps({"project_id": PROJECT_A})},
    )

    response = ObservationSpanView.as_view({"get": action_name})(request)

    assert response.status_code == expected_status
    if expected_status == 200:
        assert response.data["result"] == ["final_status"]
        assert response.data["query_complete"] is False
        assert response.data["query_status"] == "sampled"
        assert response.data["query_error_code"] == "sample_limit"
        assert "query_window_start" in response.data
        assert "query_window_end" in response.data
    else:
        assert "temporarily unavailable" in json.dumps(response.data)


@pytest.mark.parametrize(
    ("action_name", "path"),
    [
        (
            "get_span_attributes_list",
            "/tracer/observation-span/get_span_attributes_list/",
        ),
        (
            "get_eval_attributes_list",
            "/tracer/observation-span/get_eval_attributes_list/",
        ),
    ],
)
def test_observation_attribute_pickers_reject_empty_sample_limit_results(
    monkeypatch, action_name, path
):
    from tracer.views.observation_span import ObservationSpanView

    monkeypatch.setattr(
        AttributeReadSelector,
        "discover_keys",
        lambda *_args, **_kwargs: AttributeKeyRead(
            (),
            _metadata(complete=False, error_code="sample_limit"),
        ),
    )
    monkeypatch.setattr(
        ObservationSpanView,
        "_attribute_project_for_request",
        staticmethod(lambda _request, _project_id: True),
    )
    request = _authenticated_get(
        path,
        {"filters": json.dumps({"project_id": PROJECT_A})},
    )

    response = ObservationSpanView.as_view({"get": action_name})(request)

    assert response.status_code == 503
    assert "temporarily unavailable" in json.dumps(response.data)
    assert "sample_limit" not in json.dumps(response.data)


def test_span_attribute_ownership_gate_precedes_any_ch_read(monkeypatch):
    from tracer.views.span_attributes import SpanAttributeKeysView

    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        pytest.fail("ClickHouse read crossed the project ownership gate")

    monkeypatch.setattr(V2AttributeQueryExecutor, "execute", fail)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: False,
    )
    unknown_project = uuid.uuid4()

    request = _authenticated_get(
        "/api/traces/span-attribute-keys/",
        {"project_id": str(unknown_project), "q": "final_status"},
    )
    response = SpanAttributeKeysView.as_view()(request)

    assert response.status_code == 404
    assert calls == 0


def test_span_attribute_detail_ownership_gate_precedes_any_ch_read(monkeypatch):
    from tracer.views.span_attributes import SpanAttributeDetailView

    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        pytest.fail("ClickHouse read crossed the project ownership gate")

    monkeypatch.setattr(V2AttributeQueryExecutor, "execute", fail)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: False,
    )

    request = _authenticated_get(
        "/api/traces/span-attribute-detail/",
        {"project_id": PROJECT_A, "key": "final_status"},
    )
    response = SpanAttributeDetailView.as_view()(request)

    assert response.status_code == 404
    assert calls == 0


def test_span_attribute_detail_uses_bounded_v2_latest_state_distribution(monkeypatch):
    from tracer.views.span_attributes import SpanAttributeDetailView

    captured: dict[str, Any] = {}

    def read_detail(self, project_ids, key, **kwargs):
        captured.update(
            project_ids=project_ids,
            key=key,
            typed_only=self._typed_only,
            json_attribute_mode=self._json_attribute_mode,
            kwargs=kwargs,
        )
        return AttributeDetailRead(
            "string",
            (
                AttributeValueRow("Rejected", "string", 2),
                AttributeValueRow("Accepted", "string", 1),
            ),
            _metadata(),
        )

    monkeypatch.setattr(AttributeReadSelector, "read_detail", read_detail)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: True,
    )
    request = _authenticated_get(
        "/api/traces/span-attribute-detail/",
        {"project_id": PROJECT_A, "key": "final_status"},
    )

    response = SpanAttributeDetailView.as_view()(request)

    assert response.status_code == 200
    assert response.data["key"] == "final_status"
    assert response.data["type"] == "string"
    assert response.data["count"] == 3
    assert response.data["unique_values"] == 2
    assert response.data["top_values"] == [
        {"value": "Rejected", "count": 2, "percentage": 66.7},
        {"value": "Accepted", "count": 1, "percentage": 33.3},
    ]
    assert response.data["query_complete"] is True
    contract = SpanAttributeDetailResponseSerializer(data=response.data)
    assert contract.is_valid(), contract.errors
    assert captured == {
        "project_ids": [PROJECT_A],
        "key": "final_status",
        "typed_only": True,
        "json_attribute_mode": "arrays",
        "kwargs": {},
    }


def test_span_attribute_numeric_detail_uses_weighted_nearest_rank_statistics():
    from tracer.views.span_attributes import SpanAttributeDetailView

    read = AttributeDetailRead(
        "number",
        (
            AttributeValueRow(1.0, "number", 1),
            AttributeValueRow(10.0, "number", 2),
            AttributeValueRow(100.0, "number", 1),
        ),
        _metadata(),
    )

    payload = SpanAttributeDetailView._detail_payload("latency.score", read)

    assert payload == {
        "key": "latency.score",
        "type": "number",
        "count": 4,
        "min": 1.0,
        "max": 100.0,
        "avg": 30.25,
        "p50": 10.0,
        "p95": 100.0,
        **read.metadata.public_payload(),
    }
    contract = SpanAttributeDetailResponseSerializer(data=payload)
    assert contract.is_valid(), contract.errors


def test_span_attribute_detail_budget_error_is_sanitized_degraded_success(
    monkeypatch,
):
    from tracer.views.span_attributes import SpanAttributeDetailView

    def fail(*_args, **_kwargs):
        raise ServerException("secret SQL and internal stack", 159)

    monkeypatch.setattr(AttributeReadSelector, "read_detail", fail)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: True,
    )
    request = _authenticated_get(
        "/api/traces/span-attribute-detail/",
        {"project_id": PROJECT_A, "key": "final_status"},
    )

    response = SpanAttributeDetailView.as_view()(request)

    assert response.status_code == 200
    assert response.data["key"] == "final_status"
    assert response.data["type"] == "string"
    assert response.data["count"] == 0
    assert response.data["top_values"] == []
    assert response.data["query_complete"] is False
    assert response.data["query_status"] == "degraded"
    assert response.data["query_error_code"] == "read_budget_exceeded"
    contract = SpanAttributeDetailResponseSerializer(data=response.data)
    assert contract.is_valid(), contract.errors
    serialized = json.dumps(response.data)
    assert "secret" not in serialized
    assert "SELECT" not in serialized


@pytest.mark.parametrize(
    ("view_name", "selector_method", "path", "params"),
    [
        (
            "SpanAttributeKeysView",
            "discover_keys",
            "/api/traces/span-attribute-keys/",
            {"project_id": PROJECT_A},
        ),
        (
            "SpanAttributeValuesView",
            "read_values",
            "/api/traces/span-attribute-values/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
        (
            "SpanAttributeDetailView",
            "read_detail",
            "/api/traces/span-attribute-detail/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
    ],
)
def test_span_attribute_views_degrade_typed_transient_failures(
    monkeypatch,
    view_name,
    selector_method,
    path,
    params,
):
    from tracer.views import span_attributes

    def fail(*_args, **_kwargs):
        raise ServerException("secret network detail", 210)

    monkeypatch.setattr(AttributeReadSelector, selector_method, fail)
    monkeypatch.setattr(
        span_attributes,
        "_project_is_in_request_scope",
        lambda _request, _project_id: True,
    )
    request = _authenticated_get(path, params)

    response = getattr(span_attributes, view_name).as_view()(request)

    assert response.status_code == 200
    assert response.data["query_complete"] is False
    assert response.data["query_status"] == "degraded"
    assert response.data["query_error_code"] == "query_failed"
    assert "secret network detail" not in json.dumps(response.data)


@pytest.mark.parametrize(
    ("view_name", "selector_method", "path", "params"),
    [
        (
            "SpanAttributeKeysView",
            "discover_keys",
            "/api/traces/span-attribute-keys/",
            {"project_id": PROJECT_A},
        ),
        (
            "SpanAttributeValuesView",
            "read_values",
            "/api/traces/span-attribute-values/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
        (
            "SpanAttributeDetailView",
            "read_detail",
            "/api/traces/span-attribute-detail/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
    ],
)
def test_span_attribute_views_return_sanitized_500_for_programming_defects(
    monkeypatch,
    view_name,
    selector_method,
    path,
    params,
):
    from tracer.views import span_attributes

    def fail(*_args, **_kwargs):
        raise RuntimeError("attribute compiler invariant failed")

    monkeypatch.setattr(AttributeReadSelector, selector_method, fail)
    monkeypatch.setattr(
        span_attributes,
        "_project_is_in_request_scope",
        lambda _request, _project_id: True,
    )
    request = _authenticated_get(path, params)

    response = getattr(span_attributes, view_name).as_view()(request)

    assert response.status_code == 500
    serialized = json.dumps(response.data)
    assert "could not be loaded" in serialized
    assert "compiler invariant" not in serialized


@pytest.mark.parametrize(
    ("view_name", "path", "params"),
    [
        (
            "SpanAttributeKeysView",
            "/api/traces/span-attribute-keys/",
            {"project_id": PROJECT_A},
        ),
        (
            "SpanAttributeValuesView",
            "/api/traces/span-attribute-values/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
        (
            "SpanAttributeDetailView",
            "/api/traces/span-attribute-detail/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
    ],
)
def test_span_attribute_views_sanitize_unexpected_scope_failures(
    monkeypatch,
    view_name,
    path,
    params,
):
    from tracer.views import span_attributes

    def fail_scope(*_args, **_kwargs):
        raise RuntimeError("private ownership database detail")

    monkeypatch.setattr(
        span_attributes,
        "_project_is_in_request_scope",
        fail_scope,
    )
    request = _authenticated_get(path, params)

    response = getattr(span_attributes, view_name).as_view()(request)

    assert response.status_code == 500
    serialized = json.dumps(response.data)
    assert "could not be loaded" in serialized
    assert "ownership database detail" not in serialized


@pytest.mark.parametrize(
    ("view_name", "path", "params"),
    [
        (
            "SpanAttributeKeysView",
            "/api/traces/span-attribute-keys/",
            {"project_id": PROJECT_A},
        ),
        (
            "SpanAttributeValuesView",
            "/api/traces/span-attribute-values/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
        (
            "SpanAttributeDetailView",
            "/api/traces/span-attribute-detail/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
    ],
)
@pytest.mark.parametrize(
    ("failure", "expected_status", "public_message"),
    [
        (RuntimeError("private selector configuration"), 500, "could not be loaded"),
        (
            ReadDeadlineExceeded("private selector connection timeout"),
            503,
            "temporarily unavailable",
        ),
    ],
)
def test_span_attribute_views_sanitize_selector_construction_failures(
    monkeypatch,
    view_name,
    path,
    params,
    failure,
    expected_status,
    public_message,
):
    from tracer.views import span_attributes

    def fail_selector(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(span_attributes, "AttributeReadSelector", fail_selector)
    request = _authenticated_get(path, params)

    response = getattr(span_attributes, view_name).as_view()(request)

    assert response.status_code == expected_status
    serialized = json.dumps(response.data)
    assert public_message in serialized
    assert "private selector" not in serialized


@pytest.mark.parametrize(
    ("view_name", "selector_method", "path", "params"),
    [
        (
            "SpanAttributeKeysView",
            "discover_keys",
            "/api/traces/span-attribute-keys/",
            {"project_id": PROJECT_A},
        ),
        (
            "SpanAttributeValuesView",
            "read_values",
            "/api/traces/span-attribute-values/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
        (
            "SpanAttributeDetailView",
            "read_detail",
            "/api/traces/span-attribute-detail/",
            {"project_id": PROJECT_A, "key": "final_status"},
        ),
    ],
)
def test_span_attribute_views_return_sanitized_500_for_driver_query_defects(
    monkeypatch,
    view_name,
    selector_method,
    path,
    params,
):
    from tracer.views import span_attributes

    def fail(*_args, **_kwargs):
        raise ServerException("secret missing-column SQL", 47)

    monkeypatch.setattr(AttributeReadSelector, selector_method, fail)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: True,
    )
    request = _authenticated_get(path, params)

    response = getattr(span_attributes, view_name).as_view()(request)

    assert response.status_code == 500
    serialized = json.dumps(response.data)
    assert "could not be loaded" in serialized
    assert "secret missing-column SQL" not in serialized


def test_span_attribute_keys_use_v2_when_legacy_clickhouse_is_disabled(monkeypatch):
    from tracer.views.span_attributes import SpanAttributeKeysView

    captured: dict[str, Any] = {}

    def discover_keys(self, project_ids, exact_key=None):
        captured.update(
            project_ids=project_ids,
            exact_key=exact_key,
            typed_only=self._typed_only,
            json_attribute_mode=self._json_attribute_mode,
        )
        return AttributeKeyRead(
            (AttributeKeyRow("json_choices", "array", 1),),
            _metadata(),
        )

    monkeypatch.setattr(AttributeReadSelector, "discover_keys", discover_keys)
    monkeypatch.setattr(
        "tracer.views.span_attributes._project_is_in_request_scope",
        lambda _request, _project_id: True,
    )

    request = _authenticated_get(
        "/api/traces/span-attribute-keys/",
        {"project_id": PROJECT_A, "q": "json_choices"},
    )
    response = SpanAttributeKeysView.as_view()(request)

    assert response.status_code == 200
    assert response.data["result"] == [
        {"key": "json_choices", "type": "array", "count": 1}
    ]
    contract = SpanAttributeKeysResponseSerializer(data=response.data)
    assert contract.is_valid(), contract.errors
    assert captured == {
        "project_ids": [PROJECT_A],
        "exact_key": "json_choices",
        "typed_only": True,
        "json_attribute_mode": "arrays",
    }
