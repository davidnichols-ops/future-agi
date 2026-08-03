from __future__ import annotations

import inspect
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from model_hub.models.choices import AnnotationTypeChoices
from tracer.selectors.trace_filter_reads import BoundedFilterPage
from tracer.services.clickhouse import bounded_graph_reads, graph_dispatch
from tracer.services.clickhouse.bounded_graph_reads import (
    BoundedGraphReadError,
    GraphCandidateSample,
    aggregate_system_candidate_graph,
    read_graph_candidates,
)
from tracer.services.clickhouse.read_budget import ReadDeadlineExceeded

PROJECT_ID = "ca3025a9-b5eb-4872-9973-2330956d40d2"
EVAL_ID = "109f6d0d-9446-4f19-b11c-19646649a4bd"
LABEL_ID = "9cefe781-4146-488f-ac75-f013d5a725ea"
START = datetime(2026, 7, 24, 2, 40)
END = START + timedelta(minutes=5)


def _unix_microseconds(value: datetime) -> int:
    epoch = datetime(1970, 1, 1)
    delta = value - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _date_filter(start: datetime = START, end: datetime = END) -> dict:
    return {
        "column_id": "created_at",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [start.isoformat(), end.isoformat()],
        },
    }


def _date_bound_filter(operation: str, value: datetime) -> dict:
    return {
        "column_id": "created_at",
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "datetime",
            "filter_op": operation,
            "filter_value": value.isoformat(),
        },
    }


def _attribute_filter(
    key: str,
    value,
    *,
    filter_type: str = "text",
    filter_op: str = "equals",
) -> dict:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": filter_type,
            "filter_op": filter_op,
            "filter_value": value,
        },
    }


def _system_text_filter(key: str, value: str) -> dict:
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SYSTEM_METRIC",
            "filter_type": "text",
            "filter_op": "equals",
            "filter_value": value,
        },
    }


def _annotation_filter(label_id: str, value: object) -> dict:
    return {
        "column_id": label_id,
        "filter_config": {
            "col_type": "ANNOTATION",
            "filter_type": "text",
            "filter_op": "equals",
            "filter_value": value,
        },
    }


class _Result(SimpleNamespace):
    def __init__(self, rows):
        super().__init__(data=rows, columns=list(rows[0]) if rows else [])


class _CandidateAnalytics:
    def __init__(self, *, observe_type: str, rows: list[dict]):
        self.observe_type = observe_type
        self.rows = rows
        self.calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params, timeout_ms, settings))
        seed_column = "trace_id" if self.observe_type == "trace" else "id"
        if "filter_anchor_limit" in params:
            return _Result(
                [
                    {"trace_id": trace_id}
                    for trace_id in list(
                        dict.fromkeys(str(row["trace_id"]) for row in self.rows)
                    )[: params["filter_anchor_limit"]]
                ]
            )
        if "filter_seed_limit" in params:
            slice_start = params["filter_slice_start"]
            slice_end = params["filter_slice_end"]
            before_start_us = params.get("filter_before_start_us")
            before_time = (
                datetime(1970, 1, 1) + timedelta(microseconds=before_start_us)
                if before_start_us is not None
                else None
            )
            before_id = params.get("filter_before_id")
            candidates = [
                row
                for row in self.rows
                if slice_start <= row["start_time"] < slice_end
                and (
                    before_time is None
                    or (row["start_time"], str(row[seed_column]))
                    < (before_time, str(before_id))
                )
            ]
            candidates.sort(
                key=lambda row: (row["start_time"], str(row[seed_column])),
                reverse=True,
            )
            seed_rows = []
            for row in candidates[: params["filter_seed_limit"]]:
                seed_row = {
                    seed_column: row[seed_column],
                    "start_time": row["start_time"],
                }
                if self.observe_type == "span":
                    seed_row["trace_id"] = row["trace_id"]
                if self.observe_type == "trace" and row.get("root_span_id"):
                    seed_row["root_span_id"] = row["root_span_id"]
                seed_rows.append(seed_row)
            return _Result(seed_rows)
        candidate_key = (
            "candidate_trace_ids"
            if self.observe_type == "trace"
            else "candidate_span_ids"
        )
        allowed = set(params[candidate_key])
        return _Result([row for row in self.rows if str(row[seed_column]) in allowed])


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observe_type", "key", "value"),
    [
        ("trace", "final_status", "Rechazado"),
        ("span", "prompt_slug", "agent_2_identity_disclosure"),
    ],
)
def test_filtered_graph_candidates_are_finite_latest_state_samples(
    observe_type, key, value
):
    identity_key = "trace_id" if observe_type == "trace" else "id"
    row = {
        identity_key: "trace-1" if observe_type == "trace" else "span-1",
        "root_span_id": "span-1",
        "start_time": START + timedelta(minutes=4),
        "latency_ms": 25.0,
        "cost": 0.1,
        "total_tokens": 11,
        "prompt_tokens": 7,
        "completion_tokens": 4,
        "status": "OK",
    }
    if observe_type == "span":
        row["trace_id"] = "trace-1"
    analytics = _CandidateAnalytics(observe_type=observe_type, rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter(key, value)],
        observe_type=observe_type,
    )

    assert sample.rows == (row,)
    # Scalar typed Maps are authoritative. JSON/map/array filter types are
    # rejected before a query, so an exhausted scalar range is exact.
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.query_error_code is None
    assert len(analytics.calls) >= 2

    seed_query, seed_params, seed_timeout, seed_settings = analytics.calls[0]
    candidate_param = (
        "candidate_trace_ids" if observe_type == "trace" else "candidate_span_ids"
    )
    classify_query, classify_params, classify_timeout, classify_settings = next(
        call for call in analytics.calls if candidate_param in call[1]
    )
    if observe_type == "trace":
        assert "SELECT DISTINCT trace_id" in seed_query
        assert seed_params["filter_anchor_limit"] == 513
    else:
        assert "LIMIT %(filter_seed_limit)s" in seed_query
        assert seed_params["filter_seed_limit"] <= 512
    assert "argMax(" in classify_query
    assert "FINAL" not in classify_query
    assert classify_params[candidate_param] in {("trace-1",), ("span-1",)}
    assert seed_timeout <= 750 and classify_timeout <= 750
    assert seed_settings["max_threads"] == classify_settings["max_threads"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_short_graph_map_filter_is_candidate_scoped(observe_type: str) -> None:
    identity_key = "trace_id" if observe_type == "trace" else "id"
    row = {
        identity_key: "trace-1" if observe_type == "trace" else "span-1",
        "root_span_id": "span-1",
        "start_time": START + timedelta(minutes=4),
    }
    if observe_type == "span":
        row["trace_id"] = "trace-1"
    analytics = _CandidateAnalytics(observe_type=observe_type, rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(),
            _attribute_filter(
                "customer.context",
                {"tier": "vip", "attempt": 2},
                filter_type="json",
                filter_op="contains",
            ),
        ],
        observe_type=observe_type,
    )

    assert sample.rows == (row,)
    seed_query = analytics.calls[0][0]
    classify_query, classify_params, _, _ = next(
        call
        for call in analytics.calls
        if ("candidate_trace_ids" if observe_type == "trace" else "candidate_span_ids")
        in call[1]
    )
    assert "attributes_extra" not in seed_query
    assert "JSONExtractRaw(attributes_extra" in classify_query
    assert "vip" not in classify_query
    assert "vip" in classify_params.values()


@pytest.mark.unit
def test_long_graph_map_filter_uses_bounded_strata_and_candidate_classifiers() -> None:
    long_start = START - timedelta(days=180)
    row = {
        "id": "span-1",
        "trace_id": "trace-1",
        "start_time": START - timedelta(days=10),
    }
    analytics = _CandidateAnalytics(observe_type="span", rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(long_start, END),
            _attribute_filter(
                "customer.context",
                {"tier": "vip"},
                filter_type="map",
                filter_op="equals",
            ),
        ],
        observe_type="span",
        deadline_ms=8_000,
    )

    assert sample.rows == (row,)
    assert all(
        "attributes_extra" not in query
        for query, params, *_ in analytics.calls
        if "filter_seed_limit" in params
    )
    classifiers = [
        (query, params)
        for query, params, *_ in analytics.calls
        if "candidate_span_ids" in params
    ]
    assert classifiers
    assert all("JSONExtractRaw(attributes_extra" in query for query, _ in classifiers)


@pytest.mark.unit
def test_map_number_boolean_and_multiple_predicates_share_one_finite_classifier():
    row = {
        "id": "span-1",
        "trace_id": "trace-1",
        "start_time": START + timedelta(minutes=4),
    }
    analytics = _CandidateAnalytics(observe_type="span", rows=[row])
    filters = [
        _date_filter(),
        _attribute_filter("score", 0.5, filter_type="number", filter_op="greater_than"),
        _attribute_filter("accepted", True, filter_type="boolean"),
        _attribute_filter("final_status", ["Rechazado", "Aceptado"], filter_op="in"),
    ]

    read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=filters,
        observe_type="span",
    )
    query, params, _, _ = analytics.calls[1]
    assert "attrs_number" in query
    assert "attrs_bool" in query
    assert "attrs_string" in query
    assert query.count(" AND ") >= 3
    assert ("rechazado", "aceptado") in params.values()


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_mixed_annotation_graph_reuses_candidate_scoped_list_classifier(
    observe_type,
):
    identity_key = "trace_id" if observe_type == "trace" else "id"
    row = {
        identity_key: "trace-1" if observe_type == "trace" else "span-1",
        "start_time": START + timedelta(minutes=4),
    }
    if observe_type == "trace":
        row["root_span_id"] = "span-1"
    else:
        row["trace_id"] = "trace-1"
    analytics = _CandidateAnalytics(observe_type=observe_type, rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(),
            _attribute_filter("final_status", "Rechazado"),
            _annotation_filter(LABEL_ID, "approved"),
        ],
        observe_type=observe_type,
    )

    assert sample.query_complete is True
    assert sample.rows == (row,)
    classify_query, classify_params, _, _ = analytics.calls[1]
    candidate_param = (
        "candidate_trace_ids" if observe_type == "trace" else "candidate_span_ids"
    )
    assert "model_hub_score AS s FINAL" in classify_query
    assert f"%({candidate_param})s" in classify_query
    assert classify_params[candidate_param] == (
        "trace-1" if observe_type == "trace" else "span-1",
    )


@pytest.mark.unit
@pytest.mark.parametrize("filter_type", ["json", "map"])
def test_nested_json_and_map_filters_fail_closed_before_clickhouse_read(
    filter_type,
):
    analytics = _CandidateAnalytics(observe_type="span", rows=[])
    with pytest.raises(BoundedGraphReadError) as caught:
        read_graph_candidates(
            analytics=analytics,
            project_id=PROJECT_ID,
            filters=[
                _date_filter(),
                _attribute_filter(
                    "attributes_extra",
                    {"nested": {"value": "x"}},
                    filter_type=filter_type,
                ),
            ],
            observe_type="span",
        )
    assert caught.value.error_code == "unsupported_filter_shape"
    assert analytics.calls == []


@pytest.mark.unit
def test_customer_final_status_1090_rows_completes_below_any_span_ceiling():
    rows = [
        {
            "trace_id": f"trace-{index:04d}",
            "root_span_id": f"span-{index:04d}",
            "start_time": START + timedelta(seconds=index % 240),
            "latency_ms": index,
        }
        for index in range(1090)
    ]
    analytics = _CandidateAnalytics(observe_type="trace", rows=rows)

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        observe_type="trace",
    )

    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.query_error_code is None
    assert len(sample.rows) == 1090
    # One 513-ID cardinality probe plus three ordered 512-root batches and
    # their finite classifiers. The sentinel switches common values away from
    # an unordered full-window distinct scan without constructing a Set.
    assert 7 <= sample.query_count <= 10
    assert all(
        len(call[1].get("candidate_trace_ids", ())) <= 512 for call in analytics.calls
    )


@pytest.mark.unit
def test_sparse_root_trace_filter_is_not_rejected_by_query_count_preflight():
    row = {
        "trace_id": "trace-1",
        "root_span_id": "span-1",
        "start_time": START + timedelta(minutes=4),
        "latency_ms": 25.0,
    }
    analytics = _CandidateAnalytics(observe_type="trace", rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _system_text_filter("trace_id", "trace-1")],
        observe_type="trace",
    )

    assert sample.query_complete is True
    assert sample.rows == (row,)
    assert len(analytics.calls) >= 2
    seed_query, seed_params, *_ = analytics.calls[0]
    assert "ORDER BY start_time DESC, trace_id DESC" in seed_query
    assert seed_params["filter_seed_limit"] == 512


@pytest.mark.unit
def test_4096th_match_returns_visible_deterministic_sample_metadata():
    rows = [
        {
            "id": f"span-{index:04d}",
            "trace_id": f"trace-{index:04d}",
            "start_time": START + timedelta(milliseconds=index),
        }
        for index in range(4096)
    ]
    analytics = _CandidateAnalytics(observe_type="span", rows=rows)

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        observe_type="span",
    )

    assert len(sample.rows) == bounded_graph_reads.GRAPH_CANDIDATE_LIMIT
    assert sample.query_complete is False
    assert sample.query_status == "degraded"
    assert sample.query_error_code == "sample_limit"
    assert sample.metadata()["query_sample_size"] == len(sample.rows)
    assert sample.metadata()["query_total_rows_lower_bound"] >= len(sample.rows) + 1


@pytest.mark.unit
def test_1600th_root_trace_returns_visible_sample_instead_of_blank_error():
    rows = [
        {
            "trace_id": f"trace-{index:04d}",
            "root_span_id": f"span-{index:04d}",
            "start_time": START + timedelta(milliseconds=index),
        }
        for index in range(1600)
    ]
    analytics = _CandidateAnalytics(observe_type="trace", rows=rows)

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _system_text_filter("trace_id", "trace")],
        observe_type="trace",
    )

    assert len(sample.rows) == bounded_graph_reads.GRAPH_TRACE_ROOT_CANDIDATE_LIMIT
    assert sample.query_complete is False
    assert sample.query_status == "degraded"
    assert sample.query_error_code == "sample_limit"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("window_days", "selector_error", "public_error"),
    [
        (7, "deadline_exceeded", "read_budget_exceeded"),
        (180, "read_budget_exceeded", "read_budget_exceeded"),
        (365, "scan_budget_exceeded", "sample_limit"),
    ],
)
def test_long_window_incomplete_rows_are_sampled_only_for_cardinality_limits(
    monkeypatch, window_days, selector_error, public_error
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=window_days)
    partial_row = {
        "trace_id": "trace-proven-match",
        "root_span_id": "span-proven-match",
        "start_time": window_start + timedelta(minutes=1),
    }
    calls = []

    def _incomplete_page(**kwargs):
        calls.append(kwargs)
        return BoundedFilterPage(
            rows=[partial_row],
            has_more=False,
            complete=False,
            status="degraded",
            error_code=selector_error,
            total_rows_lower_bound=1,
            elapsed_ms=3899.0,
            query_count=24,
            rows_returned=25,
            result_payload_bytes=512,
            attempts=(),
        )

    monkeypatch.setattr(
        bounded_graph_reads, "read_bounded_filter_page", _incomplete_page
    )
    if public_error == "sample_limit":
        sample = read_graph_candidates(
            analytics=object(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(window_start, window_end),
                _attribute_filter("customer.final_status", "Rechazado"),
                _attribute_filter("score", 0.5, filter_type="number"),
            ],
            observe_type="trace",
        )
        assert sample.rows == (partial_row,)
        assert sample.query_complete is False
        assert sample.query_status == "degraded"
        assert sample.query_error_code == "sample_limit"
    else:
        with pytest.raises(BoundedGraphReadError) as caught:
            read_graph_candidates(
                analytics=object(),
                project_id=PROJECT_ID,
                filters=[
                    _date_filter(window_start, window_end),
                    _attribute_filter("customer.final_status", "Rechazado"),
                    _attribute_filter("score", 0.5, filter_type="number"),
                ],
                observe_type="trace",
            )
        assert caught.value.error_code == public_error

    expected_calls = (
        bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        if public_error == "sample_limit"
        else 1
    )
    assert len(calls) == expected_calls
    assert all(call["include_incomplete_rows"] is True for call in calls)
    assert all(call["max_query_count"] == 2 for call in calls)
    assert all(call["max_candidates"] == 50 for call in calls)


@pytest.mark.unit
def test_incomplete_read_without_a_proven_match_raises_only_a_sanitized_code(
    monkeypatch,
):
    monkeypatch.setattr(
        bounded_graph_reads,
        "read_bounded_filter_page",
        lambda **_: BoundedFilterPage(
            rows=[],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="deadline_exceeded",
            total_rows_lower_bound=0,
            elapsed_ms=3900.0,
            query_count=2,
            rows_returned=0,
            result_payload_bytes=0,
            attempts=(),
        ),
    )

    with pytest.raises(BoundedGraphReadError) as caught:
        read_graph_candidates(
            analytics=object(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(),
                _attribute_filter("score", 0.5, filter_type="number"),
            ],
            observe_type="span",
        )
    assert caught.value.error_code == "read_budget_exceeded"
    assert "ClickHouse" not in str(caught.value)


@pytest.mark.unit
def test_candidate_timeout_is_logged_internally_but_never_returned(monkeypatch):
    raw_error = "Code: 159 DB::Exception secret-host SELECT private_payload"
    partial_row = {
        "trace_id": "trace-proven-match",
        "root_span_id": "span-proven-match",
        "start_time": START + timedelta(minutes=1),
    }
    read_count = 0
    warning_calls = []

    def _page_or_failure(**_):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            raise ReadDeadlineExceeded(raw_error)
        return BoundedFilterPage(
            rows=[partial_row],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="scan_budget_exceeded",
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(
        bounded_graph_reads, "read_bounded_filter_page", _page_or_failure
    )
    monkeypatch.setattr(
        bounded_graph_reads,
        "logger",
        SimpleNamespace(
            warning=lambda *args, **kwargs: warning_calls.append((args, kwargs))
        ),
    )

    with pytest.raises(BoundedGraphReadError) as caught:
        read_graph_candidates(
            analytics=object(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(START, START + timedelta(days=7)),
                _attribute_filter("customer.final_status", "Rechazado"),
            ],
            observe_type="trace",
        )

    assert caught.value.error_code == "read_budget_exceeded"
    assert raw_error not in str(caught.value)
    assert warning_calls[0][1]["error_type"] == "ReadDeadlineExceeded"
    assert warning_calls[0][1]["exc_info"] is True
    assert raw_error not in str(warning_calls[0])


@pytest.mark.unit
def test_compiler_error_is_never_recast_as_a_cardinality_sample(monkeypatch):
    from clickhouse_driver.errors import ServerException

    raw_error = "Code: 47 DB::Exception Unknown identifier secret_column"
    compiler_error = ServerException(raw_error, code=47)
    monkeypatch.setattr(
        bounded_graph_reads,
        "read_bounded_filter_page",
        lambda **_: (_ for _ in ()).throw(compiler_error),
    )

    with pytest.raises(ServerException) as caught:
        read_graph_candidates(
            analytics=object(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(START, START + timedelta(days=180)),
                _attribute_filter("customer.final_status", "Rechazado"),
            ],
            observe_type="trace",
        )

    assert caught.value is compiler_error
    sanitized = graph_dispatch.degraded_graph_response("latency", caught.value)
    assert sanitized["query_complete"] is False
    assert sanitized["query_error_code"] == "query_failed"
    assert raw_error not in str(sanitized)


@pytest.mark.unit
@pytest.mark.parametrize("window_days", [7, 180, 365])
def test_sparse_old_and_new_matches_cover_every_long_window_stratum(window_days):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=window_days)
    rows = [
        {
            "trace_id": "trace-old",
            "root_span_id": "span-old",
            "start_time": window_start + timedelta(minutes=1),
        },
        {
            "trace_id": "trace-new",
            "root_span_id": "span-new",
            "start_time": window_end - timedelta(minutes=1),
        },
    ]
    analytics = _CandidateAnalytics(observe_type="trace", rows=rows)

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rechazado"),
        ],
        observe_type="trace",
    )

    assert sample.query_complete is True
    assert {row["trace_id"] for row in sample.rows} == {"trace-old", "trace-new"}
    seed_windows = sorted(
        {
            (call[1]["filter_slice_start"], call[1]["filter_slice_end"])
            for call in analytics.calls
            if "filter_slice_start" in call[1]
        }
    )
    assert len(seed_windows) == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert seed_windows[0][0] == window_start
    assert seed_windows[-1][1] == window_end
    assert all(
        seed_windows[index][1] == seed_windows[index + 1][0]
        for index in range(len(seed_windows) - 1)
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("lower_op", "upper_op"),
    [
        ("greater_than", "less_than"),
        ("greater_than_or_equal", "less_than_or_equal"),
    ],
)
def test_long_window_scalar_datetime_bounds_are_canonicalized_per_stratum(
    lower_op,
    upper_op,
):
    window_start = datetime(2026, 1, 1)
    window_end = window_start + timedelta(days=7)
    row = {
        "trace_id": "trace-middle",
        "root_span_id": "span-middle",
        "start_time": window_start + timedelta(days=3),
    }
    analytics = _CandidateAnalytics(observe_type="trace", rows=[row])
    filters = [
        _date_bound_filter(lower_op, window_start),
        _date_bound_filter(upper_op, window_end),
        _attribute_filter("final_status", "Rechazado"),
    ]

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=filters,
        observe_type="trace",
    )

    assert sample.rows == (row,)
    assert sample.query_complete is True
    assert filters[0]["filter_config"]["filter_op"] == lower_op
    assert filters[1]["filter_config"]["filter_op"] == upper_op
    seed_windows = {
        (call[1]["filter_slice_start"], call[1]["filter_slice_end"])
        for call in analytics.calls
        if "filter_slice_start" in call[1]
    }
    assert len(seed_windows) == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA


@pytest.mark.unit
def test_empty_long_window_is_exact_and_not_mislabeled_as_sampled():
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=180)
    analytics = _CandidateAnalytics(observe_type="trace", rows=[])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rechazado"),
        ],
        observe_type="trace",
    )

    assert sample.rows == ()
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.query_error_code is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observe_type", "window_days", "row_count"),
    [("trace", 180, 1600), ("span", 365, 4096)],
)
def test_high_cardinality_long_window_sample_is_deterministic_and_distributed(
    observe_type, window_days, row_count
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=window_days)
    window_width = window_end - window_start
    rows = []
    for index in range(row_count):
        row = {
            "trace_id": f"trace-{index:05d}",
            "root_span_id": f"root-{index:05d}",
            "start_time": window_start + (window_width * index / row_count),
        }
        if observe_type == "span":
            row["id"] = f"span-{index:05d}"
        rows.append(row)

    def _read_once():
        return read_graph_candidates(
            analytics=_CandidateAnalytics(observe_type=observe_type, rows=rows),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(window_start, window_end),
                _attribute_filter("final_status", "Rechazado"),
            ],
            observe_type=observe_type,
        )

    first = _read_once()
    second = _read_once()
    identity_field = "trace_id" if observe_type == "trace" else "id"
    first_ids = tuple(row[identity_field] for row in first.rows)
    second_ids = tuple(row[identity_field] for row in second.rows)

    assert first_ids == second_ids
    assert len(first.rows) == (
        bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        * bounded_graph_reads.GRAPH_ANY_SPAN_ROWS_PER_STRATUM
    )
    assert first.query_complete is False
    assert first.query_status == "degraded"
    assert first.query_error_code == "sample_limit"
    assert first.total_rows_lower_bound >= len(first.rows) + 8
    stratum_width = window_width / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert all(
        any(
            window_start + stratum_width * index
            <= row["start_time"]
            < window_start + stratum_width * (index + 1)
            for row in first.rows
        )
        for index in range(bounded_graph_reads.GRAPH_ANY_SPAN_STRATA)
    )


@pytest.mark.unit
def test_distributed_span_sample_keeps_reused_ids_trace_scoped(monkeypatch):
    window_end = START + timedelta(days=7)
    rows = (
        {"trace_id": "trace-a", "id": "shared", "start_time": START},
        {"trace_id": "trace-b", "id": "shared", "start_time": START},
    )
    monkeypatch.setattr(
        bounded_graph_reads,
        "read_bounded_filter_page",
        lambda **_: BoundedFilterPage(
            rows=list(rows),
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=2,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        ),
    )

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, window_end),
            _attribute_filter("final_status", "Rechazado"),
        ],
        observe_type="span",
    )

    assert {(row["trace_id"], row["id"]) for row in sample.rows} == {
        ("trace-a", "shared"),
        ("trace-b", "shared"),
    }


@pytest.mark.unit
def test_system_candidate_graph_covers_all_metric_families_and_zero_fills():
    sample = GraphCandidateSample(
        rows=(
            {
                "id": "span-1",
                "start_time": START + timedelta(minutes=1),
                "latency_ms": 20,
                "cost": 0.2,
                "total_tokens": 10,
                "prompt_tokens": 4,
                "completion_tokens": 6,
                "status": "ERROR",
            },
            {
                "id": "span-2",
                "start_time": START + timedelta(minutes=2),
                "latency_ms": 40,
                "cost": 0.4,
                "total_tokens": 30,
                "prompt_tokens": 12,
                "completion_tokens": 18,
                "status": "OK",
            },
        ),
        query_complete=True,
        query_status="complete",
        query_error_code=None,
        window_start=START,
        window_end=END,
        elapsed_ms=12.0,
        query_count=2,
        rows_returned=4,
        result_payload_bytes=100,
        total_rows_lower_bound=2,
    )
    expected = {
        "latency": 30.0,
        "traffic": 2.0,
        "tokens": 40.0,
        "cost": 0.3,
        "error_rate": 50.0,
        "prompt_tokens": 16.0,
        "completion_tokens": 24.0,
    }
    for metric_id, value in expected.items():
        response = aggregate_system_candidate_graph(
            sample, metric_id=metric_id, interval="hour"
        )
        assert response["data"][0]["value"] == value
        assert response["data"][0]["primary_traffic"] == 2
        assert response["query_complete"] is True


def _sample() -> GraphCandidateSample:
    return GraphCandidateSample(
        rows=(
            {
                "trace_id": "11111111-1111-4111-8111-111111111111",
                "root_span_id": "span-1",
                "start_time": START + timedelta(minutes=1),
            },
        ),
        query_complete=True,
        query_status="complete",
        query_error_code=None,
        window_start=START,
        window_end=END,
        elapsed_ms=5,
        query_count=2,
        rows_returned=2,
        result_payload_bytes=20,
        total_rows_lower_bound=1,
    )


@pytest.mark.unit
def test_system_graph_rejects_partial_rows_instead_of_rendering_a_sample(
    monkeypatch,
):
    incomplete = replace(
        _sample(),
        rows=(
            {
                "id": "span-proven-match",
                "trace_id": "trace-proven-match",
                "start_time": START + timedelta(minutes=1),
                "latency_ms": 25,
                "status": "OK",
            },
        ),
        query_complete=False,
        query_status="degraded",
        query_error_code="read_budget_exceeded",
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: incomplete)

    with pytest.raises(BoundedGraphReadError) as caught:
        graph_dispatch.fetch_system_metric_graph_ch(
            analytics=object(),
            project_id=PROJECT_ID,
            filters=[_date_filter(), _attribute_filter("score", 0.5)],
            interval="hour",
            metric_id="latency",
            observe_type="span",
        )

    assert caught.value.error_code == "read_budget_exceeded"


@pytest.mark.unit
def test_system_graph_returns_cardinality_diagnostics_without_sampled_data(
    monkeypatch,
):
    incomplete = replace(
        _sample(),
        rows=(
            {
                "id": "span-proven-match",
                "trace_id": "trace-proven-match",
                "start_time": START + timedelta(minutes=1),
                "latency_ms": 25,
                "status": "OK",
            },
        ),
        query_complete=False,
        query_status="degraded",
        query_error_code="sample_limit",
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: incomplete)

    response = graph_dispatch.fetch_system_metric_graph_ch(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("score", 0.5)],
        interval="hour",
        metric_id="latency",
        observe_type="span",
    )

    assert response["data"] == []
    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"
    assert response["query_sample_size"] == 1


@pytest.mark.unit
def test_trace_system_graph_does_not_decorate_or_publish_candidate_sample(monkeypatch):
    trace_id = "11111111-1111-4111-8111-111111111111"
    sampled = replace(
        _sample(),
        query_complete=False,
        query_status="degraded",
        query_error_code="sample_limit",
    )
    analytics = _SequenceAnalytics(
        [
            [{"trace_id": trace_id, "id": "span-1", "start_time": START}],
            [
                {
                    "time_bucket": START.replace(minute=0),
                    "avg_latency": 12,
                    "total_tokens": 5,
                    "avg_cost": 0.1,
                    "traffic_count": 1,
                    "prompt_tokens": 2,
                    "completion_tokens": 3,
                    "error_rate": 0,
                }
            ],
        ]
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: sampled)

    response = graph_dispatch.fetch_system_metric_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        metric_id="latency",
        observe_type="trace",
    )

    assert response["data"] == []
    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"
    assert analytics.calls == []


class _DecorationAnalytics:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params, timeout_ms, settings))
        return _Result(self.rows)


class _SequenceAnalytics:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params, timeout_ms, settings))
        return _Result(self.responses.pop(0))


@pytest.mark.unit
def test_time_only_system_graph_uses_rollup_not_raw_spans():
    analytics = _DecorationAnalytics(
        [
            {
                "time_bucket": START.replace(minute=0),
                "avg_latency": 12.5,
                "total_tokens": 10,
                "avg_cost": 0.01,
                "traffic_count": 2,
                "prompt_tokens": 4,
                "completion_tokens": 6,
                "error_rate": 0,
            }
        ]
    )

    response = graph_dispatch.fetch_system_metric_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter()],
        interval="hour",
        metric_id="latency",
        observe_type="trace",
    )

    query, _, timeout_ms, settings = analytics.calls[0]
    assert "FROM spans_hourly_rollup" in query
    assert "FROM spans\n" not in query
    assert timeout_ms <= 1200
    assert settings["max_threads"] == 1
    assert settings["max_result_rows"] == 10_001
    assert response["query_complete"] is True
    assert response["query_status"] == "complete"


@pytest.mark.unit
def test_filtered_graph_never_routes_to_broad_final_or_nonreversible_attr_rollup():
    source = inspect.getsource(graph_dispatch.fetch_system_metric_graph_ch)

    assert "FROM spans FINAL" not in source
    assert "dashboard_attr_rollup" not in source
    assert "read_graph_candidates" in source


@pytest.mark.unit
def test_one_year_hourly_graph_is_not_constrained_by_event_result_cap():
    window_end = datetime(2026, 7, 31)
    window_start = window_end - timedelta(days=365)
    analytics = _DecorationAnalytics([])

    response = graph_dispatch.fetch_system_metric_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(window_start, window_end)],
        interval="hour",
        metric_id="latency",
        observe_type="trace",
    )

    _, params, _, settings = analytics.calls[0]
    assert settings["max_result_rows"] == 10_001
    assert params["start_date"] == window_start
    assert params["end_date"] == window_end
    assert len(response["data"]) > 2_001
    assert len(response["data"]) <= 10_000
    assert response["query_complete"] is True


@pytest.mark.unit
def test_filtered_trace_system_graph_aggregates_all_live_child_spans(monkeypatch):
    trace_id = "11111111-1111-4111-8111-111111111111"
    analytics = _SequenceAnalytics(
        [
            [
                {"trace_id": trace_id, "id": "span-1", "start_time": START},
                {
                    "trace_id": trace_id,
                    "id": "span-child-1",
                    "start_time": START + timedelta(minutes=1),
                },
                {
                    "trace_id": trace_id,
                    "id": "span-child-2",
                    "start_time": START + timedelta(minutes=2),
                },
            ],
            [
                {
                    "time_bucket": START.replace(minute=0),
                    "avg_latency": 30,
                    "total_tokens": 33,
                    "avg_cost": 0.2,
                    "traffic_count": 3,
                    "prompt_tokens": 12,
                    "completion_tokens": 21,
                    "error_rate": 100 / 3,
                }
            ],
        ]
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: _sample())

    response = graph_dispatch.fetch_system_metric_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        metric_id="traffic",
        observe_type="trace",
    )

    seed_query, seed_params, seed_timeout_ms, seed_settings = analytics.calls[0]
    query, params, timeout_ms, settings = analytics.calls[1]
    assert "FROM spans" in seed_query
    assert "GROUP BY trace_id, id, start_time" in seed_query
    assert seed_params["graph_entity_limit"] == 4096
    assert seed_timeout_ms <= 1_200
    assert seed_settings["max_result_rows"] == 4096
    assert "FROM spans" in query
    assert "argMax(" in query
    assert "GROUP BY trace_id, id, start_time" in query
    assert "FINAL" not in query
    assert "IN (SELECT" not in query.upper()
    replay_scope = query.split("FROM spans", 1)[1].split(
        "GROUP BY trace_id, id, start_time", 1
    )[0]
    assert "id IN %(graph_span_ids)s" in replay_scope
    assert "project_id = toUUID(%(graph_project_id)s)" in replay_scope
    assert "trace_id IN %(graph_trace_ids)s" in replay_scope
    assert "toUnixTimestamp64Micro(start_time)" in replay_scope
    assert "IN %(graph_span_identities)s" in replay_scope
    assert "toDate(start_time) IN %(graph_span_dates)s" in replay_scope
    assert params["graph_project_id"] == PROJECT_ID
    assert params["graph_trace_ids"] == ("11111111-1111-4111-8111-111111111111",)
    assert params["graph_span_ids"] == (
        "span-1",
        "span-child-1",
        "span-child-2",
    )
    assert params["graph_span_identities"] == (
        (trace_id, "span-1", _unix_microseconds(START)),
        (
            trace_id,
            "span-child-1",
            _unix_microseconds(START + timedelta(minutes=1)),
        ),
        (
            trace_id,
            "span-child-2",
            _unix_microseconds(START + timedelta(minutes=2)),
        ),
    )
    assert params["graph_span_dates"] == (START.date(),)
    assert params["graph_start_date"] == START
    assert params["graph_end_date"] == END
    assert params["graph_point_limit"] == 10_001
    assert timeout_ms <= 1_200
    assert settings["max_result_rows"] == 10_001
    assert response["data"][0]["value"] == 3
    assert response["data"][0]["primary_traffic"] == 3
    assert response["query_complete"] is True


@pytest.mark.unit
def test_filtered_eval_graph_is_candidate_scoped_no_final_or_membership_subquery(
    monkeypatch,
):
    analytics = _DecorationAnalytics(
        [
            {
                "created_at": START + timedelta(minutes=2),
                "output_bool": None,
                "output_float": 0.75,
                "output_str": None,
                "output_str_list": "[]",
                "error": 0,
            }
        ]
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: _sample())

    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        req_data_config={"id": EVAL_ID, "type": "EVAL", "output_type": "SCORE"},
        observe_type="trace",
    )

    query, params, timeout_ms, settings = analytics.calls[0]
    assert "FINAL" not in query
    assert "IN (SELECT" not in query.upper()
    assert "LIMIT 1 BY id" in query
    assert params["graph_event_limit"] == 2001
    assert timeout_ms <= 900
    assert settings["max_threads"] == 1
    assert settings["max_result_rows"] == 2001
    assert response["data"][0]["value"] == 75.0
    assert response["query_complete"] is True
    assert response["query_status"] == "complete"


@pytest.mark.unit
def test_unfiltered_eval_uses_configured_live_logger_and_project_candidates(
    monkeypatch,
):
    analytics = _DecorationAnalytics(
        [
            {
                "created_at": START + timedelta(minutes=2),
                "output_bool": True,
                "output_float": None,
                "output_str": None,
                "output_str_list": "[]",
                "error": 0,
            }
        ]
    )
    candidate_calls = []

    def _candidates(**kwargs):
        candidate_calls.append(kwargs)
        return _sample()

    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", _candidates)
    monkeypatch.setattr(
        graph_dispatch,
        "eval_logger_source",
        lambda: ("tracer_eval_logger_v2", "is_deleted = 0"),
    )

    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter()],
        interval="hour",
        req_data_config={
            "id": EVAL_ID,
            "type": "EVAL",
            "output_type": "PASS_FAIL",
            "value": True,
        },
        observe_type="trace",
    )

    query, params, timeout_ms, settings = analytics.calls[0]
    assert candidate_calls[0]["project_id"] == PROJECT_ID
    assert candidate_calls[0]["allow_time_only_seed"] is True
    assert "FROM tracer_eval_logger_v2" in query
    assert "eval_metrics_hourly" not in query
    assert "LIMIT 1 BY id" in query
    assert "FINAL" not in query
    assert "IN (SELECT" not in query.upper()
    assert params["graph_eval_config_id"] == EVAL_ID
    assert params["graph_trace_ids"] == ("11111111-1111-4111-8111-111111111111",)
    assert params["graph_start_date"] == START
    assert params["graph_end_date"] == END
    assert timeout_ms <= 900
    assert settings["max_result_rows"] == 2001
    assert response["data"][0]["value"] == 100
    assert response["query_complete"] is True
    assert response["query_status"] == "complete"


@pytest.mark.unit
def test_annotation_graph_is_candidate_scoped_and_score_values_are_sanitized(
    monkeypatch,
):
    trace_id = "11111111-1111-4111-8111-111111111111"
    identities = [
        {"trace_id": trace_id, "id": "span-1", "start_time": START},
        {
            "trace_id": trace_id,
            "id": "span-child",
            "start_time": START + timedelta(minutes=1),
        },
    ]
    analytics = _SequenceAnalytics(
        [
            identities,
            identities,
            [
                {
                    "created_at": START + timedelta(minutes=3),
                    "value": '{"rating": 4.5}',
                }
            ],
        ]
    )
    label = SimpleNamespace(
        id=LABEL_ID,
        name="Quality",
        type=AnnotationTypeChoices.NUMERIC.value,
    )
    label_query = SimpleNamespace(get=lambda **_: label)
    monkeypatch.setattr(
        graph_dispatch, "get_annotation_labels_for_project", lambda _: label_query
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: _sample())

    response = graph_dispatch.fetch_annotation_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter()],
        interval="hour",
        req_data_config={"id": LABEL_ID, "type": "ANNOTATION"},
        observe_type="trace",
    )

    seed_query, seed_params, seed_timeout, seed_settings = analytics.calls[0]
    identity_query, identity_params, identity_timeout, identity_settings = (
        analytics.calls[1]
    )
    score_query, score_params, score_timeout, score_settings = analytics.calls[2]
    assert "FROM spans" in seed_query
    assert "GROUP BY trace_id, id, start_time" in seed_query
    assert seed_params["graph_entity_limit"] == 4096
    assert seed_timeout <= 900
    assert seed_settings["max_result_rows"] == 4096
    assert "FROM spans" in identity_query
    assert "argMax(" in identity_query
    assert "FINAL" not in identity_query
    assert "IN (SELECT" not in identity_query.upper()
    assert identity_params["graph_trace_ids"] == (trace_id,)
    assert identity_params["graph_span_ids"] == ("span-1", "span-child")
    assert identity_params["graph_span_identities"] == (
        (trace_id, "span-1", _unix_microseconds(START)),
        (
            trace_id,
            "span-child",
            _unix_microseconds(START + timedelta(minutes=1)),
        ),
    )
    assert identity_params["graph_span_dates"] == (START.date(),)
    assert identity_params["graph_entity_limit"] == 4096
    assert identity_timeout <= 900
    assert identity_settings["max_result_rows"] == 4096
    replay_scope = identity_query.split("FROM spans", 1)[1].split(
        "GROUP BY trace_id, id, start_time", 1
    )[0]
    assert "id IN %(graph_span_ids)s" in replay_scope
    assert "project_id = toUUID(%(graph_project_id)s)" in replay_scope
    assert "trace_id IN %(graph_trace_ids)s" in replay_scope
    assert "toUnixTimestamp64Micro(start_time)" in replay_scope
    assert "IN %(graph_span_identities)s" in replay_scope
    assert "toDate(start_time) IN %(graph_span_dates)s" in replay_scope
    assert "is_deleted" not in replay_scope
    assert "FINAL" not in score_query
    assert "IN (SELECT" not in score_query.upper()
    assert "LIMIT 1 BY id" in score_query
    assert "tracer_project_id AS graph_score_project_id" in score_query
    assert "graph_score_project_id = toUUID(%(graph_project_id)s)" in score_query
    assert score_params["graph_project_id"] == PROJECT_ID
    assert score_params["graph_span_entities"] == (
        (trace_id, "span-1"),
        (trace_id, "span-child"),
    )
    assert score_params["graph_event_limit"] == 2001
    assert score_timeout <= 900
    assert score_settings["max_result_rows"] == 2001
    assert response["metric_name"] == LABEL_ID
    assert response["name"] == "Quality"
    assert response["data"][0]["value"] == 4.5
    assert response["query_complete"] is True
    assert response["query_status"] == "complete"


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_annotation_latest_state_uses_trace_scoped_entities_without_schema_column(
    observe_type,
):
    sample = _sample()
    if observe_type == "span":
        sample = replace(sample, rows=({"id": "colliding-span", "trace_id": "t"},))
    analytics = _DecorationAnalytics(
        [{"created_at": START + timedelta(minutes=1), "value": '{"value": 1}'}]
    )

    graph_dispatch._finite_annotation_rows(
        analytics=analytics,
        sample=sample,
        project_id=PROJECT_ID,
        observe_type=observe_type,
        trace_span_identities=(
            (("t", "colliding-span", _unix_microseconds(START)),)
            if observe_type == "trace"
            else ()
        ),
        label_id=LABEL_ID,
        started=graph_dispatch.monotonic(),
    )

    query, params, _, _ = analytics.calls[0]
    dedup_query, latest_predicates = query.split("LIMIT 1 BY id", 1)
    assert "tracer_project_id AS graph_score_project_id" in dedup_query
    assert "tracer_project_id = toUUID(%(graph_project_id)s)" in dedup_query
    assert "graph_score_project_id = toUUID(%(graph_project_id)s)" in latest_predicates
    assert params["graph_project_id"] == PROJECT_ID
    entity_ids = params.get("graph_trace_ids") or params.get("graph_span_entities")
    assert entity_ids


@pytest.mark.unit
@pytest.mark.parametrize("scope_builder", ["eval", "annotation"])
def test_external_span_scope_excludes_all_null_trace_bare_id_fallback(scope_builder):
    sample = replace(
        _sample(),
        rows=(
            {"trace_id": "trace-a", "id": "shared", "start_time": START},
            {"trace_id": "trace-b", "id": "shared", "start_time": START},
            {"trace_id": "trace-c", "id": "unique", "start_time": START},
        ),
    )

    if scope_builder == "eval":
        predicate, params = graph_dispatch._eval_entity_scope(sample, "span")
    else:
        predicate, params = graph_dispatch._annotation_entity_scope(sample, "span", ())

    assert params["graph_span_entities"] == (
        ("trace-a", "shared"),
        ("trace-b", "shared"),
        ("trace-c", "unique"),
    )
    assert "NOT isNull(trace_id)" in predicate
    assert "observation_span_id IN" not in predicate
    assert set(params) == {"graph_span_entities"}


@pytest.mark.unit
def test_trace_span_replay_resolves_scope_and_tombstones_from_global_latest_state():
    trace_id = "11111111-1111-4111-8111-111111111111"
    candidate_ids = (
        (trace_id, "stable", START),
        (trace_id, "moved-trace", START + timedelta(seconds=1)),
        (trace_id, "moved-window", START + timedelta(seconds=2)),
        (trace_id, "moved-project", START + timedelta(seconds=3)),
        (trace_id, "tombstoned", START + timedelta(seconds=4)),
    )
    analytics = _SequenceAnalytics(
        [
            [
                {"trace_id": identity[0], "id": identity[1], "start_time": identity[2]}
                for identity in candidate_ids
            ],
            [{"trace_id": trace_id, "id": "stable", "start_time": START}],
        ]
    )

    span_ids, truncated, query_count, rows_returned = (
        graph_dispatch._finite_trace_span_ids(
            analytics=analytics,
            sample=_sample(),
            project_id=PROJECT_ID,
            started=graph_dispatch.monotonic(),
        )
    )

    assert span_ids == ((trace_id, "stable", _unix_microseconds(START)),)
    assert truncated is False
    assert query_count == 2
    assert rows_returned == 6
    seed_query, seed_params, _, _ = analytics.calls[0]
    replay_query, replay_params, _, _ = analytics.calls[1]
    assert seed_params["graph_trace_ids"] == ("11111111-1111-4111-8111-111111111111",)
    assert "project_id = toUUID(%(graph_project_id)s)" in seed_query
    assert "trace_id IN %(graph_trace_ids)s" in seed_query
    assert "start_time >= %(graph_start_date)s" in seed_query
    assert replay_params["graph_span_ids"] == tuple(
        identity[1] for identity in candidate_ids
    )
    assert replay_params["graph_span_identities"] == tuple(
        (trace, span, _unix_microseconds(started_at))
        for trace, span, started_at in candidate_ids
    )
    assert replay_params["graph_span_dates"] == (START.date(),)
    replay_scope = replay_query.split("FROM spans", 1)[1].split(
        "GROUP BY trace_id, id, start_time", 1
    )[0]
    assert "project_id = toUUID(%(graph_project_id)s)" in replay_scope
    assert "trace_id IN %(graph_trace_ids)s" in replay_scope
    assert "toUnixTimestamp64Micro(start_time)" in replay_scope
    assert "IN %(graph_span_identities)s" in replay_scope
    assert "toDate(start_time) IN %(graph_span_dates)s" in replay_scope
    assert "argMax(is_deleted, _version)" in replay_query
    assert "latest_start_time >= %(graph_start_date)s" in replay_query
    assert "latest_is_deleted = 0" in replay_query


@pytest.mark.unit
def test_eval_event_sentinel_returns_degraded_metadata_without_sampled_data(
    monkeypatch,
):
    analytics = _DecorationAnalytics(
        [
            {
                "created_at": START + timedelta(minutes=2),
                "output_bool": None,
                "output_float": 0.5,
                "output_str": None,
                "output_str_list": "[]",
                "error": 0,
            }
            for _ in range(2001)
        ]
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: _sample())

    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        req_data_config={"id": EVAL_ID, "type": "EVAL", "output_type": "SCORE"},
        observe_type="trace",
    )

    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"
    assert response["data"] == []


@pytest.mark.unit
def test_eval_non_exhaustive_candidate_prefix_is_not_queried_or_published(monkeypatch):
    analytics = _DecorationAnalytics([])
    incomplete = replace(
        _sample(),
        query_complete=False,
        query_status="degraded",
        query_error_code="sample_limit",
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: incomplete)

    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        req_data_config={
            "id": EVAL_ID,
            "type": "EVAL",
            "output_type": "SCORE",
        },
        observe_type="trace",
    )

    assert analytics.calls == []
    assert response["data"] == []
    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"


@pytest.mark.unit
def test_annotation_non_exhaustive_candidate_prefix_is_not_queried_or_published(
    monkeypatch,
):
    sample = replace(
        _sample(),
        rows=(
            {
                "trace_id": "trace-proven-match",
                "id": "span-proven-match",
                "start_time": START + timedelta(minutes=1),
            },
        ),
        query_complete=False,
        query_status="degraded",
        query_error_code="sample_limit",
    )
    analytics = _DecorationAnalytics([])
    label = SimpleNamespace(
        id=LABEL_ID,
        name="Quality",
        type=AnnotationTypeChoices.NUMERIC.value,
    )
    monkeypatch.setattr(
        graph_dispatch,
        "get_annotation_labels_for_project",
        lambda _: SimpleNamespace(get=lambda **__: label),
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: sample)

    response = graph_dispatch.fetch_annotation_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rechazado")],
        interval="hour",
        req_data_config={"id": LABEL_ID, "type": "ANNOTATION"},
        observe_type="span",
    )

    assert analytics.calls == []
    assert response["data"] == []
    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"


@pytest.mark.unit
def test_annotation_child_span_identity_sentinel_stays_degraded(monkeypatch):
    trace_id = "11111111-1111-4111-8111-111111111111"
    analytics = _SequenceAnalytics(
        [
            [
                {
                    "trace_id": trace_id,
                    "id": f"span-{index}",
                    "start_time": START + timedelta(microseconds=index),
                }
                for index in range(4096)
            ],
            [
                {
                    "trace_id": trace_id,
                    "id": f"span-{index}",
                    "start_time": START + timedelta(microseconds=index),
                }
                for index in range(4095)
            ],
            [],
        ]
    )
    label = SimpleNamespace(
        id=LABEL_ID,
        name="Quality",
        type=AnnotationTypeChoices.NUMERIC.value,
    )
    label_query = SimpleNamespace(get=lambda **_: label)
    monkeypatch.setattr(
        graph_dispatch, "get_annotation_labels_for_project", lambda _: label_query
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: _sample())

    response = graph_dispatch.fetch_annotation_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter()],
        interval="hour",
        req_data_config={"id": LABEL_ID, "type": "ANNOTATION"},
        observe_type="trace",
    )

    assert len(analytics.calls[1][1]["graph_span_identities"]) == 4095
    assert len(analytics.calls[2][1]["graph_span_entities"]) == 4095
    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"
    assert response["data"] == []


@pytest.mark.unit
def test_degraded_response_never_contains_clickhouse_stack_or_raw_message():
    from clickhouse_driver.errors import ServerException

    raw = "Code: 159. DB::Exception Timeout exceeded secret-host stack trace"
    response = graph_dispatch.degraded_graph_response(
        "latency", BoundedGraphReadError("read_budget_exceeded")
    )
    assert response["query_error_code"] == "read_budget_exceeded"
    assert raw not in str(response)
    response = graph_dispatch.degraded_graph_response("latency", RuntimeError(raw))
    assert response["query_error_code"] == "query_failed"
    assert raw not in str(response)

    response = graph_dispatch.degraded_graph_response(
        "latency", ServerException(raw, code=159)
    )
    assert response["query_error_code"] == "read_budget_exceeded"
    assert raw not in str(response)


@pytest.mark.unit
def test_graph_response_contract_accepts_complete_or_degraded_not_sampled():
    from tracer.serializers.filters import ObserveGraphDataResultSerializer

    complete = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [],
            "query_complete": True,
            "query_status": "complete",
        }
    )
    degraded = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [],
            "query_complete": False,
            "query_status": "degraded",
            "query_error_code": "sample_limit",
        }
    )
    invalid = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [],
            "query_complete": False,
            "query_status": "sampled",
        }
    )
    invalid_sampled_data = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [
                {
                    "timestamp": "2026-08-03T00:00:00Z",
                    "value": 999,
                    "primary_traffic": 999,
                }
            ],
            "query_complete": False,
            "query_status": "degraded",
            "query_error_code": "sample_limit",
        }
    )
    assert complete.is_valid(), complete.errors
    assert degraded.is_valid(), degraded.errors
    assert not invalid.is_valid()
    assert not invalid_sampled_data.is_valid()
    assert "data" in invalid_sampled_data.errors


@pytest.mark.unit
def test_graph_views_bind_v2_and_have_no_postgres_telemetry_fallback():
    from tracer.views.observation_span import ObservationSpanView
    from tracer.views.trace import TraceView

    for view in (TraceView, ObservationSpanView):
        source = inspect.getsource(view.get_graph_methods)
        assert "V2AnalyticsQueryService" in source
        assert "AnalyticsQueryService()" not in source.replace(
            "V2AnalyticsQueryService()", ""
        )
        assert "_system_metric_graph_postgres" not in source
        assert "str(e)" not in source
        assert "str(exc)" not in source
        assert "isinstance(exc, BoundedGraphReadError)" in source
        assert "is_read_budget_error(exc)" in source
        assert "is_clickhouse_query_error(exc)" in source
        assert "raise" in source
