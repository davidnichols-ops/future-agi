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
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.read_budget import ReadDeadlineExceeded

PROJECT_ID = "00000000-0000-4000-8000-000000000901"
EVAL_ID = "00000000-0000-4000-8000-000000000902"
LABEL_ID = "00000000-0000-4000-8000-000000000903"
START = datetime(2026, 1, 1, 0, 0)
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
            anchor_start = params.get("filter_anchor_start")
            anchor_end = params.get("filter_anchor_end")
            anchor_rows = [
                row
                for row in self.rows
                if (
                    anchor_start is None
                    or anchor_end is None
                    or anchor_start <= row["start_time"] < anchor_end
                )
            ]
            if self.observe_type == "span":
                seen = set()
                rows = []
                for row in anchor_rows:
                    identity = (
                        str(row.get("trace_id") or ""),
                        str(row.get("id") or ""),
                        row.get("start_time"),
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    rows.append(
                        {
                            "project_id": PROJECT_ID,
                            "trace_id": identity[0],
                            "id": identity[1],
                            "start_time": identity[2],
                        }
                    )
                    if len(rows) >= params["filter_anchor_limit"]:
                        break
                return _Result(rows)
            return _Result(
                [
                    {"trace_id": trace_id}
                    for trace_id in list(
                        dict.fromkeys(str(row["trace_id"]) for row in anchor_rows)
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


class _LatestRareCandidateAnalytics(_CandidateAnalytics):
    """Model a common raw attribute whose latest-state match is rare."""

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "graph_eval_config_id" in params:
            self.calls.append((query, params, timeout_ms, settings))
            return _Result(
                [
                    {
                        "created_at": params["graph_start_date"] + timedelta(minutes=1),
                        "output_bool": None,
                        "output_float": 0.5,
                        "output_str": None,
                        "output_str_list": "[]",
                        "error": 0,
                    }
                ]
            )
        candidate_key = (
            "candidate_trace_ids"
            if self.observe_type == "trace"
            else "candidate_span_ids"
        )
        if candidate_key not in params:
            return super().execute_ch_query(
                query, params, timeout_ms=timeout_ms, settings=settings
            )
        self.calls.append((query, params, timeout_ms, settings))
        seed_column = "trace_id" if self.observe_type == "trace" else "id"
        allowed = set(params[candidate_key])
        return _Result(
            [
                row
                for row in self.rows
                if str(row[seed_column]) in allowed and row.get("matches_latest")
            ]
        )


class _CrossStratumTraceAnalytics:
    """Model one trace whose root and matching children occupy other strata."""

    def __init__(self, *, root_time: datetime, child_times: tuple[datetime, ...]):
        self.root_time = root_time
        self.child_times = child_times
        self.calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params, timeout_ms, settings))
        if "filter_seed_limit" in params:
            if (
                params["filter_slice_start"]
                <= self.root_time
                < params["filter_slice_end"]
            ):
                return _Result(
                    [
                        {
                            "trace_id": "cross-stratum-trace",
                            "root_span_id": "root-span",
                            "start_time": self.root_time,
                        }
                    ]
                )
            return _Result([])
        if "candidate_trace_ids" in params:
            request_start = params["candidate_start_date"]
            request_end = params["candidate_end_date"]
            children_match = all(
                request_start <= child_time < request_end
                for child_time in self.child_times
            )
            if (
                "cross-stratum-trace" in params["candidate_trace_ids"]
                and request_start <= self.root_time < request_end
                and children_match
            ):
                return _Result(
                    [
                        {
                            "trace_id": "cross-stratum-trace",
                            "root_span_id": "root-span",
                            "start_time": self.root_time,
                        }
                    ]
                )
            return _Result([])
        raise AssertionError("unexpected graph query")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observe_type", "key", "value"),
    [
        ("trace", "final_status", "Rejected"),
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
        assert "LIMIT %(filter_anchor_limit)s" in seed_query
        assert seed_params["filter_anchor_limit"] == 513
        assert "LIMIT 1 BY project_id, trace_id, id, start_time" in seed_query
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
def test_zero_width_trace_graph_window_is_exact_without_a_query() -> None:
    analytics = _CandidateAnalytics(observe_type="trace", rows=[])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, START),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.rows == ()
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.window_start == sample.window_end == START
    assert analytics.calls == []


@pytest.mark.unit
def test_one_microsecond_trace_graph_window_keeps_exact_membership_bounds() -> None:
    window_end = START + timedelta(microseconds=1)
    row = {
        "trace_id": "trace-short",
        "root_span_id": "root-short",
        "start_time": START,
    }
    analytics = _CandidateAnalytics(observe_type="trace", rows=[row])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.rows == (row,)
    classifier_params = next(
        params for _, params, *_ in analytics.calls if "candidate_trace_ids" in params
    )
    assert classifier_params["candidate_start_date"] == START
    assert classifier_params["candidate_end_date"] == window_end


@pytest.mark.unit
def test_default_long_window_is_frozen_once_for_every_trace_stratum(
    monkeypatch,
) -> None:
    """A missing date filter must not derive a new ``now`` per builder."""

    frozen_start = datetime(2026, 6, 1)
    frozen_end = frozen_start + timedelta(days=30)
    original_parse_time_range = BaseQueryBuilder.parse_time_range
    default_calls = 0

    def drifting_default(filters, *, strict=False):
        nonlocal default_calls
        has_positive_time_leaf = any(
            (item.get("column_id") or item.get("columnId"))
            in {"created_at", "start_time"}
            and not BaseQueryBuilder.is_datetime_complement_filter(item)
            for item in filters
        )
        if has_positive_time_leaf:
            return original_parse_time_range(filters, strict=strict)
        default_calls += 1
        drift = timedelta(microseconds=default_calls)
        return frozen_start + drift, frozen_end + drift

    monkeypatch.setattr(
        BaseQueryBuilder,
        "parse_time_range",
        staticmethod(drifting_default),
    )
    analytics = _CandidateAnalytics(observe_type="trace", rows=[])

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_system_text_filter("call_type", "LLM")],
        observe_type="trace",
    )

    assert sample.rows == ()
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert default_calls == 1
    seed_ranges = [
        (params["filter_slice_start"], params["filter_slice_end"])
        for _, params, *_ in analytics.calls
        if "filter_seed_limit" in params
    ]
    assert seed_ranges
    assert min(start for start, _ in seed_ranges) == frozen_start + timedelta(
        microseconds=1
    )
    assert max(end for _, end in seed_ranges) == frozen_end + timedelta(microseconds=1)


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
def test_trace_root_before_child_after_stratum_boundary_uses_full_membership_window():
    window_start = datetime(2026, 1, 1)
    window_end = window_start + timedelta(days=8)
    boundary = window_start + timedelta(days=1)
    root_time = boundary - timedelta(microseconds=1)
    child_time = boundary + timedelta(microseconds=1)
    analytics = _CrossStratumTraceAnalytics(
        root_time=root_time,
        child_times=(child_time,),
    )

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _system_text_filter("call_type", "LLM"),
        ],
        observe_type="trace",
    )

    assert tuple(row["trace_id"] for row in sample.rows) == ("cross-stratum-trace",)
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    seed_calls = [call for call in analytics.calls if "filter_seed_limit" in call[1]]
    assert all("parent_span_id IS NULL" in query for query, *_ in seed_calls)
    assert any(
        params["filter_slice_start"] <= root_time < params["filter_slice_end"]
        for _, params, *_ in seed_calls
    )
    classifier_params = next(
        params for _, params, *_ in analytics.calls if "candidate_trace_ids" in params
    )
    assert classifier_params["candidate_start_date"] == window_start
    assert classifier_params["candidate_end_date"] == window_end


@pytest.mark.unit
def test_trace_multi_child_filters_match_across_separate_temporal_strata():
    window_start = datetime(2026, 1, 1)
    window_end = window_start + timedelta(days=8)
    root_time = window_start + timedelta(hours=6)
    child_times = (
        window_start + timedelta(days=2, hours=6),
        window_start + timedelta(days=6, hours=6),
    )
    analytics = _CrossStratumTraceAnalytics(
        root_time=root_time,
        child_times=child_times,
    )

    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
            _attribute_filter(
                "customer.context",
                {"tier": "vip"},
                filter_type="json",
                filter_op="contains",
            ),
        ],
        observe_type="trace",
    )

    assert tuple(row["trace_id"] for row in sample.rows) == ("cross-stratum-trace",)
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    classifier_query, classifier_params, *_ = next(
        call for call in analytics.calls if "candidate_trace_ids" in call[1]
    )
    assert classifier_params["candidate_start_date"] == window_start
    assert classifier_params["candidate_end_date"] == window_end
    assert classifier_query.count("countIf(") >= 3
    assert "filter_witness_0" in classifier_query
    assert "filter_witness_1" in classifier_query


@pytest.mark.unit
def test_cross_stratum_trace_sample_is_full_coverage_and_never_marked_exact():
    window_start = datetime(2026, 1, 1)
    window_end = window_start + timedelta(days=8)
    stratum_width = (window_end - window_start) / 8
    root_times: dict[str, datetime] = {}
    child_times: dict[str, datetime] = {}
    for stratum in range(8):
        for index in range(60):
            trace_id = f"trace-{stratum}-{index:02d}"
            root_times[trace_id] = (
                window_start
                + (stratum_width * stratum)
                + timedelta(hours=6, microseconds=index)
            )
            child_stratum = stratum + 1 if stratum < 7 else stratum - 1
            child_times[trace_id] = (
                window_start
                + (stratum_width * child_stratum)
                + timedelta(hours=12, microseconds=index)
            )

    class _SampleAnalytics:
        def __init__(self):
            self.calls = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((query, params, timeout_ms, settings))
            if "filter_seed_limit" in params:
                rows = [
                    {
                        "trace_id": trace_id,
                        "root_span_id": f"root-{trace_id}",
                        "start_time": root_time,
                    }
                    for trace_id, root_time in root_times.items()
                    if params["filter_slice_start"]
                    <= root_time
                    < params["filter_slice_end"]
                ]
                rows.sort(
                    key=lambda row: (row["start_time"], row["trace_id"]),
                    reverse=True,
                )
                return _Result(rows[: params["filter_seed_limit"]])
            if "candidate_trace_ids" in params:
                request_start = params["candidate_start_date"]
                request_end = params["candidate_end_date"]
                return _Result(
                    [
                        {
                            "trace_id": trace_id,
                            "root_span_id": f"root-{trace_id}",
                            "start_time": root_times[trace_id],
                        }
                        for trace_id in params["candidate_trace_ids"]
                        if request_start <= root_times[trace_id] < request_end
                        and request_start <= child_times[trace_id] < request_end
                    ]
                )
            raise AssertionError("unexpected graph query")

    analytics = _SampleAnalytics()
    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
            _attribute_filter(
                "customer.context",
                {"tier": "vip"},
                filter_type="json",
                filter_op="contains",
            ),
        ],
        observe_type="trace",
    )

    assert len(sample.rows) == 8 * bounded_graph_reads.GRAPH_ANY_SPAN_ROWS_PER_STRATUM
    assert sample.query_complete is False
    assert sample.query_status == "sampled"
    assert sample.query_error_code == "sample_limit"
    assert sample.sampling_strata == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert sample.sampling_strata_completed == sample.sampling_strata
    classifier_calls = [
        call for call in analytics.calls if "candidate_trace_ids" in call[1]
    ]
    assert classifier_calls
    assert all(
        params["candidate_start_date"] == window_start
        and params["candidate_end_date"] == window_end
        for _, params, *_ in classifier_calls
    )


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
        _attribute_filter("final_status", ["Rejected", "Accepted"], filter_op="in"),
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
    assert ("rejected", "accepted") in params.values()


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
            _attribute_filter("final_status", "Rejected"),
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
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
def test_4096_matches_complete_at_exact_graph_ceiling():
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        observe_type="span",
    )

    assert len(sample.rows) == bounded_graph_reads.GRAPH_CANDIDATE_LIMIT
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.query_error_code is None
    assert sample.metadata()["query_sample_size"] == len(sample.rows)
    assert sample.metadata()["query_total_rows_lower_bound"] == len(sample.rows)


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
    assert sample.query_status == "sampled"
    assert sample.query_error_code == "sample_limit"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("window_days", "selector_error", "public_error"),
    [
        (7, "deadline_exceeded", "read_budget_exceeded"),
        (180, "read_budget_exceeded", "read_budget_exceeded"),
        (365, "sample_limit", "sample_limit"),
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
            elapsed_ms=1.0 if public_error == "sample_limit" else 3899.0,
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
                _attribute_filter("customer.final_status", "Rejected"),
                _attribute_filter("score", 0.5, filter_type="number"),
            ],
            observe_type="trace",
        )
        assert sample.rows == (partial_row,)
        assert sample.query_complete is False
        assert sample.query_status == "sampled"
        assert sample.query_error_code == "sample_limit"
    else:
        with pytest.raises(BoundedGraphReadError) as caught:
            read_graph_candidates(
                analytics=object(),
                project_id=PROJECT_ID,
                filters=[
                    _date_filter(window_start, window_end),
                    _attribute_filter("customer.final_status", "Rejected"),
                    _attribute_filter("score", 0.5, filter_type="number"),
                ],
                observe_type="trace",
            )
        assert caught.value.error_code == public_error

    expected_calls = (
        bounded_graph_reads.GRAPH_ANY_SPAN_STRATA + 1
        if public_error == "sample_limit"
        else 1
    )
    assert len(calls) == expected_calls
    assert calls[0]["anchor_probe_only"] is True
    assert calls[0]["include_incomplete_rows"] is False
    assert calls[0]["max_query_count"] == 5
    if public_error == "sample_limit":
        assert all(call["include_incomplete_rows"] is True for call in calls[1:])
        assert all(
            call["page_size"] == bounded_graph_reads.GRAPH_ANY_SPAN_ROWS_PER_STRATUM
            for call in calls[1:]
        )
        assert all(call["max_seed_attempts"] == 1 for call in calls[1:])
        assert all(call["max_query_count"] == 2 for call in calls[1:])
        assert all(call["max_candidates"] == 50 for call in calls[1:])


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
def test_anchor_timeout_falls_back_to_fully_covered_sanitized_sample(monkeypatch):
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

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, START + timedelta(days=7)),
            _attribute_filter("customer.final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.query_status == "sampled"
    assert sample.sampling_strata_completed == sample.sampling_strata
    assert raw_error not in str(sample.metadata())
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
                _attribute_filter("customer.final_status", "Rejected"),
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
def test_sparse_old_and_new_matches_use_exact_full_window_anchor(window_days):
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
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.query_complete is True
    assert {row["trace_id"] for row in sample.rows} == {"trace-old", "trace-new"}
    anchor_calls = [
        call for call in analytics.calls if "filter_anchor_limit" in call[1]
    ]
    assert len(anchor_calls) == 1
    _, anchor_params, *_ = anchor_calls[0]
    assert anchor_params["filter_anchor_start"] == window_start
    assert anchor_params["filter_anchor_end"] == window_end
    assert anchor_params["filter_anchor_limit"] == 513
    assert not any("filter_slice_start" in call[1] for call in analytics.calls)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("lower_op", "upper_op"),
    [
        ("greater_than", "less_than"),
        ("greater_than_or_equal", "less_than_or_equal"),
    ],
)
def test_long_window_scalar_datetime_bounds_are_preserved_by_sparse_anchor(
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
        _attribute_filter("final_status", "Rejected"),
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
    anchor_params = next(
        params for _, params, *_ in analytics.calls if "filter_anchor_limit" in params
    )
    assert anchor_params["filter_anchor_start"] == window_start + (
        timedelta(microseconds=1) if lower_op == "greater_than" else timedelta(0)
    )
    assert anchor_params["filter_anchor_end"] == window_end + (
        timedelta(microseconds=1) if upper_op == "less_than_or_equal" else timedelta(0)
    )
    assert not any("filter_slice_start" in call[1] for call in analytics.calls)


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
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.rows == ()
    assert sample.query_complete is True
    assert sample.query_status == "complete"
    assert sample.query_error_code is None


@pytest.mark.unit
def test_sparse_span_anchor_replays_trace_scoped_ids_and_latest_tombstones():
    window_end = START + timedelta(days=7)
    anchor_rows = [
        {
            "project_id": PROJECT_ID,
            "trace_id": "trace-a",
            "id": "shared",
            "start_time": START + timedelta(minutes=1),
        },
        {
            "project_id": PROJECT_ID,
            "trace_id": "trace-b",
            "id": "shared",
            "start_time": START + timedelta(minutes=2),
        },
        {
            "project_id": PROJECT_ID,
            "trace_id": "trace-deleted",
            "id": "gone",
            "start_time": START + timedelta(minutes=3),
        },
    ]
    latest_live = {
        **anchor_rows[1],
        "latency_ms": 11,
        "cost": 0.1,
        "total_tokens": 7,
        "prompt_tokens": 3,
        "completion_tokens": 4,
        "status": "OK",
    }

    class _SparseReplayAnalytics:
        def __init__(self):
            self.calls = []

        def execute_ch_query(self, query, params, *, timeout_ms, settings):
            self.calls.append((query, params, timeout_ms, settings))
            if "filter_anchor_limit" in params:
                return _Result(anchor_rows)
            return _Result([latest_live])

    analytics = _SparseReplayAnalytics()
    sample = read_graph_candidates(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="span",
    )

    assert sample.query_complete is True
    assert sample.rows == (latest_live,)
    assert len(analytics.calls) == 2
    anchor_query, anchor_params, *_ = analytics.calls[0]
    classify_query, classify_params, *_ = analytics.calls[1]
    assert "project_id = %(project_id)s" in anchor_query
    assert anchor_params["project_id"] == PROJECT_ID
    assert "argMax(is_deleted" in classify_query
    assert "latest_is_deleted = 0" in classify_query
    assert classify_params["candidate_span_entities"] == (
        ("trace-a", "shared"),
        ("trace-b", "shared"),
        ("trace-deleted", "gone"),
    )
    assert len(classify_params["candidate_span_identities"]) == 3


@pytest.mark.unit
def test_long_sparse_anchor_and_strata_timeout_becomes_degraded_empty(monkeypatch):
    calls = []

    def _timed_out_page(**kwargs):
        calls.append(kwargs)
        return BoundedFilterPage(
            rows=[],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="deadline_exceeded",
            total_rows_lower_bound=0,
            elapsed_ms=500,
            query_count=1,
            rows_returned=0,
            result_payload_bytes=0,
            attempts=(),
        )

    monkeypatch.setattr(
        bounded_graph_reads, "read_bounded_filter_page", _timed_out_page
    )

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(START, START + timedelta(days=7)),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="span",
    )

    assert sample.query_status == "degraded"
    assert sample.query_error_code == "read_budget_exceeded"
    assert sample.rows == ()
    assert sample.sampling_strata_completed == 0
    with pytest.raises(BoundedGraphReadError) as caught:
        graph_dispatch._require_renderable_sample(sample)
    assert caught.value.error_code == "read_budget_exceeded"
    assert calls[0]["anchor_probe_only"] is True
    assert len(calls) == 1 + bounded_graph_reads.GRAPH_ANY_SPAN_STRATA


@pytest.mark.unit
def test_span_anchor_probe_is_graph_opt_in_not_a_list_behavior_change():
    from tracer.services.clickhouse.v2.query_builders.span_list import (
        SpanListQueryBuilderV2,
    )

    filters = [
        _date_filter(START, START + timedelta(days=7)),
        _attribute_filter("final_status", "Rejected"),
    ]
    list_builder = SpanListQueryBuilderV2(project_id=PROJECT_ID, filters=filters)
    graph_builder = SpanListQueryBuilderV2(
        project_id=PROJECT_ID,
        filters=filters,
        bounded_anchor_probe=True,
    )

    assert list_builder.supports_filter_anchor_probe() is False
    assert graph_builder.supports_filter_anchor_probe() is True


@pytest.mark.unit
@pytest.mark.parametrize("window_days", [14, 180, 365])
@pytest.mark.parametrize("structured_type", ["map", "json", "call_type"])
def test_common_raw_latest_rare_filter_returns_deterministic_stratified_sample(
    window_days,
    structured_type,
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=window_days)
    window_width = window_end - window_start
    rows = []
    rows_per_stratum = 100
    for stratum in range(bounded_graph_reads.GRAPH_ANY_SPAN_STRATA):
        stratum_start = window_start + (
            window_width * stratum / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        )
        stratum_width = window_width / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        for index in range(rows_per_stratum):
            rows.append(
                {
                    "trace_id": f"trace-{stratum}-{index:03d}",
                    "root_span_id": f"root-{stratum}-{index:03d}",
                    "start_time": stratum_start
                    + (stratum_width * index / rows_per_stratum),
                    # The newest root in every stratum is the only current
                    # match; all other raw final_status hits are stale.
                    "matches_latest": index == rows_per_stratum - 1,
                }
            )

    structured_filter = (
        {
            "column_id": "call_type",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "inbound",
            },
        }
        if structured_type == "call_type"
        else _attribute_filter(
            "customer.context",
            {"tier": "vip"},
            filter_type=structured_type,
            filter_op="contains" if structured_type == "json" else "equals",
        )
    )
    filters = [
        _date_filter(window_start, window_end),
        _attribute_filter("final_status", "Rejected"),
        _attribute_filter("score", 0.5, filter_type="number"),
        structured_filter,
    ]

    def _read_once():
        analytics = _LatestRareCandidateAnalytics(observe_type="trace", rows=rows)
        sample = read_graph_candidates(
            analytics=analytics,
            project_id=PROJECT_ID,
            filters=filters,
            observe_type="trace",
        )
        return sample, analytics

    first, analytics = _read_once()
    second, _ = _read_once()

    assert tuple(row["trace_id"] for row in first.rows) == tuple(
        row["trace_id"] for row in second.rows
    )
    assert len(first.rows) == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert first.query_complete is False
    assert first.query_status == "sampled"
    assert first.query_error_code == "sample_limit"
    assert first.metadata()["query_sampling_strategy"] == (
        "time_stratified_latest_state"
    )
    assert first.metadata()["query_sampling_strata_completed"] == (
        bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    )
    classifier_sizes = [
        len(params["candidate_trace_ids"])
        for _, params, *_ in analytics.calls
        if "candidate_trace_ids" in params
    ]
    assert classifier_sizes
    assert max(classifier_sizes) <= 50


@pytest.mark.unit
def test_distributed_sample_uses_one_shared_deadline_instead_of_equal_slices(
    monkeypatch,
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=14)
    calls = []

    def _sample_page(**kwargs):
        calls.append(kwargs)
        is_anchor = kwargs.get("anchor_probe_only", False)
        return BoundedFilterPage(
            rows=[]
            if is_anchor
            else [
                {
                    "trace_id": f"trace-{len(calls)}",
                    "root_span_id": f"root-{len(calls)}",
                    "start_time": window_start + timedelta(hours=len(calls)),
                }
            ],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="sample_limit",
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=1,
            rows_returned=1,
            result_payload_bytes=10,
            attempts=(),
        )

    monkeypatch.setattr(bounded_graph_reads, "read_bounded_filter_page", _sample_page)

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    distributed_calls = calls[1:]
    old_equal_slice_ms = (
        bounded_graph_reads.GRAPH_CANDIDATE_DEADLINE_MS
        // bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    )
    assert len(distributed_calls) == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert all(call["deadline_ms"] > old_equal_slice_ms for call in distributed_calls)
    assert sample.query_status == "sampled"
    assert sample.sampling_strata_completed == (
        bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    )


@pytest.mark.unit
@pytest.mark.parametrize("completed_strata", [0, 1])
def test_partial_or_empty_stratified_deadline_is_not_renderable(
    monkeypatch,
    completed_strata,
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=14)
    page_calls = 0

    def _page(**kwargs):
        nonlocal page_calls
        page_calls += 1
        if kwargs.get("anchor_probe_only"):
            return BoundedFilterPage(
                rows=[],
                has_more=False,
                complete=False,
                status="degraded",
                error_code="sample_limit",
                total_rows_lower_bound=513,
                elapsed_ms=1,
                query_count=1,
                rows_returned=513,
                result_payload_bytes=100,
                attempts=(),
            )
        return BoundedFilterPage(
            rows=[
                {
                    "trace_id": "trace-proven",
                    "root_span_id": "root-proven",
                    "start_time": window_start + timedelta(minutes=1),
                }
            ],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="sample_limit",
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    # anchor_started, distributed_started, first-stratum check, and optional
    # second-stratum check. Crossing the deadline before all eight strata must
    # never turn zero or partial temporal coverage into a sampled graph.
    clock = iter([0.0, 0.0, 4.0] if completed_strata == 0 else [0.0, 0.0, 0.0, 4.0])
    monkeypatch.setattr(bounded_graph_reads, "monotonic", lambda: next(clock))
    monkeypatch.setattr(bounded_graph_reads, "read_bounded_filter_page", _page)

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert sample.query_complete is False
    assert sample.query_status == "degraded"
    assert sample.query_error_code == "read_budget_exceeded"
    assert sample.sampling_strata_completed == completed_strata
    assert len(sample.rows) == completed_strata
    with pytest.raises(BoundedGraphReadError) as caught:
        graph_dispatch._require_renderable_sample(sample)
    assert caught.value.error_code == "read_budget_exceeded"


@pytest.mark.unit
def test_code_307_sparse_anchor_falls_back_without_reusing_partial_rows(monkeypatch):
    from clickhouse_driver.errors import ServerException

    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=14)
    calls = []

    def _page(**kwargs):
        calls.append(kwargs)
        if kwargs.get("anchor_probe_only"):
            raise ServerException("private Code 307 query details", code=307)
        return BoundedFilterPage(
            rows=[
                {
                    "trace_id": f"trace-{len(calls)}",
                    "root_span_id": f"root-{len(calls)}",
                    "start_time": window_start + timedelta(hours=len(calls)),
                }
            ],
            has_more=False,
            complete=False,
            status="degraded",
            error_code="sample_limit",
            total_rows_lower_bound=1,
            elapsed_ms=1,
            query_count=2,
            rows_returned=2,
            result_payload_bytes=20,
            attempts=(),
        )

    monkeypatch.setattr(bounded_graph_reads, "read_bounded_filter_page", _page)

    sample = read_graph_candidates(
        analytics=object(),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type="trace",
    )

    assert len(calls) == 1 + bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert len(sample.rows) == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert sample.query_status == "sampled"
    assert sample.sampling_strata_completed == sample.sampling_strata
    assert "private" not in str(sample.metadata())


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
@pytest.mark.parametrize("window_days", [180, 365])
@pytest.mark.parametrize("structured_type", ["map", "json", "call_type"])
def test_eval_graph_samples_long_structured_filters_without_full_window_anchor(
    observe_type,
    window_days,
    structured_type,
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=window_days)
    window_width = window_end - window_start
    rows = []
    for stratum in range(bounded_graph_reads.GRAPH_ANY_SPAN_STRATA):
        stratum_start = window_start + (
            window_width * stratum / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        )
        stratum_width = window_width / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
        for index in range(60):
            row = {
                "trace_id": f"trace-{stratum}-{index:03d}",
                "root_span_id": f"root-{stratum}-{index:03d}",
                "start_time": stratum_start + (stratum_width * index / 60),
                "matches_latest": index == 59,
            }
            if observe_type == "span":
                row["id"] = f"span-{stratum}-{index:03d}"
            rows.append(row)
    analytics = _LatestRareCandidateAnalytics(
        observe_type=observe_type,
        rows=rows,
    )

    structured_filter = (
        {
            "column_id": "call_type",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "inbound",
            },
        }
        if structured_type == "call_type"
        else _attribute_filter(
            "customer.context",
            {"tier": "vip"},
            filter_type=structured_type,
            filter_op="contains" if structured_type == "json" else "equals",
        )
    )
    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
            structured_filter,
        ],
        interval="day",
        req_data_config={
            "id": EVAL_ID,
            "type": "EVAL",
            "output_type": "SCORE",
        },
        observe_type=observe_type,
    )

    assert response["query_status"] == "sampled"
    assert response["query_sample_size"] == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert any(point["value"] == 50 for point in response["data"])
    assert not any(
        call[1].get("filter_anchor_limit") == 513 for call in analytics.calls
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("observe_type", "window_days", "row_count"),
    [("trace", 180, 1600), ("span", 365, 4096)],
)
def test_bounded_high_cardinality_long_window_is_sampled_and_distributed(
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
                _attribute_filter("final_status", "Rejected"),
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
    assert first.query_status == "sampled"
    assert first.query_error_code == "sample_limit"
    assert first.sampling_strategy == "time_stratified_latest_state"
    assert first.sampling_strata == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert first.sampling_strata_completed == bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    assert first.total_rows_lower_bound >= len(first.rows)
    first_analytics = _CandidateAnalytics(observe_type=observe_type, rows=rows)
    read_graph_candidates(
        analytics=first_analytics,
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type=observe_type,
    )
    anchor_ranges = [
        (params["filter_anchor_start"], params["filter_anchor_end"])
        for _, params, *_ in first_analytics.calls
        if "filter_anchor_limit" in params
    ]
    assert len(anchor_ranges) == 1
    assert (window_start, window_end) in anchor_ranges
    assert all(
        params["filter_seed_limit"] <= 512
        for _, params, *_ in first_analytics.calls
        if "filter_seed_limit" in params
    )
    assert len(first_analytics.calls) <= (
        1 + bounded_graph_reads.GRAPH_ANY_SPAN_STRATA * 5
    )
    if observe_type == "trace":
        classifiers = [
            query
            for query, params, *_ in first_analytics.calls
            if "candidate_trace_ids" in params
        ]
        assert classifiers
        assert all("latest_latency_ms" not in query for query in classifiers)
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
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_dense_long_window_stratum_overflow_remains_explicitly_incomplete(
    observe_type,
):
    window_end = datetime(2026, 7, 31, 7)
    window_start = window_end - timedelta(days=365)
    stratum_start = window_start + (
        (window_end - window_start)
        * (bounded_graph_reads.GRAPH_ANY_SPAN_STRATA - 1)
        / bounded_graph_reads.GRAPH_ANY_SPAN_STRATA
    )
    rows = []
    for index in range(513):
        row = {
            "trace_id": f"trace-{index:05d}",
            "root_span_id": f"root-{index:05d}",
            "start_time": stratum_start + timedelta(microseconds=index + 1),
        }
        if observe_type == "span":
            row["id"] = f"span-{index:05d}"
        rows.append(row)

    sample = read_graph_candidates(
        analytics=_CandidateAnalytics(observe_type=observe_type, rows=rows),
        project_id=PROJECT_ID,
        filters=[
            _date_filter(window_start, window_end),
            _attribute_filter("final_status", "Rejected"),
        ],
        observe_type=observe_type,
    )

    assert len(sample.rows) == bounded_graph_reads.GRAPH_ANY_SPAN_ROWS_PER_STRATUM
    assert sample.query_complete is False
    assert sample.query_status == "sampled"
    assert sample.query_error_code == "sample_limit"
    assert sample.metadata()["query_sampling_strategy"] == (
        "time_stratified_latest_state"
    )
    assert sample.total_rows_lower_bound >= len(sample.rows)


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
            _attribute_filter("final_status", "Rejected"),
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
def test_span_system_graph_returns_explicit_sampled_points(
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
        query_status="sampled",
        query_error_code="sample_limit",
        sampling_strategy="time_stratified_latest_state",
        sampling_strata=8,
        sampling_strata_completed=8,
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

    assert response["data"][0]["value"] == 25
    assert response["data"][0]["primary_traffic"] == 1
    assert response["query_complete"] is False
    assert response["query_status"] == "sampled"
    assert response["query_error_code"] == "sample_limit"
    assert response["query_sample_size"] == 1
    assert response["query_sampled"] is True


@pytest.mark.unit
def test_trace_system_graph_decorates_sample_with_any_live_span_semantics(monkeypatch):
    trace_id = "11111111-1111-4111-8111-111111111111"
    sampled = replace(
        _sample(),
        query_complete=False,
        query_status="sampled",
        query_error_code="sample_limit",
        sampling_strategy="time_stratified_latest_state",
        sampling_strata=8,
        sampling_strata_completed=8,
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        interval="hour",
        metric_id="latency",
        observe_type="trace",
    )

    assert response["data"][0]["value"] == 12
    assert response["data"][0]["primary_traffic"] == 1
    assert response["query_complete"] is False
    assert response["query_status"] == "sampled"
    assert response["query_error_code"] == "sample_limit"
    assert len(analytics.calls) == 2
    assert "argMax(is_deleted, _version)" in analytics.calls[1][0]
    assert "latest_is_deleted = 0" in analytics.calls[1][0]


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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        interval="hour",
        metric_id="traffic",
        observe_type="trace",
    )

    seed_query, seed_params, seed_timeout_ms, seed_settings = analytics.calls[0]
    query, params, timeout_ms, settings = analytics.calls[1]
    assert "FROM spans" in seed_query
    assert "GROUP BY trace_id, id, start_time" in seed_query
    assert seed_params["graph_entity_limit"] == 4097
    assert seed_timeout_ms <= 1_200
    assert seed_settings["max_result_rows"] == 4097
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
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
    assert seed_params["graph_entity_limit"] == 4097
    assert seed_timeout <= 900
    assert seed_settings["max_result_rows"] == 4097
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
    assert identity_params["graph_entity_limit"] == 4097
    assert identity_timeout <= 900
    assert identity_settings["max_result_rows"] == 4097
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        interval="hour",
        req_data_config={"id": EVAL_ID, "type": "EVAL", "output_type": "SCORE"},
        observe_type="trace",
    )

    assert response["query_complete"] is False
    assert response["query_status"] == "degraded"
    assert response["query_error_code"] == "sample_limit"
    assert response["data"] == []


@pytest.mark.unit
def test_eval_non_exhaustive_candidate_prefix_is_explicitly_sampled(monkeypatch):
    analytics = _DecorationAnalytics([])
    incomplete = replace(
        _sample(),
        query_complete=False,
        query_status="sampled",
        query_error_code="sample_limit",
        sampling_strategy="time_stratified_latest_state",
        sampling_strata=8,
        sampling_strata_completed=8,
    )
    monkeypatch.setattr(graph_dispatch, "read_graph_candidates", lambda **_: incomplete)

    response = graph_dispatch.fetch_eval_graph_ch(
        analytics=analytics,
        project_id=PROJECT_ID,
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        interval="hour",
        req_data_config={
            "id": EVAL_ID,
            "type": "EVAL",
            "output_type": "SCORE",
        },
        observe_type="trace",
    )

    assert len(analytics.calls) == 1
    assert response["data"]
    assert response["query_complete"] is False
    assert response["query_status"] == "sampled"
    assert response["query_error_code"] == "sample_limit"


@pytest.mark.unit
def test_annotation_non_exhaustive_candidate_prefix_is_explicitly_sampled(
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
        query_status="sampled",
        query_error_code="sample_limit",
        sampling_strategy="time_stratified_latest_state",
        sampling_strata=8,
        sampling_strata_completed=8,
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
        filters=[_date_filter(), _attribute_filter("final_status", "Rejected")],
        interval="hour",
        req_data_config={"id": LABEL_ID, "type": "ANNOTATION"},
        observe_type="span",
    )

    assert len(analytics.calls) == 1
    assert response["data"]
    assert response["query_complete"] is False
    assert response["query_status"] == "sampled"
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
                for index in range(4097)
            ],
            [
                {
                    "trace_id": trace_id,
                    "id": f"span-{index}",
                    "start_time": START + timedelta(microseconds=index),
                }
                for index in range(4096)
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

    assert len(analytics.calls[1][1]["graph_span_identities"]) == 4096
    assert len(analytics.calls[2][1]["graph_span_entities"]) == 4096
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
def test_graph_response_contract_distinguishes_sampled_from_degraded():
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
    sampled = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [
                {
                    "timestamp": "2026-08-03T00:00:00Z",
                    "value": 12,
                    "primary_traffic": 1,
                }
            ],
            "query_complete": False,
            "query_status": "sampled",
            "query_error_code": "sample_limit",
            "query_sampled": True,
            "query_sampling_strategy": "time_stratified_latest_state",
            "query_sampling_strata": 8,
            "query_sampling_strata_completed": 8,
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
    incomplete_sample_coverage = ObserveGraphDataResultSerializer(
        data={
            "metric_name": "latency",
            "data": [],
            "query_complete": False,
            "query_status": "sampled",
            "query_error_code": "sample_limit",
            "query_sampling_strategy": "time_stratified_latest_state",
            "query_sampling_strata": 8,
            "query_sampling_strata_completed": 1,
        }
    )
    assert complete.is_valid(), complete.errors
    assert degraded.is_valid(), degraded.errors
    assert sampled.is_valid(), sampled.errors
    assert not invalid_sampled_data.is_valid()
    assert "data" in invalid_sampled_data.errors
    assert not incomplete_sample_coverage.is_valid()
    assert "query_sampling_strata_completed" in incomplete_sample_coverage.errors


@pytest.mark.unit
def test_graph_contract_empties_sampled_points_without_full_stratum_coverage():
    response = graph_dispatch.enforce_exact_graph_data_contract(
        {
            "metric_name": "latency",
            "data": [{"timestamp": START.isoformat(), "value": 999}],
            "query_complete": False,
            "query_status": "sampled",
            "query_error_code": "sample_limit",
            "query_sampling_strategy": "time_stratified_latest_state",
            "query_sampling_strata": 8,
            "query_sampling_strata_completed": 1,
        }
    )

    assert response["data"] == []
    assert response["query_status"] == "degraded"
    assert response["query_sampled"] is False


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
