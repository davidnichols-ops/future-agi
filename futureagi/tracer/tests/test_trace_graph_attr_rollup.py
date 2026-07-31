from datetime import UTC, datetime

import pytest

from tracer.serializers.filters import ObserveGraphDataResultSerializer
from tracer.services.clickhouse.graph_dispatch import (
    GRAPH_READ_SETTINGS,
    SEGMENTED_GRAPH_QUERY_TIMEOUT_MS,
    SYSTEM_GRAPH_READ_SETTINGS,
    SYSTEM_GRAPH_READ_TIMEOUT_MS,
    _segmented_graph_windows,
    degraded_graph_response,
    fetch_system_metric_graph_ch,
    format_system_metric_graph,
)
from tracer.services.clickhouse.query_builders.time_series import (
    TimeSeriesQueryBuilder,
)

PROJECT_ID = "11111111-2222-4333-8444-555555555555"
COVERED_SINCE = datetime(2020, 1, 1, tzinfo=UTC)
_UNSET = object()


def _date_filter(start="2026-07-23T00:00:00Z", end="2026-07-30T00:00:00Z"):
    return {
        "column_id": "created_at",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [start, end],
        },
    }


def _attr_filter(
    key="final_status",
    *,
    op="in",
    value=_UNSET,
):
    if value is _UNSET:
        value = ["completed"]
    return {
        "column_id": key,
        "filter_config": {
            "col_type": "SPAN_ATTRIBUTE",
            "filter_type": "text",
            "filter_op": op,
            "filter_value": value,
        },
    }


def _builder(filters, *, observe_type="trace", metric_id="latency", interval="day"):
    return TimeSeriesQueryBuilder(
        project_id=PROJECT_ID,
        filters=filters,
        interval=interval,
        observe_type=observe_type,
        metric_id=metric_id,
    )


@pytest.mark.unit
class TestTraceGraphAttributeRollup:
    def _enable(self, settings):
        settings.TRACE_GRAPH_ATTR_ROLLUP_ENABLED = True
        settings.DASHBOARD_ATTR_ROLLUP_COVERED_SINCE = COVERED_SINCE

    @pytest.mark.parametrize("metric_id", ["latency", "traffic"])
    @pytest.mark.parametrize("observe_type", ["trace", "span"])
    def test_exact_final_status_in_routes_to_root_rollup(
        self, settings, observe_type, metric_id
    ):
        self._enable(settings)

        query, params = _builder(
            [_date_filter(), _attr_filter(value=["completed", "failed"])],
            observe_type=observe_type,
            metric_id=metric_id,
        ).build()

        assert "FROM dashboard_attr_rollup" in query
        assert "sumMerge(latency_sum)" in query
        assert "countMerge(n)" in query
        assert "lowerUTF8(attr_value) IN %(attr_values)s" in query
        assert "FROM spans" not in query
        assert params["attr_key"] == "final_status"
        assert params["attr_values"] == ("completed", "failed")
        assert params["project_id"] == PROJECT_ID

    def test_arbitrary_country_uses_filter_first_trace_candidates(self, settings):
        self._enable(settings)

        query, _ = _builder(
            [_date_filter(), _attr_filter("country", op="equals", value="US")]
        ).build()

        assert "FROM dashboard_attr_rollup" not in query
        assert "FROM spans" in query
        assert "mapContains(attrs_string, 'country')" in query
        assert "INNER JOIN (" in query
        assert "AS graph_attr_candidates USING (trace_id)" in query
        assert "PREWHERE project_id = %(project_id)s" in query
        assert "start_time >= %(start_date)s - INTERVAL 1 DAY" in query
        assert "start_time < %(end_date)s + INTERVAL 1 DAY" in query
        assert "trace_id IN (SELECT trace_id" not in query
        assert "(parent_span_id IS NULL OR parent_span_id = '')" in query

    def test_unaligned_window_stays_rollup_only_and_is_marked_adjusted(self, settings):
        self._enable(settings)

        builder = _builder(
            [
                _date_filter(
                    "2026-07-23T12:34:56Z",
                    "2026-07-30T18:12:34Z",
                ),
                _attr_filter(),
            ]
        )
        query, params = builder.build()

        assert "FROM dashboard_attr_rollup" in query
        assert "FROM spans" not in query
        assert builder.rollup_window_adjusted is True
        assert params["rollup_start"] == datetime(2026, 7, 23, 13)
        assert params["rollup_end"] == datetime(2026, 7, 30, 18)

    def test_sub_hour_span_window_falls_back_with_span_row_semantics(self, settings):
        self._enable(settings)

        query, _ = _builder(
            [
                _date_filter(
                    "2026-07-30T12:15:00Z",
                    "2026-07-30T12:45:00Z",
                ),
                _attr_filter(),
            ],
            observe_type="span",
        ).build()

        assert "FROM dashboard_attr_rollup" not in query
        assert "FROM spans" in query
        assert "attrs_string['final_status']" in query
        assert "trace_id IN (SELECT trace_id" not in query

    def test_offset_window_is_normalized_before_rounding_and_coverage(self, settings):
        settings.TRACE_GRAPH_ATTR_ROLLUP_ENABLED = True
        settings.DASHBOARD_ATTR_ROLLUP_COVERED_SINCE = datetime(
            2026, 7, 23, 20, tzinfo=UTC
        )
        builder = _builder(
            [
                _date_filter(
                    "2026-07-23T12:34:56-07:00",
                    "2026-07-30T18:12:34-07:00",
                ),
                _attr_filter(),
            ]
        )

        query, params = builder.build()

        assert "FROM dashboard_attr_rollup" in query
        assert params["rollup_start"] == datetime(2026, 7, 23, 20)
        assert params["rollup_end"] == datetime(2026, 7, 31, 1)

        settings.DASHBOARD_ATTR_ROLLUP_COVERED_SINCE = datetime(
            2026, 7, 23, 20, 0, 1, tzinfo=UTC
        )
        uncovered_query, _ = _builder(
            [
                _date_filter(
                    "2026-07-23T12:34:56-07:00",
                    "2026-07-30T18:12:34-07:00",
                ),
                _attr_filter(),
            ]
        ).build()
        assert "FROM dashboard_attr_rollup" not in uncovered_query
        assert "FROM spans" in uncovered_query

    def test_sub_hour_window_falls_back_to_raw_spans(self, settings):
        self._enable(settings)
        builder = _builder(
            [
                _date_filter(
                    "2026-07-23T12:34:56Z",
                    "2026-07-23T13:12:34Z",
                ),
                _attr_filter(),
            ]
        )

        query, _ = builder.build()

        assert "FROM dashboard_attr_rollup" not in query
        assert "FROM spans" in query
        assert builder.rollup_window_adjusted is False
        assert builder.rollup_window_start is None
        assert builder.rollup_window_end is None

    def test_hour_zero_fill_respects_exclusive_adjusted_end(self, settings):
        self._enable(settings)
        builder = _builder(
            [
                _date_filter(
                    "2026-07-23T12:34:56Z",
                    "2026-07-23T15:12:34Z",
                ),
                _attr_filter(),
            ],
            interval="hour",
        )
        builder.build()

        formatted = builder.format_result([], [])
        timestamps = [point["timestamp"] for point in formatted["latency"]]

        assert timestamps == [
            "2026-07-23T13:00:00",
            "2026-07-23T14:00:00",
        ]

    @pytest.mark.parametrize("observe_type", ["trace", "span"])
    def test_adjusted_rollup_response_keeps_data_usable(self, settings, observe_type):
        self._enable(settings)

        class Result:
            data = [
                {
                    "time_bucket": datetime(2026, 7, 24),
                    "avg_latency": 42.0,
                    "total_tokens": 0,
                    "avg_cost": 0,
                    "traffic_count": 3,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "error_rate": 0,
                }
            ]
            columns = [
                "time_bucket",
                "avg_latency",
                "total_tokens",
                "avg_cost",
                "traffic_count",
                "prompt_tokens",
                "completion_tokens",
                "error_rate",
            ]

        class Analytics:
            def execute_ch_query(self, query, params, timeout_ms, settings):
                assert "dashboard_attr_rollup" in query
                assert "FROM spans" not in query
                return Result()

        result = fetch_system_metric_graph_ch(
            analytics=Analytics(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(
                    "2026-07-23T12:34:56Z",
                    "2026-07-30T18:12:34Z",
                ),
                _attr_filter(),
            ],
            interval="day",
            metric_id="latency",
            observe_type=observe_type,
        )

        assert any(point["value"] == 42.0 for point in result["data"])
        assert result["query_complete"] is True
        assert result["query_status"] == "adjusted"
        assert result["query_window_adjusted"] is True
        assert result["query_window_start"] == "2026-07-23T13:00:00Z"
        assert result["query_window_end"] == "2026-07-30T18:00:00Z"
        assert "query_error_code" not in result
        ObserveGraphDataResultSerializer(data=result).is_valid(raise_exception=True)

    def test_unsupported_extra_filter_falls_back_to_raw_builder(self, settings):
        self._enable(settings)
        status_filter = {
            "column_id": "status",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "OK",
            },
        }

        query, _ = _builder([_date_filter(), _attr_filter(), status_filter]).build()

        assert "dashboard_attr_rollup" not in query
        assert "FROM spans" in query
        assert "status" in query

    @pytest.mark.parametrize("observe_type", ["trace", "span"])
    def test_trace_and_span_final_status_use_identical_rollup_query(
        self, settings, observe_type
    ):
        self._enable(settings)
        filters = [_date_filter(), _attr_filter(value=["completed", "failed"])]

        query, params = _builder(filters, observe_type=observe_type).build()
        trace_query, trace_params = _builder(filters, observe_type="trace").build()

        assert query == trace_query
        assert params == trace_params

    @pytest.mark.parametrize(
        ("observe_type", "metric_id"),
        [
            ("trace", "tokens"),
            ("span", "tokens"),
        ],
    )
    def test_non_latency_graph_falls_back(self, settings, observe_type, metric_id):
        self._enable(settings)

        query, _ = _builder(
            [_date_filter(), _attr_filter()],
            observe_type=observe_type,
            metric_id=metric_id,
        ).build()

        assert "dashboard_attr_rollup" not in query
        assert "FROM spans" in query

    def test_span_graph_attribute_filter_targets_each_span_row(self, settings):
        self._enable(settings)

        query, _ = _builder(
            [_date_filter(), _attr_filter("prompt_slug", op="equals", value="agent_2")],
            observe_type="span",
        ).build()

        assert "dashboard_attr_rollup" not in query
        assert "mapContains(attrs_string, 'prompt_slug')" in query
        assert "INNER JOIN (" not in query
        assert "trace_id IN (SELECT trace_id" not in query
        assert "(parent_span_id IS NULL OR parent_span_id = '')" not in query
        assert "PREWHERE project_id = %(project_id)s" in query

    @pytest.mark.parametrize(
        ("op", "value", "expected_sql"),
        [
            ("in", ["support", "success"], "lower(attrs_string['team']) IN"),
            ("not_equals", "internal", "lower(attrs_string['team']) !="),
            ("not_in", ["internal", "sandbox"], "lower(attrs_string['team']) NOT IN"),
            ("contains", "support", "attrs_string['team'] ILIKE"),
            ("not_contains", "sandbox", "attrs_string['team'] NOT ILIKE"),
            ("starts_with", "supp", "attrs_string['team'] ILIKE"),
            ("ends_with", "port", "attrs_string['team'] ILIKE"),
            ("is_null", None, "NOT mapContains(attrs_string, 'team')"),
            ("is_not_null", None, "mapContains(attrs_string, 'team')"),
        ],
    )
    def test_general_trace_text_operators_use_exact_candidate_join(
        self,
        settings,
        op,
        value,
        expected_sql,
    ):
        self._enable(settings)

        query, _ = _builder(
            [_date_filter(), _attr_filter("team", op=op, value=value)]
        ).build()

        assert "AS graph_attr_candidates USING (trace_id)" in query
        assert expected_sql in query
        assert "trace_id IN (SELECT trace_id" not in query

    def test_multiple_trace_attributes_intersect_per_filter_candidate_sets(
        self, settings
    ):
        self._enable(settings)

        query, params = _builder(
            [
                _date_filter(),
                _attr_filter("team", op="equals", value="support"),
                _attr_filter("region", op="contains", value="latam"),
            ]
        ).build()

        assert query.count("AS graph_attr_candidates USING (trace_id)") == 1
        assert "WHERE is_deleted = 0" in query
        assert " OR " in query
        having_clause = query.split("HAVING", 1)[1].split(
            ") AS graph_attr_candidates", 1
        )[0]
        assert having_clause.count("countIf(") == 2
        assert "trace_id IN (SELECT trace_id" not in query
        assert params["graph_candidate_attr_0_attr_1"] == "support"
        assert params["graph_candidate_attr_1_attr_1"] == "%latam%"

    def test_multiple_span_attributes_stay_on_the_same_span_row(self, settings):
        self._enable(settings)

        query, params = _builder(
            [
                _date_filter(),
                _attr_filter("team", op="not_equals", value="internal"),
                _attr_filter("region", op="is_not_null", value=None),
            ],
            observe_type="span",
        ).build()

        assert "AS graph_attr_candidates USING (trace_id)" not in query
        assert "HAVING countIf" not in query
        assert "lower(attrs_string['team']) !=" in query
        assert "mapContains(attrs_string, 'region')" in query
        assert params["graph_span_attr_attr_1"] == "internal"

    def test_root_and_any_span_attributes_use_direct_and_candidate_predicates(
        self, settings
    ):
        self._enable(settings)

        query, params = _builder(
            [
                _date_filter(),
                _attr_filter("final_status", op="contains", value="reject"),
                _attr_filter("region", op="equals", value="latam"),
            ]
        ).build()

        assert query.count("AS graph_attr_candidates USING (trace_id)") == 1
        assert "attrs_string['final_status'] ILIKE" in query
        assert "attrs_string['region']" in query
        assert params["graph_root_attr_attr_1"] == "%reject%"
        assert params["graph_candidate_attr_0_attr_1"] == "latam"

    def test_rollup_flag_or_coverage_must_be_ready(self, settings):
        settings.DASHBOARD_ATTR_ROLLUP_ENABLED = True
        settings.TRACE_GRAPH_ATTR_ROLLUP_ENABLED = False
        settings.DASHBOARD_ATTR_ROLLUP_COVERED_SINCE = COVERED_SINCE
        query, _ = _builder([_date_filter(), _attr_filter()]).build()
        assert "dashboard_attr_rollup" not in query

    def test_builder_exposes_the_selected_query_source(self, settings):
        self._enable(settings)
        rollup = _builder([_date_filter(), _attr_filter()])
        rollup.build()
        assert rollup.query_source == "attribute_rollup"
        assert rollup.attribute_filtered is False

        raw = _builder(
            [_date_filter(), _attr_filter("country", value=["CO"])],
        )
        raw.build()
        assert raw.query_source == "raw"
        assert raw.attribute_filtered is True
        assert raw.raw_segmentation_safe is False

        span_raw = _builder(
            [_date_filter(), _attr_filter("country", value=["CO"])],
            observe_type="span",
        )
        span_raw.build()
        assert span_raw.query_source == "raw"
        assert span_raw.attribute_filtered is True
        assert span_raw.raw_segmentation_safe is True

        forced_raw = TimeSeriesQueryBuilder(
            project_id=PROJECT_ID,
            filters=[_date_filter(), _attr_filter()],
            interval="day",
            metric_id="latency",
            allow_attr_rollup=False,
        )
        query, _ = forced_raw.build()
        assert forced_raw.query_source == "raw"
        assert forced_raw.attribute_filtered is True
        assert forced_raw.raw_segmentation_safe is True
        assert "dashboard_attr_rollup" not in query

    def test_sub_hour_rollup_fallback_is_labeled_as_raw(self, settings):
        self._enable(settings)
        builder = _builder(
            [
                _date_filter(
                    "2026-07-30T12:15:00Z",
                    "2026-07-30T12:45:00Z",
                ),
                _attr_filter(),
            ],
            interval="hour",
        )

        query, _ = builder.build()

        assert "FROM spans" in query
        assert builder.query_source == "raw"
        assert builder.attribute_filtered is True

        settings.TRACE_GRAPH_ATTR_ROLLUP_ENABLED = True
        settings.DASHBOARD_ATTR_ROLLUP_COVERED_SINCE = datetime(2026, 7, 24, tzinfo=UTC)
        query, _ = _builder([_date_filter(), _attr_filter()]).build()
        assert "dashboard_attr_rollup" not in query


@pytest.mark.unit
class TestGraphReadFailureContract:
    @pytest.mark.parametrize(
        ("metric_id", "series_key", "point_field", "expected"),
        [
            ("latency", "latency", "latency", 42.5),
            ("tokens", "tokens", "tokens", 12),
            ("total_tokens", "total_tokens", "tokens", 13),
            ("cost", "cost", "cost", 0.125),
            ("traffic", "traffic", "traffic", 9),
            ("prompt_tokens", "prompt_tokens", "prompt_tokens", 7),
            ("input_tokens", "input_tokens", "prompt_tokens", 8),
            (
                "completion_tokens",
                "completion_tokens",
                "completion_tokens",
                5,
            ),
            ("output_tokens", "output_tokens", "completion_tokens", 6),
            ("error_rate", "error_rate", "error_rate", 2.5),
        ],
    )
    def test_system_graph_uses_the_supported_metric_value_alias(
        self, metric_id, series_key, point_field, expected
    ):
        timestamp = "2026-07-30T00:00:00"
        result = format_system_metric_graph(
            {
                series_key: [
                    {
                        "timestamp": timestamp,
                        point_field: expected,
                    }
                ],
                "traffic": [{"timestamp": timestamp, "traffic": 9}],
            },
            metric_id,
        )

        assert result["data"] == [
            {
                "timestamp": timestamp,
                "value": expected,
                "primary_traffic": 9,
            }
        ]

    def test_graph_limits_throw_instead_of_returning_partial_results(self):
        assert GRAPH_READ_SETTINGS["read_overflow_mode"] == "throw"
        assert GRAPH_READ_SETTINGS["result_overflow_mode"] == "throw"
        assert GRAPH_READ_SETTINGS["timeout_overflow_mode"] == "throw"

    def test_system_graph_reads_only_requested_raw_metric_with_bounded_headroom(self):
        calls = []

        class Result:
            data = []
            columns = []

        class Analytics:
            def execute_ch_query(self, query, params, timeout_ms, settings):
                calls.append((query, params, timeout_ms, settings))
                return Result()

        fetch_system_metric_graph_ch(
            analytics=Analytics(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(),
                _attr_filter("prompt_slug", op="equals", value="agent_2"),
            ],
            interval="day",
            metric_id="latency",
            observe_type="span",
        )

        assert len(calls) == 7
        assert sorted(
            (params["start_date"], params["end_date"]) for _, params, _, _ in calls
        ) == [
            (
                datetime(2026, 7, day, tzinfo=UTC).replace(tzinfo=None),
                datetime(2026, 7, day + 1, tzinfo=UTC).replace(tzinfo=None),
            )
            for day in range(23, 30)
        ]
        for query, _, timeout_ms, settings in calls:
            assert "FROM spans" in query
            assert "avg(latency_ms) AS avg_latency" in query
            assert "count() AS traffic_count" in query
            assert "0 AS total_tokens" in query
            assert "0 AS avg_cost" in query
            assert "0 AS prompt_tokens" in query
            assert "0 AS completion_tokens" in query
            assert "0 AS error_rate" in query
            assert "sum(total_tokens)" not in query
            assert "avg(cost)" not in query
            assert "sum(prompt_tokens)" not in query
            assert "sum(completion_tokens)" not in query
            assert "countIf(status = 'ERROR')" not in query
            assert 0 < timeout_ms <= SEGMENTED_GRAPH_QUERY_TIMEOUT_MS
            assert settings == SYSTEM_GRAPH_READ_SETTINGS
            assert settings["max_memory_usage"] == 256 * 1024 * 1024
            assert settings["max_bytes_to_read"] == 1536 * 1024 * 1024
        assert SYSTEM_GRAPH_READ_TIMEOUT_MS == 1250

    def test_trace_any_span_graph_keeps_global_membership_in_one_bounded_query(self):
        calls = []

        class Result:
            data = []
            columns = []

        class Analytics:
            def execute_ch_query(self, query, params, timeout_ms, settings):
                calls.append((query, dict(params), timeout_ms, settings))
                return Result()

        fetch_system_metric_graph_ch(
            analytics=Analytics(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter(),
                _attr_filter("country", op="equals", value="CO"),
            ],
            interval="day",
            metric_id="latency",
            observe_type="trace",
        )

        assert len(calls) == 1
        query, params, timeout_ms, settings = calls[0]
        assert "AS graph_attr_candidates USING (trace_id)" in query
        assert params["start_date"] == datetime(2026, 7, 23)
        assert params["end_date"] == datetime(2026, 7, 30)
        assert "start_time >= %(start_date)s - INTERVAL 1 DAY" in query
        assert "start_time < %(end_date)s + INTERVAL 1 DAY" in query
        assert timeout_ms == SYSTEM_GRAPH_READ_TIMEOUT_MS
        assert settings == SYSTEM_GRAPH_READ_SETTINGS

    def test_root_attr_plus_any_span_system_filter_is_not_segmented(self):
        filters = [
            _date_filter(),
            _attr_filter("final_status", op="equals", value="completed"),
            {
                "column_id": "model",
                "filter_config": {
                    "col_type": "SYSTEM_METRIC",
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "gpt-4o",
                },
            },
        ]
        builder = _builder(filters)

        query, _ = builder.build()

        assert "trace_id IN (SELECT trace_id FROM spans" in " ".join(query.split())
        assert builder.query_source == "raw"
        assert builder.attribute_filtered is True
        assert builder.raw_segmentation_safe is False

    def test_segmented_graph_windows_are_exact_half_open_utc_days(self):
        start = datetime(2026, 7, 23, 12, 34, 56)
        end = datetime(2026, 7, 25, 3, 4, 5)

        assert _segmented_graph_windows(start, end) == [
            (start, datetime(2026, 7, 24)),
            (datetime(2026, 7, 24), datetime(2026, 7, 25)),
            (datetime(2026, 7, 25), end),
        ]

    def test_segmented_week_graph_merges_daily_aggregate_states_exactly(self):
        calls = []
        columns = [
            "time_bucket",
            "avg_latency",
            "total_tokens",
            "avg_cost",
            "traffic_count",
            "prompt_tokens",
            "completion_tokens",
            "error_rate",
        ]

        class Result:
            def __init__(self, day):
                self.data = [
                    {
                        "time_bucket": datetime(2026, 7, 20),
                        "avg_latency": float(day),
                        "total_tokens": 0,
                        "avg_cost": 0,
                        "traffic_count": 2,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "error_rate": 0,
                    }
                ]
                self.columns = columns

        class Analytics:
            def execute_ch_query(self, query, params, timeout_ms, settings):
                calls.append((query, dict(params), timeout_ms, settings))
                return Result(params["start_date"].day - 19)

        result = fetch_system_metric_graph_ch(
            analytics=Analytics(),
            project_id=PROJECT_ID,
            filters=[
                _date_filter("2026-07-20T00:00:00Z", "2026-07-27T00:00:00Z"),
                _attr_filter("country", value=["CO"]),
            ],
            interval="week",
            metric_id="latency",
            observe_type="span",
        )

        assert len(calls) == 7
        assert result["data"][0] == {
            "timestamp": "2026-07-20T00:00:00",
            "value": 4.0,
            "primary_traffic": 14,
        }

    @pytest.mark.parametrize(
        ("exc", "expected_code"),
        [
            (TimeoutError("private timeout detail"), "read_budget_exceeded"),
            (RuntimeError("private ClickHouse stack"), "query_failed"),
        ],
    )
    def test_degraded_response_is_explicit_and_does_not_leak_error(
        self, exc, expected_code
    ):
        result = degraded_graph_response("latency", exc)

        assert result == {
            "metric_name": "latency",
            "data": [],
            "query_complete": False,
            "query_status": "degraded",
            "query_error_code": expected_code,
        }
        assert "private" not in str(result)
        ObserveGraphDataResultSerializer(data=result).is_valid(raise_exception=True)
