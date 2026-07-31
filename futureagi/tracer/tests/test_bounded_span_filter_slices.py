"""Tests for bounded span-attribute list scans across value types."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest import mock

import pytest
from clickhouse_driver.errors import ErrorCodes, ServerException

from tracer.services.clickhouse.query_builders.span_list import SpanListQueryBuilder
from tracer.services.clickhouse.query_service import QueryResult
from tracer.views.observation_span import _execute_bounded_span_filter_prefix


def _filters(start: datetime, end: datetime) -> list[dict]:
    return [
        {
            "column_id": "arbitrary_string_key",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "arbitrary-value",
            },
        },
        {
            "column_id": "start_time",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "datetime",
                "filter_op": "between",
                "filter_value": [start.isoformat(), end.isoformat()],
            },
        },
    ]


def _builder(
    start: datetime,
    end: datetime,
    *,
    page_number: int = 0,
    page_size: int = 2,
) -> SpanListQueryBuilder:
    return SpanListQueryBuilder(
        project_id="11111111-1111-1111-1111-111111111111",
        page_number=page_number,
        page_size=page_size,
        filters=_filters(start, end),
    )


class _Analytics:
    def __init__(
        self, pages: list[list[dict]] | None = None, exc: Exception | None = None
    ):
        self.pages = list(pages or [])
        self.exc = exc
        self.calls: list[tuple[str, dict, int, dict]] = []

    def execute_ch_query(self, query, params, timeout_ms, settings):
        self.calls.append((query, dict(params), timeout_ms, dict(settings)))
        if self.exc:
            raise self.exc
        data = self.pages.pop(0) if self.pages else []
        return QueryResult(data, len(data), "clickhouse", 1)


class _FastAttemptThenSlicesAnalytics(_Analytics):
    def __init__(self, fast_exc: Exception, pages: list[list[dict]]):
        super().__init__(pages)
        self.fast_exc = fast_exc

    def execute_ch_query(self, query, params, timeout_ms, settings):
        self.calls.append((query, dict(params), timeout_ms, dict(settings)))
        if self.fast_exc is not None:
            exc, self.fast_exc = self.fast_exc, None
            raise exc
        data = self.pages.pop(0) if self.pages else []
        return QueryResult(data, len(data), "clickhouse", 1)


class _ScriptedAnalytics(_Analytics):
    def __init__(self, outcomes: list[list[dict] | Exception]):
        super().__init__()
        self.outcomes = list(outcomes)

    def execute_ch_query(self, query, params, timeout_ms, settings):
        self.calls.append((query, dict(params), timeout_ms, dict(settings)))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return QueryResult(outcome, len(outcome), "clickhouse", 1)


class _Clock:
    def __init__(self, *values: float):
        self.values = list(values)
        self.last = values[-1] if values else 0

    def __call__(self) -> float:
        if self.values:
            self.last = self.values.pop(0)
        return self.last


def test_adjacent_slices_preserve_exact_newest_first_prefix():
    end = datetime(2026, 7, 30, 12, 3)
    start = end - timedelta(minutes=3)
    analytics = _Analytics(
        [
            [{"id": "newest-2"}, {"id": "newest-1"}],
            [{"id": "older-2"}, {"id": "older-1"}],
        ]
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=_Clock(0, 0.01, 0.02, 0.03),
    )

    assert [row["id"] for row in result.data] == [
        "newest-2",
        "newest-1",
        "older-2",
        "older-1",
    ]
    assert complete is True
    assert full_window is True
    assert len(analytics.calls) == 2
    first_query, first_params, first_timeout, first_settings = analytics.calls[0]
    second_query, second_params, second_timeout, _ = analytics.calls[1]
    assert first_query == second_query
    assert "FROM spans FINAL" in first_query
    assert first_params["slice_end"] == end
    assert first_params["slice_start"] == end - timedelta(minutes=1)
    assert second_params["slice_end"] == first_params["slice_start"]
    assert second_params["slice_start"] == start
    assert first_params["limit"] == 6
    assert second_params["limit"] == 4
    assert 0 < second_timeout <= first_timeout <= 750
    assert first_settings["timeout_overflow_mode"] == "throw"
    assert first_settings["read_overflow_mode"] == "throw"
    assert first_settings["max_result_rows"] == 6


def test_duplicate_ids_across_slices_do_not_fill_the_unique_prefix():
    end = datetime(2026, 7, 30, 12, 3)
    start = end - timedelta(minutes=3)
    analytics = _Analytics(
        [
            [{"id": "newest"}, {"id": "newest"}, {"id": "second"}],
            [
                {"id": "second"},
                {"id": "third"},
                {"id": "third"},
                {"id": "fourth"},
            ],
        ]
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == [
        "newest",
        "second",
        "third",
        "fourth",
    ]
    assert len(analytics.calls) == 2
    assert complete is True
    assert full_window is False


def test_duplicate_saturated_slice_fails_closed_without_skipping_that_minute():
    end = datetime(2026, 7, 30, 12, 3)
    start = end - timedelta(minutes=3)
    analytics = _Analytics(
        [
            [{"id": "duplicate"} for _ in range(6)],
            [{"id": "must-not-be-read"}],
        ]
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == ["duplicate"]
    assert len(analytics.calls) == 1
    assert complete is False
    assert full_window is False


def test_empty_result_is_conclusive_only_after_every_slice_completes():
    end = datetime(2026, 7, 30, 12, 2)
    start = end - timedelta(minutes=2)
    analytics = _Analytics([[], []])

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert result.data == []
    assert len(analytics.calls) == 2
    assert complete is True
    assert full_window is True


def test_wide_low_volume_window_finds_old_match_with_exact_fast_attempt():
    end = datetime(2026, 7, 30, 12)
    start = end - timedelta(days=1)
    analytics = _Analytics([[{"id": "match-from-yesterday"}]])

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == ["match-from-yesterday"]
    assert complete is True
    assert full_window is True
    assert len(analytics.calls) == 1
    _, params, timeout_ms, settings = analytics.calls[0]
    assert params["slice_start"] == start
    assert params["slice_end"] == end
    assert timeout_ms == 250
    assert settings["timeout_overflow_mode"] == "throw"
    assert settings["read_overflow_mode"] == "throw"


def test_wide_window_fast_attempt_timeout_falls_back_under_shared_deadline():
    end = datetime(2026, 7, 30, 12)
    start = end - timedelta(hours=1)
    analytics = _FastAttemptThenSlicesAnalytics(
        ServerException(
            "whole window exceeded sub-budget",
            code=ErrorCodes.TIMEOUT_EXCEEDED,
        ),
        [[{"id": "newest-match"}], []],
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        max_slices=2,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == ["newest-match"]
    assert complete is False
    assert full_window is False
    assert len(analytics.calls) == 3
    _, fast_params, fast_timeout, _ = analytics.calls[0]
    _, first_slice_params, first_slice_timeout, _ = analytics.calls[1]
    _, second_slice_params, second_slice_timeout, _ = analytics.calls[2]
    assert fast_params["slice_start"] == start
    assert fast_params["slice_end"] == end
    assert first_slice_params["slice_start"] == end - timedelta(minutes=1)
    assert first_slice_params["slice_end"] == end
    assert second_slice_params["slice_start"] == end - timedelta(minutes=3)
    assert second_slice_params["slice_end"] == end - timedelta(minutes=1)
    assert fast_timeout == 250
    assert 0 < first_slice_timeout <= 750
    assert 0 < second_slice_timeout <= first_slice_timeout


def test_empty_future_tail_is_proven_before_bounded_span_slices():
    now = datetime(2026, 7, 31, 2, 50)
    end = now + timedelta(hours=4)
    start = now - timedelta(hours=1)
    analytics = _FastAttemptThenSlicesAnalytics(
        ServerException(
            "whole window exceeded sub-budget",
            code=ErrorCodes.TIMEOUT_EXCEEDED,
        ),
        [[], []],
    )

    with mock.patch("tracer.views.observation_span.timezone.now", return_value=now):
        result, complete, full_window = _execute_bounded_span_filter_prefix(
            _builder(start, end),
            analytics,
            max_slices=1,
            clock=lambda: 0,
        )

    assert result.data == []
    assert complete is False
    assert full_window is False
    assert len(analytics.calls) == 3
    _, fast_params, _, _ = analytics.calls[0]
    tail_query, tail_params, tail_timeout, tail_settings = analytics.calls[1]
    _, first_slice_params, _, _ = analytics.calls[2]
    assert fast_params["slice_end"] == end
    assert "FROM spans" in tail_query
    assert "FINAL" not in tail_query
    assert "parent_span_id" not in tail_query
    assert tail_params["future_tail_start"] == now + timedelta(minutes=5)
    assert tail_params["future_tail_end"] == end
    assert tail_timeout == 100
    assert tail_settings["max_threads"] == 1
    assert tail_settings["max_memory_usage"] == 64 * 1024 * 1024
    assert first_slice_params["slice_end"] == now + timedelta(minutes=5)
    assert first_slice_params["slice_start"] == now + timedelta(minutes=4)


def test_future_skewed_span_fails_closed_without_using_partial_fallback():
    now = datetime(2026, 7, 31, 2, 50)
    end = now + timedelta(hours=4)
    start = now - timedelta(hours=1)
    analytics = _FastAttemptThenSlicesAnalytics(
        ServerException(
            "whole window exceeded sub-budget",
            code=ErrorCodes.TIMEOUT_EXCEEDED,
        ),
        [[{"future_tail_row": 1}], [{"id": "must-not-be-used"}]],
    )

    with mock.patch("tracer.views.observation_span.timezone.now", return_value=now):
        result, complete, full_window = _execute_bounded_span_filter_prefix(
            _builder(start, end),
            analytics,
            max_slices=1,
            clock=lambda: 0,
        )

    assert result.data == []
    assert complete is False
    assert full_window is False
    assert len(analytics.calls) == 2


def test_completed_sparse_slices_expand_without_gaps_to_find_an_old_match():
    end = datetime(2026, 7, 30, 12)
    start = end - timedelta(minutes=15)
    analytics = _Analytics([[], [], [], [{"id": "old-match"}]])

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        max_slices=16,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == ["old-match"]
    assert complete is True
    assert full_window is True
    assert [
        (params["slice_start"], params["slice_end"])
        for _, params, _, _ in analytics.calls
    ] == [
        (end - timedelta(minutes=1), end),
        (end - timedelta(minutes=3), end - timedelta(minutes=1)),
        (end - timedelta(minutes=7), end - timedelta(minutes=3)),
        (start, end - timedelta(minutes=7)),
    ]


def test_failed_wide_slice_retries_same_cursor_at_minimum_width():
    end = datetime(2026, 7, 30, 12)
    start = end - timedelta(minutes=4)
    analytics = _ScriptedAnalytics(
        [
            [],
            ServerException(
                "widened slice exceeded read budget",
                code=ErrorCodes.TOO_MANY_ROWS_OR_BYTES,
            ),
            [],
            [],
        ]
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        max_slices=4,
        clock=lambda: 0,
    )

    assert result.data == []
    assert complete is True
    assert full_window is True
    assert [
        (params["slice_start"], params["slice_end"])
        for _, params, _, _ in analytics.calls
    ] == [
        (end - timedelta(minutes=1), end),
        (end - timedelta(minutes=3), end - timedelta(minutes=1)),
        (end - timedelta(minutes=2), end - timedelta(minutes=1)),
        (end - timedelta(minutes=4), end - timedelta(minutes=2)),
    ]


def test_exhausted_final_slice_keeps_all_unique_rows_for_exact_total():
    end = datetime(2026, 7, 30, 12, 1)
    start = end - timedelta(minutes=1)
    analytics = _Analytics([[{"id": f"span-{index}"} for index in range(5)]])

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert [row["id"] for row in result.data] == [
        "span-0",
        "span-1",
        "span-2",
        "span-3",
        "span-4",
    ]
    assert complete is True
    assert full_window is True


def test_shared_deadline_returns_an_explicit_incomplete_exact_prefix():
    end = datetime(2026, 7, 30, 12, 10)
    start = end - timedelta(minutes=10)
    analytics = _Analytics([[{"id": "newest-match"}]])

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=_Clock(0, 0.01, 0.8, 0.81),
    )

    assert [row["id"] for row in result.data] == ["newest-match"]
    assert len(analytics.calls) == 1
    assert analytics.calls[0][2] < 750
    assert complete is False
    assert full_window is False


def test_read_budget_error_is_not_exposed_or_mistaken_for_empty():
    end = datetime(2026, 7, 30, 12, 10)
    start = end - timedelta(minutes=10)
    analytics = _Analytics(
        exc=ServerException(
            "sensitive ClickHouse internals",
            code=ErrorCodes.TIMEOUT_EXCEEDED,
        )
    )

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end),
        analytics,
        clock=lambda: 0,
    )

    assert result.data == []
    assert complete is False
    assert full_window is False


def test_programming_error_is_not_hidden_as_an_empty_result():
    end = datetime(2026, 7, 30, 12, 10)
    start = end - timedelta(minutes=10)
    analytics = _Analytics(exc=RuntimeError("query contract bug"))

    with pytest.raises(RuntimeError, match="query contract bug"):
        _execute_bounded_span_filter_prefix(
            _builder(start, end),
            analytics,
            clock=lambda: 0,
        )


def test_deep_page_beyond_result_cap_is_explicitly_incomplete_without_query():
    end = datetime(2026, 7, 30, 12, 10)
    start = end - timedelta(minutes=10)
    analytics = _Analytics()

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        _builder(start, end, page_number=4, page_size=500),
        analytics,
        clock=lambda: 0,
    )

    assert result.data == []
    assert analytics.calls == []
    assert complete is False
    assert full_window is False


def test_custom_sort_fails_closed_because_time_slices_cannot_preserve_it():
    end = datetime(2026, 7, 30, 12, 10)
    start = end - timedelta(minutes=10)
    analytics = _Analytics()
    builder = _builder(start, end)
    builder.sort_params = [{"column_id": "latency", "direction": "asc"}]

    result, complete, full_window = _execute_bounded_span_filter_prefix(
        builder,
        analytics,
        clock=lambda: 0,
    )

    assert result.data == []
    assert analytics.calls == []
    assert complete is False
    assert full_window is False
