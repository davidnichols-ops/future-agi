"""Unit guards for bounded eval-task span identity resolution."""

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from tracer.models.eval_task import RowType, RunType
from tracer.selectors.eval_tasks import row_resolver

pytestmark = pytest.mark.unit


def test_historical_span_query_samples_before_bounded_top_k():
    sql, params = row_resolver._build_sample_query(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        salt="task-1",
        sampling_rate=50,
        filters={"observation_type": ["llm"]},
        limit=100,
    )
    compact_sql = " ".join(sql.split())

    assert "LIMIT 1 BY id" not in compact_sql
    assert "FROM spans FINAL" in compact_sql
    assert compact_sql.count("ORDER BY toStartOfMinute(start_time) DESC, id") == 1
    assert compact_sql.count("LIMIT %(id_limit)s") == 1
    assert (
        "modulo(cityHash64(%(id_sampling_salt)s, toString(id)), 100) "
        "< %(id_sampling_rate)s"
    ) in compact_sql
    assert "lower(observation_type) IN" in compact_sql
    assert params["id_limit"] == 200
    assert "lim" not in params
    assert params["id_sampling_salt"] == "task-1"
    assert params["id_sampling_rate"] == 50.0


def test_eval_task_sort_spills_before_selector_memory_cap():
    settings = row_resolver._EVAL_TASK_READ_SETTINGS

    assert settings["max_memory_usage"] == 256 * 1024 * 1024
    assert settings["max_bytes_before_external_sort"] == 128 * 1024 * 1024
    assert 0 < settings["max_bytes_before_external_sort"] < settings["max_memory_usage"]


def test_continuous_span_query_streams_without_full_window_sort():
    sql, params = row_resolver._build_sample_query(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        salt="task-1",
        sampling_rate=100,
        filters={},
        limit=None,
    )

    assert "LIMIT 1 BY id" not in sql
    assert "FROM spans FINAL" not in sql
    assert "ORDER BY" not in sql
    assert "id_limit" not in params
    assert "lim" not in params


def test_trace_task_preview_filters_final_status_on_scoped_root_row():
    sql, params = row_resolver._build_sample_query(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        salt="task-final-status",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-01T00:00:00Z",
                "2026-07-30T00:00:00Z",
            ],
            "filters": [
                {
                    "column_id": "final_status",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "completed",
                    },
                }
            ],
        },
        limit=100,
    )
    compact_sql = " ".join(sql.split())

    assert "trace_id IN (SELECT trace_id FROM spans" not in compact_sql
    assert "FROM spans FINAL" in compact_sql
    assert "(parent_span_id IS NULL OR parent_span_id = '')" in compact_sql
    assert "mapContains(attrs_string, 'final_status')" in compact_sql
    assert "mapValues(attrs_string)" not in compact_sql
    assert "project_id = %(project_id)s" in compact_sql
    assert "start_time >= %(start_date)s" in compact_sql
    assert "start_time < %(end_date)s" in compact_sql
    assert compact_sql.count("ORDER BY toStartOfMinute(start_time) DESC, trace_id") == 1
    assert compact_sql.count("LIMIT %(id_limit)s") == 1
    assert params["project_id"] == "11111111-1111-1111-1111-111111111111"


def test_trace_legacy_observation_type_keeps_required_outer_filter():
    sql, params = row_resolver._build_sample_query(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        salt="task-legacy-observation-type",
        sampling_rate=100,
        filters={"observation_type": ["llm"]},
        limit=10,
    )
    compact_sql = " ".join(sql.split())

    assert compact_sql.count("ORDER BY toStartOfMinute(start_time) DESC, trace_id") == 1
    assert (
        compact_sql.count(
            "ORDER BY toStartOfMinute(eval_order_start_time) DESC, trace_id"
        )
        == 1
    )
    assert compact_sql.count("LIMIT %(id_limit)s") == 1
    assert compact_sql.count("LIMIT %(lim)s") == 1
    assert "trace_id IN (SELECT trace_id FROM spans" in compact_sql
    assert params["id_limit"] == 20
    assert params["lim"] == 20
    assert params["otypes"] == ("llm",)


@pytest.mark.parametrize(
    ("row_type", "id_column", "candidate_limit"),
    [
        (RowType.SPANS, "id", 20),
        (RowType.TRACES, "trace_id", 20),
        (RowType.SESSIONS, "session_id", 10),
    ],
)
def test_task_identity_siblings_and_created_at_reach_bounded_v2_sql(
    row_type, id_column, candidate_limit
):
    created_at = "2026-07-01T12:30:00Z"
    session_id = "22222222-2222-2222-2222-222222222222"
    sql, params = row_resolver._build_sample_query(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=row_type,
        salt="task-1",
        sampling_rate=50,
        filters={
            "span_id": ["span-1"],
            "trace_id": ["trace-1"],
            "session_id": [session_id],
            "created_at": created_at,
        },
        limit=10,
    )
    compact_sql = " ".join(sql.split())
    bound_values = {
        str(item)
        for value in params.values()
        for item in (value if isinstance(value, tuple) else ())
    }

    assert "id IN" in compact_sql
    assert "trace_id IN" in compact_sql
    assert "trace_session_id" in compact_sql
    assert "trace_session_id IN" in compact_sql or "trace_session_id) IN" in compact_sql
    assert {"span-1", "trace-1", session_id} <= bound_values
    assert params["start_date"] == datetime(2026, 7, 1, 12, 30)
    assert params["id_sampling_salt"] == "task-1"
    assert params["id_sampling_rate"] == 50.0
    assert params["id_limit"] == candidate_limit
    if row_type in (RowType.SPANS, RowType.TRACES):
        assert "lim" not in params
        assert f"ORDER BY toStartOfMinute(start_time) DESC, {id_column}" in compact_sql
        assert compact_sql.count("ORDER BY") == 1
        assert compact_sql.count("LIMIT %(id_limit)s") == 1
        assert "WHERE 1 = 1" not in compact_sql
    else:
        assert params["lim"] == candidate_limit
        assert f"ORDER BY {id_column}" in compact_sql
        assert "WHERE 1 = 1" in compact_sql


def test_historical_span_caller_deduplicates_and_trims(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.sql = None
            self.params = None
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.sql = sql
            self.params = params
            assert batch_size == 2
            assert settings == row_resolver._EVAL_TASK_READ_SETTINGS
            yield ["span-a", "span-a"]
            yield ["span-b", "span-c"]
            yield ["span-d"]

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-1",
        sampling_rate=100,
        filters={},
        run_type=RunType.HISTORICAL,
        spans_limit=3,
    )

    assert list(row_resolver.iter_desired_rows(task, batch_size=2)) == [
        ["span-a", "span-b"],
        ["span-c"],
    ]
    assert "LIMIT 1 BY id" not in reader.sql
    assert reader.params["id_limit"] == 6
    assert "lim" not in reader.params
    assert reader.closed is True


def test_historical_span_filter_falls_back_to_adjacent_newest_minutes(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.calls = []
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            assert batch_size == 2
            if len(self.calls) == 1:
                raise TimeoutError("private ClickHouse timeout detail")

            if params["eval_slice_start"] == datetime(2026, 7, 30, 12, 2):
                yield ["span-a", "span-a", "span-b"]
            elif params["eval_slice_start"] == datetime(2026, 7, 30, 12, 1):
                yield ["span-c"]
            else:  # pragma: no cover - makes an unexpected extra slice obvious
                raise AssertionError(params["eval_slice_start"])

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-prompt-slug",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:03:00Z",
            ],
            "filters": [
                {
                    "column_id": "prompt_slug",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "synthetic_prompt_v2",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=3,
    )

    assert list(row_resolver.iter_desired_rows(task, batch_size=2)) == [
        ["span-a", "span-b"],
        ["span-c"],
    ]
    assert len(reader.calls) == 3
    assert [
        (call[1]["eval_slice_start"], call[1]["eval_slice_end"])
        for call in reader.calls[1:]
    ] == [
        (
            datetime(2026, 7, 30, 12, 2),
            datetime(2026, 7, 30, 12, 3),
        ),
        (
            datetime(2026, 7, 30, 12, 1),
            datetime(2026, 7, 30, 12, 2),
        ),
    ]
    assert all(
        call[1]["start_date"] == datetime(2026, 7, 30, 12)
        and call[1]["end_date"] == datetime(2026, 7, 30, 12, 3)
        for call in reader.calls
    )
    assert all(
        "SELECT DISTINCT id" in call[0]
        and "start_time >= %(eval_slice_start)s" in call[0]
        and "start_time < %(eval_slice_end)s" in call[0]
        for call in reader.calls[1:]
    )
    assert all("prompt_slug" in sql for sql, _, _ in reader.calls)
    assert reader.closed is True


def test_historical_trace_filter_falls_back_to_adjacent_newest_minutes(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.calls = []
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            if len(self.calls) == 1:
                raise TimeoutError("private ClickHouse timeout detail")

            if params["start_date"] == datetime(2026, 7, 30, 12, 2):
                yield ["trace-a", "trace-b"]
            elif params["start_date"] == datetime(2026, 7, 30, 12, 1):
                yield ["trace-c"]
            else:  # pragma: no cover
                raise AssertionError(params)

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        id="task-final-status",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:03:00Z",
            ],
            "filters": [
                {
                    "column_id": "final_status",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "status_rejected",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=3,
    )

    assert list(row_resolver.iter_desired_rows(task, batch_size=2)) == [
        ["trace-a", "trace-b"],
        ["trace-c"],
    ]
    assert len(reader.calls) == 3
    assert [
        (call[1]["start_date"], call[1]["end_date"]) for call in reader.calls[1:]
    ] == [
        (
            datetime(2026, 7, 30, 12, 2),
            datetime(2026, 7, 30, 12, 3),
        ),
        (
            datetime(2026, 7, 30, 12, 1),
            datetime(2026, 7, 30, 12, 2),
        ),
    ]
    assert all(
        "SELECT DISTINCT trace_id" in sql and "final_status" in sql
        for sql, _, _ in reader.calls[1:]
    )
    assert reader.closed is True


@pytest.mark.parametrize(
    ("date_range", "expected_seed_start"),
    [
        (
            ["2026-07-30T12:00:00Z", "2026-07-30T12:03:00Z"],
            datetime(2026, 7, 30, 12, 2),
        ),
        (
            ["2026-05-01T00:00:00Z", "2026-05-08T00:00:00Z"],
            datetime(2026, 5, 7, 23, 59),
        ),
    ],
)
def test_historical_trace_any_span_filter_verifies_original_window(
    monkeypatch,
    date_range,
    expected_seed_start,
):
    """A child outside the root minute/day is still matched by the full probe."""

    class FakeReader:
        def __init__(self):
            self.calls = []
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            if len(self.calls) == 1:
                raise TimeoutError("whole-window budget")
            if len(self.calls) == 2:
                yield ["trace-with-remote-child"]
                return
            yield ["trace-with-remote-child"]

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        id="task-any-span-attribute",
        sampling_rate=100,
        filters={
            "date_range": date_range,
            "filters": [
                {
                    "column_id": "synthetic_child_attribute",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "synthetic_value",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=1,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["trace-with-remote-child"]]

    assert len(reader.calls) == 3
    seed_sql, seed_params, seed_settings = reader.calls[1]
    assert "SELECT DISTINCT trace_id" in seed_sql
    assert "synthetic_child_attribute" not in seed_sql
    assert seed_params["candidate_start"] == expected_seed_start
    assert seed_params["candidate_end"] == datetime.fromisoformat(
        date_range[1].replace("Z", "")
    )
    assert seed_params["candidate_project_id"] == str(task.project_id)
    assert seed_settings["max_execution_time"] <= 0.75
    assert seed_settings["max_threads"] == 2
    assert seed_settings["max_result_rows"] == 50
    assert seed_settings["use_skip_indexes_if_final"] == 1

    probe_sql, probe_params, probe_settings = reader.calls[2]
    assert "synthetic_child_attribute" in probe_sql
    assert probe_sql.count("trace_id IN %(candidate_trace_ids)s") >= 2
    assert probe_params["candidate_trace_ids"] == ("trace-with-remote-child",)
    assert probe_params["start_date"] == datetime.fromisoformat(
        date_range[0].replace("Z", "")
    )
    assert probe_params["end_date"] == datetime.fromisoformat(
        date_range[1].replace("Z", "")
    )
    assert probe_settings["max_execution_time"] <= 0.75
    assert probe_settings["max_threads"] == 2
    assert probe_settings["max_result_rows"] == 1
    assert probe_settings["use_skip_indexes_if_final"] == 1
    assert reader.closed is True


def test_historical_trace_negative_membership_verifies_remote_child(monkeypatch):
    """NOT IN must see a disqualifying child outside the root slice ±1 day."""

    class FakeReader:
        def __init__(self):
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            if len(self.calls) == 1:
                assert "trace_id NOT IN (SELECT" in sql
                raise TimeoutError("whole-window budget")
            if len(self.calls) == 2:
                yield ["trace-disqualified", "trace-clean"]
                return
            # The full-window verifier excludes the candidate whose old child
            # carries the forbidden end-user value.
            yield ["trace-clean"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        id="task-negative-child-filter",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-05-01T00:00:00Z",
                "2026-05-08T00:00:00Z",
            ],
            "filters": [
                {
                    "column_id": "user_id",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "not_equals",
                        "filter_value": "forbidden-user",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=1,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["trace-clean"]]
    assert len(reader.calls) == 3
    seed_sql, seed_params, _ = reader.calls[1]
    probe_sql, probe_params, _ = reader.calls[2]
    assert "forbidden-user" not in seed_sql
    assert seed_params["candidate_start"] == datetime(2026, 5, 7, 23, 59)
    assert "trace_id NOT IN (SELECT" in probe_sql
    assert "trace_id IN %(candidate_trace_ids)s" in probe_sql
    assert probe_params["candidate_trace_ids"] == (
        "trace-disqualified",
        "trace-clean",
    )
    assert probe_params["start_date"] == datetime(2026, 5, 1)
    assert probe_params["end_date"] == datetime(2026, 5, 8)


def test_trace_candidate_probe_rejects_rows_yielded_before_error(monkeypatch):
    """A driver error after a partial block cannot become a false success."""

    class FakeReader:
        def __init__(self):
            self.calls = 0
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("whole-window budget")
            if self.calls == 2:
                yield ["trace-partial"]
                return
            yield ["trace-partial"]
            raise TimeoutError("probe failed after yielding a partial block")

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        id="task-partial-probe",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:01:00Z",
            ],
            "filters": [
                {
                    "column_id": "child_key",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "match",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=1,
    )

    with pytest.raises(
        row_resolver.EvalTaskReadBudgetExceeded,
        match="Evaluation task row selection exceeded its read budget",
    ):
        list(row_resolver.iter_desired_rows(task))

    assert reader.calls == 3
    assert reader.closed is True


def test_trace_candidate_probe_splits_and_reverifies_partial_batch(monkeypatch):
    """A failed multi-ID probe may split, but must re-read every accepted ID."""

    class FakeReader:
        def __init__(self):
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            if len(self.calls) == 1:
                raise TimeoutError("whole-window budget")
            if len(self.calls) == 2:
                yield ["trace-a", "trace-b"]
                return
            if len(self.calls) == 3:
                yield ["trace-a"]
                raise TimeoutError("combined candidate probe budget")
            yield list(params["candidate_trace_ids"])

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.TRACES,
        id="task-split-probe",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:01:00Z",
            ],
            "filters": [
                {
                    "column_id": "child_key",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "equals",
                        "filter_value": "match",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=2,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["trace-a", "trace-b"]]
    assert len(reader.calls) == 5
    assert reader.calls[2][1]["candidate_trace_ids"] == ("trace-a", "trace-b")
    assert reader.calls[3][1]["candidate_trace_ids"] == ("trace-a",)
    assert reader.calls[4][1]["candidate_trace_ids"] == ("trace-b",)
    assert all(
        call[2]["max_execution_time"] <= 0.75
        and call[2]["use_skip_indexes_if_final"] == 1
        for call in reader.calls[1:]
    )


def test_historical_fallback_proves_empty_future_tail_before_slicing(monkeypatch):
    now = datetime(2026, 5, 17, 3)

    class FakeReader:
        def __init__(self):
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params), dict(settings)))
            if len(self.calls) == 1:
                raise TimeoutError("wide query timeout")
            if len(self.calls) == 2:
                return
            yield ["span-now"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    monkeypatch.setattr(row_resolver.timezone, "now", lambda: now)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-future-toolbar-end",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-05-10T02:17:00Z",
                "2026-05-17T06:41:00Z",
            ]
        },
        run_type=RunType.HISTORICAL,
        spans_limit=1,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["span-now"]]
    assert len(reader.calls) == 3
    tail_query, tail_params, tail_settings = reader.calls[1]
    assert "FROM spans" in tail_query
    assert "FINAL" not in tail_query
    assert "parent_span_id" not in tail_query
    assert tail_params["future_tail_start"] == now + timedelta(minutes=5)
    assert tail_params["future_tail_end"] == datetime(2026, 5, 17, 6, 41)
    assert tail_settings["max_execution_time"] <= 0.1
    assert tail_settings["max_threads"] == 1
    _, slice_params, _ = reader.calls[2]
    assert slice_params["eval_slice_start"] == now + timedelta(minutes=4)
    assert slice_params["eval_slice_end"] == now + timedelta(minutes=5)


def test_historical_fallback_rejects_future_skewed_physical_span(monkeypatch):
    now = datetime(2026, 5, 17, 3)

    class FakeReader:
        def __init__(self):
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params)))
            if len(self.calls) == 1:
                raise TimeoutError("wide query timeout")
            yield ["future-skewed-span"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    monkeypatch.setattr(row_resolver.timezone, "now", lambda: now)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-future-skew",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-05-10T02:17:00Z",
                "2026-05-17T06:41:00Z",
            ]
        },
        run_type=RunType.HISTORICAL,
        spans_limit=1,
    )

    with pytest.raises(row_resolver.EvalTaskReadBudgetExceeded):
        list(row_resolver.iter_desired_rows(task))

    assert len(reader.calls) == 2


def test_whole_window_and_forced_fallback_select_identical_order(monkeypatch):
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-load-independent-order",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:02:00Z",
            ]
        },
        run_type=RunType.HISTORICAL,
        spans_limit=3,
    )

    class FakeReader:
        def __init__(self, *, force_fallback):
            self.force_fallback = force_fallback
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params)))
            if len(self.calls) == 1:
                if self.force_fallback:
                    raise TimeoutError("wide query timeout")
                # Canonical whole-window order: newest minute, then id.
                yield ["span-b", "span-c", "span-a"]
                return
            if params["eval_slice_start"] == datetime(2026, 7, 30, 12, 1):
                yield ["span-b", "span-c"]
            elif params["eval_slice_start"] == datetime(2026, 7, 30, 12):
                yield ["span-a"]

        def close(self):
            pass

    whole_reader = FakeReader(force_fallback=False)
    monkeypatch.setattr(row_resolver, "get_reader", lambda: whole_reader)
    whole_ids = [
        row_id for batch in row_resolver.iter_desired_rows(task) for row_id in batch
    ]

    fallback_reader = FakeReader(force_fallback=True)
    monkeypatch.setattr(row_resolver, "get_reader", lambda: fallback_reader)
    fallback_ids = [
        row_id for batch in row_resolver.iter_desired_rows(task) for row_id in batch
    ]

    assert whole_ids == fallback_ids == ["span-b", "span-c", "span-a"]
    assert (
        whole_reader.calls[0][0].count("ORDER BY toStartOfMinute(start_time) DESC, id")
        == 1
    )
    assert all(
        "SELECT DISTINCT id" in sql and "ORDER BY id" in sql
        for sql, _ in fallback_reader.calls[1:]
    )


def test_historical_span_slice_keysets_within_busy_minute(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.calls = []

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls.append((sql, dict(params)))
            if len(self.calls) == 1:
                raise TimeoutError("wide query timeout")

            if "span-b" in params.values():
                yield ["span-c", "span-d"]
            else:
                yield ["span-a", "span-b"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    monkeypatch.setattr(row_resolver, "_EVAL_TASK_SLICE_PAGE_SIZE", 2)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-busy-minute",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:01:00Z",
            ],
            "filters": [
                {
                    "column_id": "arbitrary.string.key",
                    "filter_config": {
                        "col_type": "SPAN_ATTRIBUTE",
                        "filter_type": "text",
                        "filter_op": "contains",
                        "filter_value": "needle",
                    },
                }
            ],
        },
        run_type=RunType.HISTORICAL,
        spans_limit=4,
    )

    assert list(row_resolver.iter_desired_rows(task, batch_size=10)) == [
        ["span-a", "span-b", "span-c", "span-d"]
    ]
    assert len(reader.calls) == 3
    page_two_sql, page_two_params = reader.calls[2]
    assert "SELECT DISTINCT id" in page_two_sql
    assert "id >" in page_two_sql
    assert "span-b" in page_two_params.values()


def test_whole_window_candidate_cap_with_enough_unique_ids_does_not_slice(
    monkeypatch,
):
    class FakeReader:
        def __init__(self):
            self.calls = 0

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls += 1
            assert params["id_limit"] == 6
            assert "lim" not in params
            yield ["span-a", "span-a", "span-b", "span-b", "span-c", "span-c"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-complete-prefix",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:03:00Z",
            ]
        },
        run_type=RunType.HISTORICAL,
        spans_limit=3,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [
        ["span-a", "span-b", "span-c"]
    ]
    assert reader.calls == 1


def test_historical_span_deadline_fails_explicitly_without_partial_rows(
    monkeypatch,
):
    class FakeReader:
        def __init__(self):
            self.calls = 0
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("DB::Exception private timeout")
            if self.calls == 2:
                return
            raise AssertionError("deadline should stop before a third query")
            yield  # pragma: no cover - keep this a generator

        def close(self):
            self.closed = True

    clock = iter([0.0, 0.1, 0.2, 1.1])
    monkeypatch.setattr(row_resolver, "monotonic", lambda: next(clock))
    monkeypatch.setattr(row_resolver, "_EVAL_TASK_TOTAL_READ_SECONDS", 1.0)
    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-deadline",
        sampling_rate=100,
        filters={
            "date_range": [
                "2026-07-30T12:00:00Z",
                "2026-07-30T12:02:00Z",
            ]
        },
        run_type=RunType.HISTORICAL,
        spans_limit=5,
    )

    with pytest.raises(row_resolver.EvalTaskReadBudgetExceeded) as exc_info:
        list(row_resolver.iter_desired_rows(task, batch_size=2))

    assert str(exc_info.value) == row_resolver._SAFE_READ_BUDGET_MESSAGE
    assert "DB::Exception" not in str(exc_info.value)
    assert reader.calls == 2
    assert reader.closed is True


def test_historical_span_programming_error_is_not_misreported_as_budget(
    monkeypatch,
):
    class FakeReader:
        def stream_query(self, sql, params, *, batch_size, settings):
            raise ValueError("invalid compiled SQL")
            yield  # pragma: no cover - keep this a generator

        def close(self):
            pass

    monkeypatch.setattr(row_resolver, "get_reader", FakeReader)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-programming-error",
        sampling_rate=100,
        filters={},
        run_type=RunType.HISTORICAL,
        spans_limit=5,
    )

    with pytest.raises(ValueError, match="invalid compiled SQL"):
        list(row_resolver.iter_desired_rows(task))


def test_large_historical_limit_keeps_streaming_path(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.calls = 0

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls += 1
            yield ["span-a", "span-b"]
            yield ["span-c"]

        def close(self):
            pass

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    monkeypatch.setattr(
        row_resolver,
        "_resolve_bounded_historical_span_ids",
        lambda *_args, **_kwargs: pytest.fail(
            "large limits must not enter the buffered fallback"
        ),
    )
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-large-stream",
        sampling_rate=100,
        filters={},
        run_type=RunType.HISTORICAL,
        spans_limit=row_resolver._EVAL_TASK_BUFFERED_ID_LIMIT + 1,
    )

    assert list(row_resolver.iter_desired_rows(task, batch_size=2)) == [
        ["span-a", "span-b"],
        ["span-c"],
    ]
    assert reader.calls == 1


def test_continuous_span_resolution_keeps_single_streaming_query(monkeypatch):
    class FakeReader:
        def __init__(self):
            self.calls = 0
            self.closed = False

        def stream_query(self, sql, params, *, batch_size, settings):
            self.calls += 1
            assert "eval_slice_start" not in params
            assert "SELECT DISTINCT id" not in sql
            assert settings == row_resolver._EVAL_TASK_READ_SETTINGS
            yield ["span-new"]

        def close(self):
            self.closed = True

    reader = FakeReader()
    monkeypatch.setattr(row_resolver, "get_reader", lambda: reader)
    started_at = datetime(2026, 7, 30, 12)
    task = SimpleNamespace(
        project_id="11111111-1111-1111-1111-111111111111",
        row_type=RowType.SPANS,
        id="task-continuous",
        sampling_rate=100,
        filters={},
        run_type=RunType.CONTINUOUS,
        spans_limit=50,
        continuous_cursor=None,
        start_time=started_at,
        created_at=started_at,
    )

    assert list(row_resolver.iter_desired_rows(task)) == [["span-new"]]
    assert reader.calls == 1
    assert reader.closed is True
