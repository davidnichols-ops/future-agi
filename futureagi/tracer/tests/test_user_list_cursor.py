from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tracer.services.clickhouse.list_cursor import ListCursor
from tracer.services.clickhouse.query_builders.user_list import UserListQueryBuilder
from tracer.services.clickhouse.read_budget import ReadDeadline, ReadDeadlineExceeded
from tracer.services.users_list_manager import (
    UsersListManager,
    _users_attr_enrichment_query,
)

pytestmark = pytest.mark.unit


def _manager(*, filters=None) -> UsersListManager:
    project_id = str(uuid.uuid4())
    return UsersListManager(
        organization_id=str(uuid.uuid4()),
        allowed_project_ids=[project_id],
        project_id=project_id,
        filters=filters or [],
    )


def _candidate(index: int, *, now: datetime) -> dict:
    return {
        "end_user_id": str(uuid.UUID(int=index + 1)),
        "first_seen": now - timedelta(seconds=index),
        "user_id": f"user-{index}",
        "user_id_type": "custom",
        "user_id_hash": "",
    }


def _exact(candidate: dict, *, cost: float = 1.0) -> dict:
    return {
        "end_user_id": candidate["end_user_id"],
        "user_id": candidate["user_id"],
        "total_cost": cost,
        "total_tokens": 1,
        "input_tokens": 1,
        "output_tokens": 0,
        "num_traces": 1,
        "last_active": candidate["first_seen"],
    }


def test_dimension_candidate_query_is_stable_keyset_and_finite():
    builder = UserListQueryBuilder(
        organization_id=str(uuid.uuid4()),
        project_ids=[str(uuid.uuid4())],
        search="alice",
    )
    before = datetime(2026, 8, 5, 12, tzinfo=UTC)

    sql, params = builder.build_dimension_candidate_query(
        limit=26,
        before_first_seen=before,
        before_end_user_id=str(uuid.uuid4()),
    )

    assert "FROM end_users AS eu FINAL" in sql
    assert "end_user_id_remap" in sql
    assert "ORDER BY first_seen DESC, end_user_id DESC" in sql
    assert "first_seen < %(before_first_seen)s" in sql
    assert "LIMIT %(dimension_limit)s" in sql
    assert "FROM spans" not in sql
    assert params["dimension_limit"] == 26
    assert params["before_first_seen"] == before


def test_finite_candidate_ids_narrow_before_span_replay():
    candidate_ids = [str(uuid.uuid4()), str(uuid.uuid4())]
    builder = UserListQueryBuilder(
        organization_id=str(uuid.uuid4()),
        project_ids=[str(uuid.uuid4())],
        filters=[],
        limit=2,
        offset=0,
        candidate_end_user_ids=candidate_ids,
    )

    sql, params = builder.build_candidate_page_query()

    assert "HAVING end_user_id IN %(candidate_end_user_ids)s" in sql
    assert params["candidate_end_user_ids"] == tuple(candidate_ids)
    assert "expanded_filtered_end_user_ids" in sql
    assert "latest_candidate_spans" in sql


def test_user_attribute_enrichment_reads_all_direct_write_attribute_columns():
    sql, _ = _users_attr_enrichment_query(project_ids=[str(uuid.uuid4())])

    assert "argMax(attrs_string, _version)" in sql
    assert "argMax(attrs_number, _version)" in sql
    assert "argMax(attrs_bool, _version)" in sql
    assert "length(mapKeys(latest_attrs_bool)) > 0" in sql


def test_cursor_page_publishes_only_fully_hydrated_matching_rows():
    now = datetime(2026, 8, 5, 12, tzinfo=UTC)
    candidates = [_candidate(index, now=now) for index in range(3)]
    filters = [
        {
            "column_id": "total_cost",
            "filter_config": {
                "filter_type": "number",
                "filter_op": "greater_than",
                "filter_value": 5,
            },
        }
    ]
    manager = _manager(filters=filters)
    exact_rows = [
        _exact(candidates[0], cost=10),
        _exact(candidates[1], cost=1),
        _exact(candidates[2], cost=20),
    ]

    with (
        patch.object(
            manager,
            "_read_dimension_candidates",
            return_value=candidates,
        ),
        patch.object(
            manager,
            "_read_exact_candidate_rows",
            return_value=exact_rows,
        ),
    ):
        result = manager.list_cursor_payload(page_size=25)

    assert [row["user_id"] for row in result.payload["table"]] == [
        "user-0",
        "user-2",
    ]
    assert result.payload["total_count"] == 2
    assert result.payload["count_is_lower_bound"] is False
    assert result.payload["query_complete"] is True
    assert result.has_more is False
    assert result.checkpoint_order == (
        candidates[-1]["first_seen"],
        candidates[-1]["end_user_id"],
    )


def test_cursor_checkpoint_survives_later_deadline_without_inventing_match():
    now = datetime(2026, 8, 5, 12, tzinfo=UTC)
    candidates = [_candidate(index, now=now) for index in range(100)]
    manager = _manager()

    with (
        patch.object(
            manager,
            "_read_dimension_candidates",
            side_effect=[candidates, ReadDeadlineExceeded("deadline")],
        ),
        patch.object(
            manager,
            "_read_exact_candidate_rows",
            return_value=[],
        ),
    ):
        result = manager.list_cursor_payload(page_size=25)

    assert result.payload["table"] == []
    assert result.payload["total_count"] == 0
    assert result.payload["count_is_lower_bound"] is True
    assert result.payload["query_complete"] is True
    assert result.payload["query_status"] == "complete"
    assert result.has_more is True
    assert result.unseen_row_proven is False
    assert result.checkpoint_order == (
        candidates[-1]["first_seen"],
        candidates[-1]["end_user_id"],
    )


def test_cursor_resume_reuses_frozen_window_and_keyset():
    now = datetime(2026, 8, 5, 12, tzinfo=UTC)
    candidate = _candidate(3, now=now)
    manager = _manager()
    cursor = ListCursor(
        window_start=now - timedelta(days=30),
        window_end=now,
        order=(candidate["first_seen"], candidate["end_user_id"]),
        seen_rows=7,
    )

    with patch.object(
        manager,
        "_read_dimension_candidates",
        return_value=[],
    ) as read_candidates:
        result = manager.list_cursor_payload(page_size=25, cursor=cursor)

    assert result.window_start == cursor.window_start
    assert result.window_end == cursor.window_end
    assert result.seen_rows == 7
    assert result.payload["total_count"] == 7
    kwargs = read_candidates.call_args.kwargs
    assert kwargs["before_first_seen"] == candidate["first_seen"]
    assert kwargs["before_end_user_id"] == candidate["end_user_id"]
    assert "snapshot_settings" not in kwargs


@pytest.mark.parametrize(
    ("candidate", "operator", "expected", "matches"),
    [
        (["Rechazado", "Completed"], "in", ["Rechazado"], True),
        (["Rechazado", "Completed"], "not_in", ["Failed"], True),
        (["Rechazado", "Completed"], "not_in", ["Completed"], False),
        ({"final_status": "Rechazado"}, "contains", "Rechazado", True),
        (
            '{"final_status":"Rechazado","nested":{"attempt":2}}',
            "equals",
            {"nested": {"attempt": 2}, "final_status": "Rechazado"},
            True,
        ),
        (
            '{"final_status":"Rechazado","nested":{"attempt":2}}',
            "contains",
            {"attempt": 2},
            True,
        ),
        (12.0, "greater_than", 10, True),
        (12.0, "equals", 12, True),
        ("true", "equals", True, True),
        ("false", "in", [False], True),
        (None, "is_null", None, True),
    ],
)
def test_candidate_filter_matrix(candidate, operator, expected, matches):
    assert (
        UsersListManager._candidate_value_matches(candidate, operator, expected)
        is matches
    )


@pytest.mark.parametrize("structured_first", [False, True])
def test_span_attribute_collector_preserves_mixed_scalar_and_json_values(
    structured_first,
):
    manager = _manager()
    end_user_id = str(uuid.uuid4())
    scalar_row = {
        "end_user_id": end_user_id,
        "attributes_extra": json.dumps({"mixed": "plain"}),
        "attrs_string": {},
        "attrs_number": {},
        "attrs_bool": {},
    }
    structured_row = {
        "end_user_id": end_user_id,
        "attributes_extra": json.dumps({"mixed": {"attempt": 2}}),
        "attrs_string": {},
        "attrs_number": {},
        "attrs_bool": {},
    }
    attribute_rows = (
        [structured_row, scalar_row]
        if structured_first
        else [scalar_row, structured_row]
    )

    with patch(
        "tracer.services.users_list_manager.V2AnalyticsQueryService"
    ) as analytics_cls:
        analytics_cls.return_value.execute_ch_query.return_value = SimpleNamespace(
            data=attribute_rows
        )
        attributes = manager._read_span_attributes(
            [{"end_user_id": end_user_id}], ReadDeadline.start(10_000)
        )

    rows = [{"end_user_id": end_user_id}]
    manager._apply_span_attributes(rows, attributes)

    assert rows[0]["mixed"] == ["plain", '{"attempt":2}']


def test_span_attribute_collector_preserves_explicit_null_for_is_null_filter():
    manager = _manager()
    end_user_id = str(uuid.uuid4())
    attribute_row = {
        "end_user_id": end_user_id,
        "attributes_extra": json.dumps({"optional": None}),
        "attrs_string": {},
        "attrs_number": {},
        "attrs_bool": {},
    }

    with patch(
        "tracer.services.users_list_manager.V2AnalyticsQueryService"
    ) as analytics_cls:
        analytics_cls.return_value.execute_ch_query.return_value = SimpleNamespace(
            data=[attribute_row]
        )
        attributes = manager._read_span_attributes(
            [{"end_user_id": end_user_id}], ReadDeadline.start(10_000)
        )

    rows = [{"end_user_id": end_user_id}]
    manager._apply_span_attributes(rows, attributes)

    assert "optional" in rows[0]
    assert rows[0]["optional"] is None
    assert manager._candidate_value_matches(rows[0]["optional"], "is_null", None)


def test_span_attribute_collector_unions_typed_maps_with_structured_extra():
    manager = _manager()
    end_user_id = str(uuid.uuid4())
    attribute_row = {
        "end_user_id": end_user_id,
        "attributes_extra": json.dumps({"structured": {"attempt": 2}}),
        "attrs_string": {"final_status": "Rechazado"},
        "attrs_number": {"score": 12.0},
        "attrs_bool": {"approved": True},
    }

    with patch(
        "tracer.services.users_list_manager.V2AnalyticsQueryService"
    ) as analytics_cls:
        analytics_cls.return_value.execute_ch_query.return_value = SimpleNamespace(
            data=[attribute_row]
        )
        attributes = manager._read_span_attributes(
            [{"end_user_id": end_user_id}], ReadDeadline.start(10_000)
        )

    rows = [{"end_user_id": end_user_id}]
    manager._apply_span_attributes(rows, attributes)

    assert rows[0]["structured"] == ['{"attempt":2}']
    assert rows[0]["final_status"] == "Rechazado"
    assert rows[0]["score"] == 12.0
    assert rows[0]["approved"] == "true"
    assert manager._candidate_value_matches(rows[0]["approved"], "equals", True)
