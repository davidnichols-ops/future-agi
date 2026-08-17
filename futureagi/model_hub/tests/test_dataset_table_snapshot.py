import importlib
from unittest.mock import MagicMock, call

import pytest

from model_hub.services import dataset_table_snapshot as snapshot_module
from model_hub.services.dataset_table_snapshot import (
    DATASET_TABLE_EXACT_MAX_CELLS,
    DATASET_TABLE_EXACT_MAX_COLUMNS,
    DATASET_TABLE_SERVER_WALL_SECONDS,
    DATASET_TABLE_STATEMENT_TIMEOUT_MS,
    DatasetTableCursorError,
    DatasetTableExactLimitExceeded,
    DatasetTableReadDeadline,
    DatasetTableReadDeadlineExceeded,
    DatasetTableRevision,
    DatasetTableSnapshotChanged,
    assert_dataset_table_cells_within_limits,
    assert_dataset_table_response_within_limits,
    assert_dataset_table_revision,
    assert_dataset_table_shape_within_limits,
    begin_repeatable_read_snapshot,
    capture_dataset_table_revision,
    decode_dataset_table_cursor,
    encode_dataset_table_cursor,
)


def _revision():
    return DatasetTableRevision(
        revision=7,
        active_rows=501,
    )


def test_signed_dataset_cursor_round_trip_and_scope_binding(settings):
    settings.SECRET_KEY = "dataset-cursor-test-secret"
    token = encode_dataset_table_cursor(
        dataset_id="dataset-1",
        organization_id="org-1",
        workspace_id="workspace-1",
        revision=_revision(),
        page_index=1,
        page_size=500,
        seen_rows=500,
        last_order=499,
        last_id="row-499",
    )

    decoded = decode_dataset_table_cursor(
        token,
        dataset_id="dataset-1",
        organization_id="org-1",
        workspace_id="workspace-1",
        page_index=1,
        page_size=500,
    )
    assert decoded.revision == _revision()
    assert decoded.seen_rows == 500
    assert decoded.last_order == 499
    assert decoded.last_id == "row-499"

    with pytest.raises(DatasetTableCursorError) as exc_info:
        decode_dataset_table_cursor(
            token,
            dataset_id="dataset-other",
            organization_id="org-1",
            workspace_id="workspace-1",
            page_index=1,
            page_size=500,
        )
    assert exc_info.value.code == "dataset_cursor_mismatch"


def test_revision_check_fails_closed_when_trigger_ledger_changed(monkeypatch):
    monkeypatch.setattr(
        snapshot_module,
        "_read_revision_state",
        lambda *_args, **_kwargs: DatasetTableRevision(revision=8, active_rows=501),
    )

    with pytest.raises(DatasetTableSnapshotChanged) as exc_info:
        assert_dataset_table_revision(dataset_id="dataset-1", revision=_revision())
    assert exc_info.value.code == "dataset_snapshot_changed"


def test_repeatable_read_sets_server_statement_wall_before_snapshot(monkeypatch):
    db_cursor = MagicMock()
    db_cursor.fetchone.return_value = ("100:200:150",)
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = db_cursor
    fake_connection = MagicMock(vendor="postgresql")
    fake_connection.cursor.return_value = cursor_context
    monkeypatch.setattr(snapshot_module, "connection", fake_connection)
    deadline = MagicMock()

    assert begin_repeatable_read_snapshot(deadline) == "100:200:150"
    assert db_cursor.execute.call_args_list == [
        call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        call("SELECT txid_current_snapshot()::text"),
    ]
    deadline.set_statement_timeout.assert_called_once_with()
    assert DATASET_TABLE_STATEMENT_TIMEOUT_MS == 8_500
    assert DATASET_TABLE_SERVER_WALL_SECONDS <= 8.5


def test_exact_read_deadline_reduces_every_statement_to_one_monotonic_wall(
    monkeypatch,
):
    db_cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = db_cursor
    fake_connection = MagicMock(vendor="postgresql")
    fake_connection.cursor.return_value = cursor_context
    monkeypatch.setattr(snapshot_module, "connection", fake_connection)

    deadline = DatasetTableReadDeadline(expires_at=108.5)
    monotonic = iter((100.0, 102.0, 108.5))
    monkeypatch.setattr(snapshot_module.time, "monotonic", lambda: next(monotonic))

    deadline.set_statement_timeout()
    deadline.set_statement_timeout()
    with pytest.raises(DatasetTableReadDeadlineExceeded):
        deadline.checkpoint()

    assert db_cursor.execute.call_args_list == [
        call(
            "SELECT set_config('statement_timeout', %s, true)",
            ["8500"],
        ),
        call(
            "SELECT set_config('statement_timeout', %s, true)",
            ["6500"],
        ),
    ]


def test_revision_read_is_one_indexed_ledger_lookup(monkeypatch):
    db_cursor = MagicMock()
    db_cursor.fetchone.return_value = (7, 501, True)
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = db_cursor
    fake_connection = MagicMock(vendor="postgresql")
    fake_connection.cursor.return_value = cursor_context
    monkeypatch.setattr(snapshot_module, "connection", fake_connection)

    capture_dataset_table_revision(dataset_id="dataset-1", snapshot="1:2:")

    sql = db_cursor.execute.call_args.args[0]
    assert "model_hub_dataset_table_revision" in sql
    assert "WHERE dataset_id = %s" in sql
    assert "model_hub_row" not in sql
    assert "model_hub_column" not in sql
    assert "model_hub_cell" not in sql


def test_revision_read_fails_closed_until_dataset_ledger_is_ready(monkeypatch):
    db_cursor = MagicMock()
    db_cursor.fetchone.return_value = (7, 501, False)
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = db_cursor
    fake_connection = MagicMock(vendor="postgresql")
    fake_connection.cursor.return_value = cursor_context
    monkeypatch.setattr(snapshot_module, "connection", fake_connection)

    with pytest.raises(
        snapshot_module.DatasetTableSnapshotUnavailable,
        match="revision ledger is not ready",
    ):
        capture_dataset_table_revision(dataset_id="dataset-1", snapshot="1:2:")


def test_exact_shape_and_cell_preflights_fail_before_materialization(monkeypatch):
    db_cursor = MagicMock()
    db_cursor.fetchone.side_effect = [
        (0, DATASET_TABLE_EXACT_MAX_COLUMNS + 1, 0),
        (DATASET_TABLE_EXACT_MAX_CELLS + 1, 1, 1),
    ]
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = db_cursor
    fake_connection = MagicMock(vendor="postgresql")
    fake_connection.cursor.return_value = cursor_context
    monkeypatch.setattr(snapshot_module, "connection", fake_connection)
    deadline = MagicMock()

    with pytest.raises(DatasetTableExactLimitExceeded):
        assert_dataset_table_shape_within_limits(
            dataset_id="dataset-1", deadline=deadline
        )
    with pytest.raises(DatasetTableExactLimitExceeded):
        assert_dataset_table_cells_within_limits(
            row_ids=["00000000-0000-0000-0000-000000000001"],
            column_ids=["00000000-0000-0000-0000-000000000002"],
            deadline=deadline,
        )
    assert db_cursor.execute.call_count == 2


def test_exact_response_has_a_hard_serialized_byte_ceiling(monkeypatch):
    monkeypatch.setattr(snapshot_module, "DATASET_TABLE_EXACT_MAX_SERIALIZED_BYTES", 32)

    with pytest.raises(DatasetTableExactLimitExceeded):
        assert_dataset_table_response_within_limits({"table": [{"value": "x" * 64}]})


def test_revision_ledger_migration_covers_every_dataset_table_mutation():
    migration = importlib.import_module(
        "model_hub.migrations.0124_dataset_table_revision_ledger"
    )

    assert migration.Migration.dependencies == [
        ("model_hub", "0123_annotation_score_value_projection")
    ]
    assert migration.Migration.atomic is False
    sql = migration.INSTALL_SQL
    for table, transition_name in (
        ("model_hub_dataset", "dataset"),
        ("model_hub_row", "rows"),
        ("model_hub_column", "columns"),
        ("model_hub_cell", "cells"),
    ):
        for operation in ("insert", "update"):
            assert f"dataset_revision_{transition_name}_{operation}" in sql
        if table != "model_hub_dataset":
            assert f"dataset_revision_{transition_name}_delete" in sql
        assert f"ON {table}" in sql
    assert sql.count("FOR EACH STATEMENT") == 11
    assert "model_hub_dataset_table_revision" in sql
    assert "active_rows" in sql
    assert "is_ready boolean NOT NULL DEFAULT false" in sql
    assert "LEFT JOIN model_hub_row" not in sql
    assert "COUNT(dataset_row.id)" not in sql
    assert sql.count("ON CONFLICT (dataset_id)") == 11
    assert sql.count("WHEN ledger.is_ready THEN") == 3
    assert len(migration.TRIGGER_SQL) == 11
    assert [name for name, _, _ in migration.TRIGGER_SQL[-2:]] == [
        "dataset_revision_dataset_update",
        "dataset_revision_dataset_insert",
    ]
    # Each hot-table DDL statement is its own operation and therefore commits
    # independently in this non-atomic migration.
    assert len(migration.Migration.operations) == 13
    trigger_operations = migration.Migration.operations[2:]
    # RunSQL splits a plain string with prepare_sql_script(). A one-item list
    # is deliberately executed as one simple-query message, so PgBouncer
    # transaction pooling cannot swap sessions between SET, DDL, and RESET.
    assert all(
        isinstance(operation.sql, list) and len(operation.sql) == 1
        for operation in migration.Migration.operations
    )
    assert all(
        operation.sql[0].count("CREATE OR REPLACE TRIGGER") == 1
        for operation in trigger_operations
    )
    assert all(
        "SET lock_timeout = '3s'" in operation.sql[0]
        for operation in migration.Migration.operations
    )
    assert all(
        "SET statement_timeout = '15s'" in operation.sql[0]
        for operation in migration.Migration.operations
    )
    assert all(
        "RESET lock_timeout" in operation.sql[0]
        for operation in migration.Migration.operations
    )
