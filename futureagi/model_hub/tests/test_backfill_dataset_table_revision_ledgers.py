from unittest.mock import MagicMock, call

import pytest

from model_hub.management.commands import (
    backfill_dataset_table_revision_ledgers as command_module,
)


def _mock_cursor(monkeypatch, *, fetchone_values):
    cursor = MagicMock()
    cursor.fetchone.side_effect = fetchone_values
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    connection = MagicMock()
    connection.cursor.return_value = cursor_context
    atomic_context = MagicMock()
    monkeypatch.setattr(command_module, "connection", connection)
    monkeypatch.setattr(command_module.transaction, "atomic", atomic_context)
    return cursor


def test_activate_dataset_serializes_before_count_and_publishes_ready(monkeypatch):
    cursor = _mock_cursor(monkeypatch, fetchone_values=[(7,), (42,)])
    cursor.rowcount = 1

    assert command_module._activate_dataset_ledger(
        "00000000-0000-0000-0000-000000000001",
        statement_timeout_ms=8_000,
        lock_timeout_ms=2_000,
    )

    statements = [entry.args[0] for entry in cursor.execute.call_args_list]
    insert_index = next(
        index
        for index, sql in enumerate(statements)
        if "INSERT INTO model_hub_dataset_table_revision" in sql
    )
    lock_index = next(
        index for index, sql in enumerate(statements) if "FOR UPDATE" in sql
    )
    count_index = next(
        index
        for index, sql in enumerate(statements)
        if "SELECT COUNT(*)::bigint" in sql
    )
    publish_index = next(
        index for index, sql in enumerate(statements) if "is_ready = true" in sql
    )
    assert insert_index < lock_index < count_index < publish_index
    assert "ON CONFLICT (dataset_id) DO NOTHING" in statements[insert_index]
    assert cursor.execute.call_args_list[:2] == [
        call(
            "SELECT set_config('statement_timeout', %s, true)",
            ["8000"],
        ),
        call("SELECT set_config('lock_timeout', %s, true)", ["2000"]),
    ]


def test_preflight_requires_every_enabled_trigger_on_its_expected_table(monkeypatch):
    cursor = _mock_cursor(monkeypatch, fetchone_values=[])
    installed = [
        (name, table, "O")
        for name, table in command_module.EXPECTED_TRIGGER_TABLES.items()
    ]
    cursor.fetchall.return_value = installed

    command_module._assert_mutation_capture_installed(statement_timeout_ms=8_000)

    cursor.fetchall.return_value = installed[:-1]
    with pytest.raises(RuntimeError, match="mutation capture is incomplete"):
        command_module._assert_mutation_capture_installed(statement_timeout_ms=8_000)


def test_backfill_is_bounded_and_resumable_by_unready_keyset(monkeypatch):
    pending = MagicMock(
        side_effect=[
            [
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
            ]
        ]
    )
    activate = MagicMock(return_value=True)
    monkeypatch.setattr(command_module, "_pending_dataset_ids", pending)
    monkeypatch.setattr(command_module, "_activate_dataset_ledger", activate)
    preflight = MagicMock()
    monkeypatch.setattr(command_module, "_assert_mutation_capture_installed", preflight)
    monkeypatch.setattr(
        command_module,
        "_gate_state",
        lambda **_kwargs: {
            "datasets": 3,
            "ready": 2,
            "pending": 1,
            "gate_ready": False,
        },
    )

    result = command_module.backfill_dataset_table_revision_ledgers(
        batch_size=25,
        max_datasets=2,
    )

    assert result["selected_this_run"] == 2
    assert result["activated_this_run"] == 2
    assert result["pending"] == 1
    pending.assert_called_once_with(
        batch_size=2,
        after_dataset_id=None,
        statement_timeout_ms=8_000,
    )
    assert activate.call_count == 2
    preflight.assert_called_once_with(statement_timeout_ms=8_000)


def test_audit_only_reads_gate_without_selecting_or_mutating(monkeypatch):
    pending = MagicMock()
    activate = MagicMock()
    monkeypatch.setattr(command_module, "_pending_dataset_ids", pending)
    monkeypatch.setattr(command_module, "_activate_dataset_ledger", activate)
    monkeypatch.setattr(
        command_module, "_assert_mutation_capture_installed", MagicMock()
    )
    monkeypatch.setattr(
        command_module,
        "_gate_state",
        lambda **_kwargs: {
            "datasets": 2,
            "ready": 2,
            "pending": 0,
            "gate_ready": True,
        },
    )

    result = command_module.backfill_dataset_table_revision_ledgers(audit_only=True)

    assert result["gate_ready"] is True
    pending.assert_not_called()
    activate.assert_not_called()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"batch_size": 0}, "batch_size"),
        ({"max_datasets": 0}, "max_datasets"),
        ({"statement_timeout_ms": 0}, "statement_timeout_ms"),
        ({"lock_timeout_ms": 0}, "lock_timeout_ms"),
        ({"sleep_seconds": -1}, "sleep_seconds"),
    ],
)
def test_backfill_rejects_unbounded_or_invalid_options(kwargs, message):
    with pytest.raises(ValueError, match=message):
        command_module.backfill_dataset_table_revision_ledgers(**kwargs)
