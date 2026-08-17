"""Activate exact dataset ledgers in bounded, concurrency-safe batches.

Migration 0124 installs mutation capture before this command runs.  For one
dataset at a time, the command creates/locks its ledger sentinel, counts live
Rows, and publishes ``is_ready`` in the same transaction.  Trigger writes use
that ledger row too, so a concurrent mutation is either visible to the count or
waits and applies its revision/count delta after publication.
"""

from __future__ import annotations

from time import sleep

from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connection, transaction

DEFAULT_BATCH_SIZE = 25
MAX_BATCH_SIZE = 250
DEFAULT_MAX_DATASETS = 100
MAX_DATASETS = 10_000
DEFAULT_STATEMENT_TIMEOUT_MS = 8_000
MAX_STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_LOCK_TIMEOUT_MS = 2_000
MAX_LOCK_TIMEOUT_MS = 10_000

EXPECTED_TRIGGER_TABLES = {
    "dataset_revision_dataset_insert": "model_hub_dataset",
    "dataset_revision_dataset_update": "model_hub_dataset",
    "dataset_revision_rows_insert": "model_hub_row",
    "dataset_revision_rows_delete": "model_hub_row",
    "dataset_revision_rows_update": "model_hub_row",
    "dataset_revision_columns_insert": "model_hub_column",
    "dataset_revision_columns_delete": "model_hub_column",
    "dataset_revision_columns_update": "model_hub_column",
    "dataset_revision_cells_insert": "model_hub_cell",
    "dataset_revision_cells_delete": "model_hub_cell",
    "dataset_revision_cells_update": "model_hub_cell",
}


def _assert_mutation_capture_installed(*, statement_timeout_ms: int) -> None:
    """Refuse activation while any non-atomic migration operation is missing."""

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            [str(statement_timeout_ms)],
        )
        cursor.execute(
            """
            SELECT trigger.tgname, source.relname, trigger.tgenabled
            FROM pg_trigger AS trigger
            INNER JOIN pg_class AS source ON source.oid = trigger.tgrelid
            WHERE NOT trigger.tgisinternal
              AND trigger.tgname = ANY(%s::text[])
            """,
            [list(EXPECTED_TRIGGER_TABLES)],
        )
        installed = {
            str(name): (str(table), str(enabled))
            for name, table, enabled in cursor.fetchall()
        }
    missing_or_invalid = sorted(
        name
        for name, table in EXPECTED_TRIGGER_TABLES.items()
        if installed.get(name) not in {(table, "O"), (table, "A")}
    )
    if missing_or_invalid:
        raise RuntimeError(
            "dataset mutation capture is incomplete; apply all migration 0124 "
            f"operations before backfill ({', '.join(missing_or_invalid)})"
        )


def _pending_dataset_ids(
    *,
    batch_size: int,
    after_dataset_id: str | None,
    statement_timeout_ms: int,
) -> list[str]:
    """Select one finite keyset page without scanning any Row or Cell data."""

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            [str(statement_timeout_ms)],
        )
        cursor.execute(
            """
            SELECT dataset.id::text
            FROM model_hub_dataset AS dataset
            LEFT JOIN model_hub_dataset_table_revision AS ledger
              ON ledger.dataset_id = dataset.id
            WHERE NOT dataset.deleted
              AND (ledger.dataset_id IS NULL OR NOT ledger.is_ready)
              AND (%s::uuid IS NULL OR dataset.id > %s::uuid)
            ORDER BY dataset.id
            LIMIT %s
            """,
            [after_dataset_id, after_dataset_id, batch_size],
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _activate_dataset_ledger(
    dataset_id: str,
    *,
    statement_timeout_ms: int,
    lock_timeout_ms: int,
) -> bool:
    """Publish one exact row count without losing a concurrent mutation."""

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            [str(statement_timeout_ms)],
        )
        cursor.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            [str(lock_timeout_ms)],
        )

        # INSERT ... ON CONFLICT waits for an uncommitted trigger-created
        # sentinel. That closes the otherwise subtle race where an older Row
        # write had observed no ledger, but had not committed before our count.
        cursor.execute(
            """
            INSERT INTO model_hub_dataset_table_revision (
                dataset_id,
                revision,
                active_rows,
                is_ready,
                ready_at,
                updated_at
            )
            SELECT id, 1, 0, false, NULL, clock_timestamp()
            FROM model_hub_dataset
            WHERE id = %s AND NOT deleted
            ON CONFLICT (dataset_id) DO NOTHING
            """,
            [dataset_id],
        )
        cursor.execute(
            """
            SELECT revision
            FROM model_hub_dataset_table_revision
            WHERE dataset_id = %s
            FOR UPDATE
            """,
            [dataset_id],
        )
        if cursor.fetchone() is None:
            # The dataset was deleted between candidate selection and claim.
            return False

        # This statement begins only after the ledger lock is held. A source
        # mutation that committed first is visible here; one that has not yet
        # updated the ledger must wait and apply its delta after our commit.
        cursor.execute(
            """
            SELECT COUNT(*)::bigint
            FROM model_hub_row
            WHERE dataset_id = %s AND NOT deleted
            """,
            [dataset_id],
        )
        active_rows = int(cursor.fetchone()[0])
        cursor.execute(
            """
            UPDATE model_hub_dataset_table_revision
            SET revision = revision + 1,
                active_rows = %s,
                is_ready = true,
                ready_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE dataset_id = %s
            """,
            [active_rows, dataset_id],
        )
        if cursor.rowcount != 1:
            raise RuntimeError("dataset revision ledger disappeared during activation")
        return True


def _gate_state(*, statement_timeout_ms: int) -> dict[str, int | bool]:
    """Read the lightweight per-dataset rollout gate (no Row/Cell scan)."""

    with transaction.atomic(), connection.cursor() as cursor:
        cursor.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            [str(statement_timeout_ms)],
        )
        cursor.execute(
            """
            SELECT
                COUNT(*)::bigint AS datasets,
                COUNT(*) FILTER (WHERE ledger.is_ready)::bigint AS ready,
                COUNT(*) FILTER (
                    WHERE ledger.dataset_id IS NULL OR NOT ledger.is_ready
                )::bigint AS pending
            FROM model_hub_dataset AS dataset
            LEFT JOIN model_hub_dataset_table_revision AS ledger
              ON ledger.dataset_id = dataset.id
            WHERE NOT dataset.deleted
            """
        )
        datasets, ready, pending = (int(value or 0) for value in cursor.fetchone())
    return {
        "datasets": datasets,
        "ready": ready,
        "pending": pending,
        "gate_ready": pending == 0,
    }


def backfill_dataset_table_revision_ledgers(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_datasets: int = DEFAULT_MAX_DATASETS,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS,
    sleep_seconds: float = 0.0,
    audit_only: bool = False,
    emit=lambda _message: None,
) -> dict[str, int | bool]:
    """Activate at most ``max_datasets`` and return the current gate state."""

    finite_batch_size = int(batch_size)
    finite_max_datasets = int(max_datasets)
    finite_statement_timeout = int(statement_timeout_ms)
    finite_lock_timeout = int(lock_timeout_ms)
    if finite_batch_size < 1 or finite_batch_size > MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    if finite_max_datasets < 1 or finite_max_datasets > MAX_DATASETS:
        raise ValueError(f"max_datasets must be between 1 and {MAX_DATASETS}")
    if (
        finite_statement_timeout < 1
        or finite_statement_timeout > MAX_STATEMENT_TIMEOUT_MS
    ):
        raise ValueError(
            f"statement_timeout_ms must be between 1 and {MAX_STATEMENT_TIMEOUT_MS}"
        )
    if finite_lock_timeout < 1 or finite_lock_timeout > MAX_LOCK_TIMEOUT_MS:
        raise ValueError(f"lock_timeout_ms must be between 1 and {MAX_LOCK_TIMEOUT_MS}")
    if float(sleep_seconds) < 0:
        raise ValueError("sleep_seconds cannot be negative")

    _assert_mutation_capture_installed(
        statement_timeout_ms=finite_statement_timeout,
    )

    selected = 0
    activated = 0
    failures: list[str] = []
    after_dataset_id = None
    if not audit_only:
        while selected < finite_max_datasets:
            dataset_ids = _pending_dataset_ids(
                batch_size=min(
                    finite_batch_size,
                    finite_max_datasets - selected,
                ),
                after_dataset_id=after_dataset_id,
                statement_timeout_ms=finite_statement_timeout,
            )
            if not dataset_ids:
                break
            for dataset_id in dataset_ids:
                selected += 1
                try:
                    if _activate_dataset_ledger(
                        dataset_id,
                        statement_timeout_ms=finite_statement_timeout,
                        lock_timeout_ms=finite_lock_timeout,
                    ):
                        activated += 1
                        emit(f"dataset ledger ready: {dataset_id}")
                except DatabaseError:
                    # Keep the invocation useful: a locked/large dataset stays
                    # unready and is retried first on the next resumable run.
                    failures.append(dataset_id)
                    emit(f"dataset ledger retry required: {dataset_id}")
                after_dataset_id = dataset_id
                if sleep_seconds:
                    sleep(float(sleep_seconds))

    gate = _gate_state(statement_timeout_ms=finite_statement_timeout)
    return {
        **gate,
        "selected_this_run": selected,
        "activated_this_run": activated,
        "failed_this_run": len(failures),
    }


class Command(BaseCommand):
    help = "Backfill and gate exact dataset-table revision ledgers online."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
        parser.add_argument("--max-datasets", type=int, default=DEFAULT_MAX_DATASETS)
        parser.add_argument(
            "--statement-timeout-ms",
            type=int,
            default=DEFAULT_STATEMENT_TIMEOUT_MS,
        )
        parser.add_argument(
            "--lock-timeout-ms", type=int, default=DEFAULT_LOCK_TIMEOUT_MS
        )
        parser.add_argument("--sleep", type=float, default=0.0)
        parser.add_argument("--audit-only", action="store_true")
        parser.add_argument("--gate", action="store_true")

    def handle(self, *args, **options):
        if options["gate"] and not options["audit_only"]:
            raise CommandError("--gate is read-only and requires --audit-only")
        try:
            result = backfill_dataset_table_revision_ledgers(
                batch_size=options["batch_size"],
                max_datasets=options["max_datasets"],
                statement_timeout_ms=options["statement_timeout_ms"],
                lock_timeout_ms=options["lock_timeout_ms"],
                sleep_seconds=options["sleep"],
                audit_only=options["audit_only"],
                emit=self.stdout.write,
            )
        except (DatabaseError, RuntimeError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            "dataset ledger gate: "
            f"datasets={result['datasets']} ready={result['ready']} "
            f"pending={result['pending']} selected={result['selected_this_run']} "
            f"activated={result['activated_this_run']} "
            f"failed={result['failed_this_run']}"
        )
        if result["failed_this_run"]:
            raise CommandError("one or more dataset ledgers require a retry")
        if options["gate"] and not result["gate_ready"]:
            raise CommandError(
                f"dataset ledger rollout is not ready: pending={result['pending']}"
            )
        self.stdout.write(self.style.SUCCESS("Dataset ledger batch complete."))
