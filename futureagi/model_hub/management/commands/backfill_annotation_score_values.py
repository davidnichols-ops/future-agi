"""Populate and gate the exact annotation-value projection in resumable batches.

The database trigger installed by migration 0123 claims every new or changed
Score atomically. This reconciler claims untouched historical rows and creates
readiness for post-cutover tenants that do not have a Score yet; the marker and
status tables make every finite pass idempotent and safe to retry after failure.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from math import ceil, isfinite
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

PROJECTION_VERSION = 1
DEFAULT_BATCH_SIZE = 2_000
MAX_BATCH_SIZE = 10_000
DEFAULT_MAX_SCORES = 50_000
MAX_SCORES = 1_000_000
DEFAULT_MAX_ORGANIZATIONS = 100
MAX_ORGANIZATIONS = 1_000
DEFAULT_MAX_RUNTIME_SECONDS = 600.0
MAX_RUNTIME_SECONDS = 3_600.0
DEFAULT_STATEMENT_TIMEOUT_MS = 30_000
MAX_STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_LOCK_TIMEOUT_MS = 2_000
MAX_LOCK_TIMEOUT_MS = 5_000
MAX_RECHECK_SECONDS = 31 * 24 * 60 * 60


class AnnotationProjectionDeadlineExceeded(RuntimeError):
    """The finite reconciliation wall expired before another statement."""


@dataclass(frozen=True)
class _ProjectionBudget:
    deadline: float
    statement_timeout_ms: int
    lock_timeout_ms: int

    def remaining_ms(self) -> int:
        remaining = ceil((self.deadline - time.monotonic()) * 1_000)
        if remaining <= 0:
            raise AnnotationProjectionDeadlineExceeded(
                "annotation value projection runtime limit was reached"
            )
        return remaining


_ACTIVE_BUDGET: ContextVar[_ProjectionBudget | None] = ContextVar(
    "annotation_projection_budget",
    default=None,
)


def _current_budget() -> _ProjectionBudget:
    budget = _ACTIVE_BUDGET.get()
    if budget is not None:
        return budget
    # Direct helper calls are used by narrowly scoped tests and operator tools.
    # They still receive finite database ceilings instead of silently falling
    # back to the connection's unbounded defaults.
    return _ProjectionBudget(
        deadline=time.monotonic() + DEFAULT_MAX_RUNTIME_SECONDS,
        statement_timeout_ms=DEFAULT_STATEMENT_TIMEOUT_MS,
        lock_timeout_ms=DEFAULT_LOCK_TIMEOUT_MS,
    )


def _execute(cursor, query: str, params: list[Any] | None = None) -> None:
    """Execute one statement beneath shrinking PostgreSQL transaction limits."""

    budget = _current_budget()
    remaining_ms = budget.remaining_ms()
    statement_timeout_ms = min(budget.statement_timeout_ms, remaining_ms)
    lock_timeout_ms = min(budget.lock_timeout_ms, statement_timeout_ms)
    if getattr(connection, "vendor", None) == "postgresql":
        # set_config(..., true) is transaction-local. Every caller uses a short
        # atomic block, so neither timeout can leak back into a web connection.
        cursor.execute(
            """
            SELECT
                set_config('lock_timeout', %s, true),
                set_config('statement_timeout', %s, true)
            """,
            [f"{lock_timeout_ms}ms", f"{statement_timeout_ms}ms"],
        )
    cursor.execute(query, params or [])


@contextmanager
def _bounded_cursor() -> Iterator[Any]:
    """Give one helper its own tenant-safe, timeout-scoped transaction."""

    with transaction.atomic(), connection.cursor() as cursor:
        yield cursor


def _organization_ids(
    limit: int,
    *,
    retry_after_seconds: int | None = None,
    ready_recheck_after_seconds: int | None = None,
) -> list[str]:
    """Return a fair, bounded page of tenant scopes requiring reconciliation.

    Every real tenant is a candidate, not only tenants that already have a
    Score. That closes the post-cutover lifecycle gap where a new organization
    with configured labels but zero values had no status row and therefore
    failed closed forever. Missing statuses sort first; afterwards the oldest
    reconciled status rotates to the front, so repeated finite runs make
    durable progress without another global cursor table.
    """

    due_predicate = ""
    params: list[Any] = []
    if retry_after_seconds is not None and ready_recheck_after_seconds is not None:
        # Missing rows are always due. Failed/unready tenants retry promptly,
        # while a healthy trigger-maintained tenant receives only a periodic
        # drift audit rather than a full Score coverage scan every minute.
        due_predicate = """
            AND (
                status.organization_id IS NULL
                OR status.updated_at <= NOW() - make_interval(
                    secs => CASE
                        WHEN status.ready THEN %s
                        ELSE %s
                    END
                )
            )
        """
        params.extend([ready_recheck_after_seconds, retry_after_seconds])

    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            f"""
            WITH tenant AS (
                SELECT id AS organization_id
                FROM accounts_organization
                UNION
                SELECT organization_id
                FROM model_hub_annotation_value_status
            )
            SELECT tenant.organization_id::text
            FROM tenant
            LEFT JOIN model_hub_annotation_value_status AS status
              ON status.organization_id = tenant.organization_id
            WHERE tenant.organization_id IS NOT NULL
            {due_predicate}
            ORDER BY
                (status.organization_id IS NOT NULL),
                status.updated_at NULLS FIRST,
                tenant.organization_id
            LIMIT %s
            """,
            [*params, limit],
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _ensure_status(organization_id: str) -> None:
    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            """
            INSERT INTO model_hub_annotation_value_status (
                organization_id, projection_version, ready,
                projected_scores, backfill_cursor, updated_at
            ) VALUES (%s, %s, false, 0, NULL, NOW())
            ON CONFLICT (organization_id) DO NOTHING
            """,
            [organization_id, PROJECTION_VERSION],
        )


def _backfill_cursor(organization_id: str) -> str | None:
    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            """
            SELECT backfill_cursor::text
            FROM model_hub_annotation_value_status
            WHERE organization_id = %s AND projection_version = %s
            """,
            [organization_id, PROJECTION_VERSION],
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("annotation value projection status row is missing")
        return str(row[0]) if row[0] else None


def _pending_score_ids(
    *, organization_id: str, batch_size: int, after_id: str | None
) -> list[str]:
    """Return one finite marker-missing batch; the UUID order is only a tie-break."""

    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            """
            SELECT s.id::text
            FROM model_hub_score AS s
            INNER JOIN tracer_project AS project
              ON project.id = s.tracer_project_id
             AND project.organization_id = s.organization_id
            INNER JOIN model_hub_annotationslabels AS label
              ON label.id = s.label_id
             AND label.type = 'categorical'
             AND label.organization_id = s.organization_id
             AND (
                 label.project_id IS NULL
                 OR label.project_id = s.tracer_project_id
             )
             AND (
                 label.workspace_id IS NULL
                 OR label.workspace_id = project.workspace_id
             )
            LEFT JOIN model_hub_annotation_value_marker AS marker
              ON marker.score_id = s.id
             AND marker.projection_version = %s
            WHERE NOT s.deleted
              AND s.organization_id = %s
              AND s.tracer_project_id IS NOT NULL
              AND (s.trace_id IS NOT NULL OR s.observation_span_id IS NOT NULL)
              AND marker.score_id IS NULL
              AND (%s::uuid IS NULL OR s.id > %s::uuid)
            ORDER BY s.id
            LIMIT %s
            """,
            [PROJECTION_VERSION, organization_id, after_id, after_id, batch_size],
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _project_score_ids(organization_id: str, score_ids: list[str]) -> int:
    """Claim and project one batch in a single transaction/round trip."""

    if not score_ids:
        return 0
    with _bounded_cursor() as cursor:
        # Lock before the projector attempts any Score row lock. The projector
        # uses SKIP LOCKED, so an in-flight Score transaction that is waiting to
        # run its deferred trigger cannot form a Score-row/tenant-lock cycle.
        _execute(
            cursor,
            "SELECT model_hub_annotation_organization_lock(%s::uuid)",
            [organization_id],
        )
        _execute(
            cursor,
            """
            SELECT count(*)
            FROM unnest(%s::uuid[]) AS pending(score_id)
            INNER JOIN model_hub_score AS score
              ON score.id = pending.score_id
             AND score.organization_id = %s
            WHERE model_hub_project_annotation_score_value(pending.score_id)
            """,
            [score_ids, organization_id],
        )
        claimed = int(cursor.fetchone()[0])
        _execute(
            cursor,
            """
            UPDATE model_hub_annotation_value_status
            SET backfill_cursor = %s,
                updated_at = NOW()
            WHERE organization_id = %s AND projection_version = %s
            """,
            [score_ids[-1], organization_id, PROJECTION_VERSION],
        )
        if cursor.rowcount != 1:
            raise RuntimeError("annotation value projection status row is missing")
        return claimed


def _reset_backfill_cursor(organization_id: str) -> None:
    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            """
            UPDATE model_hub_annotation_value_status
            SET backfill_cursor = NULL,
                updated_at = NOW()
            WHERE organization_id = %s AND projection_version = %s
            """,
            [organization_id, PROJECTION_VERSION],
        )
        if cursor.rowcount != 1:
            raise RuntimeError("annotation value projection status row is missing")


def _defer_status(organization_id: str) -> None:
    """Rotate a failed tenant without changing its fail-closed readiness."""

    with _bounded_cursor() as cursor:
        _execute(
            cursor,
            """
            UPDATE model_hub_annotation_value_status
            SET updated_at = NOW()
            WHERE organization_id = %s AND projection_version = %s
            """,
            [organization_id, PROJECTION_VERSION],
        )


def _projection_gate(organization_id: str, *, publish: bool) -> dict[str, int | bool]:
    """Prove and optionally publish one organization's exact coverage."""

    with _bounded_cursor() as cursor:
        # The trigger and this gate acquire the same tenant lock.  A concurrent
        # unscoped/oversize insert therefore cannot have its ready=false update
        # overwritten by this transaction's later ready=true publication. Keep
        # READ COMMITTED so coverage takes its snapshot after lock acquisition.
        if publish:
            _execute(
                cursor,
                "SELECT model_hub_annotation_organization_lock(%s::uuid)",
                [organization_id],
            )
        _execute(
            cursor,
            f"""
            SELECT projection_version
            FROM model_hub_annotation_value_status
            WHERE organization_id = %s
            {"FOR UPDATE" if publish else ""}
            """,
            [organization_id],
        )
        status_row = cursor.fetchone()
        status_exists = status_row is not None
        if status_exists and int(status_row[0]) != PROJECTION_VERSION:
            raise RuntimeError("annotation value projection status row is missing")
        _execute(
            cursor,
            """
            SELECT
                count(*) FILTER (
                    WHERE NOT s.deleted
                      AND (s.trace_id IS NOT NULL OR s.observation_span_id IS NOT NULL)
                      AND s.tracer_project_id IS NULL
                ) AS unscoped,
                count(*) FILTER (
                    WHERE NOT s.deleted
                      AND s.tracer_project_id IS NOT NULL
                      AND (s.trace_id IS NOT NULL OR s.observation_span_id IS NOT NULL)
                      AND project.id IS NOT NULL
                      AND project.organization_id = s.organization_id
                      AND label.organization_id = s.organization_id
                      AND (
                          label.project_id IS NULL
                          OR label.project_id = s.tracer_project_id
                      )
                      AND (
                          label.workspace_id IS NULL
                          OR label.workspace_id = project.workspace_id
                      )
                      AND marker.score_id IS NULL
                ) AS pending,
                count(*) FILTER (
                    WHERE marker.projection_version = %s
                      AND marker.blocked_oversize
                ) AS oversize,
                count(*) FILTER (
                    WHERE NOT s.deleted
                      AND (s.trace_id IS NOT NULL OR s.observation_span_id IS NOT NULL)
                      AND (
                          label.organization_id IS DISTINCT FROM s.organization_id
                          OR (
                              s.tracer_project_id IS NOT NULL
                              AND (
                                  project.id IS NULL
                                  OR project.organization_id IS DISTINCT FROM s.organization_id
                              )
                          )
                          OR (
                              label.project_id IS NOT NULL
                              AND label.project_id IS DISTINCT FROM s.tracer_project_id
                          )
                          OR (
                              label.workspace_id IS NOT NULL
                              AND label.workspace_id IS DISTINCT FROM project.workspace_id
                          )
                      )
                ) AS integrity_mismatch
            FROM model_hub_score AS s
            INNER JOIN model_hub_annotationslabels AS label
              ON label.id = s.label_id
             AND label.type = 'categorical'
            LEFT JOIN tracer_project AS project
              ON project.id = s.tracer_project_id
            LEFT JOIN model_hub_annotation_value_marker AS marker
              ON marker.score_id = s.id
             AND marker.projection_version = %s
            WHERE s.organization_id = %s
            """,
            [PROJECTION_VERSION, PROJECTION_VERSION, organization_id],
        )
        unscoped, pending, oversize, integrity_mismatch = (
            int(value or 0) for value in cursor.fetchone()
        )
        ready = (
            status_exists
            and unscoped == 0
            and pending == 0
            and oversize == 0
            and integrity_mismatch == 0
        )
        projected_scores = 0
        if ready:
            _execute(
                cursor,
                """
                SELECT count(*)
                FROM model_hub_annotation_value_marker
                INNER JOIN model_hub_score AS score
                  ON score.id = model_hub_annotation_value_marker.score_id
                WHERE projection_version = %s
                  AND score.organization_id = %s
                """,
                [PROJECTION_VERSION, organization_id],
            )
            projected_scores = int(cursor.fetchone()[0])
        if publish:
            _execute(
                cursor,
                """
                UPDATE model_hub_annotation_value_status
                SET ready = %s,
                    projection_version = %s,
                    projected_scores = %s,
                    updated_at = NOW()
                WHERE organization_id = %s
                """,
                [ready, PROJECTION_VERSION, projected_scores, organization_id],
            )
            if cursor.rowcount != 1:
                raise RuntimeError("annotation value projection status row is missing")
        return {
            "ready": ready,
            "pending": pending,
            "unscoped": unscoped,
            "oversize": oversize,
            "integrity_mismatch": integrity_mismatch,
            "projected_scores": projected_scores,
        }


def backfill_annotation_score_values(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_scores: int | None = DEFAULT_MAX_SCORES,
    max_organizations: int = DEFAULT_MAX_ORGANIZATIONS,
    max_runtime_seconds: float = DEFAULT_MAX_RUNTIME_SECONDS,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    lock_timeout_ms: int = DEFAULT_LOCK_TIMEOUT_MS,
    retry_after_seconds: int | None = None,
    ready_recheck_after_seconds: int | None = None,
    sleep_seconds: float = 0.0,
    audit_only: bool = False,
    continue_on_error: bool = False,
    emit=lambda _message: None,
) -> dict[str, int | bool]:
    """Run one finite, resumable projection pass and return its gate result.

    ``max_scores=None`` is retained as a compatibility input but resolves to
    the finite default. A caller that needs to drain more work reruns the
    command; the per-organization UUID cursor and marker claims make that
    retry idempotent.
    """

    finite_batch_size = int(batch_size)
    if finite_batch_size < 1 or finite_batch_size > MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    finite_max_scores = DEFAULT_MAX_SCORES if max_scores is None else int(max_scores)
    if finite_max_scores < 1 or finite_max_scores > MAX_SCORES:
        raise ValueError(f"max_scores must be between 1 and {MAX_SCORES}")
    finite_max_organizations = int(max_organizations)
    if finite_max_organizations < 1 or finite_max_organizations > MAX_ORGANIZATIONS:
        raise ValueError(f"max_organizations must be between 1 and {MAX_ORGANIZATIONS}")
    finite_runtime_seconds = float(max_runtime_seconds)
    if (
        not isfinite(finite_runtime_seconds)
        or finite_runtime_seconds <= 0
        or finite_runtime_seconds > MAX_RUNTIME_SECONDS
    ):
        raise ValueError(
            f"max_runtime_seconds must be greater than 0 and at most "
            f"{MAX_RUNTIME_SECONDS:g}"
        )
    finite_statement_timeout_ms = int(statement_timeout_ms)
    if (
        finite_statement_timeout_ms < 1
        or finite_statement_timeout_ms > MAX_STATEMENT_TIMEOUT_MS
    ):
        raise ValueError(
            f"statement_timeout_ms must be between 1 and {MAX_STATEMENT_TIMEOUT_MS}"
        )
    finite_lock_timeout_ms = int(lock_timeout_ms)
    if (
        finite_lock_timeout_ms < 1
        or finite_lock_timeout_ms > MAX_LOCK_TIMEOUT_MS
        or finite_lock_timeout_ms > finite_statement_timeout_ms
    ):
        raise ValueError(
            "lock_timeout_ms must be positive, no greater than "
            f"{MAX_LOCK_TIMEOUT_MS}, and no greater than statement_timeout_ms"
        )
    finite_sleep_seconds = float(sleep_seconds)
    if not isfinite(finite_sleep_seconds) or finite_sleep_seconds < 0:
        raise ValueError("sleep_seconds must be finite and non-negative")
    if (retry_after_seconds is None) != (ready_recheck_after_seconds is None):
        raise ValueError(
            "retry_after_seconds and ready_recheck_after_seconds must be set together"
        )
    finite_retry_after_seconds: int | None = None
    finite_ready_recheck_after_seconds: int | None = None
    if retry_after_seconds is not None and ready_recheck_after_seconds is not None:
        finite_retry_after_seconds = int(retry_after_seconds)
        finite_ready_recheck_after_seconds = int(ready_recheck_after_seconds)
        if (
            finite_retry_after_seconds < 1
            or finite_retry_after_seconds > MAX_RECHECK_SECONDS
            or finite_ready_recheck_after_seconds < finite_retry_after_seconds
            or finite_ready_recheck_after_seconds > MAX_RECHECK_SECONDS
        ):
            raise ValueError(
                "recheck intervals must be positive, ready recheck must be no "
                "shorter than retry, and both must be at most 31 days"
            )

    budget = _ProjectionBudget(
        deadline=time.monotonic() + finite_runtime_seconds,
        statement_timeout_ms=finite_statement_timeout_ms,
        lock_timeout_ms=finite_lock_timeout_ms,
    )
    token = _ACTIVE_BUDGET.set(budget)
    projected = 0
    selected = 0
    errors = 0
    runtime_exhausted = False
    score_limit_exhausted = False
    gates: list[dict[str, int | bool]] = []
    organizations: list[str] = []
    organization_page_has_more = False
    try:
        if finite_retry_after_seconds is None:
            discovered = _organization_ids(finite_max_organizations + 1)
        else:
            discovered = _organization_ids(
                finite_max_organizations + 1,
                retry_after_seconds=finite_retry_after_seconds,
                ready_recheck_after_seconds=finite_ready_recheck_after_seconds,
            )
        organization_page_has_more = len(discovered) > finite_max_organizations
        organizations = discovered[:finite_max_organizations]

        for organization_id in organizations:
            try:
                budget.remaining_ms()
                organization_complete = audit_only
                if not audit_only:
                    _ensure_status(organization_id)
                    after_id = _backfill_cursor(organization_id)
                    wrapped = False
                    organization_complete = False
                    while selected < finite_max_scores:
                        budget.remaining_ms()
                        remaining_score_budget = min(
                            finite_batch_size,
                            finite_max_scores - selected,
                        )
                        score_ids = _pending_score_ids(
                            organization_id=organization_id,
                            batch_size=remaining_score_budget,
                            after_id=after_id,
                        )
                        if not score_ids:
                            # One marker-filtered wrap proves that no row behind
                            # a persisted cursor was missed.
                            if after_id is not None and not wrapped:
                                _reset_backfill_cursor(organization_id)
                                after_id = None
                                wrapped = True
                                continue
                            organization_complete = True
                            break
                        claimed = _project_score_ids(organization_id, score_ids)
                        selected += len(score_ids)
                        projected += claimed
                        after_id = score_ids[-1]
                        emit(
                            "annotation value projection: "
                            f"organization={organization_id} "
                            f"selected={len(score_ids)} claimed={claimed} "
                            f"selected_total={selected} projected_total={projected}"
                        )
                        if finite_sleep_seconds:
                            remaining_seconds = budget.remaining_ms() / 1_000
                            if finite_sleep_seconds >= remaining_seconds:
                                raise AnnotationProjectionDeadlineExceeded(
                                    "annotation value projection runtime limit "
                                    "would be exceeded by batch sleep"
                                )
                            time.sleep(finite_sleep_seconds)

                    if not organization_complete:
                        score_limit_exhausted = selected >= finite_max_scores

                gates.append(
                    _projection_gate(
                        organization_id,
                        publish=not audit_only and organization_complete,
                    )
                )
                if score_limit_exhausted:
                    break
            except AnnotationProjectionDeadlineExceeded:
                runtime_exhausted = True
                break
            except Exception as exc:
                if not continue_on_error:
                    raise
                errors += 1
                emit(
                    "annotation value projection tenant deferred: "
                    f"organization={organization_id} error={type(exc).__name__}"
                )
                try:
                    _defer_status(organization_id)
                except AnnotationProjectionDeadlineExceeded:
                    runtime_exhausted = True
                    break
                except Exception as defer_exc:
                    emit(
                        "annotation value projection tenant rotation failed: "
                        f"organization={organization_id} "
                        f"error={type(defer_exc).__name__}"
                    )
    finally:
        _ACTIVE_BUDGET.reset(token)

    organizations_processed = len(gates) + errors
    selected_deferred = max(0, len(organizations) - organizations_processed)
    incomplete = (
        organization_page_has_more
        or runtime_exhausted
        or score_limit_exhausted
        or errors > 0
        or selected_deferred > 0
    )

    result = {
        "ready": not incomplete and all(bool(gate["ready"]) for gate in gates),
        "pending": sum(int(gate["pending"]) for gate in gates),
        "unscoped": sum(int(gate["unscoped"]) for gate in gates),
        "oversize": sum(int(gate["oversize"]) for gate in gates),
        "integrity_mismatch": sum(int(gate["integrity_mismatch"]) for gate in gates),
        "projected_scores": sum(int(gate["projected_scores"]) for gate in gates),
        "projected_this_run": projected,
        "selected_this_run": selected,
        "organizations": len(organizations),
        "organizations_processed": organizations_processed,
        "organization_page_has_more": organization_page_has_more,
        "runtime_exhausted": runtime_exhausted,
        "score_limit_exhausted": score_limit_exhausted,
        "errors": errors,
        "unready_organizations": (
            sum(1 for gate in gates if not bool(gate["ready"]))
            + errors
            + selected_deferred
            + int(organization_page_has_more)
        ),
    }
    emit(
        "annotation value readiness: "
        f"organizations={result['organizations']} "
        f"organizations_processed={result['organizations_processed']} "
        f"unready_organizations={result['unready_organizations']} "
        f"selected={selected} projected={projected} pending={result['pending']} "
        f"unscoped={result['unscoped']} oversize={result['oversize']} "
        f"integrity_mismatch={result['integrity_mismatch']} "
        f"has_more={organization_page_has_more} "
        f"runtime_exhausted={runtime_exhausted} "
        f"score_limit_exhausted={score_limit_exhausted} errors={errors} "
        f"ready={result['ready']}"
    )
    return result


class Command(BaseCommand):
    help = "Backfill and gate the exact annotation Score value projection."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
        parser.add_argument("--max-scores", type=int, default=None)
        parser.add_argument(
            "--max-organizations", type=int, default=DEFAULT_MAX_ORGANIZATIONS
        )
        parser.add_argument(
            "--max-runtime-seconds", type=float, default=DEFAULT_MAX_RUNTIME_SECONDS
        )
        parser.add_argument(
            "--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS
        )
        parser.add_argument(
            "--lock-timeout-ms", type=int, default=DEFAULT_LOCK_TIMEOUT_MS
        )
        parser.add_argument("--sleep", type=float, default=0.0)
        parser.add_argument("--audit-only", action="store_true")
        parser.add_argument("--continue-on-error", action="store_true")
        parser.add_argument("--gate", action="store_true")

    def handle(self, *args, **options):
        if options["gate"] and not options["audit_only"]:
            raise CommandError("--gate is read-only and requires --audit-only")
        if options["gate"] and options["max_scores"] is not None:
            raise CommandError("--gate cannot be combined with --max-scores")
        try:
            result = backfill_annotation_score_values(
                batch_size=options["batch_size"],
                max_scores=options["max_scores"],
                max_organizations=options["max_organizations"],
                max_runtime_seconds=options["max_runtime_seconds"],
                statement_timeout_ms=options["statement_timeout_ms"],
                lock_timeout_ms=options["lock_timeout_ms"],
                sleep_seconds=options["sleep"],
                audit_only=options["audit_only"],
                continue_on_error=options["continue_on_error"],
                emit=self.stdout.write,
            )
        except (RuntimeError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        if options["gate"] and not result["ready"]:
            raise CommandError(
                "annotation value projection is not ready: "
                f"pending={result['pending']} unscoped={result['unscoped']} "
                f"oversize={result['oversize']} "
                f"integrity_mismatch={result['integrity_mismatch']}"
            )
        if (
            not options["audit_only"]
            and options["max_scores"] is None
            and not result["ready"]
        ):
            raise CommandError(
                "strict projection is incomplete: run the "
                "Score.tracer_project_id readiness gate first; "
                f"pending={result['pending']} unscoped={result['unscoped']} "
                f"oversize={result['oversize']} "
                f"integrity_mismatch={result['integrity_mismatch']}"
            )
        self.stdout.write(self.style.SUCCESS("Annotation value projection complete."))
