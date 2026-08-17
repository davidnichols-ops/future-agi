"""Periodic repair for the exact categorical annotation-value projection.

The PostgreSQL Score trigger remains the atomic writer for live mutations.
This activity is only an idempotent repair/readiness loop: it discovers new
tenants that have no Scores yet, resumes historical marker gaps, and republishes
per-organization readiness after an exact tenant-scoped proof.
"""

import structlog

from model_hub.management.commands.backfill_annotation_score_values import (
    backfill_annotation_score_values,
)
from tfc.temporal import temporal_activity

logger = structlog.get_logger(__name__)

SCHEDULED_BATCH_SIZE = 100
SCHEDULED_MAX_SCORES = 1_000
SCHEDULED_MAX_ORGANIZATIONS = 100
SCHEDULED_MAX_RUNTIME_SECONDS = 45.0
SCHEDULED_STATEMENT_TIMEOUT_MS = 5_000
SCHEDULED_LOCK_TIMEOUT_MS = 1_000
SCHEDULED_RETRY_AFTER_SECONDS = 60
SCHEDULED_READY_RECHECK_AFTER_SECONDS = 24 * 60 * 60


@temporal_activity(time_limit=55, queue="default", max_retries=1)
def reconcile_annotation_score_values():
    """Run one finite repair pass; later schedule ticks resume any remainder."""

    result = backfill_annotation_score_values(
        batch_size=SCHEDULED_BATCH_SIZE,
        max_scores=SCHEDULED_MAX_SCORES,
        max_organizations=SCHEDULED_MAX_ORGANIZATIONS,
        max_runtime_seconds=SCHEDULED_MAX_RUNTIME_SECONDS,
        statement_timeout_ms=SCHEDULED_STATEMENT_TIMEOUT_MS,
        lock_timeout_ms=SCHEDULED_LOCK_TIMEOUT_MS,
        retry_after_seconds=SCHEDULED_RETRY_AFTER_SECONDS,
        ready_recheck_after_seconds=SCHEDULED_READY_RECHECK_AFTER_SECONDS,
        continue_on_error=True,
    )
    logger.info("annotation_score_value_reconcile_complete", **result)
    return result


__all__ = ["reconcile_annotation_score_values"]
