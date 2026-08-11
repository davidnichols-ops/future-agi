"""
Temporal activities for eval result clustering.

Mirrors trace_scanner tasks — cluster failing eval results into
TraceErrorGroup rows with source="eval".
"""

from datetime import timedelta

import structlog
from django.db import close_old_connections
from django.utils import timezone

from tfc.temporal import temporal_activity

logger = structlog.get_logger(__name__)

# Backstop on the per-dispatch drain loop: at most this many batches
# (× _CLUSTER_BATCH_LIMIT rows) before yielding. Every clustered row leaves a
# junction row, so the fetchable set strictly shrinks and normal drains stop on
# a short batch well before this — it only bounds a project whose backlog is
# genuinely larger than the loop can drain in one dispatch.
_MAX_DRAIN_BATCHES = 60

# How far back the sweep looks for eval tasks worth re-dispatching.
_SWEEP_LOOKBACK_HOURS = 24


def dispatch_eval_clustering(project_id) -> None:
    """Dispatch a project's clustering drain, coalesced per project.

    The single place that knows the dispatch contract, because there are two
    trigger sites that must not drift: ``run_entry`` (every eval-task eval, both
    the historical and continuous workflows) and the span-eval wrapper in
    ``tracer.utils.eval`` (feedback-driven re-evals, which never reach
    ``run_entry``).

    A fixed ``eval-cluster-{project_id}`` workflow id + USE_EXISTING collapses a
    burst of triggers onto the single in-flight drain. Fail-open, but at WARNING
    — never DEBUG: a silently swallowed dispatch is exactly what hid the cutover
    regression, and a clustering hiccup must not fail an eval that already
    produced a result.
    """
    try:
        from temporalio.common import WorkflowIDConflictPolicy

        cluster_eval_results_task.apply_async(
            args=(str(project_id),),
            task_id=f"eval-cluster-{project_id}",
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    except Exception:
        logger.warning(
            "eval_clustering_dispatch_failed",
            project_id=str(project_id),
            exc_info=True,
        )


@temporal_activity(time_limit=3600, queue="agent_compass", max_retries=1)
def cluster_eval_results_task(project_id: str):
    """Drain a project's unclustered failing eval-task results.

    Triggered per failing eval-task eval by ``run_entry`` — both the historical
    and continuous eval-task workflows drain every entry through it, so this
    covers both. Loops ``cluster_eval_results`` until a batch comes back short:
    one dispatch fully drains the project's current backlog, which is what lets
    us drop the old self-continuation (its distinct-id follow-up raced concurrent
    triggers). Coalesced per project via the fixed ``eval-cluster-{project_id}``
    id + USE_EXISTING at the call site, so a burst of triggers collapses onto one
    run.

    Termination keys on ``fetched``: every row clustered in a batch leaves a
    junction row carrying its ``eval_logger_id``, so it drops out of the next
    fetch and a short batch means the backlog is drained. ``clustered == 0`` on a
    full batch means a downstream dependency is failing, not that we're done — it
    stops the loop rather than let it spin.
    """
    from tracer.utils.eval_clustering import (
        _CLUSTER_BATCH_LIMIT,
        cluster_eval_results,
    )

    close_old_connections()

    clustered = new_clusters = assigned = 0
    for _ in range(_MAX_DRAIN_BATCHES):
        summary = cluster_eval_results(project_id)
        clustered += summary.clustered
        new_clusters += summary.new_clusters
        assigned += summary.assigned
        if summary.fetched < _CLUSTER_BATCH_LIMIT or summary.clustered == 0:
            break

    logger.info(
        "cluster_eval_results_task_completed",
        project_id=project_id,
        clustered=clustered,
        new_clusters=new_clusters,
        assigned=assigned,
    )
    return {
        "clustered": clustered,
        "new_clusters": new_clusters,
        "assigned": assigned,
    }


@temporal_activity(time_limit=300, queue="agent_compass", max_retries=0)
def sweep_eval_clustering():
    """Backstop the per-eval clustering trigger for recently active eval tasks.

    The trigger coalesces on a fixed per-project workflow id, so a dispatch that
    arrives while a drain is past its final fetch is folded into that run and
    dropped. Rows committed in that window then wait for the project's next
    failing eval-task eval — which, for a finished one-shot historical task,
    never comes. This sweep re-dispatches through the same coalescing gate, so
    the tail always drains on the following tick.

    Discovery keys on ``EvalTask`` rather than a time-window scan of
    ``EvalLogger``: EvalLogger is huge and has no standalone ``created_at`` index
    (only a composite with ``trace``), while EvalTask is small and both task
    shapes touch it — historical tasks when they finalize, continuous ones on
    every cursor advance. Dispatch is a no-op for a project with nothing
    unclustered, so a slightly wide net costs nothing.

    ``max_retries=0``: the next tick recovers a sweep-level failure.
    """
    from tracer.models.eval_task import EvalTask

    close_old_connections()

    since = timezone.now() - timedelta(hours=_SWEEP_LOOKBACK_HOURS)
    project_ids = list(
        EvalTask.objects.filter(updated_at__gte=since)
        # Clear Meta.ordering first: it would put created_at in the SELECT and
        # DISTINCT would then key on (project_id, created_at) — one dispatch per
        # task instead of per project.
        .order_by()
        .values_list("project_id", flat=True)
        .distinct()
    )
    for project_id in project_ids:
        dispatch_eval_clustering(project_id)

    logger.info("sweep_eval_clustering_completed", projects=len(project_ids))
    return {"projects": len(project_ids)}
