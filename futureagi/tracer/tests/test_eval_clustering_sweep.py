"""
The eval-clustering sweep — backstop for the per-eval trigger.

Triggers coalesce onto one workflow per project, so a dispatch that lands while
a drain is past its final fetch is folded into that run and dropped. For a
one-shot historical task there may be no next failing eval to re-trigger on, so
without this sweep the tail of that task's failures never clusters.

Discovery keys on ``EvalTask.updated_at`` rather than scanning ``EvalLogger``,
which is huge and has no standalone ``created_at`` index.
"""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from tracer.models.eval_task import EvalTask
from tracer.tasks import eval_clustering as tasks

_run = tasks.sweep_eval_clustering._original_func
_DISPATCH = "tracer.tasks.eval_clustering.dispatch_eval_clustering"


def _touch(task, *, hours_ago):
    """updated_at is auto_now, so age it with a direct UPDATE."""
    EvalTask.objects.filter(pk=task.pk).update(
        updated_at=timezone.now() - timedelta(hours=hours_ago)
    )


@pytest.fixture
def sweep_deps():
    """Patch the dispatch and the activity's connection hygiene.

    ``close_old_connections`` would drop the connection holding the test's
    transaction; it's activity boilerplate, not the discovery logic under test.
    """
    with patch("tracer.tasks.eval_clustering.close_old_connections"), patch(
        _DISPATCH
    ) as dispatch:
        yield dispatch


@pytest.mark.django_db
def test_dispatches_for_recently_active_task(project, eval_task, sweep_deps):
    _touch(eval_task, hours_ago=1)

    result = _run()

    sweep_deps.assert_called_once_with(project.id)
    assert result == {"projects": 1}


@pytest.mark.django_db
def test_skips_tasks_outside_the_lookback(eval_task, sweep_deps):
    _touch(eval_task, hours_ago=tasks._SWEEP_LOOKBACK_HOURS + 1)

    result = _run()

    sweep_deps.assert_not_called()
    assert result == {"projects": 0}


@pytest.mark.django_db
def test_one_dispatch_per_project(project, eval_task, sweep_deps):
    """Several active tasks in one project must collapse to a single dispatch —
    the drain is per project, not per task."""
    second = EvalTask.objects.create(project=project, name="second task")
    _touch(eval_task, hours_ago=1)
    _touch(second, hours_ago=2)

    result = _run()

    sweep_deps.assert_called_once_with(project.id)
    assert result == {"projects": 1}
