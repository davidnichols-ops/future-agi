import pytest

from tfc.management.commands.start_temporal_worker import _generic_all_queues


@pytest.mark.unit
def test_generic_all_queue_worker_excludes_dedicated_exact_queue():
    registered = ["default", "tasks_xl", "exact_aggregation", "trace_ingestion"]

    assert _generic_all_queues(registered) == [
        "default",
        "tasks_xl",
        "trace_ingestion",
    ]


@pytest.mark.unit
def test_generic_all_queue_worker_preserves_other_queue_order():
    registered = ["agent_compass", "tasks_s", "tasks_l"]

    assert _generic_all_queues(registered) == registered
