from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from clickhouse_driver.errors import ServerException
from django.core.cache import cache

from tracer.services.clickhouse.exact_graph_reads import (
    _filter_relation_requirements,
    output_bucket_partitions,
    read_exact_annotation_graph,
    read_exact_eval_graph,
    read_exact_session_system_graph,
    read_exact_system_graph,
    read_exact_user_system_graph,
)
from tracer.services.clickhouse.query_builders.dashboard import AGGREGATIONS
from tracer.services.clickhouse.query_builders.dataset_dashboard import (
    DATASET_AGGREGATIONS,
)
from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
    compile_exact_graph_filter_predicates,
)
from tracer.services.clickhouse.query_builders.simulation_dashboard import (
    SIMULATION_AGGREGATIONS,
)
from tracer.services.exact_aggregation_cache import (
    _exact_refresh_workflow_task_id,
    begin_exact_refresh,
    exact_payload_is_complete,
    exact_refresh_state,
    finish_exact_refresh,
    publish_exact_snapshot,
    publish_exact_snapshot_for_refresh,
    read_exact_snapshot,
    read_or_schedule_exact_snapshot,
    refresh_claim_is_current,
    snapshot_cache_key,
)


def _time_filter(start: datetime, end: datetime) -> dict:
    return {
        "column_id": "start_time",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [start, end],
        },
    }


def _combined_session_filters(start: datetime, end: datetime) -> list[dict]:
    return [
        _time_filter(start, end),
        {
            "column_id": "status",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "ERROR",
            },
        },
        {
            "column_id": "session_id",
            "filter_config": {
                "filter_type": "text",
                "filter_op": "in",
                "filter_value": ["11111111-1111-4111-8111-111111111111"],
            },
        },
        {
            "column_id": "duration",
            "filter_config": {
                "filter_type": "number",
                "filter_op": "greater_than_or_equal",
                "filter_value": 5,
            },
        },
        {
            "column_id": "first_message",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "contains",
                "filter_value": "hello",
            },
        },
    ]


@pytest.mark.unit
def test_output_partitions_only_cut_on_bucket_boundaries():
    start = datetime(2026, 8, 1, 0, 17)
    end = datetime(2026, 8, 1, 8, 42)

    partitions = output_bucket_partitions(start, end, "hour", max_buckets=3)

    assert partitions == (
        (start, datetime(2026, 8, 1, 3, 0)),
        (datetime(2026, 8, 1, 3, 0), datetime(2026, 8, 1, 6, 0)),
        (datetime(2026, 8, 1, 6, 0), end),
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "aggregations",
    [AGGREGATIONS, DATASET_AGGREGATIONS, SIMULATION_AGGREGATIONS],
)
def test_public_dashboard_operators_are_exact(aggregations):
    assert aggregations["median"].startswith("quantileExact(")
    assert aggregations["p95"].startswith("quantileExact(")
    assert aggregations["count_distinct"].startswith("uniqExact(")


@pytest.mark.unit
def test_exact_empty_payload_is_atomically_cacheable():
    cache.clear()
    payload = {
        "metric_name": "latency",
        "data": [],
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }

    published = publish_exact_snapshot("test-empty", {"project": "p"}, payload)

    assert published["data"] == []
    assert published["query_cached"] is False
    assert published["query_completed_at"]


@pytest.mark.unit
@pytest.mark.parametrize("query_sampled", [None, True, "false", 0])
def test_exact_payload_requires_explicit_false_sampling_attestation(query_sampled):
    payload = {
        "data": [],
        "query_complete": True,
        "query_status": "complete",
    }
    if query_sampled is not None:
        payload["query_sampled"] = query_sampled

    assert exact_payload_is_complete(payload) is False


@pytest.mark.unit
def test_exact_payload_rejects_child_metric_without_sampling_attestation():
    payload = {
        "metrics": [
            {
                "data": [],
                "query_complete": True,
                "query_status": "complete",
            }
        ],
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }

    assert exact_payload_is_complete(payload) is False


@pytest.mark.unit
def test_refresh_failure_serves_prior_exact_snapshot_without_replacing_it():
    cache.clear()
    identity = {"project": "p", "metric": "latency"}
    first = publish_exact_snapshot(
        "test-refresh",
        identity,
        {
            "metric_name": "latency",
            "data": [{"timestamp": "2026-08-01T00:00:00", "value": 4}],
            "query_complete": True,
            "query_status": "complete",
            "query_sampled": False,
        },
    )

    token = begin_exact_refresh("test-refresh", identity)
    assert token
    finish_exact_refresh(
        "test-refresh",
        identity,
        token,
        succeeded=False,
    )

    stale = read_or_schedule_exact_snapshot(
        "test-refresh",
        identity,
        refresh=False,
        pending_payload={
            "metric_name": "latency",
            "data": [],
            "query_complete": False,
            "query_status": "pending",
            "query_sampled": False,
        },
    )

    assert stale["data"] == first["data"]
    assert stale["query_completed_at"] == first["query_completed_at"]
    assert stale["query_cached"] is True
    assert stale["query_refresh_failed"] is True
    assert stale["query_refreshing"] is False


@pytest.mark.unit
def test_cold_miss_is_pending_poll_dedupes_then_exact_publish_becomes_visible():
    cache.clear()
    identity = {"project": "p", "metric": "traffic"}
    pending = {
        "metric_name": "traffic",
        "data": [],
        "query_complete": False,
        "query_status": "pending",
        "query_sampled": False,
    }

    with patch(
        "tracer.tasks.exact_aggregation.refresh_exact_aggregation_snapshot.apply_async"
    ) as enqueue:
        first = read_or_schedule_exact_snapshot(
            "test-cold", identity, refresh=False, pending_payload=pending
        )
        second = read_or_schedule_exact_snapshot(
            "test-cold", identity, refresh=False, pending_payload=pending
        )

    assert first["query_status"] == "pending"
    assert first["query_refreshing"] is True
    assert second["query_status"] == "pending"
    assert enqueue.call_count == 1
    task_kwargs = enqueue.call_args.kwargs["kwargs"]
    assert enqueue.call_args.kwargs["queue"] == "tasks_xl"
    assert enqueue.call_args.kwargs["task_id"].startswith("exact-aggregation-")
    from temporalio.common import WorkflowIDConflictPolicy

    assert (
        enqueue.call_args.kwargs["id_conflict_policy"]
        == WorkflowIDConflictPolicy.USE_EXISTING
    )
    assert enqueue.call_args.kwargs["dispatch_timeout_seconds"] == 2.0

    exact = publish_exact_snapshot(
        "test-cold",
        identity,
        {
            "metric_name": "traffic",
            "data": [],
            "query_complete": True,
            "query_status": "complete",
            "query_sampled": False,
        },
    )
    finish_exact_refresh(
        "test-cold",
        identity,
        task_kwargs["refresh_token"],
        succeeded=True,
    )
    polled = read_or_schedule_exact_snapshot(
        "test-cold", identity, refresh=False, pending_payload=pending
    )

    assert polled["query_status"] == "complete"
    assert polled["query_completed_at"] == exact["query_completed_at"]
    assert polled["query_refreshing"] is False


@pytest.mark.unit
def test_concurrent_cold_requests_enqueue_only_one_refresh():
    cache.clear()
    identity = {"project": "p", "metric": "cost"}
    pending = {
        "metric_name": "cost",
        "data": [],
        "query_complete": False,
        "query_status": "pending",
        "query_sampled": False,
    }

    with patch(
        "tracer.tasks.exact_aggregation.refresh_exact_aggregation_snapshot.apply_async"
    ) as enqueue:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda _index: read_or_schedule_exact_snapshot(
                        "test-concurrent",
                        identity,
                        refresh=False,
                        pending_payload=pending,
                    ),
                    range(16),
                )
            )

    assert enqueue.call_count == 1
    assert all(result["query_status"] == "pending" for result in results)
    assert all(result["query_refreshing"] is True for result in results)


@pytest.mark.unit
def test_cold_miss_without_a_persisted_claim_fails_closed_instead_of_spinning(
    monkeypatch,
):
    cache.clear()
    pending = {
        "metric_name": "cost",
        "data": [],
        "query_complete": False,
        "query_status": "pending",
        "query_sampled": False,
    }
    monkeypatch.setattr(
        "tracer.services.exact_aggregation_cache.begin_exact_refresh",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "tracer.services.exact_aggregation_cache.exact_refresh_state",
        lambda *_args: None,
    )

    result = read_or_schedule_exact_snapshot(
        "test-unavailable-cache",
        {"project": "p", "metric": "cost"},
        refresh=False,
        pending_payload=pending,
    )

    assert result["query_refresh_failed"] is True
    assert result["query_refreshing"] is False


@pytest.mark.unit
def test_cold_miss_enqueue_failure_releases_claim_and_fails_closed():
    cache.clear()
    identity = {"project": "p", "metric": "cost"}
    pending = {
        "metric_name": "cost",
        "data": [],
        "query_complete": False,
        "query_status": "pending",
        "query_sampled": False,
    }

    with patch(
        "tracer.tasks.exact_aggregation.refresh_exact_aggregation_snapshot.apply_async",
        side_effect=TimeoutError("Temporal unavailable"),
    ):
        result = read_or_schedule_exact_snapshot(
            "test-enqueue-failure",
            identity,
            refresh=False,
            pending_payload=pending,
        )

    assert result["query_refresh_failed"] is True
    assert result["query_refreshing"] is False
    assert exact_refresh_state("test-enqueue-failure", identity) == "failed"


@pytest.mark.unit
def test_background_worker_publishes_only_after_complete_loader(monkeypatch):
    from tracer.tasks import exact_aggregation as task_module

    cache.clear()
    identity = {"project": "p", "metric": "tokens"}
    token = begin_exact_refresh("observe-test-worker", identity)
    assert token
    monkeypatch.setattr(
        task_module,
        "_load_exact_payload",
        lambda *_args: {
            "metric_name": "tokens",
            "data": [],
            "query_complete": True,
            "query_status": "complete",
            "query_sampled": False,
        },
    )

    task_module.refresh_exact_aggregation_snapshot.run_sync(
        namespace="observe-test-worker",
        identity=identity,
        refresh_token=token,
    )

    polled = read_or_schedule_exact_snapshot(
        "observe-test-worker",
        identity,
        refresh=False,
        pending_payload={},
    )
    assert polled["query_status"] == "complete"
    assert exact_refresh_state("observe-test-worker", identity) is None


@pytest.mark.unit
def test_background_worker_failure_leaves_cache_unpublished_and_retryable(monkeypatch):
    from tracer.tasks import exact_aggregation as task_module

    cache.clear()
    identity = {"project": "p", "metric": "errors"}
    token = begin_exact_refresh("observe-test-worker-failure", identity)
    assert token

    def fail(*_args):
        raise RuntimeError("private query detail")

    monkeypatch.setattr(task_module, "_load_exact_payload", fail)
    with pytest.raises(RuntimeError, match="exact aggregation refresh failed"):
        task_module.refresh_exact_aggregation_snapshot.run_sync(
            namespace="observe-test-worker-failure",
            identity=identity,
            refresh_token=token,
        )

    assert exact_refresh_state("observe-test-worker-failure", identity) == "failed"
    failed = read_or_schedule_exact_snapshot(
        "observe-test-worker-failure",
        identity,
        refresh=False,
        pending_payload={
            "metric_name": "errors",
            "data": [],
            "query_complete": False,
            "query_status": "pending",
            "query_sampled": False,
        },
    )
    assert failed["query_refresh_failed"] is True
    assert failed["query_refreshing"] is False


@pytest.mark.unit
def test_exact_refresh_is_registered_on_existing_temporal_xl_worker():
    from tfc.temporal.common.registry import (
        TEMPORAL_ACTIVITY_MODULES,
        get_workflows_for_queue,
    )
    from tfc.temporal.drop_in.decorator import _ACTIVITY_REGISTRY
    from tfc.temporal.drop_in.workflow import TaskRunnerWorkflow
    from tracer.tasks.exact_aggregation import refresh_exact_aggregation_snapshot

    metadata = _ACTIVITY_REGISTRY[refresh_exact_aggregation_snapshot.name]
    assert metadata["queue"] == "tasks_xl"
    assert metadata["time_limit"] == 60 * 60
    assert metadata["max_retries"] == 0
    assert "tracer.tasks" in TEMPORAL_ACTIVITY_MODULES
    assert TaskRunnerWorkflow in get_workflows_for_queue("tasks_xl")


@pytest.mark.unit
def test_exact_refresh_workflow_id_is_deterministic_and_opaque_per_claim():
    token = "do-not-expose-this-refresh-token"

    first = _exact_refresh_workflow_task_id(token)
    second = _exact_refresh_workflow_task_id(token)

    assert first == second
    assert first.startswith("exact-aggregation-")
    assert token not in first
    assert first != _exact_refresh_workflow_task_id(f"{token}-next")


@pytest.mark.unit
def test_redelivered_exact_refresh_cannot_publish_after_claim_finished(monkeypatch):
    from tracer.tasks import exact_aggregation as task_module

    cache.clear()
    identity = {"project": "p", "metric": "errors"}
    token = begin_exact_refresh("observe-test-redelivery", identity)
    assert token
    finish_exact_refresh(
        "observe-test-redelivery",
        identity,
        token,
        succeeded=True,
    )
    monkeypatch.setattr(
        task_module,
        "_load_exact_payload",
        lambda *_args: pytest.fail("a stale activity must not query ClickHouse"),
    )

    task_module.refresh_exact_aggregation_snapshot.run_sync(
        namespace="observe-test-redelivery",
        identity=identity,
        refresh_token=token,
    )


@pytest.mark.unit
def test_old_worker_cannot_publish_or_clear_a_newer_refresh_claim():
    cache.clear()
    namespace = "observe-test-token-fence"
    identity = {"project": "p", "metric": "latency"}
    old_token = begin_exact_refresh(namespace, identity)
    assert old_token
    finish_exact_refresh(namespace, identity, old_token, succeeded=False)
    new_token = begin_exact_refresh(namespace, identity)
    assert new_token and new_token != old_token
    payload = {
        "metric_name": "latency",
        "data": [{"timestamp": "2026-08-01T00:00:00", "value": 9}],
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }

    assert (
        publish_exact_snapshot_for_refresh(
            namespace,
            identity,
            payload,
            old_token,
        )
        is None
    )
    finish_exact_refresh(namespace, identity, old_token, succeeded=True)

    assert refresh_claim_is_current(namespace, identity, new_token) is True
    assert read_exact_snapshot(namespace, identity) is None

    published = publish_exact_snapshot_for_refresh(
        namespace,
        identity,
        payload,
        new_token,
    )
    assert published is not None
    assert published["data"] == payload["data"]
    assert refresh_claim_is_current(namespace, identity, new_token) is False


@pytest.mark.unit
def test_redis_lua_fence_rejects_old_token_and_atomically_publishes_new(monkeypatch):
    import pickle

    from tracer.services import exact_aggregation_cache as cache_module

    class FakeRawRedis:
        def __init__(self):
            self.values = {}
            self.calls = []

        def eval(self, script, numkeys, *parts):
            keys = parts[:numkeys]
            args = parts[numkeys:]
            self.calls.append((script, keys, args))
            if script == cache_module._REDIS_FENCED_PUBLISH_SCRIPT:
                lock_key, snapshot_key, state_key = keys
                token, stored, _ttl_ms = args
                if self.values.get(lock_key) != token:
                    return 0
                self.values[snapshot_key] = stored
                self.values.pop(state_key, None)
                self.values.pop(lock_key, None)
                return 1
            if script == cache_module._REDIS_FENCED_FINISH_SCRIPT:
                lock_key, state_key = keys
                token, succeeded, failed_state, _ttl_ms = args
                if self.values.get(lock_key) != token:
                    return 0
                if str(succeeded) == "1":
                    self.values.pop(state_key, None)
                else:
                    self.values[state_key] = failed_state
                self.values.pop(lock_key, None)
                return 1
            raise AssertionError("unexpected Redis script")

    class FakeRedisAdapter:
        def __init__(self):
            self.raw = FakeRawRedis()

        def get_client(self, *, write):
            assert write is True
            return self.raw

        @staticmethod
        def make_key(key):
            return f"futureagi:1:{key}"

        @staticmethod
        def encode(value):
            return pickle.dumps(value)

    adapter = FakeRedisAdapter()
    monkeypatch.setattr(cache_module, "cache", SimpleNamespace(client=adapter))
    namespace = "observe-test-redis-token-fence"
    identity = {"project": "p", "metric": "traffic"}
    old_token = "old-token"
    new_token = "new-token"
    lock_key = adapter.make_key(cache_module._refresh_lock_key(namespace, identity))
    state_key = adapter.make_key(cache_module._refresh_state_key(namespace, identity))
    snapshot_key = adapter.make_key(
        cache_module.snapshot_cache_key(namespace, identity)
    )
    adapter.raw.values[lock_key] = adapter.encode(new_token)
    adapter.raw.values[state_key] = adapter.encode(
        {"status": "running", "token": new_token}
    )
    payload = {
        "metric_name": "traffic",
        "data": [],
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
    }

    assert (
        cache_module.publish_exact_snapshot_for_refresh(
            namespace,
            identity,
            payload,
            old_token,
        )
        is None
    )
    cache_module.finish_exact_refresh(
        namespace,
        identity,
        old_token,
        succeeded=False,
    )
    assert adapter.raw.values[lock_key] == adapter.encode(new_token)
    assert snapshot_key not in adapter.raw.values

    published = cache_module.publish_exact_snapshot_for_refresh(
        namespace,
        identity,
        payload,
        new_token,
    )

    assert published is not None
    assert lock_key not in adapter.raw.values
    assert state_key not in adapter.raw.values
    assert pickle.loads(adapter.raw.values[snapshot_key])["payload"] == payload
    assert [call[0] for call in adapter.raw.calls] == [
        cache_module._REDIS_FENCED_PUBLISH_SCRIPT,
        cache_module._REDIS_FENCED_FINISH_SCRIPT,
        cache_module._REDIS_FENCED_PUBLISH_SCRIPT,
    ]


@pytest.mark.unit
def test_snapshot_key_fails_closed_for_unknown_identity_types():
    with pytest.raises(TypeError, match="unsupported snapshot identity type"):
        snapshot_cache_key("test", {"bad": object()})


@pytest.mark.unit
def test_cache_outage_does_not_hide_a_fresh_exact_result(monkeypatch):
    from tracer.services import exact_aggregation_cache as cache_module

    class BrokenCache:
        def set(self, *_args, **_kwargs):
            raise ConnectionError("redis unavailable")

    monkeypatch.setattr(cache_module, "cache", BrokenCache())
    published = publish_exact_snapshot(
        "test-outage",
        {"project": "p"},
        {
            "metric_name": "traffic",
            "data": [],
            "query_complete": True,
            "query_status": "complete",
            "query_sampled": False,
        },
    )

    assert published["query_complete"] is True
    assert published["query_cached"] is False
    assert published["query_completed_at"]


class _ConcurrentArrivalAnalytics:
    def __init__(self):
        self.partition_calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "now64" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        self.partition_calls.append((query, dict(params), dict(settings)))
        # Pretend a newer physical version arrives after the first partition.
        # The service must keep using the original ceiling for every partition.
        return SimpleNamespace(data=[], columns=["time_bucket"])


class _BudgetSplittingAnalytics:
    def __init__(self, *, error_code: int = 159):
        self.error_code = error_code
        self.partition_calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "now64" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        self.partition_calls.append((query, dict(params), timeout_ms, dict(settings)))
        if (params["end_date"] - params["start_date"]).total_seconds() > 3600:
            raise ServerException("private detail", code=self.error_code)
        return SimpleNamespace(data=[], columns=["time_bucket"])


def _exact_multi_filters(start: datetime, end: datetime) -> list[dict]:
    return [
        _time_filter(start, end),
        {
            "column_id": "final_status",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "text",
                "filter_op": "in",
                "filter_value": ["Rechazado"],
            },
        },
        {
            "column_id": "confidence",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "number",
                "filter_op": "greater_than_or_equal",
                "filter_value": 0.8,
            },
        },
    ]


def _exact_structured_filters(start: datetime, end: datetime) -> list[dict]:
    return [
        _time_filter(start, end),
        {
            "column_id": "final_status",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "Rechazado",
            },
        },
        {
            "column_id": "tags",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "array",
                "filter_op": "contains",
                "filter_value": ["vip", 7, True],
            },
        },
        {
            "column_id": "profile",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "map",
                "filter_op": "contains",
                "filter_value": {"tier": "gold", "enabled": True},
            },
        },
        {
            "column_id": "legacy_payload",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "json",
                "filter_op": "contains",
                "filter_value": {"kind": "customer"},
            },
        },
    ]


def _combined_relation_filters(start: datetime, end: datetime) -> list[dict]:
    return [
        _time_filter(start, end),
        {
            "column_id": "has_eval",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "boolean",
                "filter_op": "equals",
                "filter_value": True,
            },
        },
        {
            "column_id": "has_annotation",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "boolean",
                "filter_op": "equals",
                "filter_value": True,
            },
        },
        {
            "column_id": "user_id",
            "filter_config": {
                "col_type": "TRACE_END_USER",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "customer-42",
            },
        },
    ]


class _RelationSnapshotAnalytics:
    def __init__(self, *, fail_table: str | None = None):
        self.fail_table = fail_table
        self.capture_calls: list[str] = []
        self.main_calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "toUnixTimestamp64Nano(now64" in query:
            self.capture_calls.append("spans")
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        if "max(_peerdb_version)" in query and "FROM tracer_eval_logger" in query:
            self.capture_calls.append("tracer_eval_logger")
            if self.fail_table == "tracer_eval_logger":
                raise RuntimeError("eval ceiling unavailable")
            return SimpleNamespace(
                data=[{"version_ceiling": 701}],
                columns=["version_ceiling"],
            )
        if "max(_peerdb_version)" in query and "FROM model_hub_score" in query:
            self.capture_calls.append("model_hub_score")
            if self.fail_table == "model_hub_score":
                raise RuntimeError("score ceiling unavailable")
            return SimpleNamespace(
                data=[{"version_ceiling": 801}],
                columns=["version_ceiling"],
            )
        if "max(toUnixTimestamp64Micro(version))" in query:
            table = next(
                (
                    name
                    for name in (
                        "end_user_id_remap",
                        "trace_session_id_remap",
                        "end_users",
                    )
                    if f"FROM {name}" in query
                ),
                "unknown_datetime_relation",
            )
            self.capture_calls.append(table)
            if self.fail_table == table:
                raise RuntimeError(f"{table} ceiling unavailable")
            return SimpleNamespace(
                data=[{"version_ceiling": 901}],
                columns=["version_ceiling"],
            )
        self.main_calls.append((query, dict(params), dict(settings)))
        if params.get("candidate_trace_ids"):
            return SimpleNamespace(
                data=[
                    {"trace_id": trace_id} for trace_id in params["candidate_trace_ids"]
                ],
                columns=["trace_id"],
            )
        if params.get("candidate_span_ids"):
            return SimpleNamespace(
                data=[
                    {"id": span_id, "identity_count": 1, "matched": 1}
                    for span_id in params["candidate_span_ids"]
                ],
                columns=["id", "identity_count", "matched"],
            )
        return SimpleNamespace(data=[], columns=["time_bucket"])


@pytest.mark.unit
@pytest.mark.parametrize(
    ("item", "expected_eval", "expected_score", "expected_end_users"),
    [
        (
            {
                "column_id": "eval-config",
                "filter_config": {"col_type": "EVAL_METRIC"},
            },
            True,
            False,
            False,
        ),
        (
            {"columnId": "has_eval", "filterConfig": {"colType": "NORMAL"}},
            True,
            False,
            False,
        ),
        (
            {
                "column_id": "annotation-label",
                "filter_config": {"col_type": "ANNOTATION"},
            },
            False,
            True,
            False,
        ),
        (
            {"column_id": "has_annotation", "filter_config": {}},
            False,
            True,
            False,
        ),
        (
            {"column_id": "my_annotations", "filter_config": {}},
            False,
            True,
            False,
        ),
        (
            {
                "column_id": "user_id",
                "filter_config": {"col_type": "TRACE_END_USER"},
            },
            False,
            False,
            True,
        ),
    ],
)
def test_filter_relation_snapshot_plan_detects_every_relational_filter(
    item,
    expected_eval,
    expected_score,
    expected_end_users,
):
    requirements = _filter_relation_requirements([item])

    assert requirements.eval_logger is expected_eval
    assert requirements.score is expected_score
    assert requirements.end_users is expected_end_users


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_combined_eval_annotation_filters_freeze_once_and_reuse_every_partition(
    monkeypatch,
    observe_type,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 4, 15)
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    result = read_exact_system_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=_combined_relation_filters(start, end),
        interval="day",
        metric_id="traffic",
        observe_type=observe_type,
    )

    assert analytics.capture_calls == [
        "spans",
        "tracer_eval_logger",
        "model_hub_score",
        "end_users",
    ]
    assert len(analytics.main_calls) > 1
    assert all(
        call_settings["additional_table_filters"]
        == {
            "spans": "_version < 900",
            "tracer_eval_logger": "_peerdb_version < 701",
            "model_hub_score": "_peerdb_version < 801",
            "end_users": "toUnixTimestamp64Micro(version) < 901",
        }
        for _query, _params, call_settings in analytics.main_calls
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_user_id_filter_alone_freezes_curated_end_users_once():
    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 4, 15)
    user_filter = _combined_relation_filters(start, end)[-1]

    result = read_exact_system_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=[_time_filter(start, end), user_filter],
        interval="day",
        metric_id="traffic",
        observe_type="trace",
    )

    assert analytics.capture_calls == ["spans", "end_users"]
    assert len(analytics.main_calls) > 1
    assert all(
        call_settings["additional_table_filters"]
        == {
            "spans": "_version < 900",
            "end_users": "toUnixTimestamp64Micro(version) < 901",
        }
        for _query, _params, call_settings in analytics.main_calls
    )
    assert result["query_complete"] is True


@pytest.mark.unit
def test_relation_ceiling_capture_failure_prevents_complete_graph(monkeypatch):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics(fail_table="model_hub_score")
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    with pytest.raises(RuntimeError, match="score ceiling unavailable"):
        read_exact_system_graph(
            analytics=analytics,
            project_id="11111111-1111-4111-8111-111111111111",
            filters=_combined_relation_filters(
                datetime(2026, 1, 1), datetime(2026, 4, 15)
            ),
            interval="day",
            metric_id="traffic",
            observe_type="trace",
        )

    assert analytics.capture_calls == [
        "spans",
        "tracer_eval_logger",
        "model_hub_score",
    ]
    assert analytics.main_calls == []


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_exact_system_graph_combines_scalar_array_map_and_legacy_json(observe_type):
    analytics = _ConcurrentArrivalAnalytics()
    start = datetime(2026, 8, 1)
    end = datetime(2026, 8, 5)

    result = read_exact_system_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=_exact_structured_filters(start, end),
        interval="day",
        metric_id="traffic",
        observe_type=observe_type,
    )

    assert analytics.partition_calls
    query, params, settings = analytics.partition_calls[0]
    assert "attrs_string" in query
    assert "JSONExtractArrayRaw(attributes_extra" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert "toString(JSONType(attributes_extra" in query
    assert params["snapshot_start_date"] == start
    assert params["snapshot_end_date"] == end
    assert params["latest_filter_key_2"] == "tags"
    assert params["latest_filter_key_3"] == "profile"
    assert params["latest_filter_key_4"] == "legacy_payload"
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    structured_memberships = query.count(
        "SELECT DISTINCT trace_id\n                FROM spans FINAL"
    )
    assert structured_memberships == (3 if observe_type == "trace" else 0)
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("filter_type", "filter_op", "filter_value", "expected_type", "negated"),
    [
        ("array", "is_null", None, "Array", True),
        ("array", "is_not_null", None, "Array", False),
        ("map", "is_null", None, "Object", True),
        ("map", "is_not_null", None, "Object", False),
        ("json", "is_null", None, "Object", True),
    ],
)
def test_exact_structured_null_domain_covers_missing_null_and_type_mismatch(
    filter_type,
    filter_op,
    filter_value,
    expected_type,
    negated,
):
    # A legacy json null filter is value-sensitive. Use an object-shaped value
    # hint so the compatibility path selects the map domain.
    if filter_type == "json":
        filter_value = {}
    clause, params = compile_exact_graph_filter_predicates(
        [
            {
                "column_id": "payload",
                "filter_config": {
                    "col_type": "SPAN_ATTRIBUTE",
                    "filter_type": filter_type,
                    "filter_op": filter_op,
                    "filter_value": filter_value,
                },
            }
        ],
        project_id="11111111-1111-4111-8111-111111111111",
        observe_type="span",
    )

    assert "JSONHas(attributes_extra" in clause
    assert f"= '{expected_type}'" in clause
    assert ("NOT (" in clause) is negated
    assert params["latest_filter_key_0"] == "payload"


@pytest.mark.unit
def test_exact_graph_budget_retry_splits_only_buckets_and_keeps_multi_filters():
    analytics = _BudgetSplittingAnalytics()
    start = datetime(2026, 8, 1, 0, 0)
    end = datetime(2026, 8, 1, 4, 0)

    result = read_exact_system_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=_exact_multi_filters(start, end),
        interval="hour",
        metric_id="traffic",
        observe_type="trace",
    )

    # The 4h probe, then each 2h probe, fail under the bounded profile. Four
    # indivisible output buckets complete exactly; the snapshot query is the
    # eighth and final query counted in the response metadata.
    assert result["query_count"] == 8
    assert result["query_complete"] is True
    assert result["query_sampled"] is False
    successful_ranges = [
        (params["start_date"], params["end_date"])
        for _query, params, _timeout, _settings in analytics.partition_calls
        if (params["end_date"] - params["start_date"]).total_seconds() <= 3600
    ]
    assert successful_ranges == [
        (datetime(2026, 8, 1, hour), datetime(2026, 8, 1, hour + 1))
        for hour in range(4)
    ]
    assert {
        timeout for _query, _params, timeout, _settings in analytics.partition_calls
    } == {30_000, 300_000}
    assert all(
        params["snapshot_start_date"] == start
        and params["snapshot_end_date"] == end
        and "attrs_string" in query
        and "attrs_number" in query
        and settings["additional_table_filters"]["spans"] == "_version < 900"
        for query, params, _timeout, settings in analytics.partition_calls
    )


@pytest.mark.unit
def test_exact_graph_does_not_retry_programming_errors():
    analytics = _BudgetSplittingAnalytics(error_code=62)

    with pytest.raises(ServerException):
        read_exact_system_graph(
            analytics=analytics,
            project_id="11111111-1111-4111-8111-111111111111",
            filters=_exact_multi_filters(
                datetime(2026, 8, 1, 0, 0),
                datetime(2026, 8, 1, 4, 0),
            ),
            interval="hour",
            metric_id="traffic",
            observe_type="trace",
        )

    assert len(analytics.partition_calls) == 1


@pytest.mark.unit
def test_partitioned_exact_read_reuses_one_version_ceiling():
    analytics = _ConcurrentArrivalAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 4, 15)

    result = read_exact_system_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=[
            _time_filter(start, end),
            {
                "column_id": "model",
                "filter_config": {
                    "filter_type": "text",
                    "filter_op": "equals",
                    "filter_value": "gpt-4",
                    "col_type": "SYSTEM_METRIC",
                },
            },
        ],
        interval="day",
        metric_id="traffic",
        observe_type="trace",
    )

    assert len(analytics.partition_calls) > 1
    assert {
        call_settings["additional_table_filters"]["spans"]
        for _query, _params, call_settings in analytics.partition_calls
    } == {"_version < 900"}
    assert all(
        "trace_id IN" in query
        and params["snapshot_start_date"] == start
        and params["snapshot_end_date"] == end
        for query, params, _settings in analytics.partition_calls
    )
    assert (
        len(
            {
                (params["start_date"], params["end_date"])
                for _query, params, _settings in analytics.partition_calls
            }
        )
        > 1
    )
    assert result["query_complete"] is True
    assert result["query_status"] == "complete"
    assert result["query_sampled"] is False


class _ExactEntityAnalytics:
    def __init__(self):
        self.main_calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "toUnixTimestamp64Nano(now64" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        if "max(toUnixTimestamp64Micro(version))" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 901}],
                columns=["version_ceiling"],
            )
        self.main_calls.append((query, dict(params), dict(settings)))
        return SimpleNamespace(
            data=[],
            columns=["time_bucket", "value", "primary_traffic"],
        )


class _EntityBudgetSplittingAnalytics:
    def __init__(self, *, always_fail=False):
        self.always_fail = always_fail
        self.main_calls = []

    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "toUnixTimestamp64Nano(now64" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        if "max(toUnixTimestamp64Micro(version))" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 901}],
                columns=["version_ceiling"],
            )
        if "max(_peerdb_version)" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 701}],
                columns=["version_ceiling"],
            )
        self.main_calls.append((query, dict(params), timeout_ms, dict(settings)))
        width = (params["end_date"] - params["start_date"]).total_seconds()
        if self.always_fail or width > 3600:
            raise ServerException("private budget detail", code=159)
        if "uniqExact(end_user_id) AS active_users" in query:
            row = {
                "time_bucket": params["start_date"],
                "avg_latency": 1,
                "total_tokens": 1,
                "avg_cost": 1,
                "traffic_count": 1,
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "error_rate": 0,
                "active_users": 1,
                "total_cost_sum": 1,
                "avg_cost_per_user": 1,
                "avg_traces_per_user": 1,
                "total_tokens_sum": 1,
            }
        else:
            row = {
                "time_bucket": params["start_date"],
                "value": 1,
                "primary_traffic": 1,
            }
        return SimpleNamespace(
            data=[row],
            columns=list(row),
        )


def _assert_entity_output_partitions(calls, start, end):
    """Assert chronological, gap-free partitions with one frozen entity window."""

    assert len(calls) > 1
    ranges = [
        (params["start_date"], params["end_date"])
        for _query, params, _settings in calls
    ]
    assert ranges[0][0] == start
    assert ranges[-1][1] == end
    assert all(
        left[1] == right[0] for left, right in zip(ranges, ranges[1:], strict=False)
    )
    assert all(left < right for left, right in ranges)
    assert all(
        params["snapshot_start_date"] == start and params["snapshot_end_date"] == end
        for _query, params, _settings in calls
    )


@pytest.mark.unit
@pytest.mark.parametrize("aggregation_context", ["session", "user"])
def test_entity_system_graph_adaptively_splits_without_bisecting_entities(
    aggregation_context,
):
    analytics = _EntityBudgetSplittingAnalytics()
    start = datetime(2026, 8, 1, 0)
    end = datetime(2026, 8, 1, 4)
    common = {
        "analytics": analytics,
        "project_id": "22222222-2222-4222-8222-222222222222",
        "filters": [_time_filter(start, end)],
        "interval": "hour",
    }

    if aggregation_context == "session":
        result = read_exact_session_system_graph(
            **common,
            metric_id="session_count",
        )
        expected_shape = "WITH candidate_sessions AS"
    else:
        result = read_exact_user_system_graph(
            **common,
            metric_id="active_users",
        )
        expected_shape = "candidate_trace_ids AS"

    # 4h and both 2h probes fail; the four one-hour entity-safe leaves pass.
    assert len(analytics.main_calls) == 7
    successful = [
        call
        for call in analytics.main_calls
        if (call[1]["end_date"] - call[1]["start_date"]).total_seconds() == 3600
    ]
    assert len(successful) == 4
    assert [call[1]["start_date"].hour for call in successful] == [0, 1, 2, 3]
    assert all(
        call_params["snapshot_start_date"] == start
        and call_params["snapshot_end_date"] == end
        and expected_shape in query
        and settings["additional_table_filters"]["spans"] == "_version < 900"
        for query, call_params, _timeout, settings in analytics.main_calls
    )
    assert {call[2] for call in analytics.main_calls} == {30_000, 300_000}
    assert sum(point["value"] for point in result["data"]) == 4
    assert sum(point["primary_traffic"] for point in result["data"]) == 4
    assert sum(point["value"] == 1 for point in result["data"]) == 4
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize("aggregation_context", ["session", "user"])
def test_entity_system_graph_indivisible_budget_failure_is_fail_closed(
    aggregation_context,
):
    analytics = _EntityBudgetSplittingAnalytics(always_fail=True)
    start = datetime(2026, 8, 1, 0)
    end = datetime(2026, 8, 1, 1)
    common = {
        "analytics": analytics,
        "project_id": "22222222-2222-4222-8222-222222222222",
        "filters": [_time_filter(start, end)],
        "interval": "hour",
    }

    with pytest.raises(ServerException):
        if aggregation_context == "session":
            read_exact_session_system_graph(**common, metric_id="session_count")
        else:
            read_exact_user_system_graph(**common, metric_id="active_users")

    assert len(analytics.main_calls) == 1
    assert analytics.main_calls[0][2] == 300_000


@pytest.mark.unit
def test_exact_session_graph_combines_native_session_and_aggregate_filters():
    analytics = _ExactEntityAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    session_id = "11111111-1111-4111-8111-111111111111"
    filters = [
        _time_filter(start, end),
        {
            "column_id": "status",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "equals",
                "filter_value": "ERROR",
            },
        },
        {
            "column_id": "session_id",
            "filter_config": {
                "filter_type": "text",
                "filter_op": "in",
                "filter_value": [session_id],
            },
        },
        {
            "column_id": "duration",
            "filter_config": {
                "filter_type": "number",
                "filter_op": "greater_than_or_equal",
                "filter_value": 5,
            },
        },
        {
            "column_id": "total_cost",
            "filter_config": {
                "filter_type": "number",
                "filter_op": "less_than",
                "filter_value": 10,
            },
        },
        {
            "column_id": "first_message",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "contains",
                "filter_value": "hello",
            },
        },
        {
            "column_id": "last_message",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "is_not_null",
                "filter_value": None,
            },
        },
    ]

    result = read_exact_session_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="day",
        metric_id="session_count",
    )

    # A >31-bucket range is safely partitioned: each query discovers sessions
    # anchored in its output range and hydrates them over the full snapshot.
    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, _settings = analytics.main_calls[0]
    assert params["start_date"] == start
    assert params["end_date"] < end
    assert params["exact_session_id_1"] == (session_id,)
    assert params["session_having_1"] == 5
    assert params["session_having_2"] == 10
    assert params["session_having_3"] == "%hello%"
    assert "session_duration >= %(session_having_1)s" in query
    assert "session_start >= %(start_date)s" in query
    assert "WITH candidate_sessions AS" in query
    assert "session_total_cost < %(session_having_2)s" in query
    assert "argMin(rs.input, rs.start_time) AS first_message" in query
    assert "argMax(rs.input, rs.start_time) AS last_message" in query
    assert "first_message ILIKE %(session_having_3)s" in query
    assert "(last_message IS NOT NULL AND last_message != '')" in query
    assert "span_attr_str['first_message']" not in query
    assert "span_attr_str['last_message']" not in query
    assert "rs.trace_session_id, ts_remap.survivor_id) IN" in query
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_exact_session_system_graph_supports_array_map_and_legacy_json_filters():
    analytics = _ExactEntityAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)

    result = read_exact_session_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=_exact_structured_filters(start, end),
        interval="day",
        metric_id="session_count",
    )

    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, settings = analytics.main_calls[0]
    assert query.count("SELECT DISTINCT trace_id") >= 3
    assert "JSONExtractArrayRaw(attributes_extra" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert params["snapshot_start_date"] == start
    assert params["snapshot_end_date"] == end
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_exact_session_graph_freezes_combined_filter_relations(monkeypatch):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    result = read_exact_session_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=_combined_relation_filters(start, end),
        interval="day",
        metric_id="session_count",
    )

    assert analytics.capture_calls == [
        "spans",
        "trace_session_id_remap",
        "tracer_eval_logger",
        "model_hub_score",
        "end_users",
    ]
    _assert_entity_output_partitions(analytics.main_calls, start, end)
    settings = analytics.main_calls[0][2]["additional_table_filters"]
    assert all(
        call_settings["additional_table_filters"] == settings
        for _query, _params, call_settings in analytics.main_calls
    )
    assert settings == {
        "spans": "_version < 900",
        "trace_session_id_remap": "toUnixTimestamp64Micro(version) < 901",
        "tracer_eval_logger": "_peerdb_version < 701",
        "model_hub_score": "_peerdb_version < 801",
        "end_users": "toUnixTimestamp64Micro(version) < 901",
    }
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("column_id", "filter_op", "filter_value", "expected_sql", "expected_param"),
    [
        (
            "first_message",
            "equals",
            "hello",
            "first_message = %(session_having_1)s",
            "hello",
        ),
        (
            "first_message",
            "not_equals",
            "hello",
            "first_message != %(session_having_1)s",
            "hello",
        ),
        (
            "first_message",
            "contains",
            "hello",
            "first_message ILIKE %(session_having_1)s",
            "%hello%",
        ),
        (
            "last_message",
            "not_contains",
            "bye",
            "last_message NOT ILIKE %(session_having_1)s",
            "%bye%",
        ),
        (
            "first_message",
            "starts_with",
            "hello",
            "first_message ILIKE %(session_having_1)s",
            "hello%",
        ),
        (
            "last_message",
            "ends_with",
            "bye",
            "last_message ILIKE %(session_having_1)s",
            "%bye",
        ),
        (
            "first_message",
            "is_null",
            None,
            "(first_message IS NULL OR first_message = '')",
            None,
        ),
        (
            "last_message",
            "is_not_null",
            None,
            "(last_message IS NOT NULL AND last_message != '')",
            None,
        ),
        # Keep the same fail-closed behavior as SessionListQueryBuilderV2 for
        # message operators it does not support.
        ("first_message", "in", ["hello", "bye"], "0 = 1", None),
    ],
)
def test_exact_session_message_filters_match_session_list_having_semantics(
    column_id,
    filter_op,
    filter_value,
    expected_sql,
    expected_param,
):
    analytics = _ExactEntityAnalytics()
    filters = [
        _time_filter(datetime(2026, 1, 1), datetime(2026, 1, 2)),
        {
            "column_id": column_id,
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": filter_op,
                "filter_value": filter_value,
            },
        },
    ]

    read_exact_session_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="hour",
        metric_id="session_count",
    )

    query, params, _settings = analytics.main_calls[0]
    assert expected_sql in query
    assert "argMin(rs.input, rs.start_time) AS first_message" in query
    assert "argMax(rs.input, rs.start_time) AS last_message" in query
    assert "span_attr_str['first_message']" not in query
    assert "span_attr_str['last_message']" not in query
    if expected_param is None:
        assert "session_having_1" not in params
    else:
        assert params["session_having_1"] == expected_param


class _SessionContextAnalytics(_ExactEntityAnalytics):
    def execute_ch_query(self, query, params, *, timeout_ms, settings):
        if "toUnixTimestamp64Nano(now64" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 900}],
                columns=["version_ceiling"],
            )
        if "max(toUnixTimestamp64Micro(version))" in query:
            return SimpleNamespace(
                data=[{"version_ceiling": 901}],
                columns=["version_ceiling"],
            )
        self.main_calls.append((query, dict(params), dict(settings)))
        if "SELECT DISTINCT trace_id" in query and params.get("candidate_trace_ids"):
            return SimpleNamespace(
                data=[
                    {"trace_id": trace_id} for trace_id in params["candidate_trace_ids"]
                ],
                columns=["trace_id"],
            )
        return SimpleNamespace(
            data=[],
            columns=["time_bucket", "value", "primary_traffic"],
        )


def _assert_session_membership_sql(query, params, start, end):
    assert "SELECT DISTINCT toString(candidate_member.trace_id) AS trace_id" in query
    assert "FROM spans AS candidate_member FINAL" in query
    assert "FROM (" in query and "AS selected_sessions" in query
    assert "argMin(rs.input, rs.start_time) AS first_message" in query
    assert "session_duration >= %(session_having_1)s" in query
    assert "first_message ILIKE %(session_having_2)s" in query
    assert "rs.trace_session_id, ts_remap.survivor_id) IN" in query
    assert "span_attr_str['first_message']" not in query
    assert params["snapshot_start_date"] == start
    assert params["snapshot_end_date"] == end
    assert params["session_having_1"] == 5
    assert params["session_having_2"] == "%hello%"


@pytest.mark.unit
def test_session_eval_graph_partitions_candidates_and_hydrates_full_sessions(
    monkeypatch,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _SessionContextAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    eval_config_id = "33333333-3333-4333-8333-333333333333"
    config = SimpleNamespace(
        name="quality",
        eval_template=SimpleNamespace(config={"output": "SCORE"}, choices=[]),
    )
    config_qs = SimpleNamespace(get=lambda **_kwargs: config)
    monkeypatch.setattr(
        exact_module.CustomEvalConfig.objects,
        "select_related",
        lambda *_args: config_qs,
    )
    # Avoid an unrelated legacy-CDC ceiling query in this SQL contract test.
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger_v2", "is_deleted = 0"),
    )

    filters = [
        *_combined_session_filters(start, end),
        *_exact_structured_filters(start, end)[2:],
    ]
    result = read_exact_eval_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="day",
        req_data_config={"id": eval_config_id, "output_type": "SCORE"},
        observe_type="trace",
        aggregation_context="session",
    )

    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, settings = analytics.main_calls[0]
    _assert_session_membership_sql(query, params, start, end)
    assert "JSONExtractArrayRaw(attributes_extra" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert params["start_date"] == start
    assert params["end_date"] < end
    assert "candidate_eval.created_at >= %(start_date)s" in query
    assert "candidate_eval.created_at < %(end_date)s" in query
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    assert (
        settings["additional_table_filters"]["tracer_eval_logger_v2"]
        == "_version < 900"
    )
    assert (
        settings["additional_table_filters"]["trace_session_id_remap"]
        == "toUnixTimestamp64Micro(version) < 901"
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize("aggregation_context", ["session", "user"])
def test_entity_eval_graph_adaptively_splits_candidate_eval_partitions(
    monkeypatch,
    aggregation_context,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _EntityBudgetSplittingAnalytics()
    start = datetime(2026, 8, 1, 0)
    end = datetime(2026, 8, 1, 4)
    eval_config_id = "33333333-3333-4333-8333-333333333333"
    config = SimpleNamespace(
        name="quality",
        eval_template=SimpleNamespace(config={"output": "SCORE"}, choices=[]),
    )
    config_qs = SimpleNamespace(get=lambda **_kwargs: config)
    monkeypatch.setattr(
        exact_module.CustomEvalConfig.objects,
        "select_related",
        lambda *_args: config_qs,
    )

    result = read_exact_eval_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=[_time_filter(start, end)],
        interval="hour",
        req_data_config={"id": eval_config_id, "output_type": "SCORE"},
        observe_type="trace",
        aggregation_context=aggregation_context,
    )

    assert len(analytics.main_calls) == 7
    assert all(
        params["snapshot_start_date"] == start
        and params["snapshot_end_date"] == end
        and "candidate_eval.created_at >= %(start_date)s" in query
        and "candidate_eval.created_at < %(end_date)s" in query
        and "SELECT DISTINCT toString(candidate_member.trace_id) AS trace_id" in query
        for query, params, _timeout, _settings in analytics.main_calls
    )
    successful = [
        params
        for _query, params, _timeout, _settings in analytics.main_calls
        if (params["end_date"] - params["start_date"]).total_seconds() == 3600
    ]
    assert [params["start_date"].hour for params in successful] == [0, 1, 2, 3]
    assert sum(point["value"] for point in result["data"]) == 4
    assert sum(point["primary_traffic"] for point in result["data"]) == 4
    assert sum(point["value"] == 1 for point in result["data"]) == 4
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_exact_eval_graph_supports_combined_structured_filters(
    monkeypatch,
    observe_type,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _SessionContextAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 10)
    eval_config_id = "33333333-3333-4333-8333-333333333333"
    config = SimpleNamespace(
        name="quality",
        eval_template=SimpleNamespace(config={"output": "SCORE"}, choices=[]),
    )
    config_qs = SimpleNamespace(get=lambda **_kwargs: config)
    monkeypatch.setattr(
        exact_module.CustomEvalConfig.objects,
        "select_related",
        lambda *_args: config_qs,
    )
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger_v2", "is_deleted = 0"),
    )

    result = read_exact_eval_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=_exact_structured_filters(start, end),
        interval="day",
        req_data_config={"id": eval_config_id, "output_type": "SCORE"},
        observe_type=observe_type,
    )

    assert len(analytics.main_calls) == 1
    query, params, settings = analytics.main_calls[0]
    assert "JSONExtractArrayRaw(attributes_extra" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert params["snapshot_start_date"] == start
    assert params["snapshot_end_date"] == end
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    assert (
        settings["additional_table_filters"]["tracer_eval_logger_v2"]
        == "_version < 900"
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_exact_eval_reader_reuses_combined_relation_ceilings(
    monkeypatch,
    observe_type,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 4, 15)
    eval_config_id = "33333333-3333-4333-8333-333333333333"
    config = SimpleNamespace(
        name="quality",
        eval_template=SimpleNamespace(config={"output": "SCORE"}, choices=[]),
    )
    config_qs = SimpleNamespace(get=lambda **_kwargs: config)
    monkeypatch.setattr(
        exact_module.CustomEvalConfig.objects,
        "select_related",
        lambda *_args: config_qs,
    )
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    result = read_exact_eval_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=_combined_relation_filters(start, end),
        interval="day",
        req_data_config={"id": eval_config_id, "output_type": "SCORE"},
        observe_type=observe_type,
    )

    assert analytics.capture_calls == [
        "spans",
        "tracer_eval_logger",
        "model_hub_score",
        "end_users",
    ]
    assert len(analytics.main_calls) > 1
    expected_filters = {
        "spans": "_version < 900",
        "tracer_eval_logger": "_peerdb_version < 701",
        "model_hub_score": "_peerdb_version < 801",
        "end_users": "toUnixTimestamp64Micro(version) < 901",
    }
    assert all(
        call_settings["additional_table_filters"] == expected_filters
        for _query, _params, call_settings in analytics.main_calls
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


class _ScoreRows:
    def __init__(self, rows):
        self.rows = rows

    def order_by(self, *_args):
        return self

    def values(self, *_args):
        return self

    def iterator(self, *, chunk_size):
        assert chunk_size > 0
        return iter(self.rows)


class _ScoreManager:
    def __init__(self, row):
        self.row = row

    def filter(self, **kwargs):
        created_at = self.row["created_at"]
        rows = (
            [self.row]
            if kwargs["created_at__gte"] <= created_at < kwargs["created_at__lt"]
            else []
        )
        return _ScoreRows(rows)


class _ScoreListManager:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter(self, **kwargs):
        return _ScoreRows(
            [
                row
                for row in self.rows
                if kwargs["created_at__gte"]
                <= row["created_at"]
                < kwargs["created_at__lt"]
            ]
        )


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_annotation_membership_batches_reuse_combined_relation_ceilings(
    monkeypatch,
    observe_type,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 3)
    scores = [
        {
            "trace_id": "44444444-4444-4444-8444-444444444441",
            "observation_span_id": "span-1" if observe_type == "span" else None,
            "created_at": datetime(2026, 1, 2, 1),
            "value": {"rating": 4},
        },
        {
            "trace_id": "44444444-4444-4444-8444-444444444442",
            "observation_span_id": "span-2" if observe_type == "span" else None,
            "created_at": datetime(2026, 1, 2, 2),
            "value": {"rating": 5},
        },
    ]
    label = SimpleNamespace(name="quality", type="numeric")
    monkeypatch.setattr(
        exact_module,
        "Score",
        SimpleNamespace(no_workspace_objects=_ScoreListManager(scores)),
    )
    monkeypatch.setattr(
        exact_module,
        "get_annotation_labels_for_project",
        lambda _project_id: SimpleNamespace(get=lambda **_kwargs: label),
    )
    monkeypatch.setattr(exact_module.transaction, "atomic", nullcontext)
    monkeypatch.setattr(exact_module, "connection", SimpleNamespace(vendor="sqlite"))
    monkeypatch.setattr(exact_module, "EXACT_GRAPH_MEMBERSHIP_BATCH_SIZE", 1)
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    result = read_exact_annotation_graph(
        analytics=analytics,
        project_id="11111111-1111-4111-8111-111111111111",
        filters=_combined_relation_filters(start, end),
        interval="day",
        req_data_config={
            "id": "55555555-5555-4555-8555-555555555555",
            "output_type": "float",
        },
        observe_type=observe_type,
    )

    assert analytics.capture_calls == [
        "spans",
        "tracer_eval_logger",
        "model_hub_score",
        "end_users",
    ]
    assert len(analytics.main_calls) == 2
    expected_filters = {
        "spans": "_version < 900",
        "tracer_eval_logger": "_peerdb_version < 701",
        "model_hub_score": "_peerdb_version < 801",
        "end_users": "toUnixTimestamp64Micro(version) < 901",
    }
    assert all(
        call_settings["additional_table_filters"] == expected_filters
        for _query, _params, call_settings in analytics.main_calls
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_session_annotation_graph_uses_full_window_session_membership(monkeypatch):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _SessionContextAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    trace_id = "44444444-4444-4444-8444-444444444444"
    score = {
        "trace_id": trace_id,
        "observation_span_id": None,
        "created_at": datetime(2026, 2, 10),
        "value": {"rating": 4},
    }
    label = SimpleNamespace(name="quality", type="numeric")
    monkeypatch.setattr(
        exact_module,
        "Score",
        SimpleNamespace(no_workspace_objects=_ScoreManager(score)),
    )
    monkeypatch.setattr(
        exact_module,
        "get_annotation_labels_for_project",
        lambda _project_id: SimpleNamespace(get=lambda **_kwargs: label),
    )
    monkeypatch.setattr(exact_module.transaction, "atomic", nullcontext)
    monkeypatch.setattr(
        exact_module,
        "connection",
        SimpleNamespace(vendor="sqlite"),
    )

    filters = [
        *_combined_session_filters(start, end),
        *_exact_structured_filters(start, end)[2:],
    ]
    result = read_exact_annotation_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="day",
        req_data_config={
            "id": "55555555-5555-4555-8555-555555555555",
            "output_type": "float",
        },
        observe_type="trace",
        aggregation_context="session",
    )

    membership_calls = [
        call for call in analytics.main_calls if "SELECT DISTINCT trace_id" in call[0]
    ]
    assert len(membership_calls) == 1
    query, params, settings = membership_calls[0]
    _assert_session_membership_sql(query, params, start, end)
    assert "JSONExtractArrayRaw(attributes_extra" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert params["candidate_trace_ids"] == (trace_id,)
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    assert (
        settings["additional_table_filters"]["trace_session_id_remap"]
        == "toUnixTimestamp64Micro(version) < 901"
    )
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize("observe_type", ["trace", "span"])
def test_exact_annotation_graph_supports_combined_structured_filters(
    monkeypatch,
    observe_type,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _SessionContextAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 10)
    trace_id = "44444444-4444-4444-8444-444444444444"
    span_id = "span-1" if observe_type == "span" else None
    score = {
        "trace_id": trace_id,
        "observation_span_id": span_id,
        "created_at": datetime(2026, 1, 5),
        "value": {"rating": 4},
    }
    label = SimpleNamespace(name="quality", type="numeric")
    monkeypatch.setattr(
        exact_module,
        "Score",
        SimpleNamespace(no_workspace_objects=_ScoreManager(score)),
    )
    monkeypatch.setattr(
        exact_module,
        "get_annotation_labels_for_project",
        lambda _project_id: SimpleNamespace(get=lambda **_kwargs: label),
    )
    monkeypatch.setattr(exact_module.transaction, "atomic", nullcontext)
    monkeypatch.setattr(exact_module, "connection", SimpleNamespace(vendor="sqlite"))

    result = read_exact_annotation_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=_exact_structured_filters(start, end),
        interval="day",
        req_data_config={
            "id": "55555555-5555-4555-8555-555555555555",
            "output_type": "float",
        },
        observe_type=observe_type,
    )

    membership_queries = [
        query
        for query, _params, _settings in analytics.main_calls
        if "JSONExtractArrayRaw(attributes_extra" in query
    ]
    assert membership_queries
    assert "JSONExtractRaw(attributes_extra" in membership_queries[0]
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
@pytest.mark.parametrize(
    ("metric_type", "namespace"),
    [
        ("eval", "observe-eval-graph"),
        ("annotation", "observe-annotation-graph"),
    ],
)
def test_session_eval_annotation_cache_identity_keeps_session_context(
    monkeypatch,
    metric_type,
    namespace,
):
    from tracer.services.clickhouse import graph_dispatch

    captured = {}

    def read_or_schedule(actual_namespace, identity, **_kwargs):
        captured.update(namespace=actual_namespace, identity=identity)
        return {"query_status": "pending"}

    monkeypatch.setattr(
        graph_dispatch,
        "read_or_schedule_exact_snapshot",
        read_or_schedule,
    )
    common = {
        "analytics": object(),
        "project_id": "22222222-2222-4222-8222-222222222222",
        "filters": _combined_session_filters(
            datetime(2026, 1, 1), datetime(2026, 3, 15)
        ),
        "interval": "day",
        "req_data_config": {"id": "55555555-5555-4555-8555-555555555555"},
        "observe_type": "trace",
        "aggregation_context": "session",
    }
    if metric_type == "eval":
        graph_dispatch.fetch_eval_graph_ch(**common)
    else:
        graph_dispatch.fetch_annotation_graph_ch(**common)

    assert captured["namespace"] == namespace
    assert captured["identity"]["aggregation_context"] == "session"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("metric_type", "namespace"),
    [
        ("eval", "observe-eval-graph"),
        ("annotation", "observe-annotation-graph"),
    ],
)
def test_user_eval_annotation_cache_identity_keeps_user_context(
    monkeypatch,
    metric_type,
    namespace,
):
    from tracer.services.clickhouse import graph_dispatch

    captured = {}

    def read_or_schedule(actual_namespace, identity, **_kwargs):
        captured.update(namespace=actual_namespace, identity=identity)
        return {"query_status": "pending"}

    monkeypatch.setattr(
        graph_dispatch,
        "read_or_schedule_exact_snapshot",
        read_or_schedule,
    )
    common = {
        "analytics": object(),
        "project_id": "22222222-2222-4222-8222-222222222222",
        "filters": [_time_filter(datetime(2026, 1, 1), datetime(2026, 3, 15))],
        "interval": "day",
        "req_data_config": {"id": "55555555-5555-4555-8555-555555555555"},
        "observe_type": "trace",
        "aggregation_context": "user",
    }
    if metric_type == "eval":
        graph_dispatch.fetch_eval_graph_ch(**common)
    else:
        graph_dispatch.fetch_annotation_graph_ch(**common)

    assert captured["namespace"] == namespace
    assert captured["identity"]["aggregation_context"] == "user"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("namespace", "reader_name"),
    [
        ("observe-eval-graph", "read_exact_eval_graph"),
        ("observe-annotation-graph", "read_exact_annotation_graph"),
    ],
)
def test_exact_worker_forwards_session_context_to_eval_annotation_reader(
    monkeypatch,
    namespace,
    reader_name,
):
    from tracer.services.clickhouse import exact_graph_reads
    from tracer.services.clickhouse.v2 import query_service
    from tracer.tasks import exact_aggregation

    captured = {}

    def reader(**kwargs):
        captured.update(kwargs)
        return {
            "metric_name": "metric",
            "data": [],
            "query_complete": True,
            "query_status": "complete",
            "query_sampled": False,
        }

    monkeypatch.setattr(exact_graph_reads, reader_name, reader)
    monkeypatch.setattr(query_service, "V2AnalyticsQueryService", lambda: object())
    exact_aggregation._observe_payload(
        namespace,
        {
            "project_id": "22222222-2222-4222-8222-222222222222",
            "filters": _combined_session_filters(
                datetime(2026, 1, 1), datetime(2026, 3, 15)
            ),
            "interval": "day",
            "req_data_config": {"id": "55555555-5555-4555-8555-555555555555"},
            "observe_type": "trace",
            "aggregation_context": "session",
        },
    )

    assert captured["aggregation_context"] == "session"


@pytest.mark.unit
def test_exact_user_graph_partitions_on_trace_anchor_and_hydrates_full_entities():
    analytics = _ExactEntityAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)

    result = read_exact_user_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=[_time_filter(start, end)],
        interval="day",
        metric_id="active_users",
    )

    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, settings = analytics.main_calls[0]
    assert params["start_date"] == start
    assert params["end_date"] < end
    assert "candidate_trace_ids AS" in query
    assert "HAVING min(start_time) >= %(start_date)s" in query
    assert "start_time >= %(snapshot_start_date)s" in query
    assert "SELECT toString(trace_id) FROM candidate_trace_ids" in query
    assert "GROUP BY end_user_id, trace_id" in query
    assert "FROM user_rows" in query
    assert settings["additional_table_filters"]["spans"] == "_version < 900"
    assert set(settings["additional_table_filters"]) >= {
        "spans",
        "end_user_id_remap",
        "trace_session_id_remap",
        "end_users",
    }
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_exact_user_graph_applies_entity_filters_after_full_window_aggregation():
    analytics = _ExactEntityAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    filters = [
        _time_filter(start, end),
        {
            "column_id": "num_traces",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "number",
                "filter_op": "greater_than_or_equal",
                "filter_value": 10,
            },
        },
        {
            "column_id": "num_sessions",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "number",
                "filter_op": "between",
                "filter_value": [2, 20],
            },
        },
        {
            "column_id": "user_id",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "text",
                "filter_op": "contains",
                "filter_value": "customer",
            },
        },
        {
            "column_id": "payload",
            "filter_config": {
                "col_type": "SPAN_ATTRIBUTE",
                "filter_type": "map",
                "filter_op": "contains",
                "filter_value": {"kind": "vip"},
            },
        },
    ]

    result = read_exact_user_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="day",
        metric_id="active_users",
    )

    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, _settings = analytics.main_calls[0]
    assert "WHERE num_traces >= %(user_filter_1)s" in query
    assert "num_sessions BETWEEN %(user_filter_2_start)s" in query
    assert "positionCaseInsensitive(toString(user_id)" in query
    assert "JSONExtractRaw(attributes_extra" in query
    assert "span_attr_num['num_traces']" not in query
    assert "span_attr_num['num_sessions']" not in query
    assert "groupUniqArray(trace_id) AS user_trace_ids" not in query
    assert params["user_filter_1"] == 10
    assert params["user_filter_2_start"] == 2
    assert params["user_filter_2_end"] == 20
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_exact_user_graph_freezes_combined_relations_without_duplicate_capture(
    monkeypatch,
):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _RelationSnapshotAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger", "deleted = 0"),
    )

    result = read_exact_user_system_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=_combined_relation_filters(start, end),
        interval="day",
        metric_id="active_users",
    )

    assert analytics.capture_calls == [
        "spans",
        "end_user_id_remap",
        "trace_session_id_remap",
        "end_users",
        "tracer_eval_logger",
        "model_hub_score",
    ]
    _assert_entity_output_partitions(analytics.main_calls, start, end)
    settings = analytics.main_calls[0][2]["additional_table_filters"]
    assert all(
        call_settings["additional_table_filters"] == settings
        for _query, _params, call_settings in analytics.main_calls
    )
    assert settings == {
        "spans": "_version < 900",
        "end_user_id_remap": "toUnixTimestamp64Micro(version) < 901",
        "trace_session_id_remap": "toUnixTimestamp64Micro(version) < 901",
        "end_users": "toUnixTimestamp64Micro(version) < 901",
        "tracer_eval_logger": "_peerdb_version < 701",
        "model_hub_score": "_peerdb_version < 801",
    }
    assert result["query_complete"] is True
    assert result["query_sampled"] is False


@pytest.mark.unit
def test_user_eval_filter_is_full_window_membership_not_raw_span_attribute(monkeypatch):
    from tracer.services.clickhouse import exact_graph_reads as exact_module

    analytics = _SessionContextAnalytics()
    start = datetime(2026, 1, 1)
    end = datetime(2026, 3, 15)
    eval_config_id = "33333333-3333-4333-8333-333333333333"
    config = SimpleNamespace(
        name="quality",
        eval_template=SimpleNamespace(config={"output": "SCORE"}, choices=[]),
    )
    config_qs = SimpleNamespace(get=lambda **_kwargs: config)
    monkeypatch.setattr(
        exact_module.CustomEvalConfig.objects,
        "select_related",
        lambda *_args: config_qs,
    )
    monkeypatch.setattr(
        exact_module,
        "eval_logger_source",
        lambda *_args, **_kwargs: ("tracer_eval_logger_v2", "eval_scan.is_deleted = 0"),
    )
    filters = [
        _time_filter(start, end),
        {
            "column_id": "eval_score",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "number",
                "filter_op": "greater_than_or_equal",
                "filter_value": 80,
            },
        },
        {
            "column_id": "total_cost",
            "filter_config": {
                "col_type": "SYSTEM_METRIC",
                "filter_type": "number",
                "filter_op": "less_than",
                "filter_value": 100,
            },
        },
    ]

    result = read_exact_eval_graph(
        analytics=analytics,
        project_id="22222222-2222-4222-8222-222222222222",
        filters=filters,
        interval="day",
        req_data_config={"id": eval_config_id, "output_type": "SCORE"},
        observe_type="trace",
        aggregation_context="user",
    )

    _assert_entity_output_partitions(analytics.main_calls, start, end)
    query, params, settings = analytics.main_calls[0]
    assert "SELECT DISTINCT toString(candidate_member.trace_id) AS trace_id" in query
    assert "AS selected_users" in query
    assert "user_eval_metrics AS" in query
    assert "WHERE bool_eval_pass_rate >= %(user_filter_1)s" in query
    assert "total_cost < %(user_filter_2)s" in query
    assert "span_attr_num['eval_score']" not in query
    assert params["snapshot_start_date"] == start
    assert params["snapshot_end_date"] == end
    assert set(settings["additional_table_filters"]) >= {
        "spans",
        "tracer_eval_logger_v2",
        "end_user_id_remap",
        "trace_session_id_remap",
        "end_users",
    }
    assert result["query_complete"] is True
    assert result["query_sampled"] is False
