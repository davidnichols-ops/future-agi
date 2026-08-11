from tracer.services.clickhouse.query_service import AnalyticsQueryService
from tracer.services.clickhouse.v2.query_settings import (
    ch_query_settings,
    current_settings,
)


class _Client:
    def __init__(self):
        self.calls = []
        self.server_enforced_readonly = False

    def execute_read(self, query, params, *, timeout_ms, settings):
        self.calls.append((query, params, timeout_ms, settings))
        return [(1,)], [("value", "UInt8")], 1.0


def test_application_query_service_normalizes_every_read_policy():
    client = _Client()
    service = AnalyticsQueryService()
    service._ch_client = client

    result = service.execute_ch_query(
        "SELECT 1 AS value",
        {},
        timeout_ms=120_000,
        settings={
            "max_rows_to_read": 1,
            "max_memory_usage": 2 * 1024 * 1024 * 1024,
            "max_bytes_to_read": 512 * 1024 * 1024,
            "max_threads": 2,
        },
    )

    assert result.data == [{"value": 1}]
    _, _, timeout_ms, settings = client.calls[0]
    assert timeout_ms == 30_000
    assert "max_rows_to_read" not in settings
    assert settings["max_memory_usage"] == 36 * 1024 * 1024 * 1024
    assert settings["max_bytes_to_read"] == 512 * 1024 * 1024
    assert settings["max_threads"] == 2


def test_application_query_service_supplies_memory_policy_when_omitted():
    client = _Client()
    service = AnalyticsQueryService()
    service._ch_client = client

    service.execute_ch_query("SELECT 1", {})

    _, _, timeout_ms, settings = client.calls[0]
    assert timeout_ms == 10_000
    assert settings == {"max_memory_usage": 36 * 1024 * 1024 * 1024}


def test_span_reader_defaults_apply_the_application_read_policy():
    assert current_settings() == {
        "max_memory_usage": 36 * 1024 * 1024 * 1024,
        "max_execution_time": 30,
    }


def test_span_reader_context_strips_rows_and_clamps_timeout():
    with ch_query_settings(
        max_rows_to_read=1,
        max_memory_usage=1_000_000,
        max_execution_time=120,
        max_threads=1,
    ):
        settings = current_settings()

    assert settings == {
        "max_memory_usage": 1_000_000,
        "max_execution_time": 30,
        "max_threads": 1,
    }

    with ch_query_settings(max_execution_time=0):
        assert current_settings()["max_execution_time"] == 0.001


def test_application_query_service_preserves_server_locked_policy():
    client = _Client()
    client.server_enforced_readonly = True
    service = AnalyticsQueryService()
    service._ch_client = client
    requested_settings = {
        "max_rows_to_read": 1,
        "max_memory_usage": 2 * 1024 * 1024 * 1024,
    }

    service.execute_ch_query(
        "SELECT 1",
        {},
        timeout_ms=120_000,
        settings=requested_settings,
    )

    _, _, timeout_ms, settings = client.calls[0]
    assert timeout_ms == 120_000
    assert settings == requested_settings


def test_application_query_service_does_not_revive_exhausted_timeout():
    client = _Client()
    service = AnalyticsQueryService()
    service._ch_client = client

    service.execute_ch_query("SELECT 1", {}, timeout_ms=0)

    assert client.calls[0][2] == 1
