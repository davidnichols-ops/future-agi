"""Contracts for server-locked read-only ClickHouse connections."""

from unittest.mock import Mock

import pytest

from tracer.services.clickhouse import client as client_module
from tracer.services.clickhouse.client import ClickHouseClient
from tracer.services.clickhouse.server_readonly import (
    ServerEnforcedReadOnlyNativeClient,
    _NativeBlockStream,
    without_query_settings,
)
from tracer.services.clickhouse.v2.span_reader import CHSpanReader


def _client(*, server_enforced_readonly: bool) -> ClickHouseClient:
    return ClickHouseClient(
        host="clickhouse.invalid",
        port=9000,
        user="readonly",
        password="",
        database="futureagi",
        server_enforced_readonly=server_enforced_readonly,
    )


def test_server_locked_client_sends_no_connection_settings(monkeypatch):
    driver = Mock(return_value=Mock())
    monkeypatch.setattr(client_module, "CHDriver", driver)
    monkeypatch.setattr(client_module, "CLICKHOUSE_AVAILABLE", True)

    _client(server_enforced_readonly=True)._create_client()

    assert driver.call_args.kwargs["settings"] is None


def test_server_locked_read_sends_no_query_setting_overrides(monkeypatch):
    native = Mock()
    native.execute.return_value = ([("ok",)], [("value", "String")])
    client = _client(server_enforced_readonly=True)
    monkeypatch.setattr(client, "_get_client", Mock(return_value=native))
    monkeypatch.setattr(client, "_return_client", Mock())

    rows, columns, _ = client.execute_read(
        "SELECT 'ok' AS value\nSETTINGS max_threads = 1",
        timeout_ms=250,
        settings={"max_threads": 1, "max_memory_usage": 1024},
    )

    assert rows == [("ok",)]
    assert columns == [("value", "String")]
    assert native.execute.call_args.kwargs["settings"] is None
    assert native.execute.call_args.args[0] == "SELECT 'ok' AS value"


def test_regular_read_keeps_client_side_guardrails(monkeypatch):
    native = Mock()
    native.execute.return_value = ([], [])
    client = _client(server_enforced_readonly=False)
    monkeypatch.setattr(client, "_get_client", Mock(return_value=native))
    monkeypatch.setattr(client, "_return_client", Mock())

    client.execute_read(
        "SELECT 1",
        timeout_ms=250,
        settings={"max_threads": 1},
    )

    assert native.execute.call_args.kwargs["settings"] == {
        "max_threads": 1,
        "readonly": 2,
        "max_execution_time": 0.25,
    }


def test_query_settings_stripper_preserves_nested_literals_and_format():
    sql = """SELECT 'SETTINGS max_threads = 9' AS value,
       (SELECT settings FROM config WHERE settings = 1) AS nested
SETTINGS max_threads = 1, max_memory_usage = 1024
FORMAT JSON"""

    stripped = without_query_settings(sql)

    assert "'SETTINGS max_threads = 9'" in stripped
    assert "WHERE settings = 1" in stripped
    assert "\nSETTINGS max_threads = 1" not in stripped
    assert stripped.endswith("FORMAT JSON")


def test_server_locked_reader_uses_settings_free_native_transport(monkeypatch):
    native = Mock()
    native.execute_read.return_value = ([], [], 1.0)
    native_factory = Mock(return_value=native)
    monkeypatch.setattr(client_module, "ClickHouseClient", native_factory)

    reader = CHSpanReader(
        host="clickhouse.invalid",
        port=8123,
        username="readonly",
        database="futureagi",
        server_enforced_readonly=True,
        native_port=9000,
    )
    reader.list_by_ids(
        ["span-a"],
        project_id="00000000-0000-4000-8000-000000000001",
    )

    assert native_factory.call_args.kwargs["server_enforced_readonly"] is True
    assert native.execute_read.call_args.kwargs["settings"] is None


def test_server_locked_native_adapter_blocks_mutation_methods(monkeypatch):
    monkeypatch.setattr(client_module, "ClickHouseClient", Mock(return_value=Mock()))
    proxy = ServerEnforcedReadOnlyNativeClient(
        host="clickhouse.invalid",
        port=9000,
        username="readonly",
        password="",
        database="futureagi",
    )

    with pytest.raises(RuntimeError, match="mutation methods are disabled"):
        proxy.insert("spans", [])


def test_server_locked_core_client_rejects_non_read_sql_before_transport(monkeypatch):
    native = Mock()
    client = _client(server_enforced_readonly=True)
    get_client = Mock(return_value=native)
    monkeypatch.setattr(client, "_get_client", get_client)
    monkeypatch.setattr(client, "_return_client", Mock())

    with pytest.raises(RuntimeError, match="Only read statements"):
        client.execute("DROP TABLE spans")

    native.execute.assert_not_called()
    get_client.assert_not_called()


def test_server_locked_execute_iter_is_blocked_before_acquiring_connection(
    monkeypatch,
):
    client = _client(server_enforced_readonly=True)
    get_client = Mock()
    monkeypatch.setattr(client, "_get_client", get_client)

    with pytest.raises(RuntimeError, match="managed native block stream"):
        client.execute_iter("SELECT 1")

    get_client.assert_not_called()


def test_native_block_stream_returns_connection_only_after_full_exhaustion():
    connection = Mock()
    connection.execute_iter.return_value = iter([(1,), (2,)])
    pool = Mock()
    pool._get_client.return_value = connection

    with _NativeBlockStream(pool, "SELECT 1", {}, block_size=1) as blocks:
        assert list(blocks) == [[(1,)], [(2,)]]

    pool._return_client.assert_called_once_with(connection)
    connection.disconnect.assert_not_called()


def test_native_block_stream_retires_connection_when_consumer_stops_early():
    connection = Mock()
    connection.execute_iter.return_value = iter([(1,), (2,)])
    pool = Mock()
    pool._get_client.return_value = connection

    with _NativeBlockStream(pool, "SELECT 1", {}, block_size=1) as blocks:
        assert next(blocks) == [(1,)]

    pool._return_client.assert_not_called()
    connection.disconnect.assert_called_once_with()


def test_native_block_stream_retires_connection_when_iterator_raises():
    def rows():
        yield (1,)
        raise RuntimeError("native stream failed")

    connection = Mock()
    connection.execute_iter.return_value = rows()
    pool = Mock()
    pool._get_client.return_value = connection

    with pytest.raises(RuntimeError, match="native stream failed"):
        with _NativeBlockStream(pool, "SELECT 1", {}, block_size=1) as blocks:
            list(blocks)

    pool._return_client.assert_not_called()
    connection.disconnect.assert_called_once_with()


@pytest.mark.parametrize(
    "reader_module",
    [
        "tracer.services.clickhouse.v2.trace_session_dict_reader",
        "tracer.services.clickhouse.v2.end_user_dict_reader",
    ],
)
def test_dimension_readers_use_native_transport_for_locked_profile(
    monkeypatch, reader_module
):
    import importlib

    module = importlib.import_module(reader_module)
    module._reset_client()
    config = {
        "host": "clickhouse.invalid",
        "http_port": 8123,
        "tcp_port": 9000,
        "user": "readonly",
        "password": "",
        "database": "futureagi",
        "server_enforced_readonly": True,
    }
    native = Mock()
    native_factory = Mock(return_value=native)
    monkeypatch.setattr(module, "get_v2_config", lambda: config)
    monkeypatch.setattr(
        "tracer.services.clickhouse.server_readonly.ServerEnforcedReadOnlyNativeClient",
        native_factory,
    )

    try:
        assert module._get_client() is native
        assert native_factory.call_args.kwargs["port"] == 9000
    finally:
        module._reset_client()
