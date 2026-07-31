from unittest.mock import Mock

import pytest

from model_hub.apps import ModelHubConfig, startup_db_mutations_disabled


@pytest.mark.parametrize("value", ["true", "TRUE", " true "])
def test_startup_db_mutation_gate_disables_mutations(monkeypatch, value):
    monkeypatch.setenv("NO_STARTUP_DB_MUTATIONS", value)

    assert startup_db_mutations_disabled() is True


@pytest.mark.parametrize("value", ["false", "FALSE", " false "])
def test_startup_db_mutation_gate_preserves_default_startup(monkeypatch, value):
    monkeypatch.setenv("NO_STARTUP_DB_MUTATIONS", value)

    assert startup_db_mutations_disabled() is False


def test_startup_db_mutation_gate_fails_closed_on_invalid_value(monkeypatch):
    monkeypatch.setenv("NO_STARTUP_DB_MUTATIONS", "maybe")

    with pytest.raises(RuntimeError, match="must be exactly"):
        startup_db_mutations_disabled()


def _warmup_sql(monkeypatch, *, drops_legacy_chain: bool) -> list[str]:
    monkeypatch.setattr(
        "tracer.services.clickhouse.schema.should_drop_legacy_chain",
        lambda: drops_legacy_chain,
    )
    client = Mock()
    existing = {"traces"} if drops_legacy_chain else {"tracer_trace"}
    client.table_exists.side_effect = lambda table: table in existing
    ModelHubConfig._warm_ch_cache(client)
    return [call.args[0] for call in client.execute_read.call_args_list]


def test_ch25_cache_warm_reads_traces_instead_of_dropped_cdc_table(monkeypatch):
    queries = _warmup_sql(monkeypatch, drops_legacy_chain=True)

    assert any("FROM traces " in query for query in queries)
    assert not any("FROM tracer_trace " in query for query in queries)


def test_legacy_cache_warm_keeps_cdc_trace_table(monkeypatch):
    queries = _warmup_sql(monkeypatch, drops_legacy_chain=False)

    assert any("FROM tracer_trace " in query for query in queries)
    assert not any("FROM traces " in query for query in queries)


def test_cache_warm_is_bounded_to_subsecond_reads(monkeypatch):
    monkeypatch.setattr(
        "tracer.services.clickhouse.schema.should_drop_legacy_chain",
        lambda: True,
    )
    client = Mock()
    client.table_exists.side_effect = lambda table: table == "traces"

    ModelHubConfig._warm_ch_cache(client)

    assert client.execute_read.call_count > 0
    for call in client.execute_read.call_args_list:
        assert call.kwargs["timeout_ms"] == 750
        assert call.kwargs["settings"]["max_threads"] == 2
        assert call.kwargs["settings"]["max_memory_usage"] == 128 * 1024 * 1024
        assert call.kwargs["settings"]["timeout_overflow_mode"] == "break"


def test_cache_warm_skips_trace_query_when_neither_trace_table_exists(monkeypatch):
    monkeypatch.setattr(
        "tracer.services.clickhouse.schema.should_drop_legacy_chain",
        lambda: False,
    )
    client = Mock()
    client.table_exists.return_value = False

    ModelHubConfig._warm_ch_cache(client)

    queries = [call.args[0] for call in client.execute_read.call_args_list]
    assert not any("FROM traces " in query for query in queries)
    assert not any("FROM tracer_trace " in query for query in queries)
