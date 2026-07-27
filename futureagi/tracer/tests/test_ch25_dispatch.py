"""
Pin the v1↔v2 dispatch behavior.

Tests the factory in tracer/services/clickhouse/v2/dispatch.py: given a
query type + settings, the right builder class comes back.
Plus how the same settings resolve into a v2 connection config.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from tracer.services.clickhouse.v2 import (
    DEFAULT_DATABASE,
    DEFAULT_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_PASSWORD,
    DEFAULT_TCP_PORT,
    DEFAULT_USER,
    get_v2_config,
)
from tracer.services.clickhouse.v2.dispatch import (
    get_query_builder_class,
    get_v1_class,
    get_v2_class,
)


def _override(**routing_overrides):
    """Helper: temporarily set CLICKHOUSE_V2 routing settings via Django."""
    from django.test.utils import override_settings
    base = {
        "QUERY_TYPES_V2_PRIMARY": "",
        "QUERY_TYPES_V2_ONLY":    "",
        "QUERY_TYPES_SHADOW":     "",
        "QUERY_TYPES_DISABLED":   "",
    }
    base.update(routing_overrides)
    return override_settings(CLICKHOUSE_V2=base)


# ─── Default routing → v1 ────────────────────────────────────────────────────
def test_unrouted_query_type_returns_v1_class():
    with _override():
        cls = get_query_builder_class("SPAN_LIST")
    assert cls.__name__ == "SpanListQueryBuilder"


def test_unrouted_works_for_every_registered_type():
    """Smoke: every query type in the registry resolves to its v1 class by default."""
    from tracer.services.clickhouse.v2.dispatch import _REGISTRY
    with _override():
        for qt in _REGISTRY:
            cls = get_query_builder_class(qt)
            assert cls.__name__ == _REGISTRY[qt].v1_class, f"{qt} → wrong class"


# ─── V2_ONLY routing → v2 ────────────────────────────────────────────────────
def test_v2_only_routing_returns_v2_class():
    with _override(QUERY_TYPES_V2_ONLY="SPAN_LIST"):
        cls = get_query_builder_class("SPAN_LIST")
    assert cls.__name__ == "SpanListQueryBuilderV2"


def test_v2_primary_routing_returns_v2_class():
    with _override(QUERY_TYPES_V2_PRIMARY="TRACE_LIST"):
        cls = get_query_builder_class("TRACE_LIST")
    assert cls.__name__ == "TraceListQueryBuilderV2"


def test_shadow_routing_returns_v1_class_for_user_facing_call():
    """SHADOW mode means BOTH run, but the user-facing result is v1.
    The shadow harness (shadow.run_with_shadow) does the parallel v2 run.
    The factory returns v1 — that's the contract.
    """
    with _override(QUERY_TYPES_SHADOW="DASHBOARD"):
        cls = get_query_builder_class("DASHBOARD")
    assert cls.__name__ == "DashboardQueryBuilder"


# ─── Case-insensitive query-type lookups ─────────────────────────────────────
def test_case_insensitive_lookup():
    with _override(QUERY_TYPES_V2_ONLY="span_list"):
        cls = get_query_builder_class("SPAN_LIST")
    assert cls.__name__ == "SpanListQueryBuilderV2"


def test_lowercase_query_type_resolves():
    with _override(QUERY_TYPES_V2_ONLY="SPAN_LIST"):
        cls = get_query_builder_class("span_list")
    assert cls.__name__ == "SpanListQueryBuilderV2"


# ─── Unknown query type → KeyError (loud, not silent fallback) ───────────────
def test_unknown_query_type_raises():
    with pytest.raises(KeyError, match="DOES_NOT_EXIST"):
        get_query_builder_class("DOES_NOT_EXIST")


# ─── Convenience helpers ─────────────────────────────────────────────────────
def test_get_v1_class_ignores_routing():
    with _override(QUERY_TYPES_V2_ONLY="SPAN_LIST"):
        cls = get_v1_class("SPAN_LIST")
    assert cls.__name__ == "SpanListQueryBuilder"


def test_get_v2_class_ignores_routing():
    with _override():  # default routing = v1 everywhere
        cls = get_v2_class("SPAN_LIST")
    assert cls is not None
    assert cls.__name__ == "SpanListQueryBuilderV2"


def test_get_v2_class_returns_none_when_unregistered():
    # Add a stub entry pointing at no v2 module
    from tracer.services.clickhouse.v2.dispatch import _BuilderEntry, _REGISTRY
    _REGISTRY["UNREGISTERED_V2"] = _BuilderEntry(
        v1_module="tracer.services.clickhouse.query_builders.span_list",
        v1_class="SpanListQueryBuilder",
        v2_module=None,
        v2_class=None,
    )
    try:
        assert get_v2_class("UNREGISTERED_V2") is None
    finally:
        del _REGISTRY["UNREGISTERED_V2"]


# ─── Connection config resolution ────────────────────────────────────────────
_LEGACY_CLICKHOUSE = {
    "CH_HOST":     "ch-legacy.internal",
    "CH_PORT":     "9440",
    "CH_USERNAME": "fagi_app",
    "CH_PASSWORD": "REAL_SECRET",
    "CH_DATABASE": "futureagi_prod",
}

_CH25_CONNECTION_KEYS = (
    "CH25_HOST",
    "CH25_HTTP_PORT",
    "CH25_TCP_PORT",
    "CH25_USER",
    "CH25_PASSWORD",
    "CH25_DATABASE",
)

_END_USER_ID = "11111111-1111-1111-1111-111111111111"


def _override_config(**ch25_overrides):
    """Helper: a legacy CLICKHOUSE plus an explicitly unconfigured CLICKHOUSE_V2."""
    from django.test.utils import override_settings

    # Spelled out key by key rather than read from the settings module, whose
    # CLICKHOUSE_V2 is frozen from the environment at import: CI exports CH25_*
    # and those values would otherwise leak into every override.
    base = {
        "QUERY_TYPES_V2_PRIMARY": "",
        "QUERY_TYPES_V2_ONLY":    "",
        "QUERY_TYPES_SHADOW":     "",
        "QUERY_TYPES_DISABLED":   "",
    }
    base.update({key: None for key in _CH25_CONNECTION_KEYS})
    base.update(ch25_overrides)
    return override_settings(CLICKHOUSE=dict(_LEGACY_CLICKHOUSE), CLICKHOUSE_V2=base)


def test_settings_leave_unset_ch25_connection_keys_unconfigured():
    """settings.py adds no default to a CH25 connection key, so legacy can win."""
    import tfc.settings.settings as deployment_settings

    for key in _CH25_CONNECTION_KEYS:
        assert deployment_settings.CLICKHOUSE_V2[key] == os.getenv(key)


@pytest.fixture
def no_ch25_env(monkeypatch):
    """Strip CH25_* from the process so only settings drive resolution."""
    for key in _CH25_CONNECTION_KEYS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def captured_connect_kwargs(monkeypatch):
    """Capture what clickhouse-connect is asked to connect with."""
    import clickhouse_connect

    captured = {}

    class _Result:
        result_rows: list = []

    class _Client:
        def query(self, *args, **kwargs):
            return _Result()

        def close(self):
            pass

    def _get_client(**kwargs):
        captured.update(kwargs)
        return _Client()

    monkeypatch.setattr(clickhouse_connect, "get_client", _get_client)
    return captured


@pytest.fixture
def end_user_reader():
    """The observe end-user lookup, with its cached client dropped either side."""
    from tracer.services.clickhouse.v2 import end_user_dict_reader

    end_user_dict_reader._reset_client()
    yield end_user_dict_reader
    end_user_dict_reader._reset_client()


def test_end_user_lookup_uses_the_legacy_pair_when_ch25_is_unset(
    no_ch25_env, captured_connect_kwargs, end_user_reader
):
    """Legacy-only config: the lookup sends that cluster's user AND its password."""
    with _override_config():
        end_user_reader.resolve_end_user_fields([_END_USER_ID])

    assert captured_connect_kwargs["host"] == _LEGACY_CLICKHOUSE["CH_HOST"]
    assert captured_connect_kwargs["username"] == _LEGACY_CLICKHOUSE["CH_USERNAME"]
    assert captured_connect_kwargs["password"] == _LEGACY_CLICKHOUSE["CH_PASSWORD"]
    assert captured_connect_kwargs["database"] == _LEGACY_CLICKHOUSE["CH_DATABASE"]


def test_end_user_lookup_pairs_a_ch25_user_with_the_legacy_password(
    no_ch25_env, captured_connect_kwargs, end_user_reader
):
    """Setting only CH25_USER leaves the password on the legacy cluster's value."""
    with _override_config(CH25_USER="ch25_app"):
        end_user_reader.resolve_end_user_fields([_END_USER_ID])

    assert captured_connect_kwargs["username"] == "ch25_app"
    assert captured_connect_kwargs["password"] == _LEGACY_CLICKHOUSE["CH_PASSWORD"]


def test_end_user_lookup_pairs_a_ch25_password_with_the_legacy_user(
    no_ch25_env, captured_connect_kwargs, end_user_reader
):
    """Setting only CH25_PASSWORD leaves the user on the legacy cluster's value."""
    with _override_config(CH25_PASSWORD="CH25_SECRET"):
        end_user_reader.resolve_end_user_fields([_END_USER_ID])

    assert captured_connect_kwargs["username"] == _LEGACY_CLICKHOUSE["CH_USERNAME"]
    assert captured_connect_kwargs["password"] == "CH25_SECRET"


def test_unset_ch25_keys_fall_through_to_the_legacy_connection(no_ch25_env):
    """Host and database come from the legacy cluster when CH25 sets none."""
    with _override_config():
        cfg = get_v2_config()

    assert cfg["host"] == _LEGACY_CLICKHOUSE["CH_HOST"]
    assert cfg["database"] == _LEGACY_CLICKHOUSE["CH_DATABASE"]
    assert cfg["http_port"] == DEFAULT_HTTP_PORT


def test_ports_never_inherit_the_legacy_port(no_ch25_env):
    """Neither port reads legacy CH_PORT, which carries an HTTP-port default."""
    with _override_config():
        cfg = get_v2_config()

    assert cfg["tcp_port"] == DEFAULT_TCP_PORT
    assert cfg["http_port"] == DEFAULT_HTTP_PORT
    assert cfg["tcp_port"] != int(_LEGACY_CLICKHOUSE["CH_PORT"])
    assert cfg["http_port"] != int(_LEGACY_CLICKHOUSE["CH_PORT"])


def test_empty_ch25_values_are_treated_as_unset(no_ch25_env):
    """An empty CH25 value is not a value: the next source still wins."""
    with _override_config(
        CH25_HOST="", CH25_HTTP_PORT="", CH25_TCP_PORT="", CH25_DATABASE=""
    ):
        cfg = get_v2_config()

    assert cfg["host"] == _LEGACY_CLICKHOUSE["CH_HOST"]
    assert cfg["database"] == _LEGACY_CLICKHOUSE["CH_DATABASE"]
    assert cfg["tcp_port"] == DEFAULT_TCP_PORT
    assert cfg["http_port"] == DEFAULT_HTTP_PORT


@pytest.mark.parametrize(
    "http_port, tcp_port, expected_http, expected_tcp",
    [
        (None, None, DEFAULT_HTTP_PORT, DEFAULT_TCP_PORT),
        ("", "", DEFAULT_HTTP_PORT, DEFAULT_TCP_PORT),
        ("18123", "19000", 18123, 19000),
        (18123, 19000, 18123, 19000),
    ],
)
def test_ports_always_resolve_to_ints(
    no_ch25_env, http_port, tcp_port, expected_http, expected_tcp
):
    """Unset, empty, string and int port settings all resolve to an int."""
    with _override_config(CH25_HTTP_PORT=http_port, CH25_TCP_PORT=tcp_port):
        cfg = get_v2_config()

    assert cfg["http_port"] == expected_http
    assert cfg["tcp_port"] == expected_tcp
    assert isinstance(cfg["http_port"], int)
    assert isinstance(cfg["tcp_port"], int)


def test_module_defaults_apply_when_neither_cluster_is_configured(no_ch25_env):
    """With no ClickHouse settings, every key falls back to its DEFAULT_* constant."""
    from django.test.utils import override_settings

    with override_settings(CLICKHOUSE={}, CLICKHOUSE_V2={}):
        cfg = get_v2_config()

    assert cfg == {
        "host": DEFAULT_HOST,
        "http_port": DEFAULT_HTTP_PORT,
        "tcp_port": DEFAULT_TCP_PORT,
        "user": DEFAULT_USER,
        "password": DEFAULT_PASSWORD,
        "database": DEFAULT_DATABASE,
    }
