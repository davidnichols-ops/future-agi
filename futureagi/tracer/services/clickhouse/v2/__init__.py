"""
ClickHouse 25.3 (`v2`) service layer for FutureAGI.

This package is the production home for everything that talks to the new
typed-Map + typed-JSON spans schema. Imports cleanly from Django so management
commands, migrations, signal handlers, and the eval runner can all use it.

Layout:
    schema/             Versioned .sql files (idempotent via apply_schema.py)
    apply_schema.py     Hash-tracked DDL runner with drift detection
    adapter.py          Pure-Python PG-row → CH-row converter
    span_reader.py      CHSpanReader for read paths (eval runner, future dashboards)

Companion management commands live in `tracer/management/commands/ch25_*`.

Configuration: pulled from `settings.CLICKHOUSE_V2` (env-backed). See the
package docstring of `apply_schema.py` and the README at the bottom of
this package for the operator-facing wiring.

Migration provenance: this code originated in
planning/clickhouse-rearch/migration/ where it was test-driven and codex-
reviewed; the validated implementation files were copied here as a single
atomic move once the migration tooling was production-ready. The original
planning directory keeps the docs (DECISIONS, RUNBOOK, REVIEWS) as the
permanent historical record.
"""

from __future__ import annotations

import os
from typing import Any

from django.conf import settings


DEFAULT_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8123
DEFAULT_TCP_PORT = 9000
DEFAULT_USER = "default"
DEFAULT_PASSWORD = ""
DEFAULT_DATABASE = "futureagi"


def _configured(*values: Any, default: Any = None) -> Any:
    """First value that was actually supplied, else `default`.

    Presence, not truthiness: an unset setting is None and an unset env var
    reads as "", so neither can shadow the next source in the chain.
    """
    for value in values:
        if value is not None and value != "":
            return value
    return default


def get_v2_config() -> dict[str, Any]:
    """Read the CH 25.3 connection config, falling back to the legacy
    `CLICKHOUSE` dict (so a single-cluster deployment Just Works).

    Per-key precedence: `settings.CLICKHOUSE_V2[...]`, then the `CH25_*` env
    var, then the legacy `settings.CLICKHOUSE` counterpart where one exists
    (`host`, `user`, `password` and `database`), then the DEFAULT_* constant.
    """
    legacy = getattr(settings, "CLICKHOUSE", {}) or {}
    cfg = getattr(settings, "CLICKHOUSE_V2", {}) or {}
    return {
        "host": _configured(
            cfg.get("CH25_HOST"),
            os.getenv("CH25_HOST"),
            legacy.get("CH_HOST"),
            default=DEFAULT_HOST,
        ),
        "http_port": int(
            _configured(
                cfg.get("CH25_HTTP_PORT"),
                os.getenv("CH25_HTTP_PORT"),
                default=DEFAULT_HTTP_PORT,
            )
        ),
        # No legacy fallback: CLICKHOUSE["CH_PORT"] defaults to the HTTP port,
        # so it is not a safe source for the native port.
        "tcp_port": int(
            _configured(
                cfg.get("CH25_TCP_PORT"),
                os.getenv("CH25_TCP_PORT"),
                default=DEFAULT_TCP_PORT,
            )
        ),
        "user": _configured(
            cfg.get("CH25_USER"),
            os.getenv("CH25_USER"),
            legacy.get("CH_USERNAME"),
            default=DEFAULT_USER,
        ),
        "password": _configured(
            cfg.get("CH25_PASSWORD"),
            os.getenv("CH25_PASSWORD"),
            legacy.get("CH_PASSWORD"),
            default=DEFAULT_PASSWORD,
        ),
        "database": _configured(
            cfg.get("CH25_DATABASE"),
            os.getenv("CH25_DATABASE"),
            legacy.get("CH_DATABASE"),
            default=DEFAULT_DATABASE,
        ),
    }


def get_reader():
    """Returns a CHSpanReader bound to the v2 cluster, configured from settings.

    Used by `tracer/utils/eval.py` (post-cutover read path) and by management
    commands that need to inspect spans during validation.
    """
    from .span_reader import CHSpanReader
    cfg = get_v2_config()
    return CHSpanReader(
        host=cfg["host"], port=cfg["http_port"],
        username=cfg["user"], password=cfg["password"],
        database=cfg["database"],
    )


__all__ = ["get_v2_config", "get_reader"]
