"""Explicit CH25 query service for v2 query-builder reads."""

from __future__ import annotations

import threading

from tracer.services.clickhouse.client import ClickHouseClient
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.query_service import (
    AnalyticsQueryService,
    QueryExecutor,
)
from tracer.services.clickhouse.v2 import get_v2_config

_client: ClickHouseClient | None = None
_client_lock = threading.Lock()


def get_v2_query_client() -> ClickHouseClient:
    """Return the process-wide pooled native client for direct-write CH25."""

    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                config = get_v2_config()
                _client = ClickHouseClient(
                    host=config["host"],
                    port=config["tcp_port"],
                    user=config["user"],
                    password=config["password"],
                    database=config["database"],
                    server_enforced_readonly=config["server_enforced_readonly"],
                )
    return _client


def reset_v2_query_client() -> None:
    """Close and clear the singleton; intended for test/config reloads."""

    global _client
    with _client_lock:
        if _client is not None:
            _client.close()
        _client = None


class V2AnalyticsQueryService(AnalyticsQueryService):
    """Run generic read SQL against the configured direct-write CH25 cluster."""

    def __init__(self) -> None:
        self._ch_client = get_v2_query_client()


def query_service_for_builder(
    query_type: str,
    builder_class: type[BaseQueryBuilder],
    fallback: QueryExecutor,
) -> QueryExecutor:
    """Use the service paired with this query type's dispatched builder.

    Builder inheritance alone is not a safe routing key: several list builders
    share base classes, and a future multiple-inheritance change could make a
    class look like the v2 implementation for a different query type. Pair the
    class with the same explicit query type passed to the dispatch factory so a
    v2 SQL builder can only execute on its matching direct-write CH25 service.
    Explicit test executors remain untouched.
    """

    if not isinstance(fallback, AnalyticsQueryService):
        return fallback

    from tracer.services.clickhouse.v2.dispatch import get_v2_class

    normalized_query_type = (
        query_type.upper() if isinstance(query_type, str) else str(query_type).upper()
    )
    v2_class = get_v2_class(normalized_query_type)
    if v2_class is not None and issubclass(builder_class, v2_class):
        return V2AnalyticsQueryService()
    return fallback


__all__ = [
    "V2AnalyticsQueryService",
    "get_v2_query_client",
    "query_service_for_builder",
    "reset_v2_query_client",
]
