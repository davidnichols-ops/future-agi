"""
ClickHouse Consistency Monitoring

Monitors PG-CH data consistency, CDC replication lag, and query performance.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog
from django.conf import settings
from django.db import connection as pg_connection

from tracer.services.clickhouse.client import (
    get_clickhouse_client,
    is_clickhouse_enabled,
)

logger = structlog.get_logger(__name__)

CDC_LAG_DEGRADED_SECONDS = 60
V2_PROBE_TIMEOUT_SECONDS = 5


@dataclass
class ConsistencyResult:
    """Result of a consistency check between PG and CH."""

    table: str
    pg_count: int
    ch_count: int
    difference: int
    difference_pct: float
    is_consistent: bool  # True if difference < threshold
    checked_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class HealthStatus:
    """Overall health status of the ClickHouse analytics backend."""

    status: str  # "healthy", "degraded", "unhealthy"
    clickhouse_connected: bool
    cdc_lag: dict[str, float]  # table -> lag_seconds
    clickhouse_v2_connected: bool = False
    last_consistency_check: dict | None = None
    details: dict[str, Any] = field(default_factory=dict)


class ConsistencyChecker:
    """Checks data consistency between PostgreSQL and ClickHouse."""

    # CH25 close-out (2026-05-28): `tracer_observation_span` removed —
    # spans live in the v2 typed-JSON `spans` table populated by
    # fi-collector via OTLP. No CDC mirror means no PG↔CH consistency
    # check is meaningful for spans.
    MONITORED_TABLES = [
        ("tracer_trace", "tracer_trace"),
        ("trace_session", "trace_session"),
        ("tracer_eval_logger", "tracer_eval_logger"),
    ]

    def __init__(self):
        self._ch_client = get_clickhouse_client()

    def check_row_counts(
        self,
        project_id: str,
        start_date: datetime,
        end_date: datetime,
        threshold_pct: float = 1.0,
    ) -> list[ConsistencyResult]:
        """Compare row counts between PG and CH for each table."""
        results = []
        for pg_table, ch_table in self.MONITORED_TABLES:
            try:
                # PG count
                with pg_connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {pg_table} WHERE project_id = %s AND created_at >= %s AND created_at <= %s",
                        [project_id, start_date, end_date],
                    )
                    pg_count = cursor.fetchone()[0]

                # CH count (with FINAL for deduplication)
                ch_result = self._ch_client.execute(
                    f"SELECT count() FROM {ch_table} FINAL WHERE project_id = %(project_id)s AND created_at >= %(start)s AND created_at <= %(end)s AND _peerdb_is_deleted = 0",
                    {"project_id": project_id, "start": start_date, "end": end_date},
                )
                ch_count = ch_result[0][0]

                diff = abs(pg_count - ch_count)
                diff_pct = (diff / max(pg_count, 1)) * 100

                results.append(
                    ConsistencyResult(
                        table=pg_table,
                        pg_count=pg_count,
                        ch_count=ch_count,
                        difference=diff,
                        difference_pct=diff_pct,
                        is_consistent=diff_pct <= threshold_pct,
                    )
                )
            except Exception as e:
                logger.error("Consistency check failed", table=pg_table, error=str(e))
                results.append(
                    ConsistencyResult(
                        table=pg_table,
                        pg_count=-1,
                        ch_count=-1,
                        difference=-1,
                        difference_pct=-1,
                        is_consistent=False,
                    )
                )
        return results

    def get_cdc_lag(self) -> dict[str, float]:
        """Get CDC replication lag per table in seconds."""
        lag = {}
        tables = [
            "tracer_trace",
            "trace_session",
            "tracer_eval_logger",
        ]
        for table in tables:
            try:
                result = self._ch_client.execute(
                    f"SELECT max(_peerdb_synced_at) FROM {table}"
                )
                if result and result[0][0]:
                    last_sync = result[0][0]
                    if isinstance(last_sync, datetime):
                        lag[table] = (datetime.utcnow() - last_sync).total_seconds()
                    else:
                        lag[table] = -1
                else:
                    lag[table] = -1  # No data
            except Exception as e:
                logger.warning("CDC lag check failed", table=table, error=str(e))
                lag[table] = -1
        return lag

    def check_v2_connection(self) -> bool:
        """Run an authenticated query against the CH25 HTTP endpoint.

        `self._ch_client` speaks the native protocol with the legacy credentials,
        so it stays green while the v2 HTTP credentials are wrong, which is how
        a v2 auth failure reached customers with this endpoint reporting healthy.
        `/ping` is unauthenticated, so it has to be a real query against the
        configured database to cover both the credentials and the database name.
        """
        try:
            import clickhouse_connect
        except ImportError:
            logger.warning("clickhouse_v2_health_probe_unavailable")
            return False

        from tracer.services.clickhouse.v2 import get_v2_config

        cfg = get_v2_config()
        try:
            client = clickhouse_connect.get_client(
                host=cfg["host"],
                port=cfg["http_port"],
                username=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                connect_timeout=V2_PROBE_TIMEOUT_SECONDS,
                send_receive_timeout=V2_PROBE_TIMEOUT_SECONDS,
            )
            try:
                return bool(client.command("SELECT 1"))
            finally:
                client.close()
        except Exception as e:
            logger.warning("clickhouse_v2_health_probe_failed", error=str(e))
            return False

    def get_health_status(self) -> HealthStatus:
        """Get overall health status."""
        if not is_clickhouse_enabled():
            return HealthStatus(
                status="disabled",
                clickhouse_connected=False,
                cdc_lag={},
            )

        connected = self._ch_client.ping()
        cdc_lag = self.get_cdc_lag() if connected else {}
        v2_connected = self.check_v2_connection() if connected else False

        # Determine status
        if not connected:
            status = "unhealthy"
        elif not v2_connected or any(
            v > CDC_LAG_DEGRADED_SECONDS for v in cdc_lag.values() if v > 0
        ):
            status = "degraded"
        else:
            status = "healthy"

        return HealthStatus(
            status=status,
            clickhouse_connected=connected,
            cdc_lag=cdc_lag,
            clickhouse_v2_connected=v2_connected,
            details={
                "routing": {
                    k: v
                    for k, v in settings.CLICKHOUSE.items()
                    if k.startswith("CH_ROUTE_")
                }
            },
        )
