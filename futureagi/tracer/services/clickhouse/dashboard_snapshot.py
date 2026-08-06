"""Shared immutable-relation snapshots for exact dashboard refreshes.

Dashboard metrics execute concurrently and may span several independently
mutable ClickHouse tables.  A response is exact only when every statement uses
the same table-version ceilings.  This module derives a strict physical-table
dependency set from the generated SQL, captures each supported ceiling once,
and returns one ``additional_table_filters`` map for the whole refresh.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from time import monotonic
from typing import Any

from tracer.services.clickhouse.list_cursor import capture_snapshot_version_ceiling


class DashboardRelationSnapshotError(RuntimeError):
    """A generated dashboard relation cannot be frozen safely."""


@dataclass(frozen=True)
class DashboardRelationSnapshot:
    settings: dict[str, Any]
    tables: tuple[str, ...]
    version_ceilings: dict[str, int]
    snapshot_query_count: int


_DIRECT_EPOCH_NANO_TABLES = frozenset({"spans", "tracer_eval_logger_v2", "traces"})
_PEERDB_INTEGER_TABLES = frozenset(
    {
        "model_hub_cell",
        "model_hub_column",
        "model_hub_dataset",
        "model_hub_score",
        "simulate_agent_definition",
        "simulate_agent_version",
        "simulate_call_execution",
        "simulate_run_test",
        "simulate_scenarios",
        "simulate_test_execution",
        "tracer_eval_logger",
        "usage_apicalllog",
    }
)
_DATETIME64_MICRO_TABLES = frozenset(
    {"end_user_id_remap", "end_users", "trace_session_id_remap"}
)
_UNVERSIONED_MUTABLE_TABLES = frozenset({"dashboard_attr_rollup"})
_SUPPORTED_TABLES = (
    _DIRECT_EPOCH_NANO_TABLES
    | _PEERDB_INTEGER_TABLES
    | _DATETIME64_MICRO_TABLES
    | _UNVERSIONED_MUTABLE_TABLES
)
_TABLE_REFERENCE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?!\()(?P<table>`?[A-Za-z_][A-Za-z0-9_]*`?)",
    re.IGNORECASE,
)
_CTE_RE = re.compile(
    r"(?:\bWITH|,)\s*`?(?P<name>[A-Za-z_][A-Za-z0-9_]*)`?\s+AS\s*\(",
    re.IGNORECASE,
)


def _capture_relation_version_ceiling(
    *,
    analytics: Any,
    table: str,
    version_column: str,
    datetime64_micro: bool,
    timeout_ms: int,
) -> int:
    """Capture one relation ceiling without importing ORM-backed graph code."""

    if (
        not table.replace("_", "").isalnum()
        or not version_column.replace("_", "").isalnum()
        or timeout_ms <= 0
    ):
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot topology is invalid"
        )
    version_expr = (
        f"toUnixTimestamp64Micro({version_column})"
        if datetime64_micro
        else version_column
    )
    result = analytics.execute_ch_query(
        f"SELECT coalesce(max({version_expr}), 0) + 1 AS version_ceiling FROM {table}",
        {},
        timeout_ms=max(1, int(timeout_ms)),
        settings={"max_threads": 1, "max_result_rows": 1},
    )
    if not result.data:
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot ceiling is unavailable"
        )
    row = result.data[0]
    if isinstance(row, dict):
        raw_ceiling = row.get("version_ceiling")
    else:
        columns = [
            value[0] if isinstance(value, (list, tuple)) else value
            for value in (result.columns or [])
        ]
        try:
            raw_ceiling = row[columns.index("version_ceiling")]
        except (ValueError, IndexError, TypeError) as exc:
            raise DashboardRelationSnapshotError(
                "dashboard relation snapshot ceiling is unavailable"
            ) from exc
    try:
        ceiling = int(raw_ceiling)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot ceiling is unavailable"
        ) from exc
    if ceiling <= 0:
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot ceiling is unavailable"
        )
    return ceiling


def _scrub_sql_literals_and_comments(sql: str) -> str:
    output = list(sql)
    index = 0
    while index < len(output):
        char = output[index]
        following = output[index + 1] if index + 1 < len(output) else ""
        # Backticks quote identifiers in ClickHouse and must remain visible to
        # the physical-relation parser. Only scrub string literals here.
        if char in {"'", '"'}:
            quote = char
            output[index] = " "
            index += 1
            while index < len(output):
                current = output[index]
                output[index] = " "
                if current == "\\":
                    if index + 1 < len(output):
                        output[index + 1] = " "
                    index += 2
                    continue
                if current == quote:
                    if index + 1 < len(output) and output[index + 1] == quote:
                        output[index + 1] = " "
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if char == "-" and following == "-":
            while index < len(output) and output[index] != "\n":
                output[index] = " "
                index += 1
            continue
        if char == "/" and following == "*":
            output[index] = output[index + 1] = " "
            index += 2
            while index < len(output):
                current = output[index]
                next_char = output[index + 1] if index + 1 < len(output) else ""
                output[index] = " "
                if current == "*" and next_char == "/":
                    output[index + 1] = " "
                    index += 2
                    break
                index += 1
            continue
        index += 1
    return "".join(output)


def dashboard_physical_relations(sql: str) -> frozenset[str]:
    """Return strict known physical dependencies from one generated query."""

    scrubbed = _scrub_sql_literals_and_comments(sql)
    ctes = {match.group("name").lower() for match in _CTE_RE.finditer(scrubbed)}
    relations: set[str] = set()
    unknown: set[str] = set()
    for match in _TABLE_REFERENCE_RE.finditer(scrubbed):
        table = match.group("table").replace("`", "").lower()
        if table in ctes:
            continue
        if table in _SUPPORTED_TABLES:
            relations.add(table)
        else:
            unknown.add(table)
    if unknown:
        raise DashboardRelationSnapshotError(
            "dashboard query references an unsupported physical relation"
        )
    if not relations:
        raise DashboardRelationSnapshotError(
            "dashboard query has no supported physical relation"
        )
    return frozenset(relations)


def capture_dashboard_relation_snapshot(
    *,
    analytics: Any,
    sql_statements: list[str],
    base_settings: dict[str, Any],
    timeout_ms: int,
) -> DashboardRelationSnapshot:
    """Freeze every supported mutable relation once for a whole refresh."""

    if not sql_statements:
        raise DashboardRelationSnapshotError("dashboard query plan is empty")
    if timeout_ms <= 0:
        raise DashboardRelationSnapshotError("dashboard snapshot timeout is invalid")
    if getattr(analytics, "supports_per_query_read_settings", True) is False:
        raise DashboardRelationSnapshotError(
            "dashboard exact snapshot settings are unavailable"
        )
    deadline = monotonic() + (timeout_ms / 1000)

    def remaining_ms() -> int:
        remaining = int((deadline - monotonic()) * 1000)
        if remaining < 1:
            raise DashboardRelationSnapshotError(
                "dashboard relation snapshot deadline exceeded"
            )
        return remaining

    tables = tuple(
        sorted(
            set().union(*(dashboard_physical_relations(sql) for sql in sql_statements))
        )
    )
    if _UNVERSIONED_MUTABLE_TABLES.intersection(tables):
        raise DashboardRelationSnapshotError(
            "dashboard query references an unversioned mutable relation"
        )

    additional_filters = dict(base_settings.get("additional_table_filters") or {})
    version_ceilings: dict[str, int] = {}
    snapshot_query_count = 0
    direct_tables = sorted(_DIRECT_EPOCH_NANO_TABLES.intersection(tables))
    if direct_tables:
        existing_direct_ceilings = {
            _existing_version_ceiling(
                additional_filters,
                table=table,
                pattern=r"_version\s*<\s*(?P<ceiling>[1-9][0-9]*)",
            )
            for table in direct_tables
            if table in additional_filters
        }
        if len(existing_direct_ceilings) > 1:
            raise DashboardRelationSnapshotError(
                "dashboard direct-write snapshot ceilings conflict"
            )
        if existing_direct_ceilings:
            shared_direct_ceiling = existing_direct_ceilings.pop()
        else:
            try:
                shared_direct_ceiling = capture_snapshot_version_ceiling(
                    analytics,
                    timeout_ms=remaining_ms(),
                )
            except Exception as exc:
                raise DashboardRelationSnapshotError(
                    "dashboard direct-write snapshot capture failed"
                ) from exc
            snapshot_query_count += 1
        for table in direct_tables:
            additional_filters[table] = f"_version < {shared_direct_ceiling}"
            version_ceilings[table] = shared_direct_ceiling

    for table in sorted(_PEERDB_INTEGER_TABLES.intersection(tables)):
        if table in additional_filters:
            ceiling = _existing_version_ceiling(
                additional_filters,
                table=table,
                pattern=r"_peerdb_version\s*<\s*(?P<ceiling>[1-9][0-9]*)",
            )
        else:
            try:
                ceiling = _capture_relation_version_ceiling(
                    analytics=analytics,
                    table=table,
                    version_column="_peerdb_version",
                    datetime64_micro=False,
                    timeout_ms=remaining_ms(),
                )
            except Exception as exc:
                raise DashboardRelationSnapshotError(
                    "dashboard PeerDB snapshot capture failed"
                ) from exc
            snapshot_query_count += 1
        additional_filters[table] = f"_peerdb_version < {ceiling}"
        version_ceilings[table] = ceiling

    for table in sorted(_DATETIME64_MICRO_TABLES.intersection(tables)):
        if table in additional_filters:
            ceiling = _existing_version_ceiling(
                additional_filters,
                table=table,
                pattern=(
                    r"toUnixTimestamp64Micro\(version\)\s*<\s*"
                    r"(?P<ceiling>[1-9][0-9]*)"
                ),
            )
        else:
            try:
                ceiling = _capture_relation_version_ceiling(
                    analytics=analytics,
                    table=table,
                    version_column="version",
                    datetime64_micro=True,
                    timeout_ms=remaining_ms(),
                )
            except Exception as exc:
                raise DashboardRelationSnapshotError(
                    "dashboard dimension snapshot capture failed"
                ) from exc
            snapshot_query_count += 1
        additional_filters[table] = f"toUnixTimestamp64Micro(version) < {ceiling}"
        version_ceilings[table] = ceiling

    if set(version_ceilings) != set(tables):
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot topology is incomplete"
        )
    return DashboardRelationSnapshot(
        settings={**base_settings, "additional_table_filters": additional_filters},
        tables=tables,
        version_ceilings=version_ceilings,
        snapshot_query_count=snapshot_query_count,
    )


def _existing_version_ceiling(
    additional_filters: dict[str, str],
    *,
    table: str,
    pattern: str,
) -> int:
    """Validate and return a ceiling captured earlier in this refresh."""

    predicate = additional_filters.get(table)
    match = re.fullmatch(pattern, str(predicate or "").strip())
    if match is None:
        raise DashboardRelationSnapshotError(
            "dashboard relation snapshot settings conflict"
        )
    return int(match.group("ceiling"))


__all__ = [
    "DashboardRelationSnapshot",
    "DashboardRelationSnapshotError",
    "capture_dashboard_relation_snapshot",
    "dashboard_physical_relations",
]
