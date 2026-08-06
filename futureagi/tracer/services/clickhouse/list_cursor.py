"""Opaque, signed continuation cursors for ClickHouse list endpoints.

The cursor is transport state, not a client-readable contract.  It freezes the
request window and carries the complete last-row ordering tuple.  Every token
is bound to the authenticated tenant scope and the normalized query shape, so
it cannot be replayed for a different project, filter, sort, or page size.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from django.conf import settings
from django.core import signing

# Relation ceilings and scan checkpoints are part of the signed wire state.
# Do not let a token minted before those fields existed resume a query against
# only the spans ceiling: residual eval/annotation/user membership could then
# change between pages.  A version/salt bump makes those old tokens fail closed.
CURSOR_VERSION = 2
CURSOR_SALT = "tracer.clickhouse-list-cursor.v2"
DEFAULT_CURSOR_MAX_AGE_SECONDS = 24 * 60 * 60


class ListCursorError(ValueError):
    """A sanitized cursor validation error safe to expose at the API edge."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class ListCursor:
    window_start: datetime
    window_end: datetime
    order: tuple[Any, ...]
    total_rows: int | None = None
    version_ceiling: int = 0
    seen_rows: int = 0
    relation_version_ceilings: dict[str, int] | None = None
    scan_slice_end: datetime | None = None
    scan_before_start_time: datetime | None = None
    scan_before_id: Any = None


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value
        return {"$datetime": normalized.astimezone(UTC).isoformat()}
    if hasattr(value, "hex") and value.__class__.__name__ == "UUID":
        return str(value)
    if isinstance(value, dict):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _restore_json_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"$datetime"}:
        try:
            return datetime.fromisoformat(str(value["$datetime"]))
        except ValueError as exc:
            raise ListCursorError(
                "invalid_cursor", "The continuation cursor is invalid."
            ) from exc
    if isinstance(value, list):
        return tuple(_restore_json_value(item) for item in value)
    if isinstance(value, dict):
        return {key: _restore_json_value(item) for key, item in value.items()}
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _normalized_filter(item: Any) -> Any:
    if not isinstance(item, dict):
        return _json_value(item)
    normalized = _json_value(item)
    config = normalized.get("filter_config") or normalized.get("filterConfig") or {}
    operator = config.get("filter_op") or config.get("filterOp")
    value_key = "filter_value" if "filter_value" in config else "filterValue"
    if operator in {"in", "not_in"} and isinstance(config.get(value_key), list):
        config[value_key] = sorted(
            config[value_key], key=lambda value: _canonical_json(value)
        )
    return normalized


def normalize_cursor_query(query: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize semantically equivalent list query payloads."""

    normalized: dict[str, Any] = {}
    for key, value in query.items():
        if key in {
            "allow_sampled",
            "cursor",
            "cursor_mode",
            "page",
            "page_number",
        }:
            continue
        if key == "filters":
            filters = [_normalized_filter(item) for item in (value or [])]
            normalized[key] = sorted(filters, key=_canonical_json)
        elif key in {"project_ids"} and isinstance(value, (list, tuple)):
            normalized[key] = sorted(str(item) for item in value)
        elif key == "search" and isinstance(value, str):
            normalized[key] = value.strip()
        else:
            normalized[key] = _json_value(value)
    return normalized


def exact_total_explicitly_required(
    request: Any,
    validated_data: dict[str, Any],
) -> bool:
    """Return whether the client explicitly rejected a bounded total.

    ``allow_sampled`` was added after the list APIs were already deployed, so
    older clients omit it.  A complete bounded page is safe to return to those
    clients as long as its lower-bound total is labelled truthfully.  Clients
    that explicitly send ``allow_sampled=false`` retain the strict exact-total
    contract.  Incomplete pages are rejected before this compatibility check.
    """

    query_params = getattr(request, "query_params", None)
    return (
        query_params is not None
        and "allow_sampled" in query_params
        and validated_data.get("allow_sampled") is False
    )


def cursor_scope_for_request(
    request: Any,
    *,
    project_ids: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Return stable authentication + tenant identifiers without token secrets."""

    user = getattr(request, "user", None)
    organization = getattr(request, "organization", None) or getattr(
        user, "organization", None
    )
    workspace = getattr(request, "workspace", None)
    auth = getattr(request, "auth", None)
    auth_id = None
    for attr in ("pk", "id", "key_prefix"):
        candidate = getattr(auth, attr, None)
        if candidate not in (None, ""):
            auth_id = str(candidate)
            break
    return {
        "principal_id": str(getattr(user, "pk", None) or getattr(user, "id", "")),
        "auth_type": auth.__class__.__name__ if auth is not None else "session",
        "auth_id": auth_id,
        "organization_id": str(getattr(organization, "pk", "") or ""),
        "workspace_id": str(
            getattr(workspace, "pk", None)
            or getattr(request, "workspace_id", None)
            or getattr(user, "default_workspace_id", None)
            or ""
        ),
        "project_ids": sorted(str(project_id) for project_id in project_ids),
    }


def _max_age_seconds() -> int:
    value = int(
        getattr(
            settings,
            "TRACER_LIST_CURSOR_MAX_AGE_SECONDS",
            DEFAULT_CURSOR_MAX_AGE_SECONDS,
        )
    )
    return max(1, value)


def encode_list_cursor(
    *,
    resource: str,
    scope: dict[str, Any],
    query: dict[str, Any],
    page_size: int,
    window_start: datetime,
    window_end: datetime,
    order: tuple[Any, ...] | list[Any],
    version_ceiling: int,
    seen_rows: int,
    total_rows: int | None = None,
    relation_version_ceilings: dict[str, int] | None = None,
    scan_slice_end: datetime | None = None,
    scan_before_start_time: datetime | None = None,
    scan_before_id: Any = None,
) -> str:
    if (
        not resource
        or page_size <= 0
        or window_start >= window_end
        or not order
        or version_ceiling <= 0
        or seen_rows < 0
    ):
        raise ValueError("invalid list cursor state")
    normalized_relation_ceilings = {
        str(table): int(ceiling)
        for table, ceiling in (relation_version_ceilings or {}).items()
    }
    if normalized_relation_ceilings and (
        any(not table for table in normalized_relation_ceilings)
        or any(ceiling <= 0 for ceiling in normalized_relation_ceilings.values())
        or normalized_relation_ceilings.get("spans") != int(version_ceiling)
    ):
        raise ValueError("invalid list relation snapshot")
    if (scan_before_start_time is None) != (scan_before_id is None):
        raise ValueError("invalid list scan checkpoint")
    if scan_slice_end is not None and not (window_start < scan_slice_end <= window_end):
        raise ValueError("invalid list scan checkpoint")
    if scan_before_start_time is not None and not (
        window_start <= scan_before_start_time < (scan_slice_end or window_end)
    ):
        raise ValueError("invalid list scan checkpoint")
    payload = {
        "v": CURSOR_VERSION,
        "resource": resource,
        "scope": _digest(scope),
        "query": _digest(normalize_cursor_query(query)),
        "page_size": int(page_size),
        "window_start": _json_value(window_start),
        "window_end": _json_value(window_end),
        "order": _json_value(list(order)),
        "total_rows": int(total_rows) if total_rows is not None else None,
        "version_ceiling": int(version_ceiling),
        "seen_rows": int(seen_rows),
        "relation_version_ceilings": normalized_relation_ceilings or None,
        "scan_slice_end": _json_value(scan_slice_end),
        "scan_before_start_time": _json_value(scan_before_start_time),
        "scan_before_id": _json_value(scan_before_id),
    }
    return signing.dumps(
        payload, key=settings.SECRET_KEY, salt=CURSOR_SALT, compress=True
    )


def decode_list_cursor(
    token: str,
    *,
    resource: str,
    scope: dict[str, Any],
    query: dict[str, Any],
    page_size: int,
) -> ListCursor:
    try:
        payload = signing.loads(
            token,
            key=settings.SECRET_KEY,
            salt=CURSOR_SALT,
            max_age=_max_age_seconds(),
        )
    except signing.SignatureExpired as exc:
        raise ListCursorError(
            "cursor_expired", "The continuation cursor has expired."
        ) from exc
    except (signing.BadSignature, TypeError, ValueError) as exc:
        raise ListCursorError(
            "invalid_cursor", "The continuation cursor is invalid."
        ) from exc

    if not isinstance(payload, dict) or payload.get("v") != CURSOR_VERSION:
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    expected = (
        payload.get("resource") == resource
        and payload.get("scope") == _digest(scope)
        and payload.get("query") == _digest(normalize_cursor_query(query))
        and payload.get("page_size") == int(page_size)
    )
    if not expected:
        raise ListCursorError(
            "cursor_mismatch",
            "The continuation cursor does not match this request.",
        )
    try:
        window_start = _restore_json_value(payload["window_start"])
        window_end = _restore_json_value(payload["window_end"])
        order = _restore_json_value(payload["order"])
    except (KeyError, TypeError) as exc:
        raise ListCursorError(
            "invalid_cursor", "The continuation cursor is invalid."
        ) from exc
    if (
        not isinstance(window_start, datetime)
        or not isinstance(window_end, datetime)
        or window_start >= window_end
        or not isinstance(order, tuple)
        or not order
        or not isinstance(payload.get("version_ceiling"), int)
        or payload["version_ceiling"] <= 0
        or not isinstance(payload.get("seen_rows"), int)
        or payload["seen_rows"] < 0
    ):
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    raw_relation_ceilings = payload.get("relation_version_ceilings")
    if raw_relation_ceilings is None:
        relation_version_ceilings = None
    elif not isinstance(raw_relation_ceilings, dict):
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    else:
        try:
            relation_version_ceilings = {
                str(table): int(ceiling)
                for table, ceiling in raw_relation_ceilings.items()
            }
        except (TypeError, ValueError, OverflowError) as exc:
            raise ListCursorError(
                "invalid_cursor", "The continuation cursor is invalid."
            ) from exc
        if (
            not relation_version_ceilings
            or any(not table for table in relation_version_ceilings)
            or any(ceiling <= 0 for ceiling in relation_version_ceilings.values())
            or relation_version_ceilings.get("spans") != int(payload["version_ceiling"])
        ):
            raise ListCursorError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
    scan_slice_end = _restore_json_value(payload.get("scan_slice_end"))
    scan_before_start_time = _restore_json_value(payload.get("scan_before_start_time"))
    scan_before_id = _restore_json_value(payload.get("scan_before_id"))
    if scan_slice_end is not None and (
        not isinstance(scan_slice_end, datetime)
        or not window_start < scan_slice_end <= window_end
    ):
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    if (scan_before_start_time is None) != (scan_before_id is None):
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    if scan_before_start_time is not None and (
        not isinstance(scan_before_start_time, datetime)
        or not window_start <= scan_before_start_time < (scan_slice_end or window_end)
    ):
        raise ListCursorError("invalid_cursor", "The continuation cursor is invalid.")
    return ListCursor(
        window_start=window_start,
        window_end=window_end,
        order=order,
        total_rows=(
            int(payload["total_rows"])
            if payload.get("total_rows") is not None
            else None
        ),
        version_ceiling=payload["version_ceiling"],
        seen_rows=payload["seen_rows"],
        relation_version_ceilings=relation_version_ceilings,
        scan_slice_end=scan_slice_end,
        scan_before_start_time=scan_before_start_time,
        scan_before_id=scan_before_id,
    )


def snapshot_read_settings(
    base: dict[str, Any], *, builder: Any, version_ceiling: int
) -> dict[str, Any]:
    """Apply a raw-table version ceiling before latest-state aggregation."""

    if version_ceiling <= 0:
        raise ValueError("version_ceiling must be positive")
    table = str(getattr(builder, "TABLE", "spans"))
    is_v2 = ".v2." in builder.__class__.__module__
    version_column = getattr(
        builder,
        "SNAPSHOT_VERSION_COLUMN",
        "_version" if is_v2 else "_peerdb_version",
    )
    existing = dict(base.get("additional_table_filters") or {})
    existing[table] = f"{version_column} < {int(version_ceiling)}"
    return {**base, "additional_table_filters": existing}


_DIRECT_EPOCH_NANO_TABLES = frozenset({"spans", "tracer_eval_logger_v2", "traces"})
_PEERDB_INTEGER_TABLES = frozenset({"model_hub_score", "tracer_eval_logger"})
_DATETIME64_MICRO_TABLES = frozenset(
    {"end_user_id_remap", "end_users", "trace_session_id_remap"}
)
_LIST_SNAPSHOT_TABLES = (
    _DIRECT_EPOCH_NANO_TABLES | _PEERDB_INTEGER_TABLES | _DATETIME64_MICRO_TABLES
)


def relation_snapshot_read_settings(
    base: dict[str, Any], *, version_ceilings: dict[str, int]
) -> dict[str, Any]:
    """Rebuild strict per-table filters carried by a signed list cursor."""

    if not version_ceilings or "spans" not in version_ceilings:
        raise ValueError("list relation snapshot is incomplete")
    unknown = set(version_ceilings) - _LIST_SNAPSHOT_TABLES
    if unknown:
        raise ValueError("list relation snapshot contains an unsupported table")
    additional_filters = dict(base.get("additional_table_filters") or {})
    for table, raw_ceiling in version_ceilings.items():
        ceiling = int(raw_ceiling)
        if ceiling <= 0:
            raise ValueError("list relation snapshot ceiling must be positive")
        if table in _DIRECT_EPOCH_NANO_TABLES:
            predicate = f"_version < {ceiling}"
        elif table in _PEERDB_INTEGER_TABLES:
            predicate = f"_peerdb_version < {ceiling}"
        else:
            predicate = f"toUnixTimestamp64Micro(version) < {ceiling}"
        existing = additional_filters.get(table)
        if existing is not None and existing != predicate:
            raise ValueError("list relation snapshot settings conflict")
        additional_filters[table] = predicate
    return {**base, "additional_table_filters": additional_filters}


def capture_list_relation_snapshot(
    *,
    analytics: Any,
    builder: Any,
    base_settings: dict[str, Any],
    timeout_ms: int,
) -> tuple[dict[str, Any], dict[str, int]]:
    """Freeze every physical relation used by a bounded list classifier.

    The generated classifier is the source of truth for dependencies, avoiding
    a second hand-maintained mapping from public filter shapes to tables.  The
    dashboard snapshot helper is imported lazily to avoid its intentional
    dependency on the direct-write ceiling capture in this module.
    """

    if timeout_ms <= 0:
        raise ValueError("list snapshot timeout must be positive")
    query, _ = builder.build_filter_match_query(
        ["00000000-0000-0000-0000-000000000000"]
    )
    if not query:
        raise ValueError("list classifier snapshot query is unavailable")
    from tracer.services.clickhouse.dashboard_snapshot import (
        capture_dashboard_relation_snapshot,
    )

    snapshot = capture_dashboard_relation_snapshot(
        analytics=analytics,
        sql_statements=[query],
        base_settings=base_settings,
        timeout_ms=timeout_ms,
    )
    if "spans" not in snapshot.version_ceilings:
        raise ValueError("list classifier snapshot omitted spans")
    unknown = set(snapshot.version_ceilings) - _LIST_SNAPSHOT_TABLES
    if unknown:
        raise ValueError("list classifier uses an unsupported snapshot relation")
    return snapshot.settings, dict(snapshot.version_ceilings)


def snapshot_cursor_supported(filters: list[dict[str, Any]], *, resource: str) -> bool:
    """Whether the bounded compiler can freeze every list membership relation."""

    from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
        partition_span_filter_plans,
        partition_trace_filter_plans,
    )

    partition = {
        "observe_traces": partition_trace_filter_plans,
        "observe_spans": partition_span_filter_plans,
    }.get(resource)
    if partition is None:
        raise ValueError("unsupported cursor resource")
    try:
        partition(filters)
    except (TypeError, ValueError):
        return False
    return True


def cursor_requires_relation_snapshot(
    filters: list[dict[str, Any]], *, resource: str
) -> bool:
    """Return whether membership reads relations beyond direct-write spans."""

    from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
        partition_span_filter_plans,
        partition_trace_filter_plans,
    )

    partition = {
        "observe_traces": partition_trace_filter_plans,
        "observe_spans": partition_span_filter_plans,
    }.get(resource)
    if partition is None:
        raise ValueError("unsupported cursor resource")
    _, residual_filters = partition(filters)
    return bool(residual_filters)


def cursor_page_metadata(
    *,
    enabled: bool,
    has_more: bool,
    seen_rows: int,
    next_cursor: str | None,
    unseen_row_proven: bool = False,
) -> dict[str, Any]:
    """Build cursor totals, or no cursor contract for a legacy fallback page."""

    if not enabled:
        return {}
    if seen_rows < 0:
        raise ValueError("seen_rows must be non-negative")
    if has_more and not next_cursor:
        raise RuntimeError("cursor page with more rows requires a continuation token")
    return {
        # A scan checkpoint means more search space, not necessarily another
        # matching row. Add the sentinel only when the selector has already
        # classified an extra match beyond the published page.
        "total_rows": seen_rows + (1 if has_more and unseen_row_proven else 0),
        "total_rows_exact": None if has_more else seen_rows,
        "total_rows_is_lower_bound": has_more,
        "has_more": has_more,
        "next_cursor": next_cursor,
    }


def capture_snapshot_version_ceiling(analytics: Any, *, timeout_ms: int = 250) -> int:
    """Read CH server time in epoch-ns, matching spans._version defaults."""

    result = analytics.execute_ch_query(
        "SELECT toUnixTimestamp64Nano(now64(9, 'UTC')) AS version_ceiling",
        {},
        timeout_ms=timeout_ms,
        settings={"max_threads": 1, "max_result_rows": 1},
    )
    if not result.data:
        raise RuntimeError("ClickHouse did not return a snapshot ceiling")
    ceiling = int(result.data[0].get("version_ceiling", 0))
    if ceiling <= 0:
        raise RuntimeError("ClickHouse returned an invalid snapshot ceiling")
    return ceiling


def frozen_window_filter(cursor: ListCursor) -> dict[str, Any]:
    """Return an internal finite bound that preserves the first page snapshot."""

    return {
        "column_id": "start_time",
        "filter_config": {
            "filter_type": "datetime",
            "filter_op": "between",
            "filter_value": [cursor.window_start, cursor.window_end],
        },
    }


__all__ = [
    "ListCursor",
    "ListCursorError",
    "capture_snapshot_version_ceiling",
    "cursor_page_metadata",
    "cursor_scope_for_request",
    "decode_list_cursor",
    "encode_list_cursor",
    "frozen_window_filter",
    "normalize_cursor_query",
    "capture_list_relation_snapshot",
    "cursor_requires_relation_snapshot",
    "relation_snapshot_read_settings",
    "snapshot_cursor_supported",
    "snapshot_read_settings",
]
