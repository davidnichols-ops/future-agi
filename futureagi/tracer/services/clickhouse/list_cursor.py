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

CURSOR_VERSION = 1
CURSOR_SALT = "tracer.clickhouse-list-cursor.v1"
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
        if key in {"cursor", "cursor_mode", "page", "page_number"}:
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
    )


def snapshot_read_settings(
    base: dict[str, Any], *, builder: Any, version_ceiling: int
) -> dict[str, Any]:
    """Apply a raw-table version ceiling before latest-state aggregation."""

    if version_ceiling <= 0:
        raise ValueError("version_ceiling must be positive")
    table = str(getattr(builder, "TABLE", "spans"))
    is_v2 = ".v2." in builder.__class__.__module__
    version_column = "_version" if is_v2 else "_peerdb_version"
    existing = dict(base.get("additional_table_filters") or {})
    existing[table] = f"{version_column} < {int(version_ceiling)}"
    return {**base, "additional_table_filters": existing}


def snapshot_cursor_supported(filters: list[dict[str, Any]], *, resource: str) -> bool:
    """Whether list membership depends only on the versioned spans table.

    Relational eval, annotation, and end-user filters consult independently
    mutable ClickHouse relations. Until a cursor carries a ceiling for every
    one of those relations, they must retain legacy numbered pagination rather
    than claiming a cross-page snapshot that is not actually frozen.
    """

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
        _, residual_filters = partition(filters)
    except (TypeError, ValueError):
        return False
    return not residual_filters


def cursor_page_metadata(
    *, enabled: bool, has_more: bool, seen_rows: int, next_cursor: str | None
) -> dict[str, Any]:
    """Build cursor totals, or no cursor contract for a legacy fallback page."""

    if not enabled:
        return {}
    if seen_rows < 0:
        raise ValueError("seen_rows must be non-negative")
    if has_more and not next_cursor:
        raise RuntimeError("cursor page with more rows requires a continuation token")
    return {
        "total_rows": seen_rows + (1 if has_more else 0),
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
    "snapshot_cursor_supported",
    "snapshot_read_settings",
]
