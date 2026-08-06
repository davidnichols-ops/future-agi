"""Immutable server-side de-duplication state for attribute browse cursors.

Attribute keys and values are discovered from a newest-first physical span
walk.  A continuation therefore needs both a physical checkpoint and the set
of already-published logical values.  Copying that set into every signed URL
eventually exceeds proxy request-line limits; copying the complete set into a
new cache value on every page also has quadratic storage cost.

This module stores an immutable linked list of small digest chunks.  Each
digest is persisted once, cursor tokens carry only the latest node id, and a
retry of the same page deterministically resolves to the same node.  Cache
misses, binding mismatches and failed TTL renewal all fail closed: losing
de-duplication state must never silently re-publish duplicate options.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.core.cache import cache

ATTRIBUTE_CURSOR_STATE_VERSION = 1
ATTRIBUTE_CURSOR_STATE_TTL_SECONDS = 24 * 60 * 60
ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE = 64
# Existing deployed cursors embed a tuple of digests.  Keep accepting those
# during a rolling deploy; the next continuation migrates them into immutable
# server-side chunks.
ATTRIBUTE_CURSOR_LEGACY_INLINE_LIMIT = 224
_CACHE_PREFIX = "attribute-cursor-state"


class AttributeCursorStateError(ValueError):
    """A continuation's required server-side state is invalid or unavailable."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class AttributeCursorSeenState:
    """Fully resolved exact de-duplication state for one continuation."""

    digests: tuple[str, ...]
    state_id: str | None


def _ttl_seconds() -> int:
    return max(
        60,
        int(
            getattr(
                settings,
                "ATTRIBUTE_CURSOR_STATE_TTL_SECONDS",
                ATTRIBUTE_CURSOR_STATE_TTL_SECONDS,
            )
        ),
    )


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=str,
    )


def attribute_cursor_binding_digest(
    *, resource: str, binding: Any
) -> str:
    """Return the tenant/query binding persisted in every immutable node."""

    if not resource:
        raise ValueError("attribute cursor resource is required")
    return hashlib.sha256(
        _canonical({"resource": resource, "binding": binding}).encode("utf-8")
    ).hexdigest()


def _cache_key(state_id: str) -> str:
    return f"{_CACHE_PREFIX}:v{ATTRIBUTE_CURSOR_STATE_VERSION}:{state_id}"


def _validate_digest_tuple(
    values: Iterable[Any], validate_digest: Callable[[str], bool]
) -> tuple[str, ...]:
    normalized = tuple(str(value) for value in values)
    if len(set(normalized)) != len(normalized) or any(
        not validate_digest(value) for value in normalized
    ):
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )
    return normalized


def _touch_or_fail(key: str) -> None:
    try:
        touched = cache.touch(key, timeout=_ttl_seconds())
    except Exception as exc:
        raise AttributeCursorStateError(
            "expired_cursor",
            "The continuation cursor has expired. Please restart the search.",
        ) from exc
    if touched is not True:
        raise AttributeCursorStateError(
            "expired_cursor",
            "The continuation cursor has expired. Please restart the search.",
        )


def load_attribute_cursor_seen_state(
    reference: Any,
    *,
    resource: str,
    binding: Any,
    validate_digest: Callable[[str], bool],
) -> AttributeCursorSeenState:
    """Resolve a state reference, accepting the deployed inline tuple format.

    The linked list is validated from leaf to root and then reversed, retaining
    first-publication order.  Every live node is renewed together so a cursor
    cannot appear valid while an older required chunk is about to expire.
    """

    if reference in (None, (), []):
        return AttributeCursorSeenState((), None)
    if isinstance(reference, tuple):
        if len(reference) == 2 and reference[0] == "state":
            state_id = reference[1]
        else:
            # Legacy inline digest tuple from the previous release.
            if len(reference) > ATTRIBUTE_CURSOR_LEGACY_INLINE_LIMIT:
                raise AttributeCursorStateError(
                    "invalid_cursor", "The continuation cursor is invalid."
                )
            return AttributeCursorSeenState(
                _validate_digest_tuple(reference, validate_digest), None
            )
    elif isinstance(reference, list):
        if len(reference) == 2 and reference[0] == "state":
            state_id = reference[1]
        else:
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
    else:
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )
    if not isinstance(state_id, str) or len(state_id) != 64:
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )

    binding_digest = attribute_cursor_binding_digest(
        resource=resource, binding=binding
    )
    nodes: list[tuple[str, tuple[str, ...]]] = []
    visited: set[str] = set()
    leaf_count: int | None = None
    remaining_count: int | None = None
    current: str | None = state_id
    while current is not None:
        if current in visited:
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
        visited.add(current)
        key = _cache_key(current)
        try:
            stored = cache.get(key)
        except Exception as exc:
            raise AttributeCursorStateError(
                "expired_cursor",
                "The continuation cursor has expired. Please restart the search.",
            ) from exc
        if not isinstance(stored, dict):
            raise AttributeCursorStateError(
                "expired_cursor",
                "The continuation cursor has expired. Please restart the search.",
            )
        if (
            stored.get("v") != ATTRIBUTE_CURSOR_STATE_VERSION
            or stored.get("resource") != resource
            or stored.get("binding") != binding_digest
            or stored.get("id") != current
        ):
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
        chunk = _validate_digest_tuple(stored.get("chunk") or (), validate_digest)
        if not 1 <= len(chunk) <= ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE:
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
        try:
            count = int(stored["count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            ) from exc
        if leaf_count is None:
            leaf_count = count
            remaining_count = count
        if remaining_count is None or count != remaining_count:
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
        remaining_count -= len(chunk)
        parent = stored.get("parent")
        if parent is not None and (
            not isinstance(parent, str) or len(parent) != 64
        ):
            raise AttributeCursorStateError(
                "invalid_cursor", "The continuation cursor is invalid."
            )
        nodes.append((key, chunk))
        current = parent

    assert leaf_count is not None
    if remaining_count != 0:
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )
    digests = tuple(
        digest for _key, chunk in reversed(nodes) for digest in chunk
    )
    if len(digests) != leaf_count or len(set(digests)) != len(digests):
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )
    # Renew only after the entire chain has been proven internally consistent.
    for key, _chunk in nodes:
        _touch_or_fail(key)
    return AttributeCursorSeenState(digests, state_id)


def persist_attribute_cursor_seen_state(
    prior: AttributeCursorSeenState,
    appended: Iterable[Any],
    *,
    resource: str,
    binding: Any,
    validate_digest: Callable[[str], bool],
) -> tuple[str, str] | tuple[()]:
    """Append new digests as immutable chunks and return a compact reference."""

    new_digests = _validate_digest_tuple(appended, validate_digest)
    prior_digests = set(prior.digests)
    if any(value in prior_digests for value in new_digests):
        raise AttributeCursorStateError(
            "invalid_cursor", "The continuation cursor is invalid."
        )
    all_values = (*prior.digests, *new_digests)
    if not all_values:
        return ()

    binding_digest = attribute_cursor_binding_digest(
        resource=resource, binding=binding
    )
    parent = prior.state_id
    count = len(prior.digests) if prior.state_id is not None else 0

    # Migrate a legacy inline cursor once, in fixed-size chunks.  A retry uses
    # the same deterministic ids and cache.add verifies the existing content.
    values_to_store = (
        (*prior.digests, *new_digests)
        if prior.state_id is None
        else new_digests
    )
    for offset in range(0, len(values_to_store), ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE):
        chunk = tuple(
            values_to_store[offset : offset + ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE]
        )
        count += len(chunk)
        canonical_node = {
            "v": ATTRIBUTE_CURSOR_STATE_VERSION,
            "resource": resource,
            "binding": binding_digest,
            "parent": parent,
            "chunk": chunk,
            "count": count,
        }
        state_id = hashlib.sha256(
            _canonical(canonical_node).encode("utf-8")
        ).hexdigest()
        stored = {**canonical_node, "id": state_id}
        key = _cache_key(state_id)
        try:
            created = cache.add(key, stored, timeout=_ttl_seconds())
            if not created and cache.get(key) != stored:
                raise AttributeCursorStateError(
                    "invalid_cursor", "The continuation cursor is invalid."
                )
        except AttributeCursorStateError:
            raise
        except Exception as exc:
            raise AttributeCursorStateError(
                "cursor_state_unavailable",
                "A continuation could not be created. Please retry.",
            ) from exc
        parent = state_id

    # No appended values with an already server-backed state simply reuses the
    # leaf; it was renewed by the load path.
    if parent is None:
        raise AttributeCursorStateError(
            "cursor_state_unavailable",
            "A continuation could not be created. Please retry.",
        )
    return ("state", parent)


__all__ = [
    "ATTRIBUTE_CURSOR_LEGACY_INLINE_LIMIT",
    "ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE",
    "ATTRIBUTE_CURSOR_STATE_TTL_SECONDS",
    "AttributeCursorSeenState",
    "AttributeCursorStateError",
    "attribute_cursor_binding_digest",
    "load_attribute_cursor_seen_state",
    "persist_attribute_cursor_seen_state",
]
