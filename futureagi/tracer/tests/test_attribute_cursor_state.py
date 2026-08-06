"""Regression contracts for exhaustive attribute picker continuations."""

from __future__ import annotations

import hashlib
from math import ceil

import pytest
from django.core.cache import cache

from tracer.services.clickhouse.attribute_cursor_state import (
    ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE,
    AttributeCursorSeenState,
    AttributeCursorStateError,
    load_attribute_cursor_seen_state,
    persist_attribute_cursor_seen_state,
)

RESOURCE = "attribute-cursor-test"
BINDING = {"project_id": "project-a", "query": "final_status"}


def _digest(index: int) -> str:
    return hashlib.md5(f"value-{index}".encode(), usedforsecurity=False).hexdigest()


def _valid(value: str) -> bool:
    return len(value) == 32 and all(char in "0123456789abcdef" for char in value)


@pytest.fixture(autouse=True)
def _empty_cache():
    cache.clear()
    yield
    cache.clear()


def test_cursor_state_is_fixed_width_and_stores_each_digest_once(monkeypatch):
    stored_nodes = []
    original_add = cache.add

    def recording_add(key, value, timeout=None, version=None):
        stored_nodes.append(value)
        return original_add(key, value, timeout=timeout, version=version)

    monkeypatch.setattr(cache, "add", recording_add)
    values = tuple(_digest(index) for index in range(149))

    reference = persist_attribute_cursor_seen_state(
        AttributeCursorSeenState((), None),
        values,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )

    assert reference[0] == "state"
    assert len(reference[1]) == 64
    assert len(stored_nodes) == ceil(len(values) / ATTRIBUTE_CURSOR_STATE_CHUNK_SIZE)
    assert tuple(digest for node in stored_nodes for digest in node["chunk"]) == values
    assert sum(len(node["chunk"]) for node in stored_nodes) == len(values)
    assert load_attribute_cursor_seen_state(
        reference,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    ).digests == values


def test_cursor_state_append_is_immutable_and_retry_is_idempotent():
    first_values = tuple(_digest(index) for index in range(70))
    first_reference = persist_attribute_cursor_seen_state(
        AttributeCursorSeenState((), None),
        first_values,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )
    first = load_attribute_cursor_seen_state(
        first_reference,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )
    appended = tuple(_digest(index) for index in range(70, 91))

    second_reference = persist_attribute_cursor_seen_state(
        first,
        appended,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )
    retry_reference = persist_attribute_cursor_seen_state(
        first,
        appended,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )

    assert retry_reference == second_reference
    assert first_reference != second_reference
    assert load_attribute_cursor_seen_state(
        first_reference,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    ).digests == first_values
    assert load_attribute_cursor_seen_state(
        second_reference,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    ).digests == (*first_values, *appended)


def test_legacy_inline_state_is_accepted_then_migrated():
    legacy = tuple(_digest(index) for index in range(12))
    loaded = load_attribute_cursor_seen_state(
        legacy,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )

    reference = persist_attribute_cursor_seen_state(
        loaded,
        (_digest(12),),
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )

    assert loaded == AttributeCursorSeenState(legacy, None)
    assert reference[0] == "state"
    assert load_attribute_cursor_seen_state(
        reference,
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    ).digests == (*legacy, _digest(12))


def test_state_loss_binding_mismatch_and_ttl_renewal_failure_fail_closed(
    monkeypatch,
):
    reference = persist_attribute_cursor_seen_state(
        AttributeCursorSeenState((), None),
        (_digest(1),),
        resource=RESOURCE,
        binding=BINDING,
        validate_digest=_valid,
    )

    with pytest.raises(AttributeCursorStateError, match="invalid") as mismatch:
        load_attribute_cursor_seen_state(
            reference,
            resource=RESOURCE,
            binding={**BINDING, "project_id": "project-b"},
            validate_digest=_valid,
        )
    assert mismatch.value.code == "invalid_cursor"

    monkeypatch.setattr(cache, "touch", lambda *_args, **_kwargs: False)
    with pytest.raises(AttributeCursorStateError, match="expired") as renewal:
        load_attribute_cursor_seen_state(
            reference,
            resource=RESOURCE,
            binding=BINDING,
            validate_digest=_valid,
        )
    assert renewal.value.code == "expired_cursor"

    cache.clear()
    with pytest.raises(AttributeCursorStateError, match="expired") as missing:
        load_attribute_cursor_seen_state(
            reference,
            resource=RESOURCE,
            binding=BINDING,
            validate_digest=_valid,
        )
    assert missing.value.code == "expired_cursor"

