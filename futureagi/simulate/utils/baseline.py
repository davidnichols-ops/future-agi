"""Baseline-id resolution for the drawer's compare-with-baseline view."""

import uuid

# Wire values of CallExecution.SimulationCallType, inlined so this module stays
# import-free (simulate.models already reaches into utils; importing back would
# cycle).
TEXT = "text"
VOICE = "voice"


def _as_uuid(value):
    """Return ``value`` only when it is a real UUID string, else None.

    Every baseline is addressed by a session or trace UUID. Generated scenarios
    that were NOT seeded from transcripts carry synthetic ``UC-XX`` intent ids,
    and the ``intent_id`` fallback below would otherwise surface one of those as
    a baseline. Downstream that matches no traces and renders an empty
    comparison as a successful one, so reject it here — a hidden button beats a
    blank baseline.
    """
    if not isinstance(value, str):
        return None
    try:
        uuid.UUID(value)
    except ValueError:
        return None
    return value


def resolve_baseline_id(row_metadata, *, is_replay, simulation_call_type=None):
    """Pick the baseline session/trace id from a Row's metadata.

    Chat replays are addressed by ``session_id``, voice replays by ``trace_id``.
    Neither key is written today (``dataset_persister`` persists only
    ``intent_id``), so replay-derived rows resolve through the ``intent_id``
    fallback: for a transcript-seeded scenario that value IS the session or
    trace id, because intent extraction keys its dict off the transcript map.

    ``simulation_call_type`` scopes the lookup to the keys the comparison
    pipeline for that modality can consume. The pipelines are not
    interchangeable: chat resolves a session into Postgres ``Trace`` rows, voice
    resolves a trace into a ClickHouse conversation span. Omitting it keeps the
    legacy modality-blind order, correct only where the modality is unknown.

    Every candidate is UUID-checked, so a ``UC-XX`` intent id never escapes.
    """
    if not isinstance(row_metadata, dict):
        return None

    session_id = _as_uuid(row_metadata.get("session_id"))
    trace_id = _as_uuid(row_metadata.get("trace_id"))
    intent_id = _as_uuid(row_metadata.get("intent_id")) if is_replay else None

    if simulation_call_type == TEXT:
        return session_id or intent_id
    if simulation_call_type == VOICE:
        return trace_id or intent_id
    return session_id or trace_id or intent_id
