import uuid

import pytest

from simulate.utils.baseline import TEXT, VOICE, resolve_baseline_id


@pytest.mark.unit
class TestResolveBaselineId:
    def test_returns_none_for_non_dict_metadata(self):
        assert resolve_baseline_id(None, is_replay=True) is None
        assert resolve_baseline_id("session-1", is_replay=True) is None
        assert resolve_baseline_id([], is_replay=True) is None

    def test_returns_none_for_empty_metadata(self):
        assert resolve_baseline_id({}, is_replay=True) is None

    def test_chat_resolves_the_session_id(self):
        session_id = str(uuid.uuid4())
        trace_id = str(uuid.uuid4())

        resolved = resolve_baseline_id(
            {"session_id": session_id, "trace_id": trace_id},
            is_replay=True,
            simulation_call_type=TEXT,
        )

        assert resolved == session_id

    def test_chat_ignores_a_voice_only_trace_id(self):
        resolved = resolve_baseline_id(
            {"trace_id": str(uuid.uuid4())},
            is_replay=True,
            simulation_call_type=TEXT,
        )

        assert resolved is None

    def test_voice_resolves_the_trace_id(self):
        session_id = str(uuid.uuid4())
        trace_id = str(uuid.uuid4())

        resolved = resolve_baseline_id(
            {"session_id": session_id, "trace_id": trace_id},
            is_replay=True,
            simulation_call_type=VOICE,
        )

        assert resolved == trace_id

    def test_voice_ignores_a_chat_only_session_id(self):
        resolved = resolve_baseline_id(
            {"session_id": str(uuid.uuid4())},
            is_replay=True,
            simulation_call_type=VOICE,
        )

        assert resolved is None

    @pytest.mark.parametrize("call_type", [TEXT, VOICE, None])
    def test_falls_back_to_the_intent_id_for_replay_rows(self, call_type):
        intent_id = str(uuid.uuid4())

        resolved = resolve_baseline_id(
            {"intent_id": intent_id},
            is_replay=True,
            simulation_call_type=call_type,
        )

        assert resolved == intent_id

    @pytest.mark.parametrize("call_type", [TEXT, VOICE, None])
    def test_never_falls_back_to_the_intent_id_outside_replay(self, call_type):
        resolved = resolve_baseline_id(
            {"intent_id": str(uuid.uuid4())},
            is_replay=False,
            simulation_call_type=call_type,
        )

        assert resolved is None

    @pytest.mark.parametrize(
        "synthetic_id", ["UC-01", "UC-42", "use-case-3", "", "not-a-uuid"]
    )
    def test_rejects_synthetic_intent_ids(self, synthetic_id):
        resolved = resolve_baseline_id(
            {"intent_id": synthetic_id},
            is_replay=True,
            simulation_call_type=TEXT,
        )

        assert resolved is None

    def test_rejects_non_uuid_session_and_trace_ids(self):
        metadata = {"session_id": "UC-07", "trace_id": "UC-08"}

        assert (
            resolve_baseline_id(metadata, is_replay=True, simulation_call_type=TEXT)
            is None
        )
        assert (
            resolve_baseline_id(metadata, is_replay=True, simulation_call_type=VOICE)
            is None
        )
        assert resolve_baseline_id(metadata, is_replay=True) is None

    def test_rejects_non_string_ids(self):
        assert (
            resolve_baseline_id(
                {"session_id": uuid.uuid4()},
                is_replay=True,
                simulation_call_type=TEXT,
            )
            is None
        )

    def test_unknown_modality_keeps_the_legacy_order(self):
        session_id = str(uuid.uuid4())
        trace_id = str(uuid.uuid4())

        assert (
            resolve_baseline_id(
                {"session_id": session_id, "trace_id": trace_id}, is_replay=True
            )
            == session_id
        )
        assert (
            resolve_baseline_id({"trace_id": trace_id}, is_replay=True) == trace_id
        )
