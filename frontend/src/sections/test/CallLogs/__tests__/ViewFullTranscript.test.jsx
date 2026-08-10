import { beforeAll, describe, expect, it, vi } from "vitest";
import { render, screen } from "src/utils/test-utils";
import ViewFullTranscript from "../ViewFullTranscript";

// jsdom doesn't implement Element.prototype.scrollTo; TranscriptView calls it
// on mount to bring the active row into view.
beforeAll(() => {
  if (!Element.prototype.scrollTo) {
    Element.prototype.scrollTo = vi.fn();
  }
});

// Rows below mirror what `CallExecutionDetailSerializer.get_transcript`
// actually emits (futureagi/simulate/serializers/test_execution.py:646). The
// two branches have completely different shapes under the same `transcript`
// key, which is what made this dialog fragile:
//
//   chat  (simulation_call_type == "text")  -> ChatMessageSerializer
//     { id, role, messages: list[str], content: list[dict], created_at, ... }
//   voice (simulation_call_type == "voice") -> CallTranscriptSerializer
//     { id, speaker_role, content: str, start_time_seconds, end_time_seconds, ... }
const CHAT_ROWS = [
  {
    id: "chat-1",
    role: "assistant",
    messages: ["Hi, how can I help you today?"],
    content: [{ role: "assistant", content: "Hi, how can I help you today?" }],
    session_id: "sess-1",
    tool_calls: [],
    created_at: "2026-08-10T10:00:00Z",
  },
  {
    id: "chat-2",
    role: "user",
    messages: ["I need to reset my password."],
    content: [{ role: "user", content: "I need to reset my password." }],
    session_id: "sess-1",
    tool_calls: [],
    created_at: "2026-08-10T10:00:12Z",
  },
];

const VOICE_ROWS = [
  {
    id: "voice-1",
    speaker_role: "assistant",
    content: "Thanks for calling, how can I help?",
    start_time_ms: 0,
    start_time_seconds: 0,
    end_time_ms: 2500,
    end_time_seconds: 2.5,
    confidence_score: 0.98,
    created_at: "2026-08-10T10:00:00Z",
  },
  {
    id: "voice-2",
    speaker_role: "user",
    content: "I would like to check my order.",
    start_time_ms: 3000,
    start_time_seconds: 3,
    end_time_ms: 5200,
    end_time_seconds: 5.2,
    confidence_score: 0.95,
    created_at: "2026-08-10T10:00:03Z",
  },
];

const noop = () => {};

describe("ViewFullTranscript", () => {
  it("renders chat rows, reading the turn body from messages[0]", () => {
    render(
      <ViewFullTranscript
        open
        onClose={noop}
        transcript={CHAT_ROWS}
        simulationCallType="text"
      />,
    );

    expect(screen.getByText("Hi, how can I help you today?")).toBeInTheDocument();
    expect(screen.getByText("I need to reset my password.")).toBeInTheDocument();
    // The raw `content` list-of-dicts must never leak through as JSON.
    expect(screen.queryByText(/\{"role"/)).not.toBeInTheDocument();
  });

  it("renders voice rows, reading the turn body from the content string", () => {
    render(
      <ViewFullTranscript
        open
        onClose={noop}
        transcript={VOICE_ROWS}
        simulationCallType="voice"
      />,
    );

    expect(
      screen.getByText("Thanks for calling, how can I help?"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("I would like to check my order."),
    ).toBeInTheDocument();
  });

  it("shows the empty state instead of crashing when transcript is missing", () => {
    expect(() =>
      render(
        <ViewFullTranscript open onClose={noop} simulationCallType="text" />,
      ),
    ).not.toThrow();

    expect(screen.getByText("No transcript available")).toBeInTheDocument();
  });
});
