import { useCallback, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import alkAxios from "./client";
import { ALK_KEYS } from "./alEnvironment";
import { streamHarness } from "./streamHarness";

/**
 * Turn one stream event into a transcript entry, or null for the control events.
 *
 * `status` and `done` drive the page rather than the conversation: they say the turn is
 * over and everything should resync, which is not something to render as a message.
 */
const asMessage = (event) => {
  switch (event.kind) {
    case "text":
      return { role: "tester", text: event.text };
    case "tool":
      return { tool: event.tool, detail: event.detail };
    case "result":
      // `is_error` is how the harness says a tool refused. Without reading it a refusal
      // arrives in the thread wearing a green tick, which is the one thing a reader of a
      // running turn must not be told wrongly.
      return {
        tool: event.tool || "result",
        text: event.text,
        ok: !event.detail?.is_error,
        detail: event.detail,
      };
    case "exchange":
      return { role: event.detail?.speaker || "tester", text: event.text };
    case "result_card":
      return { role: "verdict", text: event.text, detail: event.detail };
    case "artifact":
      // A signal to refresh the artifact tabs, not something to say in the thread.
      return null;
    default:
      return null;
  }
};

/** What the strip says while a turn runs, following whatever the stream is doing. */
const labelFor = (event) => {
  if (event.kind === "tool") return event.detail?.label || event.tool;
  if (event.kind === "exchange") return "the conversation is running";
  if (event.kind === "result_card") return "grading";
  if (event.kind === "text" || event.kind === "result") return "working";
  return null;
};

/** Everything the tabs and header read, refreshed once a turn finishes. */
const TOUCHED_BY_A_TURN = [
  ALK_KEYS.status,
  ALK_KEYS.history,
  ALK_KEYS.contract,
  ALK_KEYS.world,
  ALK_KEYS.scenarios,
  ALK_KEYS.simulations,
  ALK_KEYS.subgoals,
  ALK_KEYS.runs,
];

/**
 * Holds the turn that is happening right now. Stored history comes from React Query; this
 * is only the part that is still arriving, so the two are concatenated for display.
 */
export const useAlkConversation = () => {
  const queryClient = useQueryClient();
  const [live, setLive] = useState([]);
  const [streaming, setStreaming] = useState(false);
  const [thinking, setThinking] = useState("");
  const [error, setError] = useState("");
  const abortRef = useRef(null);

  const append = useCallback((message) => {
    if (message) setLive((all) => [...all, message]);
  }, []);

  const drive = useCallback(
    async (path, body, opening) => {
      setError("");
      setStreaming(true);
      setThinking(path.endsWith("/run") ? "running the scenarios" : "thinking");
      if (opening) append(opening);

      const controller = new AbortController();
      abortRef.current = controller;

      try {
        await streamHarness({
          path,
          body,
          signal: controller.signal,
          onEvent: (event) => {
            const said = labelFor(event);
            if (said) setThinking(said);
            append(asMessage(event));
          },
        });
        TOUCHED_BY_A_TURN.forEach((queryKey) => queryClient.invalidateQueries({ queryKey }));
      } catch (failed) {
        // Pressing Stop aborts the reader on purpose. That is not something to report.
        const stopped =
          failed?.name === "AbortError" || /aborted/i.test(failed?.message || "");
        if (!stopped) {
          // A refusal is an ordinary outcome — the harness takes one request at a time — so
          // it belongs in the thread, where the turn it interrupted is.
          const said = failed?.message || "The harness could not be reached.";
          setError(said);
          append({ role: "error", text: said });
        }
      } finally {
        setStreaming(false);
        setThinking("");
        abortRef.current = null;
      }
    },
    [append, queryClient]
  );

  const say = useCallback(
    (text) => drive("/api/say", { text }, { role: "you", text }),
    [drive]
  );

  /** An empty string runs every scenario; names separated by spaces run only those. */
  const runScenarios = useCallback((names = "") => drive("/api/run", { text: names }, null), [drive]);

  const stop = useCallback(async () => {
    abortRef.current?.abort();
    try {
      await alkAxios.post("/api/stop");
    } catch {
      // Stopping is best-effort: if the turn already ended there is nothing to interrupt.
    }
  }, []);

  /** Live messages are folded into stored history once the turn is saved server-side. */
  const clearLive = useCallback(() => setLive([]), []);

  return { live, streaming, thinking, error, say, runScenarios, stop, clearLive };
};
