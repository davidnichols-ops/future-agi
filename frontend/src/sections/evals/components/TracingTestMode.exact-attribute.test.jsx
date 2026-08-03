import React from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, userEvent, waitFor } from "src/utils/test-utils";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
  exactFields: ["final_status"],
  exactReadState: "complete",
}));

vi.mock("src/utils/axios", () => ({
  default: { get: mocks.get, post: mocks.post },
  endpoints: {
    project: {
      getProjectById: (id) => `/projects/${id}`,
      getSpansForObserveProject: () => "/spans/",
      getTracesForObserveProject: () => "/traces/",
      projectSessionList: () => "/sessions/",
      getTrace: (id) => `/traces/${id}/`,
      listProjects: () => "/projects/",
      traceSession: "/sessions/",
      getCallLogs: "/calls/",
      getVoiceCallDetail: "/calls/detail/",
      getEvalAttributeList: () => "/eval-attributes/",
    },
    develop: {
      eval: {
        evalPlayground: "/eval-playground/",
        executeCompositeEval: (id) => `/composite/${id}/execute/`,
        executeCompositeEvalAdhoc: "/composite/execute/",
      },
    },
  },
}));

vi.mock("./useExactEvalAttributeFields", async (importOriginal) => ({
  ...(await importOriginal()),
  useExactEvalAttributeFields: () => ({
    data: mocks.exactFields,
    queryReadState: mocks.exactReadState,
    isFetching: false,
  }),
}));

vi.mock("src/sections/tasks/components/TaskLivePreview", () => ({
  buildApiFilterArray: () => [],
}));
vi.mock("src/sections/tasks/components/TaskFilterBar", () => ({
  default: () => null,
}));
vi.mock("./DatasetTestMode", () => ({ JsonValueTree: () => null }));
vi.mock("./SpanRowList", () => ({ default: () => null }));
vi.mock("./EvalResultDisplay", () => ({ default: () => null }));
vi.mock("src/components/draggable-col-resizer", () => ({
  default: () => null,
}));
vi.mock("src/components/iconify", () => ({ default: () => null }));
vi.mock("src/components/tooltip", () => ({
  default: ({ children }) => children,
}));
vi.mock("src/components/inline-audio/inline-row-audio", () => ({
  InlineAudio: () => null,
  RecordingGroup: () => null,
}));
vi.mock("../hooks/useErrorLocalizerPoll", () => ({
  default: () => ({
    state: { status: null, details: null, message: null },
    start: vi.fn(),
  }),
}));
vi.mock("../hooks/useCompositeEval", () => ({
  useExecuteCompositeEvalAdhoc: () => ({ mutateAsync: vi.fn() }),
}));

import TracingTestMode from "./TracingTestMode";

const PROJECT_ID = "00000000-0000-4000-8000-000000000901";

function renderTaskMapping(onReadyChange) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <TracingTestMode
        templateId="eval-template-1"
        variables={["evaluation_result"]}
        initialProjectId={PROJECT_ID}
        initialRowType="spans"
        allowCustomFieldPath
        onReadyChange={onReadyChange}
      />
    </QueryClientProvider>,
  );
}

describe("TracingTestMode exact task attribute mapping", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.exactFields = ["final_status"];
    mocks.exactReadState = "complete";
    mocks.get.mockImplementation(async (url) => {
      if (url === `/projects/${PROJECT_ID}`) {
        return { data: { result: { id: PROJECT_ID, source: "api" } } };
      }
      if (url === "/spans/") {
        return {
          data: {
            result: {
              config: [],
              table: [
                {
                  span_id: "span-1",
                  trace_id: "trace-1",
                  input: "generic preview value",
                },
              ],
              metadata: { total_rows: 1 },
            },
          },
        };
      }
      if (url === "/traces/trace-1/") {
        return {
          data: {
            result: {
              trace: { trace_id: "trace-1" },
              observation_spans: [
                {
                  observation_span: {
                    id: "span-1",
                    input: "generic preview value",
                    span_attributes: { input: "generic preview value" },
                  },
                  children: [],
                },
              ],
            },
          },
        };
      }
      throw new Error(`Unexpected GET ${url}`);
    });
  });

  it("lists and selects final_status even when the preview row omits it", async () => {
    const onReadyChange = vi.fn();
    renderTaskMapping(onReadyChange);

    const input = await screen.findByPlaceholderText(
      "Search or type a path (e.g. attributes.input.value)",
    );
    await waitFor(() => expect(input).not.toBeDisabled());

    await userEvent.click(input);
    await userEvent.type(input, "final_status");
    const option = await screen.findByRole("option", { name: "final_status" });
    await userEvent.click(option);

    expect(input).toHaveValue("final_status");
    await waitFor(() =>
      expect(onReadyChange).toHaveBeenCalledWith(true, {
        evaluation_result: "final_status",
      }),
    );
  });

  it.each([
    ["degraded", "Results are incomplete. Please retry in a moment."],
    ["error", "We couldn't load this data. Please retry in a moment."],
  ])(
    "shows a sanitized %s warning instead of treating no exact fields as authoritative",
    async (readState, message) => {
      mocks.exactFields = [];
      mocks.exactReadState = readState;
      renderTaskMapping(vi.fn());

      const input = await screen.findByPlaceholderText(
        "Search or type a path (e.g. attributes.input.value)",
      );
      await waitFor(() => expect(input).not.toBeDisabled());
      await userEvent.type(input, "final_status");

      expect(await screen.findByText(message)).toBeInTheDocument();
    },
  );

  it("keeps a bounded exact suggestion selectable while warning that the read is degraded", async () => {
    mocks.exactFields = ["final_status"];
    mocks.exactReadState = "degraded";
    const onReadyChange = vi.fn();
    renderTaskMapping(onReadyChange);

    const input = await screen.findByPlaceholderText(
      "Search or type a path (e.g. attributes.input.value)",
    );
    await waitFor(() => expect(input).not.toBeDisabled());
    await userEvent.click(input);
    await userEvent.type(input, "final_status");

    expect(
      await screen.findByText(
        "Results are incomplete. Please retry in a moment.",
      ),
    ).toBeInTheDocument();
    await userEvent.click(
      await screen.findByRole("option", { name: "final_status" }),
    );

    expect(input).toHaveValue("final_status");
    await waitFor(() =>
      expect(onReadyChange).toHaveBeenCalledWith(true, {
        evaluation_result: "final_status",
      }),
    );
  });
});
