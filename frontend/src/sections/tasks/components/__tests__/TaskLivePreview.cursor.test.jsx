import React from "react";
import PropTypes from "prop-types";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { useForm } from "react-hook-form";
import { render, screen, waitFor } from "src/utils/test-utils";

const mocks = vi.hoisted(() => ({ get: vi.fn(), post: vi.fn() }));

vi.mock("src/utils/axios", () => ({
  default: { get: mocks.get, post: mocks.post },
  endpoints: {
    project: {
      getCallLogs: "/calls/",
      getTracesForObserveProject: () => "/traces/",
      getSpansForObserveProject: () => "/spans/",
      projectSessionList: () => "/sessions/",
      getTrace: (id) => `/traces/${id}/`,
      getVoiceCallDetail: "/calls/detail/",
      traceSession: "/sessions/",
    },
  },
}));
vi.mock("src/components/iconify", () => ({ default: () => null }));
vi.mock("src/components/tooltip/CustomTooltip", () => ({
  default: ({ children }) => children,
}));
vi.mock("src/sections/evals/components/DatasetTestMode", () => ({
  JsonValueTree: () => null,
}));
vi.mock("src/sections/evals/components/EvalResultDisplay", () => ({
  default: () => null,
}));
vi.mock("src/sections/evals/components/SpanRowList", () => ({
  default: () => null,
}));
vi.mock("src/components/inline-audio/inline-row-audio", () => ({
  InlineAudio: () => null,
  RecordingGroup: () => null,
}));

import TaskLivePreview from "../TaskLivePreview";

const PROJECT_ID = "00000000-0000-4000-8000-000000000902";

function PreviewHarness({ rowType = "spans" }) {
  const { control } = useForm({
    defaultValues: {
      filters: [],
      startDate: null,
      endDate: null,
      evalsDetails: [],
      rowType,
    },
  });
  return <TaskLivePreview control={control} projectId={PROJECT_ID} />;
}

PreviewHarness.propTypes = { rowType: PropTypes.string };

describe("TaskLivePreview sparse cursor continuation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("does not publish a false empty preview at the browser hop bound", async () => {
    let spanListCalls = 0;
    mocks.get.mockImplementation(async (url) => {
      if (url === "/spans/") {
        const callIndex = spanListCalls;
        spanListCalls += 1;
        if (callIndex < 13) {
          return {
            data: {
              result: {
                config: [],
                table: [],
                metadata: {
                  has_more: true,
                  next_cursor: `checkpoint-${callIndex}`,
                  total_rows_is_lower_bound: true,
                },
              },
            },
          };
        }
        return {
          data: {
            result: {
              config: [],
              table: [
                {
                  span_id: "span-rare",
                  trace_id: "trace-rare",
                  input: "rare preview value",
                },
              ],
              metadata: {
                has_more: false,
                next_cursor: null,
                total_rows: 1,
              },
            },
          },
        };
      }
      if (url === "/traces/trace-rare/") {
        return {
          data: {
            result: {
              trace: { trace_id: "trace-rare" },
              observation_spans: [
                {
                  observation_span: {
                    id: "span-rare",
                    input: "rare preview value",
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
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <PreviewHarness />
      </QueryClientProvider>,
    );

    await screen.findByText("Row 1 of 1");

    const spanRequests = mocks.get.mock.calls.filter(
      ([url]) => url === "/spans/",
    );
    expect(spanRequests).toHaveLength(14);
    expect(spanRequests[13][1].params).toEqual(
      expect.objectContaining({
        cursor_mode: true,
        cursor: "checkpoint-12",
      }),
    );
    await waitFor(() =>
      expect(screen.queryByText("No matching rows")).not.toBeInTheDocument(),
    );
  });

  it("resumes a sparse voice-call preview with the same signed cursor", async () => {
    let listCalls = 0;
    mocks.get.mockImplementation(async (url) => {
      if (url === "/calls/") {
        const callIndex = listCalls;
        listCalls += 1;
        if (callIndex < 13) {
          return {
            data: {
              result: {
                results: [],
                has_more: true,
                next_cursor: `voice-checkpoint-${callIndex}`,
              },
            },
          };
        }
        return {
          data: {
            result: {
              results: [{ id: "call-rare", trace_id: "trace-voice-rare" }],
              has_more: false,
              next_cursor: null,
              count: 1,
            },
          },
        };
      }
      if (url === "/calls/detail/") {
        return { data: { result: { status: "completed" } } };
      }
      throw new Error(`Unexpected GET ${url}`);
    });
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <PreviewHarness rowType="voiceCalls" />
      </QueryClientProvider>,
    );

    await screen.findByText("Row 1 of 1");

    const listRequests = mocks.get.mock.calls.filter(
      ([url]) => url === "/calls/",
    );
    expect(listRequests).toHaveLength(14);
    expect(listRequests[13][1].params).toEqual(
      expect.objectContaining({
        cursor_mode: true,
        cursor: "voice-checkpoint-12",
      }),
    );
  });
});
