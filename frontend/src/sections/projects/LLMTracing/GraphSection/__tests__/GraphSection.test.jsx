import React from "react";
import { fireEvent, render, screen, waitFor } from "src/utils/test-utils";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import axios from "src/utils/axios";
import GraphSection from "../GraphSection";

vi.mock("react-apexcharts", () => ({
  default: () => <div data-testid="apex-chart" />,
}));

vi.mock("../LeftControl", () => ({
  default: ({ onGraphConfigChange }) => (
    <button
      type="button"
      onClick={() =>
        onGraphConfigChange({
          id: "latency",
          type: "SYSTEM_METRIC",
        })
      }
    >
      Select latency
    </button>
  ),
}));

vi.mock("../RightControl", () => ({ default: () => null }));
vi.mock("../Legend", () => ({ default: () => null }));
vi.mock("../GraphSkeleton", () => ({ default: () => null }));
vi.mock("src/assets/illustrations/empty-graph", () => ({
  default: () => null,
}));
vi.mock("src/components/svg-color", () => ({ default: () => null }));
vi.mock("src/components/show", () => ({
  ShowComponent: ({ condition, children }) => (condition ? children : null),
}));
vi.mock("src/sections/projects/LLMTracing/states", () => ({
  useLLMTracingStoreShallow: (selector) =>
    selector({
      primaryCollapsed: false,
      setPrimaryCollapsed: vi.fn(),
    }),
}));
vi.mock("react-router", async (importOriginal) => {
  const original = await importOriginal();
  return { ...original, useParams: () => ({ observeId: "project-1" }) };
});
vi.mock("src/utils/axios", () => ({
  default: { post: vi.fn() },
  endpoints: {
    project: {
      getTraceGraphData: () => "/tracer/trace/get_graph_methods/",
      getSpanGraphData: () => "/tracer/observation-span/get_graph_methods/",
    },
  },
}));

function renderGraph() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <GraphSection
        selectedTab="trace"
        filters={[]}
        showCompare={false}
        selectedGraphProperty="latency"
        selectedGraphEvals={[]}
        setSelectedGraphEvals={vi.fn()}
        setSelectedGraphProperty={vi.fn()}
        selectedGraphAttributes={{}}
        setSelectedGraphAttributes={vi.fn()}
        compareType="primary"
        dateFilter={{
          dateFilter: ["2026-08-02T00:00:00Z", "2026-08-03T00:00:00Z"],
          dateOption: "Custom",
        }}
        setDateFilter={vi.fn()}
        selectedInterval="hour"
        setSelectedInterval={vi.fn()}
        lineColor="#3366ff"
        trafficColor="#99aaff"
      />
    </QueryClientProvider>,
  );
}

describe("GraphSection exact graph boundary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("does not chart points carried by a degraded response", async () => {
    axios.post.mockResolvedValue({
      data: {
        result: {
          metric_name: "latency",
          data: [
            {
              timestamp: "2026-08-03T00:00:00Z",
              value: 999,
              primary_traffic: 999,
            },
          ],
          query_complete: false,
          query_status: "degraded",
          query_error_code: "sample_limit",
        },
      },
    });

    renderGraph();
    fireEvent.click(screen.getByRole("button", { name: "Select latency" }));

    expect(
      await screen.findByText(
        "Results are incomplete. Please retry in a moment.",
      ),
    ).toBeInTheDocument();
    await waitFor(() => expect(axios.post).toHaveBeenCalledOnce());
    expect(screen.queryByTestId("apex-chart")).not.toBeInTheDocument();
  });

  it("charts explicitly sampled points with a visible sample warning", async () => {
    axios.post.mockResolvedValue({
      data: {
        result: {
          metric_name: "latency",
          data: [
            {
              timestamp: "2026-08-03T00:00:00Z",
              value: 12,
              primary_traffic: 1,
            },
          ],
          query_complete: false,
          query_status: "sampled",
          query_error_code: "sample_limit",
          query_sampling_strategy: "time_stratified_latest_state",
          query_sampling_strata: 8,
          query_sampling_strata_completed: 8,
        },
      },
    });

    renderGraph();
    fireEvent.click(screen.getByRole("button", { name: "Select latency" }));

    expect(
      await screen.findByText(
        "Showing sampled values, not full totals.",
      ),
    ).toBeInTheDocument();
    await waitFor(() => expect(axios.post).toHaveBeenCalledOnce());
    expect(screen.getByTestId("apex-chart")).toBeInTheDocument();
  });
});
