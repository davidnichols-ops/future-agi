import React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "src/utils/test-utils";
import axios from "src/utils/axios";
import PrimaryGraph from "../PrimaryGraph";

vi.mock("react-apexcharts", () => ({
  default: ({ series, options }) => (
    <div
      data-testid="apex-chart"
      data-traffic-series-name={series?.[1]?.name}
      data-traffic-axis-series-name={options?.yaxis?.[1]?.seriesName}
    />
  ),
}));

vi.mock("src/components/custom-datepicker/DatePicker", () => ({
  default: () => null,
}));

vi.mock("../../common", () => ({
  toBackendFilters: (filters) =>
    filters.map(({ id: _id, ...filter }) => filter),
}));

vi.mock("src/utils/axios", () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
  endpoints: {
    dashboard: {
      metrics: "/dashboard/metrics/",
    },
    project: {
      getTraceGraphData: () => "/tracer/trace/get_graph_methods/",
      getSpanGraphData: () => "/tracer/observation-span/get_graph_methods/",
    },
  },
}));

function renderWithQueryClient(ui) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>,
  );
}

describe("PrimaryGraph", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    axios.get.mockResolvedValue({
      data: {
        result: {
          metrics: [
            {
              category: "system_metric",
              name: "latency",
              displayName: "Latency",
              type: "number",
            },
          ],
        },
      },
    });
    axios.post.mockResolvedValue({
      data: {
        result: {
          metric_name: "latency",
          data: [],
          query_complete: true,
          query_status: "complete",
          query_sampled: false,
          query_completed_at: "2026-08-03T02:00:00Z",
        },
      },
    });
  });

  afterEach(() => vi.useRealTimers());

  it("uses observeIdOverride as the graph project id", async () => {
    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );

    await waitFor(() => expect(axios.post).toHaveBeenCalled());

    expect(axios.post).toHaveBeenCalledWith(
      "/tracer/trace/get_graph_methods/",
      expect.objectContaining({
        project_id: "project-override",
      }),
      { params: { allow_sampled: false } },
    );
  });

  it("uses the supplied graph endpoint for span graphs", async () => {
    renderWithQueryClient(
      <PrimaryGraph
        observeIdOverride="project-override"
        graphEndpoint="/tracer/observation-span/get_graph_methods/"
      />,
    );

    await waitFor(() => expect(axios.post).toHaveBeenCalled());

    expect(axios.post).toHaveBeenCalledWith(
      "/tracer/observation-span/get_graph_methods/",
      expect.objectContaining({
        project_id: "project-override",
      }),
      { params: { allow_sampled: false } },
    );
  });

  const statusFilter = {
    column_id: "status",
    filter_config: {
      col_type: "NORMAL",
      filter_type: "text",
      filter_op: "equals",
      filter_value: "SUCCESS",
    },
  };

  const metricFilter = {
    id: "fe-react-key",
    column_id: "latency",
    filter_config: {
      col_type: "SYSTEM_METRIC",
      filter_type: "number",
      filter_op: "greater_than",
      filter_value: 2,
    },
  };

  const postedFilters = () => axios.post.mock.calls.at(-1)[1].filters;

  it("keeps non-date filters when extraFilters is omitted (users/sessions)", async () => {
    // Regression guard for the round-1 review bug: UsersView and
    // SessionsView render PrimaryGraph WITHOUT extraFilters, and their
    // graph must receive the same chip filters as their table.
    renderWithQueryClient(
      <PrimaryGraph
        observeIdOverride="project-override"
        filters={[statusFilter]}
      />,
    );

    await waitFor(() => expect(axios.post).toHaveBeenCalled());

    expect(postedFilters()).toEqual([statusFilter]);
  });

  it("strips col-level filters when extraFilters is passed, even empty (trace/span)", async () => {
    // Regression guard for the round-2 review bug: the mode gate must be
    // prop PRESENCE — an empty toolbar filter list is still trace mode.
    renderWithQueryClient(
      <PrimaryGraph
        observeIdOverride="project-override"
        filters={[statusFilter]}
        extraFilters={[]}
      />,
    );

    await waitFor(() => expect(axios.post).toHaveBeenCalled());

    expect(postedFilters()).toEqual([]);
  });

  it("forwards toolbar extraFilters and strips the FE-only id (trace/span)", async () => {
    renderWithQueryClient(
      <PrimaryGraph
        observeIdOverride="project-override"
        filters={[statusFilter]}
        extraFilters={[metricFilter]}
      />,
    );

    await waitFor(() => expect(axios.post).toHaveBeenCalled());

    const { id: _id, ...metricFilterWithoutId } = metricFilter;
    expect(postedFilters()).toEqual([metricFilterWithoutId]);
  });

  it("does not present a degraded graph read as an empty time range", async () => {
    axios.post.mockResolvedValue({
      data: {
        query_complete: false,
        query_status: "degraded",
        result: {
          data: [
            {
              timestamp: "2026-08-03T00:00:00Z",
              value: 999,
              primary_traffic: 999,
            },
          ],
        },
      },
    });

    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );

    expect(
      await screen.findByText("Preparing exact data…"),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("No data available for this time range"),
    ).not.toBeInTheDocument();
    expect(screen.queryByTestId("apex-chart")).not.toBeInTheDocument();
  });

  it("does not chart an explicitly sampled graph", async () => {
    axios.post.mockResolvedValue({
      data: {
        result: {
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

    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );

    expect(
      await screen.findByText("Preparing exact data…"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("apex-chart")).not.toBeInTheDocument();
    expect(screen.queryByText(/sampled estimates/i)).not.toBeInTheDocument();
    expect(
      screen.queryByText("No data available for this time range"),
    ).not.toBeInTheDocument();
  });

  it("shows a generic graph error without exposing backend exception text", async () => {
    axios.post.mockRejectedValue({
      result: "Code: 159 DB::Exception: Timeout exceeded Stack trace...",
    });

    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );

    expect(
      await screen.findByText("Preparing exact data…"),
    ).toBeInTheDocument();
    expect(screen.queryByText(/DB::Exception/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/Stack trace/i)).not.toBeInTheDocument();
  });

  it("keeps exact data visible when an explicit refresh is not exact", async () => {
    const exactCompletion = vi.fn();
    window.addEventListener("observe-aggregation-completed", exactCompletion, {
      once: true,
    });
    axios.post
      .mockResolvedValueOnce({
        data: {
          result: {
            data: [
              {
                timestamp: "2026-08-03T00:00:00Z",
                value: 12,
                primary_traffic: 1,
              },
              {
                timestamp: "2026-08-03T01:00:00Z",
                value: null,
                primary_traffic: null,
              },
            ],
            query_complete: true,
            query_status: "complete",
            query_sampled: false,
            query_completed_at: "2026-08-03T02:00:00Z",
          },
        },
      })
      .mockResolvedValueOnce({
        data: {
          result: {
            data: [
              {
                timestamp: "2026-08-03T00:00:00Z",
                value: 999,
                primary_traffic: 999,
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

    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );
    expect(await screen.findByTestId("apex-chart")).toBeInTheDocument();
    expect(exactCompletion).toHaveBeenCalledOnce();
    expect(exactCompletion.mock.calls[0][0].detail).toEqual({
      observeId: "project-override",
      queryCompletedAt: "2026-08-03T02:00:00.000Z",
    });

    act(() => window.dispatchEvent(new CustomEvent("observe-refresh")));

    await waitFor(() => expect(axios.post).toHaveBeenCalledTimes(2));
    expect(screen.getByTestId("apex-chart")).toBeInTheDocument();
    expect(axios.post).toHaveBeenNthCalledWith(
      2,
      "/tracer/trace/get_graph_methods/",
      expect.any(Object),
      { params: { allow_sampled: false, refresh: true } },
    );
    expect(screen.queryByText(/sampled estimates/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/Preparing exact data/i)).not.toBeInTheDocument();
  });

  it("polls a cold pending graph without refresh and publishes only final completion", async () => {
    vi.useFakeTimers();
    const exactCompletion = vi.fn();
    window.addEventListener("observe-aggregation-completed", exactCompletion);
    axios.post
      .mockResolvedValueOnce({
        data: {
          result: {
            data: [],
            query_complete: false,
            query_status: "pending",
            query_sampled: false,
            query_refreshing: true,
          },
        },
      })
      .mockResolvedValueOnce({
        data: {
          result: {
            data: [
              {
                timestamp: "2026-08-03T00:00:00Z",
                value: 12,
                primary_traffic: 1,
              },
            ],
            query_complete: true,
            query_status: "complete",
            query_sampled: false,
            query_refreshing: false,
            query_completed_at: "2026-08-03T03:00:00Z",
          },
        },
      });

    renderWithQueryClient(
      <PrimaryGraph observeIdOverride="project-override" />,
    );
    await act(async () => vi.advanceTimersByTimeAsync(10));

    expect(axios.post).toHaveBeenCalledOnce();
    expect(screen.getByText("Preparing exact data…")).toBeInTheDocument();
    expect(exactCompletion).not.toHaveBeenCalled();

    await act(async () => vi.advanceTimersByTimeAsync(1000));
    await act(async () => vi.advanceTimersByTimeAsync(10));

    expect(axios.post).toHaveBeenCalledTimes(2);
    expect(axios.post).toHaveBeenNthCalledWith(
      2,
      "/tracer/trace/get_graph_methods/",
      expect.any(Object),
      { params: { allow_sampled: false } },
    );
    expect(screen.getByTestId("apex-chart")).toBeInTheDocument();
    expect(exactCompletion).toHaveBeenCalledOnce();
    window.removeEventListener(
      "observe-aggregation-completed",
      exactCompletion,
    );
  });
});
