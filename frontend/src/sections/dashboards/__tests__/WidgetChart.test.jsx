import React from "react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "src/utils/test-utils";
import WidgetChart from "../WidgetChart";

const h = vi.hoisted(() => ({
  query: { data: null, isPending: false, isError: false, mutate: vi.fn() },
  apex: vi.fn(),
}));

vi.mock("src/hooks/useDashboards", () => ({
  useDashboardQuery: () => h.query,
}));

vi.mock("react-apexcharts", () => ({
  default: (props) => {
    h.apex(props);
    return <div data-testid={`apex-${props.type}`} />;
  },
}));

const baseWidget = {
  id: "w-1",
  query_config: {
    metrics: [{ name: "Latency", aggregation: "avg" }],
  },
  chart_config: { chart_type: "line" },
};

const queryResult = (points) => ({
  data: {
    result: {
      metrics: [
        {
          name: "Latency",
          aggregation: "avg",
          series: [{ name: "total", data: points }],
        },
      ],
    },
  },
});

const NO_DATA_MESSAGE = /No data available for this time period/i;
const SAMPLED_MESSAGE = /Showing sampled values, not full totals/i;
const DEGRADED_MESSAGE = /Results are incomplete. Please retry/i;

describe("WidgetChart — empty time-range state", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.query.isPending = false;
    h.query.isError = false;
    h.query.data = null;
  });

  it("shows the empty-range message when the metric's series has zero data points", () => {
    h.query.data = queryResult([]);
    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByText(NO_DATA_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByTestId("apex-line")).not.toBeInTheDocument();
  });

  it("renders the chart, not the empty-range message, once the series has data points", () => {
    h.query.data = queryResult([
      { timestamp: "2026-07-09T00:00:00Z", value: 12 },
      { timestamp: "2026-07-09T01:00:00Z", value: 18 },
    ]);
    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByTestId("apex-line")).toBeInTheDocument();
    expect(screen.queryByText(NO_DATA_MESSAGE)).not.toBeInTheDocument();
  });

  // Regression guard: hasNoDataForRange must stay ABOVE the metric-card/table/pie/
  // horizontal early returns so those widget types show this message too, instead of
  // falling into their own type-specific render with an empty series.
  it("shows the empty-range message for a pie widget with zero data points, not the pie render", () => {
    h.query.data = queryResult([]);
    const pieWidget = { ...baseWidget, chart_config: { chart_type: "pie" } };
    render(<WidgetChart widget={pieWidget} globalDateRange={null} />);

    expect(screen.getByText(NO_DATA_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByTestId("apex-pie")).not.toBeInTheDocument();
  });
});

describe("WidgetChart — bounded dashboard read state", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.query.isPending = false;
    h.query.isError = false;
    h.query.data = null;
  });

  it("visibly labels a bounded sampled metric while rendering its series", () => {
    h.query.data = {
      data: {
        result: {
          metrics: [
            {
              name: "final_status",
              aggregation: "count_distinct",
              query_complete: false,
              query_status: "sampled",
              query_error_code: "sample_limit",
              query_sampling_strategy: "bounded_physical_rows_per_time_bucket",
              query_sampling_interval_seconds: 86400,
              query_sample_limit: 8192,
              query_sample_per_bucket: 128,
              series: [
                {
                  name: "total",
                  data: [{ timestamp: "2026-07-09T00:00:00Z", value: 12 }],
                },
              ],
            },
          ],
        },
      },
    };

    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByText(SAMPLED_MESSAGE)).toBeInTheDocument();
    expect(screen.getByTestId("apex-line")).toBeInTheDocument();
    expect(h.apex.mock.calls.at(-1)[0].series[0].name).toContain("sampled");
  });

  it.each(["metric", "table", "pie", "bar"])(
    "keeps the sampled disclosure visible for the %s render path",
    (chartType) => {
      h.query.data = {
        data: {
          result: {
            metrics: [
              {
                name: "final_status",
                aggregation: "count_distinct",
                query_complete: false,
                query_status: "sampled",
                query_error_code: "sample_limit",
                query_sampling_strategy:
                  "bounded_physical_rows_per_time_bucket",
                query_sampling_interval_seconds: 86400,
                query_sample_limit: 8192,
                query_sample_per_bucket: 128,
                series: [
                  {
                    name: "total",
                    data: [{ timestamp: "2026-07-09T00:00:00Z", value: 12 }],
                  },
                ],
              },
            ],
          },
        },
      };

      render(
        <WidgetChart
          widget={{ ...baseWidget, chart_config: { chart_type: chartType } }}
          globalDateRange={null}
        />,
      );

      expect(screen.getByText(SAMPLED_MESSAGE)).toBeInTheDocument();
    },
  );

  it("does not plot a malformed sampled metric even when it contains points", () => {
    h.query.data = {
      data: {
        result: {
          metrics: [
            {
              name: "Latency",
              aggregation: "avg",
              query_complete: false,
              query_status: "sampled",
              query_error_code: "query_failed",
              query_sampling_strategy: "bounded_physical_rows_per_time_bucket",
              query_sampling_interval_seconds: 86400,
              query_sample_limit: 8192,
              query_sample_per_bucket: 128,
              series: [
                {
                  name: "total",
                  data: [{ timestamp: "2026-07-09T00:00:00Z", value: 999 }],
                },
              ],
            },
          ],
        },
      },
    };

    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByText(DEGRADED_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByTestId("apex-line")).not.toBeInTheDocument();
    expect(h.apex).not.toHaveBeenCalled();
  });

  it("does not plot a degraded read-budget metric as exact data", () => {
    h.query.data = {
      data: {
        result: {
          metrics: [
            {
              name: "Latency",
              aggregation: "avg",
              query_complete: false,
              query_status: "degraded",
              query_error_code: "read_budget_exceeded",
              series: [
                {
                  name: "total",
                  data: [{ timestamp: "2026-07-09T00:00:00Z", value: 999 }],
                },
              ],
            },
          ],
        },
      },
    };

    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByText(DEGRADED_MESSAGE)).toBeInTheDocument();
    expect(screen.queryByTestId("apex-line")).not.toBeInTheDocument();
    expect(h.apex).not.toHaveBeenCalled();
  });

  it("keeps exact metrics renderable while excluding a degraded sibling", () => {
    h.query.data = {
      data: {
        result: {
          metrics: [
            {
              name: "Latency",
              aggregation: "avg",
              query_complete: true,
              query_status: "complete",
              series: [
                {
                  name: "total",
                  data: [{ timestamp: "2026-07-09T00:00:00Z", value: 12 }],
                },
              ],
            },
            {
              name: "Cost",
              aggregation: "sum",
              query_complete: false,
              query_status: "degraded",
              query_error_code: "read_budget_exceeded",
              series: [
                {
                  name: "total",
                  data: [{ timestamp: "2026-07-09T00:00:00Z", value: 999 }],
                },
              ],
            },
          ],
        },
      },
    };

    render(<WidgetChart widget={baseWidget} globalDateRange={null} />);

    expect(screen.getByText(DEGRADED_MESSAGE)).toBeInTheDocument();
    expect(screen.getByTestId("apex-line")).toBeInTheDocument();
    const renderedSeries = h.apex.mock.calls.at(-1)[0].series;
    expect(renderedSeries).toHaveLength(1);
    expect(renderedSeries[0].name).toContain("Latency");
    expect(renderedSeries[0].name).not.toContain("Cost");
  });
});
