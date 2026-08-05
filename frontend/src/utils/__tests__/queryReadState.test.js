import { describe, expect, it, vi } from "vitest";
import {
  failServerSideGridRead,
  getExactGraphData,
  getQueryReadMessage,
  getQueryReadState,
  getRenderableGraphData,
  QUERY_FAILED_RETRY_MESSAGE,
  QUERY_READ_RETRY_MESSAGE,
  QUERY_READ_SAMPLED_MESSAGE,
} from "../queryReadState";

describe("queryReadState", () => {
  it("preserves legacy behaviour when bounded-read metadata is absent", () => {
    expect(getQueryReadState({ result: { table: [] } })).toBe("complete");
  });

  it("recognizes explicit complete metadata", () => {
    expect(
      getQueryReadState({
        result: {
          metadata: { query_complete: true, query_status: "complete" },
        },
      }),
    ).toBe("complete");
  });

  it.each([
    ["only a completion flag", { query_complete: true }],
    ["only a status flag", { query_status: "complete" }],
    [
      "an error code without a status pair",
      { query_error_code: "query_failed" },
    ],
    [
      "sampling coverage without a status pair",
      {
        query_sampling_strategy: "time_stratified_latest_state",
        query_sampling_strata: 8,
        query_sampling_strata_completed: 8,
      },
    ],
    [
      "a complete status marked incomplete",
      { query_complete: false, query_status: "complete" },
    ],
    [
      "a degraded status marked complete",
      { query_complete: true, query_status: "degraded" },
    ],
    [
      "an exact result with an active error code",
      {
        query_complete: true,
        query_status: "complete",
        query_error_code: "read_budget_exceeded",
      },
    ],
    [
      "an exact result marked sampled",
      {
        query_complete: true,
        query_status: "complete",
        query_sampled: true,
      },
    ],
  ])("fails closed for %s", (_, payload) => {
    expect(getQueryReadState(payload)).toBe("degraded");
  });

  it("rejects a sampled status that contradicts its completion flag", () => {
    expect(
      getQueryReadState({
        query_complete: true,
        query_status: "sampled",
        query_sampling_strategy: "time_stratified_latest_state",
        query_sampling_strata: 8,
        query_sampling_strata_completed: 8,
      }),
    ).toBe("degraded");
  });

  it.each([
    { query_complete: false },
    { query_status: "degraded" },
    { result: { query_complete: false, query_status: "degraded" } },
    { result: { metadata: { query_status: "degraded" } } },
  ])("recognizes degraded metadata at every API response level", (payload) => {
    expect(getQueryReadState(payload)).toBe("degraded");
    expect(getQueryReadMessage("degraded")).toBe(QUERY_READ_RETRY_MESSAGE);
  });

  it("recognizes an explicitly sampled graph without treating it as a failure", () => {
    const payload = {
      query_complete: false,
      query_status: "sampled",
      query_sampling_strategy: "time_stratified_latest_state",
      query_sampling_strata: 8,
      query_sampling_strata_completed: 8,
    };

    expect(getQueryReadState(payload)).toBe("sampled");
    expect(getQueryReadMessage("sampled")).toBe(QUERY_READ_SAMPLED_MESSAGE);
  });

  it("recognizes sampled metadata on public chart-series arrays", () => {
    const series = [
      {
        name: "quality",
        data: [],
        query_complete: false,
        query_status: "sampled",
        query_sampling_strategy: "time_stratified_latest_state",
        query_sampling_strata: 8,
        query_sampling_strata_completed: 8,
      },
    ];

    expect(getQueryReadState({ result: series })).toBe("sampled");
  });

  it("recognizes the bounded dashboard sample contract inside metric results", () => {
    const payload = {
      metrics: [
        {
          query_complete: false,
          query_status: "sampled",
          query_error_code: "sample_limit",
          query_sampling_strategy: "bounded_physical_rows_per_time_bucket",
          query_sampling_interval_seconds: 86400,
          query_sample_limit: 8192,
          query_sample_per_bucket: 128,
        },
      ],
    };

    expect(getQueryReadState(payload)).toBe("sampled");
  });

  const validDashboardSample = {
    query_complete: false,
    query_status: "sampled",
    query_error_code: "sample_limit",
    query_sampling_strategy: "bounded_physical_rows_per_time_bucket",
    query_sampling_interval_seconds: 86400,
    query_sample_limit: 8192,
    query_sample_per_bucket: 128,
  };

  it.each([
    ["missing completion flag", { query_complete: undefined }],
    ["contradictory completion flag", { query_complete: true }],
    ["missing error code", { query_error_code: undefined }],
    ["wrong error code", { query_error_code: "query_failed" }],
    ["wrong strategy", { query_sampling_strategy: "full_scan" }],
    ["missing interval", { query_sampling_interval_seconds: undefined }],
    ["zero interval", { query_sampling_interval_seconds: 0 }],
    ["missing sample limit", { query_sample_limit: undefined }],
    ["zero sample limit", { query_sample_limit: 0 }],
    ["missing per-bucket limit", { query_sample_per_bucket: undefined }],
    ["zero per-bucket limit", { query_sample_per_bucket: 0 }],
    ["per-bucket above total limit", { query_sample_per_bucket: 8193 }],
  ])("fails closed for a dashboard sample with %s", (_, invalidFields) => {
    expect(
      getQueryReadState({
        result: {
          metrics: [{ ...validDashboardSample, ...invalidFields }],
        },
      }),
    ).toBe("degraded");
  });

  it("applies the strictest state across nested dashboard metrics", () => {
    const complete = { query_complete: true, query_status: "complete" };
    const degraded = {
      query_complete: false,
      query_status: "degraded",
      query_error_code: "read_budget_exceeded",
    };

    expect(
      getQueryReadState({
        result: { metrics: [complete, validDashboardSample] },
      }),
    ).toBe("sampled");
    expect(
      getQueryReadState({
        result: { metrics: [complete, validDashboardSample, degraded] },
      }),
    ).toBe("degraded");
    expect(
      getQueryReadState({
        result: {
          metrics: [
            complete,
            validDashboardSample,
            { ...validDashboardSample, query_sample_per_bucket: 0 },
          ],
        },
      }),
    ).toBe("degraded");
  });

  it("uses a generic message for request failures", () => {
    const rawError = "Code: 159 DB::Exception: Timeout exceeded";
    expect(getQueryReadState({ result: rawError }, { isError: true })).toBe(
      "error",
    );
    expect(getQueryReadMessage("error")).toBe(QUERY_FAILED_RETRY_MESSAGE);
    expect(getQueryReadMessage("error")).not.toContain(rawError);
  });

  it("returns exact graph points for current and legacy complete responses", () => {
    const points = [{ timestamp: "2026-08-03T00:00:00Z", value: 2 }];

    expect(
      getExactGraphData({
        data: points,
        query_complete: true,
        query_status: "complete",
      }),
    ).toEqual(points);
    expect(getExactGraphData({ result: { data: points } })).toEqual(points);
  });

  it.each([{ query_complete: false }, { query_status: "degraded" }])(
    "refuses to chart points from an incomplete backend response",
    (metadata) => {
      const sampledPoints = [{ timestamp: "2026-08-03T00:00:00Z", value: 999 }];

      expect(getExactGraphData({ ...metadata, data: sampledPoints })).toEqual(
        [],
      );
      expect(
        getExactGraphData({
          ...metadata,
          result: { data: sampledPoints },
        }),
      ).toEqual([]);
    },
  );

  it("renders only explicitly labelled samples", () => {
    const points = [{ timestamp: "2026-08-03T00:00:00Z", value: 2 }];

    expect(
      getRenderableGraphData({
        data: points,
        query_complete: false,
        query_status: "sampled",
        query_sampling_strategy: "time_stratified_latest_state",
        query_sampling_strata: 8,
        query_sampling_strata_completed: 8,
      }),
    ).toEqual(points);
    expect(
      getRenderableGraphData({
        data: points,
        query_complete: false,
        query_status: "degraded",
      }),
    ).toEqual([]);
  });

  it.each([
    {},
    { query_sampling_strata: 8, query_sampling_strata_completed: 0 },
    { query_sampling_strata: 8, query_sampling_strata_completed: 1 },
  ])("refuses a sampled graph without full temporal coverage", (coverage) => {
    const payload = {
      data: [{ timestamp: "2026-08-03T00:00:00Z", value: 999 }],
      query_complete: false,
      query_status: "sampled",
      query_sampling_strategy: "time_stratified_latest_state",
      ...coverage,
    };

    expect(getQueryReadState(payload)).toBe("degraded");
    expect(getRenderableGraphData(payload)).toEqual([]);
  });

  it("preserves server-side pagination failure semantics", () => {
    const params = {
      fail: vi.fn(),
      success: vi.fn(),
      api: { showNoRowsOverlay: vi.fn() },
    };

    failServerSideGridRead(params);

    expect(params.fail).toHaveBeenCalledOnce();
    expect(params.success).not.toHaveBeenCalled();
    expect(params.api.showNoRowsOverlay).toHaveBeenCalledOnce();
  });
});
