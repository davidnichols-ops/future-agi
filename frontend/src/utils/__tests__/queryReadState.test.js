import { describe, expect, it, vi } from "vitest";
import {
  failServerSideGridRead,
  getExactGraphData,
  getQueryReadMessage,
  getQueryReadState,
  QUERY_FAILED_RETRY_MESSAGE,
  QUERY_READ_RETRY_MESSAGE,
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
    { query_complete: false },
    { query_status: "degraded" },
    { result: { query_complete: false, query_status: "degraded" } },
    { result: { metadata: { query_status: "degraded" } } },
  ])("recognizes degraded metadata at every API response level", (payload) => {
    expect(getQueryReadState(payload)).toBe("degraded");
    expect(getQueryReadMessage("degraded")).toBe(QUERY_READ_RETRY_MESSAGE);
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
