import { describe, expect, it } from "vitest";

import {
  formatSelectionCount,
  getListTotalState,
  getSelectionCountState,
} from "../listTotalMetadata";

describe("list total metadata", () => {
  it("keeps an exact total available to exact-count consumers", () => {
    expect(
      getListTotalState({
        total_rows: 25,
        total_rows_exact: 25,
        total_rows_is_lower_bound: false,
      }),
    ).toEqual({
      totalRowCount: 25,
      totalRowCountLowerBound: null,
      totalRowCountIsLowerBound: false,
    });
  });

  it("never exposes a lower bound through the exact-total field", () => {
    expect(
      getListTotalState({
        total_rows: 26,
        total_rows_exact: null,
        total_rows_is_lower_bound: true,
      }),
    ).toEqual({
      totalRowCount: null,
      totalRowCountLowerBound: 26,
      totalRowCountIsLowerBound: true,
    });
  });

  it("preserves lower-bound semantics after select-all exclusions", () => {
    const selection = getSelectionCountState({
      selectAll: true,
      toggledNodes: ["trace-a", "trace-b"],
      totalRowCount: null,
      totalRowCountLowerBound: 26,
      totalRowCountIsLowerBound: true,
    });

    expect(selection).toEqual({ count: 24, isLowerBound: true });
    expect(formatSelectionCount(selection)).toBe("≥24");
  });

  it("keeps explicit row selection exact even when the list total is not", () => {
    expect(
      getSelectionCountState({
        selectAll: false,
        toggledNodes: ["trace-a", "trace-b"],
        totalRowCount: null,
        totalRowCountLowerBound: 26,
        totalRowCountIsLowerBound: true,
      }),
    ).toEqual({ count: 2, isLowerBound: false });
  });

  it("preserves the existing minimum count for exact select-all state", () => {
    expect(
      getSelectionCountState({
        selectAll: true,
        toggledNodes: ["trace-a"],
        totalRowCount: 1,
        totalRowCountLowerBound: null,
        totalRowCountIsLowerBound: false,
      }),
    ).toEqual({ count: 1, isLowerBound: false });
  });
});
