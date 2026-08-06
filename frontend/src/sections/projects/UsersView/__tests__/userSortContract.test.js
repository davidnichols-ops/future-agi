import { describe, expect, it } from "vitest";
import {
  sanitizeUserColumnState,
  sanitizeUserSortModel,
} from "../userSortContract";

describe("Observe Users server sort contract", () => {
  it("keeps only exact server-supported sort columns", () => {
    expect(
      sanitizeUserSortModel([
        { colId: "last_active", sort: "desc" },
        { colId: "num_sessions", sort: "asc" },
        { colId: "avg_trace_latency", sort: "desc" },
        { colId: "eval_score", sort: "asc" },
      ]),
    ).toEqual([{ colId: "last_active", sort: "desc" }]);
  });

  it("clears unsupported saved-view sorts without changing display state", () => {
    expect(
      sanitizeUserColumnState([
        {
          colId: "num_sessions",
          sort: "desc",
          sortIndex: 0,
          hide: true,
          width: 240,
        },
        {
          colId: "total_cost",
          sort: "asc",
          sortIndex: 1,
          hide: false,
          width: 180,
        },
      ]),
    ).toEqual([
      {
        colId: "num_sessions",
        sort: null,
        sortIndex: null,
        hide: true,
        width: 240,
      },
      {
        colId: "total_cost",
        sort: "asc",
        sortIndex: 1,
        hide: false,
        width: 180,
      },
    ]);
  });
});
