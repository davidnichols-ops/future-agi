import { describe, expect, it } from "vitest";
import { getAgentGraphPresentationState } from "../agent-graph";

describe("getAgentGraphPresentationState", () => {
  it("turns a terminal failed refresh into an error instead of an endless spinner", () => {
    const state = getAgentGraphPresentationState({
      data: {
        nodes: [],
        edges: [],
        path_edges: [],
        query_complete: false,
        query_status: "pending",
        query_sampled: false,
        query_refreshing: false,
        query_refresh_failed: true,
      },
      isLoading: false,
      isError: false,
    });

    expect(state).toEqual(
      expect.objectContaining({
        data: undefined,
        isLoading: false,
        isError: true,
        queryReadState: "pending",
      }),
    );
  });

  it("keeps a live pending refresh in loading state", () => {
    const state = getAgentGraphPresentationState({
      data: {
        nodes: [],
        edges: [],
        path_edges: [],
        query_complete: false,
        query_status: "pending",
        query_sampled: false,
        query_refreshing: true,
        query_refresh_failed: false,
      },
      isLoading: false,
      isError: false,
    });

    expect(state.isLoading).toBe(true);
    expect(state.isError).toBe(false);
  });
});
