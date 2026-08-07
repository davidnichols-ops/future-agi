import React from "react";
import PropTypes from "prop-types";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn() }));

vi.mock("src/hooks/use-debounce", () => ({
  useDebounce: (value) => value,
}));

vi.mock("src/utils/axios", () => ({
  default: mocks,
  endpoints: {
    project: {
      spanAttributeKeys: () => "/api/traces/span-attribute-keys/",
    },
  },
}));

import {
  getAttributeKeyPageReadState,
  useExactTraceAttributeProperties,
} from "../useExactTraceAttributeProperties";

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  function Wrapper({ children }) {
    return (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
  }
  Wrapper.propTypes = { children: PropTypes.node };
  return Wrapper;
}

describe("useExactTraceAttributeProperties", () => {
  beforeEach(() => vi.clearAllMocks());

  it("loads ten recent keys first and de-duplicates cursor pages", async () => {
    mocks.get
      .mockResolvedValueOnce({
        data: {
          result: [
            { key: "call.status", type: "string", count: 3 },
            { key: "final_status", type: "string", count: 2 },
          ],
          query_complete: true,
          query_status: "complete",
          browse_mode: "recent_suggestions",
          browse_status: "continuation",
          browse_limit: 224,
          has_more: true,
          next_cursor: "signed-page-2",
        },
      })
      .mockResolvedValueOnce({
        data: {
          result: [
            { key: "final_status", type: "string", count: 1 },
            { key: "cost_cents", type: "number", count: 1 },
          ],
          query_complete: true,
          query_status: "complete",
          browse_mode: "recent_suggestions",
          browse_status: "exhausted",
          browse_limit: 224,
          has_more: false,
          next_cursor: null,
        },
      });

    const { result } = renderHook(
      () =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search: "",
          source: "traces",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mocks.get).toHaveBeenNthCalledWith(
      1,
      "/api/traces/span-attribute-keys/",
      expect.objectContaining({
        params: {
          project_id: "project-synthetic",
          page_size: 10,
        },
      }),
    );
    await act(async () => result.current.fetchNextPage());
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(2));
    expect(mocks.get).toHaveBeenNthCalledWith(
      2,
      "/api/traces/span-attribute-keys/",
      expect.objectContaining({
        params: {
          project_id: "project-synthetic",
          page_size: 10,
          cursor: "signed-page-2",
        },
      }),
    );
    expect(result.current.data.map((item) => item.id)).toEqual([
      "call.status",
      "final_status",
      "cost_cents",
    ]);
    expect(result.current.hasNextPage).toBe(false);
    expect(result.current.queryReadState).toBe("complete");
    expect(result.current.browseStatus).toBe("exhausted");
    expect(result.current.browseLimit).toBe(224);
    expect(result.current.browseLimitReached).toBe(false);
  });

  it("uses endpoint-specific browse state instead of generic sampling state", () => {
    expect(
      getAttributeKeyPageReadState({
        query_complete: true,
        query_status: "complete",
        browse_mode: "recent_suggestions",
        browse_status: "limit_reached",
      }),
    ).toBe("complete");
    expect(
      getAttributeKeyPageReadState({
        query_complete: false,
        query_status: "degraded",
        browse_mode: "recent_suggestions",
        browse_status: "continuation",
      }),
    ).toBe("degraded");
  });

  it("treats a verified positive exact lookup as authoritative beyond browse", () => {
    expect(
      getAttributeKeyPageReadState(
        {
          result: [{ key: "older_exact_key", type: "string", count: 1 }],
          query_complete: false,
          query_status: "sampled",
          query_error_code: "sample_limit",
          lookup_mode: "exact",
          exact_match: true,
        },
        { exact: true },
      ),
    ).toBe("complete");
  });

  it("keeps degraded exact matches scoped to the selected project and source", async () => {
    mocks.get.mockResolvedValue({
      data: {
        result: [{ key: "final_status", type: "string", count: 1 }],
        query_complete: false,
        query_status: "degraded",
      },
    });

    const { result } = renderHook(
      () =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search: "final_status",
          source: "traces",
          contextKey: "past-7-days",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mocks.get).toHaveBeenCalledWith(
      "/api/traces/span-attribute-keys/",
      expect.objectContaining({
        signal: expect.any(AbortSignal),
        params: {
          project_id: "project-synthetic",
          q: "final_status",
        },
      }),
    );
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: "final_status",
        category: "attribute",
        type: "string",
        apiColType: "SPAN_ATTRIBUTE",
      }),
    ]);
    expect(result.current.queryReadState).toBe("degraded");
  });

  it("cancels the stale exact lookup when the search context changes", async () => {
    const requests = [];
    mocks.get.mockImplementation(
      (_url, config) =>
        new Promise((resolve) => {
          requests.push({ config, resolve });
        }),
    );

    const { rerender } = renderHook(
      ({ search }) =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search,
          source: "traces",
          contextKey: "past-7-days",
        }),
      {
        initialProps: { search: "final_status" },
        wrapper: createWrapper(),
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    rerender({ search: "prompt_slug" });
    await waitFor(() => expect(requests).toHaveLength(2));

    expect(requests[0].config.signal.aborted).toBe(true);
    expect(requests[1].config.params.q).toBe("prompt_slug");

    await act(async () => {
      requests[1].resolve({
        data: {
          result: [{ key: "prompt_slug", type: "string", count: 1 }],
          query_complete: true,
          query_status: "complete",
        },
      });
    });
  });

  it("does not query without a project or for an unsupported source", () => {
    const { rerender } = renderHook(
      (props) => useExactTraceAttributeProperties(props),
      {
        initialProps: {
          projectId: "",
          search: "final_status",
          source: "traces",
        },
        wrapper: createWrapper(),
      },
    );

    expect(mocks.get).not.toHaveBeenCalled();
    rerender({
      projectId: "project-synthetic",
      search: "final_status",
      source: "sessions",
    });
    expect(mocks.get).not.toHaveBeenCalled();
  });

  it.each([
    ["retry_count", "number"],
    ["was_escalated", "boolean"],
    ["json_choices", "array"],
    ["customer_context", "map"],
  ])("preserves the exact %s attribute type", async (key, type) => {
    mocks.get.mockResolvedValue({
      data: {
        result: [{ key, type, count: 1 }],
        query_complete: true,
        query_status: "complete",
      },
    });

    const { result } = renderHook(
      () =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search: key,
          source: "traces",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: key,
        type,
        apiColType: "SPAN_ATTRIBUTE",
      }),
    ]);
  });

  it("preserves every observed storage type for a mixed attribute", async () => {
    mocks.get.mockResolvedValue({
      data: {
        result: [
          {
            key: "mixed_status",
            type: "string",
            types: ["string", "number", "boolean"],
            count: 3,
            count_exact: false,
          },
        ],
        query_complete: true,
        query_status: "complete",
        lookup_mode: "exact",
        exact_match: true,
      },
    });

    const { result } = renderHook(
      () =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search: "mixed_status",
          source: "traces",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data[0].attributeTypes).toEqual([
      "string",
      "number",
      "boolean",
    ]);
    expect(result.current.data[0].attributeTypesExact).toBe(false);
  });

  it("only certifies storage-type coverage when the server does", async () => {
    mocks.get.mockResolvedValue({
      data: {
        result: [
          {
            key: "certified_status",
            type: "string",
            types: ["string"],
            types_exact: true,
          },
        ],
        query_complete: true,
        query_status: "complete",
        lookup_mode: "exact",
        exact_match: true,
      },
    });

    const { result } = renderHook(
      () =>
        useExactTraceAttributeProperties({
          projectId: "project-synthetic",
          search: "certified_status",
          source: "traces",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data[0].attributeTypesExact).toBe(true);
  });
});
