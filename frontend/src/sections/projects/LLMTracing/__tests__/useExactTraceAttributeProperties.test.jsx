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

import { useExactTraceAttributeProperties } from "../useExactTraceAttributeProperties";

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
});
