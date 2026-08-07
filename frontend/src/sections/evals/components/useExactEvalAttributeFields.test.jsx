import React from "react";
import PropTypes from "prop-types";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  getEvalAttributes: vi.fn(),
}));

vi.mock("src/hooks/use-debounce", () => ({
  useDebounce: (value) => value,
}));

vi.mock("src/utils/axios", () => ({
  default: { get: mocks.get },
  endpoints: {
    project: {
      spanAttributeKeys: () => "/api/traces/span-attribute-keys/",
    },
  },
}));

vi.mock("src/generated/api-contracts/api", () => ({
  tracerObservationSpanGetEvalAttributesList: mocks.getEvalAttributes,
}));

import {
  retainedAttributeFieldName,
  useExactEvalAttributeFields,
} from "./useExactEvalAttributeFields";

function retainedPage(keys, overrides = {}) {
  return {
    data: {
      result: keys.map((key) => ({ key, type: "string", count: 1 })),
      query_complete: true,
      query_status: "complete",
      browse_mode: "retained_catalog",
      browse_status: "exhausted",
      has_more: false,
      next_cursor: null,
      ...overrides,
    },
  };
}

function exactResponse(fields, overrides = {}) {
  return {
    data: {
      result: fields,
      query_complete: true,
      query_status: "complete",
      ...overrides,
    },
    status: 200,
  };
}

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

describe("useExactEvalAttributeFields", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.get.mockResolvedValue(retainedPage(["retained_status"]));
    mocks.getEvalAttributes.mockResolvedValue(exactResponse([]));
  });

  it.each([
    ["Span", "spans", "final_status", ["retained_status", "final_status"]],
    [
      "traces",
      "traces",
      "spans.0.final_status",
      ["spans.0.retained_status", "spans.0.final_status"],
    ],
  ])(
    "merges retained %s fields with the project-scoped exact q fast path",
    async (rowType, expectedRowType, expectedField, expectedFields) => {
      mocks.getEvalAttributes.mockResolvedValue(exactResponse([expectedField]));

      const { result } = renderHook(
        () =>
          useExactEvalAttributeFields({
            projectId: "00000000-0000-4000-8000-000000000901",
            rowType,
            search: " final_status ",
          }),
        { wrapper: createWrapper() },
      );

      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(mocks.get).toHaveBeenCalledWith(
        "/api/traces/span-attribute-keys/",
        expect.objectContaining({
          signal: expect.any(AbortSignal),
          params: {
            project_id: "00000000-0000-4000-8000-000000000901",
            page_size: 10,
          },
        }),
      );
      expect(mocks.getEvalAttributes).toHaveBeenCalledWith(
        {
          filters: JSON.stringify({
            project_id: "00000000-0000-4000-8000-000000000901",
          }),
          row_type: expectedRowType,
          q: "final_status",
        },
        { signal: expect.any(AbortSignal) },
      );
      expect(result.current.data).toEqual(expectedFields);
      expect(result.current.queryReadState).toBe("complete");
    },
  );

  it("continues the retained cursor and de-duplicates fields across pages", async () => {
    mocks.get
      .mockResolvedValueOnce(
        retainedPage(["first", "duplicate"], {
          browse_status: "continuation",
          has_more: true,
          next_cursor: "retained-page-2",
        }),
      )
      .mockResolvedValueOnce(retainedPage(["duplicate", "older"]));

    const { result } = renderHook(
      () =>
        useExactEvalAttributeFields({
          projectId: "project-synthetic",
          rowType: "traces",
          search: "",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.hasNextPage).toBe(true));
    await act(async () => result.current.fetchNextPage());
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(2));

    expect(mocks.get).toHaveBeenNthCalledWith(
      2,
      "/api/traces/span-attribute-keys/",
      expect.objectContaining({
        params: {
          project_id: "project-synthetic",
          page_size: 10,
          cursor: "retained-page-2",
        },
      }),
    );
    expect(result.current.data).toEqual([
      "spans.0.first",
      "spans.0.duplicate",
      "spans.0.older",
    ]);
    expect(mocks.getEvalAttributes).not.toHaveBeenCalled();
  });

  it("reuses retained pages while exact typed search changes", async () => {
    const requests = [];
    mocks.getEvalAttributes.mockImplementation(
      (params, options) =>
        new Promise((resolve) => requests.push({ options, params, resolve })),
    );

    const { result, rerender } = renderHook(
      ({ search }) =>
        useExactEvalAttributeFields({
          projectId: "project-synthetic",
          rowType: "spans",
          search,
        }),
      {
        initialProps: { search: "final_status" },
        wrapper: createWrapper(),
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    rerender({ search: "customer_outcome" });
    await waitFor(() => expect(requests).toHaveLength(2));

    expect(mocks.get).toHaveBeenCalledTimes(1);
    expect(requests[0].options.signal.aborted).toBe(true);
    expect(requests[1].params).toMatchObject({
      filters: JSON.stringify({ project_id: "project-synthetic" }),
      row_type: "spans",
      q: "customer_outcome",
    });

    await act(async () => {
      requests[0].resolve(exactResponse(["final_status"]));
      requests[1].resolve(exactResponse(["customer_outcome"]));
    });

    await waitFor(() =>
      expect(result.current.data).toEqual([
        "retained_status",
        "customer_outcome",
      ]),
    );
    expect(result.current.data).not.toContain("final_status");
  });

  it("keeps verified fields while reporting a degraded exact fast path", async () => {
    mocks.getEvalAttributes.mockResolvedValue(
      exactResponse(["final_status"], {
        query_complete: false,
        query_status: "degraded",
        query_error_code: "read_budget_exceeded",
      }),
    );

    const { result } = renderHook(
      () =>
        useExactEvalAttributeFields({
          projectId: "project-synthetic",
          rowType: "spans",
          search: "final_status",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(["retained_status", "final_status"]);
    expect(result.current.queryReadState).toBe("degraded");
  });

  it("keeps retained fields and exposes a sanitized exact-read error state", async () => {
    mocks.getEvalAttributes.mockRejectedValue(new Error("internal details"));

    const { result } = renderHook(
      () =>
        useExactEvalAttributeFields({
          projectId: "project-synthetic",
          rowType: "spans",
          search: "final_status",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.data).toEqual(["retained_status"]);
    expect(result.current.queryReadState).toBe("error");
  });

  it.each(["sessions", "voiceCalls"])(
    "does not probe unsupported %s mappings",
    (rowType) => {
      renderHook(
        () =>
          useExactEvalAttributeFields({
            projectId: "project-synthetic",
            rowType,
            search: "final_status",
          }),
        { wrapper: createWrapper() },
      );

      expect(mocks.get).not.toHaveBeenCalled();
      expect(mocks.getEvalAttributes).not.toHaveBeenCalled();
    },
  );

  it("maps retained keys to the resolver's canonical row paths", () => {
    expect(retainedAttributeFieldName("llm.model", "spans")).toBe("llm.model");
    expect(retainedAttributeFieldName("llm.model", "traces")).toBe(
      "spans.0.llm.model",
    );
    expect(retainedAttributeFieldName("", "traces")).toBeNull();
  });
});
