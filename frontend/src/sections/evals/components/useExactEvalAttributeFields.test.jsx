import React from "react";
import PropTypes from "prop-types";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ getEvalAttributes: vi.fn() }));

vi.mock("src/hooks/use-debounce", () => ({
  useDebounce: (value) => value,
}));

vi.mock("src/generated/api-contracts/api", () => ({
  tracerObservationSpanGetEvalAttributesList: mocks.getEvalAttributes,
}));

import { useExactEvalAttributeFields } from "./useExactEvalAttributeFields";

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
  beforeEach(() => vi.clearAllMocks());

  it.each([
    ["Span", "spans", "final_status"],
    ["traces", "traces", "spans.0.final_status"],
  ])(
    "fetches a rare %s field with a project- and row-scoped exact q",
    async (rowType, expectedRowType, expectedField) => {
      mocks.getEvalAttributes.mockResolvedValue({
        data: {
          result: [expectedField],
          query_complete: true,
          query_status: "complete",
        },
        status: 200,
      });

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
      expect(result.current.data).toEqual([expectedField]);
      expect(result.current.queryReadState).toBe("complete");
    },
  );

  it("cancels and ignores a stale lookup when q changes", async () => {
    const requests = [];
    mocks.getEvalAttributes.mockImplementation(
      (params, options) =>
        new Promise((resolve) => requests.push({ options, params, resolve })),
    );

    const { result, rerender } = renderHook(
      ({ projectId, rowType, search }) =>
        useExactEvalAttributeFields({
          projectId,
          rowType,
          search,
        }),
      {
        initialProps: {
          projectId: "project-synthetic",
          rowType: "traces",
          search: "final_status",
        },
        wrapper: createWrapper(),
      },
    );

    await waitFor(() => expect(requests).toHaveLength(1));
    rerender({
      projectId: "project-other",
      rowType: "spans",
      search: "customer_outcome",
    });
    await waitFor(() => expect(requests).toHaveLength(2));

    expect(requests[0].options.signal.aborted).toBe(true);
    expect(requests[1].params).toMatchObject({
      filters: JSON.stringify({ project_id: "project-other" }),
      row_type: "spans",
      q: "customer_outcome",
    });

    await act(async () => {
      requests[0].resolve({
        data: { result: ["spans.0.final_status"], query_complete: true },
        status: 200,
      });
      requests[1].resolve({
        data: { result: ["customer_outcome"], query_complete: true },
        status: 200,
      });
    });

    await waitFor(() =>
      expect(result.current.data).toEqual(["customer_outcome"]),
    );
    expect(result.current.data).not.toContain("spans.0.final_status");
  });

  it("keeps latest-state-verified fields from a degraded bounded read", async () => {
    mocks.getEvalAttributes.mockResolvedValue({
      data: {
        result: ["final_status"],
        query_complete: false,
        query_status: "degraded",
        query_error_code: "read_budget_exceeded",
      },
      status: 200,
    });

    const { result } = renderHook(
      () =>
        useExactEvalAttributeFields({
          projectId: "project-synthetic",
          rowType: "traces",
          search: "final_status",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(["final_status"]);
    expect(result.current.queryReadState).toBe("degraded");
  });

  it("exposes a sanitized error read state", async () => {
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
    expect(result.current.data).toEqual([]);
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

      expect(mocks.getEvalAttributes).not.toHaveBeenCalled();
    },
  );
});
