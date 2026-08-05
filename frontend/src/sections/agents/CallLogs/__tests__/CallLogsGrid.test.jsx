import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "src/utils/test-utils";

const { agGridState, prefetchCallLogsMock, useCallLogsMock } = vi.hoisted(
  () => ({
    agGridState: { props: null },
    prefetchCallLogsMock: vi.fn(),
    useCallLogsMock: vi.fn(),
  }),
);

vi.mock("ag-grid-react", async () => {
  const ReactModule = await import("react");
  const AgGridReact = ReactModule.forwardRef(
    function MockAgGridReact(props, _ref) {
      agGridState.props = props;
      return (
        <div data-testid="call-logs-grid">
          {props.rowData?.length === 0 && props.noRowsOverlayComponent?.()}
        </div>
      );
    },
  );
  return { AgGridReact };
});

vi.mock("src/styles/clean-data-table.css", () => ({}));
vi.mock("@tanstack/react-query", async (importOriginal) => ({
  ...(await importOriginal()),
  useQueryClient: () => ({ prefetchQuery: vi.fn() }),
}));
vi.mock("src/hooks/use-ag-theme", () => ({
  useAgTheme: () => ({ withParams: () => ({}) }),
}));
vi.mock("src/sections/agents/helper", () => ({
  getCallLogsColumnDefs: () => [],
  prefetchCallLogs: (...args) => prefetchCallLogsMock(...args),
  useCallLogs: (...args) => useCallLogsMock(...args),
}));
vi.mock("src/sections/agents/store/agentDetailsStore", () => ({
  useAgentDetailsStore: () => ({ selectedVersion: "version-1" }),
}));
vi.mock("src/sections/agents/store", () => ({
  useShallowToggleAnnotationsStore: (selector) =>
    selector({ showMetricsIds: false, reset: vi.fn() }),
}));
vi.mock("src/sections/test-detail/states", () => ({
  resetState: vi.fn(),
  useTestDetailSideDrawerStoreShallow: (selector) =>
    selector({ testDetailDrawerOpen: null }),
}));
vi.mock(
  "src/sections/test-detail/TestDetailDrawer/TestDetailSideDrawer",
  () => ({ default: () => null }),
);
vi.mock("src/components/show", () => ({
  ShowComponent: ({ condition, children }) => (condition ? children : null),
}));
vi.mock("src/components/iconify", () => ({ default: () => null }));
vi.mock("src/sections/project-detail/CompareDrawer/NoRowsOverlay", () => ({
  default: (content) => content,
}));

import CallLogsGrid from "../CallLogsGrid";

const incompleteData = {
  count: 0,
  count_is_lower_bound: true,
  total_pages: 20,
  current_page: 1,
  results: [],
  config: [],
  has_more: false,
  query_complete: false,
  query_status: "degraded",
  query_error_code: "scan_budget_exceeded",
};

const completeData = {
  count: 16,
  count_is_lower_bound: true,
  total_pages: 2,
  current_page: 1,
  results: [{ id: "trace-a", trace_id: "trace-a", status: "completed" }],
  config: [],
  has_more: true,
  query_complete: true,
  query_status: "complete",
  query_error_code: null,
};

describe("CallLogsGrid bounded-read state", () => {
  beforeEach(() => {
    agGridState.props = null;
    prefetchCallLogsMock.mockReset();
    useCallLogsMock.mockReset();
  });

  it("labels an incomplete page and disables misleading pagination/prefetch", async () => {
    useCallLogsMock.mockReturnValue({
      data: incompleteData,
      isLoading: false,
      error: null,
      queryKey: ["callLogs", "project", "project-1", 15, {}, 1],
    });

    render(<CallLogsGrid id="project-1" module="project" hideDrawer />);

    expect(await screen.findByRole("status")).toHaveTextContent(
      "Results are incomplete. Please retry in a moment.",
    );
    expect(screen.queryByText("No calls found")).not.toBeInTheDocument();
    expect(agGridState.props.rowData).toEqual([]);
    expect(
      screen.queryByRole("button", { name: /go to page 2/i }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Next").closest("button")).toBeDisabled();
    await waitFor(() => expect(prefetchCallLogsMock).not.toHaveBeenCalled());
  });

  it("keeps complete-page rendering and next-page prefetch unchanged", async () => {
    useCallLogsMock.mockReturnValue({
      data: completeData,
      isLoading: false,
      error: null,
      queryKey: ["callLogs", "project", "project-1", 15, {}, 1],
    });

    render(<CallLogsGrid id="project-1" module="project" hideDrawer />);

    await waitFor(() => expect(prefetchCallLogsMock).toHaveBeenCalledTimes(1));
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(agGridState.props.rowData).toEqual(completeData.results);
    expect(
      screen.getByRole("button", { name: /go to page 2/i }),
    ).toBeInTheDocument();
    expect(screen.getByText("Next").closest("button")).not.toBeDisabled();
    expect(prefetchCallLogsMock).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        module: "project",
        id: "project-1",
        page: 2,
        pageLimit: 15,
      }),
    );
  });
});
