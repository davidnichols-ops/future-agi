import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "src/utils/test-utils";
import AddLabelDrawer from "../AddLabelDrawer";

const {
  entitlementMessage,
  mockGetOrCreate,
  mockGetOrCreateMutate,
  mockInvalidateQueries,
} = vi.hoisted(() => ({
  entitlementMessage:
    "You've reached the 10 annotation queues limit across this organization.",
  mockGetOrCreate: vi.fn(),
  mockGetOrCreateMutate: vi.fn(),
  mockInvalidateQueries: vi.fn(),
}));

vi.mock("@tanstack/react-query", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    useQueryClient: () => ({ invalidateQueries: mockInvalidateQueries }),
  };
});

vi.mock("src/api/annotation-labels/annotation-labels", () => ({
  annotationLabelKeys: { all: ["annotation-labels"] },
  useAnnotationLabelsList: () => ({
    data: {
      results: [{ id: "label-1", name: "Review", type: "categorical" }],
    },
  }),
}));

vi.mock("src/api/annotation-queues/annotation-queues", () => ({
  extractErrorMessage: (error, fallback) =>
    error?.response?.data?.result ||
    error?.response?.data?.error?.message ||
    fallback,
  useGetOrCreateDefaultQueue: (options) => {
    mockGetOrCreate(options);
    return { mutate: mockGetOrCreateMutate, isPending: false };
  },
  useAddLabelToQueue: () => ({ mutateAsync: vi.fn() }),
  useRemoveLabelFromQueue: () => ({ mutateAsync: vi.fn() }),
}));

vi.mock("src/sections/annotations/labels/create-label-drawer", () => ({
  default: () => null,
}));

describe("AddLabelDrawer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetOrCreateMutate.mockImplementation((_variables, callbacks) => {
      callbacks.onError({
        response: {
          data: {
            status: false,
            result: entitlementMessage,
            error: {
              code: "ENTITLEMENT_LIMIT",
              message: entitlementMessage,
            },
          },
        },
      });
    });
  });

  it("renders the exact queue entitlement once and suppresses generic UI", async () => {
    render(<AddLabelDrawer open onClose={vi.fn()} projectId="project-1" />);

    await waitFor(() => {
      expect(screen.getAllByText(entitlementMessage)).toHaveLength(1);
    });
    expect(screen.queryByText("Something went wrong")).not.toBeInTheDocument();
    expect(mockGetOrCreate).toHaveBeenCalledWith({ notifyOnError: false });
    expect(screen.getByRole("checkbox")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Next" })).toBeEnabled();
  });
});
