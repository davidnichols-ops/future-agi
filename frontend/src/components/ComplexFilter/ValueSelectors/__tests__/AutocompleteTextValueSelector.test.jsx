import React from "react";
import PropTypes from "prop-types";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ get: vi.fn() }));

vi.mock("src/hooks/use-debounce", () => ({
  useDebounce: (value) => value,
}));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal()),
  useParams: () => ({ id: "project-large" }),
}));
vi.mock("src/utils/axios", () => ({
  default: mocks,
  endpoints: { dashboard: { filterValues: "/filter-values/" } },
}));

import AutocompleteTextValueSelector from "../AutocompleteTextValueSelector";

function Wrapper({ children }) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
Wrapper.propTypes = { children: PropTypes.node };

describe("AutocompleteTextValueSelector", () => {
  beforeEach(() => vi.clearAllMocks());

  it("loads exact cursor pages on scroll and normalizes the attribute type", async () => {
    mocks.get
      .mockResolvedValueOnce({
        data: {
          result: {
            values: [{ value: "completed", type: "string" }],
            query_complete: true,
            query_status: "complete",
            has_more: true,
            next_cursor: "page-2",
          },
        },
      })
      .mockResolvedValueOnce({
        data: {
          result: {
            values: [{ value: "ended", type: "string" }],
            query_complete: true,
            query_status: "complete",
            has_more: false,
            next_cursor: null,
          },
        },
      });

    render(
      <AutocompleteTextValueSelector
        definition={{ propertyId: "call.status", type: "text" }}
        filter={{ filter_config: { filter_value: "" } }}
        updateFilter={vi.fn()}
      />,
      { wrapper: Wrapper },
    );

    fireEvent.mouseDown(screen.getByRole("combobox"));
    expect(
      await screen.findByRole("option", { name: "completed" }),
    ).toBeVisible();
    expect(mocks.get).toHaveBeenNthCalledWith(
      1,
      "/filter-values/",
      expect.objectContaining({
        params: expect.objectContaining({
          project_ids: "project-large",
          metric_name: "call.status",
          metric_type: "custom_attribute",
          attribute_type: "string",
          page_size: 10,
        }),
      }),
    );

    const listbox = screen.getByRole("listbox");
    let scrollTop = 80;
    Object.defineProperties(listbox, {
      scrollTop: {
        configurable: true,
        get: () => scrollTop,
        set: (value) => {
          scrollTop = value;
        },
      },
      clientHeight: { configurable: true, get: () => 20 },
      scrollHeight: { configurable: true, get: () => 100 },
    });
    fireEvent.scroll(listbox);

    expect(await screen.findByRole("option", { name: "ended" })).toBeVisible();
    await waitFor(() => expect(mocks.get).toHaveBeenCalledTimes(2));
    expect(mocks.get).toHaveBeenNthCalledWith(
      2,
      "/filter-values/",
      expect.objectContaining({
        params: expect.objectContaining({ cursor: "page-2" }),
      }),
    );
    expect(screen.queryByText(/incomplete|sample/i)).not.toBeInTheDocument();
  });

  it("offers an explicit next-page action when an exact page has no values", async () => {
    mocks.get
      .mockResolvedValueOnce({
        data: {
          result: {
            values: [],
            query_complete: true,
            query_status: "complete",
            has_more: true,
            next_cursor: "older-page",
          },
        },
      })
      .mockResolvedValueOnce({
        data: {
          result: {
            values: [{ value: "completed", type: "string" }],
            query_complete: true,
            query_status: "complete",
            has_more: false,
            next_cursor: null,
          },
        },
      });

    render(
      <AutocompleteTextValueSelector
        definition={{ propertyId: "call.status", type: "text" }}
        filter={{ filter_config: { filter_value: "" } }}
        updateFilter={vi.fn()}
      />,
      { wrapper: Wrapper },
    );

    fireEvent.mouseDown(screen.getByRole("combobox"));
    fireEvent.click(
      await screen.findByRole("option", { name: "Load more values" }),
    );

    expect(
      await screen.findByRole("option", { name: "completed" }),
    ).toBeVisible();
    expect(mocks.get).toHaveBeenNthCalledWith(
      2,
      "/filter-values/",
      expect.objectContaining({
        params: expect.objectContaining({ cursor: "older-page" }),
      }),
    );
  });
});
