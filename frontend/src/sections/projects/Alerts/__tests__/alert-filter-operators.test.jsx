import React from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import {
  fireEvent,
  renderWithRouter,
  screen,
  within,
} from "src/utils/test-utils";

import TraceFilterPanel from "src/sections/projects/LLMTracing/TraceFilterPanel";
import {
  CATEGORIES,
  SPAN_TYPE_PROPERTY,
  toPanelType,
} from "../components/alertFilterRows";

// The point of the backend type change: an attribute's ClickHouse type drives
// which operators the user is offered, with no manual "Type" selector.
const TYPED_ATTRIBUTES = [
  { key: "customer_tier", type: "string" },
  { key: "confidence_score", type: "number" },
  { key: "cache_hit", type: "boolean" },
];

// Mirrors how AlertFilterBar builds `properties`.
const properties = [
  SPAN_TYPE_PROPERTY,
  ...TYPED_ATTRIBUTES.map((attr) => ({
    id: attr.key,
    name: attr.key,
    category: "attribute",
    rawCategory: "custom_attribute",
    type: toPanelType(attr.type),
    apiColType: "SPAN_ATTRIBUTE",
  })),
];

const renderPanel = (currentFilters) =>
  renderWithRouter(
    <QueryClientProvider
      client={
        new QueryClient({ defaultOptions: { queries: { retry: false } } })
      }
    >
      <TraceFilterPanel
        anchorEl={document.body}
        open
        onClose={vi.fn()}
        onApply={vi.fn()}
        currentFilters={currentFilters}
        properties={properties}
        categories={CATEGORIES}
        projectId="test-project"
        showAi={false}
        showQueryTab={false}
      />
    </QueryClientProvider>,
  );

// Row order is field (a button) -> operator -> value, so the operator is the
// first combobox; text values add a second one.
const operatorOptions = async () => {
  fireEvent.mouseDown(screen.getAllByRole("combobox")[0]);
  const listbox = await screen.findByRole("listbox");
  return within(listbox)
    .getAllByRole("option")
    .map((o) => o.textContent);
};

describe("alert filter operators follow the attribute's ClickHouse type", () => {
  it("offers span type only `is one of` — the API cannot express any other", () => {
    renderPanel([
      {
        field: "observation_type",
        fieldType: "string",
        fieldCategory: "system",
        operator: "in",
        value: ["llm"],
      },
    ]);

    // `is not` would save as the positive; `contains` would be a no-op.
    const options = screen
      .getAllByRole("combobox")
      .slice(0, 1)
      .flatMap(() => {
        fireEvent.mouseDown(screen.getAllByRole("combobox")[0]);
        return within(screen.getByRole("listbox"))
          .getAllByRole("option")
          .map((o) => o.textContent);
      });
    expect(options).toHaveLength(1);
    expect(options[0]).toBe("equals");
  });

  it("renders span type as multi-select, not a single-value radio list", () => {
    renderPanel([
      {
        field: "observation_type",
        fieldType: "string",
        fieldCategory: "system",
        operator: "in",
        value: ["llm", "retriever"],
      },
    ]);

    // `equals` would make the panel single-select while the row holds two
    // values — clicking an option would silently drop one.
    fireEvent.click(screen.getByText(/llm/i));
    expect(screen.getByText(/select one or more values/i)).toBeInTheDocument();
    expect(screen.queryByText(/select a single value/i)).not.toBeInTheDocument();
  });

  it("offers numeric comparisons for a number attribute", async () => {
    renderPanel([
      {
        field: "confidence_score",
        fieldType: "number",
        fieldCategory: "attribute",
        operator: "greater_than",
        value: 0.8,
      },
    ]);

    const options = await operatorOptions();
    expect(options).toEqual(
      expect.arrayContaining([
        "greater than",
        "less than",
        "between",
        "greater than or equals",
      ]),
    );
    expect(options).not.toContain("contains");
  });

  it("narrows a boolean attribute to equality only", async () => {
    renderPanel([
      {
        field: "cache_hit",
        fieldType: "boolean",
        fieldCategory: "attribute",
        operator: "equals",
        value: true,
      },
    ]);

    const options = await operatorOptions();
    expect(options).toEqual(["equals", "not equals", "is null", "is not null"]);
    expect(options).not.toContain("greater than");
    expect(options).not.toContain("contains");
  });

  it("offers text matching for a string attribute", async () => {
    renderPanel([
      {
        field: "customer_tier",
        fieldType: "text",
        fieldCategory: "attribute",
        operator: "equals",
        value: "premium",
      },
    ]);

    const options = await operatorOptions();
    expect(options).toEqual(
      expect.arrayContaining(["contains", "not contains"]),
    );
    expect(options).not.toContain("between");
  });
});
