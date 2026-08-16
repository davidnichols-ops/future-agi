import React from "react";
import { describe, expect, it, vi } from "vitest";
import {
  render,
  screen,
  userEvent,
  waitFor,
  within,
} from "src/utils/test-utils";

import FilterPanel from "../FilterPanel";

// A `single` field carries exactly one value, so a second row pointing at it
// would be merged away on apply while the UI kept showing it as active.
const SINGLE_FIELDS = [
  {
    value: "metric_type",
    label: "Alert Type",
    type: "enum",
    operators: ["is"],
    single: true,
    choices: ["span_response_time"],
    choiceLabels: { span_response_time: "Span response time" },
  },
  {
    value: "status",
    label: "Status",
    type: "enum",
    operators: ["is"],
    single: true,
    choices: ["triggered"],
    choiceLabels: { triggered: "Triggered" },
  },
];

const renderPanel = (fields = SINGLE_FIELDS, onApply = vi.fn()) =>
  render(
    <FilterPanel
      anchorEl={document.body}
      open
      onClose={vi.fn()}
      filterFields={fields}
      currentFilters={null}
      onApply={onApply}
      basicOnly
    />,
  );

// The value list renders in its own popover, where the option text collides
// with the chips already shown in the row.
const openValuePicker = async (user, rowIndex) => {
  await user.click(screen.getAllByText("Select values...")[rowIndex]);
  return within(
    screen
      .getByPlaceholderText("Search values...")
      .closest(".MuiPopover-paper"),
  );
};

describe("FilterPanel — single-value fields", () => {
  it("adds a row for the next unused field instead of duplicating the first", async () => {
    const user = userEvent.setup();
    renderPanel();

    expect(screen.getByText("Alert Type")).toBeInTheDocument();
    expect(screen.queryByText("Status")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /add filter/i }));

    expect(screen.getByText("Status")).toBeInTheDocument();
    expect(screen.getAllByText("Alert Type")).toHaveLength(1);
  });

  it("stops offering new rows once every single-value field is taken", async () => {
    const user = userEvent.setup();
    renderPanel();

    const addFilter = screen.getByRole("button", { name: /add filter/i });
    expect(addFilter).toBeEnabled();

    await user.click(addFilter);

    expect(addFilter).toBeDisabled();
  });

  it("keeps adding rows when the fields allow multiple values", async () => {
    const user = userEvent.setup();
    renderPanel([
      { value: "name", label: "Name", type: "enum", choices: ["a", "b"] },
    ]);

    const addFilter = screen.getByRole("button", { name: /add filter/i });
    await user.click(addFilter);

    // Two rows on a multi-value field merge into one array, which is coherent —
    // the guard must not block it.
    expect(screen.getAllByText("Name")).toHaveLength(2);
    expect(addFilter).toBeEnabled();
  });

  it("sends a value once when two rows on the same field both select it", async () => {
    const user = userEvent.setup();
    const onApply = vi.fn();
    const multiField = [
      {
        value: "project_id",
        label: "Project",
        type: "enum",
        choices: ["p1", "p2"],
      },
    ];
    renderPanel(multiField, onApply);

    const firstRow = await openValuePicker(user, 0);
    await user.click(firstRow.getByText("p1"));
    await user.click(firstRow.getByText("p2"));
    await user.keyboard("{Escape}");

    await user.click(screen.getByRole("button", { name: /add filter/i }));

    const secondRow = await openValuePicker(user, 0);
    await user.click(secondRow.getByText("p1"));
    await user.keyboard("{Escape}");

    await waitFor(
      () =>
        expect(onApply).toHaveBeenLastCalledWith({ project_id: ["p1", "p2"] }),
      { timeout: 2000 },
    );
  });
});
