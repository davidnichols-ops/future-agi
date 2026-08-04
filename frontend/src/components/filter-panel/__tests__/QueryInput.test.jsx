import { describe, expect, it, vi } from "vitest";
import { createRef } from "react";
import { act, fireEvent, render, waitFor } from "src/utils/test-utils";
import { QueryInput } from "../FilterPanel";

const selectPhaseOption = async (utils, typed, nextPlaceholder) => {
  const input = utils.getByRole("combobox");
  fireEvent.focus(input);
  fireEvent.change(input, { target: { value: typed } });
  fireEvent.keyDown(input, { key: "ArrowDown" });
  fireEvent.keyDown(input, { key: "Enter" });
  await waitFor(() =>
    expect(utils.getByRole("combobox")).toHaveAttribute(
      "placeholder",
      nextPlaceholder,
    ),
  );
};

const renderQueryInput = ({ field, valueOptions = [], inputRef }) => {
  const onApply = vi.fn();
  const utils = render(
    <QueryInput
      ref={inputRef}
      filterFields={[field]}
      fieldMap={{ [field.value]: field }}
      onApply={onApply}
      valueOptions={valueOptions}
    />,
  );
  return { onApply, utils };
};

describe("QueryInput explicit values", () => {
  it("commits typed text when sampled suggestions are fuzzy, not exact", async () => {
    const field = {
      value: "final_status",
      label: "Final status",
      type: "string",
    };
    const { onApply, utils } = renderQueryInput({
      field,
      valueOptions: ["Rechazado parcialmente"],
    });

    await selectPhaseOption(utils, "Final status", "pick operator...");
    await selectPhaseOption(utils, "Contains", "type or pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "Rechazado" } });
    expect(
      await utils.findByText("Rechazado parcialmente"),
    ).toBeInTheDocument();
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => expect(onApply).toHaveBeenCalledTimes(1));
    expect(onApply).toHaveBeenLastCalledWith([
      {
        field: "final_status",
        operator: "contains",
        value: "Rechazado",
      },
    ]);
  });

  it("commits the exact option even when an earlier suggestion is fuzzy", async () => {
    const field = {
      value: "final_status",
      label: "Final status",
      type: "string",
    };
    const { onApply, utils } = renderQueryInput({
      field,
      valueOptions: [
        { value: "Rechazado parcialmente", label: "Rechazado parcialmente" },
        { value: "Rechazado", label: "Rechazado" },
      ],
    });

    await selectPhaseOption(utils, "Final status", "pick operator...");
    await selectPhaseOption(utils, "Contains", "type or pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "Rechazado" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => expect(onApply).toHaveBeenCalledTimes(1));
    expect(onApply).toHaveBeenLastCalledWith([
      {
        field: "final_status",
        operator: "contains",
        value: "Rechazado",
      },
    ]);
  });

  it.each([
    [false, "false"],
    [0, "0"],
  ])("preserves the typed option id %p", async (optionValue, typedValue) => {
    const field = {
      value: "custom_value",
      label: "Custom value",
      type: "string",
    };
    const { onApply, utils } = renderQueryInput({
      field,
      valueOptions: [{ value: optionValue, label: typedValue }],
    });

    await selectPhaseOption(utils, "Custom value", "pick operator...");
    await selectPhaseOption(utils, "Contains", "type or pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: typedValue } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => expect(onApply).toHaveBeenCalledTimes(1));
    expect(onApply).toHaveBeenLastCalledWith([
      {
        field: "custom_value",
        operator: "contains",
        value: optionValue,
      },
    ]);
  });

  it("does not accept arbitrary text for fields with fixed choices", async () => {
    const inputRef = createRef();
    const field = {
      value: "status",
      label: "Status",
      type: "enum",
      choices: ["OK", "ERROR"],
    };
    const { onApply, utils } = renderQueryInput({ field, inputRef });

    await selectPhaseOption(utils, "Status", "pick operator...");
    await selectPhaseOption(utils, "Is", "pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "WARNING" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onApply).not.toHaveBeenCalled();
    expect(input).toHaveAttribute("placeholder", "pick value...");
    expect(input).toHaveValue("WARNING");
    expect(inputRef.current.flushPartial()).toBeNull();
  });

  it("flushes only a case-insensitive exact fixed choice", async () => {
    const inputRef = createRef();
    const field = {
      value: "status",
      label: "Status",
      type: "enum",
      choices: ["OK", "ERROR"],
    };
    const { onApply, utils } = renderQueryInput({ field, inputRef });

    await selectPhaseOption(utils, "Status", "pick operator...");
    await selectPhaseOption(utils, "Is", "pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "ok" } });

    let flushed;
    act(() => {
      flushed = inputRef.current.flushPartial();
    });
    expect(flushed).toEqual([
      { field: "status", operator: "is_not", value: "OK" },
    ]);
    expect(onApply).not.toHaveBeenCalled();
  });
});
