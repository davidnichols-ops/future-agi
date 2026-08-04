import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, waitFor } from "src/utils/test-utils";
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

const renderQueryInput = ({ field, valueOptions = [] }) => {
  const onApply = vi.fn();
  const utils = render(
    <QueryInput
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

  it("does not accept arbitrary text for fields with fixed choices", async () => {
    const field = {
      value: "status",
      label: "Status",
      type: "enum",
      choices: ["OK", "ERROR"],
    };
    const { onApply, utils } = renderQueryInput({ field });

    await selectPhaseOption(utils, "Status", "pick operator...");
    await selectPhaseOption(utils, "Is", "pick value...");

    const input = utils.getByRole("combobox");
    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "WARNING" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(onApply).not.toHaveBeenCalled();
    expect(input).toHaveAttribute("placeholder", "pick value...");
    expect(input).toHaveValue("WARNING");
  });
});
