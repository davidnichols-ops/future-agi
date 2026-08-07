import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import AgentPath from "../AgentPath";

describe("AgentPath failure state", () => {
  it("shows a sanitized retry message instead of a false empty state", () => {
    render(<AgentPath data={undefined} isLoading={false} isError />);

    expect(
      screen.getByText(
        "We couldn't load the agent path. Please retry in a moment.",
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("No agent path data available for this time range"),
    ).not.toBeInTheDocument();
  });
});
