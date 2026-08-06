import { describe, expect, it } from "vitest";
import { computeSankeyLayout } from "../agentPathUtils";

const nodes = [
  { id: "agent:root", name: "root", type: "agent", span_count: 3 },
  { id: "tool:lookup", name: "lookup", type: "tool", span_count: 2 },
  { id: "llm:answer", name: "answer", type: "llm", span_count: 2 },
];

describe("computeSankeyLayout Agent Path contract", () => {
  it("uses chronological path_edges instead of hierarchy edges", () => {
    const layout = computeSankeyLayout({
      nodes,
      edges: [
        { source: "agent:root", target: "tool:lookup", transition_count: 99 },
      ],
      path_edges: [
        { source: "tool:lookup", target: "llm:answer", transition_count: 7 },
      ],
    });

    expect(layout.flows).toEqual([
      expect.objectContaining({
        source: "tool:lookup",
        target: "llm:answer",
        count: 7,
      }),
    ]);
  });

  it("does not fall back to hierarchy when an exact path is explicitly empty", () => {
    const layout = computeSankeyLayout({
      nodes,
      edges: [
        { source: "agent:root", target: "tool:lookup", transition_count: 99 },
      ],
      path_edges: [],
    });

    expect(layout.flows).toEqual([]);
    expect(layout.columns.flatMap((column) => column.nodes)).toHaveLength(3);
  });

  it("accepts camelCase pathEdges during generated-client normalization", () => {
    const layout = computeSankeyLayout({
      nodes,
      pathEdges: [
        { source: "agent:root", target: "llm:answer", transitionCount: 2 },
      ],
    });

    expect(layout.flows[0]).toEqual(
      expect.objectContaining({ count: 2, target: "llm:answer" }),
    );
  });
});
