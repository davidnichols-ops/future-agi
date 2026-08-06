import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "src/utils/test-utils";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import TraceFilterPanel, {
  buildManualAttributeProperty,
  buildTraceFilterProperties,
  filterPropertiesForPicker,
  getTraceFilterFields,
  normalizeFilterRowOperator,
  toStaticFilterProperty,
} from "../TraceFilterPanel";
import {
  getPickerOptionSearchText,
  getPickerOptionSecondaryLabel,
  normalizePickerValues,
} from "../filterValuePickerUtils";

const parseQueryMock = vi.fn();
const dashboardFilterValuesMock = vi.hoisted(() => vi.fn());
const exactAttributePropertiesMock = vi.hoisted(() => vi.fn());

const defaultDashboardFilterValues = () => ({
  data: [],
  isLoading: false,
  isError: false,
  queryReadState: "complete",
  fetchNextPage: vi.fn(),
  hasNextPage: false,
  isFetchingNextPage: false,
  refetch: vi.fn(),
});

beforeEach(() => {
  dashboardFilterValuesMock.mockReturnValue(defaultDashboardFilterValues());
  exactAttributePropertiesMock.mockReturnValue({
    data: [],
    isFetching: false,
    fetchNextPage: vi.fn(),
    hasNextPage: false,
    isFetchingNextPage: false,
    queryReadState: "complete",
    debouncedSearch: "",
  });
});

describe("JSON array picker value identity", () => {
  it("preserves scalar JSON types and removes only exact duplicates", () => {
    expect(
      normalizePickerValues([
        { value: true, label: "true" },
        { value: 1, label: "1" },
        { value: 1.0, label: "1.0" },
        { value: "1", label: "1" },
        { value: false, label: "false" },
        { value: 0, label: "0" },
        { value: true, label: "duplicate" },
        true,
        7,
        false,
        0,
        "  text  ",
        null,
        Number.NaN,
      ]),
    ).toEqual([true, 1, "1", false, 0, 7, "text"]);
  });
});

vi.mock("src/hooks/use-ai-filter", () => ({
  useAIFilter: () => ({
    parseQuery: parseQueryMock,
    loading: false,
    error: null,
  }),
}));

vi.mock("src/hooks/useDashboards", () => ({
  useDashboardFilterValues: dashboardFilterValuesMock,
}));

vi.mock("../useExactTraceAttributeProperties", () => ({
  useExactTraceAttributeProperties: exactAttributePropertiesMock,
}));

function renderPanel({
  currentFilters = [],
  properties,
  onApply = vi.fn(),
  onClose = vi.fn(),
  open = true,
}) {
  const anchorEl = document.createElement("button");
  document.body.appendChild(anchorEl);
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const utils = render(
    <QueryClientProvider client={queryClient}>
      <TraceFilterPanel
        anchorEl={anchorEl}
        open={open}
        onClose={onClose}
        onApply={onApply}
        currentFilters={currentFilters}
        properties={properties}
        showQueryTab={false}
      />
    </QueryClientProvider>,
  );
  return { anchorEl, onApply, onClose, ...utils };
}

describe("TraceFilterPanel AI apply (#577)", () => {
  beforeEach(() => {
    parseQueryMock.mockReset();
  });

  it("runs the AI filter when the AI query is submitted (Enter)", async () => {
    parseQueryMock.mockResolvedValue([
      { field: "status", operator: "equals", value: "ERROR" },
    ]);
    const onApply = vi.fn();
    const onClose = vi.fn();
    const anchorEl = document.createElement("button");
    document.body.appendChild(anchorEl);
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={queryClient}>
        <TraceFilterPanel
          anchorEl={anchorEl}
          open
          onClose={onClose}
          onApply={onApply}
          currentFilters={[]}
          properties={[
            {
              id: "status",
              name: "Status",
              category: "system",
              type: "string",
            },
          ]}
          showQueryTab={false}
        />
      </QueryClientProvider>,
    );

    const aiInput = screen.getByPlaceholderText(/Ask AI/i);
    fireEvent.change(aiInput, { target: { value: "show errors" } });
    // Auto-apply removed the footer "Apply" button; the AI query is now
    // submitted via Enter (or the inline send button in the input).
    fireEvent.keyDown(aiInput, { key: "Enter" });

    await waitFor(() => {
      expect(parseQueryMock).toHaveBeenCalledWith("show errors", {
        smart: true,
        projectId: undefined,
        source: "traces",
      });
    });
    // The AI path now applies computeValidFilters(converted) like every other
    // path, so the operator is normalized to the canonical string op ("in").
    expect(onApply).toHaveBeenCalledWith([
      {
        field: "status",
        fieldCategory: "system",
        fieldType: "string",
        apiColType: undefined,
        operator: "in",
        value: ["ERROR"],
      },
    ]);
    expect(onClose).toHaveBeenCalled();

    document.body.removeChild(anchorEl);
  });
});

describe("TraceFilterPanel AI apply: additive, empty, single-call", () => {
  const properties = [
    { id: "status", name: "Status", category: "system", type: "string" },
    { id: "language", name: "Language", category: "system", type: "string" },
  ];

  beforeEach(() => {
    parseQueryMock.mockReset();
  });

  it("merges the AI-returned filter with the already-applied filter set", async () => {
    parseQueryMock.mockResolvedValue([
      { field: "language", operator: "equals", value: "english" },
    ]);
    const { anchorEl, onApply } = renderPanel({
      currentFilters: [
        {
          field: "status",
          fieldCategory: "system",
          fieldType: "string",
          operator: "in",
          value: ["ERROR"],
        },
      ],
      properties,
    });

    const aiInput = screen.getByPlaceholderText(/Ask AI/i);
    fireEvent.change(aiInput, { target: { value: "language is english" } });
    fireEvent.keyDown(aiInput, { key: "Enter" });

    await waitFor(() => expect(parseQueryMock).toHaveBeenCalled());
    await waitFor(() => expect(onApply).toHaveBeenCalled());

    const lastCall = onApply.mock.calls[onApply.mock.calls.length - 1][0];
    expect(lastCall).toHaveLength(2);
    expect(lastCall[0]).toMatchObject({ field: "status", value: ["ERROR"] });
    expect(lastCall[1]).toMatchObject({
      field: "language",
      value: ["english"],
    });

    document.body.removeChild(anchorEl);
  });

  it("shows an inline caption when the AI returns an empty filter list", async () => {
    parseQueryMock.mockResolvedValue([]);
    const { anchorEl, onApply, onClose } = renderPanel({
      properties,
    });

    const aiInput = screen.getByPlaceholderText(/Ask AI/i);
    fireEvent.change(aiInput, { target: { value: "gibberish" } });
    fireEvent.keyDown(aiInput, { key: "Enter" });

    await waitFor(() => expect(parseQueryMock).toHaveBeenCalled());
    await waitFor(() =>
      expect(
        screen.getByText(/Could not derive filters from that query/i),
      ).toBeInTheDocument(),
    );

    expect(onApply).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(aiInput.value).toBe("gibberish");

    document.body.removeChild(anchorEl);
  });

  it("clears the empty-result caption when the user edits the query", async () => {
    parseQueryMock.mockResolvedValue([]);
    const { anchorEl } = renderPanel({ properties });

    const aiInput = screen.getByPlaceholderText(/Ask AI/i);
    fireEvent.change(aiInput, { target: { value: "gibberish" } });
    fireEvent.keyDown(aiInput, { key: "Enter" });

    await waitFor(() =>
      expect(
        screen.getByText(/Could not derive filters from that query/i),
      ).toBeInTheDocument(),
    );

    fireEvent.change(aiInput, { target: { value: "gibberish typing more" } });

    expect(
      screen.queryByText(/Could not derive filters from that query/i),
    ).not.toBeInTheDocument();

    document.body.removeChild(anchorEl);
  });

  it("only calls onApply once with the AI filter set on a successful apply", async () => {
    parseQueryMock.mockResolvedValue([
      { field: "status", operator: "equals", value: "ERROR" },
    ]);
    const { anchorEl, onApply } = renderPanel({ properties });

    const aiInput = screen.getByPlaceholderText(/Ask AI/i);
    fireEvent.change(aiInput, { target: { value: "show errors" } });
    fireEvent.keyDown(aiInput, { key: "Enter" });

    await waitFor(() => expect(onApply).toHaveBeenCalledTimes(1));
    const [applied] = onApply.mock.calls[0];
    expect(applied).not.toBeNull();
    expect(applied[0]).toMatchObject({ field: "status" });

    document.body.removeChild(anchorEl);
  });
});

describe("getTraceFilterFields (TH-4571)", () => {
  it("prepends Trace ID when tab is 'trace'", () => {
    const fields = getTraceFilterFields("trace");
    expect(fields[0]).toMatchObject({
      value: "trace_id",
      label: "Trace ID",
      type: "string",
    });
    expect(fields.some((f) => f.value === "span_id")).toBe(false);
  });

  it("offers every backend span kind for node_type and drops the dead 'generation'", () => {
    const nodeType = getTraceFilterFields("trace").find(
      (f) => f.value === "node_type",
    );
    expect(nodeType).toBeTruthy();
    // Every span kind the backend can store must be filterable.
    [
      "chain",
      "retriever",
      "llm",
      "tool",
      "agent",
      "embedding",
      "reranker",
      "guardrail",
      "evaluator",
      "conversation",
      "unknown",
    ].forEach((kind) => expect(nodeType.choices).toContain(kind));
    // `generation` is not an FI span kind (Langfuse's maps to `llm` on ingest).
    expect(nodeType.choices).not.toContain("generation");
  });

  it("prepends Trace ID and Span ID when tab is 'spans'", () => {
    const fields = getTraceFilterFields("spans");
    expect(fields[0]).toMatchObject({ value: "trace_id", label: "Trace ID" });
    expect(fields[1]).toMatchObject({ value: "span_id", label: "Span ID" });
  });

  it("returns base fields unchanged when tab is null/undefined/unknown", () => {
    const fromNull = getTraceFilterFields(null);
    const fromUndefined = getTraceFilterFields(undefined);
    const fromUnknown = getTraceFilterFields("bogus");

    // None of the fallback calls should inject trace_id or span_id
    [fromNull, fromUndefined, fromUnknown].forEach((fields) => {
      expect(fields.some((f) => f.value === "trace_id")).toBe(false);
      expect(fields.some((f) => f.value === "span_id")).toBe(false);
    });

    // All fallbacks must return the same base list (same reference semantics
    // are not required; structural equality is what consumers rely on).
    expect(fromNull).toEqual(fromUndefined);
    expect(fromNull).toEqual(fromUnknown);
  });

  it("uses canonical voice-call fields without remapping global OTel status", () => {
    const fields = getTraceFilterFields("voiceCalls");

    expect(
      fields.find((field) => field.responseKey === "status"),
    ).toMatchObject({
      value: "call_status",
      category: "system",
      apiColType: "SYSTEM_METRIC",
    });
    expect(
      fields.find((field) => field.responseKey === "cost_cents"),
    ).toMatchObject({
      value: "cost_cents",
      type: "number",
      apiColType: "SYSTEM_METRIC",
    });
    expect(
      fields.find((field) => field.responseKey === "duration_seconds"),
    ).toMatchObject({ value: "duration", type: "number" });

    // Normal trace/spans surfaces retain the OTel status column.
    expect(
      getTraceFilterFields("trace").some((field) => field.value === "status"),
    ).toBe(true);
  });
});

describe("voice-call property search aliases", () => {
  const properties = getTraceFilterFields("voiceCalls").map((field) =>
    toStaticFilterProperty(field),
  );

  it("finds the displayed cost field by its Live Preview response key", () => {
    expect(
      filterPropertiesForPicker({ properties, search: "cost_cents" }),
    ).toEqual([
      expect.objectContaining({
        id: "cost_cents",
        name: "Cost (cents)",
        apiColType: "SYSTEM_METRIC",
      }),
    ]);
  });

  it("finds status and uses the normalized voice-list system metric", () => {
    expect(filterPropertiesForPicker({ properties, search: "status" })).toEqual(
      [
        expect.objectContaining({
          id: "call_status",
          category: "system",
          apiColType: "SYSTEM_METRIC",
        }),
      ],
    );
  });

  it("requests provider-normalized status suggestions from the system alias", () => {
    renderPanel({
      properties,
      currentFilters: [
        {
          field: "call_status",
          fieldName: "Status",
          fieldCategory: "system",
          fieldType: "string",
          apiColType: "SYSTEM_METRIC",
          operator: "in",
          value: [],
        },
      ],
    });

    fireEvent.click(
      document.querySelector('[data-filter-value-trigger="call_status"]'),
    );

    expect(dashboardFilterValuesMock).toHaveBeenLastCalledWith(
      expect.objectContaining({
        metricName: "call_status",
        metricType: "system_metric",
        source: "traces",
        pageSize: 10,
        enabled: true,
      }),
    );
  });

  it("shows provider status aliases once under their canonical row status", () => {
    dashboardFilterValuesMock.mockReturnValue({
      ...defaultDashboardFilterValues(),
      data: [
        { value: "ended", label: "ended" },
        { value: "DONE", label: "DONE" },
        { value: "completed", label: "completed" },
      ],
    });
    const { anchorEl } = renderPanel({
      properties,
      currentFilters: [
        {
          field: "call_status",
          fieldName: "Status",
          fieldCategory: "system",
          fieldType: "string",
          apiColType: "SYSTEM_METRIC",
          operator: "in",
          value: ["ended"],
        },
      ],
    });

    fireEvent.click(
      document.querySelector('[data-filter-value-trigger="call_status"]'),
    );

    expect(
      document.querySelectorAll('[data-filter-value-option="completed"]'),
    ).toHaveLength(1);
    expect(
      document.querySelector('[data-filter-value-option="ended"]'),
    ).not.toBeInTheDocument();
    expect(screen.getAllByText("completed").length).toBeGreaterThan(0);

    document.body.removeChild(anchorEl);
  });
});

describe("exact manual attribute fallback", () => {
  it("offers an exact text attribute only after bounded discovery has no exact key", () => {
    expect(
      buildManualAttributeProperty({
        search: "final_status",
        category: "all",
        properties: [],
      }),
    ).toEqual({
      id: "final_status",
      name: "final_status",
      category: "attribute",
      rawCategory: "custom_attribute",
      type: "string",
      apiColType: "SPAN_ATTRIBUTE",
      isManualExactAttribute: true,
    });
  });

  it("keeps the exact backend type and never duplicates an existing attribute", () => {
    expect(
      buildManualAttributeProperty({
        search: "final_status",
        category: "attribute",
        properties: [
          {
            id: "final_status",
            category: "attribute",
            type: "boolean",
          },
        ],
      }),
    ).toBeNull();
  });

  it("does not shadow an exact system property such as voice cost_cents", () => {
    expect(
      buildManualAttributeProperty({
        search: "cost_cents",
        category: "all",
        properties: [
          {
            id: "cost_cents",
            category: "system",
            type: "number",
          },
        ],
      }),
    ).toBeNull();
  });

  it("does not inject attributes into a system-only or specialized picker", () => {
    expect(
      buildManualAttributeProperty({
        search: "final_status",
        category: "system",
        properties: [],
      }),
    ).toBeNull();
    expect(
      buildManualAttributeProperty({
        search: "final_status",
        category: "all",
        properties: [],
        hasCategorySidebar: false,
      }),
    ).toBeNull();
  });

  it("loads the next recent attribute page when the property list is scrolled", () => {
    const fetchNextPage = vi.fn();
    exactAttributePropertiesMock.mockReturnValue({
      data: Array.from({ length: 10 }, (_, index) => ({
        id: `recent_${index}`,
        name: `recent_${index}`,
        category: "attribute",
        rawCategory: "custom_attribute",
        type: "string",
        apiColType: "SPAN_ATTRIBUTE",
      })),
      isFetching: false,
      fetchNextPage,
      hasNextPage: true,
      isFetchingNextPage: false,
      queryReadState: "sampled",
      debouncedSearch: "",
    });
    const { anchorEl } = renderPanel({ properties: [] });

    fireEvent.click(screen.getByRole("button", { name: "Property" }));
    const propertyList = document.querySelector(
      "[data-filter-property-options-list]",
    );
    expect(propertyList).toBeTruthy();
    Object.defineProperties(propertyList, {
      scrollTop: { configurable: true, value: 180 },
      clientHeight: { configurable: true, value: 220 },
      scrollHeight: { configurable: true, value: 400 },
    });
    fireEvent.scroll(propertyList);

    expect(fetchNextPage).toHaveBeenCalledOnce();
    expect(
      screen.queryByText(/results are incomplete/i),
    ).not.toBeInTheDocument();
    document.body.removeChild(anchorEl);
  });
});

describe("filter-value picker bounded-read UX", () => {
  const statusProperty = {
    id: "call.status",
    name: "Status",
    category: "attribute",
    type: "string",
    apiColType: "SPAN_ATTRIBUTE",
  };
  const currentFilters = [
    {
      field: "call.status",
      fieldName: "Status",
      fieldCategory: "attribute",
      fieldType: "string",
      apiColType: "SPAN_ATTRIBUTE",
      operator: "in",
      value: [],
    },
  ];

  const openValuePicker = () => {
    const trigger = document.querySelector(
      '[data-filter-value-trigger="call.status"]',
    );
    expect(trigger).toBeTruthy();
    fireEvent.click(trigger);
  };

  it("renders sampled recent values normally without incomplete-result copy", () => {
    dashboardFilterValuesMock.mockReturnValue({
      ...defaultDashboardFilterValues(),
      data: [{ value: "completed", label: "completed" }],
      queryReadState: "sampled",
    });
    const { anchorEl } = renderPanel({
      currentFilters,
      properties: [statusProperty],
    });

    openValuePicker();

    expect(screen.getByText("completed")).toBeInTheDocument();
    expect(
      screen.getByText("Recent values — search or enter an exact value."),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/results are incomplete/i),
    ).not.toBeInTheDocument();

    document.body.removeChild(anchorEl);
  });

  it("offers Retry and exact free-text entry only for a real request error", () => {
    const refetch = vi.fn();
    dashboardFilterValuesMock.mockReturnValue({
      ...defaultDashboardFilterValues(),
      isError: true,
      queryReadState: "error",
      refetch,
    });
    const { anchorEl } = renderPanel({
      currentFilters,
      properties: [statusProperty],
    });

    openValuePicker();
    expect(
      screen.getByText(
        "Suggestions are temporarily unavailable. Enter an exact value or retry.",
      ),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(refetch).toHaveBeenCalledOnce();

    fireEvent.change(screen.getByPlaceholderText("Search values..."), {
      target: { value: "completed" },
    });
    expect(
      screen.getByText("completed", { selector: "strong" }),
    ).toBeInTheDocument();

    document.body.removeChild(anchorEl);
  });

  it("loads the next value page when the options list reaches the bottom", () => {
    const fetchNextPage = vi.fn();
    dashboardFilterValuesMock.mockReturnValue({
      ...defaultDashboardFilterValues(),
      data: [{ value: "completed", label: "completed" }],
      hasNextPage: true,
      fetchNextPage,
    });
    const { anchorEl } = renderPanel({
      currentFilters,
      properties: [statusProperty],
    });

    openValuePicker();
    const optionsList = document.querySelector(
      "[data-filter-value-options-list]",
    );
    Object.defineProperties(optionsList, {
      scrollTop: { configurable: true, value: 180 },
      clientHeight: { configurable: true, value: 220 },
      scrollHeight: { configurable: true, value: 400 },
    });
    fireEvent.scroll(optionsList);

    expect(fetchNextPage).toHaveBeenCalledOnce();
    document.body.removeChild(anchorEl);
  });
});

describe("toStaticFilterProperty (spans Span Name)", () => {
  const nameField = { value: "name", label: "Trace Name", type: "string" };

  it("remaps the name field to span_name in spans view", () => {
    expect(toStaticFilterProperty(nameField, true)).toMatchObject({
      id: "span_name",
      name: "Span Name",
      type: "string",
    });
  });

  it("keeps the name field as name outside spans view", () => {
    expect(toStaticFilterProperty(nameField, false)).toMatchObject({
      id: "name",
      name: "Trace Name",
    });
  });

  it("does not remap non-name fields in spans view", () => {
    const field = { value: "status", label: "Status", type: "string" };
    expect(toStaticFilterProperty(field, true).id).toBe("status");
  });
});

describe("normalizeFilterRowOperator", () => {
  it("maps list operators to canonical equality panel operators before apply", () => {
    expect(
      normalizeFilterRowOperator({
        field: "status",
        fieldType: "categorical",
        operator: "in",
        value: ["OK"],
      }).operator,
    ).toBe("equals");

    expect(
      normalizeFilterRowOperator({
        field: "status",
        fieldType: "categorical",
        operator: "not_in",
        value: ["ERROR"],
      }).operator,
    ).toBe("not_equals");
  });

  it("keeps canonical number and date ops", () => {
    expect(
      normalizeFilterRowOperator({
        field: "latency_ms",
        fieldType: "number",
        operator: "equals",
        value: "100",
      }).operator,
    ).toBe("equals");

    expect(
      normalizeFilterRowOperator({
        field: "created_at",
        fieldType: "date",
        operator: "less_than",
        value: "2026-05-09T00:00",
      }).operator,
    ).toBe("less_than");
  });

  it("falls back to exact multi-select operators for restricted id fields", () => {
    expect(
      normalizeFilterRowOperator({
        field: "trace_id",
        fieldType: "string",
        operator: "contains",
        value: "abc",
      }).operator,
    ).toBe("in");

    expect(
      normalizeFilterRowOperator({
        field: "span_id",
        fieldType: "string",
        operator: "contains",
        value: "abc",
      }).operator,
    ).toBe("in");
  });

  it("keeps canonical annotation equality operators for the restricted annotator operator", () => {
    expect(
      normalizeFilterRowOperator({
        field: "annotator",
        fieldType: "annotator",
        operator: "equals",
        value: ["user-a", "user-b"],
      }).operator,
    ).toBe("equals");
  });

  it("preserves no-value operators for eval and annotation filter rows", () => {
    for (const fieldType of ["categorical", "thumbs", "annotator", "date"]) {
      expect(
        normalizeFilterRowOperator({
          field: `${fieldType}-field`,
          fieldType,
          operator: "is_null",
          value: "",
        }).operator,
      ).toBe("is_null");
    }
  });
});

describe("annotator annotation filter (TH-4710)", () => {
  it("does not show ended_reason for unrelated property search text (TH-5149)", () => {
    const properties = [
      {
        id: "ended_reason",
        name: "Ended Reason",
        category: "attribute",
        type: "string",
      },
      {
        id: "status",
        name: "Status",
        category: "system",
        type: "string",
      },
    ];

    expect(
      filterPropertiesForPicker({
        properties,
        search: "xqz-not-a-match",
      }),
    ).toEqual([]);
    expect(
      filterPropertiesForPicker({
        properties,
        search: "ended reason",
      }),
    ).toEqual([properties[0]]);
  });

  it("only exposes span-owned metrics when building span filter properties", () => {
    const metrics = [
      {
        name: "latency",
        display_name: "Latency",
        category: "system_metric",
        source: "traces",
        type: "number",
      },
      {
        name: "latency_ms",
        display_name: "Duration",
        category: "system_metric",
        source: "spans",
        sources: ["spans"],
        type: "number",
      },
    ];

    expect(
      buildTraceFilterProperties(metrics, { sourceScope: "traces" }).some(
        (property) => property.id === "latency_ms",
      ),
    ).toBe(false);

    expect(
      buildTraceFilterProperties(metrics, { sourceScope: "spans" }),
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: "latency_ms",
          name: "Duration",
          type: "number",
        }),
      ]),
    );
  });

  it("adds a global Annotator property inside annotation filters", () => {
    const properties = buildTraceFilterProperties([
      {
        name: "latency",
        display_name: "Latency",
        category: "system_metric",
        source: "traces",
        type: "number",
      },
      {
        name: "label-1",
        display_name: "Quality",
        category: "annotation_metric",
        source: "both",
        output_type: "numeric",
      },
    ]);

    const annotator = properties.find(
      (property) => property.id === "annotator",
    );
    expect(annotator).toMatchObject({
      name: "Annotator",
      category: "annotation",
      type: "annotator",
      apiColType: "SYSTEM_METRIC",
      allowCustomValue: false,
    });

    const annotatorIndex = properties.findIndex(
      (property) => property.id === "annotator",
    );
    const labelIndex = properties.findIndex(
      (property) => property.id === "label-1",
    );
    expect(annotatorIndex).toBeLessThan(labelIndex);
  });

  it("maps every annotation label output type to the matching filter input type", () => {
    const properties = buildTraceFilterProperties([
      {
        name: "numeric-label",
        display_name: "Numeric",
        category: "annotation_metric",
        source: "both",
        output_type: "numeric",
      },
      {
        name: "star-label",
        display_name: "Star",
        category: "annotation_metric",
        source: "both",
        output_type: "star",
      },
      {
        name: "text-label",
        display_name: "Text",
        category: "annotation_metric",
        source: "both",
        output_type: "text",
      },
      {
        name: "thumbs-label",
        display_name: "Thumbs",
        category: "annotation_metric",
        source: "both",
        output_type: "thumbs_up_down",
      },
      {
        name: "category-label",
        display_name: "Category",
        category: "annotation_metric",
        source: "both",
        output_type: "categorical",
        choices: ["refund", "billing"],
      },
    ]);

    expect(properties).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "numeric-label", type: "number" }),
        expect.objectContaining({ id: "star-label", type: "number" }),
        expect.objectContaining({ id: "text-label", type: "text" }),
        expect.objectContaining({
          id: "thumbs-label",
          type: "thumbs",
          choices: ["Thumbs Up", "Thumbs Down"],
        }),
        expect.objectContaining({
          id: "category-label",
          type: "categorical",
          choices: ["refund", "billing"],
        }),
      ]),
    );
  });

  it("uses annotator email as secondary display text and searchable text", () => {
    const option = {
      value: "user-1",
      label: "Kartik",
      name: "Kartik",
      email: "kartik.nvj@futureagi.com",
      description: "kartik.nvj@futureagi.com",
    };

    expect(getPickerOptionSecondaryLabel(option)).toBe(
      "kartik.nvj@futureagi.com",
    );
    expect(getPickerOptionSearchText(option)).toContain("Kartik");
    expect(getPickerOptionSearchText(option)).toContain(
      "kartik.nvj@futureagi.com",
    );
    expect(
      getPickerOptionSecondaryLabel({
        value: "user-2",
        label: "reviewer@futureagi.com",
        email: "reviewer@futureagi.com",
      }),
    ).toBe("");
  });
});
