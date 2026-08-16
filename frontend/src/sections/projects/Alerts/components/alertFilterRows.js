import { AllowedEvalSpanTypes } from "src/utils/constant";
import { getRandomId } from "src/utils/utils";

const OBSERVATION_TYPE_FIELD = "observation_type";

// The form stores one row per selected span type; the panel shows them as a
// single multi-value row.
export const SPAN_TYPE_PROPERTY = {
  id: OBSERVATION_TYPE_FIELD,
  name: "Span Type",
  category: "system",
  rawCategory: "system_metric",
  // Not categorical: the panel drives multi-select off the operator and every
  // categorical operator is single-value. Not "text" either — that renders a
  // free-text box. "string" keeps the choices picker and carries `in`, the
  // only operator the API can express: it takes a value list and applies
  // `observation_type IN (...)`, with nowhere to put an operator.
  type: "string",
  operators: ["in"],
  choices: AllowedEvalSpanTypes.map((t) => t.value),
  choiceLabels: Object.fromEntries(
    AllowedEvalSpanTypes.map((t) => [t.value, t.label]),
  ),
};

export const CATEGORIES = [
  { key: "all", label: "All" },
  { key: "system", label: "Span" },
  { key: "attribute", label: "Attributes" },
];

// CH reports the type from which typed map the key lives in; the panel's
// operator sets are keyed off its own vocabulary.
const PANEL_TYPE_BY_ATTR_TYPE = {
  number: "number",
  boolean: "boolean",
  string: "text",
};

const FILTER_TYPE_BY_PANEL_TYPE = {
  number: "number",
  boolean: "boolean",
};

export const toPanelType = (filterType) =>
  PANEL_TYPE_BY_ATTR_TYPE[filterType] || "text";

const toFilterType = (panelType) =>
  FILTER_TYPE_BY_PANEL_TYPE[panelType] || "text";

/**
 * Form rows → panel rows.
 *
 * Row `fieldType` comes from the row's *stored* `filterType`, never from the
 * attribute's discovered type: a stored type that disagrees is preserved so
 * an untouched alert re-saves byte-identically.
 */
export const toPanelRows = (formRows = []) => {
  const rows = [];
  const spanTypes = [];

  formRows.forEach((row) => {
    if (row?.property === "observationType") {
      const value = row?.filterConfig?.filterValue;
      if (value) spanTypes.push(value);
      return;
    }
    if (row?.property !== "attributes" || !row?.propertyId) return;

    rows.push({
      field: row.propertyId,
      fieldName: row.propertyId,
      fieldCategory: "attribute",
      fieldType: toPanelType(row?.filterConfig?.filterType),
      apiColType: "SPAN_ATTRIBUTE",
      operator: row?.filterConfig?.filterOp || "equals",
      value: row?.filterConfig?.filterValue ?? "",
    });
  });

  if (spanTypes.length > 0) {
    rows.unshift({
      field: OBSERVATION_TYPE_FIELD,
      fieldName: "Span Type",
      fieldCategory: "system",
      fieldType: "string",
      operator: "in",
      value: spanTypes,
    });
  }

  return rows;
};

const LEGACY_SCALAR_OP = { in: "equals", not_in: "not_equals" };

/**
 * The panel has no scalar `equals` for string fields — it stores `in` and
 * labels it "equals", so hydrating a saved row rewrites the operator and wraps
 * the value. Map a single value back to the scalar form the API has always
 * stored; leave genuinely multi-value rows as `in`, the only shape that isn't
 * lossy.
 */
const toLegacyScalarOp = (operator, value) => {
  if (!LEGACY_SCALAR_OP[operator]) return { operator, value };

  const values = Array.isArray(value) ? value : [value];
  if (values.length > 1) return { operator, value };

  return { operator: LEGACY_SCALAR_OP[operator], value: values[0] ?? "" };
};

/** Panel rows → form rows, matching what `transformFilterResponse` produces. */
export const toFormRows = (panelRows = []) => {
  const out = [];

  panelRows.forEach((row) => {
    if (!row?.field) return;

    if (row.field === OBSERVATION_TYPE_FIELD) {
      const values = Array.isArray(row.value)
        ? row.value
        : [row.value].filter(Boolean);
      values.forEach((value) => {
        out.push({
          id: getRandomId(),
          propertyId: "",
          property: "observationType",
          filterConfig: {
            filterType: "text",
            filterOp: "equals",
            filterValue: value,
          },
        });
      });
      return;
    }

    const { operator, value } = toLegacyScalarOp(
      row.operator || "equals",
      row.value,
    );

    out.push({
      id: getRandomId(),
      propertyId: row.field,
      property: "attributes",
      filterConfig: {
        filterType: toFilterType(row.fieldType),
        filterOp: operator,
        filterValue: value,
      },
    });
  });

  return out;
};
