import { useMemo } from "react";
import { create } from "zustand";
import { useShallow } from "zustand/react/shallow";
import { alertTypes } from "../common";

// Fields the alerts list can be filtered on, in FilterPanel's `filterFields`
// shape. `choices` hold the values the API expects; `choiceLabels` map them to
// what the user sees. Everything is single-select except Project, matching what
// the monitor-list endpoint accepts.
const buildAlertTypeField = () => {
  const options = alertTypes.flatMap((group) => group.options);
  return {
    value: "metric_type",
    label: "Alert Type",
    type: "enum",
    operators: ["is"],
    single: true,
    choices: options.map((o) => o.value),
    choiceLabels: Object.fromEntries(options.map((o) => [o.value, o.label])),
  };
};

const STATUS_FIELD = {
  value: "status",
  label: "Status",
  type: "enum",
  operators: ["is"],
  single: true,
  choices: ["triggered", "healthy"],
  choiceLabels: { triggered: "Triggered", healthy: "Healthy" },
};

const buildProjectField = (projectOptions) => ({
  value: "project_id",
  label: "Project",
  type: "enum",
  operators: ["is"],
  choices: projectOptions.map((p) => p.value),
  choiceLabels: Object.fromEntries(
    projectOptions.map((p) => [p.value, p.label]),
  ),
});

// Only `project_id` goes to the API as a repeated param; the rest are scalars.
const MULTI_VALUE_FIELDS = new Set(["project_id"]);

const hasAnyValue = (filters) =>
  !!filters && Object.values(filters).some((v) => v?.length > 0);

/**
 * FilterPanel emits `{field: [values]}`. Translate that into the query params
 * the monitor-list endpoint expects, unwrapping single-select fields back to
 * scalars so the wire format is unchanged.
 */
export const buildAlertFilterParams = (activeFilters) => {
  if (!activeFilters) return null;

  const params = Object.entries(activeFilters).reduce(
    (acc, [field, values]) => {
      if (!values?.length) return acc;
      acc[field] = MULTI_VALUE_FIELDS.has(field) ? values : values[0];
      return acc;
    },
    {},
  );

  return Object.keys(params).length > 0 ? params : null;
};

export const useAlertFilterStore = create((set) => ({
  // `{field: [values]}` as produced by FilterPanel, or null when cleared.
  activeFilters: null,
  hasValidFilters: false,
  projectOptions: [],

  setProjectOptions: (options) =>
    set({
      projectOptions:
        options?.map(({ id, name }) => ({ label: name, value: id })) || [],
    }),

  setActiveFilters: (filters) =>
    set({
      activeFilters: hasAnyValue(filters) ? filters : null,
      hasValidFilters: hasAnyValue(filters),
    }),

  clearAllFilters: () => set({ activeFilters: null, hasValidFilters: false }),

  resetFilters: () => set({ activeFilters: null, hasValidFilters: false }),
}));

export const useAlertFilterShallow = () =>
  useAlertFilterStore(
    useShallow((state) => ({
      activeFilters: state.activeFilters,
      hasValidFilters: state.hasValidFilters,
      projectOptions: state.projectOptions,

      setProjectOptions: state.setProjectOptions,
      setActiveFilters: state.setActiveFilters,
      clearAllFilters: state.clearAllFilters,
      resetFilters: state.resetFilters,
    })),
  );

// Built with useMemo rather than a store getter: FilterPanel compares
// `filterFields` by identity, and a selector that rebuilt the array on every
// read would re-render on each store change.
export const useAlertFilterFields = (mainPage) => {
  const projectOptions = useAlertFilterStore((state) => state.projectOptions);
  return useMemo(
    () => [
      buildAlertTypeField(),
      STATUS_FIELD,
      ...(mainPage ? [buildProjectField(projectOptions)] : []),
    ],
    [mainPage, projectOptions],
  );
};

export const resetAlertFilterStoreState = () => {
  useAlertFilterStore.setState({
    activeFilters: null,
    hasValidFilters: false,
    projectOptions: [],
  });
};
