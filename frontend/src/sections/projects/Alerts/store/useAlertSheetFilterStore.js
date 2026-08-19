import { create } from "zustand";
import { useShallow } from "zustand/react/shallow";

const hasAnyValue = (filters) =>
  !!filters && Object.values(filters).some((v) => v?.length > 0);

/**
 * FilterPanel emits `{field: [values]}`; the alert-details endpoint takes each
 * of these fields as a scalar.
 */
export const buildIssueFilterParams = (activeFilters) => {
  if (!activeFilters) return null;

  const params = Object.entries(activeFilters).reduce(
    (acc, [field, values]) => {
      if (values?.length) acc[field] = values[0];
      return acc;
    },
    {},
  );

  return Object.keys(params).length > 0 ? params : null;
};

export const useAlertSheetFilterStore = create((set) => ({
  // `{field: [values]}` as produced by FilterPanel, or null when cleared.
  activeFilters: null,
  hasValidFilters: false,

  setActiveFilters: (filters) =>
    set({
      activeFilters: hasAnyValue(filters) ? filters : null,
      hasValidFilters: hasAnyValue(filters),
    }),

  clearAllFilters: () => set({ activeFilters: null, hasValidFilters: false }),

  resetFilters: () => set({ activeFilters: null, hasValidFilters: false }),
}));

export const useAlertSheetFilterShallow = () =>
  useAlertSheetFilterStore(
    useShallow((state) => ({
      activeFilters: state.activeFilters,
      hasValidFilters: state.hasValidFilters,

      setActiveFilters: state.setActiveFilters,
      clearAllFilters: state.clearAllFilters,
      resetFilters: state.resetFilters,
    })),
  );

export const resetAlertSheetFilterStoreState = () => {
  useAlertSheetFilterStore.setState({
    activeFilters: null,
    hasValidFilters: false,
  });
};
