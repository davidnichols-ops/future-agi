const USER_GLOBAL_SORTABLE_COLUMN_IDS = new Set([
  "user_id",
  "user_id_type",
  "user_id_hash",
  "activated_at",
  "last_active",
  "num_traces",
  "total_tokens",
  "total_cost",
  "input_tokens",
  "output_tokens",
]);

const USER_SORT_DIRECTIONS = new Set(["asc", "desc"]);

export const isUserGlobalSortSupported = (columnId) =>
  USER_GLOBAL_SORTABLE_COLUMN_IDS.has(columnId);

export const sanitizeUserSortModel = (sortModel) => {
  if (!Array.isArray(sortModel)) return [];

  return sortModel
    .filter(
      (sort) =>
        isUserGlobalSortSupported(sort?.colId) &&
        USER_SORT_DIRECTIONS.has(sort?.sort),
    )
    .map(({ colId, sort }) => ({ colId, sort }));
};

export const sanitizeUserColumnState = (columnState) => {
  if (!Array.isArray(columnState)) return [];

  return columnState.map((column) => {
    if (!column || typeof column !== "object") return column;

    const hasValidSort =
      isUserGlobalSortSupported(column.colId) &&
      USER_SORT_DIRECTIONS.has(column.sort);
    if (hasValidSort || (column.sort == null && column.sortIndex == null)) {
      return column;
    }

    return { ...column, sort: null, sortIndex: null };
  });
};
