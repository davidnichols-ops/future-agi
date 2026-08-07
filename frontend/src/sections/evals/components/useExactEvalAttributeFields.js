import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { useDebounce } from "src/hooks/use-debounce";
import axios, { endpoints } from "src/utils/axios";
import { getQueryReadState } from "src/utils/queryReadState";

const EXACT_ATTRIBUTE_ROW_TYPES = {
  span: "spans",
  spans: "spans",
  trace: "traces",
  traces: "traces",
};

export function normalizeExactAttributeRowType(rowType) {
  return EXACT_ATTRIBUTE_ROW_TYPES[String(rowType || "").toLowerCase()] || null;
}

export function mergeTracingFieldNames(genericFields, exactFields) {
  return [
    ...new Set(
      [...(genericFields || []), ...(exactFields || [])].filter(
        (field) => typeof field === "string" && field,
      ),
    ),
  ];
}

export function retainedAttributeFieldName(attributeKey, rowType) {
  if (typeof attributeKey !== "string" || !attributeKey) return null;
  return normalizeExactAttributeRowType(rowType) === "traces"
    ? `spans.0.${attributeKey}`
    : attributeKey;
}

function combineQueryReadStates(...states) {
  if (states.includes("error")) return "error";
  if (states.includes("degraded")) return "degraded";
  if (states.includes("sampled")) return "sampled";
  return "complete";
}

export function useExactEvalAttributeFields({
  projectId,
  rowType,
  search,
  enabled = true,
}) {
  const normalizedRowType = normalizeExactAttributeRowType(rowType);
  const debouncedSearch = useDebounce(String(search || "").trim(), 350);
  const exactSearch =
    normalizedRowType === "traces" && debouncedSearch.startsWith("spans.0.")
      ? debouncedSearch.slice("spans.0.".length)
      : debouncedSearch;

  const retainedQuery = useInfiniteQuery({
    // The retained project schema is deliberately independent of the task's
    // preview filters and scheduling window. Search also stays local so typing
    // cannot discard cursor progress through older retained rows.
    queryKey: ["eval-attribute-retained", projectId, normalizedRowType],
    queryFn: ({ signal, pageParam }) =>
      axios
        .get(endpoints.project.spanAttributeKeys(), {
          signal,
          params: {
            project_id: projectId,
            page_size: 10,
            ...(pageParam ? { cursor: pageParam } : {}),
          },
        })
        .then(({ data }) => data || {}),
    initialPageParam: null,
    getNextPageParam: (lastPage) =>
      lastPage?.has_more && lastPage?.next_cursor
        ? lastPage.next_cursor
        : undefined,
    enabled: enabled && Boolean(projectId) && Boolean(normalizedRowType),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    meta: { errorHandled: true },
  });

  // Search the same retained-data endpoint as a supplemental fast path while
  // the base cursor walks older project data.  This request is deliberately
  // non-authoritative: a slow/failed exact lookup must not disable the mapping
  // control, publish a warning, or hide names already loaded by the catalog.
  const exactQuery = useQuery({
    queryKey: [
      "eval-attribute-exact",
      projectId,
      normalizedRowType,
      exactSearch,
    ],
    queryFn: ({ signal }) =>
      axios
        .get(endpoints.project.spanAttributeKeys(), {
          signal,
          params: {
            project_id: projectId,
            page_size: 10,
            q: exactSearch,
          },
        })
        .then(({ data }) => data || {}),
    select: (data) => ({
      fields: (Array.isArray(data?.result) ? data.result : []).flatMap(
        ({ key }) => {
          const field = retainedAttributeFieldName(key, normalizedRowType);
          return field ? [field] : [];
        },
      ),
    }),
    enabled:
      enabled &&
      Boolean(projectId) &&
      Boolean(normalizedRowType) &&
      Boolean(exactSearch),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    // The mapping picker keeps free-text entry available on a failed read;
    // suppress the global backend-exception snackbar for this optional probe.
    meta: { errorHandled: true },
  });

  const retainedPages = retainedQuery.data?.pages || [];
  const seenRetainedKeys = new Set();
  const retainedFields = retainedPages.flatMap((page) =>
    (Array.isArray(page?.result) ? page.result : []).flatMap(({ key }) => {
      if (!key || seenRetainedKeys.has(key)) return [];
      seenRetainedKeys.add(key);
      const field = retainedAttributeFieldName(key, normalizedRowType);
      return field ? [field] : [];
    }),
  );
  const retainedReadState = retainedQuery.isError
    ? "error"
    : combineQueryReadStates(...retainedPages.map(getQueryReadState));
  const exactFields = exactQuery.data?.fields || [];
  const queryReadState = retainedReadState;

  return {
    data: mergeTracingFieldNames(retainedFields, exactFields),
    queryReadState,
    debouncedSearch,
    isSupportedRowType: Boolean(normalizedRowType),
    // Only the retained inventory controls loading/error UI.  Exact search is
    // an opportunistic accelerator and free-text mapping remains available.
    isFetching: retainedQuery.isFetching,
    isLoading: retainedQuery.isLoading,
    isError: retainedQuery.isError,
    isSuccess: retainedQuery.isSuccess,
    error: retainedQuery.error,
    fetchNextPage: retainedQuery.fetchNextPage,
    hasNextPage: retainedQuery.hasNextPage,
    isFetchingNextPage: retainedQuery.isFetchingNextPage,
    isFetchNextPageError: retainedQuery.isFetchNextPageError,
    pageCount: retainedPages.length,
    browseStatus: retainedPages.at(-1)?.browse_status,
  };
}
