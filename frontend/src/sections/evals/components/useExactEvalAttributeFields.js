import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { tracerObservationSpanGetEvalAttributesList } from "src/generated/api-contracts/api";
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

  // Keep the existing exact-name lookup as a supplemental fast path while
  // the retained cursor is still walking older project data. It can surface a
  // typed name immediately, but it is never the inventory source: retained
  // pagination remains available independently of task filters and dates.
  const exactQuery = useQuery({
    queryKey: [
      "eval-attribute-exact",
      projectId,
      normalizedRowType,
      debouncedSearch,
    ],
    queryFn: ({ signal }) =>
      tracerObservationSpanGetEvalAttributesList(
        {
          filters: JSON.stringify({ project_id: projectId }),
          row_type: normalizedRowType,
          q: debouncedSearch,
        },
        { signal },
      ),
    select: ({ data }) => {
      const queryReadState = getQueryReadState(data);
      return {
        // An exact-q response can be degraded because the bounded selector
        // could not finish counting every matching span or sampling the full
        // trace cardinality.  The fields it did return have still passed the
        // latest-state replay, so hiding them makes common attributes vanish
        // on large projects.  Keep those verified suggestions while retaining
        // the degraded state below so the picker still shows its retry warning.
        fields: Array.isArray(data?.result)
          ? data.result.filter((field) => typeof field === "string" && field)
          : [],
        queryReadState,
      };
    },
    enabled:
      enabled &&
      Boolean(projectId) &&
      Boolean(normalizedRowType) &&
      Boolean(debouncedSearch),
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
  const exactReadState = exactQuery.isError
    ? "error"
    : exactQuery.data?.queryReadState || "complete";
  const queryReadState = combineQueryReadStates(
    retainedReadState,
    ...(debouncedSearch ? [exactReadState] : []),
  );

  return {
    data: mergeTracingFieldNames(retainedFields, exactFields),
    queryReadState,
    debouncedSearch,
    isSupportedRowType: Boolean(normalizedRowType),
    isFetching: retainedQuery.isFetching || exactQuery.isFetching,
    isLoading: retainedQuery.isLoading || exactQuery.isLoading,
    isError: retainedQuery.isError || exactQuery.isError,
    isSuccess:
      retainedQuery.isSuccess && (!debouncedSearch || exactQuery.isSuccess),
    error: exactQuery.error || retainedQuery.error,
    fetchNextPage: retainedQuery.fetchNextPage,
    hasNextPage: retainedQuery.hasNextPage,
    isFetchingNextPage: retainedQuery.isFetchingNextPage,
    isFetchNextPageError: retainedQuery.isFetchNextPageError,
    pageCount: retainedPages.length,
    browseStatus: retainedPages.at(-1)?.browse_status,
  };
}
