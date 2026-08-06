import { useInfiniteQuery } from "@tanstack/react-query";
import { useDebounce } from "src/hooks/use-debounce";
import axios, { endpoints } from "src/utils/axios";
import { getQueryReadState } from "src/utils/queryReadState";

const ATTRIBUTE_BROWSE_STATUSES = new Set([
  "continuation",
  "exhausted",
  "limit_reached",
]);

export function getAttributeKeyPageReadState(page, { exact = false } = {}) {
  if (exact && page?.lookup_mode === "exact" && page?.exact_match === true) {
    // A typed latest-state row verified the requested key. The surrounding
    // one-year absence proof may be bounded, but the positive exact match is
    // authoritative and must not inherit browse-sampling UI.
    return "complete";
  }
  if (page?.browse_mode === "recent_suggestions") {
    return page?.query_complete === true &&
      page?.query_status === "complete" &&
      ATTRIBUTE_BROWSE_STATUSES.has(page?.browse_status)
      ? "complete"
      : "degraded";
  }
  return getQueryReadState(page);
}

export function useExactTraceAttributeProperties({
  projectId,
  search,
  source = "traces",
  enabled = true,
  contextKey = "",
}) {
  const debouncedSearch = useDebounce(String(search || "").trim(), 350);
  const supportedSource = source === "traces" || source === "spans";

  const query = useInfiniteQuery({
    queryKey: [
      "trace-attribute-exact",
      projectId,
      source,
      contextKey,
      debouncedSearch,
    ],
    queryFn: ({ signal, pageParam }) =>
      axios
        .get(endpoints.project.spanAttributeKeys(), {
          signal,
          params: {
            project_id: projectId,
            ...(debouncedSearch
              ? { q: debouncedSearch }
              : {
                  page_size: 10,
                  ...(pageParam ? { cursor: pageParam } : {}),
                }),
          },
        })
        .then(({ data }) => data || {}),
    initialPageParam: null,
    getNextPageParam: (lastPage) =>
      !debouncedSearch && lastPage?.has_more && lastPage?.next_cursor
        ? lastPage.next_cursor
        : undefined,
    enabled: enabled && supportedSource && Boolean(projectId),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    // The picker owns a concise retry state; never let the global handler
    // display backend exception text to the customer.
    meta: { errorHandled: true },
  });

  const pages = query.data?.pages || [];
  const seenKeys = new Set();
  const properties = pages.flatMap((page) =>
    (Array.isArray(page?.result) ? page.result : []).flatMap(
      ({ key, type }) => {
        if (!key || seenKeys.has(key)) return [];
        seenKeys.add(key);
        return [
          {
            id: key,
            name: key,
            category: "attribute",
            rawCategory: "custom_attribute",
            type,
            apiColType: "SPAN_ATTRIBUTE",
          },
        ];
      },
    ),
  );
  const pageReadStates = pages.map((page) =>
    getAttributeKeyPageReadState(page, { exact: Boolean(debouncedSearch) }),
  );
  const queryReadState = query.isError
    ? "error"
    : pageReadStates.includes("degraded")
      ? "degraded"
      : pageReadStates.includes("sampled")
        ? "sampled"
        : "complete";
  const lastPage = pages.at(-1);
  const browseStatus = !debouncedSearch ? lastPage?.browse_status : undefined;

  return {
    ...query,
    data: properties,
    queryReadState,
    browseStatus,
    browseLimit: !debouncedSearch ? lastPage?.browse_limit : undefined,
    browseLimitReached: browseStatus === "limit_reached",
    debouncedSearch,
  };
}
