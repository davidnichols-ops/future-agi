import { useInfiniteQuery } from "@tanstack/react-query";
import { useDebounce } from "src/hooks/use-debounce";
import axios, { endpoints } from "src/utils/axios";
import { getQueryReadState } from "src/utils/queryReadState";

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
  const pageReadStates = pages.map((page) => getQueryReadState(page));
  const queryReadState = query.isError
    ? "error"
    : pageReadStates.includes("degraded")
      ? "degraded"
      : pageReadStates.includes("sampled")
        ? "sampled"
        : "complete";

  return {
    ...query,
    data: properties,
    queryReadState,
    debouncedSearch,
  };
}
