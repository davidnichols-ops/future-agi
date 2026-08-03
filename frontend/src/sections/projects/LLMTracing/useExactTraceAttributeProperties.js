import { useQuery } from "@tanstack/react-query";
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

  const query = useQuery({
    queryKey: [
      "trace-attribute-exact",
      projectId,
      source,
      contextKey,
      debouncedSearch,
    ],
    queryFn: ({ signal }) =>
      axios.get(endpoints.project.spanAttributeKeys(), {
        signal,
        params: {
          project_id: projectId,
          q: debouncedSearch,
        },
      }),
    select: ({ data }) => {
      const attributes = Array.isArray(data?.result) ? data.result : [];
      return {
        properties: attributes.map(({ key, type }) => ({
          id: key,
          name: key,
          category: "attribute",
          rawCategory: "custom_attribute",
          type,
          apiColType: "SPAN_ATTRIBUTE",
        })),
        queryReadState: getQueryReadState(data),
      };
    },
    enabled:
      enabled &&
      supportedSource &&
      Boolean(projectId) &&
      Boolean(debouncedSearch),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    // The picker owns a concise retry state; never let the global handler
    // display backend exception text to the customer.
    meta: { errorHandled: true },
  });

  return {
    ...query,
    data: query.data?.properties || [],
    queryReadState: query.isError
      ? "error"
      : query.data?.queryReadState || "complete",
    debouncedSearch,
  };
}
