import { useQuery } from "@tanstack/react-query";
import { tracerObservationSpanGetEvalAttributesList } from "src/generated/api-contracts/api";
import { useDebounce } from "src/hooks/use-debounce";
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

export function useExactEvalAttributeFields({
  projectId,
  rowType,
  search,
  enabled = true,
}) {
  const normalizedRowType = normalizeExactAttributeRowType(rowType);
  const debouncedSearch = useDebounce(String(search || "").trim(), 350);

  const query = useQuery({
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

  return {
    ...query,
    data: query.data?.fields || [],
    queryReadState: query.isError
      ? "error"
      : query.data?.queryReadState || "complete",
    debouncedSearch,
    isSupportedRowType: Boolean(normalizedRowType),
  };
}
