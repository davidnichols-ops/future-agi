import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useDebounce } from "src/hooks/use-debounce";
import axios, { endpoints } from "src/utils/axios";
import { useEvalsList } from "src/sections/evals/hooks/useEvalsList";
import { paramsSerializer } from "src/utils/utils";

/**
 * Map one raw eval from the old `getEvalsList(sourceId)` endpoint to the
 * picker row shape. The picker also consumes the typed
 * `eval-templates/list` endpoint whose serializer (EvalTemplateListItem)
 * emits snake_case, so this mapper emits the same snake_case shape; the
 * component then reads one canonical key set regardless of which endpoint
 * supplied the row.
 */
export function normalizeOldEndpointEval(e) {
  const owner = e.owner || (e.type === "futureagi_built" ? "system" : "user");
  const eval_type =
    e.eval_type ||
    (e.eval_template_tags?.includes("CODE_EVAL")
      ? "code"
      : e.eval_template_tags?.includes("AGENT_EVAL")
        ? "agent"
        : "llm");
  const created_by_name =
    e.created_by_name || (owner === "system" ? "System" : "User");
  const template_id = e.template_id || e.eval_template_id || e.id;
  return {
    id: template_id,
    template_id,
    user_eval_id: e.template_id ? e.id : undefined,
    name: e.name || e.eval_template_name,
    template_type: e.template_type || "single",
    eval_type,
    output_type: e.output_type || e.output || "pass_fail",
    created_by_name,
    last_updated: e.updated_at || e.created_at,
    current_version: e.current_version || null,
    is_draft: e.is_draft || false,
    required_keys: e.eval_required_keys || e.required_keys || [],
    description: e.description,
    model: e.model || e.selected_model,
    owner,
    eval_template_tags: e.eval_template_tags,
    // Keep original for pass-through
    _original: e,
  };
}

/**
 * Hook for fetching eval list data in the picker context.
 *
 * Uses the old getEvalsList endpoint (which returns ALL evals including system)
 * when a sourceId is available, falls back to listEvalTemplates otherwise.
 */
export function useEvalPickerData({
  sourceId = "",
  enabled = true,
  lockedFilters = null,
} = {}) {
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const [sorting, setSorting] = useState([{ id: "last_updated", desc: true }]);
  const [filters, setFilters] = useState(null);
  const debouncedSearch = useDebounce(searchQuery.trim(), 500);

  const ownerFilter = filters?.owner || "all";
  const apiFilters = useMemo(() => {
    const f = {};
    if (filters?.eval_type) f.eval_type = filters.eval_type;
    if (filters?.output_type) f.output_type = filters.output_type;
    if (filters?.tags) f.tags = filters.tags;
    // Locked filters override and cannot be removed by the user.
    const lf = lockedFilters;
    if (lf?.eval_type) f.eval_type = lf.eval_type;
    if (lf?.output_type) f.output_type = lf.output_type;
    if (lf?.template_type) f.template_type = lf.template_type;
    return Object.keys(f).length > 0 ? f : null;
  }, [filters, lockedFilters]);

  // Use the old endpoint that returns ALL evals (system + user) when sourceId is available
  const oldEndpointQuery = useQuery({
    queryKey: ["eval-picker", "all-evals", sourceId, debouncedSearch],
    queryFn: async () => {
      const params = {};
      if (debouncedSearch) params.search_text = debouncedSearch;
      const { data } = await axios.get(
        endpoints.develop.eval.getEvalsList(sourceId),
        { params, paramsSerializer: paramsSerializer() },
      );
      return data?.result;
    },
    enabled: enabled && !!sourceId,
    keepPreviousData: true,
  });

  // Fallback to the new templates endpoint when no sourceId
  const SORT_FIELD_MAP = {
    name: "name",
    last_updated: "updated_at",
    created_by_name: "created_at",
  };
  const sortBy = sorting[0]
    ? SORT_FIELD_MAP[sorting[0].id] || "updated_at"
    : "updated_at";
  const sortOrder = sorting[0]?.desc ? "desc" : "asc";

  const newEndpointQuery = useEvalsList({
    page,
    pageSize,
    search: debouncedSearch || null,
    ownerFilter,
    filters: apiFilters,
    sortBy,
    sortOrder,
    enabled: enabled && !sourceId,
  });

  // Normalize response — the old endpoint returns { evals: [...], evalRecommendations: [...] }
  // The new endpoint returns { items: [...], total: N }
  const isUsingOldEndpoint = !!sourceId;
  const rawData = isUsingOldEndpoint
    ? oldEndpointQuery.data
    : newEndpointQuery.data;
  const isLoading = isUsingOldEndpoint
    ? oldEndpointQuery.isLoading
    : newEndpointQuery.isLoading;
  const isFetching = isUsingOldEndpoint
    ? oldEndpointQuery.isFetching
    : newEndpointQuery.isFetching;

  // True while the user is typing (debounce window) or the server is fetching
  // results. `isLoading` only flips during the initial load because the
  // queries use keepPreviousData, so we expose this as the search-in-progress
  // signal for the input adornment.
  const isSearchPending = searchQuery.trim() !== debouncedSearch;
  const isSearching = isSearchPending || isFetching;

  const items = useMemo(() => {
    if (!rawData) return [];
    if (isUsingOldEndpoint) {
      const evals = rawData?.evals || [];
      return evals.map(normalizeOldEndpointEval);
    }
    return (rawData?.items || []).map((item) => ({
      ...item,
      template_id: item.template_id || item.eval_template_id || item.id,
    }));
  }, [rawData, isUsingOldEndpoint]);

  // Client-side filtering for the old endpoint. The `getEvalsList(sourceId)`
  // API only accepts `search_text` — it ignores eval_type / output_type /
  // owner / template_type filters. Without this layer the Filter popover
  // looked like it did nothing in the dataset flow.
  const filteredItems = useMemo(() => {
    if (!isUsingOldEndpoint) return items;
    if (!filters && !lockedFilters) return items;
    const lf = lockedFilters;
    const evalTypes = lf?.eval_type || filters?.eval_type;
    const outputTypes = lf?.output_type || filters?.output_type;
    const templateTypes = lf?.template_type;
    const owner = filters?.owner;
    const templateType = filters?.template_type;
    const tags = filters?.tags;
    const nameMatch = filters?.search;
    return items.filter((it) => {
      if (evalTypes?.length && !evalTypes.includes(it.eval_type)) return false;
      if (outputTypes?.length && !outputTypes.includes(it.output_type))
        return false;
      if (owner && owner !== "all" && it.owner !== owner) return false;
      if (templateTypes?.length && !templateTypes.includes(it.template_type))
        return false;
      if (templateType && it.template_type !== templateType) return false;
      if (tags?.length && !tags.some((t) => it.eval_template_tags?.includes(t)))
        return false;
      if (
        nameMatch &&
        !it.name?.toLowerCase().includes(nameMatch.toLowerCase())
      )
        return false;
      return true;
    });
  }, [items, isUsingOldEndpoint, filters, lockedFilters]);

  const total = isUsingOldEndpoint ? filteredItems.length : rawData?.total || 0;

  // Client-side pagination for old endpoint (it returns all evals at once)
  const paginatedItems = useMemo(() => {
    if (!isUsingOldEndpoint) return filteredItems;
    const start = page * pageSize;
    return filteredItems.slice(start, start + pageSize);
  }, [filteredItems, isUsingOldEndpoint, page, pageSize]);

  return {
    items: paginatedItems,
    total,
    isLoading,
    isSearching,
    searchQuery,
    setSearchQuery,
    page,
    setPage,
    pageSize,
    setPageSize,
    sorting,
    setSorting,
    filters,
    setFilters,
  };
}
