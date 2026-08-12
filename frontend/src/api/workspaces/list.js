import { useMemo } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { useOrganization } from "src/contexts/OrganizationContext";

export const workspacesListKey = ["workspaces-list"];

const WORKSPACES_PAGE_LIMIT = 100;

const flattenPages = (data) =>
  data.pages.flatMap((page) => page?.data?.results || []);

export function useWorkspacesList({ enabled = true } = {}) {
  const { currentOrganizationId } = useOrganization();

  const query = useInfiniteQuery({
    // A cached list must never be served to a different org.
    queryKey: [...workspacesListKey, currentOrganizationId],
    queryFn: ({ pageParam }) =>
      axios.get(endpoints.workspaces.list, {
        params: { page: pageParam, limit: WORKSPACES_PAGE_LIMIT },
      }),
    getNextPageParam: ({ data }) =>
      data?.next ? data?.current_page + 1 : null,
    initialPageParam: 1,
    staleTime: Infinity,
    select: flattenPages,
    // Firing before the org is known sends no X-Organization-Id.
    enabled: enabled && !!currentOrganizationId,
  });

  // Spreading the result would read every property, marking them all tracked
  // and re-rendering consumers on transitions none of them use. Adding a
  // property here is the price of a consumer needing one.
  return {
    data: query.data,
    fetchNextPage: query.fetchNextPage,
    // A disabled query is still pending, which is what the switcher renders on.
    isPending: query.isPending,
    isFetchingNextPage: query.isFetchingNextPage,
    isError: query.isError,
    // A disabled query is not "loading", but callers have nothing to render.
    // Gate on the same value as `enabled`: org resolution can finish without
    // producing an id — a failed org list still sets isReady — which would
    // otherwise leave this "not loading, no error, no data" forever.
    isLoading: query.isLoading || (enabled && !currentOrganizationId),
  };
}

export function useWorkspaceFromList(workspaceId, { enabled = true } = {}) {
  const query = useWorkspacesList({ enabled: enabled && !!workspaceId });

  const workspace = useMemo(
    () => (query.data || []).find((ws) => ws.id === workspaceId) || null,
    [query.data, workspaceId],
  );

  // `query` is the narrowed object above, not the tracked proxy, so spreading
  // it here reads only plain properties.
  return { ...query, workspace };
}
