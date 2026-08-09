import { useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import {
  AGGREGATION_REQUEST_TIMEOUT_MS,
  awaitAggregationRequestWithDeadline,
  getAggregationPollDelay,
  getAggregationRefreshState,
  getExactAggregationReadState,
} from "src/utils/queryReadState";

export const getAgentGraphPresentationState = (query) => {
  const readState = query.data
    ? getExactAggregationReadState(query.data)
    : null;
  const hasExactSnapshot = readState === "complete";
  const { refreshFailed } = getAggregationRefreshState(query.data);
  const failedPendingRefresh =
    readState === "pending" && (refreshFailed || query.isError);
  const hasUnreadablePayload =
    Boolean(query.data) && readState !== "complete" && readState !== "pending";

  return {
    data: hasExactSnapshot ? query.data : undefined,
    isLoading:
      !hasExactSnapshot &&
      !query.isError &&
      (query.isLoading || (readState === "pending" && !failedPendingRefresh)),
    // A polling transport/refresh failure must never hide an exact snapshot
    // already returned by the server. Cold failures still render the generic,
    // retryable error state below the exactness gate.
    isError:
      !hasExactSnapshot &&
      (query.isError || hasUnreadablePayload || failedPendingRefresh),
    queryReadState: readState,
  };
};

/**
 * Fetch an exact aggregate Agent Graph/Path snapshot.
 *
 * Cold reads are background jobs: the hook polls their explicit pending
 * envelope and never exposes its empty arrays as a completed graph. A manual
 * Observe refresh asks the backend to recompute atomically; if a prior exact
 * snapshot exists it remains visible while that refresh runs.
 */
export const useAgentGraph = (
  projectId,
  filters = [],
  { enabled = true } = {},
) => {
  const forceRefreshRef = useRef(false);
  const pollAttemptRef = useRef(0);

  const query = useQuery({
    queryKey: ["agent-graph", projectId, filters],
    queryFn: async ({ signal }) => {
      const refresh = forceRefreshRef.current;
      forceRefreshRef.current = false;
      const response = await awaitAggregationRequestWithDeadline(
        axios.get(endpoints.project.getAgentGraph(), {
          params: {
            project_id: projectId,
            filters: JSON.stringify(filters || []),
            ...(refresh ? { refresh: true } : {}),
          },
          signal,
        }),
        { timeoutMs: AGGREGATION_REQUEST_TIMEOUT_MS },
      );
      return response.data?.result;
    },
    enabled: !!projectId && enabled,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: (activeQuery) => {
      const payload = activeQuery.state.data;
      const { isRefreshing, refreshFailed } =
        getAggregationRefreshState(payload);
      const readState = getExactAggregationReadState(payload);
      if (
        !isRefreshing ||
        refreshFailed ||
        (readState !== "pending" && readState !== "complete")
      ) {
        pollAttemptRef.current = 0;
        return false;
      }
      const delay = getAggregationPollDelay(pollAttemptRef.current);
      pollAttemptRef.current += 1;
      return delay;
    },
    refetchIntervalInBackground: false,
    retry: false,
    meta: { errorHandled: true },
  });
  const { refetch } = query;

  useEffect(() => {
    const handleRefresh = (event) => {
      if (!enabled || !projectId) return;
      if (
        event?.detail?.observeId &&
        String(event.detail.observeId) !== String(projectId)
      ) {
        return;
      }
      forceRefreshRef.current = true;
      pollAttemptRef.current = 0;
      refetch({ cancelRefetch: true });
    };
    window.addEventListener("observe-refresh", handleRefresh);
    return () => window.removeEventListener("observe-refresh", handleRefresh);
  }, [enabled, projectId, refetch]);

  const presentationState = getAgentGraphPresentationState(query);

  return {
    ...query,
    ...presentationState,
  };
};
