import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import {
  AGGREGATION_POLL_MAX_CONSECUTIVE_FAILURES,
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
  const consecutiveFailureRef = useRef(0);
  const serverPendingRef = useRef(false);
  const requestScopeRef = useRef(null);
  const [aggregationTransportFailed, setAggregationTransportFailed] =
    useState(false);

  const query = useQuery({
    queryKey: ["agent-graph", projectId, filters],
    queryFn: async ({ queryKey, signal }) => {
      const requestScope = JSON.stringify(queryKey);
      if (requestScopeRef.current !== requestScope) {
        requestScopeRef.current = requestScope;
        pollAttemptRef.current = 0;
        consecutiveFailureRef.current = 0;
        serverPendingRef.current = false;
        setAggregationTransportFailed(false);
      }
      const refresh = forceRefreshRef.current;
      forceRefreshRef.current = false;
      try {
        const response = await awaitAggregationRequestWithDeadline(
          (requestSignal) =>
            axios.get(endpoints.project.getAgentGraph(), {
              params: {
                project_id: projectId,
                filters: JSON.stringify(filters || []),
                ...(refresh ? { refresh: true } : {}),
              },
              signal: requestSignal,
            }),
          { timeoutMs: AGGREGATION_REQUEST_TIMEOUT_MS, signal },
        );
        const result = response.data?.result;
        const { isRefreshing, refreshFailed } =
          getAggregationRefreshState(result);
        const readState = getExactAggregationReadState(result);
        serverPendingRef.current =
          isRefreshing &&
          !refreshFailed &&
          (readState === "pending" || readState === "complete");
        consecutiveFailureRef.current = 0;
        setAggregationTransportFailed(false);
        return result;
      } catch (error) {
        if (!signal.aborted && serverPendingRef.current) {
          consecutiveFailureRef.current += 1;
          if (
            consecutiveFailureRef.current >=
            AGGREGATION_POLL_MAX_CONSECUTIVE_FAILURES
          ) {
            serverPendingRef.current = false;
            setAggregationTransportFailed(true);
          }
        }
        throw error;
      }
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
        consecutiveFailureRef.current >=
          AGGREGATION_POLL_MAX_CONSECUTIVE_FAILURES ||
        !isRefreshing ||
        refreshFailed ||
        (readState !== "pending" && readState !== "complete")
      ) {
        pollAttemptRef.current = 0;
        serverPendingRef.current = false;
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
      consecutiveFailureRef.current = 0;
      serverPendingRef.current = false;
      setAggregationTransportFailed(false);
      refetch({ cancelRefetch: true });
    };
    window.addEventListener("observe-refresh", handleRefresh);
    return () => window.removeEventListener("observe-refresh", handleRefresh);
  }, [enabled, projectId, refetch]);

  const pollingTransportError =
    query.isError &&
    serverPendingRef.current &&
    consecutiveFailureRef.current < AGGREGATION_POLL_MAX_CONSECUTIVE_FAILURES;
  const presentationState = getAgentGraphPresentationState({
    ...query,
    isError:
      aggregationTransportFailed || (query.isError && !pollingTransportError),
  });

  return {
    ...query,
    ...presentationState,
  };
};
