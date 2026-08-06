import { useCallback, useEffect, useMemo, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { startOfDay, endOfDay, startOfMinute, subDays } from "date-fns";
import axios, { endpoints } from "src/utils/axios";
import {
  getAggregationPollDelay,
  getAggregationRefreshState,
  getExactAggregationReadState,
  getQueryCompletedAt,
} from "src/utils/queryReadState";

const readAggregationResult = (data) => {
  const queryReadState = getExactAggregationReadState(data);
  const { isRefreshing, refreshFailed } = getAggregationRefreshState(data);
  if (queryReadState === "pending") {
    return {
      result: null,
      queryPending: true,
      queryRefreshing: isRefreshing,
      queryRefreshFailed: refreshFailed,
      queryCompletedAt: null,
    };
  }
  if (queryReadState !== "complete") {
    throw new Error("Exact evaluation usage data is not available");
  }
  return {
    result: data?.result || {},
    queryPending: false,
    queryRefreshing: isRefreshing,
    queryRefreshFailed: refreshFailed,
    queryCompletedAt: getQueryCompletedAt(data)?.toISOString() || null,
  };
};

const getRefetchInterval = (pollAttemptRef) => (query) => {
  const data = query.state.data;
  if (!data?.queryRefreshing || data?.queryRefreshFailed) {
    pollAttemptRef.current = 0;
    return false;
  }
  return getAggregationPollDelay(pollAttemptRef.current);
};

/**
 * Compute explicit start/end dates for date options that map to calendar
 * ranges (Today, Yesterday) or custom pickers, so the backend receives the
 * actual window rather than a coarse period string.
 */
function getDateParams(dateOption, dateFilter) {
  if (dateOption === "Today") {
    return {
      start_date: startOfDay(new Date()).toISOString(),
      // Floor to the minute so the query key is stable across renders.
      end_date: startOfMinute(new Date()).toISOString(),
    };
  }
  if (dateOption === "Yesterday") {
    const yesterday = subDays(new Date(), 1);
    return {
      start_date: startOfDay(yesterday).toISOString(),
      end_date: endOfDay(yesterday).toISOString(),
    };
  }
  if (dateOption === "Custom" && dateFilter?.[0] && dateFilter?.[1]) {
    return {
      start_date: new Date(dateFilter[0]).toISOString(),
      end_date: endOfDay(new Date(dateFilter[1])).toISOString(),
    };
  }
  return {};
}

/**
 * Fetch chart + stats for a period. Does NOT depend on page/pageSize.
 */
export function useEvalUsageChart(
  templateId,
  period = "30d",
  dateOption,
  dateFilter,
) {
  const dateParams = useMemo(
    () => getDateParams(dateOption, dateFilter),
    [dateOption, dateFilter],
  );
  const forceRefreshRef = useRef(false);
  const pollAttemptRef = useRef(0);
  const pollingRef = useRef(false);
  useEffect(() => {
    pollAttemptRef.current = 0;
    pollingRef.current = false;
  }, [dateParams, period, templateId]);
  const query = useQuery({
    queryKey: ["evals", "usage-chart", templateId, period, dateParams],
    queryFn: async () => {
      if (pollingRef.current) pollAttemptRef.current += 1;
      const refresh = forceRefreshRef.current;
      forceRefreshRef.current = false;
      const { data } = await axios.get(
        endpoints.develop.eval.getEvalUsage(templateId),
        {
          params: {
            page: 0,
            page_size: 1,
            period,
            ...dateParams,
            ...(refresh ? { refresh: true } : {}),
          },
        },
      );
      const aggregation = readAggregationResult(data);
      pollingRef.current =
        aggregation.queryRefreshing && !aggregation.queryRefreshFailed;
      if (!pollingRef.current) pollAttemptRef.current = 0;
      const result = aggregation.result || {};
      return {
        stats: result.stats,
        chart: result.chart,
        queryPending: aggregation.queryPending,
        queryRefreshing: aggregation.queryRefreshing,
        queryRefreshFailed: aggregation.queryRefreshFailed,
        queryCompletedAt: aggregation.queryCompletedAt,
      };
    },
    enabled:
      !!templateId &&
      !(dateOption === "Custom" && !(dateFilter?.[0] && dateFilter?.[1])),
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: getRefetchInterval(pollAttemptRef),
    refetchIntervalInBackground: false,
    retry: false,
    meta: { errorHandled: true },
  });
  const refetch = query.refetch;
  const refresh = useCallback(() => {
    pollAttemptRef.current = 0;
    pollingRef.current = false;
    forceRefreshRef.current = true;
    return refetch({ cancelRefetch: true });
  }, [refetch]);

  return { ...query, refresh };
}

/**
 * Fetch paginated logs. Keeps previous data while loading next page.
 */
export function useEvalUsageLogs(
  templateId,
  { page = 0, pageSize = 25, period = "30d", dateOption, dateFilter } = {},
) {
  const dateParams = useMemo(
    () => getDateParams(dateOption, dateFilter),
    [dateOption, dateFilter],
  );
  const forceRefreshRef = useRef(false);
  const pollAttemptRef = useRef(0);
  const pollingRef = useRef(false);
  useEffect(() => {
    pollAttemptRef.current = 0;
    pollingRef.current = false;
  }, [dateParams, page, pageSize, period, templateId]);
  const query = useQuery({
    queryKey: [
      "evals",
      "usage-logs",
      templateId,
      period,
      page,
      pageSize,
      dateParams,
    ],
    queryFn: async () => {
      if (pollingRef.current) pollAttemptRef.current += 1;
      const refresh = forceRefreshRef.current;
      forceRefreshRef.current = false;
      const { data } = await axios.get(
        endpoints.develop.eval.getEvalUsage(templateId),
        {
          params: {
            page,
            page_size: pageSize,
            period,
            ...dateParams,
            ...(refresh ? { refresh: true } : {}),
          },
        },
      );
      const aggregation = readAggregationResult(data);
      pollingRef.current =
        aggregation.queryRefreshing && !aggregation.queryRefreshFailed;
      if (!pollingRef.current) pollAttemptRef.current = 0;
      const result = aggregation.result || {};
      return {
        table: result.table || [],
        pagination: result.logs || {},
        queryPending: aggregation.queryPending,
        queryRefreshing: aggregation.queryRefreshing,
        queryRefreshFailed: aggregation.queryRefreshFailed,
        queryCompletedAt: aggregation.queryCompletedAt,
      };
    },
    enabled:
      !!templateId &&
      !(dateOption === "Custom" && !(dateFilter?.[0] && dateFilter?.[1])),
    keepPreviousData: true,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: getRefetchInterval(pollAttemptRef),
    refetchIntervalInBackground: false,
    retry: false,
    meta: { errorHandled: true },
  });
  const refetch = query.refetch;
  const refresh = useCallback(() => {
    pollAttemptRef.current = 0;
    pollingRef.current = false;
    forceRefreshRef.current = true;
    return refetch({ cancelRefetch: true });
  }, [refetch]);

  return { ...query, refresh };
}
