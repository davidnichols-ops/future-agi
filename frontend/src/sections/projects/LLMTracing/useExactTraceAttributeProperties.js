import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { useDebounce } from "src/hooks/use-debounce";
import axios, { endpoints } from "src/utils/axios";
import { getQueryReadState } from "src/utils/queryReadState";
import {
  ATTRIBUTE_KEY_REQUEST_TIMEOUT_MS,
  getAttributeKeyCursorStopSignature,
  getNextAttributeKeyPageParam,
  isAttributeKeyCursorChainStopped,
  readAttributeKeyPage,
} from "./attributeKeyCursorPagination";

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
}) {
  const queryClient = useQueryClient();
  const normalizedSearch = String(search || "").trim();
  const debouncedSearch = useDebounce(normalizedSearch, 350);
  const supportedSource = source === "traces" || source === "spans";
  const retainedQueryKey = ["trace-attribute-retained", projectId, source];
  const exactQueryKey = [
    "trace-attribute-exact",
    projectId,
    source,
    debouncedSearch,
  ];
  const retainedRetryIdentity = JSON.stringify([projectId, source]);
  const exactRetryIdentity = JSON.stringify([
    projectId,
    source,
    debouncedSearch,
  ]);
  const exactRetryGestureIdentity = JSON.stringify([
    projectId,
    source,
    normalizedSearch,
  ]);
  const [cursorRetryState, setCursorRetryState] = useState({
    retained: null,
    exact: null,
  });
  const exactSearchGestureStateRef = useRef({
    scope: null,
    previous: null,
    pendingRetry: null,
  });

  useEffect(() => {
    // A stopped cursor belongs to one exact project/source/search identity.
    // Re-entering the same text after another search is a fresh bounded lookup,
    // so it must not inherit an exhausted retry from the previous session.
    setCursorRetryState((current) =>
      current.exact && current.exact.identity !== exactRetryGestureIdentity
        ? { ...current, exact: null }
        : current,
    );
  }, [exactRetryGestureIdentity]);

  const retainedQuery = useInfiniteQuery({
    // Attribute names describe the retained project schema. Task/dashboard
    // row filters and scheduling windows deliberately do not participate in
    // this cache key or request. Search is supplemental, so typing never
    // discards cursor progress through the retained catalog.
    queryKey: retainedQueryKey,
    queryFn: ({ signal, pageParam }) =>
      readAttributeKeyPage({
        pageParam,
        pageSize: 10,
        publishedData: queryClient.getQueryData(retainedQueryKey),
        signal,
        requestPage: (cursor, requestSignal = signal) =>
          axios
            .get(endpoints.project.spanAttributeKeys(), {
              signal: requestSignal,
              timeout: ATTRIBUTE_KEY_REQUEST_TIMEOUT_MS,
              params: {
                project_id: projectId,
                page_size: 10,
                ...(cursor ? { cursor } : {}),
              },
            })
            .then(({ data }) => data || {}),
      }),
    initialPageParam: null,
    getNextPageParam: getNextAttributeKeyPageParam,
    enabled: enabled && supportedSource && Boolean(projectId),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    // The picker owns a concise retry state; never let the global handler
    // display backend exception text to the customer.
    meta: { errorHandled: true },
  });

  const exactQuery = useInfiniteQuery({
    // The exact cursor uses the indexed typed-Map lane first, then continues
    // the same bounded retained-data walk for JSON-only keys. It supplements
    // the stable catalog so partial text still filters already loaded names.
    queryKey: exactQueryKey,
    queryFn: ({ signal, pageParam }) =>
      readAttributeKeyPage({
        pageParam,
        pageSize: 10,
        publishedData: queryClient.getQueryData(exactQueryKey),
        signal,
        requestPage: (cursor, requestSignal = signal) =>
          axios
            .get(endpoints.project.spanAttributeKeys(), {
              signal: requestSignal,
              timeout: ATTRIBUTE_KEY_REQUEST_TIMEOUT_MS,
              params: {
                project_id: projectId,
                page_size: 10,
                q: debouncedSearch,
                ...(cursor ? { cursor } : {}),
              },
            })
            .then(({ data }) => data || {}),
      }),
    initialPageParam: null,
    getNextPageParam: getNextAttributeKeyPageParam,
    enabled:
      enabled &&
      supportedSource &&
      Boolean(projectId) &&
      Boolean(debouncedSearch),
    retry: false,
    staleTime: 60_000,
    gcTime: 5 * 60_000,
    meta: { errorHandled: true },
  });

  useEffect(() => {
    const scope = JSON.stringify([projectId, source]);
    const identity = JSON.stringify([projectId, source, normalizedSearch]);
    const state = exactSearchGestureStateRef.current;
    if (state.scope !== scope) {
      state.scope = scope;
      state.previous = null;
      state.pendingRetry = null;
    }
    if (state.previous === identity) return;
    state.previous = identity;
    if (!enabled || !normalizedSearch) {
      state.pendingRetry = null;
      return;
    }
    // Every non-empty raw transition may recover an already-cached failed
    // query. A genuinely new query is fetching and consumes this marker below
    // without issuing a second request.
    state.pendingRetry = identity;
  }, [enabled, normalizedSearch, projectId, source]);

  useEffect(() => {
    const identity = JSON.stringify([projectId, source, normalizedSearch]);
    const state = exactSearchGestureStateRef.current;
    if (
      state.pendingRetry !== identity ||
      !normalizedSearch ||
      debouncedSearch !== normalizedSearch
    ) {
      return;
    }
    if (exactQuery.isFetching) {
      state.pendingRetry = null;
      return;
    }
    const continuationFailed =
      exactQuery.isFetchNextPageError && exactQuery.hasNextPage;
    const cachedReadFailed = exactQuery.isError || exactQuery.isRefetchError;
    if (!continuationFailed && !cachedReadFailed) {
      state.pendingRetry = null;
      return;
    }

    // Consume the re-entry before starting I/O. Query state changes cannot
    // trigger another automatic request; another retry requires another raw
    // search gesture or the explicit picker action.
    state.pendingRetry = null;
    if (continuationFailed) void exactQuery.fetchNextPage();
    else void exactQuery.refetch();
  }, [debouncedSearch, exactQuery, normalizedSearch, projectId, source]);

  const retainedPages = retainedQuery.data?.pages || [];
  const exactPages = exactQuery.data?.pages || [];
  const retainedCursorStopped = isAttributeKeyCursorChainStopped(
    retainedQuery.data,
  );
  const exactCursorStopped = isAttributeKeyCursorChainStopped(exactQuery.data);
  const retainedStopSignature = getAttributeKeyCursorStopSignature(
    retainedQuery.data,
  );
  const exactStopSignature = getAttributeKeyCursorStopSignature(
    exactQuery.data,
  );
  const retainedStopRetryAttempted = Boolean(
    retainedStopSignature &&
      cursorRetryState.retained?.identity === retainedRetryIdentity &&
      cursorRetryState.retained?.signature === retainedStopSignature,
  );
  const exactStopRetryAttempted = Boolean(
    exactStopSignature &&
      cursorRetryState.exact?.identity === exactRetryIdentity &&
      cursorRetryState.exact?.signature === exactStopSignature,
  );
  const retainedStoppedRetryAvailable =
    retainedCursorStopped && !retainedStopRetryAttempted;
  const exactStoppedRetryAvailable =
    exactCursorStopped && !exactStopRetryAttempted;
  // During an exact lookup, prefer its typed row over a duplicate from an
  // earlier generic catalog page. The exact row may be the only one carrying
  // authoritative mixed/structured type metadata needed by filter controls.
  const pages = debouncedSearch
    ? [...exactPages, ...retainedPages]
    : retainedPages;
  const seenKeys = new Set();
  const properties = pages.flatMap((page) =>
    (Array.isArray(page?.result) ? page.result : []).flatMap(
      ({ key, type, types, types_exact: typesExact }) => {
        if (!key || seenKeys.has(key)) return [];
        seenKeys.add(key);
        return [
          {
            id: key,
            name: key,
            category: "attribute",
            rawCategory: "custom_attribute",
            type,
            attributeTypes:
              Array.isArray(types) && types.length > 0 ? types : [type],
            // Key discovery is deliberately bounded. Even a positive exact-key
            // lookup proves existence, not that the first observed storage type
            // is the only type in the full window. Consumers may pin a typed
            // value query only when the server explicitly certifies coverage.
            attributeTypesExact: typesExact === true,
            apiColType: "SPAN_ATTRIBUTE",
          },
        ];
      },
    ),
  );
  // The retained cursor is the authoritative property inventory. The exact-q
  // chain is a supplemental accelerator for one raw key, so its timeout or
  // protocol stop must not hide already-loaded partial matches or turn their
  // picker into a global error. Keep the exact failure separately retryable.
  const retainedPageReadStates = retainedPages.map((page) =>
    getAttributeKeyPageReadState(page),
  );
  const queryReadState = retainedQuery.isError
    ? "error"
    : retainedCursorStopped
      ? "degraded"
      : retainedPageReadStates.includes("degraded")
        ? "degraded"
        : retainedPageReadStates.includes("sampled")
          ? "sampled"
          : "complete";
  const retainedLastPage = retainedPages.at(-1);
  const exactSearchMatched = Boolean(
    debouncedSearch &&
      exactPages.some(
        (page) =>
          page?.exact_match === true ||
          (Array.isArray(page?.result) &&
            page.result.some(({ key }) => key === debouncedSearch)),
      ),
  );
  const browseStatus = retainedLastPage?.browse_status;
  const retainedHasNextPage =
    retainedQuery.hasNextPage || retainedStoppedRetryAvailable;
  const shouldAdvanceExact = Boolean(debouncedSearch) && !exactSearchMatched;
  // A failed page-one exact probe has no pageParam to retry through
  // `fetchNextPage`; refetching the same query key correctly starts from a
  // cursorless page one. A failed later page keeps `hasNextPage` and retries
  // that same signed continuation instead.
  const exactInitialError = Boolean(
    shouldAdvanceExact && exactQuery.isError && !exactQuery.data,
  );
  const exactRefetchError = Boolean(
    shouldAdvanceExact && exactQuery.isRefetchError && exactQuery.data,
  );
  const exactHasNextPage =
    shouldAdvanceExact &&
    (exactQuery.hasNextPage ||
      exactStoppedRetryAvailable ||
      exactInitialError ||
      exactRefetchError);
  // Give the exact-q chain first priority, then let a later deliberate gesture
  // resume the cached retained cursor. A positive exact identity is terminal
  // only for the supplemental exact chain: it must not hide sibling retained
  // keys such as `foo_archive` from a search for `foo`. These predicates are
  // mutually exclusive, so one gesture still issues only one request.
  const shouldAdvanceRetained = !debouncedSearch || !exactHasNextPage;
  const hasNextPage =
    exactHasNextPage || (shouldAdvanceRetained && retainedHasNextPage);
  const fetchNextPage = (...args) => {
    const reads = [];
    if (shouldAdvanceRetained && retainedStoppedRetryAvailable) {
      setCursorRetryState((current) => ({
        ...current,
        retained: {
          identity: retainedRetryIdentity,
          signature: retainedStopSignature,
        },
      }));
      reads.push(retainedQuery.refetch(...args));
    } else if (shouldAdvanceRetained && retainedQuery.hasNextPage) {
      reads.push(retainedQuery.fetchNextPage(...args));
    }
    if (shouldAdvanceExact && exactStoppedRetryAvailable) {
      setCursorRetryState((current) => ({
        ...current,
        exact: {
          identity: exactRetryIdentity,
          signature: exactStopSignature,
        },
      }));
      reads.push(exactQuery.refetch(...args));
    } else if (shouldAdvanceExact && exactQuery.hasNextPage) {
      reads.push(exactQuery.fetchNextPage(...args));
    } else if (exactInitialError) {
      reads.push(exactQuery.refetch(...args));
    } else if (exactRefetchError) {
      reads.push(exactQuery.refetch(...args));
    }
    return reads.length === 1 ? reads[0] : Promise.allSettled(reads);
  };
  const fetchNextExactPage = (...args) => {
    if (!shouldAdvanceExact) return Promise.resolve();
    if (exactStoppedRetryAvailable) {
      setCursorRetryState((current) => ({
        ...current,
        exact: {
          identity: exactRetryIdentity,
          signature: exactStopSignature,
        },
      }));
      return exactQuery.refetch(...args);
    }
    if (exactQuery.hasNextPage) return exactQuery.fetchNextPage(...args);
    if (exactInitialError) return exactQuery.refetch(...args);
    if (exactRefetchError) return exactQuery.refetch(...args);
    return Promise.resolve();
  };
  const refetch = (...args) => {
    const reads = [retainedQuery.refetch(...args)];
    if (debouncedSearch) reads.push(exactQuery.refetch(...args));
    return Promise.allSettled(reads);
  };

  return {
    data: properties,
    queryReadState,
    browseStatus,
    browseLimit: retainedLastPage?.browse_limit,
    browseLimitReached: browseStatus === "limit_reached" && !hasNextPage,
    // This is intentionally raw-key/backend identity, not the picker's fuzzy
    // punctuation-normalized match. It terminates only the supplemental exact
    // chain; the retained catalog may still contain distinct sibling keys.
    exactSearchMatched,
    debouncedSearch,
    isFetching:
      retainedQuery.isFetching ||
      (Boolean(debouncedSearch) && exactQuery.isFetching),
    isLoading: retainedQuery.isLoading,
    isError: retainedQuery.isError,
    isSuccess: retainedQuery.isSuccess,
    error: retainedQuery.error,
    // Optional exact lookup failures stay visible to the search controls
    // without poisoning the authoritative retained inventory above.
    exactSearchError:
      Boolean(debouncedSearch) && exactQuery.isError ? exactQuery.error : null,
    hasNextPage,
    // Exact typed search has its own cursor chain. The property picker may
    // advance this once automatically for a settled search without also
    // walking the unrelated retained-catalog cursor.
    hasNextExactPage: exactHasNextPage,
    fetchNextExactPage,
    isFetchingExactSearch: Boolean(debouncedSearch) && exactQuery.isFetching,
    isFetchingNextExactPage:
      (shouldAdvanceExact && exactQuery.isFetchingNextPage) ||
      (shouldAdvanceExact &&
        (exactCursorStopped || exactInitialError) &&
        exactQuery.isFetching),
    // One action advances exactly one chain: exact-q first, then the retained
    // catalog after exact absence. This keeps partial local search reachable
    // without racing two unrelated signed cursors.
    fetchNextPage,
    refetch,
    isFetchingNextPage:
      (shouldAdvanceRetained && retainedQuery.isFetchingNextPage) ||
      (shouldAdvanceExact && exactQuery.isFetchingNextPage) ||
      (shouldAdvanceRetained &&
        retainedCursorStopped &&
        retainedQuery.isFetching) ||
      (shouldAdvanceExact && exactCursorStopped && exactQuery.isFetching),
    isFetchNextPageError:
      (shouldAdvanceRetained && retainedQuery.isFetchNextPageError) ||
      (shouldAdvanceExact && exactQuery.isFetchNextPageError) ||
      exactInitialError ||
      exactRefetchError ||
      (shouldAdvanceRetained && retainedStoppedRetryAvailable) ||
      (shouldAdvanceExact && exactStoppedRetryAvailable),
    cursorRetryExhausted:
      (shouldAdvanceRetained &&
        retainedCursorStopped &&
        retainedStopRetryAttempted) ||
      (shouldAdvanceExact && exactCursorStopped && exactStopRetryAttempted),
    // Consumers use this completion revision to unlock exactly one new
    // scroll-to-load action even when a valid continuation page contains no
    // previously unseen keys and therefore leaves `data.length` unchanged.
    pageCount: retainedPages.length + exactPages.length,
  };
}
