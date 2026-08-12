import { accumulateUniqueListContinuations } from "./listCursorPagination";

// `limit_reached` describes one bounded backend walk, not necessarily the end
// of the retained catalog. When the response also carries an advancing signed
// cursor the next explicit Load more action must be able to continue. Only
// `exhausted` is an unconditional terminal browse state.
const TERMINAL_BROWSE_STATUSES = new Set(["exhausted"]);
const FOLLOWED_CURSORS_KEY = "__attributeKeyFollowedCursors";
const CURSOR_STOPPED_KEY = "__attributeKeyCursorStopped";

// The shared Axios client intentionally has no global timeout. Attribute-key
// browsing is interactive, so one stalled proxy/backend response must not
// leave a picker in an endless loading state. Keep this just above the
// server-side 9.5-second ceiling so structured server timeouts still win.
export const ATTRIBUTE_KEY_REQUEST_TIMEOUT_MS = 9_800;

const attributeKey = (item) =>
  typeof item?.key === "string" && item.key.length > 0 ? item.key : null;

const normalizeAttributeKeyPage = (page = {}) =>
  TERMINAL_BROWSE_STATUSES.has(page?.browse_status)
    ? { ...page, has_more: false, next_cursor: null }
    : page;

const stopAttributeKeyCursor = (page, reason) => ({
  ...page,
  [CURSOR_STOPPED_KEY]: reason,
});

export const isAttributeKeyCursorStopped = (page) =>
  typeof page?.[CURSOR_STOPPED_KEY] === "string";

export const getAttributeKeyNextCursor = (page) => {
  if (isAttributeKeyCursorStopped(page)) return undefined;
  const normalized = normalizeAttributeKeyPage(page);
  const cursor = normalized?.next_cursor;
  return normalized?.has_more === true &&
    typeof cursor === "string" &&
    cursor.length > 0
    ? cursor
    : undefined;
};

/**
 * Read one visible attribute-key page.
 *
 * ClickHouse can advance a signed cursor after proving that a bounded physical
 * slice contains no new keys. Such a response is a transport checkpoint, not
 * an empty picker page. Follow advancing checkpoints until a new key arrives
 * or the server proves exhaustion. The shared follower bounds one browser
 * action. If that bound is reached, return the still-advancing checkpoint to
 * the picker so the user can continue with another bounded Load more action;
 * never start an unbounded background request chain.
 */
export const readAttributeKeyPage = async ({
  pageParam,
  pageSize = 10,
  publishedData,
  requestPage,
  signal,
}) => {
  const actionStartedAt = Date.now();
  const isFreshChainRead = pageParam == null;
  const publishedPages = isFreshChainRead ? [] : publishedData?.pages || [];
  const knownKeys = publishedPages.flatMap((page) =>
    (Array.isArray(page?.result) ? page.result : [])
      .map(attributeKey)
      .filter(Boolean),
  );
  const knownCursors = new Set(
    [
      ...(isFreshChainRead ? [] : publishedData?.pageParams || []),
      ...publishedPages.flatMap((page) => page?.[FOLLOWED_CURSORS_KEY] || []),
      pageParam,
    ].filter((cursor) => typeof cursor === "string" && cursor.length > 0),
  );

  const checkedMetadata = (page) => {
    const normalized = normalizeAttributeKeyPage(page);
    if (normalized?.has_more !== true) return normalized;
    const nextCursor = normalized?.next_cursor;
    if (typeof nextCursor !== "string" || nextCursor.length === 0) {
      return stopAttributeKeyCursor(normalized, "malformed_cursor");
    }
    if (knownCursors.has(nextCursor)) {
      return stopAttributeKeyCursor(normalized, "repeated_cursor");
    }
    return normalized;
  };

  // The private marker is the client-side retry contract. Give the shared
  // transport follower a terminal projection so it stops without mutating or
  // impersonating the API response fields published to React Query.
  const continuationMetadata = (page) => {
    const checked = checkedMetadata(page);
    return isAttributeKeyCursorStopped(checked)
      ? { ...checked, has_more: false, next_cursor: null }
      : checked;
  };

  const initialPage = await requestPage(pageParam);
  const {
    response: page,
    rows: visibleRows,
    followedCursors,
  } = await accumulateUniqueListContinuations({
    initialResponse: initialPage,
    rowsFromResponse: (response) =>
      Array.isArray(response?.result) ? response.result : [],
    metadataFromResponse: continuationMetadata,
    identityFromRow: attributeKey,
    knownIdentities: knownKeys,
    targetRowCount: isFreshChainRead ? 1 : pageSize,
    nextResponse: requestPage,
    onContinuation: (metadata) => {
      const nextCursor = getAttributeKeyNextCursor(metadata);
      if (nextCursor) knownCursors.add(nextCursor);
    },
    isCurrent: () => !signal?.aborted,
    cancellationSignal: signal,
    startedAt: actionStartedAt,
  });
  const normalized = checkedMetadata(page);

  return {
    ...normalized,
    // Transport-only and duplicate-only rows are never published to picker
    // consumers. If this bounded action stopped at an advancing checkpoint,
    // next_cursor remains available for the next explicit Load more action.
    result: visibleRows,
    // Store only cursors consumed by this chunk. Copying the cumulative cursor
    // history onto every page makes long sparse catalogs grow quadratically.
    [FOLLOWED_CURSORS_KEY]: followedCursors,
  };
};

export const getNextAttributeKeyPageParam = (
  lastPage,
  allPages,
  lastPageParam,
  allPageParams,
) => {
  const nextCursor = getAttributeKeyNextCursor(lastPage);
  if (!nextCursor) return undefined;

  const consumedCursors = new Set(
    (allPageParams || []).filter(
      (cursor) => typeof cursor === "string" && cursor.length > 0,
    ),
  );
  for (const page of allPages || []) {
    for (const cursor of page?.[FOLLOWED_CURSORS_KEY] || []) {
      consumedCursors.add(cursor);
    }
  }

  return nextCursor === lastPageParam || consumedCursors.has(nextCursor)
    ? undefined
    : nextCursor;
};

/**
 * Detect a cursor protocol failure across already-published React Query pages.
 *
 * A bounded chunk can validate its own cursor hops without consulting cached
 * rows. A later chunk can still return a cursor consumed by an older chunk,
 * though. React Query correctly refuses to fetch that cursor, but an undefined
 * next-page parameter would otherwise look identical to real exhaustion. Keep
 * that state explicitly degraded and retryable instead.
 */
export const isAttributeKeyCursorChainStopped = (data) => {
  const pages = Array.isArray(data?.pages) ? data.pages : [];
  if (pages.some(isAttributeKeyCursorStopped)) return true;
  if (pages.length === 0) return false;

  const pageParams = Array.isArray(data?.pageParams) ? data.pageParams : [];
  const lastPage = pages.at(-1);
  const nextCursor = getAttributeKeyNextCursor(lastPage);
  if (!nextCursor) return false;

  const lastPageParam = pageParams.at(-1);
  return (
    getNextAttributeKeyPageParam(lastPage, pages, lastPageParam, pageParams) ===
    undefined
  );
};

/**
 * Stable identity for one deterministic cursor-protocol stop.
 *
 * Consumers use this to offer one explicit fresh-chain retry without turning a
 * malformed/repeated cursor into an endless Retry loop. If a later request
 * advances to a different physical cursor, it is a new stop and may be retried
 * independently.
 */
export const getAttributeKeyCursorStopSignature = (data) => {
  if (!isAttributeKeyCursorChainStopped(data)) return null;

  const pages = Array.isArray(data?.pages) ? data.pages : [];
  const pageParams = Array.isArray(data?.pageParams) ? data.pageParams : [];
  const lastPage = pages.at(-1) || {};
  const lastPageParam = pageParams.at(-1);

  return JSON.stringify([
    lastPage?.[CURSOR_STOPPED_KEY] || "chain_stopped",
    typeof lastPageParam === "string" ? lastPageParam : null,
    typeof lastPage?.next_cursor === "string" ? lastPage.next_cursor : null,
  ]);
};
