const CURSOR_MODE = "cursor";
const NUMBERED_MODE = "numbered";
const UNKNOWN_MODE = "unknown";
const MIXED_VERSION_ERROR_CODE = "LIST_CURSOR_MIXED_VERSION";
const DEFAULT_MAX_EMPTY_CONTINUATIONS = 12;
const DEFAULT_EMPTY_CONTINUATION_DEADLINE_MS = 30_000;

const hasOwn = (value, key) =>
  Object.prototype.hasOwnProperty.call(value || {}, key);

/**
 * Keep the opaque continuation chain for one immutable grid query.
 *
 * Cursor pagination is opt-in. The first response decides the mode: explicit
 * cursor metadata enables keyset continuation; a legacy page-zero response
 * falls back to numbered pages. Once a cursor chain starts, every continuation
 * must reach a cursor-capable API pod, so backend rollout must finish before
 * the cursor-enabled frontend is released.
 */
export const createListCursorPagination = ({
  pageParam = "page_number",
  pageOffset = 0,
} = {}) => {
  if (typeof pageParam !== "string" || pageParam.length === 0) {
    throw new Error("Invalid list page parameter");
  }
  if (!Number.isInteger(pageOffset) || pageOffset < 0) {
    throw new Error("Invalid list page offset");
  }

  let mode = UNKNOWN_MODE;
  let generation = 0;
  const cursorByPage = new Map([[0, null]]);
  const transportCursorByPage = new Map();

  const reset = () => {
    generation += 1;
    mode = UNKNOWN_MODE;
    cursorByPage.clear();
    cursorByPage.set(0, null);
    transportCursorByPage.clear();
  };

  const disableCursor = () => {
    generation += 1;
    mode = NUMBERED_MODE;
    cursorByPage.clear();
    cursorByPage.set(0, null);
    transportCursorByPage.clear();
  };

  const requestParams = (pageNumber, baseParams) => {
    if (!Number.isInteger(pageNumber) || pageNumber < 0) {
      throw new Error("Invalid list page number");
    }

    if (pageNumber === 0) {
      const continuation = transportCursorByPage.get(0) || cursorByPage.get(0);
      if (mode === CURSOR_MODE && continuation) {
        return {
          ...baseParams,
          cursor_mode: true,
          cursor: continuation,
        };
      }
      if (mode === NUMBERED_MODE) {
        return {
          ...baseParams,
          [pageParam]: pageOffset,
        };
      }
      return {
        ...baseParams,
        cursor_mode: true,
        [pageParam]: pageOffset,
      };
    }

    const cursor =
      transportCursorByPage.get(pageNumber) || cursorByPage.get(pageNumber);
    if (mode === CURSOR_MODE) {
      if (!cursor) {
        throw new Error("Continuation cursor is unavailable for this page");
      }
      return {
        ...baseParams,
        cursor_mode: true,
        cursor,
      };
    }

    // An old API response to page zero may not return cursor metadata. Preserve
    // the accepted numbered-page contract for that request chain. Deployment
    // still has to complete the backend rollout before enabling the frontend:
    // a chain that already received a cursor cannot safely switch modes.
    return {
      ...baseParams,
      [pageParam]: pageNumber + pageOffset,
    };
  };

  const recordResponse = (pageNumber, metadata) => {
    const hasCursorContract =
      hasOwn(metadata, "has_more") && hasOwn(metadata, "next_cursor");
    if (!hasCursorContract) {
      if (mode === CURSOR_MODE && pageNumber > 0) {
        const error = new Error(
          "Cursor continuation reached a legacy list API",
        );
        error.code = MIXED_VERSION_ERROR_CODE;
        throw error;
      }
      mode = NUMBERED_MODE;
      transportCursorByPage.delete(pageNumber);
      cursorByPage.delete(pageNumber + 1);
      return;
    }

    mode = CURSOR_MODE;
    if (metadata.has_more === true) {
      if (
        typeof metadata.next_cursor !== "string" ||
        metadata.next_cursor.length === 0
      ) {
        throw new Error("List response omitted its continuation cursor");
      }
      cursorByPage.set(pageNumber + 1, metadata.next_cursor);
      transportCursorByPage.delete(pageNumber);
      return;
    }

    if (metadata.has_more !== false || metadata.next_cursor != null) {
      throw new Error("List response returned invalid cursor metadata");
    }
    cursorByPage.delete(pageNumber + 1);
    transportCursorByPage.delete(pageNumber);
  };

  // A bounded transport page may scan a proven candidate prefix without
  // finding a matching row. Keep the signed checkpoint on the same visible
  // grid page so the caller can follow it immediately; advancing the visible
  // page here would create an empty UI block and misalign later cursors.
  const recordEmptyContinuation = (pageNumber, metadata) => {
    if (
      metadata?.has_more !== true ||
      typeof metadata?.next_cursor !== "string" ||
      metadata.next_cursor.length === 0
    ) {
      throw new Error("Empty list continuation is unavailable");
    }
    if (
      (transportCursorByPage.get(pageNumber) ||
        cursorByPage.get(pageNumber)) === metadata.next_cursor
    ) {
      throw new Error("List API returned a repeated continuation cursor");
    }
    mode = CURSOR_MODE;
    transportCursorByPage.set(pageNumber, metadata.next_cursor);
    cursorByPage.delete(pageNumber + 1);
  };

  const isLastPage = (metadata, rowCount, pageSize) => {
    if (mode === CURSOR_MODE && hasOwn(metadata, "has_more")) {
      return metadata.has_more === false;
    }
    return rowCount < pageSize;
  };

  return {
    reset,
    disableCursor,
    requestParams,
    recordResponse,
    recordEmptyContinuation,
    isLastPage,
    mode: () => mode,
    generation: () => generation,
    isCurrent: (requestGeneration) => requestGeneration === generation,
    canRecoverFromContinuationError: (pageNumber, error) =>
      mode === CURSOR_MODE &&
      pageNumber > 0 &&
      (error?.response?.status === 400 ||
        error?.response?.status === 422 ||
        error?.code === MIXED_VERSION_ERROR_CODE),
  };
};

export const LIST_CURSOR_MODES = Object.freeze({
  CURSOR: CURSOR_MODE,
  NUMBERED: NUMBERED_MODE,
  UNKNOWN: UNKNOWN_MODE,
});

export const listContinuationParams = (baseParams, cursor) => {
  if (typeof cursor !== "string" || cursor.length === 0) {
    throw new Error("Invalid list continuation cursor");
  }
  const { page: _page, page_number: _pageNumber, ...query } = baseParams;
  return { ...query, cursor_mode: true, cursor };
};

/**
 * Return the signed checkpoint for a transport-only empty response.
 *
 * An empty table is not a user-visible empty result while `has_more` is true:
 * the bounded backend scan has only proved that its current candidate prefix
 * contains no matches. Callers must keep this cursor on the same visible page
 * and resume that page instead of publishing an empty row set.
 */
export const getEmptyListContinuation = (rows, metadata) => {
  if (
    Array.isArray(rows) &&
    rows.length === 0 &&
    metadata?.has_more === true &&
    typeof metadata?.next_cursor === "string" &&
    metadata.next_cursor.length > 0
  ) {
    return metadata.next_cursor;
  }
  return null;
};

/** Preserve and asynchronously resume a transport-only page for AG Grid. */
export const resumeEmptyListPage = ({
  rows,
  metadata,
  pagination,
  pageNumber,
  resume,
  schedule = queueMicrotask,
}) => {
  if (!getEmptyListContinuation(rows, metadata)) return false;
  pagination.recordEmptyContinuation(pageNumber, metadata);
  schedule(resume);
  return true;
};

/**
 * Follow checkpoint-only transport pages until the API returns genuine rows
 * or proves the cursor chain is exhausted.  Sparse filters can legitimately
 * classify a bounded prefix without finding a match; exposing that transport
 * page as an empty visible page would be both misleading and would strand
 * older matches behind it.
 */
export const followEmptyListContinuations = async ({
  initialResponse,
  rowsFromResponse,
  metadataFromResponse,
  nextResponse,
  onContinuation,
  isCurrent = () => true,
  maxContinuations = DEFAULT_MAX_EMPTY_CONTINUATIONS,
  maxElapsedMs = DEFAULT_EMPTY_CONTINUATION_DEADLINE_MS,
  now = () => Date.now(),
}) => {
  if (!Number.isInteger(maxContinuations) || maxContinuations < 1) {
    throw new Error("Invalid list continuation limit");
  }
  if (!Number.isFinite(maxElapsedMs) || maxElapsedMs < 1) {
    throw new Error("Invalid list continuation deadline");
  }
  let response = initialResponse;
  const followed = new Set();
  const startedAt = now();
  let rows = rowsFromResponse(response) || [];

  while (rows.length === 0) {
    const metadata = metadataFromResponse(response) || {};
    const nextCursor = metadata.next_cursor;
    if (
      metadata.has_more !== true ||
      typeof nextCursor !== "string" ||
      nextCursor.length === 0
    ) {
      return response;
    }
    if (!isCurrent()) return response;
    // A repeated checkpoint is a malformed continuation chain regardless of
    // whether this request has also reached its local hop/time budget.
    if (followed.has(nextCursor)) {
      throw new Error("List API returned a repeated continuation cursor");
    }
    if (
      followed.size >= maxContinuations ||
      now() - startedAt >= maxElapsedMs
    ) {
      // Sparse exact filters can legitimately need more checkpoints than one
      // browser request should follow. Return the current transport page with
      // its signed continuation intact; the normal page/cursor flow can resume
      // from it without turning a valid sparse result into a user-visible
      // failure or starting an unbounded request fan-out.
      return response;
    }
    followed.add(nextCursor);
    onContinuation?.(metadata);
    response = await nextResponse(nextCursor);
    rows = rowsFromResponse(response) || [];
  }
  return response;
};
