const CURSOR_MODE = "cursor";
const NUMBERED_MODE = "numbered";
const UNKNOWN_MODE = "unknown";
const MIXED_VERSION_ERROR_CODE = "LIST_CURSOR_MIXED_VERSION";

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
export const createListCursorPagination = () => {
  let mode = UNKNOWN_MODE;
  let generation = 0;
  const cursorByPage = new Map([[0, null]]);

  const reset = () => {
    generation += 1;
    mode = UNKNOWN_MODE;
    cursorByPage.clear();
    cursorByPage.set(0, null);
  };

  const disableCursor = () => {
    generation += 1;
    mode = NUMBERED_MODE;
    cursorByPage.clear();
    cursorByPage.set(0, null);
  };

  const requestParams = (pageNumber, baseParams) => {
    if (!Number.isInteger(pageNumber) || pageNumber < 0) {
      throw new Error("Invalid list page number");
    }

    if (pageNumber === 0) {
      if (mode === NUMBERED_MODE) {
        return {
          ...baseParams,
          page_number: 0,
        };
      }
      return {
        ...baseParams,
        cursor_mode: true,
        page_number: 0,
      };
    }

    const cursor = cursorByPage.get(pageNumber);
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
      page_number: pageNumber,
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
      return;
    }

    if (metadata.has_more !== false || metadata.next_cursor != null) {
      throw new Error("List response returned invalid cursor metadata");
    }
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
    isLastPage,
    mode: () => mode,
    generation: () => generation,
    isCurrent: (requestGeneration) => requestGeneration === generation,
    canRecoverFromContinuationError: (pageNumber, error) =>
      mode === CURSOR_MODE &&
      pageNumber > 0 &&
      (error?.response?.status === 400 ||
        error?.code === MIXED_VERSION_ERROR_CODE),
  };
};

export const LIST_CURSOR_MODES = Object.freeze({
  CURSOR: CURSOR_MODE,
  NUMBERED: NUMBERED_MODE,
  UNKNOWN: UNKNOWN_MODE,
});
