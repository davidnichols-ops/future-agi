import { describe, expect, it } from "vitest";

import {
  createListCursorPagination,
  LIST_CURSOR_MODES,
} from "../listCursorPagination";

describe("list cursor pagination", () => {
  it("opts page zero into cursor mode while preserving page-zero compatibility", () => {
    const pagination = createListCursorPagination();

    expect(pagination.requestParams(0, { project_id: "p1" })).toEqual({
      project_id: "p1",
      cursor_mode: true,
      page_number: 0,
    });
  });

  it("uses the returned opaque cursor and omits page_number", () => {
    const pagination = createListCursorPagination();
    pagination.recordResponse(0, {
      has_more: true,
      next_cursor: "signed-page-1",
    });

    expect(pagination.mode()).toBe(LIST_CURSOR_MODES.CURSOR);
    expect(pagination.requestParams(1, { project_id: "p1" })).toEqual({
      project_id: "p1",
      cursor_mode: true,
      cursor: "signed-page-1",
    });
  });

  it("invalidates the continuation chain when the grid query resets", () => {
    const pagination = createListCursorPagination();
    const staleGeneration = pagination.generation();
    pagination.recordResponse(0, {
      has_more: true,
      next_cursor: "stale-cursor",
    });
    pagination.reset();

    expect(pagination.mode()).toBe(LIST_CURSOR_MODES.UNKNOWN);
    expect(pagination.isCurrent(staleGeneration)).toBe(false);
    expect(pagination.requestParams(1, { project_id: "p1" })).toEqual({
      project_id: "p1",
      page_number: 1,
    });
  });

  it("fails closed when a cursor response claims another page without a token", () => {
    const pagination = createListCursorPagination();

    expect(() =>
      pagination.recordResponse(0, {
        has_more: true,
        next_cursor: null,
      }),
    ).toThrow("omitted its continuation cursor");
  });

  it("falls back to numbered pages when page zero is served by a legacy API", () => {
    const pagination = createListCursorPagination();
    pagination.recordResponse(0, { total_rows: 100 });

    expect(pagination.mode()).toBe(LIST_CURSOR_MODES.NUMBERED);
    expect(pagination.requestParams(1, { project_id: "p1" })).toEqual({
      project_id: "p1",
      page_number: 1,
    });
  });

  it("honors has_more=false even when the terminal page is full", () => {
    const pagination = createListCursorPagination();
    const metadata = { has_more: false, next_cursor: null };
    pagination.recordResponse(0, metadata);

    expect(pagination.isLastPage(metadata, 25, 25)).toBe(true);
  });

  it("restarts safely in numbered mode when a cursor hits a legacy API pod", () => {
    const pagination = createListCursorPagination();
    pagination.recordResponse(0, {
      has_more: true,
      next_cursor: "signed-page-1",
    });
    const cursorGeneration = pagination.generation();

    expect(
      pagination.canRecoverFromContinuationError(1, {
        response: { status: 400 },
      }),
    ).toBe(true);
    expect(
      pagination.canRecoverFromContinuationError(1, {
        response: { status: 503 },
      }),
    ).toBe(false);

    pagination.disableCursor();

    expect(pagination.isCurrent(cursorGeneration)).toBe(false);
    expect(pagination.mode()).toBe(LIST_CURSOR_MODES.NUMBERED);
    expect(pagination.requestParams(0, { project_id: "p1" })).toEqual({
      project_id: "p1",
      page_number: 0,
    });
  });

  it("restarts instead of accepting a legacy success as a cursor page", () => {
    const pagination = createListCursorPagination();
    pagination.recordResponse(0, {
      has_more: true,
      next_cursor: "signed-page-1",
    });

    let mixedVersionError;
    try {
      pagination.recordResponse(1, { total_rows: 100 });
    } catch (error) {
      mixedVersionError = error;
    }

    expect(mixedVersionError).toBeInstanceOf(Error);
    expect(
      pagination.canRecoverFromContinuationError(1, mixedVersionError),
    ).toBe(true);
    expect(pagination.mode()).toBe(LIST_CURSOR_MODES.CURSOR);
  });
});
