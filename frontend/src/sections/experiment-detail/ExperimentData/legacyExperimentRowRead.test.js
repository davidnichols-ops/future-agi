import { describe, expect, it, vi } from "vitest";
import {
  LEGACY_EXPERIMENT_ROW_REQUEST_TIMEOUT_MS,
  readLegacyExperimentRow,
} from "./legacyExperimentRowRead";

const rowId = "00000000-0000-4000-8000-000000000001";
const nextId = "00000000-0000-4000-8000-000000000002";

const response = (overrides = {}) => ({
  data: {
    status: true,
    result: {
      column_config: [],
      table: [{ row_id: rowId }],
      next_row_ids: [nextId],
      ...overrides,
    },
  },
});

describe("readLegacyExperimentRow", () => {
  it("reads the authoritative snake-case continuation under one wall", async () => {
    const requestRow = vi.fn(({ signal, timeout }) => {
      expect(signal).toBeInstanceOf(AbortSignal);
      expect(timeout).toBe(LEGACY_EXPERIMENT_ROW_REQUEST_TIMEOUT_MS);
      return Promise.resolve(response());
    });

    await expect(readLegacyExperimentRow(requestRow, rowId)).resolves.toEqual([
      nextId,
    ]);
    expect(requestRow).toHaveBeenCalledOnce();
  });

  it.each([
    [{ next_row_ids: null }, "missing continuation"],
    [{ nextRowIds: [nextId], next_row_ids: undefined }, "legacy key only"],
    [{ table: [{ row_id: nextId }] }, "wrong point row"],
    [{ next_row_ids: [nextId, nextId] }, "duplicate continuation"],
    [{ next_row_ids: [rowId] }, "self continuation"],
  ])("fails closed for %s (%s)", async (overrides) => {
    await expect(
      readLegacyExperimentRow(
        () => Promise.resolve(response(overrides)),
        rowId,
      ),
    ).rejects.toMatchObject({ code: "legacy_experiment_row_invalid_response" });
  });

  it("aborts a stalled request at nine seconds", async () => {
    vi.useFakeTimers();
    let requestSignal;
    const request = readLegacyExperimentRow(({ signal }) => {
      requestSignal = signal;
      return new Promise(() => {});
    }, rowId);
    const rejection = expect(request).rejects.toMatchObject({
      code: "aggregation_request_timeout",
    });

    await vi.advanceTimersByTimeAsync(LEGACY_EXPERIMENT_ROW_REQUEST_TIMEOUT_MS);

    await rejection;
    expect(requestSignal.aborted).toBe(true);
    vi.useRealTimers();
  });
});
