import { awaitAggregationRequestWithDeadline } from "src/utils/queryReadState";

export const RUN_INSIGHT_LIST_REQUEST_TIMEOUT_MS = 9_000;

/** Bound one Run Insights list request even when the transport stalls. */
export function readRunInsightListPage(requestPage) {
  return awaitAggregationRequestWithDeadline(
    (signal) =>
      requestPage({
        signal,
        timeout: RUN_INSIGHT_LIST_REQUEST_TIMEOUT_MS,
      }),
    { timeoutMs: RUN_INSIGHT_LIST_REQUEST_TIMEOUT_MS },
  );
}
