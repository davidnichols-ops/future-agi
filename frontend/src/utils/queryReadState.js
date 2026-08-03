export const QUERY_READ_RETRY_MESSAGE =
  "Results are incomplete. Please retry in a moment.";

export const QUERY_FAILED_RETRY_MESSAGE =
  "We couldn't load this data. Please retry in a moment.";

const payloadCandidates = (payload) =>
  [
    payload,
    payload?.result,
    payload?.metadata,
    payload?.result?.metadata,
  ].filter(Boolean);

/**
 * Interpret the bounded-read metadata returned by tracing APIs.
 *
 * Older deployments do not return this metadata. Treating an absent marker as
 * complete preserves their existing empty-state behaviour during rollout.
 */
export function getQueryReadState(payload, { isError = false } = {}) {
  if (isError) return "error";

  const candidates = payloadCandidates(payload);
  if (candidates.some((candidate) => candidate?.queryReadState === "error")) {
    return "error";
  }

  const degraded = candidates.some(
    (candidate) =>
      candidate?.query_complete === false ||
      candidate?.query_status === "degraded" ||
      candidate?.queryReadState === "degraded",
  );

  return degraded ? "degraded" : "complete";
}

export function getQueryReadMessage(state) {
  if (state === "degraded") return QUERY_READ_RETRY_MESSAGE;
  if (state === "error") return QUERY_FAILED_RETRY_MESSAGE;
  return null;
}

/**
 * Return graph points only when the bounded read is exact.
 *
 * The backend contract keeps incomplete samples out of `data`, but this is a
 * client-side safety boundary as well: a stale or regressed backend response
 * must not be charted as exact traffic/count/cost/token/latency data merely
 * because it carries points alongside `query_complete: false`.
 */
export function getExactGraphData(payload) {
  if (getQueryReadState(payload) !== "complete") return [];

  const data = payload?.data ?? payload?.result?.data;
  return Array.isArray(data) ? data : [];
}

/**
 * Preserve AG Grid's server-side failure semantics while displaying the
 * sanitized read-error overlay. A failed page must never be reported as an
 * empty successful dataset because that can truncate pagination state.
 */
export function failServerSideGridRead(params) {
  params.fail();
  params.api?.showNoRowsOverlay();
}
