export const QUERY_READ_RETRY_MESSAGE =
  "Results are incomplete. Please retry in a moment.";

export const QUERY_READ_SAMPLED_MESSAGE =
  "Showing sampled values, not full totals.";

export const QUERY_FAILED_RETRY_MESSAGE =
  "We couldn't load this data. Please retry in a moment.";

const payloadCandidates = (payload) => {
  const candidates = [
    payload,
    payload?.result,
    payload?.metadata,
    payload?.result?.metadata,
  ]
    .flatMap((candidate) =>
      Array.isArray(candidate) ? candidate : [candidate],
    )
    .filter(Boolean);

  return candidates.flatMap((candidate) =>
    candidate?.metadata ? [candidate, candidate.metadata] : [candidate],
  );
};

const hasCompleteSamplingCoverage = (candidate) => {
  const planned = candidate?.query_sampling_strata;
  return (
    Boolean(candidate?.query_sampling_strategy) &&
    Number.isInteger(planned) &&
    planned > 0 &&
    candidate?.query_sampling_strata_completed === planned
  );
};

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

  const sampledCandidates = candidates.filter(
    (candidate) => candidate?.query_status === "sampled",
  );
  const sampled = sampledCandidates.some(hasCompleteSamplingCoverage);
  const invalidSample = sampledCandidates.length > 0 && !sampled;
  const degraded = candidates.some(
    (candidate) =>
      candidate?.query_status === "degraded" ||
      (candidate?.query_complete === false &&
        candidate?.query_status !== "sampled") ||
      candidate?.queryReadState === "degraded",
  );

  if (degraded || invalidSample) return "degraded";
  return sampled ? "sampled" : "complete";
}

export function getQueryReadMessage(state) {
  if (state === "sampled") return QUERY_READ_SAMPLED_MESSAGE;
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
 * Return points that are safe to chart as exact data or as an explicitly
 * labelled bounded sample. Unlabelled incomplete and degraded responses remain
 * non-renderable.
 */
export function getRenderableGraphData(payload) {
  const state = getQueryReadState(payload);
  if (state !== "complete" && state !== "sampled") return [];

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
