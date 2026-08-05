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

  return candidates.flatMap((candidate) => [
    candidate,
    ...(candidate?.metadata ? [candidate.metadata] : []),
    ...(Array.isArray(candidate?.metrics) ? candidate.metrics : []),
  ]);
};

const hasBoundedReadMetadata = (candidate) =>
  Object.keys(candidate || {}).some(
    (key) =>
      key === "query_complete" ||
      key === "query_status" ||
      key === "query_sampled" ||
      key.startsWith("query_error_") ||
      key.startsWith("query_sample_") ||
      key.startsWith("query_sampling_"),
  );

const hasValidStatusPair = (candidate) => {
  if (!hasBoundedReadMetadata(candidate)) return true;

  const status = candidate?.query_status;
  const complete = candidate?.query_complete;
  if (typeof complete !== "boolean") return false;

  if (status === "complete") {
    return (
      complete === true &&
      !candidate?.query_error_code &&
      candidate?.query_sampled !== true
    );
  }
  if (status === "sampled") {
    return complete === false && candidate?.query_sampled !== false;
  }
  if (status === "degraded") return complete === false;
  return false;
};

const hasCompleteSamplingCoverage = (candidate) => {
  const planned = candidate?.query_sampling_strata;
  const hasCompletedStrata =
    candidate?.query_complete === false &&
    Boolean(candidate?.query_sampling_strategy) &&
    Number.isInteger(planned) &&
    planned > 0 &&
    candidate?.query_sampling_strata_completed === planned;
  const hasBoundedDashboardSample =
    candidate?.query_complete === false &&
    candidate?.query_error_code === "sample_limit" &&
    candidate?.query_sampling_strategy ===
      "bounded_physical_rows_per_time_bucket" &&
    Number.isInteger(candidate?.query_sampling_interval_seconds) &&
    candidate.query_sampling_interval_seconds > 0 &&
    Number.isInteger(candidate?.query_sample_limit) &&
    candidate.query_sample_limit > 0 &&
    Number.isInteger(candidate?.query_sample_per_bucket) &&
    candidate.query_sample_per_bucket > 0 &&
    candidate.query_sample_per_bucket <= candidate.query_sample_limit;
  return hasCompletedStrata || hasBoundedDashboardSample;
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

  const invalidMetadata = candidates.some(
    (candidate) => !hasValidStatusPair(candidate),
  );

  const sampledCandidates = candidates.filter(
    (candidate) => candidate?.query_status === "sampled",
  );
  const sampled =
    sampledCandidates.length > 0 &&
    sampledCandidates.every(hasCompleteSamplingCoverage);
  const invalidSample = sampledCandidates.some(
    (candidate) => !hasCompleteSamplingCoverage(candidate),
  );
  const degraded = candidates.some(
    (candidate) =>
      candidate?.query_status === "degraded" ||
      (candidate?.query_complete === false &&
        candidate?.query_status !== "sampled") ||
      candidate?.queryReadState === "degraded",
  );

  if (invalidMetadata || degraded || invalidSample) return "degraded";
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
