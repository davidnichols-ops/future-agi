import { RESPONSE_CODES } from "./constants";

const DEFAULT_RATE_LIMIT_MESSAGE = "Rate limit reached.";
const DEFAULT_RETRY_GUIDANCE = "Please try again in a few minutes.";

const hasTerminalPunctuation = (message) => /[.!?]$/.test(message);

const pickMessage = (...messages) =>
  messages.find(
    (message) => typeof message === "string" && message.trim().length > 0,
  ) || "";

const SAFE_VALIDATION_STATUS_CODES = new Set([400, 404, 409, 422]);
const INTERNAL_ERROR_MARKERS =
  /DB::|ClickHouse|Stack\s*trace|Traceback|Code:\s*\d+|SELECT\s|maximum:\s*\d+|elapsed\s+\d+/i;

/**
 * Return concise validation feedback, but never expose infrastructure/query
 * details from a failed mutation. Server-side failures and suspiciously large
 * or multiline payloads intentionally collapse to the caller's safe fallback.
 */
export function getSafeActionErrorMessage(error, fallback) {
  const responseData = error?.response?.data || {};
  const statusCode = Number(
    error?.response?.status ||
      responseData?.statusCode ||
      error?.status ||
      error?.statusCode,
  );
  const message = pickMessage(
    responseData?.message,
    responseData?.detail,
    responseData?.error,
    responseData?.result,
    error?.result,
  ).trim();

  if (
    !SAFE_VALIDATION_STATUS_CODES.has(statusCode) ||
    !message ||
    message.length > 240 ||
    /[\r\n]/.test(message) ||
    INTERNAL_ERROR_MARKERS.test(message)
  ) {
    return fallback;
  }
  return message;
}

function withRetryGuidance(message, retryAction) {
  const baseMessage = (message || DEFAULT_RATE_LIMIT_MESSAGE).trim();
  const guidance = retryAction
    ? `Please try ${retryAction} again in a few minutes.`
    : DEFAULT_RETRY_GUIDANCE;

  return `${baseMessage}${hasTerminalPunctuation(baseMessage) ? " " : ". "}${guidance}`;
}

export function getRequestErrorMessage(
  error,
  fallback = "Something went wrong",
  options = {},
) {
  const { retryAction } = options;
  const responseData = error?.response?.data || {};
  const statusCode =
    error?.response?.status ||
    responseData?.statusCode ||
    error?.status ||
    error?.statusCode;

  const extractedMessage = pickMessage(
    responseData?.result,
    responseData?.message,
    responseData?.error,
    responseData?.detail,
    error?.result,
    error?.message,
  );

  if (statusCode === RESPONSE_CODES.LIMIT_REACHED) {
    return withRetryGuidance(extractedMessage || fallback, retryAction);
  }

  return extractedMessage || fallback;
}
