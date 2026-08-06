/**
 * Voice-call list rows expose friendly top-level keys, while ClickHouse
 * filters use canonical span/system metric ids. Keep that mapping in one
 * place so tracing and eval-task filters cannot silently diverge.
 */
export const VOICE_CALL_FILTER_FIELDS = [
  {
    value: "call_status",
    responseKey: "status",
    label: "Status",
    type: "string",
    category: "system",
    apiColType: "SYSTEM_METRIC",
    // The voice-list alias matches the normalized status rendered in Live
    // Preview (for example provider `ended` becomes `completed`). Generic
    // call.status remains a raw span attribute everywhere else.
    legacyWireValues: ["call.status"],
    searchAliases: ["status", "call.status"],
  },
  {
    value: "duration",
    responseKey: "duration_seconds",
    label: "Duration (seconds)",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
    searchAliases: ["duration_seconds"],
  },
  {
    value: "avg_agent_latency_ms",
    responseKey: "avg_agent_latency_ms",
    label: "Avg Agent Latency (ms)",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "turn_count",
    responseKey: "turn_count",
    label: "Turn Count",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "talk_ratio",
    responseKey: "talk_ratio",
    label: "Agent Talk (%)",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "gen_ai.usage.total_tokens",
    responseKey: "gen_ai.usage.total_tokens",
    label: "Tokens",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
    searchAliases: ["tokens", "total_tokens"],
  },
  {
    value: "cost_cents",
    responseKey: "cost_cents",
    label: "Cost (cents)",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
    // Older task drafts used total_cost (VAPI currency units). The backend's
    // voice-list-only cost_cents alias now normalizes providers to the exact
    // top-level value rendered by Live Preview.
    legacyWireValues: ["total_cost"],
    legacyApiValueScale: 0.01,
    searchAliases: ["cost", "total_cost"],
  },
  {
    value: "user_interruption_count",
    responseKey: "user_interruption_count",
    label: "User Interruptions",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "ai_interruption_count",
    responseKey: "ai_interruption_count",
    label: "Agent Interruptions",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "ended_reason",
    responseKey: "ended_reason",
    label: "Ended Reason",
    type: "string",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "call_type",
    responseKey: "call_type",
    label: "Call Type",
    type: "string",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "user_wpm",
    responseKey: "user_wpm",
    label: "User WPM",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "bot_wpm",
    responseKey: "bot_wpm",
    label: "Agent WPM",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
  {
    value: "agent_talk_percentage",
    responseKey: "agent_talk_percentage",
    label: "Agent Talk Percentage",
    type: "number",
    category: "system",
    apiColType: "SYSTEM_METRIC",
  },
];

const VOICE_FIELD_BY_ID = new Map(
  VOICE_CALL_FILTER_FIELDS.flatMap((field) =>
    [field.value, field.responseKey, ...(field.legacyWireValues || [])].map(
      (id) => [id, field],
    ),
  ),
);

export const getVoiceCallFilterField = (fieldId) =>
  VOICE_FIELD_BY_ID.get(fieldId);

const scaleValue = (value, scale) => {
  if (value === "" || value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map((item) => scaleValue(item, scale));
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return value;
  return Number((numeric * scale).toPrecision(15));
};

const COMPLETED_STATUS_ALIASES = new Set([
  "ended",
  "done",
  "complete",
  "completed",
  "success",
  "succeeded",
]);
const IN_PROGRESS_STATUS_ALIASES = new Set([
  "in-progress",
  "in_progress",
  "ongoing",
  "started",
  "ringing",
  "queued",
  "pending",
]);

export const normalizeVoiceCallStatus = (value) => {
  if (Array.isArray(value)) {
    return [...new Set(value.map(normalizeVoiceCallStatus))];
  }
  if (typeof value !== "string") return value;
  const normalized = value.trim().toLowerCase();
  if (COMPLETED_STATUS_ALIASES.has(normalized)) return "completed";
  if (IN_PROGRESS_STATUS_ALIASES.has(normalized)) return "in-progress";
  return normalized;
};

export const toVoiceCallApiValue = (fieldId, value) => {
  const field = getVoiceCallFilterField(fieldId);
  if (field?.value === "call_status") return normalizeVoiceCallStatus(value);
  const scale =
    field?.apiValueScale ||
    (field?.legacyWireValues?.includes(fieldId)
      ? field.legacyApiValueScale
      : undefined);
  return scale ? scaleValue(value, scale) : value;
};

export const fromVoiceCallApiValue = (fieldId, value) => {
  const field = getVoiceCallFilterField(fieldId);
  if (field?.value === "call_status") return normalizeVoiceCallStatus(value);
  const scale =
    field?.apiValueScale ||
    (field?.legacyWireValues?.includes(fieldId)
      ? field.legacyApiValueScale
      : undefined);
  return scale ? scaleValue(value, 1 / scale) : value;
};
