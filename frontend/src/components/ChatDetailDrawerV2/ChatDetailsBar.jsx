import React, { useMemo, useState } from "react";
import PropTypes from "prop-types";
import { Box, Stack } from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import { fDateTime } from "src/utils/format-time";
import CustomTooltip from "src/components/tooltip/CustomTooltip";
import Iconify from "src/components/iconify";
import TagChip from "src/components/traceDetail/TagChip";
import AddTagsPopover from "src/components/traceDetail/AddTagsPopover";
import {
  normalizeTags,
  resolveCallTags,
} from "src/components/traceDetail/tagUtils";
import { useGetTraceDetail } from "src/api/project/trace-detail";
import VoiceActionsDropdown, {
  VOICE_ACTIONS,
} from "src/components/VoiceDetailDrawerV2/VoiceActionsDropdown";
import { DRAWER_MODULE, TAG_INVALIDATION_QUERY_KEYS } from "./constants";

// Chat top bar — chip strip + actions dropdown + inline tags row.
// Mirrors the voice CallDetailsBar, minus voice-only fields.

// Action ids that operate on the trace record (and therefore require
// `data.trace_id` to be a real tracer trace — CallExecution ids would
// 404). Mirrors the gating in `VoiceDetailDrawerV2/CallDetailsBar.jsx`.
const TRACE_GATED_ACTION_IDS = new Set(["tags", "dataset"]);

/**
 * Responsive metric chip. The value cell gets a `min-width: 0` + ellipsis
 * so long strings (notably ended_reason, which is a free-form sentence)
 * truncate instead of blowing past the drawer's right edge. Full text is
 * always available via tooltip on hover.
 */
const MetricChip = ({ label, value }) => {
  const fullText =
    typeof value === "string" || typeof value === "number" ? String(value) : "";
  return (
    <CustomTooltip
      show={!!fullText}
      title={fullText}
      arrow
      size="small"
      type="black"
      placement="top"
    >
      <Box
        sx={{
          display: "inline-flex",
          alignItems: "center",
          gap: 0.5,
          px: 1,
          py: 0.25,
          bgcolor: "background.neutral",
          border: "1px solid",
          borderColor: "divider",
          borderRadius: "2px",
          minWidth: 64,
          maxWidth: "100%",
          fontSize: 11,
          color: "text.primary",
          lineHeight: "16px",
        }}
      >
        <Box component="span" sx={{ flexShrink: 0, whiteSpace: "nowrap" }}>
          {label} :
        </Box>
        <Box
          component="span"
          sx={{
            minWidth: 0,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {value}
        </Box>
      </Box>
    </CustomTooltip>
  );
};
MetricChip.propTypes = {
  label: PropTypes.string.isRequired,
  value: PropTypes.node.isRequired,
};

const formatDuration = (seconds) => {
  if (seconds == null) return null;
  const n = Number(seconds);
  if (!Number.isFinite(n)) return null;
  const m = Math.floor(n / 60);
  const s = Math.round(n % 60);
  if (m === 0) return `${s}s`;
  return `${m}m ${s}s`;
};

const formatMs = (ms) => {
  if (ms == null) return null;
  const n = Number(ms);
  if (!Number.isFinite(n) || n === 0) return null;
  return n < 1000 ? `${Math.round(n)}ms` : `${(n / 1000).toFixed(1)}s`;
};

const formatCost = (cost) => {
  if (cost == null) return null;
  const n = Number(cost);
  if (!Number.isFinite(n)) return null;
  if (n < 0.01) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(2)}`;
};

/* Uses the shared AddTagsPopover so tagging behaves the same everywhere. */
const InlineTagsRow = ({ tags = [], traceId, projectId }) => {
  const [anchorEl, setAnchorEl] = useState(null);

  const normalized = useMemo(() => normalizeTags(tags), [tags]);
  const queryClient = useQueryClient();

  return (
    <>
      <Stack
        direction="row"
        onClick={(e) => setAnchorEl(e.currentTarget)}
        sx={{
          flexWrap: "wrap",
          gap: 0.5,
          alignItems: "center",
          cursor: "pointer",
        }}
      >
        <Iconify
          icon="mdi:tag-outline"
          width={13}
          sx={{ color: "text.disabled" }}
        />
        {normalized.map((tag, idx) => (
          <TagChip
            key={`${tag.name}-${idx}`}
            name={tag.name}
            color={tag.color}
            size="small"
            readOnly
          />
        ))}
        <Box
          sx={{
            display: "inline-flex",
            alignItems: "center",
            gap: "2px",
            px: 0.5,
            py: "1px",
            borderRadius: "3px",
            border: "1px dashed",
            borderColor: "divider",
            fontSize: 11,
            color: "text.disabled",
            lineHeight: "16px",
            "&:hover": { borderColor: "primary.main", color: "primary.main" },
          }}
        >
          <Iconify icon="mdi:plus" width={12} />
          tag
        </Box>
      </Stack>

      <AddTagsPopover
        open={Boolean(anchorEl)}
        anchorEl={anchorEl}
        onClose={() => setAnchorEl(null)}
        traceId={traceId}
        projectId={projectId}
        currentTags={tags}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ["chatCallDetail"] });
          queryClient.invalidateQueries({ queryKey: ["trace-detail"] });
        }}
      />
    </>
  );
};

InlineTagsRow.propTypes = {
  tags: PropTypes.array,
  traceId: PropTypes.string,
  // Which project's reusable tags the popover offers, and what rename /
  // recolour / delete act on.
  projectId: PropTypes.string,
};

const ChatDetailsBar = ({ data, onAction }) => {
  const chips = useMemo(() => {
    const out = [];

    const status = data?.status;
    if (status) {
      out.push({
        label: "Status",
        value: String(status).replace(/_/g, " "),
      });
    }

    const duration = formatDuration(data?.duration_seconds ?? data?.duration);
    if (duration) out.push({ label: "Duration", value: duration });

    const avgLatency = formatMs(
      data?.avg_agent_latency_ms ?? data?.avg_latency_ms ?? data?.avg_latency,
    );
    if (avgLatency) out.push({ label: "Avg Latency", value: avgLatency });

    const turns = data?.turn_count ?? data?.turnCount;
    if (turns != null) out.push({ label: "Turns", value: String(turns) });

    const totalTokens = data?.total_tokens ?? data?.totalTokens;
    if (totalTokens != null) {
      out.push({ label: "Tokens", value: String(totalTokens) });
    }

    const cost = formatCost(data?.cost ?? data?.total_cost);
    if (cost) out.push({ label: "Cost", value: cost });

    const timestamp = data?.timestamp || data?.created_at;
    if (timestamp) out.push({ label: "When", value: fDateTime(timestamp) });

    if (data?.ended_reason) {
      out.push({
        label: "Ended",
        value: data.ended_reason,
      });
    }
    return out;
  }, [data]);

  // Tags persist on the trace record so we need a real `trace_id`
  // (CallExecution ids 404 against the tag endpoint). For the
  // simulate module we read tags from the data payload directly; for
  // observe (project) we fall back to fetching the canonical trace
  // detail so the chip row stays in sync.
  const traceId = data?.trace_id;
  const isObserve = data?.module === DRAWER_MODULE.OBSERVE;
  const { data: traceDetail } = useGetTraceDetail(isObserve ? traceId : null);
  const tags = resolveCallTags(traceDetail, data);

  if (chips.length === 0 && !onAction && !traceId) return null;

  return (
    <Box
      sx={{
        px: 1.25,
        py: 1,
        borderBottom: "1px solid",
        borderColor: "divider",
        bgcolor: "background.default",
        flexShrink: 0,
      }}
    >
      {onAction && (
        <Stack direction="row" justifyContent="flex-end" sx={{ mb: 0.75 }}>
          <VoiceActionsDropdown
            onAction={onAction}
            // Trace-gated actions (tags, dataset) are hidden when there's
            // no real trace_id so the menu doesn't render dead clicks.
            actions={
              data?.trace_id
                ? VOICE_ACTIONS
                : VOICE_ACTIONS.filter((a) => !TRACE_GATED_ACTION_IDS.has(a.id))
            }
          />
        </Stack>
      )}

      {chips.length > 0 && (
        <Stack
          direction="row"
          alignItems="center"
          gap={0.5}
          sx={{
            flexWrap: "wrap",
            minWidth: 0,
            width: "100%",
          }}
        >
          {chips.map((c) => (
            <MetricChip key={c.label} label={c.label} value={c.value} />
          ))}
        </Stack>
      )}

      {traceId && (
        <Box sx={{ mt: 0.75 }}>
          <InlineTagsRow
            tags={tags}
            traceId={traceId}
            projectId={data?.project_id}
          />
        </Box>
      )}
    </Box>
  );
};

ChatDetailsBar.propTypes = {
  data: PropTypes.object,
  onAction: PropTypes.func,
};

export default ChatDetailsBar;
