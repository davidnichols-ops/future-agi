import React, { useState, useCallback, useMemo } from "react";
import PropTypes from "prop-types";
import {
  Box,
  Button,
  Checkbox,
  Divider,
  IconButton,
  Popover,
  Stack,
  Typography,
} from "@mui/material";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import axios from "src/utils/axios";
import { apiPath } from "src/api/contracts/api-surface";
import { enqueueSnackbar } from "notistack";
import { normalizeTags, toTagPayload } from "./tagUtils";
import TagChip from "./TagChip";
import TagInput from "./TagInput";
import Iconify from "src/components/iconify";
import ConfirmDialog from "src/components/custom-dialog/confirm-dialog";
import {
  useDeleteProjectTag,
  useEditProjectTag,
  useProjectTags,
} from "./useProjectTags";

const AddTagsPopover = ({
  anchorEl,
  open,
  onClose,
  traceId,
  spanId,
  projectId,
  bulkItems,
  currentTags = [],
  onSuccess,
}) => {
  const items = Array.isArray(bulkItems) ? bulkItems : [];
  const isBulk = items.length > 1;

  const [tags, setTags] = useState(() =>
    isBulk ? [] : normalizeTags(currentTags),
  );
  const queryClient = useQueryClient();

  // Seed on open / entity change only. Callers build `currentTags` inline, so
  // depending on its identity would discard optimistic state on every render.
  const entityKey = spanId || traceId || null;
  const currentTagsRef = React.useRef(currentTags);
  currentTagsRef.current = currentTags;
  React.useEffect(() => {
    if (open) setTags(isBulk ? [] : normalizeTags(currentTagsRef.current));
  }, [open, entityKey, isBulk]);

  // Pin to where the anchor was on open: saving replaces the anchor's DOM node,
  // and MUI drops the popover to the screen corner when it measures a detached one.
  const [anchorRect, setAnchorRect] = useState(null);
  React.useEffect(() => {
    if (open && anchorEl?.getBoundingClientRect) {
      setAnchorRect(anchorEl.getBoundingClientRect());
    } else if (!open) {
      setAnchorRect(null);
    }
    // Trade-off: no re-measure on window resize.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Offer existing tags so the same tag isn't retyped into variants.
  const { data: projectTags = [] } = useProjectTags(projectId, open);

  const patchTrace = (id, newTags) =>
    axios.patch(apiPath("/tracer/trace/{id}/tags/", { id }), {
      tags: toTagPayload(newTags),
    });
  const patchSpan = (id, newTags) =>
    axios.post(apiPath("/tracer/observation-span/update-tags/"), {
      span_id: id,
      tags: toTagPayload(newTags),
    });

  const { mutate: saveTags, isPending } = useMutation({
    mutationFn: (newTags) => {
      if (isBulk) {
        // Merge with each item's existing tags to avoid overwriting.
        // Backend PATCH replaces tags[], so we compute the full set here.
        return Promise.all(
          items.map((item) => {
            const existing = normalizeTags(item.currentTags || []);
            const merged = [...existing];
            newTags.forEach((t) => {
              if (!merged.some((e) => e.name === t.name)) merged.push(t);
            });
            return item.type === "span"
              ? patchSpan(item.id, merged)
              : patchTrace(item.id, merged);
          }),
        );
      }
      if (spanId) {
        return patchSpan(spanId, newTags);
      }
      return patchTrace(traceId, newTags);
    },
    onSuccess: () => {
      enqueueSnackbar(
        isBulk ? `Tags applied to ${items.length} items` : "Tags updated",
        { variant: "success" },
      );
      // Refreshes the trace-detail drawer. The LLM tracing grid is AG-Grid
      // server-side (not React Query), so it relies on onSuccess instead.
      queryClient.invalidateQueries({ queryKey: ["trace-detail"] });
      queryClient.invalidateQueries({
        queryKey: ["project-tag-definitions", projectId],
      });
      onSuccess?.();
    },
    onError: () => {
      // `persist` restores state — it knows what this save replaced.
      enqueueSnackbar("Failed to update tags", { variant: "error" });
    },
  });

  const persist = useCallback(
    (nextTags) => {
      // Roll back to what was on screen, not `currentTags` — the grid freezes
      // that prop, so it would discard tags saved earlier in this session.
      const previous = tags;
      setTags(nextTags);
      saveTags(nextTags, {
        onError: () => {
          if (!isBulk) setTags(previous);
        },
      });
    },
    [tags, saveTags, isBulk],
  );

  const handleAdd = useCallback(
    (newTag) => {
      if (tags.some((t) => t.name === newTag.name)) return;
      persist([...tags, newTag]);
    },
    [tags, persist],
  );

  const handleRemove = useCallback(
    (idx) => persist(tags.filter((_, i) => i !== idx)),
    [tags, persist],
  );

  // Edits the tag itself, project-wide — editing via the row forks a duplicate.
  // Must stay above the handlers that close over `editTag`.
  const { mutate: editTag } = useEditProjectTag(projectId, {
    onSuccess: (_res, variables) => {
      setTags((prev) =>
        prev.map((t) =>
          t.name === variables.name
            ? {
                name: variables.newName || t.name,
                color: variables.color || t.color,
              }
            : t,
        ),
      );
      onSuccess?.();
    },
  });

  const handleColorChange = useCallback(
    (idx, color) => {
      const target = tags[idx];
      if (!target || !color || color === target.color) return;
      if (!projectId) return;
      editTag({ name: target.name, color });
    },
    [tags, projectId, editTag],
  );

  const handleRename = useCallback(
    (idx, newName) => {
      const target = tags[idx];
      if (!target || !newName || newName === target.name) return;
      if (!projectId) return;
      editTag({ name: target.name, newName });
    },
    [tags, projectId, editTag],
  );

  // Project-wide and irreversible — always confirmed, never shares a click target.
  const [pendingDelete, setPendingDelete] = useState(null);
  const { mutate: deleteTag, isPending: isDeleting } = useDeleteProjectTag(
    projectId,
    {
      onSuccess: (_res, variables) => {
        setTags((prev) => prev.filter((t) => t.name !== variables.name));
        setPendingDelete(null);
        onSuccess?.();
      },
    },
  );

  const handleToggleExisting = useCallback(
    (tag) => {
      // Saves post the full set; a second click would compute from stale state.
      if (isPending) return;
      const applied = tags.some((t) => t.name === tag.name);
      persist(
        applied
          ? tags.filter((t) => t.name !== tag.name)
          : [...tags, { name: tag.name, color: tag.color }],
      );
    },
    [tags, persist, isPending],
  );

  // "Already applied" has no single answer across many rows.
  const pickerTags = useMemo(
    () => (isBulk ? [] : projectTags),
    [isBulk, projectTags],
  );

  return (
    <Popover
      open={open}
      {...(anchorRect
        ? {
            anchorReference: "anchorPosition",
            anchorPosition: { top: anchorRect.bottom, left: anchorRect.right },
          }
        : { anchorEl })}
      onClose={onClose}
      anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      transformOrigin={{ vertical: "top", horizontal: "right" }}
      slotProps={{ paper: { sx: { width: 300, p: 1.5, mt: 0.5 } } }}
    >
      <Typography sx={{ fontSize: 12, fontWeight: 600, mb: 1 }}>
        {isBulk ? `Add tags to ${items.length} items` : "Tags"}
      </Typography>

      {!isBulk && tags.length > 0 && (
        <Stack direction="row" sx={{ flexWrap: "wrap", gap: 0.5, mb: 1.5 }}>
          {tags.map((tag, idx) => (
            <TagChip
              key={`${tag.name}-${idx}`}
              name={tag.name}
              color={tag.color}
              onRemove={() => handleRemove(idx)}
              onColorChange={(c) => handleColorChange(idx, c)}
              onRename={(n) => handleRename(idx, n)}
            />
          ))}
        </Stack>
      )}

      {pickerTags.length > 0 && (
        <>
          <Box sx={{ maxHeight: 180, overflow: "auto", mb: 1 }}>
            {pickerTags.map((tag) => {
              const checked = tags.some((t) => t.name === tag.name);
              return (
                <Box
                  key={tag.name}
                  // Keyboard-operable: otherwise delete is the row's only focusable control.
                  role="checkbox"
                  aria-checked={checked}
                  aria-label={tag.name}
                  tabIndex={0}
                  onClick={() => handleToggleExisting(tag)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      handleToggleExisting(tag);
                    }
                  }}
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 0.5,
                    px: 0.5,
                    py: 0.25,
                    cursor: isPending ? "default" : "pointer",
                    borderRadius: "4px",
                    "&:hover": { bgcolor: "action.hover" },
                    "&:focus-visible": {
                      outline: "2px solid",
                      outlineColor: "primary.main",
                      outlineOffset: -2,
                    },
                    // Revealed on hover, and on focus so it stays keyboard-reachable.
                    "&:hover .tag-delete, &:focus-within .tag-delete": {
                      opacity: 1,
                    },
                  }}
                >
                  <Checkbox
                    size="small"
                    checked={checked}
                    tabIndex={-1}
                    disabled={isPending}
                    sx={{ p: 0.25, "& .MuiSvgIcon-root": { fontSize: 16 } }}
                  />
                  <Box
                    sx={{
                      width: 8,
                      height: 8,
                      borderRadius: "50%",
                      bgcolor: tag.color,
                      flexShrink: 0,
                    }}
                  />
                  <Typography
                    noWrap
                    sx={{ fontSize: 12, fontWeight: 500, flex: 1, minWidth: 0 }}
                  >
                    {tag.name}
                  </Typography>
                  <IconButton
                    className="tag-delete"
                    size="small"
                    aria-label={`Delete tag ${tag.name}`}
                    disabled={isPending || isDeleting}
                    onClick={(e) => {
                      e.stopPropagation();
                      setPendingDelete(tag);
                    }}
                    sx={{
                      p: 0.25,
                      flexShrink: 0,
                      // Reserved space — the row must not reflow on hover.
                      opacity: 0,
                      color: "text.disabled",
                      transition: "opacity 120ms, color 120ms",
                      "@media (prefers-reduced-motion: reduce)": {
                        transition: "none",
                      },
                      "&:hover": { color: "error.main" },
                      "&:focus-visible": { opacity: 1 },
                    }}
                  >
                    <Iconify icon="mdi:trash-can-outline" width={14} />
                  </IconButton>
                </Box>
              );
            })}
          </Box>
          <Divider sx={{ mb: 1 }} />
        </>
      )}

      <TagInput
        onAdd={handleAdd}
        existingNames={tags.map((t) => t.name)}
        disabled={isPending}
      />

      <Typography sx={{ fontSize: 10, color: "text.disabled", mt: 0.75 }}>
        {isBulk
          ? "Tags will be added to every selected item"
          : "Double-click name to rename · Click dot to change color"}
      </Typography>

      <ConfirmDialog
        open={Boolean(pendingDelete)}
        onClose={() => setPendingDelete(null)}
        title="Delete tag"
        content={
          <Typography variant="body2">
            {pendingDelete?.usage_count
              ? `"${pendingDelete.name}" is on ${pendingDelete.usage_count} ${
                  pendingDelete.usage_count === 1 ? "item" : "items"
                } in this project. Deleting removes it from all of them, and can't be undone.`
              : `"${pendingDelete?.name}" isn't used yet. Deleting removes it from this project.`}
          </Typography>
        }
        action={
          <Button
            size="small"
            variant="contained"
            color="error"
            disabled={isDeleting}
            onClick={() => deleteTag({ name: pendingDelete.name })}
            sx={{ paddingX: "24px" }}
          >
            Delete
          </Button>
        }
      />
    </Popover>
  );
};

AddTagsPopover.propTypes = {
  anchorEl: PropTypes.any,
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  traceId: PropTypes.string,
  spanId: PropTypes.string,
  // Which project's reusable tags the picker offers.
  projectId: PropTypes.string,
  bulkItems: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.string.isRequired,
      type: PropTypes.oneOf(["trace", "span"]).isRequired,
      currentTags: PropTypes.array,
    }),
  ),
  currentTags: PropTypes.array,
  onSuccess: PropTypes.func,
};

export default React.memo(AddTagsPopover);
