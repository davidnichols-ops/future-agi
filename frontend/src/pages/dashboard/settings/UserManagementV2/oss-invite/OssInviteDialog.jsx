import React, { useState } from "react";
import PropTypes from "prop-types";
import {
  Dialog,
  Box,
  Stack,
  Typography,
  TextField,
  MenuItem,
  Button,
  IconButton,
  Tooltip,
  Chip,
  alpha,
} from "@mui/material";
import Iconify from "src/components/iconify";
import { enqueueSnackbar } from "notistack";
import { orgRoleOptions } from "../constant";
import { createInvites } from "./ossInviteState";

const isValidEmail = (e) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e.trim());
const ROLE_OPTIONS = orgRoleOptions.filter((o) => o.label !== "Owner");

function CopyLinkButton({ link }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(link);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      enqueueSnackbar("Could not copy", { variant: "error" });
    }
  };
  return (
    <Tooltip title={copied ? "Copied" : "Copy link"}>
      <Button
        onClick={copy}
        size="small"
        variant={copied ? "contained" : "outlined"}
        color={copied ? "success" : "primary"}
        startIcon={<Iconify icon={copied ? "solar:check-read-linear" : "solar:copy-linear"} width={15} />}
        sx={{ flexShrink: 0, minWidth: 96 }}
      >
        {copied ? "Copied" : "Copy"}
      </Button>
    </Tooltip>
  );
}
CopyLinkButton.propTypes = { link: PropTypes.string };

export default function OssInviteDialog({ open, onClose, onInvited }) {
  const [emailDraft, setEmailDraft] = useState("");
  const [emails, setEmails] = useState([]);
  const [level, setLevel] = useState(3); // Member
  const [result, setResult] = useState(null);

  const reset = () => {
    setEmailDraft("");
    setEmails([]);
    setLevel(3);
    setResult(null);
  };
  const handleClose = () => {
    reset();
    onClose();
  };

  const parseDraft = () =>
    emailDraft
      .split(/[,\s]+/)
      .map((e) => e.trim().toLowerCase())
      .filter(Boolean);

  const commitDraft = () => {
    const parts = parseDraft();
    if (!parts.length) return true;
    const next = [...emails];
    for (const p of parts) {
      if (!isValidEmail(p)) {
        enqueueSnackbar(`"${p}" is not a valid email`, { variant: "error" });
        return false;
      }
      if (!next.includes(p)) next.push(p);
    }
    setEmails(next);
    setEmailDraft("");
    return true;
  };

  const removeEmail = (e) => setEmails((list) => list.filter((x) => x !== e));

  const handleSubmit = () => {
    const all = [...emails];
    for (const p of parseDraft()) {
      if (!isValidEmail(p)) {
        enqueueSnackbar(`"${p}" is not a valid email`, { variant: "error" });
        return;
      }
      if (!all.includes(p)) all.push(p);
    }
    if (!all.length) {
      enqueueSnackbar("Add at least one email address", { variant: "error" });
      return;
    }
    setResult(createInvites(all, level));
    onInvited?.();
  };

  const roleLabel = ROLE_OPTIONS.find((o) => o.value === level)?.label || "Member";
  const compose = !result;

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{ sx: { borderRadius: 2, bgcolor: "background.paper" } }}
    >
      <Box sx={{ p: 3 }}>
        <Stack direction="row" alignItems="flex-start">
          <Box sx={{ flex: 1 }}>
            <Typography variant="m3" fontWeight="fontWeightSemiBold" color="text.primary">
              {compose ? "Invite teammates" : "Invites created"}
            </Typography>
            <Typography variant="s1" color="text.secondary" sx={{ mt: 0.5, display: "block" }}>
              {compose
                ? "Add people to your organization and pick their access level."
                : "Copy each link and send it to the teammate it belongs to."}
            </Typography>
          </Box>
          <IconButton onClick={handleClose}>
            <Iconify icon="mdi:close" />
          </IconButton>
        </Stack>

        {compose ? (
          <>
            <Stack direction="row" spacing={1.5} alignItems="flex-start" sx={{ mt: 2.5 }}>
              <TextField
                label="Email addresses"
                placeholder="john@futureagi.com, jane@…"
                size="small"
                fullWidth
                value={emailDraft}
                onChange={(e) => setEmailDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === ",") {
                    e.preventDefault();
                    commitDraft();
                  }
                }}
                onBlur={commitDraft}
                sx={{
                  // Kill Chrome's blue autofill background — keep the dialog bg.
                  "& input:-webkit-autofill, & input:-webkit-autofill:hover, & input:-webkit-autofill:focus, & input:-webkit-autofill:active":
                    {
                      WebkitBoxShadow: (t) =>
                        `0 0 0 1000px ${t.palette.background.paper} inset`,
                      WebkitTextFillColor: (t) => t.palette.text.primary,
                      caretColor: (t) => t.palette.text.primary,
                      transition: "background-color 9999s ease-out 0s",
                    },
                }}
              />
              <TextField
                label="Role"
                select
                size="small"
                value={level}
                onChange={(e) => setLevel(Number(e.target.value))}
                sx={{ minWidth: 130 }}
              >
                {ROLE_OPTIONS.map((o) => (
                  <MenuItem key={o.value} value={o.value}>
                    {o.label}
                  </MenuItem>
                ))}
              </TextField>
            </Stack>

            {emails.length > 0 && (
              <Box sx={{ mt: 1.5, display: "flex", flexWrap: "wrap", gap: 0.75 }}>
                {emails.map((e) => (
                  <Chip
                    key={e}
                    label={e}
                    size="small"
                    onDelete={() => removeEmail(e)}
                    sx={{ bgcolor: (t) => alpha(t.palette.primary.main, 0.1), color: "primary.main" }}
                  />
                ))}
              </Box>
            )}

            <Stack direction="row" spacing={1.5} justifyContent="flex-end" sx={{ mt: 3 }}>
              <Button variant="outlined" color="inherit" onClick={handleClose}>
                Cancel
              </Button>
              <Button variant="contained" color="primary" onClick={handleSubmit}>
                Create invite links
              </Button>
            </Stack>
          </>
        ) : (
          <>
            <Stack spacing={1.25} sx={{ mt: 2.5 }}>
              {result.map((inv) => (
                <Box
                  key={inv.id}
                  sx={{
                    p: 1.5,
                    borderRadius: 1,
                    border: "1px solid",
                    borderColor: "divider",
                    bgcolor: (t) => alpha(t.palette.common.white, 0.02),
                  }}
                >
                  <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                    <Typography variant="s1" color="text.primary" fontWeight="fontWeightMedium" sx={{ flex: 1 }}>
                      {inv.email}
                    </Typography>
                    <Chip label={roleLabel} size="small" variant="outlined" />
                  </Stack>
                  <Stack direction="row" spacing={1} alignItems="center">
                    <TextField
                      value={inv.invite_link}
                      size="small"
                      fullWidth
                      InputProps={{ readOnly: true, sx: { fontSize: 12.5, fontFamily: "monospace" } }}
                    />
                    <CopyLinkButton link={inv.invite_link} />
                  </Stack>
                </Box>
              ))}
            </Stack>

            <Stack direction="row" spacing={1.5} justifyContent="flex-end" sx={{ mt: 3 }}>
              <Button variant="outlined" color="inherit" onClick={() => setResult(null)}>
                Invite more
              </Button>
              <Button variant="contained" color="primary" onClick={handleClose}>
                Done
              </Button>
            </Stack>
          </>
        )}
      </Box>
    </Dialog>
  );
}

OssInviteDialog.propTypes = {
  open: PropTypes.bool,
  onClose: PropTypes.func,
  onInvited: PropTypes.func,
};
