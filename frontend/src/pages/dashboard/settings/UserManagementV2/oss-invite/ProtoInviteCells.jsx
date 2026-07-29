import React, { useState } from "react";
import PropTypes from "prop-types";
import { Box, Stack, Typography, IconButton, Tooltip } from "@mui/material";
import Iconify from "src/components/iconify";
import { enqueueSnackbar } from "notistack";
import { removeInvite, DELIVERY } from "./ossInviteState";

// "Invite link" grid cell — only pending invites carry a link. Active members
// (the owner) render a dash. Includes a delivery indicator so the admin knows
// whether the mail was caught locally, actually emailed, or link-only.
export function InviteLinkCell({ data }) {
  const [copied, setCopied] = useState(false);
  const link = data?.invite_link;

  if (!link) {
    return (
      <Typography variant="s2" color="text.disabled" sx={{ lineHeight: "40px" }}>
        —
      </Typography>
    );
  }

  const copy = async (e) => {
    e?.stopPropagation();
    try {
      await navigator.clipboard.writeText(link);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      enqueueSnackbar("Could not copy", { variant: "error" });
    }
  };

  const delivery = data?.delivery;
  const badge =
    delivery === DELIVERY.PROVIDER
      ? { icon: "solar:letter-linear", color: "success.main", title: "Emailed to their inbox" }
      : delivery === DELIVERY.CATCHER
        ? { icon: "solar:inbox-line-linear", color: "warning.main", title: "Caught in your local mail catcher — share the link yourself" }
        : null;

  return (
    <Stack direction="row" spacing={0.5} alignItems="center" sx={{ height: "100%", minWidth: 0 }}>
      <Tooltip title={copied ? "Copied" : "Copy invite link"}>
        <IconButton size="small" onClick={copy} sx={{ color: copied ? "success.main" : "primary.main" }}>
          <Iconify icon={copied ? "solar:check-read-linear" : "solar:copy-linear"} width={16} />
        </IconButton>
      </Tooltip>
      <Typography
        variant="s2"
        onClick={copy}
        sx={{
          fontFamily: "monospace",
          color: "text.secondary",
          cursor: "pointer",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          minWidth: 0,
        }}
      >
        {link}
      </Typography>
      {badge && (
        <Tooltip title={badge.title}>
          <Box sx={{ display: "inline-flex", flexShrink: 0 }}>
            <Iconify icon={badge.icon} width={14} sx={{ color: badge.color }} />
          </Box>
        </Tooltip>
      )}
    </Stack>
  );
}
InviteLinkCell.propTypes = { data: PropTypes.object };

// Cancel a pending invite (local state). Active members show nothing here.
export function InviteActionCell({ data, onRefresh }) {
  if (!data?.invite_link) return null;
  return (
    <Box sx={{ height: "100%", display: "flex", alignItems: "center" }}>
      <Tooltip title="Cancel invite">
        <IconButton
          size="small"
          onClick={(e) => {
            e.stopPropagation();
            removeInvite(data.id);
            onRefresh?.();
          }}
          sx={{ color: "text.disabled", "&:hover": { color: "error.main" } }}
        >
          <Iconify icon="solar:trash-bin-minimalistic-linear" width={16} />
        </IconButton>
      </Tooltip>
    </Box>
  );
}
InviteActionCell.propTypes = { data: PropTypes.object, onRefresh: PropTypes.func };
