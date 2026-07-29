import React from "react";
import PropTypes from "prop-types";
import { Box, Stack, Typography, IconButton, Tooltip } from "@mui/material";
import { alpha } from "@mui/material/styles";
import Iconify from "src/components/iconify";
import { useSnackbar } from "src/components/snackbar";
import { fToNow } from "src/utils/format-time";

// Self-hosted instances often have no SMTP configured, so invite emails never
// arrive. Mirror the self-host convention: surface the per-invite link so the
// admin can share it with each teammate directly.
export default function OrgInviteLinks({ invites = [] }) {
  const { enqueueSnackbar } = useSnackbar();

  const handleCopy = async (link) => {
    try {
      await navigator.clipboard.writeText(link);
      enqueueSnackbar("Invite link copied", { variant: "success" });
    } catch {
      enqueueSnackbar("Could not copy the link", { variant: "error" });
    }
  };

  const renderNotice = (
    <Stack
      direction="row"
      spacing={1.5}
      sx={{
        p: 1.75,
        borderRadius: 1,
        border: "1px solid",
        borderColor: "divider",
        bgcolor: (t) => alpha(t.palette.info.main, 0.08),
        alignItems: "flex-start",
      }}
    >
      <Iconify
        icon="solar:info-circle-linear"
        width={18}
        sx={{ color: "info.main", mt: "2px", flexShrink: 0 }}
      />
      <Typography sx={{ fontSize: "13px", color: "text.secondary", lineHeight: 1.6 }}>
        This self-hosted instance may not be configured to send emails. Remember
        to <b>share the invite link</b> with each team member you invite.
      </Typography>
    </Stack>
  );

  if (!invites.length) return renderNotice;

  return (
    <Stack spacing={2}>
      {renderNotice}

      <Box>
        <Typography
          fontWeight="fontWeightSemiBold"
          sx={{ fontSize: "14px", color: "text.primary", mb: 1 }}
        >
          Invite Links
        </Typography>

        <Box
          sx={{
            border: "1px solid",
            borderColor: "divider",
            borderRadius: 1,
            overflow: "hidden",
          }}
        >
          {/* Header */}
          <Stack
            direction="row"
            alignItems="center"
            sx={{
              px: 2,
              py: 1,
              bgcolor: (t) => alpha(t.palette.common.white, 0.03),
              borderBottom: "1px solid",
              borderColor: "divider",
            }}
          >
            <Typography sx={{ flex: 2, fontSize: "11px", color: "text.disabled", letterSpacing: 0.6 }}>
              INVITEE
            </Typography>
            <Typography sx={{ flex: 1, fontSize: "11px", color: "text.disabled", letterSpacing: 0.6 }}>
              LEVEL
            </Typography>
            <Typography sx={{ flex: 1, fontSize: "11px", color: "text.disabled", letterSpacing: 0.6 }}>
              CREATED
            </Typography>
            <Typography sx={{ flex: 1.4, fontSize: "11px", color: "text.disabled", letterSpacing: 0.6 }}>
              INVITE LINK
            </Typography>
          </Stack>

          {invites.map((invite) => {
            // Backend field; absent until the API exposes a per-invite link.
            const link = invite?.invite_link || invite?.invitation_link || "";
            return (
              <Stack
                key={invite.id || invite.email}
                direction="row"
                alignItems="center"
                sx={{
                  px: 2,
                  py: 1.25,
                  borderBottom: "1px solid",
                  borderColor: "divider",
                  "&:last-of-type": { borderBottom: "none" },
                }}
              >
                {/* Invitee */}
                <Stack direction="row" alignItems="center" spacing={1} sx={{ flex: 2, minWidth: 0 }}>
                  <Box
                    sx={{
                      width: 24,
                      height: 24,
                      borderRadius: "50%",
                      flexShrink: 0,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      bgcolor: (t) => alpha(t.palette.primary.main, 0.16),
                      color: "primary.main",
                      fontSize: "11px",
                      fontWeight: 600,
                      textTransform: "uppercase",
                    }}
                  >
                    {(invite.email || "?").charAt(0)}
                  </Box>
                  <Typography
                    sx={{
                      fontSize: "13px",
                      color: "text.primary",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {invite.email}
                  </Typography>
                </Stack>

                {/* Level */}
                <Box sx={{ flex: 1 }}>
                  <Box
                    sx={{
                      display: "inline-flex",
                      px: 0.75,
                      py: 0.25,
                      borderRadius: 0.5,
                      border: "1px solid",
                      borderColor: "divider",
                      fontSize: "11px",
                      color: "text.secondary",
                    }}
                  >
                    {invite.organization_role || "Member"}
                  </Box>
                </Box>

                {/* Created */}
                <Typography sx={{ flex: 1, fontSize: "12px", color: "text.secondary" }}>
                  {invite.created_at ? fToNow(invite.created_at) : "—"}
                </Typography>

                {/* Invite link */}
                <Box sx={{ flex: 1.4, minWidth: 0 }}>
                  {link ? (
                    <Stack direction="row" alignItems="center" spacing={0.5}>
                      <Tooltip title="Copy invite link">
                        <IconButton size="small" onClick={() => handleCopy(link)}>
                          <Iconify icon="solar:copy-linear" width={15} />
                        </IconButton>
                      </Tooltip>
                      <Typography
                        sx={{
                          fontSize: "12px",
                          color: "text.secondary",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {link}
                      </Typography>
                    </Stack>
                  ) : (
                    <Tooltip title="The server has not returned an invite link for this invite yet">
                      <Typography sx={{ fontSize: "12px", color: "text.disabled" }}>
                        Link unavailable
                      </Typography>
                    </Tooltip>
                  )}
                </Box>
              </Stack>
            );
          })}
        </Box>
      </Box>
    </Stack>
  );
}

OrgInviteLinks.propTypes = {
  invites: PropTypes.array,
};
