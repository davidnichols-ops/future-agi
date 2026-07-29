import React from "react";
import PropTypes from "prop-types";
import {
  Box,
  Button,
  Dialog,
  Divider,
  IconButton,
  Stack,
  Typography,
} from "@mui/material";
import { alpha } from "@mui/material/styles";
import Iconify from "src/components/iconify";

// Explains that joining an existing organization requires a personal invite
// link (there is no self-serve "join org" flow) — shown from the sign-up form.
export default function JoinOrgDialog({ open, onClose }) {
  return (
    <Dialog
      fullWidth
      maxWidth="xs"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          borderRadius: 2,
          border: "1px solid",
          borderColor: "divider",
          backgroundImage: "none",
        },
      }}
    >
      <Box sx={{ p: 3, position: "relative" }}>
        <IconButton
          onClick={onClose}
          size="small"
          sx={{ position: "absolute", top: 12, right: 12, color: "text.secondary" }}
        >
          <Iconify icon="mdi:close" width={18} />
        </IconButton>

        <Stack alignItems="center" sx={{ mb: 2 }}>
          <Box
            sx={{
              width: 56,
              height: 56,
              borderRadius: "50%",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              bgcolor: (t) => alpha(t.palette.primary.main, 0.14),
              color: "primary.main",
              mb: 2,
            }}
          >
            <Iconify icon="solar:letter-opened-linear" width={26} />
          </Box>
          <Typography
            fontWeight="fontWeightSemiBold"
            sx={{ fontSize: "18px", color: "text.primary", textAlign: "center" }}
          >
            You&apos;ll need your invite link to join
          </Typography>
        </Stack>

        <Stack spacing={1.5} sx={{ mb: 3 }}>
          <Typography
            sx={{
              fontSize: "14px",
              color: "text.secondary",
              textAlign: "center",
              lineHeight: 1.6,
            }}
          >
            When a teammate invites you to a Future AGI organization, we email you
            a personal invite link. You can only join the existing organization by
            opening that email and clicking the link.
          </Typography>
          <Typography
            sx={{
              fontSize: "14px",
              color: "text.secondary",
              textAlign: "center",
              lineHeight: 1.6,
            }}
          >
            Didn&apos;t get an email? Check your spam folder, or ask the teammate
            who invited you to resend it from the organization&apos;s members
            settings.
          </Typography>
        </Stack>

        <Divider sx={{ mb: 2 }} />

        <Stack direction="row" justifyContent="flex-end">
          <Button
            variant="contained"
            color="primary"
            onClick={onClose}
            sx={{ borderRadius: 0.5, px: 3, height: 38 }}
          >
            Got it
          </Button>
        </Stack>
      </Box>
    </Dialog>
  );
}

JoinOrgDialog.propTypes = {
  open: PropTypes.bool,
  onClose: PropTypes.func,
};
