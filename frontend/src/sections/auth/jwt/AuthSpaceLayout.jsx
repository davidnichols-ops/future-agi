import React from "react";
import PropTypes from "prop-types";
import { Box, Stack } from "@mui/material";
import SvgColor from "src/components/svg-color";
import SpaceBackdrop from "src/sections/oss-setup/SpaceBackdrop";

// Single-column, space-themed auth layout — the Future AGI wordmark on top and
// the form content centered on the page (no split panel).
export default function AuthSpaceLayout({ children, maxWidth = 440 }) {
  return (
    <Box
      sx={{
        position: "relative",
        width: "100%",
        height: "100vh",
        overflowY: "auto",
        bgcolor: "background.default",
        display: "flex",
      }}
    >
      <SpaceBackdrop />

      <Box
        sx={{
          position: "relative",
          zIndex: 1,
          width: "100%",
          maxWidth,
          px: 3,
          py: { xs: 6, md: 9 },
          // margin auto centers vertically + horizontally and stays scrollable
          // if the content is ever taller than the viewport.
          m: "auto",
          display: "flex",
          flexDirection: "column",
          // Center everything — brand, headings, and form all share the same
          // centered axis.
          alignItems: "center",
          textAlign: "center",
        }}
      >
        {/* Brand */}
        <Stack direction="row" gap={0.75} alignItems="center" sx={{ mb: 5 }}>
          <SvgColor
            src="/favicon/logo.svg"
            sx={{ height: 40, width: 40, color: "common.white" }}
          />
          <SvgColor
            src="/logo/future_agi_text.svg"
            sx={{ height: 20, width: 128, color: "common.white" }}
          />
        </Stack>

        <Box sx={{ width: "100%" }}>{children}</Box>
      </Box>
    </Box>
  );
}

AuthSpaceLayout.propTypes = {
  children: PropTypes.node,
  maxWidth: PropTypes.number,
};
