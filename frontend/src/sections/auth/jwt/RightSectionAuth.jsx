import React from "react";
import { Box, Stack, Typography } from "@mui/material";
import { keyframes } from "@mui/system";
import SvgColor from "src/components/svg-color";
import BlueprintSpaceship from "src/sections/oss-setup/BlueprintSpaceship";
import AgentEvalShowcase from "./AgentEvalShowcase";

const twinkle = keyframes`
  0%, 100% { opacity: 0.15; }
  50%      { opacity: 0.85; }
`;

const shoot = keyframes`
  0%   { transform: translate3d(0,0,0) rotate(28deg); opacity: 0; }
  10%  { opacity: 1; }
  30%  { opacity: 1; }
  45%  { transform: translate3d(380px, 200px, 0) rotate(28deg); opacity: 0; }
  100% { opacity: 0; }
`;

// Deterministic starfield so it renders identically each mount.
const STARS = [
  { top: "6%", left: "14%", s: 2, d: 0 },
  { top: "12%", left: "76%", s: 2.5, d: 1.1 },
  { top: "22%", left: "42%", s: 1.5, d: 2.0 },
  { top: "31%", left: "88%", s: 2, d: 0.6 },
  { top: "44%", left: "8%", s: 1.5, d: 1.6 },
  { top: "58%", left: "92%", s: 2, d: 0.3 },
  { top: "67%", left: "20%", s: 1.5, d: 2.4 },
  { top: "78%", left: "70%", s: 2, d: 1.3 },
  { top: "86%", left: "36%", s: 1.5, d: 0.9 },
  { top: "92%", left: "84%", s: 2, d: 2.1 },
  { top: "17%", left: "58%", s: 1.5, d: 1.8 },
  { top: "72%", left: "50%", s: 1.5, d: 0.5 },
];

const RightSectionAuth = () => {
  return (
    <Box
      sx={{
        width: "100%",
        height: "100%",
        px: { xs: 3, lg: 5 },
        py: 4,
        overflowY: "auto",
        position: "relative",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Space backdrop */}
      <Box
        aria-hidden
        sx={{ position: "absolute", inset: 0, overflow: "hidden", pointerEvents: "none" }}
      >
        {STARS.map((st, i) => (
          <Box
            key={i}
            sx={{
              position: "absolute",
              top: st.top,
              left: st.left,
              width: st.s,
              height: st.s,
              borderRadius: "50%",
              bgcolor: "common.white",
              boxShadow: "0 0 6px 1px rgba(255,255,255,0.45)",
              animation: `${twinkle} ${2.4 + (i % 4) * 0.7}s ease-in-out ${st.d}s infinite`,
            }}
          />
        ))}
        <Box
          sx={{
            position: "absolute",
            top: "10%",
            left: "6%",
            width: 90,
            height: 1.5,
            borderRadius: 2,
            background:
              "linear-gradient(90deg, rgba(255,255,255,0.9), rgba(255,255,255,0))",
            filter: "drop-shadow(0 0 6px rgba(255,255,255,0.6))",
            opacity: 0,
            animation: `${shoot} 11s ease-in 3s infinite`,
          }}
        />
      </Box>

      {/* Brand */}
      <Stack
        direction="row"
        gap={0.75}
        alignItems="center"
        sx={{ position: "relative", zIndex: 1 }}
      >
        <SvgColor
          src="/favicon/logo.svg"
          sx={{ height: 40, width: 40, color: "common.white" }}
        />
        <SvgColor
          src="/logo/future_agi_text.svg"
          sx={{ height: 20, width: 128, color: "common.white" }}
        />
      </Stack>

      {/* Headline + product showcase */}
      <Stack
        sx={{
          position: "relative",
          zIndex: 1,
          flex: 1,
          justifyContent: "center",
          maxWidth: 620,
          width: "100%",
          mx: "auto",
          py: 4,
        }}
      >
        <Stack sx={{ mb: 3 }}>
          <Typography
            fontWeight="fontWeightSemiBold"
            sx={{
              fontSize: { xs: "22px", lg: "28px" },
              lineHeight: 1.28,
              fontFamily: "Inter",
              color: "common.white",
            }}
          >
            Ship AI agents you can actually trust.
          </Typography>
          <Typography
            fontWeight="fontWeightMedium"
            sx={{
              fontSize: { xs: "22px", lg: "28px" },
              lineHeight: 1.28,
              fontFamily: "Inter",
              color: "text.secondary",
            }}
          >
            Trace, evaluate and optimise every run — end to end.
          </Typography>
        </Stack>

        <AgentEvalShowcase />
      </Stack>

      {/* Blueprint spaceship accent */}
      <Box
        aria-hidden
        sx={{
          position: "absolute",
          right: { xs: 8, lg: 28 },
          top: 14,
          opacity: 0.45,
          pointerEvents: "none",
          display: { xs: "none", lg: "block" },
        }}
      >
        <BlueprintSpaceship size={92} />
      </Box>
    </Box>
  );
};

export default RightSectionAuth;
