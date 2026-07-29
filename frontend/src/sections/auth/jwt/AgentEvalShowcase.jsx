import React from "react";
import { Box, Stack, Typography } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { keyframes } from "@mui/system";
import Iconify from "src/components/iconify";

// Product showcase for the auth screens — a miniature of the Agent IDE:
// agent graph on the left, live evaluation scores on the right. The eval run
// loops so the panel feels alive without a carousel.

const MONO = "ui-monospace, SFMono-Regular, Menlo, monospace";

const fadeUp = keyframes`
  from { opacity: 0; transform: translateY(8px); }
  to   { opacity: 1; transform: none; }
`;

// One full "Run Eval" cycle: bars grow, hold, reset.
const grow = keyframes`
  0%   { transform: scaleX(0); }
  22%  { transform: scaleX(1); }
  88%  { transform: scaleX(1); }
  100% { transform: scaleX(0); }
`;

const glow = keyframes`
  0%, 100% { box-shadow: 0 0 0 0 rgba(255,255,255,0.25); }
  50%      { box-shadow: 0 0 0 6px rgba(255,255,255,0); }
`;

const dash = keyframes`
  to { stroke-dashoffset: -24; }
`;

const NODES = [
  {
    icon: "solar:magic-stick-3-bold",
    title: "Agent Node",
    sub: "customer_support_v1",
  },
  {
    icon: "solar:magnifer-linear",
    title: "Tool: KB Search",
    sub: "vector_retrieval(top_k=5)",
    tag: "NEW",
  },
  {
    icon: "solar:chat-square-code-linear",
    title: "LLM Prompt",
    model: "gpt-4o-mini",
    tag: "EDITED",
    diff: {
      old: "“You are a helpful support assistant”",
      next: "“Use KB context to resolve issues step-by-step”",
    },
  },
  {
    icon: "solar:transfer-horizontal-linear",
    title: "Router",
    sub: "escalate | resolve | clarify",
  },
  {
    icon: "solar:plain-linear",
    title: "Output",
    sub: "response → user",
  },
];

const METRICS = [
  { label: "Factuality", value: "62%", pct: 62, color: "warning.main" },
  { label: "Relevance", value: "71%", pct: 71, color: "warning.main" },
  { label: "Safety", value: "Pass ✓", pct: 100, color: "success.main" },
  { label: "Completeness", value: "48%", pct: 48, color: "error.main" },
];

const TABS = [
  { icon: "solar:refresh-linear", label: "Iterate" },
  { icon: "solar:play-linear", label: "Simulate" },
  { icon: "solar:check-circle-linear", label: "Evaluate" },
  { icon: "solar:bolt-linear", label: "Optimize" },
  { icon: "solar:eye-linear", label: "Observe" },
  { icon: "solar:shield-check-linear", label: "Command Center" },
];

function Connector() {
  return (
    <Box sx={{ display: "flex", justifyContent: "center", height: 13 }}>
      <Box component="svg" viewBox="0 0 2 13" sx={{ width: 2, height: 13, overflow: "visible" }}>
        <line
          x1="1" y1="0" x2="1" y2="13"
          stroke="currentColor"
          strokeOpacity="0.35"
          strokeWidth="1"
          strokeDasharray="3 3"
          style={{ animation: `${dash} 1.6s linear infinite` }}
        />
      </Box>
    </Box>
  );
}

export default function AgentEvalShowcase() {
  return (
    <Box
      sx={{
        borderRadius: 2,
        border: "1px solid",
        borderColor: "divider",
        bgcolor: (t) => alpha(t.palette.common.black, 0.55),
        backdropFilter: "blur(6px)",
        overflow: "hidden",
        boxShadow: "0 24px 60px rgba(0,0,0,0.55)",
        animation: `${fadeUp} 0.6s ease both`,
      }}
    >
      {/* Title bar */}
      <Stack
        direction="row"
        alignItems="center"
        sx={{ px: 1.5, py: 1, borderBottom: "1px solid", borderColor: "divider" }}
      >
        <Stack direction="row" spacing={0.75}>
          {["#3A3A3D", "#3A3A3D", "#3A3A3D"].map((c, i) => (
            <Box key={i} sx={{ width: 8, height: 8, borderRadius: "50%", bgcolor: c }} />
          ))}
        </Stack>
        <Typography
          sx={{ flex: 1, textAlign: "center", fontFamily: MONO, fontSize: 10.5, color: "text.disabled" }}
        >
          futureagi.com / agents / support-bot
        </Typography>
        <Box sx={{ width: 34 }} />
      </Stack>

      {/* App header */}
      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{ px: 1.5, py: 1.25, borderBottom: "1px solid", borderColor: "divider" }}
      >
        <Iconify icon="solar:widget-4-linear" width={15} sx={{ color: "text.secondary" }} />
        <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: "text.primary" }}>
          Support Agent
        </Typography>
        <Box
          sx={{
            px: 0.75, py: 0.1, borderRadius: 0.5, border: "1px solid",
            borderColor: "divider", fontSize: 10, fontFamily: MONO, color: "text.secondary",
          }}
        >
          v2
        </Box>
        <Box sx={{ flex: 1 }} />
        <Typography sx={{ fontSize: 12, fontWeight: 700, color: "warning.main" }}>67%</Typography>
        <Stack
          direction="row"
          alignItems="center"
          spacing={0.5}
          sx={{
            px: 1, py: 0.4, borderRadius: 0.75,
            bgcolor: "common.white", color: "common.black",
            animation: `${glow} 3.2s ease-in-out infinite`,
          }}
        >
          <Iconify icon="solar:play-bold" width={10} />
          <Typography sx={{ fontSize: 11, fontWeight: 700 }}>Run Eval</Typography>
        </Stack>
      </Stack>

      {/* Body */}
      <Stack direction="row">
        {/* Left rail */}
        <Stack
          alignItems="center"
          spacing={1.25}
          sx={{ width: 34, py: 1.5, borderRight: "1px solid", borderColor: "divider" }}
        >
          {["solar:magic-stick-3-linear", "solar:add-circle-linear", "solar:link-linear"].map(
            (ic, i) => (
              <Box
                key={ic}
                sx={{
                  width: 20, height: 20, borderRadius: 0.75,
                  display: "flex", alignItems: "center", justifyContent: "center",
                  bgcolor: i === 0 ? "action.selected" : "transparent",
                  color: i === 0 ? "text.primary" : "text.disabled",
                }}
              >
                <Iconify icon={ic} width={12} />
              </Box>
            ),
          )}
        </Stack>

        {/* Agent graph */}
        <Box
          sx={{
            flex: 1, p: 1.75, color: "text.secondary",
            backgroundImage:
              "linear-gradient(rgba(255,255,255,0.025) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.025) 1px, transparent 1px)",
            backgroundSize: "26px 26px",
          }}
        >
          {NODES.map((n, i) => (
            <React.Fragment key={n.title}>
              {i > 0 && <Connector />}
              <Box
                sx={{
                  borderRadius: 1,
                  border: "1px solid",
                  borderColor: "divider",
                  bgcolor: (t) => alpha(t.palette.common.white, 0.03),
                  px: 1.25, py: 0.75,
                  animation: `${fadeUp} 0.5s ease both`,
                  animationDelay: `${0.15 + i * 0.12}s`,
                }}
              >
                <Stack direction="row" alignItems="center" spacing={1}>
                  <Box
                    sx={{
                      width: 20, height: 20, borderRadius: 0.75, flexShrink: 0,
                      display: "flex", alignItems: "center", justifyContent: "center",
                      bgcolor: (t) => alpha(t.palette.common.white, 0.07),
                      color: "text.secondary",
                    }}
                  >
                    <Iconify icon={n.icon} width={12} />
                  </Box>
                  <Stack sx={{ flex: 1, minWidth: 0 }}>
                    <Stack direction="row" alignItems="center" spacing={0.75}>
                      <Typography sx={{ fontSize: 11.5, fontWeight: 600, color: "text.primary" }}>
                        {n.title}
                      </Typography>
                      {n.model && (
                        <Typography sx={{ fontSize: 9.5, fontFamily: MONO, color: "text.disabled" }}>
                          {n.model}
                        </Typography>
                      )}
                    </Stack>
                    {n.sub && (
                      <Typography sx={{ fontSize: 9.5, fontFamily: MONO, color: "text.disabled" }}>
                        {n.sub}
                      </Typography>
                    )}
                  </Stack>
                  {n.tag && (
                    <Box
                      sx={{
                        px: 0.6, py: 0.05, borderRadius: 0.5, border: "1px solid",
                        borderColor: "divider", fontSize: 8.5, fontFamily: MONO,
                        color: "text.secondary", flexShrink: 0,
                      }}
                    >
                      {n.tag}
                    </Box>
                  )}
                </Stack>

                {n.diff && (
                  <Stack sx={{ mt: 0.75, pl: 3.5 }} spacing={0.25}>
                    <Typography
                      sx={{
                        fontSize: 9.5, color: "text.disabled",
                        textDecoration: "line-through", opacity: 0.6,
                      }}
                    >
                      {n.diff.old}
                    </Typography>
                    <Typography sx={{ fontSize: 9.5, color: "text.primary" }}>
                      {n.diff.next}
                    </Typography>
                  </Stack>
                )}
              </Box>
            </React.Fragment>
          ))}
        </Box>

        {/* Evaluation panel */}
        <Box sx={{ width: "42%", p: 1.75, borderLeft: "1px solid", borderColor: "divider" }}>
          <Stack direction="row" alignItems="center" sx={{ mb: 1.5 }}>
            <Typography
              sx={{ flex: 1, fontFamily: MONO, fontSize: 9.5, letterSpacing: 1, color: "text.disabled" }}
            >
              EVALUATION
            </Typography>
            <Typography sx={{ fontFamily: MONO, fontSize: 9.5, color: "text.disabled" }}>
              Run 1
            </Typography>
          </Stack>

          {METRICS.map((m, i) => (
            <Box key={m.label} sx={{ mb: 1.25 }}>
              <Stack direction="row" alignItems="center" sx={{ mb: 0.5 }}>
                <Typography sx={{ flex: 1, fontSize: 10.5, color: "text.secondary" }}>
                  {m.label}
                </Typography>
                <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: m.color }}>
                  {m.value}
                </Typography>
              </Stack>
              <Box
                sx={{
                  height: 3, borderRadius: 2, overflow: "hidden",
                  bgcolor: (t) => alpha(t.palette.common.white, 0.08),
                }}
              >
                <Box sx={{ width: `${m.pct}%`, height: "100%" }}>
                  <Box
                    sx={{
                      width: "100%", height: "100%", borderRadius: 2, bgcolor: m.color,
                      transformOrigin: "left",
                      animation: `${grow} 7s ease-in-out infinite`,
                      animationDelay: `${i * 0.18}s`,
                    }}
                  />
                </Box>
              </Box>
            </Box>
          ))}

          <Stack direction="row" alignItems="baseline" sx={{ mt: 2, mb: 1.5 }}>
            <Typography sx={{ flex: 1, fontSize: 12, fontWeight: 600, color: "text.primary" }}>
              Overall
            </Typography>
            <Typography sx={{ fontSize: 15, fontWeight: 800, color: "warning.main" }}>
              67%
            </Typography>
          </Stack>

          <Box
            sx={{
              borderRadius: 1, border: "1px solid", borderColor: "divider",
              bgcolor: (t) => alpha(t.palette.common.white, 0.03), p: 1,
            }}
          >
            <Typography sx={{ fontSize: 9.5, fontFamily: MONO, color: "text.secondary", lineHeight: 1.5 }}>
              ⚠ Agent relies on general knowledge. Add retrieval step for KB articles.
            </Typography>
          </Box>
        </Box>
      </Stack>

      {/* Version bar */}
      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{ px: 1.5, py: 0.75, borderTop: "1px solid", borderColor: "divider" }}
      >
        <Box sx={{ width: 5, height: 5, borderRadius: "50%", bgcolor: "text.disabled" }} />
        <Typography sx={{ fontFamily: MONO, fontSize: 9.5, color: "text.disabled" }}>
          v1 67%
        </Typography>
      </Stack>

      {/* Bottom tabs */}
      <Stack direction="row" sx={{ borderTop: "1px solid", borderColor: "divider" }}>
        {TABS.map((t, i) => (
          <Stack
            key={t.label}
            direction="row"
            alignItems="center"
            justifyContent="center"
            spacing={0.5}
            sx={{
              flex: 1, py: 1,
              borderLeft: i === 0 ? "none" : "1px solid",
              borderColor: "divider",
              color: i === 0 ? "text.primary" : "text.disabled",
            }}
          >
            <Iconify icon={t.icon} width={11} />
            <Typography sx={{ fontSize: 9.5, fontWeight: 600, whiteSpace: "nowrap" }}>
              {t.label}
            </Typography>
          </Stack>
        ))}
      </Stack>
    </Box>
  );
}
