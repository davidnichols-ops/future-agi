// The check list itself comes from GET /api/setup-checks/ — nothing about it
// lives here, so checks can change server-side without a frontend release.

// Mirrors the server's launch modes. Sent as the `mode` query param.
export const LAUNCH_MODE = {
  LIVE: "live",
  EXPERIMENT: "experiment",
};

export const LAUNCH_MODES = [
  {
    id: LAUNCH_MODE.LIVE,
    title: "Live implementation",
    description:
      "Production-ready. All security and infrastructure requirements are enforced.",
    icon: "solar:rocket-2-bold",
  },
  {
    id: LAUNCH_MODE.EXPERIMENT,
    title: "Just experimenting",
    description:
      "Explore locally. Some security requirements are relaxed so you can get started fast.",
    icon: "solar:test-tube-bold",
  },
];

export const DEFAULT_LAUNCH_MODE = LAUNCH_MODE.LIVE;

export const MODE_NOTE = {
  [LAUNCH_MODE.LIVE]:
    "All security requirements will be enforced for a live implementation.",
  [LAUNCH_MODE.EXPERIMENT]:
    "We will not enforce some security requirements in experimentation mode.",
};

// Mirrors the server enum. No status gates the flow.
export const CHECK_STATUS = {
  PENDING: "pending",
  PASSED: "passed",
  WARNING: "warning",
  FAILED: "failed",
  SKIPPED: "skipped",
};

// Derived from the transport, not from any check.
export const CONNECTION_STATE = {
  CONNECTING: "connecting",
  REACHABLE: "reachable",
  UNREACHABLE: "unreachable",
};

export const CHECK_REVEAL_STAGGER_MS = 350;
