// OSS setup flow — static definitions (UI prototype, no backend).

// Screen 1 — launch mode choices.
export const LAUNCH_MODES = [
  {
    id: "live",
    title: "Live implementation",
    description:
      "Production-ready. All security and infrastructure requirements are enforced.",
    icon: "solar:rocket-2-bold",
  },
  {
    id: "experiment",
    title: "Just experimenting",
    description:
      "Explore locally. Some security requirements are relaxed so you can get started fast.",
    icon: "solar:test-tube-bold",
  },
];

// Footnote shown under the mode picker, keyed by the selected mode.
export const MODE_NOTE = {
  live: "All security requirements will be enforced for a live implementation.",
  experiment:
    "We will not enforce some security requirements in experimentation mode.",
};

export const CHECK_STATUS = {
  PENDING: "pending",
  RUNNING: "running",
  PASSED: "passed",
  WARNING: "warning",
  FAILED: "failed",
  OPTIONAL: "optional",
};

// Screen 2 — the infrastructure checks. `requiredIn` narrows which modes treat
// the check as a hard requirement (blocks Continue when it fails).
export const VALIDATION_CHECKS = [
  { id: "env", label: "Environment configuration", requiredIn: ["live", "experiment"] },
  { id: "database", label: "Application database · Postgres", requiredIn: ["live", "experiment"] },
  { id: "cache", label: "Cache · Redis", requiredIn: ["live", "experiment"] },
  { id: "backend", label: "Backend server · Django", requiredIn: ["live", "experiment"] },
  { id: "worker", label: "Background jobs · Celery", requiredIn: ["live", "experiment"] },
  { id: "frontend", label: "Frontend build · Vite", requiredIn: ["live", "experiment"] },
  { id: "storage", label: "Object storage", requiredIn: ["live", "experiment"] },
  { id: "ports", label: "Network ports available", requiredIn: ["live"] },
  { id: "ssl", label: "SSL/TLS certificate", requiredIn: ["live"] },
];

// Contextual sub-text shown for a check when it lands in a non-passing state.
export const CHECK_DETAIL = {
  ports: "Some ports need elevated privileges",
  ssl_failed: "Certificate not found — required for a live setup",
  ssl_optional: "Not required in experimentation mode",
};

// Seeded outcome of a validation run for the given mode. Deterministic so the
// prototype behaves predictably: a live setup starts with a failing SSL check
// the user must re-run; experimentation relaxes it to optional.
export function seedResults(mode) {
  const { PASSED, WARNING, FAILED, OPTIONAL } = CHECK_STATUS;
  const map = {};
  VALIDATION_CHECKS.forEach((c) => {
    if (c.id === "ssl") map[c.id] = mode === "live" ? FAILED : OPTIONAL;
    else if (c.id === "ports") map[c.id] = WARNING;
    else map[c.id] = PASSED;
  });
  return map;
}
