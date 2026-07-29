import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import {
  Box,
  Stack,
  Typography,
  Link,
  Collapse,
  IconButton,
  CircularProgress,
  Tooltip,
  useTheme,
} from "@mui/material";
import { alpha } from "@mui/material/styles";
import LoadingButton from "@mui/lab/LoadingButton";
import Iconify from "src/components/iconify";
import {
  VALIDATION_CHECKS,
  CHECK_STATUS,
  CHECK_DETAIL,
  seedResults,
} from "./constants";

const { PENDING, RUNNING, PASSED, WARNING, FAILED, OPTIONAL } = CHECK_STATUS;

// Per-status presentation: icon, colour token and status pill label.
const STATUS_META = {
  [PASSED]: { icon: "solar:check-circle-bold", color: "success.main", label: "Validated" },
  [WARNING]: { icon: "solar:danger-triangle-bold", color: "warning.main", label: "Warning" },
  [FAILED]: { icon: "solar:close-circle-bold", color: "error.main", label: "Failed" },
  [OPTIONAL]: { icon: "solar:minus-circle-linear", color: "text.disabled", label: "Optional" },
};

// Screen 2 — runs the infrastructure checks for the chosen mode. Checks animate
// pending → running → result; anything that fails can be re-run individually or
// via "Validate requirements". Continue unlocks once every required check passes.
export default function ValidationStep({ mode, onBack, onContinue, onProgress }) {
  const theme = useTheme();
  const tint = (main, a = 0.14) => alpha(theme.palette[main].main, a);
  const seed = useMemo(() => seedResults(mode), [mode]);
  // Checks the user has manually fixed via a re-run — these override the seed
  // on any subsequent full re-validation.
  const overridesRef = useRef({});
  const timers = useRef([]);
  const [statuses, setStatuses] = useState(() =>
    VALIDATION_CHECKS.map((c) => ({ id: c.id, status: PENDING })),
  );
  const [expanded, setExpanded] = useState(true);
  const [running, setRunning] = useState(false);

  const clearTimers = () => {
    timers.current.forEach((t) => clearTimeout(t));
    timers.current = [];
  };

  const setOne = useCallback((id, status) => {
    setStatuses((prev) => prev.map((s) => (s.id === id ? { id, status } : s)));
  }, []);

  const resolvedStatus = useCallback(
    (id) => overridesRef.current[id] ?? seed[id],
    [seed],
  );

  // Re-validate everything: reset to running, then stagger each result in.
  const runAll = useCallback(() => {
    clearTimers();
    setRunning(true);
    setStatuses(VALIDATION_CHECKS.map((c) => ({ id: c.id, status: RUNNING })));
    VALIDATION_CHECKS.forEach((c, i) => {
      const t = setTimeout(
        () => {
          setOne(c.id, resolvedStatus(c.id));
          if (i === VALIDATION_CHECKS.length - 1) setRunning(false);
        },
        350 * (i + 1),
      );
      timers.current.push(t);
    });
  }, [resolvedStatus, setOne]);

  // Re-run a single failing/warning check — resolves to passed in the prototype.
  const runOne = useCallback(
    (id) => {
      setOne(id, RUNNING);
      const t = setTimeout(() => {
        overridesRef.current[id] = PASSED;
        setOne(id, PASSED);
      }, 900);
      timers.current.push(t);
    },
    [setOne],
  );

  useEffect(() => {
    runAll();
    return clearTimers;
    // Run once on mount for the selected mode.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Fraction of checks that have finished running — drives the ship's position
  // so it reaches the end of the track exactly when every check has settled.
  const progress = useMemo(() => {
    const settled = statuses.filter(
      (s) => s.status !== PENDING && s.status !== RUNNING,
    ).length;
    return statuses.length ? settled / statuses.length : 0;
  }, [statuses]);

  useEffect(() => {
    onProgress?.(progress);
  }, [progress, onProgress]);

  const counts = useMemo(() => {
    const c = { passed: 0, warning: 0, failed: 0, optional: 0 };
    statuses.forEach((s) => {
      if (s.status === PASSED) c.passed += 1;
      else if (s.status === WARNING) c.warning += 1;
      else if (s.status === FAILED) c.failed += 1;
      else if (s.status === OPTIONAL) c.optional += 1;
    });
    return c;
  }, [statuses]);

  // Continue is blocked while anything is still running or a required check fails.
  const blocked = useMemo(
    () =>
      running ||
      statuses.some(
        (s) =>
          s.status === RUNNING ||
          s.status === PENDING ||
          s.status === FAILED,
      ),
    [running, statuses],
  );

  const summary = useMemo(() => {
    const parts = [];
    if (counts.passed) parts.push(`${counts.passed} successful`);
    if (counts.warning) parts.push(`${counts.warning} warning`);
    if (counts.failed) parts.push(`${counts.failed} failed`);
    if (counts.optional) parts.push(`${counts.optional} optional`);
    return parts.join(", ") || "Running checks…";
  }, [counts]);

  const summaryIcon = counts.failed
    ? { icon: "solar:close-circle-bold", color: "error.main", bg: tint("error", 0.16) }
    : counts.warning
      ? { icon: "solar:danger-triangle-bold", color: "warning.main", bg: tint("warning", 0.16) }
      : { icon: "solar:check-circle-bold", color: "success.main", bg: tint("success", 0.16) };

  const detailFor = (id, status) => {
    if (id === "ports" && status === WARNING) return CHECK_DETAIL.ports;
    if (id === "ssl" && status === FAILED) return CHECK_DETAIL.ssl_failed;
    if (id === "ssl" && status === OPTIONAL) return CHECK_DETAIL.ssl_optional;
    return null;
  };

  const renderHead = (
    <Stack sx={{ mb: 2.5 }}>
      <Typography
        fontWeight="fontWeightSemiBold"
        sx={{
          fontSize: "28px",
          color: "text.primary",
          fontFamily: "Inter",
          lineHeight: "36px",
        }}
      >
        Validate your setup
      </Typography>
      <Typography
        sx={{
          fontSize: "15px",
          color: "text.secondary",
          maxWidth: "460px",
          mt: 1,
          lineHeight: "24px",
        }}
      >
        Validation runs immediately. You can re-run any check that fails. If you
        get stuck, see the{" "}
        <Link
          href="https://docs.futureagi.com"
          target="_blank"
          rel="noopener"
          underline="always"
        >
          self-host guide
        </Link>
        .
      </Typography>
    </Stack>
  );

  const renderRow = (check) => {
    const status = statuses.find((s) => s.id === check.id)?.status ?? PENDING;
    const meta = STATUS_META[status];
    const detail = detailFor(check.id, status);
    const isBusy = status === RUNNING || status === PENDING;
    const canRerun = status === FAILED || status === WARNING;

    return (
      <Stack
        key={check.id}
        direction="row"
        alignItems="center"
        spacing={1.5}
        sx={{
          px: 2,
          py: 1.5,
          borderTop: "1px solid",
          borderColor: status === FAILED ? tint("error", 0.28) : "divider",
          bgcolor: status === FAILED ? tint("error", 0.1) : "transparent",
        }}
      >
        <Box sx={{ width: 22, display: "flex", justifyContent: "center", flexShrink: 0 }}>
          {isBusy ? (
            <CircularProgress size={18} thickness={5} />
          ) : (
            <Iconify icon={meta.icon} width={22} sx={{ color: meta.color }} />
          )}
        </Box>

        <Stack sx={{ flex: 1, minWidth: 0 }}>
          <Typography
            fontWeight="fontWeightMedium"
            sx={{ fontSize: "14px", color: "text.primary" }}
          >
            {check.label}
          </Typography>
          {detail && (
            <Typography sx={{ fontSize: "12px", color: "text.secondary", lineHeight: "18px" }}>
              {detail}
            </Typography>
          )}
        </Stack>

        {canRerun && (
          <Tooltip title="Re-run this check">
            <IconButton size="small" onClick={() => runOne(check.id)}>
              <Iconify icon="solar:refresh-linear" width={16} />
            </IconButton>
          </Tooltip>
        )}

        <Typography
          fontWeight="fontWeightSemiBold"
          sx={{
            fontSize: "12px",
            color: isBusy ? "text.secondary" : meta.color,
            whiteSpace: "nowrap",
          }}
        >
          {isBusy ? "Checking…" : meta.label}
        </Typography>
      </Stack>
    );
  };

  const renderChecks = (
    <Box
      sx={{
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        overflow: "hidden",
        maxWidth: "460px",
      }}
    >
      {/* Summary header */}
      <Stack
        direction="row"
        alignItems="center"
        spacing={1.5}
        sx={{ px: 2, py: 1.75 }}
      >
        <Box
          sx={{
            width: 36,
            height: 36,
            borderRadius: "50%",
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            bgcolor: summaryIcon.bg,
            color: summaryIcon.color,
          }}
        >
          <Iconify icon={summaryIcon.icon} width={20} />
        </Box>
        <Stack sx={{ flex: 1 }}>
          <Typography
            fontWeight="fontWeightSemiBold"
            sx={{ fontSize: "15px", color: "text.primary" }}
          >
            Validation checks
          </Typography>
          <Typography sx={{ fontSize: "13px", color: "text.secondary" }}>
            {summary}
          </Typography>
        </Stack>
        <IconButton size="small" onClick={() => setExpanded((v) => !v)}>
          <Iconify
            icon={expanded ? "solar:alt-arrow-up-linear" : "solar:alt-arrow-down-linear"}
            width={18}
          />
        </IconButton>
      </Stack>

      <Collapse in={expanded}>
        {/* Rows fill the remaining viewport height (≈560px is everything else on
            screen), so the list stretches toward the bottom on tall screens and
            scrolls on short ones — while Continue / Back stay visible. */}
        <Box sx={{ maxHeight: "calc(100vh - 560px)", overflowY: "auto" }}>
          {VALIDATION_CHECKS.map(renderRow)}
        </Box>

        {/* Re-run all */}
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="center"
          spacing={1}
          onClick={running ? undefined : runAll}
          sx={{
            px: 2,
            py: 1.5,
            borderTop: "1px solid",
            borderColor: "divider",
            cursor: running ? "default" : "pointer",
            color: running ? "text.disabled" : "text.primary",
            "&:hover": { bgcolor: running ? "transparent" : "action.hover" },
          }}
        >
          <Iconify icon="solar:refresh-linear" width={16} />
          <Typography fontWeight="fontWeightSemiBold" sx={{ fontSize: "13px" }}>
            Validate requirements
          </Typography>
        </Stack>
      </Collapse>
    </Box>
  );

  return (
    <>
      {renderHead}
      {renderChecks}

      <Stack spacing={0.5} sx={{ maxWidth: "460px", mt: 2 }}>
        <LoadingButton
          fullWidth
          color="primary"
          variant="contained"
          onClick={onContinue}
          disabled={blocked}
          sx={{ height: "40px", borderRadius: 0.5 }}
        >
          Continue
        </LoadingButton>
        <LoadingButton
          fullWidth
          variant="text"
          onClick={onBack}
          sx={{ height: "34px", borderRadius: 0.5, color: "text.secondary" }}
        >
          Back
        </LoadingButton>
      </Stack>
    </>
  );
}

ValidationStep.propTypes = {
  mode: PropTypes.string.isRequired,
  onBack: PropTypes.func.isRequired,
  onContinue: PropTypes.func.isRequired,
  onProgress: PropTypes.func,
};
