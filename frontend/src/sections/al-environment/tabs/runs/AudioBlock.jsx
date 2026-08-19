import { useState } from "react";
import PropTypes from "prop-types";
import { Box, Stack, Typography } from "@mui/material";
import { alkBaseUrl, isDirectToHarness } from "src/api/al-environment/client";
import { ALK_MONO } from "../../alkTokens";
import Tag from "../../parts/Tag";

const ALK_BASE = alkBaseUrl(import.meta.env);

// The proxied base already ends where /api begins — the backend adds it when it
// forwards. Only a base that points straight at the harness still needs it, and
// this is the one URL on the page built by hand rather than through the axios
// instance whose baseURL encodes that difference.
const ALK_PREFIX = isDirectToHarness(ALK_BASE) ? "/api" : "";

const trackUrl = (runId, scenario, label) =>
  `${ALK_BASE}${ALK_PREFIX}/recording/${encodeURIComponent(runId)}/${encodeURIComponent(scenario)}` +
  `?track=${encodeURIComponent(label)}`;

/**
 * Every recording that exists, with the best selected. Several tracks are written and any of
 * them can be missing, so the fallback is the normal case: a page that only knew about stereo
 * would show a dead player most days.
 *
 * Deliberately not behind a fold — it is the thing a spoken run exists to produce, and the
 * fastest way to understand a failure is to listen to it.
 */
const AudioBlock = ({ runId, scenario, tracks }) => {
  const list = tracks || [];
  const [chosen, setChosen] = useState(list[0]?.label || "");

  return (
    <Box
      sx={{
        my: 1.2,
        px: 2.4,
        py: 2,
        borderRadius: "6px",
        bgcolor: "action.hover",
        border: "1px solid",
        borderColor: "divider",
      }}
    >
      {list.length === 0 ? (
        <Typography variant="body2" color="text.secondary">
          no recording for this run
        </Typography>
      ) : (
        <>
          <Typography
            sx={{
              fontFamily: ALK_MONO,
              fontSize: 11.2,
              textTransform: "uppercase",
              letterSpacing: "0.06em",
              color: "text.secondary",
              mb: 0.6,
            }}
          >
            {`recording · ${list.length} tracks`}
          </Typography>
          <Box
            component="audio"
            controls
            preload="none"
            data-testid="alk-audio"
            src={chosen ? trackUrl(runId, scenario, chosen) : undefined}
            sx={{ width: "100%", height: 34, display: "block" }}
          />
          <Stack direction="row" spacing={1.4} flexWrap="wrap" useFlexGap sx={{ mt: 1.6 }}>
            {list.map((track) => (
              <Box
                key={track.label}
                component="button"
                type="button"
                // Which track you are listening to is the whole question when there are eight
                // of them: the room's mix, the caller alone, the agent alone, and the
                // provider's own copy of each.
                aria-pressed={track.label === chosen}
                onClick={() => setChosen(track.label)}
                sx={{ p: 0, border: 0, background: "none", cursor: "pointer" }}
              >
                <Tag kind={track.label === chosen ? "pass" : "soft"}>{track.label}</Tag>
              </Box>
            ))}
          </Stack>
        </>
      )}
    </Box>
  );
};

AudioBlock.propTypes = {
  runId: PropTypes.string.isRequired,
  scenario: PropTypes.string.isRequired,
  tracks: PropTypes.array,
};

export default AudioBlock;
