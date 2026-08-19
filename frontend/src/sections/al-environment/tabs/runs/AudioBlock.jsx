import React, { useEffect, useState } from "react";
import PropTypes from "prop-types";
import { useParams } from "react-router-dom";
import { Box, Stack, Typography } from "@mui/material";
import alkAxios from "src/api/al-environment/client";
import { ALK_MONO } from "../../alkTokens";
import Tag from "../../parts/Tag";

// The session rides along because the harness serves recordings out of the session the URL
// names, not only the one it happens to have open — a run outlives its session being open.
export const trackPath = (runId, scenario, label, session) =>
  `/recording/${encodeURIComponent(runId)}/${encodeURIComponent(scenario)}` +
  `?track=${encodeURIComponent(label)}` +
  (session ? `&session=${encodeURIComponent(session)}` : "");

/**
 * Every recording that exists, with the best selected. Several tracks are written and any of
 * them can be missing, so the fallback is the normal case: a page that only knew about stereo
 * would show a dead player most days.
 *
 * Deliberately not behind a fold — it is the thing a spoken run exists to produce, and the
 * fastest way to understand a failure is to listen to it.
 */
const AudioBlock = ({ runId, scenario, tracks }) => {
  const { sessionId } = useParams();
  const list = tracks || [];
  const [chosen, setChosen] = useState(list[0]?.label || "");
  // Fetched through the axios instance rather than pointed at by src: the platform proxy is
  // authenticated, and the audio element's own request carries no Authorization header — every
  // recording 401ed while the same URL answered a curl with a token. The blob rides in with
  // auth and the player gets an object URL instead.
  const [src, setSrc] = useState(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!chosen) return undefined;
    let dead = false;
    let held = null;
    setFailed(false);
    setSrc(null);
    alkAxios
      .get(trackPath(runId, scenario, chosen, sessionId), { responseType: "blob" })
      .then((got) => {
        held = URL.createObjectURL(got.data);
        if (dead) {
          URL.revokeObjectURL(held);
        } else {
          setSrc(held);
        }
      })
      .catch(() => {
        if (!dead) setFailed(true);
      });
    return () => {
      dead = true;
      if (held) URL.revokeObjectURL(held);
    };
  }, [runId, scenario, chosen, sessionId]);

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
          {failed ? (
            <Typography variant="body2" color="text.secondary">
              this track could not be fetched
            </Typography>
          ) : (
            <Box
              component="audio"
              controls
              data-testid="alk-audio"
              data-track={chosen || undefined}
              src={src || undefined}
              sx={{ width: "100%", height: 34, display: "block" }}
            />
          )}
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
