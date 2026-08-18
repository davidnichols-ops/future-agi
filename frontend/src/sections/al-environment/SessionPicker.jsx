import { useState } from "react";
import PropTypes from "prop-types";
import { Button, MenuItem, Select, Stack, Typography } from "@mui/material";
import ConfirmDialog from "src/components/custom-dialog/confirm-dialog";
import { ALK_MONO } from "./alkTokens";

/**
 * Deleting a session removes its whole folder on the harness side, artifacts included,
 * so it asks first. The vanilla page does not, and that is not a precedent worth keeping.
 */
const SessionPicker = ({ sessions, openSessionId, busy, onOpen, onCreate, onDelete }) => {
  const [confirming, setConfirming] = useState(false);
  const open = sessions.find((one) => one.id === openSessionId) || null;

  return (
    <Stack direction="row" alignItems="center" spacing={1}>
      {sessions.length === 0 ? (
        <Typography variant="body2" sx={{ fontFamily: ALK_MONO }}>
          no session — start one
        </Typography>
      ) : (
        <Select
          size="small"
          value={openSessionId || ""}
          disabled={busy}
          onChange={(event) => onOpen(event.target.value)}
          sx={{
            fontFamily: ALK_MONO,
            minWidth: 180,
            // The theme pins small buttons to 30px (theme/overrides/components/button.js),
            // while MUI's own small Select is 41px. Match the buttons it sits beside.
            height: 30,
            fontSize: 12,
            "& .MuiSelect-select": {
              paddingTop: 0,
              paddingBottom: 0,
              lineHeight: "30px",
            },
          }}
          renderValue={() => open?.agent || open?.id || ""}
        >
          {sessions.map((one) => (
            <MenuItem key={one.id} value={one.id} sx={{ fontFamily: ALK_MONO }}>
              {one.agent || one.id}
            </MenuItem>
          ))}
        </Select>
      )}

      <Button size="small" variant="contained" disabled={busy} onClick={() => onCreate()}>
        New
      </Button>
      <Button
        size="small"
        variant="outlined"
        color="error"
        disabled={busy || !openSessionId}
        onClick={() => setConfirming(true)}
      >
        Delete
      </Button>

      <ConfirmDialog
        open={confirming}
        onClose={() => setConfirming(false)}
        title="Delete this session?"
        content="This removes the conversation and every artifact it produced. It cannot be undone."
        action={
          <Button
            size="small"
            variant="contained"
            color="error"
            onClick={() => {
              setConfirming(false);
              onDelete(openSessionId);
            }}
          >
            Delete
          </Button>
        }
      />
    </Stack>
  );
};

SessionPicker.propTypes = {
  sessions: PropTypes.array.isRequired,
  openSessionId: PropTypes.string,
  busy: PropTypes.bool,
  onOpen: PropTypes.func.isRequired,
  onCreate: PropTypes.func.isRequired,
  onDelete: PropTypes.func.isRequired,
};

export default SessionPicker;
