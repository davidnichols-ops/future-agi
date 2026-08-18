import PropTypes from "prop-types";
import { Box, Button, Stack, Typography } from "@mui/material";
import { ALK_MONO } from "./alkTokens";

/**
 * By far the likeliest failure: the harness simply is not running. A generic toast
 * would hide the one fact that fixes it, so name the URL and the command.
 */
const HarnessUnreachable = ({ baseUrl, onRetry }) => (
  <Stack spacing={2} alignItems="center" justifyContent="center" sx={{ height: "100%", p: 4 }}>
    <Typography variant="h6">Can&apos;t reach the harness</Typography>
    <Typography variant="body2" color="text.secondary" align="center">
      Nothing answered at{" "}
      <Box component="span" sx={{ fontFamily: ALK_MONO }}>
        {baseUrl}
      </Box>
      . Start the agent-learning-kit server and try again.
    </Typography>
    <Box
      component="pre"
      sx={{
        fontFamily: ALK_MONO,
        fontSize: 13,
        p: 2,
        m: 0,
        borderRadius: 1,
        bgcolor: "background.neutral",
        color: "text.secondary",
      }}
    >
      .venv/bin/python harness-ui/server.py
    </Box>
    <Button variant="outlined" onClick={onRetry}>
      Try again
    </Button>
  </Stack>
);

HarnessUnreachable.propTypes = {
  baseUrl: PropTypes.string.isRequired,
  onRetry: PropTypes.func.isRequired,
};

export default HarnessUnreachable;
