import { Box } from "@mui/material";
import { useCallback } from "react";
import { Helmet } from "react-helmet-async";
import { useNavigate } from "react-router-dom";

import { useCreateAlkSession } from "src/api/al-environment/alEnvironment";
import { paths } from "src/routes/paths";
import EnvironmentsListView from "src/sections/rl-environments/EnvironmentsListView";

const RlEnvironments = () => {
  const navigate = useNavigate();

  const createSession = useCreateAlkSession();

  const handleOpen = useCallback(
    (sessionId) => navigate(paths.dashboard.simulate.alEnvironmentDetail(sessionId)),
    [navigate],
  );

  // Creating returns the fresh status, so the new session's id comes back with it and there
  // is nothing to look up before navigating.
  const handleAdd = useCallback(
    () =>
      createSession.mutate("", {
        onSuccess: (status) => {
          if (status?.session?.id) {
            navigate(paths.dashboard.simulate.alEnvironmentDetail(status.session.id));
          }
        },
      }),
    [createSession, navigate],
  );

  return (
    <>
      <Helmet>
        <title>RL Environments</title>
      </Helmet>
      <Box
        sx={{
          backgroundColor: "background.default",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}
      >
        <EnvironmentsListView onOpen={handleOpen} onAdd={handleAdd} />
      </Box>
    </>
  );
};

export default RlEnvironments;
