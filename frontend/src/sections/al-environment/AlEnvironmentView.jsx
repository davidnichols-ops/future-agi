import { useState } from "react";
import { Box, Stack, Tab, Tabs, Typography } from "@mui/material";
import {
  useAlkContract, useAlkHistory, useAlkScenarios, useAlkSessions, useAlkSimulation,
  useAlkRuns, useAlkSimulations, useAlkStatus, useAlkSubgoals, useAlkWorld,
  useCreateAlkSession, useDeleteAlkSession, useOpenAlkSession, useSetAlkStage,
} from "src/api/al-environment/alEnvironment";
import { alkBaseUrl } from "src/api/al-environment/client";
import { useAlkConversation } from "src/api/al-environment/useAlkConversation";
import { ALK_MONO } from "./alkTokens";
import Composer from "./Composer";
import HarnessUnreachable from "./HarnessUnreachable";
import SessionPicker from "./SessionPicker";
import StageRoadmap from "./StageRoadmap";
import { ALK_STAGES } from "./stages";
import StatusReadout from "./StatusReadout";
import TranscriptPane from "./TranscriptPane";
import ContractTab from "./tabs/ContractTab";
import EnvironmentTab from "./tabs/EnvironmentTab";
import ScenariosTab from "./tabs/ScenariosTab";
import RunsTab from "./tabs/RunsTab";

/** Each tab carries what it holds, so the reader can see where the work has got to. */
const TABS = [
  { value: "contract", label: "Contract", count: (s) => (s?.have?.contract ? "✓" : "") },
  { value: "world", label: "Environment", count: (s) => (s?.have?.world ? "✓" : "") },
  { value: "scenarios", label: "Scenarios", count: (s) => s?.have?.scenarios || "" },
  { value: "runs", label: "Runs", count: (s) => s?.have?.runs || "" },
];

const AlEnvironmentView = () => {
  const [tab, setTab] = useState("contract");
  const [selectedRunId, setSelectedRunId] = useState(null);

  const { status, isError, refetch } = useAlkStatus();
  const hasSession = Boolean(status?.session);

  const { sessions, openSessionId } = useAlkSessions();
  const { messages } = useAlkHistory(hasSession);
  const { contract } = useAlkContract(hasSession);
  const { world } = useAlkWorld(hasSession);
  const { subgoals } = useAlkSubgoals(hasSession);
  const { scenarios } = useAlkScenarios(hasSession);
  const { runs } = useAlkSimulations(hasSession);
  // Sessions whose results predate the simulations format still have readable runs.
  const { legacyRuns } = useAlkRuns(hasSession);
  const { run } = useAlkSimulation(selectedRunId);

  const createSession = useCreateAlkSession();
  const openSession = useOpenAlkSession();
  const deleteSession = useDeleteAlkSession();
  const setStage = useSetAlkStage();
  const conversation = useAlkConversation();

  if (isError) {
    return <HarnessUnreachable baseUrl={alkBaseUrl(import.meta.env)} onRetry={refetch} />;
  }

  /**
   * The harness answers 409 rather than interleaving when a stage is already running, and its
   * body carries the reason. Show that sentence rather than a generic failure — it is the only
   * thing that tells the operator to simply wait.
   */
  const refusal = [setStage, createSession, openSession, deleteSession].find((one) => one.error)?.error;
  // Conversation errors are rendered in the thread by TranscriptPane; only refusals from the
  // session and stage controls need saying up here, next to the controls that caused them.
  const refusalMessage = refusal?.response?.data?.error || refusal?.message || "";

  // Stored history plus whatever is still arriving. The harness writes the turn to disk when
  // it finishes, so the live half is dropped as soon as history catches up.
  const transcript = [...messages, ...conversation.live];

  /** The roadmap is navigation as well as a control: opening a finished stage shows its output. */
  const selectStage = (stageKey) => {
    const stage = ALK_STAGES.find((one) => one.key === stageKey);
    if (stage?.tab) setTab(stage.tab);
    setStage.mutate(stageKey);
  };

  return (
    <Stack sx={{ height: "100%" }}>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        spacing={2}
        sx={{
          px: 2,
          py: 1.5,
          flexWrap: "wrap",
          gap: 1,
          bgcolor: "background.paper",
          // The rule is inset to the same 24px as the content it separates; run edge to edge
          // it sticks out past everything and reads as belonging to the sidebar.
          "&::after": {
            content: '""',
            position: "absolute",
            left: 24,
            right: 24,
            bottom: 0,
            borderBottom: "1px solid",
            borderColor: "divider",
          },
          position: "relative",
        }}
      >
        <SessionPicker
          sessions={sessions}
          openSessionId={openSessionId}
          busy={Boolean(status?.busy)}
          // Each of these changes which conversation is open, so the turn still on screen
          // belongs to the old one. Stored history refetches itself; the live half has to be
          // dropped explicitly or the previous session's messages hang around under the new one.
          onOpen={(id) => openSession.mutate(id, { onSuccess: conversation.clearLive })}
          onCreate={() => createSession.mutate("", { onSuccess: conversation.clearLive })}
          onDelete={(id) => deleteSession.mutate(id, { onSuccess: conversation.clearLive })}
        />
        <StageRoadmap status={status} onSelectStage={selectStage} />
        <StatusReadout
          model={status?.model}
          spentUsd={status?.spent_usd}
          busy={Boolean(status?.busy)}
        />
      </Stack>

      {refusalMessage && (
        <Typography
          sx={{
            mx: 2,
            my: 1,
            pl: 1.25,
            fontFamily: ALK_MONO,
            fontSize: 12.5,
            color: "error.main",
            borderLeft: "2px solid",
            borderColor: "error.main",
          }}
        >
          {refusalMessage}
        </Typography>
      )}

      <Stack direction="row" sx={{ flexGrow: 1, minHeight: 0 }}>
        <Box
          data-testid="alk-transcript-pane"
          sx={{
            // flexShrink 0 keeps the split fixed. Without it the pane shrinks by whatever the
            // active tab's content happens to be wide, so the transcript jumped between tabs.
            width: "40%",
            minWidth: 320,
            flexShrink: 0,
            display: "flex",
            flexDirection: "column",
            bgcolor: "background.paper",
            borderRight: "1px solid",
            borderColor: "divider",
          }}
        >
          <Stack sx={{ height: "100%" }}>
            <Box sx={{ flexGrow: 1, minHeight: 0, overflowY: "auto" }}>
              <TranscriptPane
                messages={transcript}
                hasSession={hasSession}
                thinking={conversation.thinking}
              />
            </Box>
            <Composer
              onSay={conversation.say}
              onRun={conversation.runScenarios}
              onStop={conversation.stop}
              streaming={conversation.streaming}
              status={status}
              sessionId={status?.session?.id}
              artifactsPath={status?.out}
            />
          </Stack>
        </Box>

        <Box
          data-testid="alk-artifact-pane"
          // flexBasis 0 so the tab content's intrinsic width never feeds back into the split.
          sx={{ flexGrow: 1, flexBasis: 0, minWidth: 0, display: "flex", flexDirection: "column" }}
        >
          <Tabs
            value={tab}
            onChange={(event, next) => setTab(next)}
            // The theme defaults every Tabs to variant="scrollable", which draws ‹ › buttons
            // even though four tabs always fit.
            variant="standard"
            scrollButtons={false}
            sx={{
              px: 2,
              minHeight: 40,
              backgroundColor: "background.paper",
              borderBottom: (theme) => `1px solid ${theme.palette.divider}`,
              // The theme spaces tabs with `&:not(:last-of-type) { marginRight }`, so the
              // override has to match that selector to win. The reference's tabs sit next to
              // each other and are spaced by their own padding instead.
              "& .MuiTab-root:not(:last-of-type)": { marginRight: 0 },
              "& .MuiTab-root": {
                minHeight: 40,
                minWidth: 0,
                paddingLeft: "14px",
                paddingRight: "14px",
                fontFamily: ALK_MONO,
                fontSize: 11.8,
                letterSpacing: "0.04em",
                textTransform: "none",
              },
            }}
          >
            {TABS.map((one) => (
              <Tab
                key={one.value}
                value={one.value}
                label={
                  <Box component="span" sx={{ display: "inline-flex", gap: 0.6, alignItems: "baseline" }}>
                    {one.label}
                    {one.count(status) && (
                      <Box component="span" sx={{ opacity: 0.55 }}>
                        {one.count(status)}
                      </Box>
                    )}
                  </Box>
                }
              />
            ))}
          </Tabs>
          <Box
            sx={{
              flexGrow: 1,
              overflowY: "auto",
              // No card here: each tab draws its own panes, and wrapping them in another
              // bordered box produced a card inside a card.
              p: 2,
            }}
          >
            {tab === "contract" && <ContractTab contract={contract} />}
            {tab === "world" && <EnvironmentTab world={world} subgoals={subgoals} />}
            {tab === "scenarios" && (
              <ScenariosTab
                scenarios={scenarios}
                // The per-scenario records from /api/runs, not the simulation summaries:
                // this is looked up by scenario name, and a summary has no `scenario`.
                runs={legacyRuns}
                hasWorld={Boolean(status?.have?.world)}
                onSay={conversation.say}
                onSeeRun={() => setTab("runs")}
              />
            )}
            {tab === "runs" && (
              <RunsTab
                runs={runs}
                selectedRunId={selectedRunId}
                onSelectRun={setSelectedRunId}
                run={run}
                legacyRuns={legacyRuns}
              />
            )}
          </Box>
        </Box>
      </Stack>
    </Stack>
  );
};

export default AlEnvironmentView;
