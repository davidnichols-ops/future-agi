import React, { useEffect, useState } from "react";
import {
  Box,
  Button,
  Chip,
  Drawer,
  IconButton,
  LinearProgress,
  Paper,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import { Icon } from "@iconify/react";
import PropTypes from "prop-types";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { useParams } from "react-router";
import { enqueueSnackbar } from "src/components/snackbar";
import SvgColor from "src/components/svg-color";
import { ShowComponent } from "src/components/show";
import ConfirmRunEvaluations from "src/sections/common/EvaluationDrawer/ConfirmRunEvaluations";
import { getVersionedEvalName } from "src/components/run-tests/common";
import { EvalPickerDrawer } from "src/sections/common/EvalPicker";
import { buildExperimentEvalRuntimePayload } from "src/sections/common/EvalPicker/evalPickerConfigUtils";

const isUUID = (str) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(str);

const transformEvals = (evalList) =>
  evalList.map((evalItem) => ({
    ...(evalItem?.actualEvalCreatedId &&
      isUUID(evalItem?.actualEvalCreatedId) && {
        id: evalItem?.actualEvalCreatedId,
      }),
    template_id: evalItem.template_id || evalItem.templateId || evalItem.id,
    name: evalItem.name || evalItem.evalTemplateName || "Unnamed Evaluation",
    config: evalItem.config,
    model: evalItem.model,
    error_localizer: evalItem.errorLocalizer,
    kb_id: evalItem.kbId || null,
    pinned_version_id: evalItem.pinned_version_id,
    ...(evalItem.composite_weight_overrides
      ? { composite_weight_overrides: evalItem.composite_weight_overrides }
      : {}),
  }));

const ManageExperimentEvalsDrawer = ({
  open,
  onClose,
  allColumns,
  refreshGrid,
  experimentData,
  isExperimentDetailLoading,
}) => {
  const theme = useTheme();
  const { experimentId } = useParams();
  const datasetId =
    new URLSearchParams(window.location.search).get("datasetId") || "";
  const queryClient = useQueryClient();
  const [evals, setEvals] = useState([]);
  const [openEvaluationDialog, setOpenEvaluationDialog] = useState(false);
  const [openConfirmRun, setOpenConfirmRun] = useState(false);
  const [editingEval, setEditingEval] = useState(null);

  // Initialize evals from experiment data
  useEffect(() => {
    if (experimentData?.userEvalMetrics) {
      setEvals(
        experimentData.userEvalMetrics.map((item) => ({
          ...item,
          actualEvalCreatedId: item.id,
        })),
      );
    }
  }, [experimentData]);

  // Update experiment mutation
  const { mutate: updateExperiment, isPending: isUpdating } = useMutation({
    mutationFn: async (updatedEvals) => {
      return axios.put(endpoints.develop.experiment.update(experimentId), {
        ...experimentData,
        user_eval_metrics: transformEvals(updatedEvals),
      });
    },
    onSuccess: () => {
      enqueueSnackbar("Evaluations updated successfully", {
        variant: "success",
      });
      invalidateEvalCaches();
      refreshGrid?.({ purge: true });
    },
    onError: (error) => {
      enqueueSnackbar(error?.message || "Failed to update evaluations", {
        variant: "error",
      });
    },
  });

  const invalidateEvalCaches = () => {
    queryClient.invalidateQueries({ queryKey: ["experiment", experimentId] });
    queryClient.invalidateQueries({ queryKey: ["experiment-list"] });
    queryClient.invalidateQueries({
      queryKey: ["experiment", "user-eval-list"],
    });
    queryClient.invalidateQueries({ queryKey: ["develop", "user-eval-list"] });
    queryClient.invalidateQueries({
      queryKey: ["develop", "previously_configured-eval-list"],
    });
    queryClient.invalidateQueries({
      queryKey: ["optimize-develop-column-info"],
    });
  };

  const { mutate: runEvaluations, isPending: isRunningEvaluations } =
    useMutation({
      mutationFn: ({ data }) =>
        axios.post(endpoints.develop.eval.runEvals(datasetId), {
          user_eval_ids: data,
          experiment_id: experimentId,
          failed_only: false,
        }),
      onSuccess: () => {
        setOpenConfirmRun(false);
        invalidateEvalCaches();
        refreshGrid?.({ purge: true });
        onClose?.();
      },
      onError: (error) => {
        enqueueSnackbar(error?.message || "Failed to run evaluations", {
          variant: "error",
        });
      },
    });

  const { mutate: stopSingleEval } = useMutation({
    mutationFn: (evalId) =>
      axios.post(endpoints.develop.eval.stopEval(datasetId, evalId), {
        experiment_id: experimentId,
      }),
    onSuccess: () => {
      enqueueSnackbar("User evaluation stopped", { variant: "success" });
      invalidateEvalCaches();
      refreshGrid?.({ purge: true });
    },
    onError: (error) => {
      enqueueSnackbar(error?.message || "Failed to stop evaluation", {
        variant: "error",
      });
    },
  });

  const handleStopEval = (evalId) => {
    if (!evalId || !isUUID(evalId)) return;
    stopSingleEval(evalId);
  };

  const handleAddEvaluation = (evalConfig) => {
    const rawMapping = evalConfig.mapping || {};
    const translatedMapping = {};
    for (const [variable, colName] of Object.entries(rawMapping)) {
      const col = updatedEvalColumns.find(
        (c) =>
          c.headerName === colName || c.field === colName || c.name === colName,
      );
      translatedMapping[variable] = col?.field || colName;
    }

    const builtEval = {
      template_id: evalConfig.template_id,
      templateId: evalConfig.template_id,
      evalTemplateName: evalConfig.name,
      model: evalConfig.model,
      mapping: translatedMapping,
      config: buildExperimentEvalRuntimePayload(evalConfig, translatedMapping),
      template_type: evalConfig.template_type,
      templateType: evalConfig.template_type,
      pinned_version_id: evalConfig.version_id,
      ...(evalConfig.template_type === "composite" &&
      evalConfig.composite_weight_overrides
        ? {
            composite_weight_overrides: evalConfig.composite_weight_overrides,
          }
        : {}),
    };

    if (editingEval) {
      builtEval.name = evalConfig.name;
      builtEval.actualEvalCreatedId = editingEval.user_eval_id;
      builtEval.id = editingEval.user_eval_id;
      setEvals((prev) => {
        const updated = prev.map((e) => {
          const eid = e.actualEvalCreatedId || e.evalId || e.id;
          return eid === editingEval.user_eval_id ? { ...e, ...builtEval } : e;
        });
        updateExperiment(updated);
        return updated;
      });
    } else {
      const versionedName = getVersionedEvalName(
        evalConfig.name,
        evals,
        evalConfig.template_id,
      );
      builtEval.name = versionedName;
      setEvals((prev) => {
        const updated = [...prev, builtEval];
        updateExperiment(updated);
        return updated;
      });
    }
    setEditingEval(null);
    setOpenEvaluationDialog(false);
  };

  const handleRemoveEval = (evalId) => {
    setEvals((prev) => {
      const updated = prev.filter(
        (e) =>
          e.actualEvalCreatedId !== evalId &&
          e.evalId !== evalId &&
          e.id !== evalId,
      );
      updateExperiment(updated);
      return updated;
    });
  };

  const handleEditEval = (evalItem) => {
    const tplId =
      evalItem.template_id ||
      evalItem.templateId ||
      evalItem.eval_template_id ||
      evalItem.evalTemplateId;
    setEditingEval({
      id: tplId || evalItem.id,
      template_id: tplId || evalItem.id,
      user_eval_id: evalItem.actualEvalCreatedId || evalItem.id,
      name: evalItem.name || evalItem.evalTemplateName,
      template_type: evalItem.template_type || evalItem.templateType,
      mapping: evalItem.config?.mapping || evalItem.mapping,
      model: evalItem.model || evalItem.selected_model,
      run_config: evalItem.config,
      pinned_version_id: evalItem.pinned_version_id,
      composite_weight_overrides:
        evalItem.composite_weight_overrides ||
        evalItem.compositeWeightOverrides,
    });
    setOpenEvaluationDialog(true);
  };

  const experimentVirtualColumns = [
    { field: "output", headerName: "Output", dataType: "text" },
    { field: "prompt_chain", headerName: "Prompt Chain", dataType: "text" },
  ];
  const updatedEvalColumns = [
    ...experimentVirtualColumns,
    ...(allColumns || []),
  ];
  return (
    <>
      <Drawer
        anchor="right"
        open={open}
        onClose={onClose}
        PaperProps={{
          sx: (t) => ({
            height: "100vh",
            position: "fixed",
            zIndex: 10,
            boxShadow: t.customShadows.drawer,
            borderRadius: "0px !important",
            backgroundColor: "background.paper",
          }),
        }}
        ModalProps={{
          BackdropProps: {
            style: {
              backgroundColor: "transparent",
              borderRadius: "0px !important",
            },
          },
        }}
      >
        <Box
          sx={{
            p: 2,
            height: "100%",
            display: "flex",
            flexDirection: "column",
            width: "31vw",
            minWidth: "400px",
            backgroundColor: theme.palette.background.paper,
          }}
        >
          {/* Header */}
          <Box
            display="flex"
            justifyContent="space-between"
            alignItems="center"
            mb={2}
          >
            <Typography fontSize={16} fontWeight={600}>
              Add Evaluations
            </Typography>
            <IconButton onClick={onClose} sx={{ p: 0, color: "text.primary" }}>
              <Icon icon="mingcute:close-line" />
            </IconButton>
          </Box>
          {isUpdating && <LinearProgress sx={{ mb: 1, borderRadius: 1 }} />}

          {/* Content */}
          <Box sx={{ flex: 1, overflowY: "auto", mb: 2 }}>
            {isExperimentDetailLoading ? (
              <Box
                display="flex"
                justifyContent="center"
                alignItems="center"
                height={200}
              >
                <Typography color="text.secondary">
                  Loading evaluations...
                </Typography>
              </Box>
            ) : evals.length === 0 ? (
              <Box
                border="1px solid"
                borderColor={theme.palette.divider}
                borderRadius={1}
                p={4}
                flexGrow={1}
                display="flex"
                flexDirection="column"
                justifyContent="center"
                alignItems="center"
                textAlign="center"
                height="100%"
              >
                <Typography fontSize={16} fontWeight="bold" mb={1}>
                  No evaluations added
                </Typography>
                <Typography
                  fontSize={12}
                  color={theme.palette.text.disabled}
                  mb={2}
                >
                  Add evaluations to run on your experiment
                </Typography>
                <Button
                  variant="contained"
                  color="primary"
                  size="small"
                  onClick={() => {
                    setOpenEvaluationDialog(true);
                  }}
                  startIcon={
                    <SvgColor
                      src="/assets/icons/action_buttons/ic_add.svg"
                      sx={{
                        width: 16,
                        height: 16,
                        color: "inherit",
                      }}
                    />
                  }
                  sx={{ px: 3, fontWeight: 500 }}
                >
                  Add Evaluations
                </Button>
              </Box>
            ) : (
              <Box
                sx={{
                  border: "1px solid",
                  borderColor: "divider",
                  padding: 2,
                  height: "100%",
                  borderRadius: 0.5,
                }}
              >
                <Box>
                  <Typography>
                    <Box
                      display="flex"
                      justifyContent="space-between"
                      alignItems="center"
                      mb={2}
                    >
                      <Typography fontSize={12} fontWeight={500}>
                        All Evals ({evals.length})
                      </Typography>
                      <Button
                        variant="outlined"
                        color="primary"
                        onClick={() => {
                          setOpenEvaluationDialog(true);
                        }}
                        startIcon={
                          <SvgColor
                            src="/assets/icons/action_buttons/ic_add.svg"
                            sx={{
                              width: 16,
                              height: 16,
                              color: "primary.main",
                            }}
                          />
                        }
                        size="small"
                        sx={{
                          color: "primary.main",
                          borderColor: "primary.main",
                          fontSize: "12px",
                          fontWeight: 500,
                          "& .MuiButton-startIcon": {
                            margin: 0,
                            paddingRight: theme.spacing(1),
                          },
                          whiteSpace: "nowrap",
                        }}
                      >
                        Add Evaluations
                      </Button>
                    </Box>
                  </Typography>
                </Box>
                <Stack spacing={1.5}>
                  {evals.map((evalItem) => {
                    const evalId =
                      evalItem?.evalId ||
                      evalItem?.actualEvalCreatedId ||
                      evalItem?.id;
                    const requiredKeys =
                      evalItem?.requiredKeys ||
                      evalItem?.evalRequiredKeys ||
                      evalItem?.templateDetails?.config?.requiredKeys ||
                      [];

                    return (
                      <Paper
                        key={evalId}
                        sx={{
                          p: 2,
                          border: "1px solid",
                          borderColor: "divider",
                          borderRadius: 1,
                          backgroundColor: "background.paper",
                        }}
                      >
                        <Box
                          sx={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "flex-start",
                          }}
                        >
                          <Box sx={{ flex: 1 }}>
                            <Box
                              sx={{
                                display: "flex",
                                alignItems: "center",
                                gap: 1,
                              }}
                            >
                              <Typography variant="subtitle2">
                                {evalItem.name}
                              </Typography>
                              {(evalItem.templateType === "composite" ||
                                evalItem.template_type === "composite") && (
                                <Chip
                                  label="Composite"
                                  size="small"
                                  sx={{
                                    height: "20px",
                                    fontSize: "10px",
                                    fontWeight: 600,
                                    borderRadius: "2px",
                                    bgcolor: "info.lighter",
                                    color: "info.dark",
                                    "& .MuiChip-label": { px: 0.75 },
                                  }}
                                />
                              )}
                            </Box>
                            <Box
                              sx={{
                                mt: 1,
                                display: "flex",
                                gap: 1,
                                flexWrap: "wrap",
                              }}
                            >
                              <ShowComponent condition={!!evalItem.evalGroup}>
                                <Chip
                                  label={`Group name - ${evalItem.evalGroup}.`}
                                  size="small"
                                  sx={{
                                    height: "24px",
                                    backgroundColor: "background.neutral",
                                    borderColor: "divider",
                                    fontSize: "11px",
                                    borderRadius: "2px",
                                    paddingX: "12px",
                                    lineHeight: "16px",
                                    fontWeight: 400,
                                    color: "text.primary",
                                    "& .MuiChip-label": { padding: 0 },
                                    ".MuiChip-icon ": { marginRight: "6px" },
                                    "&:hover": {
                                      backgroundColor: "background.neutral",
                                      borderColor: "divider",
                                    },
                                  }}
                                  icon={
                                    <SvgColor
                                      src="/assets/icons/ic_dashed_square.svg"
                                      sx={{ width: 16, height: 16, mr: 1 }}
                                      style={{
                                        color: theme.palette.text.primary,
                                      }}
                                    />
                                  }
                                />
                              </ShowComponent>
                              {evalItem.model && (
                                <Chip
                                  key={`model-${evalId}`}
                                  label={evalItem.model}
                                  size="small"
                                  sx={{
                                    height: "24px",
                                    backgroundColor: "background.neutral",
                                    borderColor: "divider",
                                    fontSize: "11px",
                                    borderRadius: "2px",
                                    paddingX: "12px",
                                    lineHeight: "16px",
                                    fontWeight: 400,
                                    color: "text.primary",
                                    "& .MuiChip-label": { padding: 0 },
                                    "&:hover": {
                                      backgroundColor: "background.neutral",
                                      borderColor: "divider",
                                    },
                                  }}
                                />
                              )}
                              <Chip
                                key={`required-${evalId}`}
                                label={`Required Columns - ${requiredKeys.join(", ")}`}
                                size="small"
                                icon={
                                  <SvgColor
                                    src="/assets/icons/custom/eval_columns.svg"
                                    sx={{ width: 16, height: 16 }}
                                    style={{
                                      color: theme.palette.text.primary,
                                    }}
                                  />
                                }
                                sx={{
                                  height: "24px",
                                  backgroundColor: "background.neutral",
                                  borderColor: "divider",
                                  fontSize: "11px",
                                  borderRadius: "2px",
                                  paddingX: "12px",
                                  paddingY: "4px",
                                  lineHeight: "16px",
                                  fontWeight: 400,
                                  color: "text.primary",
                                  "&:hover": {
                                    backgroundColor: "background.neutral",
                                    borderColor: "divider",
                                  },
                                }}
                              />
                            </Box>
                          </Box>

                          <IconButton
                            size="small"
                            onClick={() => handleEditEval(evalItem)}
                            sx={{
                              ml: 1,
                              border: "1px solid",
                              borderColor: "divider",
                              borderRadius: "2px",
                              color: "text.disabled",
                            }}
                          >
                            <SvgColor
                              src="/assets/icons/ic_edit.svg"
                              sx={{ width: 16, height: 16 }}
                            />
                          </IconButton>
                          <IconButton
                            size="small"
                            title="Stop evaluation"
                            onClick={() =>
                              handleStopEval(
                                evalItem.actualEvalCreatedId || evalItem.id,
                              )
                            }
                            sx={{
                              ml: 1,
                              border: "1px solid",
                              borderColor: "divider",
                              borderRadius: "2px",
                              color: "error.main",
                            }}
                          >
                            <SvgColor
                              src="/assets/icons/ic_stop.svg"
                              sx={{ width: 16, height: 16 }}
                            />
                          </IconButton>
                          <IconButton
                            size="small"
                            onClick={() => handleRemoveEval(evalId)}
                            disabled={evals.length <= 1}
                            title={
                              evals.length <= 1
                                ? "An experiment must have at least one evaluation"
                                : undefined
                            }
                            sx={{
                              ml: 1,
                              border: "1px solid",
                              borderColor: "divider",
                              borderRadius: "2px",
                              color: "text.disabled",
                            }}
                          >
                            <SvgColor
                              src="/assets/icons/components/ic_delete.svg"
                              sx={{ width: 16, height: 16 }}
                            />
                          </IconButton>
                        </Box>
                      </Paper>
                    );
                  })}
                </Stack>
              </Box>
            )}
          </Box>

          {/* Footer */}
          <Box
            display="flex"
            justifyContent="flex-end"
            alignItems="center"
            gap={1}
            pt={2}
            borderTop="1px solid"
            borderColor={theme.palette.divider}
          >
            <Button
              variant="outlined"
              color="primary"
              size="small"
              onClick={onClose}
              sx={{
                color: "primary.main",
                borderColor: "primary.main",
                fontSize: "12px",
                fontWeight: 500,
                whiteSpace: "nowrap",
              }}
            >
              Cancel
            </Button>
            <Button
              variant="contained"
              color="primary"
              onClick={() => setOpenConfirmRun(true)}
              size="small"
              sx={{ width: "150px", fontWeight: 500 }}
              disabled={evals.length === 0 || isUpdating}
            >
              Run Evaluations
            </Button>
          </Box>
        </Box>
      </Drawer>

      <ConfirmRunEvaluations
        open={openConfirmRun}
        onClose={() => setOpenConfirmRun(false)}
        onConfirm={(evalsToRun) => {
          const sourceIds = evalsToRun.map((e) => e.id);
          runEvaluations({ data: sourceIds });
        }}
        selectedUserEvalList={evals}
        loading={isRunningEvaluations}
      />

      {/* Evaluation Picker */}
      <EvalPickerDrawer
        open={openEvaluationDialog}
        onClose={() => {
          setOpenEvaluationDialog(false);
          setEditingEval(null);
        }}
        source="experiment"
        sourceId={datasetId}
        sourceColumns={updatedEvalColumns}
        extraColumns={experimentVirtualColumns}
        existingEvals={evals}
        onEvalAdded={handleAddEvaluation}
        initialEval={editingEval}
      />
    </>
  );
};

ManageExperimentEvalsDrawer.propTypes = {
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  allColumns: PropTypes.array,
  refreshGrid: PropTypes.func,
  experimentData: PropTypes.object,
  isExperimentDetailLoading: PropTypes.bool,
};

export default ManageExperimentEvalsDrawer;
