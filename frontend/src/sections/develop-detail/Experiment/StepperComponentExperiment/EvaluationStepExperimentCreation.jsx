import {
  Box,
  Button,
  Chip,
  IconButton,
  Paper,
  Stack,
  Typography,
  useTheme,
} from "@mui/material";
import React, { useEffect, useRef, useState } from "react";
import StepsHeaderComponent from "./StepsHeaderComponent";
import PropTypes from "prop-types";
import SvgColor from "src/components/svg-color";
import { FormSearchSelectFieldControl } from "src/components/FromSearchSelectField";
import { useFieldArray, useWatch } from "react-hook-form";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "react-router";
import { useSearchParams } from "react-router-dom";
import axios, { endpoints } from "src/utils/axios";
import { enqueueSnackbar } from "src/components/snackbar";
import { EvalPickerDrawer } from "src/sections/common/EvalPicker";
import { buildExperimentEvalRuntimePayload } from "src/sections/common/EvalPicker/evalPickerConfigUtils";
import { getVersionedEvalName } from "src/components/run-tests/common";
import { ShowComponent } from "src/components/show";
import { isUUID } from "src/utils/utils";
import { transformUserEvalMetricsForApi } from "../schema";

const EvaluationStepExperimentCreation = ({
  control,
  allColumns,
  errors,
  isEditingExperiment = false,
  experimentId = null,
}) => {
  const queryClient = useQueryClient();
  const selectedColumn = useWatch({ control, name: "columnId" });
  const { dataset: datasetParam } = useParams();
  const [searchParam] = useSearchParams();
  const datasetId = datasetParam || searchParam.get("datasetId") || "";
  const theme = useTheme();
  const userChangedColumnRef = useRef(false);
  const experimentVirtualColumns = [
    { field: "output", headerName: "Output", dataType: "text" },
    { field: "prompt_chain", headerName: "Prompt Chain", dataType: "text" },
  ];
  const updatedEvalColumns = [
    ...experimentVirtualColumns,
    ...(allColumns || []),
  ];

  const [openEvaluationDialog, setOpenEvaluationDialog] = useState(false);
  const [editingEval, setEditingEval] = useState(null);

  const { data: userEvalList } = useQuery({
    queryFn: () =>
      axios.get(endpoints.develop.optimizeDevelop.columnInfo, {
        params: { column_id: selectedColumn },
      }),
    queryKey: ["optimize-develop-column-info", "eval-step", selectedColumn],
    enabled:
      Boolean(selectedColumn?.length) &&
      (!isEditingExperiment || userChangedColumnRef.current),
    select: (data) => data?.data?.result,
  });
  const {
    fields: evalFields,
    replace: replaceEvals,
    append,
    remove,
    update,
  } = useFieldArray({ control, name: "userEvalMetrics" });

  const canSaveInline = isEditingExperiment && Boolean(experimentId);
  const { mutate: saveEvalsInline } = useMutation({
    mutationFn: (nextEvals) =>
      axios.put(endpoints.develop.experiment.update(experimentId), {
        user_eval_metrics: transformUserEvalMetricsForApi(nextEvals, true),
      }),
    onSuccess: async (resp, nextEvals) => {
      const fresh = resp?.data?.result?.user_eval_metrics;
      if (Array.isArray(fresh)) {
        const prevById = new Map(
          (nextEvals || []).map((e) => [
            String(e.actualEvalCreatedId || e.id || ""),
            e,
          ]),
        );
        replaceEvals(
          fresh.map((item) => {
            const prev =
              prevById.get(String(item.id)) ||
              (nextEvals || []).find(
                (e) =>
                  String(e.template_id || e.templateId) ===
                    String(item.template_id) && e.name === item.name,
              );
            return {
              ...prev,
              ...item,
              evalId: item.id,
              actualEvalCreatedId: item.id,
              template_id: item.template_id,
              templateId: item.template_id,
              template_type: prev?.template_type || prev?.templateType,
              templateType: prev?.template_type || prev?.templateType,
              requiredKeys: prev?.requiredKeys,
              templateDetails: prev?.templateDetails || item.template_details,
              composite_weight_overrides:
                prev?.composite_weight_overrides ??
                item.composite_weight_overrides,
              mapping: prev?.mapping || item.config?.mapping,
              pinned_version_id: item.pinned_version_id,
            };
          }),
        );
      }
      await queryClient.refetchQueries({
        queryKey: ["experiment", experimentId],
      });
    },
    onError: (error) => {
      enqueueSnackbar(error?.message || "Failed to save evaluation", {
        variant: "error",
      });
    },
  });
  useEffect(() => {
    if (
      userEvalList &&
      (!isEditingExperiment || userChangedColumnRef.current)
    ) {
      const manualEvals = evalFields.filter((f) => !f?.showInSidebar);
      const apiEvals = userEvalList.map((item) => ({
        ...item,
        evalId: item.id,
        config: item.config || {
          ...item.params,
          mapping: item.mapping || {},
        },
      }));
      replaceEvals([...apiEvals, ...manualEvals]);
      userChangedColumnRef.current = false;
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userEvalList, replaceEvals]);
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

    const fullConfig = buildExperimentEvalRuntimePayload(
      evalConfig,
      translatedMapping,
    );

    const evalEntry = {
      evalId: evalConfig.template_id,
      evalTemplateName: evalConfig.name,
      template_id: evalConfig.template_id,
      templateId: evalConfig.template_id,
      mapping: translatedMapping,
      model: evalConfig.model,
      config: fullConfig,
      templateDetails: evalConfig.eval_template,
      template_type: evalConfig.template_type,
      templateType: evalConfig.template_type,
      requiredKeys:
        evalConfig.eval_template?.required_keys ||
        evalConfig.eval_template?.requiredKeys ||
        evalConfig.config?.required_keys ||
        evalConfig.config?.requiredKeys ||
        [],
      pinned_version_id: evalConfig.version_id,
      ...(evalConfig.template_type === "composite" &&
      evalConfig.composite_weight_overrides
        ? {
            composite_weight_overrides: evalConfig.composite_weight_overrides,
          }
        : {}),
    };

    let nextEvals;
    if (editingEval) {
      const idx = evalFields.findIndex((f) => {
        const fid = f.actualEvalCreatedId || f.evalId || f.id;
        return fid === editingEval.user_eval_id;
      });
      if (idx !== -1) {
        const merged = {
          ...evalFields[idx],
          ...evalEntry,
          name: evalConfig.name,
        };
        update(idx, merged);
        nextEvals = evalFields.map((f, i) => (i === idx ? merged : f));
      } else {
        nextEvals = evalFields;
      }
    } else {
      const versionedName = getVersionedEvalName(
        evalConfig.name,
        evalFields,
        evalConfig.template_id,
      );
      const created = { ...evalEntry, name: versionedName };
      append(created);
      nextEvals = [...evalFields, created];
    }
    if (canSaveInline) saveEvalsInline(nextEvals);
    setEditingEval(null);
    setOpenEvaluationDialog(false);
  };

  const handleRemoveEval = (evalId) => {
    const idx = evalFields.findIndex((f) => (f.evalId || f.id) === evalId);
    if (idx !== -1) remove(idx);
  };

  const handleEditEval = (evalItem) => {
    const tplId =
      evalItem.template_id ||
      evalItem.templateId ||
      evalItem.eval_template_id ||
      evalItem.evalTemplateId ||
      evalItem.evalId ||
      evalItem.id;
    setEditingEval({
      id: tplId,
      template_id: tplId,
      user_eval_id:
        evalItem.actualEvalCreatedId || evalItem.evalId || evalItem.id,
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
  const hasError = errors?.userEvalMetrics?.message;
  return (
    <Stack spacing={3}>
      <StepsHeaderComponent
        title={"Configure Evaluations"}
        subtitle={
          "Select a column from your dataset to compare model outputs against"
        }
      />

      {/* Baseline column selector */}
      <Box
        sx={{
          border: "1px solid",
          borderColor: "blue.o20",
          backgroundColor: "blue.o5",
          padding: 2,
          borderRadius: 0.5,
          display: "flex",
          flexDirection: "row",
          alignItems: "flex-start",
          gap: 2,
        }}
      >
        <Box
          sx={{
            width: 36,
            height: 36,
            borderRadius: 0.5,
            backgroundColor: "blue.o10",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          <SvgColor
            sx={{ width: 24, height: 24, color: "blue.600" }}
            src="/assets/icons/ic_two_arrows_reverse.svg"
          />
        </Box>
        <Stack spacing={2} flex={1}>
          <Box>
            <Typography typography={"s1_2"} fontWeight={"fontWeightMedium"}>
              Compare against baseline (optional)
            </Typography>
            <Typography typography={"s2_1"} fontWeight={"fontWeightRegular"}>
              Select a column from your dataset to compare model outputs against
            </Typography>
          </Box>
          <FormSearchSelectFieldControl
            required
            fullWidth
            placeholder="Select column"
            control={control}
            fieldName="columnId"
            size="small"
            onChange={() => {
              userChangedColumnRef.current = true;
            }}
            options={(allColumns || []).map((column) => ({
              value: column.field,
              label: column.headerName,
            }))}
            MenuProps={{ sx: { maxHeight: "400px" } }}
            sx={{
              "& .MuiFormHelperText-root": { margin: 0 },
              width: "100%",
              backgroundColor: "background.default",
            }}
          />
        </Stack>
      </Box>

      {/* Evaluations section */}
      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          border: "1px solid",
          borderColor: hasError ? "error.main" : "divider",
          backgroundColor: "background.neutral",
          padding: 2,
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 2,
          }}
        >
          <Stack>
            <Typography typography="m3" fontWeight={"fontWeightMedium"}>
              Add Evaluations
            </Typography>
            <Typography
              typography={"s2_1"}
              fontWeight={"fontWeightRegular"}
              color="text.secondary"
            >
              Select and configure evals to run
            </Typography>
          </Stack>
          <Button
            variant="outlined"
            color="primary"
            size="small"
            onClick={() => {
              setEditingEval(null);
              setOpenEvaluationDialog(true);
            }}
            startIcon={
              <SvgColor
                src="/assets/icons/action_buttons/ic_add.svg"
                sx={{ width: 16, height: 16 }}
              />
            }
            sx={{
              px: theme.spacing(1.5),
              mt: -0.8,
              fontWeight: 500,
              height: 34,
              bgcolor: "background.paper",
            }}
          >
            Add Evaluations
          </Button>
        </Box>
        <ShowComponent condition={hasError}>
          <Typography typography={"s2"} color="error.main">
            {errors?.userEvalMetrics?.message}
          </Typography>
        </ShowComponent>
        <ShowComponent condition={Boolean(evalFields.length)}>
          <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
            {evalFields.map((evalItem) => {
              const itemAny = evalItem;
              const evalId = itemAny.evalId || itemAny.id;
              const mapping = itemAny.config?.mapping || itemAny.mapping || {};
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
                      <Typography variant="subtitle2">
                        {itemAny.name}
                      </Typography>
                      {itemAny.description && (
                        <Typography
                          variant="body2"
                          color="text.secondary"
                          sx={{ mt: 0.5 }}
                        >
                          {itemAny.description}
                        </Typography>
                      )}
                      <Box
                        sx={{
                          mt: 1,
                          display: "flex",
                          gap: 1,
                          flexWrap: "wrap",
                        }}
                      >
                        <ShowComponent
                          condition={!!itemAny.evalGroup && !!itemAny.groupName}
                        >
                          <Chip
                            label={`Group name - ${itemAny.groupName}.`}
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
                                style={{ color: theme.palette.text.primary }}
                              />
                            }
                          />
                        </ShowComponent>
                        {Object.entries(mapping).map(([key, value]) => {
                          let label = value;
                          if (isUUID(String(value))) {
                            const match = (allColumns || []).find(
                              (col) => col.field === value,
                            );
                            if (match) label = match.headerName;
                          }
                          return (
                            <Chip
                              key={key}
                              label={`${key}: ${label}`}
                              size="small"
                              variant="outlined"
                            />
                          );
                        })}
                      </Box>
                    </Box>

                    <IconButton
                      size="small"
                      onClick={() => handleEditEval(itemAny)}
                      sx={{
                        ml: 1,
                        border: "1px solid",
                        borderColor: "divider",
                        borderRadius: "2px",
                        color: "text.action",
                      }}
                    >
                      <SvgColor
                        src="/assets/icons/ic_edit.svg"
                        sx={{ width: 16, height: 16 }}
                      />
                    </IconButton>
                    <IconButton
                      size="small"
                      onClick={() => handleRemoveEval(evalId)}
                      sx={{
                        ml: 1,
                        border: "1px solid",
                        borderColor: "divider",
                        borderRadius: "2px",
                        color: "text.action",
                      }}
                    >
                      <SvgColor
                        src="/assets/icons/ic_delete.svg"
                        sx={{ height: 16, width: 16 }}
                      />
                    </IconButton>
                  </Box>
                </Paper>
              );
            })}
          </Box>
        </ShowComponent>
      </Box>

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
        existingEvals={evalFields}
        onEvalAdded={handleAddEvaluation}
        initialEval={editingEval}
      />
    </Stack>
  );
};

export default EvaluationStepExperimentCreation;

EvaluationStepExperimentCreation.propTypes = {
  control: PropTypes.object,
  allColumns: PropTypes.array,
  errors: PropTypes.object,
  isEditingExperiment: PropTypes.bool,
  experimentId: PropTypes.string,
};
