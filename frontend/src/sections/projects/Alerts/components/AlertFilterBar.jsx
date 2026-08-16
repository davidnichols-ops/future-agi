import { Box, Button, Typography } from "@mui/material";
import PropTypes from "prop-types";
import React, { useCallback, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useWatch } from "react-hook-form";

import Iconify from "src/components/iconify";
import TraceFilterPanel from "src/sections/projects/LLMTracing/TraceFilterPanel";
import axios, { endpoints } from "src/utils/axios";
import {
  CATEGORIES,
  SPAN_TYPE_PROPERTY,
  toPanelRows,
  toFormRows,
  toPanelType,
} from "./alertFilterRows";

const OP_DISPLAY = {
  equals: "is",
  not_equals: "is not",
  in: "is",
  not_in: "is not",
  contains: "contains",
  not_contains: "does not contain",
  greater_than: ">",
  greater_than_or_equal: "≥",
  less_than: "<",
  less_than_or_equal: "≤",
  is_null: "is null",
  is_not_null: "is not null",
};

const FilterChip = ({ filter, labelFor, onRemove }) => (
  <Box
    sx={{
      display: "inline-flex",
      alignItems: "center",
      gap: 0.5,
      px: 0.75,
      py: 0.25,
      border: "1px solid",
      borderColor: "divider",
      borderRadius: "6px",
      minHeight: 30,
    }}
  >
    <Typography sx={{ fontSize: 12, color: "text.secondary" }}>
      {filter.fieldName || filter.field}
    </Typography>
    <Typography sx={{ fontSize: 11, color: "text.disabled" }}>
      {OP_DISPLAY[filter.operator] || filter.operator}
    </Typography>
    <Typography
      sx={{
        fontSize: 12,
        fontWeight: 600,
        maxWidth: 200,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}
    >
      {(Array.isArray(filter.value) ? filter.value : [filter.value])
        .filter((v) => v !== undefined && v !== "")
        .map(labelFor)
        .join(", ")}
    </Typography>
    <Box
      component="button"
      type="button"
      onClick={onRemove}
      aria-label={`Remove ${filter.fieldName || filter.field} filter`}
      sx={{
        display: "inline-flex",
        border: 0,
        p: 0,
        bgcolor: "transparent",
        color: "text.disabled",
        cursor: "pointer",
        "&:hover": { color: "text.primary" },
      }}
    >
      <Iconify icon="mdi:close" width={12} />
    </Box>
  </Box>
);

FilterChip.propTypes = {
  filter: PropTypes.object.isRequired,
  labelFor: PropTypes.func.isRequired,
  onRemove: PropTypes.func.isRequired,
};

export default function AlertFilterBar({ control, setValue, projectId }) {
  const anchorRef = useRef();
  const [open, setOpen] = useState(false);

  const formFilters = useWatch({ control, name: "filters" });

  // Typed attribute inventory. Passing `properties` to the panel also stops it
  // fetching its own, which is workspace-scoped and returns nothing for
  // projects without a workspace.
  const { data: attributes = [] } = useQuery({
    queryKey: ["eval-attributes-typed", projectId],
    queryFn: () =>
      axios.get(endpoints.project.getEvalAttributeList(), {
        params: {
          filters: JSON.stringify({ project_id: projectId }),
          include_types: true,
        },
      }),
    enabled: !!projectId,
    select: (data) => data.data?.result ?? [],
  });

  const properties = useMemo(
    () => [
      SPAN_TYPE_PROPERTY,
      ...attributes.map((attr) => ({
        id: attr.key,
        name: attr.key,
        category: "attribute",
        rawCategory: "custom_attribute",
        type: toPanelType(attr.type),
        apiColType: "SPAN_ATTRIBUTE",
      })),
    ],
    [attributes],
  );

  const panelFilters = useMemo(
    () => toPanelRows(formFilters || []),
    [formFilters],
  );

  const handleApply = useCallback(
    (next) => {
      setValue("filters", toFormRows(next || []), {
        shouldDirty: true,
        shouldValidate: true,
      });
    },
    [setValue],
  );

  const handleRemove = useCallback(
    (index) => {
      handleApply(panelFilters.filter((_, i) => i !== index));
    },
    [handleApply, panelFilters],
  );

  // Attribute keys are their own label; span types have display names.
  const labelFor = useCallback(
    (value) => SPAN_TYPE_PROPERTY.choiceLabels[value] ?? String(value),
    [],
  );

  return (
    <Box
      ref={anchorRef}
      sx={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 1 }}
    >
      {panelFilters.map((filter, index) => (
        <FilterChip
          key={`${filter.field}-${index}`}
          filter={filter}
          labelFor={labelFor}
          onRemove={() => handleRemove(index)}
        />
      ))}

      <Button
        startIcon={<Iconify color="text.primary" icon="material-symbols:add" />}
        onClick={() => setOpen(true)}
        variant="text"
        color="primary"
        size="small"
        sx={{
          fontSize: "12px",
          color: "text.disabled",
          border: "1px solid",
          borderColor: "divider",
          borderRadius: "8px",
          height: "30px",
          px: 1.5,
        }}
      >
        {panelFilters.length > 0 ? "Edit Filters" : "Add Filter"}
      </Button>

      <TraceFilterPanel
        anchorEl={anchorRef?.current}
        open={open}
        onClose={() => setOpen(false)}
        currentFilters={panelFilters}
        onApply={handleApply}
        properties={properties}
        categories={CATEGORIES}
        projectId={projectId}
        showAi={false}
        showQueryTab={false}
      />
    </Box>
  );
}

AlertFilterBar.propTypes = {
  control: PropTypes.object.isRequired,
  setValue: PropTypes.func.isRequired,
  projectId: PropTypes.string,
};
