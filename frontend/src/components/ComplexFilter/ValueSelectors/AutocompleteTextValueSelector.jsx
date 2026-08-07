import { useRef, useState } from "react";
import PropTypes from "prop-types";
import { Autocomplete, TextField, CircularProgress } from "@mui/material";
import { useInfiniteQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { useDebounce } from "src/hooks/use-debounce";
import { useParams } from "react-router-dom";
import {
  FILTER_TYPE_ALLOWED_OPS,
  LIST_FILTER_OPS,
} from "src/api/contracts/filter-contract.generated";
import { followEmptyListContinuations } from "src/sections/projects/LLMTracing/listCursorPagination";

const LOAD_MORE_OPTION = Object.freeze({ __loadMore: true });
const RETRY_OPTION = Object.freeze({ __retry: true });
const LIST_OPERATORS = new Set(LIST_FILTER_OPS);

const isPaginationOption = (option) =>
  option === LOAD_MORE_OPTION || option === RETRY_OPTION;

const optionValue = (option) =>
  option && typeof option === "object" && "value" in option
    ? option.value
    : option;

const optionStorageType = (option) => {
  if (option && typeof option === "object" && option.type) return option.type;
  const value = optionValue(option);
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "string";
};

const storageTypeToFilterType = (type) => {
  if (type === "number") return "number";
  if (type === "boolean") return "boolean";
  return "text";
};

const normalizeAttributeType = (type) => {
  if (type === "text") return "string";
  if (["float", "integer"].includes(type)) return "number";
  return type;
};

const AutocompleteTextValueSelector = ({
  definition,
  filter,
  updateFilter,
  projectId: projectIdProp,
}) => {
  const initialValue = filter?.filter_config?.filter_value;
  const [inputValue, setInputValue] = useState(
    typeof initialValue === "string" ? initialValue : "",
  );
  // MUI mirrors the selected option label into inputValue. That reset is not a
  // free-text edit: committing it again on blur would turn 42/false back into
  // the strings "42"/"false" and silently change ClickHouse storage family.
  const freeTextDirtyRef = useRef(false);
  const debouncedInput = useDebounce(inputValue, 300);
  const { observeId, id } = useParams();
  const projectId = projectIdProp || observeId || id;
  const definitionFilterType = definition?.filterType?.type || definition?.type;
  const attributeType =
    definitionFilterType &&
    definition?.attributeTypesExact === true &&
    Array.isArray(definition?.attributeTypes) &&
    definition.attributeTypes.length === 1
      ? normalizeAttributeType(definitionFilterType)
      : undefined;

  const {
    data,
    isLoading,
    isFetching,
    isError,
    hasNextPage,
    fetchNextPage,
    isFetchingNextPage,
    isFetchNextPageError,
    refetch,
  } = useInfiniteQuery({
    queryKey: [
      "span-attribute-values",
      projectId,
      definition?.propertyId,
      attributeType || "all-types",
      debouncedInput,
    ],
    queryFn: async ({ signal, pageParam }) => {
      const requestPage = (cursor) =>
        axios.get(endpoints.dashboard.filterValues, {
          signal,
          params: {
            project_ids: projectId,
            metric_name: definition?.propertyId,
            metric_type: "custom_attribute",
            source: "traces",
            search: debouncedInput,
            page_size: 10,
            ...(attributeType ? { attribute_type: attributeType } : {}),
            ...(cursor ? { cursor } : {}),
          },
        });
      const initialResponse = await requestPage(pageParam);
      // The shared guard follows at most 12 empty checkpoints per UI action.
      // Its 30-second elapsed check is deliberately soft and runs between
      // completed requests; cancellation of an in-flight request remains the
      // responsibility of the query's AbortSignal/HTTP client.
      return followEmptyListContinuations({
        initialResponse,
        rowsFromResponse: (response) => response?.data?.result?.values || [],
        metadataFromResponse: (response) => response?.data?.result || {},
        nextResponse: requestPage,
        isCurrent: () => !signal.aborted,
      });
    },
    initialPageParam: null,
    getNextPageParam: (lastPage) =>
      lastPage?.data?.result?.has_more && lastPage?.data?.result?.next_cursor
        ? lastPage.data.result.next_cursor
        : undefined,
    enabled: Boolean(projectId) && Boolean(definition?.propertyId),
    staleTime: 30000,
    retry: false,
    meta: { errorHandled: true },
  });
  const seen = new Set();
  const options = (data?.pages || []).flatMap((page) =>
    (page?.data?.result?.values || []).flatMap((item) => {
      const value = optionValue(item);
      const type = optionStorageType(item);
      const key = `${type}:${JSON.stringify(value)}`;
      if (seen.has(key)) return [];
      seen.add(key);
      return [{ value, type }];
    }),
  );
  const pickerOptions = hasNextPage
    ? [...options, LOAD_MORE_OPTION]
    : isError
      ? [...options, RETRY_OPTION]
      : options;
  const filterConfig = filter?.filter_config || {};
  const isListOperator = LIST_OPERATORS.has(filterConfig.filter_op);
  const selectedRawValues = isListOperator
    ? Array.isArray(filterConfig.filter_value)
      ? filterConfig.filter_value
      : filterConfig.filter_value == null || filterConfig.filter_value === ""
        ? []
        : [filterConfig.filter_value]
    : [filterConfig.filter_value].filter(
        (value) => value !== undefined && value !== null && value !== "",
      );
  const selectedTypes = Array.isArray(filterConfig.attribute_value_types)
    ? filterConfig.attribute_value_types
    : [];
  const selectedOptions = selectedRawValues.map((value, index) => {
    const selectedType = selectedTypes[index];
    return (
      options.find(
        (option) =>
          Object.is(option.value, value) &&
          (!selectedType || option.type === selectedType),
      ) || { value, type: selectedType || optionStorageType(value) }
    );
  });

  const updateSelectedValues = (selection) => {
    const selected = (
      Array.isArray(selection) ? selection : [selection]
    ).filter((option) => option != null && !isPaginationOption(option));
    const values = selected.map(optionValue);
    const types = selected.map(optionStorageType);

    updateFilter(filter.id, (existingFilter) => {
      const existingConfig = existingFilter.filter_config || {};
      if (isListOperator) {
        return {
          ...existingFilter,
          filter_config: {
            ...existingConfig,
            // Typed provenance is only valid for in/not_in. Keep the wire type
            // text so a mixed scalar list is accepted, while the aligned type
            // array selects the exact ClickHouse storage family per value.
            filter_type: "text",
            filter_value: values,
            attribute_value_types: types,
          },
        };
      }

      const value = values[0] ?? "";
      const nextFilterType = storageTypeToFilterType(types[0]);
      const currentOp = existingConfig.filter_op || "equals";
      const validOps = FILTER_TYPE_ALLOWED_OPS[nextFilterType] || [];
      const nextConfig = { ...existingConfig };
      delete nextConfig.attribute_value_types;
      return {
        ...existingFilter,
        filter_config: {
          ...nextConfig,
          filter_type: nextFilterType,
          filter_op: validOps.includes(currentOp) ? currentOp : "equals",
          filter_value: value,
        },
      };
    });
  };

  return (
    <Autocomplete
      freeSolo
      multiple={isListOperator}
      size="small"
      options={pickerOptions}
      filterOptions={(availableOptions) => availableOptions}
      getOptionLabel={(option) => {
        if (option === LOAD_MORE_OPTION) {
          return isFetchNextPageError
            ? "Retry loading values"
            : "Load more values";
        }
        if (option === RETRY_OPTION) return "Retry loading values";
        const value = optionValue(option);
        return typeof value === "string" ? value : JSON.stringify(value);
      }}
      isOptionEqualToValue={(option, value) =>
        Object.is(optionValue(option), optionValue(value)) &&
        optionStorageType(option) === optionStorageType(value)
      }
      renderOption={(props, option) =>
        isPaginationOption(option) ? (
          <li
            {...props}
            onMouseDown={(event) => event.preventDefault()}
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              if (option === RETRY_OPTION) {
                if (!isFetching) refetch();
              } else if (!isFetchingNextPage) {
                fetchNextPage();
              }
            }}
          >
            {option === RETRY_OPTION
              ? isFetching
                ? "Retrying values…"
                : "Retry loading values"
              : isFetchingNextPage
                ? "Loading more values…"
                : isFetchNextPageError
                  ? "Retry loading values"
                  : "Load more values"}
          </li>
        ) : (
          <li {...props}>
            {typeof optionValue(option) === "string"
              ? optionValue(option)
              : JSON.stringify(optionValue(option))}
          </li>
        )
      }
      loading={isLoading}
      ListboxProps={{
        onScroll: (event) => {
          const list = event.currentTarget;
          if (
            hasNextPage &&
            !isFetchingNextPage &&
            list.scrollTop + list.clientHeight >= list.scrollHeight - 24
          ) {
            fetchNextPage();
          }
        },
      }}
      inputValue={inputValue}
      onInputChange={(_, newInputValue, reason) => {
        if (
          reason === "reset" &&
          ["Load more values", "Retry loading values"].includes(newInputValue)
        ) {
          return;
        }
        freeTextDirtyRef.current = reason === "input";
        setInputValue(newInputValue);
      }}
      value={isListOperator ? selectedOptions : selectedOptions[0] || null}
      onChange={(_, newValue) => {
        freeTextDirtyRef.current = false;
        if (newValue === RETRY_OPTION) {
          if (!isFetching) refetch();
          return;
        }
        if (newValue === LOAD_MORE_OPTION) {
          if (!isFetchingNextPage) fetchNextPage();
          return;
        }
        if (
          Array.isArray(newValue) &&
          newValue.some((option) => isPaginationOption(option))
        ) {
          if (newValue.includes(RETRY_OPTION)) {
            if (!isFetching) refetch();
          } else if (!isFetchingNextPage) {
            fetchNextPage();
          }
          return;
        }
        updateSelectedValues(newValue);
      }}
      onBlur={() => {
        if (!isListOperator && freeTextDirtyRef.current) {
          freeTextDirtyRef.current = false;
          updateSelectedValues({ value: inputValue, type: "string" });
        }
      }}
      renderInput={(params) => (
        <TextField
          {...params}
          placeholder="Type or select a value..."
          variant="outlined"
          size="small"
          sx={{ minWidth: 180 }}
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <>
                {isLoading || isFetching ? (
                  <CircularProgress color="inherit" size={16} />
                ) : null}
                {params.InputProps.endAdornment}
              </>
            ),
          }}
        />
      )}
      sx={{ minWidth: 200 }}
    />
  );
};

AutocompleteTextValueSelector.propTypes = {
  definition: PropTypes.shape({
    propertyId: PropTypes.string,
    type: PropTypes.string,
    filterType: PropTypes.shape({ type: PropTypes.string }),
    attributeTypes: PropTypes.arrayOf(PropTypes.string),
    attributeTypesExact: PropTypes.bool,
  }),
  filter: PropTypes.shape({
    filter_config: PropTypes.shape({
      filter_value: PropTypes.oneOfType([
        PropTypes.string,
        PropTypes.number,
        PropTypes.bool,
        PropTypes.array,
      ]),
      filter_op: PropTypes.string,
      filter_type: PropTypes.string,
      attribute_value_types: PropTypes.arrayOf(PropTypes.string),
    }),
    id: PropTypes.string.isRequired,
  }),
  updateFilter: PropTypes.func.isRequired,
  projectId: PropTypes.string,
};

export default AutocompleteTextValueSelector;
