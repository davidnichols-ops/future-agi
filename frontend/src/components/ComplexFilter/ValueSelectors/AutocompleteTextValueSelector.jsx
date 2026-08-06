import { useState } from "react";
import PropTypes from "prop-types";
import { Autocomplete, TextField, CircularProgress } from "@mui/material";
import { useInfiniteQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { useDebounce } from "src/hooks/use-debounce";
import { useParams } from "react-router-dom";

const LOAD_MORE_OPTION = Object.freeze({ __loadMore: true });

const normalizeAttributeType = (type) => {
  if (type === "text") return "string";
  if (["float", "integer"].includes(type)) return "number";
  return type;
};

const AutocompleteTextValueSelector = ({
  definition,
  filter,
  updateFilter,
}) => {
  const [inputValue, setInputValue] = useState(
    filter?.filter_config?.filter_value || "",
  );
  const debouncedInput = useDebounce(inputValue, 300);
  const { id: projectId } = useParams();

  const { data, isLoading, hasNextPage, fetchNextPage, isFetchingNextPage } =
    useInfiniteQuery({
      queryKey: [
        "span-attribute-values",
        projectId,
        definition?.propertyId,
        debouncedInput,
      ],
      queryFn: ({ signal, pageParam }) =>
        axios.get(endpoints.dashboard.filterValues, {
          signal,
          params: {
            project_ids: projectId,
            metric_name: definition?.propertyId,
            metric_type: "custom_attribute",
            source: "traces",
            search: debouncedInput,
            page_size: 10,
            ...(definition?.type
              ? { attribute_type: normalizeAttributeType(definition.type) }
              : {}),
            ...(pageParam ? { cursor: pageParam } : {}),
          },
        }),
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
      const value = item?.value ?? item;
      const key = `${typeof value}:${JSON.stringify(value)}`;
      if (seen.has(key)) return [];
      seen.add(key);
      return [value];
    }),
  );
  const pickerOptions = hasNextPage ? [...options, LOAD_MORE_OPTION] : options;

  return (
    <Autocomplete
      freeSolo
      size="small"
      options={pickerOptions}
      filterOptions={(availableOptions) => availableOptions}
      getOptionLabel={(option) => {
        if (option === LOAD_MORE_OPTION) return "Load more values";
        if (typeof option === "string") return option;
        return JSON.stringify(option);
      }}
      renderOption={(props, option) =>
        option === LOAD_MORE_OPTION ? (
          <li
            {...props}
            onMouseDown={(event) => event.preventDefault()}
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              if (!isFetchingNextPage) fetchNextPage();
            }}
          >
            {isFetchingNextPage ? "Loading more values…" : "Load more values"}
          </li>
        ) : (
          <li {...props}>
            {typeof option === "string" ? option : JSON.stringify(option)}
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
        if (reason === "reset" && newInputValue === "Load more values") return;
        setInputValue(newInputValue);
      }}
      value={filter?.filter_config?.filter_value || ""}
      onChange={(_, newValue) => {
        if (newValue === LOAD_MORE_OPTION) {
          if (!isFetchingNextPage) fetchNextPage();
          return;
        }
        updateFilter({
          ...filter,
          filter_config: {
            ...filter?.filter_config,
            filter_value: newValue || "",
          },
        });
      }}
      onBlur={() => {
        if (inputValue !== filter?.filter_config?.filter_value) {
          updateFilter({
            ...filter,
            filter_config: {
              ...filter?.filter_config,
              filter_value: inputValue,
            },
          });
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
                {isLoading || isFetchingNextPage ? (
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
  }),
  filter: PropTypes.shape({
    filter_config: PropTypes.shape({
      filter_value: PropTypes.string,
    }),
  }),
  updateFilter: PropTypes.func.isRequired,
};

export default AutocompleteTextValueSelector;
