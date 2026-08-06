import { useState, useMemo } from "react";
import { Box, Typography, CircularProgress, Button } from "@mui/material";
import { useInfiniteQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { useParams } from "react-router-dom";
import AttributeGroupList from "./AttributeGroupList";
import AttributeKeyList from "./AttributeKeyList";
import AttributeDetail from "./AttributeDetail";

const AttributesView = () => {
  const { id: projectId } = useParams();
  const [selectedGroup, setSelectedGroup] = useState(null);
  const [selectedKey, setSelectedKey] = useState(null);

  const { data, isLoading, hasNextPage, fetchNextPage, isFetchingNextPage } =
    useInfiniteQuery({
      queryKey: ["span-attribute-keys", projectId],
      queryFn: ({ signal, pageParam }) =>
        axios.get(endpoints.project.spanAttributeKeys(), {
          signal,
          params: {
            project_id: projectId,
            page_size: 25,
            ...(pageParam ? { cursor: pageParam } : {}),
          },
        }),
      initialPageParam: null,
      getNextPageParam: (lastPage) =>
        lastPage?.data?.has_more && lastPage?.data?.next_cursor
          ? lastPage.data.next_cursor
          : undefined,
      enabled: Boolean(projectId),
      retry: false,
      meta: { errorHandled: true },
    });
  const seenAttributeKeys = new Set();
  const attributeKeys = (data?.pages || []).flatMap((page) =>
    (page?.data?.result || []).filter(({ key }) => {
      if (!key || seenAttributeKeys.has(key)) return false;
      seenAttributeKeys.add(key);
      return true;
    }),
  );

  // Group attributes by dot-delimited prefix
  const groups = useMemo(() => {
    const grouped = {};
    attributeKeys.forEach(({ key, type, count }) => {
      const parts = key.split(".");
      const prefix = parts.length > 1 ? parts.slice(0, -1).join(".") : key;
      if (!grouped[prefix]) grouped[prefix] = { keys: [], totalCount: 0 };
      grouped[prefix].keys.push({ key, type, count });
      grouped[prefix].totalCount += count;
    });
    return Object.entries(grouped)
      .map(([prefix, data]) => ({ prefix, ...data }))
      .sort((a, b) => b.totalCount - a.totalCount);
  }, [attributeKeys]);

  const filteredKeys = useMemo(() => {
    if (!selectedGroup) return attributeKeys;
    return groups.find((g) => g.prefix === selectedGroup)?.keys || [];
  }, [selectedGroup, groups, attributeKeys]);

  if (isLoading) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "calc(100vh - 180px)",
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (attributeKeys.length === 0 && hasNextPage) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "calc(100vh - 180px)",
          flexDirection: "column",
          gap: 1,
        }}
      >
        {isFetchingNextPage ? (
          <CircularProgress size={24} />
        ) : (
          <Button variant="outlined" onClick={() => fetchNextPage()}>
            Continue loading attributes
          </Button>
        )}
        <Typography variant="body2" color="text.secondary">
          Searching older traces for attributes…
        </Typography>
      </Box>
    );
  }

  if (attributeKeys.length === 0) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "calc(100vh - 180px)",
          flexDirection: "column",
          gap: 1,
        }}
      >
        <Typography variant="h6" color="text.secondary">
          No Span Attributes Found
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Span attributes will appear here once trace data is ingested.
        </Typography>
      </Box>
    );
  }

  return (
    <Box
      sx={{
        display: "flex",
        height: "calc(100vh - 180px)",
        overflow: "hidden",
      }}
    >
      <AttributeGroupList
        groups={groups}
        selectedGroup={selectedGroup}
        onSelectGroup={setSelectedGroup}
      />
      <AttributeKeyList
        keys={filteredKeys}
        selectedKey={selectedKey}
        onSelectKey={setSelectedKey}
        hasMore={hasNextPage}
        isLoadingMore={isFetchingNextPage}
        onLoadMore={fetchNextPage}
      />
      <AttributeDetail projectId={projectId} attributeKey={selectedKey} />
    </Box>
  );
};

export default AttributesView;
