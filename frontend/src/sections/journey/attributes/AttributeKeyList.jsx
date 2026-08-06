import { useState } from "react";
import PropTypes from "prop-types";
import {
  Box,
  List,
  ListItemButton,
  ListItemText,
  TextField,
  InputAdornment,
  Chip,
  Button,
} from "@mui/material";
import Iconify from "src/components/iconify";

const TYPE_COLORS = {
  string: "info",
  number: "warning",
  boolean: "success",
};

const AttributeKeyList = ({
  keys,
  selectedKey,
  onSelectKey,
  hasMore,
  isLoadingMore,
  onLoadMore,
}) => {
  const [search, setSearch] = useState("");

  const filtered = keys.filter((k) =>
    k.key.toLowerCase().includes(search.toLowerCase()),
  );

  return (
    <Box
      sx={{
        width: 300,
        borderRight: 1,
        borderColor: "divider",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <Box sx={{ p: 1.5, borderBottom: 1, borderColor: "divider" }}>
        <TextField
          size="small"
          fullWidth
          placeholder="Search attributes..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <Iconify
                  icon="eva:search-fill"
                  width={16}
                  sx={{ color: "text.disabled" }}
                />
              </InputAdornment>
            ),
          }}
        />
      </Box>
      <List
        sx={{ overflow: "auto", flex: 1, p: 1 }}
        dense
        onScroll={(event) => {
          const list = event.currentTarget;
          if (
            hasMore &&
            !isLoadingMore &&
            list.scrollTop + list.clientHeight >= list.scrollHeight - 32
          ) {
            onLoadMore?.();
          }
        }}
      >
        {filtered.map(({ key, type, count }) => (
          <ListItemButton
            key={key}
            selected={selectedKey === key}
            onClick={() => onSelectKey(key)}
            sx={{ borderRadius: 1, py: 0.75 }}
          >
            <ListItemText
              primary={key}
              secondary={count.toLocaleString() + " spans"}
              primaryTypographyProps={{
                variant: "body2",
                fontWeight: selectedKey === key ? 600 : 400,
                noWrap: true,
              }}
              secondaryTypographyProps={{ variant: "caption" }}
            />
            <Chip
              label={type}
              size="small"
              color={TYPE_COLORS[type] || "default"}
              variant="outlined"
              sx={{ ml: 1, height: 20, fontSize: "0.65rem" }}
            />
          </ListItemButton>
        ))}
        {filtered.length === 0 && !hasMore && (
          <Box sx={{ p: 2, textAlign: "center", color: "text.secondary" }}>
            No attributes found
          </Box>
        )}
        {isLoadingMore && (
          <Box sx={{ p: 1, textAlign: "center", color: "text.secondary" }}>
            Loading more…
          </Box>
        )}
        {hasMore && !isLoadingMore && (
          <Box sx={{ p: 1, textAlign: "center" }}>
            <Button size="small" onClick={() => onLoadMore?.()}>
              Load more attributes
            </Button>
          </Box>
        )}
      </List>
    </Box>
  );
};

AttributeKeyList.propTypes = {
  keys: PropTypes.arrayOf(
    PropTypes.shape({
      key: PropTypes.string,
      type: PropTypes.string,
      count: PropTypes.number,
    }),
  ).isRequired,
  selectedKey: PropTypes.string,
  onSelectKey: PropTypes.func.isRequired,
  hasMore: PropTypes.bool,
  isLoadingMore: PropTypes.bool,
  onLoadMore: PropTypes.func,
};

export default AttributeKeyList;
