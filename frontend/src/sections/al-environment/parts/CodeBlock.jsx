import { useState } from "react";
import PropTypes from "prop-types";
import { Box } from "@mui/material";
import { ALK_MONO } from "../alkTokens";

/**
 * Source the operator will want out of the page — handler code, scenario files, a check's
 * body. The copy button stays hidden until the block is hovered or focused, so a page full
 * of these is not a page full of buttons.
 */
const CodeBlock = ({ children, wrap }) => {
  const [said, setSaid] = useState("copy");

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(String(children ?? ""));
      setSaid("copied");
    } catch {
      setSaid("select + ⌘C");
    }
    setTimeout(() => setSaid("copy"), 1400);
  };

  return (
    <Box
      sx={{
        position: "relative",
        "&:hover .alk-copy, &:focus-within .alk-copy": { opacity: 1 },
      }}
    >
      <Box
        component="pre"
        sx={{
          m: 0,
          px: 1,
          py: 0.9,
          pr: 5,
          bgcolor: "background.default",
          border: "1px solid",
          borderColor: "divider",
          borderRadius: 1,
          fontFamily: ALK_MONO,
          fontSize: 11.6,
          lineHeight: 1.55,
          color: "text.primary",
          overflowX: wrap ? "visible" : "auto",
          whiteSpace: wrap ? "pre-wrap" : "pre",
          overflowWrap: wrap ? "anywhere" : "normal",
        }}
      >
        {children}
      </Box>
      <Box
        className="alk-copy"
        component="button"
        type="button"
        onClick={copy}
        sx={{
          position: "absolute",
          top: 6,
          right: 6,
          px: 0.75,
          py: 0.2,
          opacity: 0,
          transition: "opacity 120ms",
          border: "1px solid",
          borderColor: "divider",
          borderRadius: "4px",
          background: (theme) => theme.palette.background.paper,
          color: "text.secondary",
          fontFamily: ALK_MONO,
          fontSize: 10.5,
          cursor: "pointer",
          "&:focus-visible": { opacity: 1 },
        }}
      >
        {said}
      </Box>
    </Box>
  );
};

CodeBlock.propTypes = { children: PropTypes.node, wrap: PropTypes.bool };

export default CodeBlock;
