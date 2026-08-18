import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// These are ES modules, so __dirname does not exist.
const here = dirname(fileURLToPath(import.meta.url));

/**
 * The base URL already carries the prefix — /simulate/harness on the platform, or
 * http://localhost:8777/api locally — so a path that also starts with /api resolves to
 * /api/api/… and 404s. Two template literals kept theirs through a bulk rewrite that only
 * matched quoted strings, and nothing caught it because every call site is mocked.
 */
const read = (name) =>
  readFileSync(join(here, "..", name), "utf8")
    .split("\n")
    .filter((line) => !line.trimStart().startsWith("*") && !line.trimStart().startsWith("//"));

describe("harness request paths", () => {
  it.each(["alEnvironment.js", "useAlkConversation.js", "streamHarness.js"])(
    "%s never prefixes a path with /api",
    (name) => {
      const offending = read(name).filter((line) => /["'`]\/api\//.test(line));
      expect(offending).toEqual([]);
    }
  );
});
