import { describe, it, expect } from "vitest";
import { alkBaseUrl } from "../client";

describe("alkBaseUrl", () => {
  it("falls back to the local harness when nothing is configured", () => {
    expect(alkBaseUrl({})).toBe("http://localhost:8777");
  });

  it("uses VITE_ALK_API_URL when it is set", () => {
    expect(alkBaseUrl({ VITE_ALK_API_URL: "http://harness.internal:8777" })).toBe(
      "http://harness.internal:8777"
    );
  });

  it("strips a trailing slash so paths do not double up", () => {
    expect(alkBaseUrl({ VITE_ALK_API_URL: "http://localhost:8777/" })).toBe(
      "http://localhost:8777"
    );
  });

  it("ignores a blank value rather than producing a relative base", () => {
    expect(alkBaseUrl({ VITE_ALK_API_URL: "   " })).toBe("http://localhost:8777");
  });
});
