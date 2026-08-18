import { describe, it, expect } from "vitest";
import { alkBaseUrl, isDirectToHarness, applyAuth, ALK_DEFAULT_BASE } from "../client";

describe("alkBaseUrl", () => {
  it("defaults to the backend proxy, which is how the platform reaches the harness", () => {
    expect(alkBaseUrl({})).toBe("/simulate/harness");
    expect(ALK_DEFAULT_BASE).toBe("/simulate/harness");
  });

  it("can be pointed straight at a local harness for development", () => {
    expect(alkBaseUrl({ VITE_ALK_API_BASE: "http://localhost:8777/api" })).toBe(
      "http://localhost:8777/api"
    );
  });

  it("strips a trailing slash so paths do not double up", () => {
    expect(alkBaseUrl({ VITE_ALK_API_BASE: "http://localhost:8777/api/" })).toBe(
      "http://localhost:8777/api"
    );
  });

  it("ignores a blank value rather than producing an empty base", () => {
    expect(alkBaseUrl({ VITE_ALK_API_BASE: "   " })).toBe("/simulate/harness");
  });
});

describe("isDirectToHarness", () => {
  it("recognises an absolute base as a direct connection", () => {
    expect(isDirectToHarness("http://localhost:8777/api")).toBe(true);
    expect(isDirectToHarness("https://harness.internal/api")).toBe(true);
  });

  it("treats the proxy path as going through our backend", () => {
    expect(isDirectToHarness("/simulate/harness")).toBe(false);
  });
});

describe("applyAuth", () => {
  const shared = {
    Authorization: "Bearer token",
    "X-Organization-Id": "org-1",
    "X-Workspace-Id": "ws-1",
  };

  it("authenticates proxied calls the same way as any other /simulate/ call", () => {
    const config = { headers: {} };
    applyAuth(config, "/simulate/harness", shared);
    expect(config.headers).toEqual(shared);
  });

  it("sends nothing to a harness reached directly, which has no auth", () => {
    const config = { headers: {} };
    applyAuth(config, "http://localhost:8777/api", shared);
    expect(config.headers).toEqual({});
  });

  it("omits headers the app has not set yet", () => {
    const config = { headers: {} };
    applyAuth(config, "/simulate/harness", { Authorization: "Bearer token" });
    expect(config.headers).toEqual({ Authorization: "Bearer token" });
  });
});
