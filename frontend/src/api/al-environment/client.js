import axios from "axios";
import appAxios from "src/utils/axios";

/**
 * On the platform the harness is reached through our own backend, which authenticates the
 * call and proxies it verbatim: /simulate/harness/<path> → harness /api/<path>. For local
 * development you can talk to the harness directly instead.
 *
 * One config value covers both, because the base replaces the prefix rather than sitting in
 * front of it — everything after it is byte-identical:
 *   platform  VITE_ALK_API_BASE unset  → /simulate/harness
 *   local     VITE_ALK_API_BASE=http://localhost:8777/api
 */
export const ALK_DEFAULT_BASE = "/simulate/harness";

export const alkBaseUrl = (env = {}) => {
  const configured = (env.VITE_ALK_API_BASE || "").trim();
  return (configured || ALK_DEFAULT_BASE).replace(/\/+$/, "");
};

/** An absolute base means we are talking to the harness directly, which needs no auth. */
export const isDirectToHarness = (base) => /^https?:\/\//i.test(base);

/** The headers the app maintains for every authenticated call. */
export const AUTH_HEADERS = ["Authorization", "X-Organization-Id", "X-Workspace-Id"];

/**
 * Deliberately a separate instance from src/utils/axios: that one asserts every request and
 * response against the generated OpenAPI contract, and the proxied harness routes are not in
 * it. It also redirects to login on 401, which must not be triggered by a harness error.
 *
 * Auth is borrowed from it instead of reimplemented, so a token refresh or an organisation
 * switch applies here too without this file knowing how any of that works.
 */
const alkAxios = axios.create({
  baseURL: alkBaseUrl(import.meta.env),
  headers: { "Content-Type": "application/json" },
});

export const applyAuth = (config, base, shared) => {
  if (isDirectToHarness(base)) return config;
  AUTH_HEADERS.forEach((header) => {
    const value = shared?.[header];
    if (value) config.headers[header] = value;
  });
  return config;
};

alkAxios.interceptors.request.use((config) =>
  applyAuth(config, config.baseURL || "", appAxios.defaults.headers.common)
);

export default alkAxios;
