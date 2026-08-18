import axios from "axios";

export const ALK_DEFAULT_BASE_URL = "http://localhost:8777";

/**
 * The harness runs as a separate service, so its base URL is configuration rather
 * than our API host. Kept as a pure function so it can be tested without a build.
 */
export const alkBaseUrl = (env = {}) => {
  const configured = (env.VITE_ALK_API_URL || "").trim();
  return (configured || ALK_DEFAULT_BASE_URL).replace(/\/+$/, "");
};

/**
 * Deliberately NOT src/utils/axios.js: that instance carries our auth interceptors and
 * points at our own API. The harness is a third party — it must not receive our token,
 * and its errors must not reach our 401 handling.
 */
const alkAxios = axios.create({
  baseURL: alkBaseUrl(import.meta.env),
  headers: { "Content-Type": "application/json" },
});

export default alkAxios;
