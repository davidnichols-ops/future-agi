/**
 * Deployment mode hook — detects oss / ee / cloud from backend.
 *
 * Uses React Query cache (staleTime: Infinity) — fetches once, shared globally.
 * No Context/Provider needed.
 *
 * Usage:
 *   const { isOSS, isCloud, isEE } = useDeploymentMode();
 */

import { useQuery } from "@tanstack/react-query";
import axios, { endpoints } from "src/utils/axios";
import { paths } from "src/routes/paths";

export function useDeploymentMode() {
  // Dev/prototype override — force a mode without a backend that reports it.
  // e.g. VITE_DEPLOYMENT_MODE_OVERRIDE=oss to preview the self-host experience.
  const override = import.meta.env.VITE_DEPLOYMENT_MODE_OVERRIDE;

  const { data, isLoading } = useQuery({
    queryKey: ["deployment-info"],
    queryFn: () => axios.get(endpoints.settings.v2.deploymentInfo),
    select: (res) => res.data?.result?.mode || "oss",
    staleTime: Infinity,
    retry: 1,
    enabled: !override,
  });

  const mode = override || data || "oss";

  return {
    mode,
    isCloud: mode === "cloud",
    isOSS: mode === "oss",
    isEE: mode === "ee",
    isLoading,
  };
}

export function usePostLoginPath() {
  const { isOSS } = useDeploymentMode();

  const returnTo = localStorage.getItem("redirectUrl");
  if (returnTo) return returnTo;
  // OSS always lands inside the product on the Get Started page.
  return isOSS ? paths.dashboard.getstarted : paths.dashboard.falconAI;
}
