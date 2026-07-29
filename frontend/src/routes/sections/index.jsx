import React, { Suspense, useMemo, useEffect } from "react";
import lazyWithRetry from "src/utils/lazyWithRetry";
import { Navigate, useRoutes, useLocation, useNavigate } from "react-router-dom";

import { mainRoutes } from "./main";
import { authRoutes } from "./auth";
import { dashboardRoutes } from "./dashboard";
import { useAuthContext } from "src/auth/hooks";
import { AuthGuard } from "src/auth/guard";
import { SplashScreen } from "src/components/loading-screen";
import { useWorkspace } from "src/contexts/WorkspaceContext";
import {
  useDeploymentMode,
  usePostLoginPath,
} from "src/hooks/useDeploymentMode";
import SOSLoginPage from "src/pages/SOSLoginPage";
import { paths } from "src/routes/paths";
import { isAccountCreated } from "src/sections/oss-setup/ossFlowState";

const OAuthConsent = lazyWithRetry(() => import("src/pages/mcp/OAuthConsent"));
const SharedView = lazyWithRetry(() => import("src/pages/shared/SharedView"));
const OssSetupView = lazyWithRetry(
  () => import("src/sections/oss-setup/OssSetupView"),
);

// ----------------------------------------------------------------------

export default function Router() {
  const { user } = useAuthContext();
  const { currentWorkspaceRole } = useWorkspace();
  const { isOSS, isLoading: isDeploymentModeLoading } = useDeploymentMode();
  const postLoginPath = usePostLoginPath();

  const dashboardRoutesArray = useMemo(
    () => dashboardRoutes(user, currentWorkspaceRole, { isOSS }),
    [user, currentWorkspaceRole, isOSS],
  );

  // OSS: show the launch-mode screen once per browser session. A returning user
  // who reopens the app (browser restores a deep dashboard URL) is routed
  // through /setup first; after launch mode they land back in the product.
  const navigate = useNavigate();
  const location = useLocation();
  useEffect(() => {
    if (!isOSS || !user) return;
    if (sessionStorage.getItem("oss_launch_seen") === "1") return;
    if (location.pathname.startsWith("/dashboard")) {
      navigate("/setup", { replace: true });
    }
  }, [isOSS, user, location.pathname, navigate]);

  // OSS entry routing:
  //   • authenticated              → /setup (launch mode → skips validation → product)
  //   • first-time (no account)    → /setup (launch mode → validation → sign up)
  //   • returning, logged out      → login
  let rootTarget = postLoginPath;
  if (isOSS) {
    if (user) rootTarget = "/setup";
    else rootTarget = isAccountCreated() ? paths.auth.jwt.login : "/setup";
  }

  const element = useRoutes([
    {
      path: "/",
      element: <Navigate to={rootTarget} replace />,
    },
    {
      path: "/sos",
      element: <SOSLoginPage />,
    },

    // OSS self-host setup flow (pre-login, no dashboard layout)
    {
      path: "/setup",
      element: (
        <Suspense fallback={<SplashScreen />}>
          <OssSetupView />
        </Suspense>
      ),
    },

    // MCP OAuth consent (standalone, no dashboard layout, requires auth)
    {
      path: "/mcp/authorize",
      element: (
        <AuthGuard>
          <Suspense fallback={<SplashScreen />}>
            <OAuthConsent />
          </Suspense>
        </AuthGuard>
      ),
    },

    // Auth routes
    ...authRoutes,

    // Dashboard routes
    ...dashboardRoutesArray,

    // Shared resource viewer (public — no dashboard layout, no auth guard)
    {
      path: "/shared/:token",
      element: (
        <Suspense fallback={<SplashScreen />}>
          <SharedView />
        </Suspense>
      ),
    },

    // Main routes
    ...mainRoutes,

    // No match 404
    { path: "*", element: <Navigate to="/404" replace /> },
  ]);

  // Wait for deployment-mode resolution before rendering the route tree.
  // Otherwise the first render uses the hook's default `isOSS=true`, which
  // omits non-OSS routes (billing/pricing/etc.). Stripe Checkout redirects
  // back to /dashboard/settings/pricing?upgrade=success&session_id=... — if
  // that route isn't registered yet, the catch-all sends users to /404 and
  // the session_id is lost before PricingPage can confirm the upgrade.
  if (isDeploymentModeLoading) {
    return <SplashScreen />;
  }

  return element;
}
