/* eslint-disable react-refresh/only-export-components */

import React from "react";
import { Suspense } from "react";
import lazyWithRetry from "src/utils/lazyWithRetry";
import { Outlet } from "react-router-dom";

import { AuthGuard, GuestGuard } from "src/auth/guard";
import AuthClassicLayout from "src/layouts/auth/classic";

import { SplashScreen } from "src/components/loading-screen";

// ----------------------------------------------------------------------

// JWT
const JwtLoginPage = lazyWithRetry(() => import("src/pages/auth/jwt/login"));
const JwtRegisterPage = lazyWithRetry(
  () => import("src/pages/auth/jwt/register"),
);
const ForgetPassword = lazyWithRetry(
  () => import("src/pages/auth/jwt/forget-password"),
);
const ResetPassword = lazyWithRetry(
  () => import("src/pages/auth/jwt/reset-password"),
);
const SSOLogin = lazyWithRetry(() => import("src/sections/auth/jwt/sso-login"));
const SetupOrg = lazyWithRetry(() => import("src/sections/auth/jwt/setup-org"));
const OrgRemoved = lazyWithRetry(
  () => import("src/sections/auth/jwt/org-removed"),
);
const TwoFactorPage = lazyWithRetry(
  () => import("src/pages/auth/jwt/two-factor"),
);
const InviteAccepted = lazyWithRetry(
  () => import("src/sections/auth/jwt/invite-accepted"),
);

// ----------------------------------------------------------------------

const authJwt = {
  path: "jwt",
  element: (
    <GuestGuard>
      <Suspense fallback={<SplashScreen />}>
        <Outlet />
      </Suspense>
    </GuestGuard>
  ),
  children: [
    {
      path: "invitation/accept/:uuid/:token",
      element: (
        <AuthClassicLayout>
          <JwtLoginPage />
        </AuthClassicLayout>
      ),
    },
    {
      path: "invitation/set-password/:uuid/:token",
      element: <InviteAccepted />,
    },
    {
      path: "login",
      element: (
        <AuthClassicLayout>
          <JwtLoginPage />
        </AuthClassicLayout>
      ),
    },
    {
      path: "forget-password",
      element: (
        <AuthClassicLayout>
          <ForgetPassword />
        </AuthClassicLayout>
      ),
    },
    {
      path: "verify/:uuid/:token",
      element: (
        <AuthClassicLayout>
          <ResetPassword />
        </AuthClassicLayout>
      ),
    },
    {
      path: "register",
      element: <JwtRegisterPage />,
    },
    {
      path: "sso-sml",
      element: (
        <AuthClassicLayout>
          <SSOLogin />
        </AuthClassicLayout>
      ),
    },
    {
      path: "setup-org",
      element: (
        // SetupOrg brings its own full-screen space layout.
        <AuthGuard>
          <SetupOrg />
        </AuthGuard>
      ),
    },
    {
      path: "org-removed",
      element: (
        <AuthGuard>
          <OrgRemoved />
        </AuthGuard>
      ),
    },
    {
      path: "two-factor",
      element: (
        <AuthClassicLayout>
          <TwoFactorPage />
        </AuthClassicLayout>
      ),
    },
  ],
};

export const authRoutes = [
  {
    path: "auth",
    children: [authJwt],
  },
];
