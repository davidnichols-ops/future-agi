import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { paths } from "src/routes/paths";
import { useAuthContext } from "src/auth/hooks";
import { usePostLoginPath } from "src/hooks/useDeploymentMode";
import OssSetupShell from "./OssSetupShell";
import LaunchModeStep from "./LaunchModeStep";
import ValidationStep from "./ValidationStep";
import HorizontalSpaceship from "./HorizontalSpaceship";
import { isValidationDone, markValidationDone } from "./ossFlowState";

// OSS setup flow orchestrator (UI prototype).
//   step 0 — pick launch mode (shown every time the app is opened)
//   step 1 — validation checks (first-time setup only)
//
// After launch mode:
//   • first time (validation not done)  → validation → sign up
//   • returning + authenticated          → straight into the product
//   • returning + not authenticated      → login
export default function OssSetupView() {
  const navigate = useNavigate();
  const { authenticated } = useAuthContext();
  const postLoginPath = usePostLoginPath();
  const [step, setStep] = useState(0);
  const [mode, setMode] = useState("live");
  const [validationProgress, setValidationProgress] = useState(0);

  const handleLaunchContinue = () => {
    // Launch mode is shown once per browser session — mark it so returning
    // users aren't bounced back here on every in-app navigation.
    sessionStorage.setItem("oss_launch_seen", "1");
    if (!isValidationDone()) {
      setStep(1);
      return;
    }
    // Setup already done on a previous run — skip validation.
    navigate(authenticated ? postLoginPath : paths.auth.jwt.login);
  };

  const handleValidationContinue = () => {
    markValidationDone();
    // First-time local setup → create the admin account.
    navigate(paths.auth.jwt.register);
  };

  return (
    <OssSetupShell
      step={step}
      totalSteps={2}
      illustration={
        step === 1 ? (
          <HorizontalSpaceship progress={validationProgress} height={46} />
        ) : undefined
      }
    >
      {step === 0 && (
        <LaunchModeStep
          value={mode}
          onChange={setMode}
          onContinue={handleLaunchContinue}
        />
      )}

      {step === 1 && (
        <ValidationStep
          mode={mode}
          onBack={() => setStep(0)}
          onContinue={handleValidationContinue}
          onProgress={setValidationProgress}
        />
      )}
    </OssSetupShell>
  );
}
