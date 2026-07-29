// Persistent flags driving the OSS onboarding sequence (prototype).
//   validation-done  → validation checks run once on first setup, skipped after
//   account-created  → an account exists, so logged-out users see login (not signup)

const VALIDATION_DONE = "oss_validation_done";
const ACCOUNT_CREATED = "oss_account_created";

export const isValidationDone = () =>
  localStorage.getItem(VALIDATION_DONE) === "true";
export const markValidationDone = () =>
  localStorage.setItem(VALIDATION_DONE, "true");

export const isAccountCreated = () =>
  localStorage.getItem(ACCOUNT_CREATED) === "true";
export const markAccountCreated = () =>
  localStorage.setItem(ACCOUNT_CREATED, "true");
