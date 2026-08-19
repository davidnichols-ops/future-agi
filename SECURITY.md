# Security Scan — `future-agi`

> Best-effort static scan by `repo-archaeologist`. This is **not** a substitute for a real SAST/DAST tool (bandit, semgrep, gitleaks, dependabot). It is a first-pass 'is there anything obviously scary here?' check.

## Summary

| Severity | Count |
|---|---:|
| 🔴 CRITICAL | 23 |
| 🟠 HIGH | 216 |
| 🟡 MEDIUM | 14 |
| 🔵 LOW | 4 |

**Total findings:** 257

## Findings

### 🔴 CRITICAL (23)

| File | Line | Category | Message |
|---|---:|---|---|
| `agentcc-gateway/internal/guardrails/pii/pii_test.go` | 125 | secret | AWS access key id detected |
| `agentcc-gateway/internal/guardrails/secrets/secrets_test.go` | 28 | secret | AWS access key id detected |
| `agentcc-gateway/internal/guardrails/secrets/secrets_test.go` | 85 | secret | Private key block detected |
| `agentcc-gateway/internal/guardrails/secrets/secrets_test.go` | 188 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 132 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 141 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 142 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 154 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 180 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 344 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 393 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 404 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 435 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 465 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 492 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 522 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 548 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 566 | secret | AWS access key id detected |
| `agentcc-gateway/internal/providers/bedrock/bedrock_test.go` | 18 | secret | AWS access key id detected |
| `futureagi/model_hub/tests/test_custom_models_api.py` | 526 | secret | AWS access key id detected |
| `futureagi/model_hub/tests/test_custom_models_api.py` | 527 | secret | Likely AWS secret access key |
| `futureagi/model_hub/tests/test_custom_models_api.py` | 658 | secret | AWS access key id detected |
| `futureagi/model_hub/tests/test_custom_models_api.py` | 659 | secret | Likely AWS secret access key |

### 🟠 HIGH (216)

| File | Line | Category | Message |
|---|---:|---|---|
| `agentcc-gateway/config.example.yaml` | 390 | secret | Possible secret assigned to `token` |
| `agentcc-gateway/config.license_auth.example.yaml` | 28 | secret | Possible secret assigned to `token_type` |
| `agentcc-gateway/internal/config/config.go` | 915 | secret | Possible secret assigned to `TokenType` |
| `agentcc-gateway/internal/guardrails/secrets/secrets_test.go` | 61 | secret | Slack token detected |
| `agentcc-gateway/internal/middleware/license_auth.go` | 234 | secret | Possible secret assigned to `TokenType` |
| `agentcc-gateway/internal/models/errors.go` | 14 | secret | Possible secret assigned to `ErrTypeAuthentication` |
| `agentcc-gateway/internal/plugins/otel/bodies_test.go` | 115 | secret | Possible secret assigned to `secret` |
| `agentcc-gateway/internal/providers/azure/azure_test.go` | 106 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/providers/azure/azure_test.go` | 258 | secret | Possible secret assigned to `apiKey` |
| `agentcc-gateway/internal/providers/azure/azure_test.go` | 545 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/providers/azure/azure_test.go` | 627 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 132 | secret | Possible secret assigned to `AWSAccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 133 | secret | Possible secret assigned to `AWSSecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 134 | secret | Possible secret assigned to `AWSSessionToken` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 154 | secret | Possible secret assigned to `AWSAccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 155 | secret | Possible secret assigned to `AWSSecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 169 | secret | Possible secret assigned to `AWSSecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 180 | secret | Possible secret assigned to `AWSAccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 344 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 345 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 393 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 394 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 435 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 436 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 437 | secret | Possible secret assigned to `SessionToken` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 465 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 466 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 492 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 493 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 522 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 523 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 548 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 549 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 566 | secret | Possible secret assigned to `AccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/auth_test.go` | 567 | secret | Possible secret assigned to `SecretAccessKey` |
| `agentcc-gateway/internal/providers/bedrock/bedrock_test.go` | 18 | secret | Possible secret assigned to `testAWSAccessKeyID` |
| `agentcc-gateway/internal/providers/bedrock/bedrock_test.go` | 19 | secret | Possible secret assigned to `testAWSSecretAccessKey` |
| `agentcc-gateway/internal/providers/gemini/gemini_test.go` | 1003 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/providers/gemini/gemini_test.go` | 1141 | secret | Possible secret assigned to `apiKey` |
| `agentcc-gateway/internal/providers/gemini/gemini_test.go` | 1218 | secret | Possible secret assigned to `apiKey` |
| `agentcc-gateway/internal/server/server_test.go` | 519 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/server/server_test.go` | 723 | secret | Possible secret assigned to `APIKey` |
| `agentcc-gateway/internal/server/server_test.go` | 743 | secret | Possible secret assigned to `APIKey` |
| `frontend/scripts/api-journeys/browser/alerts-lifecycle-mutation-smoke.mjs` | 398 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/alerts-lifecycle-mutation-smoke.mjs` | 402 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/annotation-simulation-content-smoke.mjs` | 246 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/gateway-settings-smoke.mjs` | 390 | secret | Possible secret assigned to `password` |
| `frontend/scripts/api-journeys/browser/resources-docs-help-smoke.mjs` | 165 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/resources-docs-help-smoke.mjs` | 236 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/settings-falcon-connectors-smoke.mjs` | 32 | secret | Possible secret assigned to `authHeaderName` |
| `frontend/scripts/api-journeys/browser/settings-workspace-smoke.mjs` | 177 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/settings-workspace-smoke.mjs` | 182 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/simulate-agent-definitions-lifecycle-smoke.mjs` | 387 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/simulate-personas-lifecycle-smoke.mjs` | 317 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/simulate-run-tests-read-smoke.mjs` | 393 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/browser/simulate-scenarios-detail-smoke.mjs` | 283 | dangerous-call | Use of `eval()` |
| `frontend/scripts/api-journeys/journeys/app-core.mjs` | 9122 | secret | Possible secret assigned to `auth_header_name` |
| `frontend/scripts/api-journeys/journeys/app-core.mjs` | 9134 | secret | Possible secret assigned to `authHeaderName` |
| `frontend/scripts/api-journeys/journeys/app-core.mjs` | 9268 | secret | Possible secret assigned to `auth_header_name` |
| `frontend/scripts/api-journeys/journeys/app-core.mjs` | 9310 | secret | Possible secret assigned to `auth_header_name` |
| `frontend/scripts/api-journeys/journeys/observe-filters.mjs` | 3320 | secret | Possible secret assigned to `api_key` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 218 | secret | Possible secret assigned to `refresh_token` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3204 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3262 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3556 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3696 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3858 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 3888 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6040 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6082 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6115 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6143 | secret | Possible secret assigned to `token` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6151 | secret | Possible secret assigned to `token` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6156 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/api-journeys/journeys/public-api.mjs` | 6177 | secret | Possible secret assigned to `auth_boundary` |
| `frontend/scripts/test-annotation-rules.mjs` | 712 | dangerous-call | Use of `eval()` |
| `frontend/scripts/test-annotation-rules.mjs` | 720 | dangerous-call | Use of `eval()` |
| `frontend/scripts/test-annotation-rules.mjs` | 840 | dangerous-call | Use of `eval()` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 18 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 65 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 2412 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 3609 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 3644 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 3827 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 4062 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 4121 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 4750 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 5092 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 5326 | secret | Possible secret assigned to `authorization_code` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 5463 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 6320 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 13020 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 14213 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 14984 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 15838 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 15887 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 16685 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 17042 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 18069 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 18913 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 21524 | secret | Possible secret assigned to `authentication_error` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 21971 | secret | Possible secret assigned to `daily_tokens_spent` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 21972 | secret | Possible secret assigned to `monthly_tokens_spent` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 22107 | secret | Possible secret assigned to `daily_tokens_spent` |
| `frontend/src/generated/api-contracts/api.schemas.ts` | 22108 | secret | Possible secret assigned to `monthly_tokens_spent` |
| `frontend/src/pages/dashboard/settings/__tests__/ProfileSettings.test.jsx` | 26 | secret | Possible secret assigned to `passwordResetInitiate` |
| `frontend/src/sections/agents/__tests__/agent-definition-schema.test.jsx` | 5 | secret | Possible secret assigned to `verifyApiKey` |
| `frontend/src/sections/common/EvalPicker/EvalPickerConfigFull.jsx` | 512 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/develop-detail/DataTab/DatapointDrawerV2/DatapointDrawerV2.jsx` | 992 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/develop-detail/Experiment/StepperComponentExperiment/EvaluationStepExperimentCreation.jsx` | 102 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/evals/components/CompositeDetailPanel.jsx` | 119 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/evals/components/SimulationTestMode.jsx` | 268 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/projects/LLMTracing/Renderers/CustomTraceRenderer.jsx` | 33 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/tasks/components/TaskLivePreview.jsx` | 961 | dangerous-call | Use of `eval()` |
| `frontend/src/sections/test/TestRuns/common.js` | 195 | secret | Possible secret assigned to `ToolEvaluationApiKey` |
| `frontend/src/utils/Mixpanel/EventNames.js` | 17 | secret | Possible secret assigned to `forgotPasswordClicked` |
| `frontend/src/utils/Mixpanel/EventNames.js` | 19 | secret | Possible secret assigned to `sendPasswordClicked` |
| `futureagi/accounts/tests/scripts/test_login_error_codes_e2e.py` | 49 | secret | Possible secret assigned to `WRONG_PASSWORD` |
| `futureagi/accounts/tests/test_keys.py` | 14 | secret | Possible secret assigned to `SECRET_KEYS_URL` |
| `futureagi/accounts/tests/test_signup.py` | 2150 | secret | Possible secret assigned to `password` |
| `futureagi/accounts/tests/test_workspace.py` | 35 | secret | Possible secret assigned to `password` |
| `futureagi/accounts/tests/test_workspace.py` | 64 | secret | Possible secret assigned to `password` |
| `futureagi/accounts/tests/test_workspace_management.py` | 41 | secret | Possible secret assigned to `password` |
| `futureagi/accounts/tests/test_workspace_management.py` | 60 | secret | Possible secret assigned to `password` |
| `futureagi/accounts/tests/test_workspace_management.py` | 79 | secret | Possible secret assigned to `password` |
| `futureagi/agentcc/tests/test_api.py` | 34 | secret | Possible secret assigned to `AGENTCC_TEST_ADMIN_TOKEN` |
| `futureagi/agentcc/tests/test_api.py` | 585 | secret | Possible secret assigned to `raw_secret` |
| `futureagi/agentcc/tests/test_api_key_bulk.py` | 12 | secret | Possible secret assigned to `ADMIN_TOKEN` |
| `futureagi/agentcc/tests/test_org_config_bulk.py` | 11 | secret | Possible secret assigned to `ADMIN_TOKEN` |
| `futureagi/agentcc/tests/test_provider_credential_api.py` | 340 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/agentcc/tests/test_spend_summary.py` | 15 | secret | Possible secret assigned to `ADMIN_TOKEN` |
| `futureagi/agentic_eval/core_evals/fi_utils/tests/test_restricted_code_execution.py` | 214 | dangerous-call | Use of `eval()` |
| `futureagi/agentic_eval/core_evals/run_prompt/error_handler.py` | 85 | secret | Possible secret assigned to `AuthenticationError` |
| `futureagi/agentic_eval/core_evals/run_prompt/tests/test_image_generation.py` | 462 | secret | Possible secret assigned to `api_key` |
| `futureagi/agentic_eval/core_evals/run_prompt/tests/test_live_all_providers.py` | 1377 | secret | Possible secret assigned to `api_key` |
| `futureagi/ai_tools/tools/evaluations/update_eval_template.py` | 82 | dangerous-call | Use of `eval()` |
| `futureagi/ee/agenthub/eval_orchestrator/orchestrator.py` | 5 | dangerous-call | Use of `eval()` |
| `futureagi/ee/evals/llm/agent_evaluator/evaluator.py` | 2870 | dangerous-call | Use of `eval()` |
| `futureagi/ee/voice/tests/test_livekit_api.py` | 20 | secret | Possible secret assigned to `INTERNAL_SECRET` |
| `futureagi/ee/voice/tests/test_livekit_service.py` | 33 | secret | Possible secret assigned to `api_secret` |
| `futureagi/evaluations/scripts/migrate_all_to_three_types.py` | 451 | dangerous-call | Use of `eval()` |
| `futureagi/mcp_server/tests/test_mcp_e2e.py` | 20 | secret | Possible secret assigned to `API_KEY` |
| `futureagi/mcp_server/tests/test_mcp_e2e.py` | 21 | secret | Possible secret assigned to `SECRET_KEY` |
| `futureagi/mcp_server/tests/test_oauth.py` | 125 | secret | Possible secret assigned to `secret` |
| `futureagi/model_hub/management/commands/migrate_user_evals_to_agent.py` | 189 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/tasks/agent.py` | 231 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/tests/test_dataset_dynamic_columns.py` | 688 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/tests/test_dataset_runtime_contracts.py` | 435 | secret | Possible secret assigned to `raw_secret_key` |
| `futureagi/model_hub/tests/test_dataset_runtime_contracts.py` | 499 | secret | Possible secret assigned to `raw_secret_key` |
| `futureagi/model_hub/tests/test_eval_list.py` | 146 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/tests/test_eval_sdk_code_contracts.py` | 80 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/model_hub/tests/test_eval_sdk_code_contracts.py` | 81 | secret | Possible secret assigned to `raw_secret_key` |
| `futureagi/model_hub/tests/test_run_prompt_api.py` | 39 | secret | Possible secret assigned to `API_KEYS_URL` |
| `futureagi/model_hub/tests/test_secrets_api.py` | 31 | secret | Possible secret assigned to `raw_other_secret` |
| `futureagi/model_hub/tests/test_secrets_api.py` | 47 | secret | Possible secret assigned to `raw_secret` |
| `futureagi/model_hub/tests/test_secrets_api.py` | 100 | secret | Possible secret assigned to `rotated_secret` |
| `futureagi/model_hub/utils/eval_cell_status.py` | 22 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/utils/json_path_resolver.py` | 307 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/views/develop_dataset.py` | 7684 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/views/develop_dataset.py` | 14542 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/views/eval_runner.py` | 2182 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/views/experiments.py` | 3465 | dangerous-call | Use of `eval()` |
| `futureagi/model_hub/views/separate_evals.py` | 3228 | dangerous-call | Use of `eval()` |
| `futureagi/simulate/services/hosted_runner.py` | 447 | secret | Possible secret assigned to `_ENV_LIVEKIT_API_SECRET` |
| `futureagi/simulate/tests/test_agent_definition_api.py` | 204 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/simulate/tests/test_agent_definition_api.py` | 465 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/simulate/tests/test_agent_definition_services.py` | 220 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_agent_definition_services.py` | 292 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_agent_definition_services.py` | 324 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_agent_definition_services.py` | 342 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_agent_definition_services.py` | 362 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_agent_version_api.py` | 184 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/simulate/tests/test_agent_version_api.py` | 219 | secret | Possible secret assigned to `raw_api_key` |
| `futureagi/simulate/tests/test_agent_version_api.py` | 260 | secret | Possible secret assigned to `raw_secret` |
| `futureagi/simulate/tests/test_alk_simulate_ingestion.py` | 487 | secret | Possible secret assigned to `INTERNAL_API_SECRET` |
| `futureagi/simulate/tests/test_alk_simulate_ingestion.py` | 513 | secret | Possible secret assigned to `INTERNAL_API_SECRET` |
| `futureagi/simulate/tests/test_alk_simulate_ingestion.py` | 1046 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_alk_simulate_ingestion.py` | 1047 | secret | Possible secret assigned to `api_secret` |
| `futureagi/simulate/tests/test_livekit_api_contract.py` | 355 | secret | Possible secret assigned to `LIVEKIT_API_KEY` |
| `futureagi/simulate/tests/test_livekit_api_contract.py` | 356 | secret | Possible secret assigned to `LIVEKIT_API_SECRET` |
| `futureagi/simulate/tests/test_livekit_api_functional.py` | 91 | secret | Possible secret assigned to `LIVEKIT_API_KEY` |
| `futureagi/simulate/tests/test_livekit_api_functional.py` | 92 | secret | Possible secret assigned to `LIVEKIT_API_SECRET` |
| `futureagi/simulate/tests/test_livekit_api_functional.py` | 140 | secret | Possible secret assigned to `LIVEKIT_API_KEY` |
| `futureagi/simulate/tests/test_livekit_api_functional.py` | 141 | secret | Possible secret assigned to `LIVEKIT_API_SECRET` |
| `futureagi/simulate/tests/test_scenario_columns_walker.py` | 164 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_simulation_read_surfaces.py` | 143 | secret | Possible secret assigned to `api_key` |
| `futureagi/simulate/tests/test_simulation_read_surfaces.py` | 144 | secret | Possible secret assigned to `secret_key` |
| `futureagi/tfc/settings/test.py` | 170 | secret | Possible secret assigned to `SECRET_KEY` |
| `futureagi/tfc/temporal/evaluations/activities.py` | 82 | dangerous-call | Use of `eval()` |
| `futureagi/tfc/utils/api_errors.py` | 10 | secret | Possible secret assigned to `AUTHENTICATION_ERROR` |
| `futureagi/tfc/utils/api_errors.py` | 26 | secret | Possible secret assigned to `NOT_AUTHENTICATED` |
| `futureagi/tracer/models/monitor.py` | 45 | secret | Possible secret assigned to `DAILY_TOKENS_SPENT` |
| `futureagi/tracer/models/monitor.py` | 46 | secret | Possible secret assigned to `MONTHLY_TOKENS_SPENT` |
| `futureagi/tracer/queries/feed.py` | 1882 | dangerous-call | Use of `eval()` |
| `futureagi/tracer/services/clickhouse/query_builders/monitor_metrics.py` | 43 | secret | Possible secret assigned to `DAILY_TOKENS_SPENT` |
| `futureagi/tracer/services/clickhouse/query_builders/monitor_metrics.py` | 44 | secret | Possible secret assigned to `MONTHLY_TOKENS_SPENT` |
| `futureagi/tracer/services/clickhouse/v2/span_reader.py` | 1951 | dangerous-call | Use of `eval()` |
| `futureagi/tracer/tests/integration/_seed.py` | 159 | dangerous-call | Use of `eval()` |
| `futureagi/tracer/tests/integration/_seed.py` | 383 | dangerous-call | Use of `eval()` |
| `futureagi/tracer/tests/integration/_seed.py` | 406 | dangerous-call | Use of `eval()` |
| `futureagi/tracer/tests/test_observability_provider_api.py` | 343 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_provider_api.py` | 394 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_provider_api.py` | 456 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 25 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 419 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 460 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 544 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 581 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 665 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 715 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 759 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 848 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 922 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_observability_providers.py` | 967 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_vapi_normalize_inline_rehost.py` | 108 | secret | Possible secret assigned to `api_key` |
| `futureagi/tracer/tests/test_vapi_normalize_inline_rehost.py` | 110 | secret | Possible secret assigned to `api_key` |

### 🟡 MEDIUM (14)

| File | Line | Category | Message |
|---|---:|---|---|
| `agentcc-gateway/internal/guardrails/secrets/secrets_test.go` | 101 | secret | JWT token literal detected |
| `frontend/scripts/api-journeys/journeys/app-core.mjs` | 13650 | secret | High-entropy string literal (entropy=5.0) |
| `frontend/src/pages/shared/__tests__/SharedView.test.jsx` | 106 | secret | High-entropy string literal (entropy=4.7) |
| `futureagi/accounts/tests/test_passkeys.py` | 32 | secret | High-entropy string literal (entropy=4.5) |
| `futureagi/accounts/tests/test_passkeys.py` | 36 | secret | High-entropy string literal (entropy=4.7) |
| `futureagi/accounts/tests/test_recovery_codes.py` | 148 | secret | High-entropy string literal (entropy=4.5) |
| `futureagi/agentic_eval/core_evals/fi_utils/restricted_code_execution.py` | 257 | dangerous-call | Use of `exec()` |
| `futureagi/agentic_eval/core_evals/fi_utils/sandbox.py` | 259 | dangerous-call | Use of `exec()` |
| `futureagi/agentic_eval/core_evals/fi_utils/tests/test_restricted_code_execution.py` | 205 | dangerous-call | Use of `exec()` |
| `futureagi/code-executor/server.py` | 257 | dangerous-call | Use of `exec()` |
| `futureagi/model_hub/tests/test_dataset_dynamic_columns.py` | 689 | dangerous-call | Use of `exec()` |
| `futureagi/model_hub/views/develop_dataset.py` | 9931 | dangerous-call | Use of `exec()` |
| `futureagi/model_hub/views/dynamic_columns.py` | 1338 | dangerous-call | Use of `exec()` |
| `futureagi/tests/stress/conftest.py` | 275 | dangerous-call | Use of `mktemp` — race condition; use mkstemp/TemporaryFile |

### 🔵 LOW (4)

| File | Line | Category | Message |
|---|---:|---|---|
| `futureagi/accounts/demo_dataset/experiment.json` | 4045 | secret | Possible 40-hex token (legacy GitHub SHA or token) |
| `futureagi/accounts/demo_dataset/table_data.json` | 5018 | secret | Possible 40-hex token (legacy GitHub SHA or token) |
| `futureagi/accounts/demo_dataset/table_data.json` | 5063 | secret | Possible 40-hex token (legacy GitHub SHA or token) |
| `futureagi/accounts/demo_dataset/table_data.json` | 5155 | secret | Possible 40-hex token (legacy GitHub SHA or token) |

## What this scan checks

- **Secrets**: AWS access keys, AWS secret keys, GitHub tokens, Slack tokens, Google API keys, JWT literals, private key blocks, and high-entropy strings assigned to secret-looking variable names.
- **Deprecated packages**: a small offline list of widely-abandoned packages from `requirements.txt`, `pyproject.toml`, and `package.json`.
- **Dangerous calls**: `eval`, `exec`, `os.system`, `subprocess` with `shell=True`, `pickle.load`, `mktemp`, and JS `new Function` / `child_process` with `shell: true`.

_False positives are expected. Triage by severity, then verify each hit._
