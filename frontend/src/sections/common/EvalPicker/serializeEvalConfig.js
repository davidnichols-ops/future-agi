const RUN_CONFIG_KEYS = [
  "model",
  "agent_mode",
  "check_internet",
  "summary",
  "tools",
  "knowledge_bases",
  "mcp_connectors",
  "data_injection",
  "pass_threshold",
  "params",
];

export function serializeEvalConfig(evalConfig) {
  const runConfig = {};
  for (const k of RUN_CONFIG_KEYS) {
    if (evalConfig[k] !== undefined) runConfig[k] = evalConfig[k];
  }
  if (evalConfig.error_localizer_enabled !== undefined) {
    runConfig.error_localizer_enabled = !!evalConfig.error_localizer_enabled;
  }
  return {
    template_id: evalConfig.template_id,
    name: evalConfig.name,
    model: evalConfig.model,
    mapping: evalConfig.mapping || {},
    config: {
      ...(evalConfig.config || {}),
      ...(evalConfig.params !== undefined && { params: evalConfig.params }),
      run_config: {
        ...(evalConfig.config?.run_config || {}),
        ...runConfig,
      },
    },
    error_localizer: !!evalConfig.error_localizer_enabled,
    filters: evalConfig.filters || [],
  };
}
