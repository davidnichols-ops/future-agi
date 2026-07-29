import React, { useState, useMemo, useEffect } from "react";
import PropTypes from "prop-types";
import { Box, Stack, Typography, Button, Tooltip, LinearProgress, TextField, IconButton } from "@mui/material";
import { alpha } from "@mui/material/styles";
import { keyframes } from "@mui/system";
import { useQuery } from "@tanstack/react-query";
import { RouterLink } from "src/routes/components";
import { paths } from "src/routes/paths";
import Iconify from "src/components/iconify";
import { useSnackbar } from "src/components/snackbar";
import axios, { endpoints } from "src/utils/axios";
import { useApiKeysStatus } from "src/api/model/api-keys";
import { filterAndSortProviders } from "src/components/custom-model-dropdown/KeysHelper";
import KeyCard from "src/sections/develop-detail/Common/ConfigureKeys/KeyCard";
import CustomModalKeyCard from "src/components/custom-model-dropdown/CustomModalKeyCard";
import CloudProviderCard from "src/components/custom-model-dropdown/CloudProviderCard";
import { useDeleteApiKey, DELETE_MODAL_TYPE } from "src/hooks/use-delete-api-key";
import ObserveInstruments from "src/sections/project/NewProject/ObserveInstuments";
import { LOGOS } from "./provider-logos";

const MONO = "ui-monospace, SFMono-Regular, Menlo, monospace";

// ---- animations -----------------------------------------------------------
const sonar = keyframes`
  0%   { transform: scale(0.6); opacity: 0.7; }
  100% { transform: scale(2.6); opacity: 0; }
`;
const dot = keyframes`
  0%, 100% { opacity: 0.5; transform: scale(0.85); }
  50%      { opacity: 1; transform: scale(1); }
`;
const shimmer = keyframes`
  0%   { transform: translateX(-120%); }
  100% { transform: translateX(320%); }
`;

// ---- integration snippets -------------------------------------------------
const SNIPPETS = {
  Python: {
    install: "pip install futureagi traceAI-openai",
    code: `from fi_instrumentation import register
from fi_instrumentation.fi_types import ProjectType
from traceai_openai import OpenAIInstrumentor

# Streams every LLM call to Future AGI
tracer = register(
    project_name="my-first-project",
    project_type=ProjectType.OBSERVE,
)
OpenAIInstrumentor().instrument(tracer_provider=tracer)`,
  },
  TypeScript: {
    install: "npm install @futureagi/instrumentation",
    code: `import { register } from "@futureagi/instrumentation";

// Streams every LLM call to Future AGI
register({
  projectName: "my-first-project",
  projectType: "observe",
});`,
  },
};

const AGENT_PROMPT =
  "Add Future AGI tracing to this application. Install the traceAI SDK, register the tracer with project_type OBSERVE using my FI_API_KEY and FI_SECRET_KEY, and instrument every LLM and agent call following Future AGI's best practices. Refer to https://docs.futureagi.com for details.";

const EVAL_SNIPPET = `from ai_evaluation import Evaluator

evaluator = Evaluator()
result = evaluator.evaluate(
    eval_templates="factual_accuracy",
    inputs={"response": "The Eiffel Tower is in Paris."},
)
print(result.eval_results)`;

const FEATURES = [
  { icon: "solar:test-tube-linear", label: "Evaluations", desc: "Score factuality, safety & more", to: paths.dashboard.evals },
  { icon: "solar:widget-5-linear", label: "Datasets", desc: "Build & version test data", to: paths.dashboard.develop },
  { icon: "solar:chat-square-code-linear", label: "Prompts", desc: "Iterate & compare versions", to: paths.dashboard.prompt },
  { icon: "solar:routing-2-linear", label: "Agents", desc: "Trace & optimise agent runs", to: paths.dashboard.agents },
  { icon: "solar:play-circle-linear", label: "Simulate", desc: "Test on real-world scenarios", to: paths.dashboard.simulate.root },
  { icon: "solar:chart-2-linear", label: "Dashboards", desc: "Monitor latency, cost, quality", to: paths.dashboard.dashboards.root },
];

// ---------------------------------------------------------------------------
function CopyButton({ text }) {
  const { enqueueSnackbar } = useSnackbar();
  const [copied, setCopied] = useState(false);
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      enqueueSnackbar("Could not copy", { variant: "error" });
    }
  };
  return (
    <Tooltip title={copied ? "Copied" : "Copy"}>
      <Box
        component="button"
        onClick={onCopy}
        sx={{
          border: "none",
          cursor: "pointer",
          bgcolor: "transparent",
          color: "text.secondary",
          display: "inline-flex",
          p: 0.5,
          borderRadius: 0.75,
          "&:hover": { color: "common.white", bgcolor: (t) => alpha(t.palette.common.white, 0.06) },
        }}
      >
        <Iconify icon={copied ? "solar:check-read-linear" : "solar:copy-linear"} width={15} />
      </Box>
    </Tooltip>
  );
}

function CodeBlock({ label, children, copyText }) {
  return (
    <Box
      sx={{
        borderRadius: 1.5,
        border: "1px solid",
        borderColor: "divider",
        overflow: "hidden",
        bgcolor: "#0B0B0F",
      }}
    >
      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{ px: 1.5, py: 1, borderBottom: "1px solid", borderColor: "divider" }}
      >
        <Stack direction="row" spacing={0.6}>
          {[0, 1, 2].map((i) => (
            <Box key={i} sx={{ width: 9, height: 9, borderRadius: "50%", bgcolor: (t) => alpha(t.palette.common.white, 0.18) }} />
          ))}
        </Stack>
        {label && (
          <Typography sx={{ fontFamily: MONO, fontSize: 11, color: "text.disabled" }}>{label}</Typography>
        )}
        <Box sx={{ flex: 1 }} />
        <CopyButton text={copyText ?? children} />
      </Stack>
      <Box sx={{ p: 2, overflowX: "auto" }}>
        <Box component="pre" sx={{ m: 0, fontFamily: MONO, fontSize: 12.5, lineHeight: 1.75, color: "#E6E6EB", whiteSpace: "pre" }}>
          {children}
        </Box>
      </Box>
    </Box>
  );
}

function StepCard({ n, title, action, children, last }) {
  return (
    <Stack direction="row" spacing={2}>
      {/* connecting rail */}
      <Stack alignItems="center" sx={{ flexShrink: 0 }}>
        <Box
          sx={{
            width: 28,
            height: 28,
            borderRadius: "50%",
            flexShrink: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontFamily: "Inter",
            fontSize: 13,
            fontWeight: 600,
            color: "common.black",
            bgcolor: "common.white",
          }}
        >
          {n}
        </Box>
        {!last && (
          <Box
            sx={{
              flex: 1,
              my: 0.75,
              borderLeft: "1.5px dotted",
              borderColor: (t) => alpha(t.palette.common.white, 0.28),
            }}
          />
        )}
      </Stack>

      {/* content */}
      <Box sx={{ flex: 1, minWidth: 0, pb: last ? 0 : 4 }}>
        <Stack
          direction="row"
          alignItems="center"
          spacing={1.5}
          sx={{ minHeight: 28, mb: 1.5 }}
        >
          <Typography sx={{ fontFamily: "Inter", fontSize: 16, fontWeight: 500, color: "text.primary", flex: 1 }}>
            {title}
          </Typography>
          {action}
        </Stack>
        {children}
      </Box>
    </Stack>
  );
}

// white pill button (monochrome primary)
const whiteBtnSx = {
  borderRadius: 1,
  height: 32,
  textTransform: "none",
  fontFamily: "Inter",
  bgcolor: "common.white",
  color: "common.black",
  "&:hover": { bgcolor: (t) => alpha(t.palette.common.white, 0.88) },
};

// outlined ghost button (monochrome secondary)
const ghostBtnSx = {
  borderRadius: 1,
  height: 32,
  textTransform: "none",
  fontFamily: "Inter",
  color: "text.primary",
  borderColor: "divider",
  "&:hover": { borderColor: (t) => alpha(t.palette.common.white, 0.4), bgcolor: "transparent" },
};

// scrollable container shell for the reused key / instrument sections
const scrollShellSx = {
  borderRadius: 1.5,
  border: "1px solid",
  borderColor: "divider",
  bgcolor: (t) => alpha(t.palette.common.white, 0.02),
  overflowY: "auto",
  "&::-webkit-scrollbar": { width: 8 },
  "&::-webkit-scrollbar-thumb": {
    backgroundColor: (t) => alpha(t.palette.common.white, 0.14),
    borderRadius: 4,
  },
};

// Prototype demo: the key/instrument APIs 401 under the fake proto session, so
// fall back to representative mock data (matching the real response shapes) so
// the Get Started page looks populated. On a real OSS backend the live data is
// used instead.
const isProtoSession = () =>
  import.meta.env.VITE_PROTOTYPE_AUTH_BYPASS === "true" &&
  localStorage.getItem("oss_proto_session") === "1";

// Real brand logos, bundled locally (public/assets/icons/providers) so they
// always render offline — colored brands keep their colour; monochrome brands
// (OpenAI/Anthropic/…) were fetched in white so they show on the dark cards.
// (On a real OSS backend the live logoUrl from the API is used instead.)
const LOGO = (f) => LOGOS[f];

const MOCK_KEY_PROVIDERS = [
  { id: "m-openai", provider: "openai", display_name: "OpenAI", type: "text", logoUrl: LOGO("openai"), hasKey: true, maskedKey: "sk-p*********dtMA" },
  { id: "m-anthropic", provider: "anthropic", display_name: "Anthropic", type: "text", logoUrl: LOGO("anthropic"), hasKey: false, maskedKey: null },
  { id: "m-google", provider: "google", display_name: "Gemini", type: "text", logoUrl: LOGO("gemini"), hasKey: true, maskedKey: "AIza*********5TtM" },
  { id: "m-vertex", provider: "vertex_ai", display_name: "Vertex AI", type: "text", logoUrl: LOGO("vertex"), hasKey: false, maskedKey: null },
  { id: "m-azure", provider: "azure", display_name: "Azure OpenAI", type: "text", logoUrl: LOGO("azure"), hasKey: false, maskedKey: null },
  { id: "m-bedrock", provider: "bedrock", display_name: "AWS Bedrock", type: "text", logoUrl: LOGO("aws"), hasKey: false, maskedKey: null },
  { id: "m-sagemaker", provider: "sagemaker", display_name: "SageMaker", type: "text", logoUrl: LOGO("aws"), hasKey: false, maskedKey: null },
  { id: "m-hf", provider: "huggingface", display_name: "Hugging Face", type: "text", logoUrl: LOGO("huggingface"), hasKey: false, maskedKey: null },
  { id: "m-mistral", provider: "mistral", display_name: "Mistral AI", type: "text", logoUrl: LOGO("mistral"), hasKey: false, maskedKey: null },
  { id: "m-perplexity", provider: "perplexity", display_name: "Perplexity", type: "text", logoUrl: LOGO("perplexity"), hasKey: false, maskedKey: null },
  { id: "m-nvidia", provider: "nvidia", display_name: "NVIDIA NIM", type: "text", logoUrl: LOGO("nvidia"), hasKey: false, maskedKey: null },
  { id: "m-ollama", provider: "ollama", display_name: "Ollama", type: "text", logoUrl: LOGO("ollama"), hasKey: false, maskedKey: null },
];

const GH = "https://github.com/future-agi/traceAI";
const pyInstr = (pkg, cls) =>
  `from traceai_${pkg} import ${cls}\n\n# Trace every call automatically\n${cls}().instrument(tracer_provider=tracer)`;
const tsInstr = (mod, cls) =>
  `import { ${cls} } from "@traceai/${mod}";\n\nregisterInstrumentations({\n  instrumentations: [new ${cls}()],\n});`;

// Shape mirrors getCodeBlockTracer().result.instruments (name/logo/Python/TypeScript).
// ObserveInstruments renders <img> with no theme filter, so monochrome brands
// are forced white here to stay visible on the dark cards.
const MOCK_INSTRUMENTS = {
  langchain: { name: "LangChain", logo: LOGO("langchain"), Python: { github: GH, code: pyInstr("langchain", "LangChainInstrumentor") }, TypeScript: { github: GH, code: tsInstr("langchain", "LangChainInstrumentation") } },
  langgraph: { name: "LangGraph", logo: LOGO("langgraph"), Python: { github: GH, code: pyInstr("langchain", "LangChainInstrumentor") } },
  openai: { name: "OpenAI", logo: LOGO("openai"), Python: { github: GH, code: pyInstr("openai", "OpenAIInstrumentor") }, TypeScript: { github: GH, code: tsInstr("openai", "OpenAIInstrumentation") } },
  openai_agents: { name: "OpenAI Agents", logo: LOGO("openai"), Python: { github: GH, code: pyInstr("openai_agents", "OpenAIAgentsInstrumentor") } },
  anthropic: { name: "Anthropic", logo: LOGO("anthropic"), Python: { github: GH, code: pyInstr("anthropic", "AnthropicInstrumentor") } },
  bedrock: { name: "AWS Bedrock", logo: LOGO("aws"), Python: { github: GH, code: pyInstr("bedrock", "BedrockInstrumentor") } },
  gemini: { name: "Google Gemini", logo: LOGO("gemini"), Python: { github: GH, code: pyInstr("google_genai", "GoogleGenAIInstrumentor") } },
  vertexai: { name: "Vertex AI", logo: LOGO("vertex"), Python: { github: GH, code: pyInstr("vertexai", "VertexAIInstrumentor") } },
  mistral: { name: "Mistral AI", logo: LOGO("mistral"), Python: { github: GH, code: pyInstr("mistralai", "MistralAIInstrumentor") } },
  groq: { name: "Groq", logo: LOGO("groq"), Python: { github: GH, code: pyInstr("groq", "GroqInstrumentor") } },
  crewai: { name: "CrewAI", logo: LOGO("crewai"), Python: { github: GH, code: pyInstr("crewai", "CrewAIInstrumentor") } },
  litellm: { name: "LiteLLM", logo: LOGO("litellm"), Python: { github: GH, code: pyInstr("litellm", "LiteLLMInstrumentor") } },
  haystack: { name: "Haystack", logo: LOGO("haystack"), Python: { github: GH, code: pyInstr("haystack", "HaystackInstrumentor") } },
  smolagents: { name: "Smolagents", logo: LOGO("huggingface"), Python: { github: GH, code: pyInstr("smolagents", "SmolagentsInstrumentor") } },
  pydantic_ai: { name: "Pydantic AI", logo: LOGO("pydantic"), Python: { github: GH, code: pyInstr("pydantic_ai", "PydanticAIInstrumentor") } },
  vercel_ai: { name: "Vercel AI SDK", logo: LOGO("vercel"), TypeScript: { github: GH, code: tsInstr("vercel", "VercelAIInstrumentation") } },
  mcp: { name: "MCP (Model Context Protocol)", logo: LOGO("mcp"), Python: { github: GH, code: pyInstr("mcp", "MCPInstrumentor") } },
};

// Prototype key card — plain <img> (crisp, full control) so we don't fight the
// shared KeyCard's lazy/blur/cover <Image>. Used only for the OSS mock; the real
// backend path still uses KeyCard.
function ProviderKeyCard({ data }) {
  const hasKey = !!data.maskedKey;
  return (
    <Box
      sx={{
        border: "1px solid",
        borderColor: "divider",
        bgcolor: "background.paper",
        borderRadius: 1,
        p: 2,
        display: "flex",
        flexDirection: "column",
        gap: 1.5,
      }}
    >
      <Stack direction="row" alignItems="center" spacing={1.25}>
        <Box
          component="img"
          src={data.logoUrl}
          alt={data.display_name}
          sx={{ width: 24, height: 24, p: "3px", boxSizing: "border-box", objectFit: "contain", flexShrink: 0 }}
        />
        <Typography sx={{ fontFamily: "Inter", fontSize: 14, fontWeight: 500, color: "text.primary", flex: 1 }}>
          {data.display_name}
        </Typography>
        {hasKey && <Iconify icon="solar:check-circle-bold" width={18} sx={{ color: "success.main" }} />}
      </Stack>
      <Stack direction="row" spacing={1} alignItems="center">
        <TextField
          size="small"
          fullWidth
          label="API Key"
          value={data.maskedKey || ""}
          InputProps={{ readOnly: true }}
          sx={{ "& .MuiInputBase-input": { fontFamily: MONO, fontSize: 12.5, color: "text.secondary" } }}
        />
        {hasKey ? (
          <Stack direction="row" spacing={0.5} sx={{ flexShrink: 0 }}>
            <IconButton size="small" component={RouterLink} href={paths.dashboard.keys} sx={{ color: "text.secondary", border: "1px solid", borderColor: "divider", borderRadius: 1 }}>
              <Iconify icon="solar:pen-linear" width={15} />
            </IconButton>
            <IconButton size="small" component={RouterLink} href={paths.dashboard.keys} sx={{ color: "text.secondary", border: "1px solid", borderColor: "divider", borderRadius: 1 }}>
              <Iconify icon="solar:trash-bin-minimalistic-linear" width={15} />
            </IconButton>
          </Stack>
        ) : (
          <Button
            component={RouterLink}
            href={paths.dashboard.keys}
            size="small"
            variant="outlined"
            sx={{
              flexShrink: 0,
              minWidth: 64,
              height: 36,
              borderRadius: 1,
              textTransform: "none",
              fontFamily: "Inter",
              color: "text.primary",
              borderColor: "divider",
              "&:hover": { borderColor: (t) => alpha(t.palette.common.white, 0.4) },
            }}
          >
            Add
          </Button>
        )}
      </Stack>
    </Box>
  );
}
ProviderKeyCard.propTypes = { data: PropTypes.object };

// ---- Platform (model provider) keys — reused from the old Get Started -------
function PlatformKeys() {
  const { data, isFetching } = useApiKeysStatus({});
  const usingMock =
    isProtoSession() && (!data || data.length === 0);
  const providers = usingMock ? MOCK_KEY_PROVIDERS : data;

  const { data: customModels } = useQuery({
    queryKey: ["customModals"],
    queryFn: async () => {
      const { data } = await axios.get(endpoints.settings.customModal.getCustomModal);
      return data;
    },
    select: (d) => d?.results || [],
  });

  const defaultModelProviders = useMemo(
    () => filterAndSortProviders(providers, "text", ""),
    [providers],
  );
  const cloudProviders = useMemo(
    () => filterAndSortProviders(providers, "json", ""),
    [providers],
  );

  const { setOpenDeleteModal } = useDeleteApiKey();

  return (
    <Box sx={{ ...scrollShellSx, height: 320, p: 1.5 }}>
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: { xs: "1fr", sm: "1fr 1fr" },
          gap: "16px 12px",
        }}
      >
        {usingMock ? (
          // OSS mock — our own crisp cards (plain <img>).
          MOCK_KEY_PROVIDERS.map((d) => (
            <ProviderKeyCard key={d.provider} data={d} />
          ))
        ) : (
          <>
            {defaultModelProviders?.map((d) => (
              <KeyCard
                key={d.provider}
                data={d}
                onClose={() => {}}
                isFetching={isFetching}
                onDeleteClick={() =>
                  setOpenDeleteModal({ id: d.id, type: DELETE_MODAL_TYPE.NORMAL })
                }
              />
            ))}
            {(customModels || []).map((model) => (
              <CustomModalKeyCard
                key={model.id}
                data={model}
                onDeleteClick={() =>
                  setOpenDeleteModal({ id: model?.id, type: DELETE_MODAL_TYPE.CUSTOM })
                }
              />
            ))}
            {cloudProviders.map((provider) =>
              provider.type === "json" ? (
                <CloudProviderCard
                  key={provider.provider}
                  provider={provider}
                  showJsonField={false}
                  onDeleteClick={() =>
                    setOpenDeleteModal({ id: provider.id, type: DELETE_MODAL_TYPE.NORMAL })
                  }
                />
              ) : (
                <KeyCard
                  key={provider.provider}
                  data={provider}
                  onClose={() => {}}
                  isFetching={isFetching}
                  onDeleteClick={() =>
                    setOpenDeleteModal({ id: provider.id, type: DELETE_MODAL_TYPE.NORMAL })
                  }
                />
              ),
            )}
          </>
        )}
      </Box>
    </Box>
  );
}

// ---- Connect your applications — pick an app (3/row), see its code below ----
function ConnectApplications() {
  const [lang, setLang] = useState("python");
  const [selected, setSelected] = useState(null);

  const { data: keysData, isLoading, error } = useQuery({
    queryKey: ["oss-observe-instruments"],
    queryFn: () =>
      axios.get(endpoints.project.getCodeBlockTracer, {
        params: { project_type: "observe" },
      }),
    select: (d) => d.data?.result,
  });

  // Prototype: API 401s → show a representative mock list instead of an error.
  const useMock = isProtoSession() && (!!error || !keysData?.instruments);
  const instruments = useMock ? MOCK_INSTRUMENTS : keysData?.instruments;
  const entries = useMemo(
    () => (instruments ? Object.entries(instruments) : []),
    [instruments],
  );

  // Default to the first application once the list is available.
  useEffect(() => {
    if (!selected && entries.length) setSelected(entries[0][0]);
  }, [entries, selected]);

  if (isLoading && !useMock) return <LinearProgress />;
  if (!entries.length) return null;

  const sel = selected ? instruments[selected] : null;
  const langKey = lang === "python" ? "Python" : "TypeScript";
  const langData = sel ? sel[langKey] || sel.Python || sel.TypeScript : null;
  const hasBoth = sel?.Python && sel?.TypeScript;

  return (
    <Box>
      {/* App grid — 4 per row */}
      <Box
        sx={{
          display: "grid",
          gridTemplateColumns: { xs: "1fr 1fr", sm: "1fr 1fr 1fr", md: "1fr 1fr 1fr 1fr" },
          gap: 1.25,
        }}
      >
        {entries.map(([key, item]) => {
          const active = key === selected;
          return (
            <Box
              key={key}
              component="button"
              onClick={() => setSelected(key)}
              sx={{
                cursor: "pointer",
                textAlign: "left",
                display: "flex",
                alignItems: "center",
                gap: 1.25,
                px: 1.5,
                py: 1.25,
                borderRadius: 1.5,
                bgcolor: "background.paper",
                border: "1px solid",
                borderColor: active ? (t) => alpha(t.palette.common.white, 0.55) : "divider",
                transition: "border-color 0.15s, background-color 0.15s",
                "&:hover": { borderColor: (t) => alpha(t.palette.common.white, 0.35) },
              }}
            >
              <Box
                component="img"
                src={item?.logo}
                alt={item?.name}
                sx={{ width: 22, height: 22, p: "2px", boxSizing: "border-box", objectFit: "contain", flexShrink: 0 }}
              />
              <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, fontWeight: 500, color: "text.primary", flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {item?.name}
              </Typography>
              {active && <Iconify icon="solar:check-circle-bold" width={16} sx={{ color: "common.white" }} />}
            </Box>
          );
        })}
      </Box>

      {/* Selected app's setup code */}
      {sel && langData && (
        <Box sx={{ mt: 2 }}>
          <Stack direction="row" alignItems="center" spacing={1.5} sx={{ mb: 1 }}>
            <Box
              component="img"
              src={sel?.logo}
              alt={sel?.name}
              sx={{ width: 18, height: 18, objectFit: "contain", flexShrink: 0 }}
            />
            <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, fontWeight: 500, color: "text.primary", flex: 1 }}>
              {sel?.name}
            </Typography>
            {langData.github && (
              <Tooltip title="View on GitHub">
                <Box
                  component="a"
                  href={langData.github}
                  target="_blank"
                  rel="noopener noreferrer"
                  sx={{ display: "inline-flex", color: "text.secondary", "&:hover": { color: "text.primary" } }}
                >
                  <Iconify icon="mdi:github" width={18} />
                </Box>
              </Tooltip>
            )}
            {hasBoth && (
              <Stack direction="row" spacing={0.5} sx={{ p: 0.4, borderRadius: 1, bgcolor: (t) => alpha(t.palette.common.white, 0.05) }}>
                {[
                  { k: "python", label: "Python" },
                  { k: "typescript", label: "TypeScript" },
                ].map((l) => (
                  <Box
                    key={l.k}
                    component="button"
                    onClick={() => setLang(l.k)}
                    sx={{
                      border: "none",
                      cursor: "pointer",
                      borderRadius: 0.75,
                      px: 1.25,
                      py: 0.35,
                      fontFamily: "Inter",
                      fontSize: 12,
                      fontWeight: 500,
                      color: lang === l.k ? "common.black" : "text.secondary",
                      bgcolor: lang === l.k ? "common.white" : "transparent",
                    }}
                  >
                    {l.label}
                  </Box>
                ))}
              </Stack>
            )}
          </Stack>
          <CodeBlock label={`${sel?.name}`} copyText={langData.code}>
            {langData.code}
          </CodeBlock>
        </Box>
      )}
    </Box>
  );
}

// ---------------------------------------------------------------------------
export default function GetStartedOssView() {
  const [method, setMethod] = useState("sdk"); // "sdk" (applications) | "agent"

  return (
    <Box sx={{ px: { xs: 3, md: 6 }, py: 4, width: "100%" }}>
      {/* Header */}
      <Box
        sx={{
          display: "inline-flex",
          alignItems: "center",
          gap: 0.75,
          px: 1,
          py: 0.4,
          mb: 1.75,
          borderRadius: 1,
          border: "1px solid",
          borderColor: "divider",
          bgcolor: (t) => alpha(t.palette.common.white, 0.05),
        }}
      >
        <Iconify icon="solar:rocket-2-linear" width={13} sx={{ color: "text.secondary" }} />
        <Typography sx={{ fontFamily: "Inter", fontSize: 11, fontWeight: 500, letterSpacing: 0.8, color: "text.secondary" }}>
          GET STARTED
        </Typography>
      </Box>

      <Typography sx={{ fontFamily: "Inter", fontSize: 22, fontWeight: 400, color: "text.primary", lineHeight: 1.25 }}>
        Let&apos;s launch{" "}
        <Box component="span" sx={{ color: "text.secondary" }}>
          your first project
        </Box>
      </Typography>
      <Typography sx={{ fontFamily: "Inter", fontSize: 14.5, fontWeight: 400, color: "text.secondary", mt: 0.75, whiteSpace: { md: "nowrap" } }}>
        Trace every LLM call, evaluate quality, and optimise — a few steps, a couple of minutes.
      </Typography>

      {/* Waiting for trace — compact, monochrome */}
      <WaitingForTrace />

      {/* Steps */}
      <Box sx={{ mt: 3 }}>
        {/* Step 1 — the platform keys the user adds to their environment */}
        <StepCard
          n={1}
          title="Add your platform keys"
          action={
            <Button component={RouterLink} href={paths.dashboard.keys} size="small" variant="contained" sx={whiteBtnSx}>
              Create key
            </Button>
          }
        >
          <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, color: "text.secondary", mb: 1.5 }}>
            Grab your Future AGI platform keys and add them to your environment — the SDK reads
            them to authenticate. Keep them secret.
          </Typography>
          <CodeBlock label="shell" copyText={"export FI_API_KEY=your_api_key\nexport FI_SECRET_KEY=your_secret_key"}>
{`export FI_API_KEY=your_api_key
export FI_SECRET_KEY=your_secret_key`}
          </CodeBlock>
        </StepCard>

        {/* Step 2 — connect your applications (pick an app → code, or coding agent) */}
        <StepCard
          n={2}
          title="Connect your applications"
          action={
            <Stack direction="row" spacing={0.5} sx={{ p: 0.4, borderRadius: 1, bgcolor: (t) => alpha(t.palette.common.white, 0.05) }}>
              {[
                { k: "sdk", label: "Applications" },
                { k: "agent", label: "Coding agent" },
              ].map((m) => (
                <Box
                  key={m.k}
                  component="button"
                  onClick={() => setMethod(m.k)}
                  sx={{
                    border: "none",
                    cursor: "pointer",
                    borderRadius: 0.75,
                    px: 1.25,
                    py: 0.4,
                    fontFamily: "Inter",
                    fontSize: 12.5,
                    fontWeight: 500,
                    whiteSpace: "nowrap",
                    color: method === m.k ? "common.black" : "text.secondary",
                    bgcolor: method === m.k ? "common.white" : "transparent",
                  }}
                >
                  {m.label}
                </Box>
              ))}
            </Stack>
          }
        >
          {method === "agent" ? (
            <>
              <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, color: "text.secondary", mb: 1.5 }}>
                Paste this prompt into Claude, Cursor, Copilot or any coding agent — it wires up tracing for you.
              </Typography>
              <Box
                sx={{
                  borderRadius: 1.5,
                  border: "1px solid",
                  borderColor: "divider",
                  bgcolor: (t) => alpha(t.palette.common.white, 0.02),
                  p: 2,
                }}
              >
                <Stack direction="row" spacing={2} alignItems="flex-start">
                  <Typography
                    sx={{
                      flex: 1,
                      fontFamily: MONO,
                      fontSize: 12.5,
                      lineHeight: 1.7,
                      color: "text.primary",
                    }}
                  >
                    {AGENT_PROMPT}
                  </Typography>
                  <Button
                    onClick={() => {
                      navigator.clipboard?.writeText(AGENT_PROMPT);
                    }}
                    size="small"
                    variant="contained"
                    startIcon={<Iconify icon="solar:copy-linear" width={15} />}
                    sx={{ ...whiteBtnSx, flexShrink: 0 }}
                  >
                    Copy prompt
                  </Button>
                </Stack>
              </Box>
              <Box
                component="button"
                onClick={() => setMethod("sdk")}
                sx={{
                  mt: 1.5,
                  border: "none",
                  bgcolor: "transparent",
                  cursor: "pointer",
                  p: 0,
                  fontFamily: "Inter",
                  fontSize: 13,
                  color: "text.secondary",
                  textDecoration: "underline",
                  textUnderlineOffset: 3,
                  "&:hover": { color: "text.primary" },
                }}
              >
                or pick your application from the list
              </Box>
            </>
          ) : (
            <>
              <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, color: "text.secondary", mb: 1.5 }}>
                Pick the framework or SDK you&apos;re using — its setup code appears below.
              </Typography>
              <ConnectApplications />
            </>
          )}
        </StepCard>

        {/* Step 3 — run your first eval */}
        <StepCard
          n={3}
          title="Run your first eval"
          last
          action={
            <Button
              component={RouterLink}
              href={paths.dashboard.evals}
              size="small"
              variant="outlined"
              sx={ghostBtnSx}
            >
              Open Evals
            </Button>
          }
        >
          <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, color: "text.secondary", mb: 1.5 }}>
            Run an eval to score your outputs for factuality, safety, relevance and 50+ built-in metrics.
          </Typography>
          <CodeBlock label="evaluate.py" copyText={EVAL_SNIPPET}>{EVAL_SNIPPET}</CodeBlock>
        </StepCard>
      </Box>

      {/* Explore */}
      <Typography sx={{ fontFamily: "Inter", fontSize: 16, fontWeight: 500, color: "text.primary", mt: 5, mb: 2 }}>
        Explore Future AGI
      </Typography>
      <Box sx={{ display: "grid", gap: 1.5, gridTemplateColumns: { xs: "1fr", sm: "1fr 1fr", md: "1fr 1fr 1fr" } }}>
        {FEATURES.map((f) => (
          <Box
            key={f.label}
            component={RouterLink}
            href={f.to}
            sx={{
              textDecoration: "none",
              borderRadius: 2,
              border: "1px solid",
              borderColor: "divider",
              bgcolor: (t) => alpha(t.palette.common.white, 0.015),
              p: 2,
              transition: "transform 0.18s, border-color 0.18s, box-shadow 0.18s",
              "&:hover": {
                transform: "translateY(-3px)",
                borderColor: (t) => alpha(t.palette.common.white, 0.28),
                boxShadow: (t) => `0 10px 28px ${alpha(t.palette.common.black, 0.5)}`,
              },
              "&:hover .fx-icon": { color: "common.white", borderColor: (t) => alpha(t.palette.common.white, 0.35) },
            }}
          >
            <Stack direction="row" alignItems="center" spacing={1.25} sx={{ mb: 1 }}>
              <Box
                className="fx-icon"
                sx={{
                  width: 32,
                  height: 32,
                  borderRadius: 1.25,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  color: "text.secondary",
                  bgcolor: (t) => alpha(t.palette.common.white, 0.05),
                  border: "1px solid",
                  borderColor: "divider",
                  transition: "color 0.18s, border-color 0.18s",
                }}
              >
                <Iconify icon={f.icon} width={16} />
              </Box>
              <Typography sx={{ fontFamily: "Inter", fontSize: 14.5, fontWeight: 500, color: "text.primary", flex: 1 }}>
                {f.label}
              </Typography>
              <Iconify icon="solar:arrow-right-up-linear" width={16} sx={{ color: "text.disabled" }} />
            </Stack>
            <Typography sx={{ fontFamily: "Inter", fontSize: 12.5, color: "text.secondary" }}>{f.desc}</Typography>
          </Box>
        ))}
      </Box>
    </Box>
  );
}

// ---- compact "waiting" sonar (monochrome) ---------------------------------
function WaitingForTrace() {
  return (
    <Box
      sx={{
        position: "relative",
        overflow: "hidden",
        mt: 3,
        borderRadius: 1.5,
        border: "1px solid",
        borderColor: "divider",
        bgcolor: (t) => alpha(t.palette.common.white, 0.03),
        px: 2,
        py: 1.5,
      }}
    >
      <Box
        aria-hidden
        sx={{
          position: "absolute",
          inset: 0,
          width: "30%",
          background: `linear-gradient(90deg, transparent, ${alpha("#FFFFFF", 0.05)}, transparent)`,
          animation: `${shimmer} 3.2s ease-in-out infinite`,
        }}
      />
      <Stack direction="row" alignItems="center" spacing={1.5} sx={{ position: "relative" }}>
        <Box sx={{ position: "relative", width: 22, height: 22, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
          {[0, 1].map((i) => (
            <Box
              key={i}
              sx={{
                position: "absolute",
                width: 22,
                height: 22,
                borderRadius: "50%",
                border: "1.5px solid",
                borderColor: (t) => alpha(t.palette.common.white, 0.6),
                animation: `${sonar} 2s ease-out ${i}s infinite`,
              }}
            />
          ))}
          <Box
            sx={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              bgcolor: "common.white",
              boxShadow: (t) => `0 0 8px ${alpha(t.palette.common.white, 0.6)}`,
              animation: `${dot} 1.6s ease-in-out infinite`,
            }}
          />
        </Box>
        <Box>
          <Typography sx={{ fontFamily: "Inter", fontSize: 13.5, fontWeight: 500, color: "text.primary" }}>
            Waiting for your first trace
          </Typography>
          <Typography sx={{ fontFamily: "Inter", fontSize: 12, color: "text.secondary" }}>
            Complete the steps below — it&apos;ll appear here within seconds.
          </Typography>
        </Box>
      </Stack>
    </Box>
  );
}
