// Real brand logos, bundled as inline data-URIs so they load synchronously.
// The shared minimal <Image> lazy-loader leaves file/CDN logos stuck on a
// blurred placeholder when they resolve too fast (local files load instantly);
// data: URLs sidestep that entirely. Colored brands keep their colour;
// monochrome brands (OpenAI/Anthropic/…) were fetched in white so they show on
// the dark cards. On a real OSS backend the live logoUrl from the API is used.
import openai from "./openai.svg?raw";
import anthropic from "./anthropic.svg?raw";
import gemini from "./gemini.svg?raw";
import vertex from "./vertex.svg?raw";
import azure from "./azure.svg?raw";
import aws from "./aws.svg?raw";
import huggingface from "./huggingface.svg?raw";
import mistral from "./mistral.svg?raw";
import perplexity from "./perplexity.svg?raw";
import nvidia from "./nvidia.svg?raw";
import ollama from "./ollama.svg?raw";
import langchain from "./langchain.svg?raw";
import crewai from "./crewai.svg?raw";
import mcp from "./mcp.svg?raw";
import langgraph from "./langgraph.svg?raw";
import groq from "./groq.svg?raw";
import litellm from "./litellm.svg?raw";
import haystack from "./haystack.svg?raw";
import vercel from "./vercel.svg?raw";
import pydantic from "./pydantic.svg?raw";

const uri = (svg) => `data:image/svg+xml,${encodeURIComponent(svg)}`;

export const LOGOS = {
  openai: uri(openai),
  anthropic: uri(anthropic),
  gemini: uri(gemini),
  vertex: uri(vertex),
  azure: uri(azure),
  aws: uri(aws),
  huggingface: uri(huggingface),
  mistral: uri(mistral),
  perplexity: uri(perplexity),
  nvidia: uri(nvidia),
  ollama: uri(ollama),
  langchain: uri(langchain),
  crewai: uri(crewai),
  mcp: uri(mcp),
  langgraph: uri(langgraph),
  groq: uri(groq),
  litellm: uri(litellm),
  haystack: uri(haystack),
  vercel: uri(vercel),
  pydantic: uri(pydantic),
};
