// providers/openai-compat.js — OpenAI-compatible driver (OpenCode Go).
//
// This is the OTHER world: ONE OpenAI-compatible baseUrl (OpenCode Go at
// https://opencode.ai/zen/go/v1), Bearer sk- keys, no fingerprint, no gate.
// Every model id the host serves (glm/kimi/deepseek/qwen/minimax/mimo/hy3…)
// goes to the same /chat/completions endpoint. An Anthropic-protocol client is
// handled by translating its request to OpenAI and the response back — the
// host itself has no /v1/messages.
//
// Nothing about FreeModel (fingerprint, cc host, claude-*/gpt-* split) appears
// here. The two drivers share only the ProviderBase plumbing in base.js.
"use strict";

const https = require("https");

const {
  ProviderBase,
  buildOpenAIUpstreamHeaders,
  anthropicToOpenAIRequest,
} = require("./base");

class OpenAICompatProvider extends ProviderBase {
  // parse baseUrl once → {host, port, basePath}
  _endpoint() {
    let u;
    try { u = new URL(this.baseUrl); } catch { u = { hostname: this.baseUrl, port: "", pathname: "/v1", protocol: "https:" }; }
    return {
      host: u.hostname,
      port: u.port ? parseInt(u.port, 10) : (u.protocol === "https:" ? 443 : 80),
      basePath: (u.pathname || "/v1").replace(/\/+$/, "") || "/v1",
    };
  }

  route(clientProto /*, model */) {
    const e = this._endpoint();
    // One endpoint, always OpenAI upstream. Anthropic clients get translated.
    return { host: e.host, port: e.port, path: e.basePath + "/chat/completions",
      upstreamProto: "openai", fingerprint: false, translate: clientProto !== "openai" };
  }

  headersFor(route, bodyBuf, key) {
    return buildOpenAIUpstreamHeaders(bodyBuf, key, route.host, this.ua);
  }

  // Anthropic-protocol client → translate to OpenAI, hit the one baseUrl.
  prepareAnthropicBody(parsed /*, raw */) {
    const oai = anthropicToOpenAIRequest(parsed);
    return { ok: true, bodyBuf: Buffer.from(JSON.stringify(oai)), clientStream: oai.stream === true };
  }

  // OpenAI-protocol client → forward as-is.
  prepareOpenAIBody(oai) {
    return { ok: true, bodyBuf: Buffer.from(JSON.stringify(oai)), clientStream: oai.stream === true };
  }

  listModelsEndpoints() {
    const e = this._endpoint();
    return [{ host: e.host, port: e.port, path: e.basePath + "/models", anthropic: false }];
  }
  // probeKey is the base's probeKeyViaModels (GET /v1/models on baseUrl).
}

module.exports = { OpenAICompatProvider };
