// providers/freemodel.js — FreeModel driver.
//
// FreeModel is its own world: TWO upstream hosts, a fingerprint gate, and the
// fe_oa_ key pool. Everything specific to that lives HERE and only here — the
// shell never knows about cc.freemodel.dev, the Claude Code fingerprint, or the
// claude-*/gpt-* split. It calls this driver's route()/prepareX()/probeKey().
//
//   claude-* → cc.freemodel.dev (Anthropic Messages, Claude Code fingerprint
//             injected into the body + the matching headers)
//   gpt-*    → api.freemodel.dev (OpenAI Chat Completions, no fingerprint)
//
// Cross-protocol (Anthropic client ↔ gpt model, OpenAI client ↔ claude model)
// is handled by translating the request body and picking the responder through
// forwardWithRetry; the route decides the upstream, the body-prep decides the
// shape. Both are this driver's job.
"use strict";

const https = require("https");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const {
  ProviderBase,
  buildAnthropicUpstreamHeaders, buildOpenAIUpstreamHeaders,
  anthropicToOpenAIRequest, oaiToAnthropic,
  pushLog,
} = require("./base");

// ─── Claude Code fingerprint ─────────────────────────────────────────────
// cc.freemodel.dev validates the request BODY shape, not TLS or key. The
// required values are the ones the official Claude Code client sends, captured
// via mitmproxy. They live in a profile file (refreshable without code edits).
const FINGERPRINT_DEFAULTS = {
  billingHeader: "x-anthropic-billing-header: cc_version=2.1.179.efd; cc_entrypoint=sdk-cli; cch=fa71b;",
  agentHeader: "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
  userAgent: "claude-cli/2.1.179 (external, sdk-cli)",
  anthropicBeta: "claude-code-20250219,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24",
  anthropicVersion: "2023-06-01",
  capturedFrom: "claude-cli/2.1.179",
};
function loadFingerprint(file) {
  let fp = { ...FINGERPRINT_DEFAULTS };
  try { Object.assign(fp, JSON.parse(fs.readFileSync(file, "utf8"))); } catch {}
  for (const k of Object.keys(FINGERPRINT_DEFAULTS)) {
    const env = process.env["FMCC_FP_" + k.replace(/([A-Z])/g, "_$1").toUpperCase()];
    if (env != null) fp[k] = env;
  }
  return fp;
}

// Inject the Claude Code fingerprint into an Anthropic Messages body. Returns
// the original client stream preference so the response can be reassembled for
// non-streaming clients (the gate forces stream:true upstream).
function injectFingerprint(bodyStr, fp, deviceId, sessionId) {
  let d;
  try { d = JSON.parse(bodyStr); } catch { return { ok: false, body: bodyStr }; }
  const clientStream = d.stream === true;
  d.stream = true; // gate requires streaming
  const sys = Array.isArray(d.system)
    ? d.system
    : typeof d.system === "string" ? [{ type: "text", text: d.system }] : [];
  d.system = [{ type: "text", text: fp.billingHeader }, { type: "text", text: fp.agentHeader }, ...sys];
  d.metadata = { user_id: JSON.stringify({ device_id: deviceId, account_uuid: "", session_id: sessionId }) };
  if (d.max_tokens == null) d.max_tokens = 4096;
  return { ok: true, body: JSON.stringify(d), model: d.model, clientStream };
}

function isClaude(id) { return !!id && /^claude/i.test(id); }
function isGpt(id) { return !!id && /^gpt-/i.test(id); }

class FreeModelProvider extends ProviderBase {
  constructor(config, opts) {
    super(config, opts);
    this.fp = opts.fp || loadFingerprint(opts.fingerprintFile);
    this.deviceId = opts.deviceId || crypto.randomBytes(32).toString("hex");
    this.sessionId = opts.sessionId || crypto.randomUUID();
  }

  route(clientProto, model) {
    if (isClaude(model)) {
      return { host: this.claudeUpstream, port: 443, path: "/v1/messages?beta=true",
        upstreamProto: "anthropic", fingerprint: !!this.needsFingerprint, translate: clientProto !== "anthropic" };
    }
    return { host: this.openaiUpstream, port: 443, path: "/v1/chat/completions",
      upstreamProto: "openai", fingerprint: false, translate: clientProto !== "openai" };
  }

  headersFor(route, bodyBuf, key) {
    if (route.upstreamProto === "anthropic")
      return buildAnthropicUpstreamHeaders(bodyBuf, key, route.host, this.ua, this.fp);
    return buildOpenAIUpstreamHeaders(bodyBuf, key, route.host, this.ua);
  }

  // Anthropic-protocol client → FreeModel.
  prepareAnthropicBody(parsed, raw) {
    if (isClaude(parsed.model)) {
      // claude-* → fingerprint + cc host (Anthropic-shape upstream)
      const inj = injectFingerprint(raw, this.fp, this.deviceId, this.sessionId);
      if (!inj.ok) return { ok: false, error: "body is not valid JSON" };
      return { ok: true, bodyBuf: Buffer.from(inj.body), clientStream: inj.clientStream };
    }
    // gpt-* → translate Anthropic→OpenAI, hit api host
    const oai = anthropicToOpenAIRequest(parsed);
    return { ok: true, bodyBuf: Buffer.from(JSON.stringify(oai)), clientStream: oai.stream === true };
  }

  // OpenAI-protocol client → FreeModel.
  prepareOpenAIBody(oai) {
    if (isGpt(oai.model)) {
      // gpt-* → forward OpenAI body as-is to api host
      return { ok: true, bodyBuf: Buffer.from(JSON.stringify(oai)), clientStream: oai.stream === true };
    }
    // claude-* → translate OpenAI→Anthropic, fingerprint, cc host
    const ant = oaiToAnthropic(oai);
    const inj = injectFingerprint(JSON.stringify(ant), this.fp, this.deviceId, this.sessionId);
    if (!inj.ok) return { ok: false, error: "failed to translate request" };
    return { ok: true, bodyBuf: Buffer.from(inj.body), clientStream: oai.stream === true };
  }

  listModelsEndpoints() {
    return [
      { host: this.claudeUpstream, port: 443, path: "/v1/models", anthropic: true },
      { host: this.openaiUpstream, port: 443, path: "/v1/models", anthropic: false },
    ];
  }
  // probeKey is the base's probeKeyViaModels (GET /v1/models on the cc host).
  // The cc /v1/models endpoint is open (no fingerprint gate) — confirmed in
  // the skill — so it validates the key without any generation cost.
}

module.exports = { FreeModelProvider, loadFingerprint, injectFingerprint, FINGERPRINT_DEFAULTS };
