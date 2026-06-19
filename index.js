// freemodel-cc-proxy
// Local Anthropic Messages API backed by FreeModel's cc.freemodel.dev, which
// serves real Claude (Opus/Sonnet/Haiku) but gates access to the official
// Claude Code client. This proxy impersonates that client's request fingerprint
// so any Anthropic-compatible tool (Factory Droid, Claude Code, Cline, Cursor,
// Anthropic SDK) can use FreeModel's Claude through one local endpoint.
//
// It also speaks the OpenAI Chat Completions protocol (/v1/chat/completions):
// requests are translated OpenAI→Anthropic, responses back Anthropic→OpenAI,
// streaming and non-streaming. So OpenAI-only clients work too.
//
// The upstream gate forces stream:true, so even when a client asks for a single
// JSON object (stream:false / OpenAI non-stream), the proxy buffers the SSE and
// reassembles one JSON object before returning it.
//
// Run:  FMCC_KEY=fe_oa_<...> node index.js
// API:  http://127.0.0.1:11440  (/v1/messages, /v1/chat/completions, /v1/models)
// UI:   http://127.0.0.1:11440/  (status, models, test, logs)
"use strict";

const http = require("http");
const https = require("https");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const os = require("os");

// ─── config ──────────────────────────────────────────────────────────────
const CONFIG_DIR = process.env.FMCC_CONFIG_DIR || path.join(os.homedir(), ".freemodel-cc-proxy");
const CONFIG_FILE = path.join(CONFIG_DIR, "config.json");
const LOG_FILE = process.env.FMCC_LOG_FILE || path.join(CONFIG_DIR, "proxy.log");
const UI_FILE = path.join(__dirname, "ui.html");

function loadConfig() {
  const cfg = { port: 11440 };
  try { Object.assign(cfg, JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"))); } catch {}
  if (process.env.FMCC_PORT) cfg.port = parseInt(process.env.FMCC_PORT, 10);
  return cfg;
}
const CFG = loadConfig();

// ─── providers (multi-provider: switch the active one from the UI/API) ─────
// A provider is a named LLM gateway this proxy can front. Exactly one is
// "active" at a time; all /v1/* traffic flows through it. Two kinds:
//   - "freemodel":    two upstream hosts (cc.freemodel.dev for claude-* with
//                     fingerprint gate, api.freemodel.dev for gpt-*), one
//                     fe_oa_ key pool, needs the Claude Code fingerprint.
//   - "openai-compat": one OpenAI-compatible baseUrl (e.g. opencode.ai/zen/go/v1),
//                     any model id, Bearer sk-... keys, no fingerprint. claude-* is
//                     served by translating Anthropic<->OpenAI in the proxy.
// State persists in providers.json + per-provider keys-<id>.json. The profile
// enable toggle (global on/off) lives on top, separate from which provider
// is active.
const PROVIDERS_FILE = path.join(CONFIG_DIR, "providers.json");
const PROFILE_FILE = path.join(CONFIG_DIR, "profile.json");
const LEGACY_KEYS_FILE = path.join(CONFIG_DIR, "keys.json");

function defaultProviders() {
  // First-run defaults seed the two providers the user actually has. If legacy
  // config.json/profile.json exist, their upstream values win for freemodel.
  const legacyCfg = (() => { try { return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")); } catch { return {}; } })();
  const legacyProf = (() => { try { return JSON.parse(fs.readFileSync(PROFILE_FILE, "utf8")); } catch { return null; } })();
  const claudeUp = legacyProf?.claudeUpstream || legacyCfg.upstream || "cc.freemodel.dev";
  const openaiUp = legacyProf?.openaiUpstream || legacyCfg.upstreamOpenai || "api.freemodel.dev";
  return [
    {
      id: "freemodel", name: "FreeModel", kind: "freemodel", enabled: true,
      claudeUpstream: claudeUp, openaiUpstream: openaiUp,
      keyPrefix: "fe_oa_", needsFingerprint: true,
      note: "Real Claude (cc.freemodel.dev, fingerprint-gated) + GPT (api.freemodel.dev) via one fe_oa key pool.",
    },
    {
      id: "opencode", name: "OpenCode Go", kind: "openai-compat", enabled: true,
      baseUrl: "https://opencode.ai/zen/go/v1",
      keyPrefix: "sk-", needsFingerprint: false, probeModel: "glm-5.2",
      note: "OpenCode Go subscription — glm / kimi / deepseek / qwen / minimax / mimo. One OpenAI-compatible endpoint, Bearer sk- key.",
    },
  ];
}
function loadProviders() {
  let d = { activeId: "freemodel", enabled: true, providers: defaultProviders() };
  let loaded = null;
  try { loaded = JSON.parse(fs.readFileSync(PROVIDERS_FILE, "utf8")); } catch {}
  if (loaded) {
    if (typeof loaded.activeId === "string") d.activeId = loaded.activeId;
    if (typeof loaded.enabled === "boolean") d.enabled = loaded.enabled;
    if (Array.isArray(loaded.providers) && loaded.providers.length) d.providers = loaded.providers;
  }
  if (process.env.FMCC_PROFILE_ENABLED === "0" || process.env.FMCC_PROFILE_ENABLED === "false") d.enabled = false;
  // legacy profile.json had an `enabled` field for the single profile; migrate it
  // only if providers.json wasn't loaded (first upgrade).
  if (!loaded) {
    try { const p = JSON.parse(fs.readFileSync(PROFILE_FILE, "utf8")); if (typeof p.enabled === "boolean") d.enabled = p.enabled; } catch {}
  }
  return d;
}
let PROVIDERS = loadProviders();
function persistProviders() {
  try { fs.mkdirSync(CONFIG_DIR, { recursive: true }); fs.writeFileSync(PROVIDERS_FILE, JSON.stringify(PROVIDERS, null, 2) + "\n"); } catch {}
}
function activeProvider() { return PROVIDERS.providers.find((p) => p.id === PROVIDERS.activeId) || PROVIDERS.providers[0]; }
function providerById(id) { return PROVIDERS.providers.find((p) => p.id === id); }
// Global enable toggle ("profile enabled") — independent of which provider is
// active. When off, /v1/* returns 503; UI + /api/* stay up.
function profileEnabled() { return PROVIDERS.enabled !== false; }

// One-time migration from the legacy single-provider layout: copy keys.json ->
// keys-freemodel.json, and if the user gave us opencode keys out-of-band seed
// keys-opencode.json. Idempotent (skips if target exists).
function migrateLegacyKeys() {
  try {
    const fmPath = path.join(CONFIG_DIR, "keys-freemodel.json");
    if (!fs.existsSync(fmPath) && fs.existsSync(LEGACY_KEYS_FILE)) {
      const j = JSON.parse(fs.readFileSync(LEGACY_KEYS_FILE, "utf8"));
      if (Array.isArray(j.keys) && j.keys.length) {
        fs.writeFileSync(fmPath, JSON.stringify({ keys: j.keys }, null, 2) + "\n");
      }
    }
  } catch {}
}
migrateLegacyKeys();

// ─── per-provider key pools ───────────────────────────────────────────────
// Each provider has its own key pool, its own rotation cursor, and its own
// stats. All keyed by provider id so concurrent requests to different
// providers don't interfere.
const POOLS = {};        // id -> { keys: [], idx: 0 }
const keyStats = {};     // id -> { [key]: {ok,limited,bad,count,lastStatus,lastUsed} }
function keysFileFor(id) { return path.join(CONFIG_DIR, "keys-" + id + ".json"); }
function loadPool(id) {
  let keys = [];
  try { const j = JSON.parse(fs.readFileSync(keysFileFor(id), "utf8")); if (Array.isArray(j.keys)) keys = j.keys.filter((k) => typeof k === "string" && k); } catch {}
  // bootstrap freemodel from legacy single key (env FMCC_KEY / config.json)
  if (!keys.length && id === "freemodel") {
    try { const c = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")); if (c.key) keys = [c.key]; } catch {}
    if (!keys.length && process.env.FMCC_KEY) keys = [process.env.FMCC_KEY];
  }
  return { keys, idx: 0 };
}
function poolOf(id) {
  if (!POOLS[id]) {
    POOLS[id] = loadPool(id);
    keyStats[id] = {};
    POOLS[id].keys.forEach((k) => ensureStats(id, k));
  }
  return POOLS[id];
}
function persistPool(id) {
  try { fs.mkdirSync(CONFIG_DIR, { recursive: true }); fs.writeFileSync(keysFileFor(id), JSON.stringify({ keys: POOLS[id].keys }, null, 2) + "\n"); } catch {}
}
// Rotate to another key only on auth/rate-limit errors. 5xx is an upstream
// outage, not a key problem — rotating would burn the whole pool in one
// request and mark every key "limited" for nothing. 5xx is returned to the
// client as-is (normalized to the right error shape).
const ROTATE_STATUSES = new Set([401, 402, 403, 429]);
function ensureStats(id, k) { if (!keyStats[id]) keyStats[id] = {}; if (!keyStats[id][k]) keyStats[id][k] = { ok: 0, limited: 0, bad: false, count: 0, lastStatus: null, lastUsed: null }; return keyStats[id][k]; }
function maskKey(k) { return k ? k.slice(0, 8) + "…" + k.slice(-4) : "(none)"; }
function currentKey(id) { const p = poolOf(id); return p.keys[p.idx]; }
function reloadKeyStats(id) { const p = poolOf(id); p.keys.forEach((k) => ensureStats(id, k)); }
// Per-request cursor advance: returns the next non-bad key index after `from`,
// WITHOUT mutating the shared pool.idx. Each request snapshots idx, walks the
// pool on a local cursor, and only commits idx onto a key that returned 200.
function nextUsable(id, from) {
  const p = poolOf(id);
  if (p.keys.length === 0) return from;
  for (let i = 0; i < p.keys.length; i++) {
    const idx = (from + 1 + i) % p.keys.length;
    const s = keyStats[id][p.keys[idx]];
    if (!s || !s.bad) return idx;
  }
  return (from + 1) % p.keys.length; // all bad — just step forward
}

// Sequential forward health probe for a provider's pool: find the FIRST
// working key, lock the cursor onto it. Probe shape depends on provider kind:
// freemodel -> fingerprinted /v1/messages on cc host; openai-compat ->
// /chat/completions on baseUrl. Stops at first 200, skips known bad/limited.
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function findFirstWorking(providerId) {
  const p = providerById(providerId) || activeProvider();
  const pool = poolOf(p.id);
  if (!pool.keys.length) return { ok: false, error: "empty pool" };
  const start = pool.idx;
  const tried = [];
  for (let step = 0; step < pool.keys.length; step++) {
    const i = (start + step) % pool.keys.length;
    const key = pool.keys[i];
    const s = ensureStats(p.id, key);
    if (s.bad) { tried.push({ index: i, masked: maskKey(key), status: "skip(bad)" }); continue; }
    if (s.limited) { tried.push({ index: i, masked: maskKey(key), status: "skip(limited×" + s.limited + ")" }); continue; }
    const res = await probeProviderOnce(p, key);
    s.lastStatus = res.status; s.lastUsed = new Date().toISOString(); s.count++;
    if (res.status === 200) { s.ok++; pool.idx = i; return { ok: true, current: i, masked: maskKey(key), probed: step + 1, tried }; }
    if (res.status === 401 || res.status === 403) s.bad = true; else if (ROTATE_STATUSES.has(res.status)) s.limited++;
    tried.push({ index: i, masked: maskKey(key), status: res.status || "neterr" });
    await sleep(400);
  }
  return { ok: false, current: pool.idx, probed: tried.length, tried, error: "no working key in pool" };
}

// Pre-flight: ensure the active provider has at least one key. If not, point
// the user at the UI/API. (Other providers may still be empty — that's fine,
// they're not active.)
{
  const ap = activeProvider();
  if (ap && !poolOf(ap.id).keys.length) {
    console.error("Active provider '" + ap.id + "' has no keys. Add some:");
    console.error("  UI:  http://127.0.0.1:" + CFG.port + "/  -> Key pool -> Add");
    console.error("  API: POST /api/keys { keys: [\"" + (ap.keyPrefix || "") + "…\"] }");
    console.error("  CLI: node keys.js add " + (ap.keyPrefix || "") + "...");
  }
}

// ─── fingerprint (client-impersonation profile, externalized) ────────────
// cc.freemodel.dev validates the request BODY shape, not TLS or key. The
// required values are the ones the official Claude Code client sends, captured
// via mitmproxy. They are NOT hardcoded in code — they live in a profile file
// so they can be refreshed after a FreeModel/Claude-Code update without
// touching index.js. Load order: env FMCC_FINGERPRINT_FILE →
// ~/.freemodel-cc-proxy/fingerprint.json → built-in defaults below.
const FINGERPRINT_FILE = process.env.FMCC_FINGERPRINT_FILE || path.join(CONFIG_DIR, "fingerprint.json");
const FINGERPRINT_DEFAULTS = {
  // The leading system block that carries the Claude Code billing/identity tag.
  billingHeader: "x-anthropic-billing-header: cc_version=2.1.179.efd; cc_entrypoint=sdk-cli; cch=fa71b;",
  // The second leading system block.
  agentHeader: "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
  // User-Agent the gate expects.
  userAgent: "claude-cli/2.1.179 (external, sdk-cli)",
  // anthropic-beta header value.
  anthropicBeta: "claude-code-20250219,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24",
  // anthropic-version header value.
  anthropicVersion: "2023-06-01",
  // captured from Claude Code 2.1.179; bump when FreeModel tightens the gate.
  capturedFrom: "claude-cli/2.1.179",
};
function loadFingerprint() {
  let fp = { ...FINGERPRINT_DEFAULTS };
  try { Object.assign(fp, JSON.parse(fs.readFileSync(FINGERPRINT_FILE, "utf8"))); } catch {}
  // env overrides for individual fields
  for (const k of Object.keys(FINGERPRINT_DEFAULTS)) {
    const env = process.env["FMCC_FP_" + k.replace(/([A-Z])/g, "_$1").toUpperCase()];
    if (env != null) fp[k] = env;
  }
  return fp;
}
const FP = loadFingerprint();
const BILLING_HDR = FP.billingHeader;
const AGENT_HDR = FP.agentHeader;
const UA = FP.userAgent;
const ANTHROPIC_BETA = FP.anthropicBeta;
const DEVICE_ID = crypto.randomBytes(32).toString("hex");
const SESSION_ID = crypto.randomUUID();

// Injects the Claude Code fingerprint into an Anthropic Messages body.
// Returns the original client stream preference so the response can be
// reassembled for non-streaming clients (upstream always streams).
function injectFingerprint(bodyStr) {
  let d;
  try { d = JSON.parse(bodyStr); } catch { return { ok: false, body: bodyStr }; }
  const clientStream = d.stream === true;
  d.stream = true; // gate requires streaming
  const sys = Array.isArray(d.system)
    ? d.system
    : typeof d.system === "string" ? [{ type: "text", text: d.system }] : [];
  d.system = [{ type: "text", text: BILLING_HDR }, { type: "text", text: AGENT_HDR }, ...sys];
  d.metadata = {
    user_id: JSON.stringify({ device_id: DEVICE_ID, account_uuid: "", session_id: SESSION_ID }),
  };
  if (d.max_tokens == null) d.max_tokens = 4096;
  return { ok: true, body: JSON.stringify(d), model: d.model, clientStream };
}

// ─── SSE collector: reassemble a non-stream Anthropic Message from SSE ────
function collectAnthropicMessage(sseText) {
  const blocks = {};      // content-block index -> block being assembled
  const order = [];        // block indices in arrival order
  const msg = {
    id: null, type: "message", role: "assistant", model: null,
    content: [], stop_reason: null, stop_sequence: null,
    usage: { input_tokens: 0, output_tokens: 0 },
  };
  let hadError = null;
  for (const rawLine of sseText.split("\n")) {
    const line = rawLine.trim();
    if (!line.startsWith("data:")) continue;
    const payload = line.slice(5).trim();
    if (!payload || payload === "[DONE]") continue;
    let ev;
    try { ev = JSON.parse(payload); } catch { continue; }
    switch (ev.type) {
      case "message_start":
        if (ev.message) {
          msg.id = ev.message.id;
          msg.model = ev.message.model;
          if (ev.message.usage) {
            msg.usage.input_tokens = ev.message.usage.input_tokens || 0;
            msg.usage.output_tokens = ev.message.usage.output_tokens || 0;
          }
        }
        break;
      case "content_block_start": {
        if (ev.index == null) break;
        const b = ev.content_block || {};
        const block = { type: b.type || "text" };
        if (block.type === "text") block.text = b.text || "";
        else if (block.type === "tool_use") { block.id = b.id; block.name = b.name; block.input = b.input || {}; block._raw = ""; }
        else if (block.type === "thinking") { block.thinking = b.thinking || ""; block._raw = ""; }
        else Object.assign(block, b);
        blocks[ev.index] = block;
        order.push(ev.index);
        break;
      }
      case "content_block_delta": {
        if (ev.index == null || !blocks[ev.index] || !ev.delta) break;
        const block = blocks[ev.index];
        const dt = ev.delta.type;
        if (dt === "text_delta") block.text += ev.delta.text || "";
        else if (dt === "input_json_delta") block._raw = (block._raw || "") + (ev.delta.partial_json || "");
        else if (dt === "thinking_delta") block.thinking += ev.delta.thinking || "";
        else if (dt === "signature_delta") block.signature = (block.signature || "") + (ev.delta.signature || "");
        break;
      }
      case "content_block_stop": {
        const block = blocks[ev.index];
        if (!block) break;
        if (block.type === "tool_use" && block._raw != null) {
          try { block.input = block._raw ? JSON.parse(block._raw) : {}; } catch { block.input = {}; }
          delete block._raw;
        } else if (block._raw != null) {
          delete block._raw;
        }
        break;
      }
      case "message_delta":
        if (ev.delta) {
          if (ev.delta.stop_reason != null) msg.stop_reason = ev.delta.stop_reason;
          if (ev.delta.stop_sequence != null) msg.stop_sequence = ev.delta.stop_sequence;
        }
        if (ev.usage && ev.usage.output_tokens != null) msg.usage.output_tokens = ev.usage.output_tokens;
        break;
      case "error":
        hadError = ev.error || ev;
        break;
      default: break; // ping, message_stop, etc.
    }
  }
  if (hadError) return { ok: false, error: hadError };
  msg.content = order.map((i) => {
    const b = blocks[i];
    return b.type === "thinking" ? { type: "thinking", thinking: b.thinking, signature: b.signature || "" } : b;
  });
  return { ok: true, message: msg };
}

// ─── OpenAI Chat Completions ⇄ Anthropic Messages translation ────────────
function oaiToAnthropic(oai) {
  const out = {
    model: oai.model || "claude-opus-4-8",
    max_tokens: oai.max_tokens || oai.max_completion_tokens || 4096,
    messages: [],
    stream: oai.stream === true,
  };
  if (oai.temperature != null) out.temperature = oai.temperature;
  if (oai.top_p != null) out.top_p = oai.top_p;
  if (Array.isArray(oai.stop)) out.stop_sequences = oai.stop;
  else if (typeof oai.stop === "string") out.stop_sequences = [oai.stop];

  const systemParts = [];
  const conv = [];
  for (const m of oai.messages || []) {
    if (m.role === "system") {
      const t = typeof m.content === "string"
        ? m.content
        : (Array.isArray(m.content) ? m.content.map((c) => c.text || "").join("") : "");
      if (t) systemParts.push(t);
    } else conv.push(m);
  }
  if (systemParts.length) out.system = systemParts.join("\n\n");
  out.messages = oaiMessagesToAnthropic(conv);
  if (Array.isArray(oai.tools)) {
    out.tools = oai.tools
      .filter((t) => t && (t.function || t))
      .map((t) => {
        const fn = t.function || t;
        return {
          name: fn.name,
          description: fn.description || "",
          input_schema: fn.parameters || fn.input_schema || { type: "object", properties: {} },
        };
      });
  }
  if (oai.tool_choice) {
    if (oai.tool_choice === "auto" || oai.tool_choice === "none") out.tool_choice = { type: oai.tool_choice };
    else if (oai.tool_choice === "required") out.tool_choice = { type: "any" };
    else if (typeof oai.tool_choice === "object" && (oai.tool_choice.function || oai.tool_choice.name))
      out.tool_choice = { type: "tool", name: (oai.tool_choice.function || {}).name || oai.tool_choice.name };
  }
  return mergeAdjacentRoles(out);
}

function oaiMessagesToAnthropic(msgs) {
  const out = [];
  for (const m of msgs) {
    if (m.role === "user" || m.role === "assistant") {
      if (m.role === "assistant" && Array.isArray(m.tool_calls) && m.tool_calls.length) {
        const blocks = [];
        const txt = typeof m.content === "string"
          ? m.content
          : (Array.isArray(m.content) ? m.content.map((c) => c.text || "").join("") : "");
        if (txt) blocks.push({ type: "text", text: txt });
        for (const tc of m.tool_calls) {
          const fn = tc.function || {};
          let input = {};
          try { input = fn.arguments ? JSON.parse(fn.arguments) : {}; } catch { input = {}; }
          blocks.push({ type: "tool_use", id: tc.id, name: fn.name, input });
        }
        out.push({ role: "assistant", content: blocks });
      } else {
        out.push({ role: m.role, content: oaiContentToAnthropic(m.content) });
      }
    } else if (m.role === "tool") {
      out.push({
        role: "user",
        content: [{
          type: "tool_result",
          tool_use_id: m.tool_call_id,
          content: typeof m.content === "string" ? m.content : JSON.stringify(m.content),
        }],
      });
    }
  }
  return out;
}

function oaiContentToAnthropic(content) {
  if (typeof content === "string") return [{ type: "text", text: content }];
  if (Array.isArray(content)) {
    return content.map((part) => {
      if (part.type === "text") return { type: "text", text: part.text || "" };
      if (part.type === "image_url") {
        const url = (part.image_url && part.image_url.url) || "";
        const m = url.match(/^data:([^;]+);base64,(.*)$/);
        if (m) return { type: "image", source: { type: "base64", media_type: m[1], data: m[2] } };
        return { type: "image", source: { type: "url", url } };
      }
      return null;
    }).filter(Boolean);
  }
  return [{ type: "text", text: "" }];
}

// Anthropic requires alternating user/assistant roles; merge adjacent same-role messages.
function mergeAdjacentRoles(out) {
  const msgs = [];
  for (const m of out.messages) {
    const prev = msgs[msgs.length - 1];
    if (prev && prev.role === m.role) {
      const a = Array.isArray(prev.content) ? prev.content : [{ type: "text", text: String(prev.content) }];
      const b = Array.isArray(m.content) ? m.content : [{ type: "text", text: String(m.content) }];
      prev.content = a.concat(b);
    } else {
      msgs.push({ role: m.role, content: m.content });
    }
  }
  out.messages = msgs;
  return out;
}

function mapFinishReason(sr) {
  if (sr === "end_turn" || sr === "stop_sequence") return "stop";
  if (sr === "tool_use" || sr === "tool_calls") return "tool_calls";
  if (sr === "max_tokens") return "length";
  return sr || "stop";
}

function anthropicMessageToOpenAI(msg, model) {
  const id = "chatcmpl-" + (msg.id || crypto.randomUUID().replace(/-/g, "").slice(0, 24));
  const choice = { index: 0, message: { role: "assistant", content: "" }, finish_reason: mapFinishReason(msg.stop_reason) };
  const toolCalls = [];
  for (const block of msg.content || []) {
    if (block.type === "text") choice.message.content += block.text || "";
    else if (block.type === "tool_use") {
      toolCalls.push({ id: block.id, type: "function", function: { name: block.name, arguments: JSON.stringify(block.input || {}) } });
    }
  }
  if (toolCalls.length) {
    choice.message.tool_calls = toolCalls;
    if (!choice.message.content) choice.message.content = null;
  }
  const it = (msg.usage && msg.usage.input_tokens) || 0;
  const ot = (msg.usage && msg.usage.output_tokens) || 0;
  return {
    id, object: "chat.completion", created: Math.floor(Date.now() / 1000), model: model || msg.model || "claude",
    choices: [choice],
    usage: { prompt_tokens: it, completion_tokens: ot, total_tokens: it + ot },
  };
}

// ─── logging (ring buffer + disk) ─────────────────────────────────────────
const LOGS = [];
const MAX_LOGS = 200;
function pushLog(entry) {
  entry.ts = new Date().toISOString();
  LOGS.push(entry);
  if (LOGS.length > MAX_LOGS) LOGS.shift();
  try {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, `[${entry.ts}] ${entry.kind} ${entry.method} ${entry.path} ${entry.status || ""} ${entry.note || ""}\n`);
  } catch {}
}

// ─── routing: map (provider, client proto, model) -> upstream endpoint ────
// One place that decides where a request goes and whether it needs
// translation/fingerprint. Keeps the handlers free of provider/kind branches.
function isClaudeModel(id) { return !!id && /^claude/i.test(id); }
function isGptModel(id) { return !!id && /^gpt-/i.test(id); }

function openAICompatEndpoint(p) {
  // baseUrl like https://opencode.ai/zen/go/v1 -> {host, port, basePath}
  let u;
  try { u = new URL(p.baseUrl); } catch { u = { hostname: p.baseUrl, port: "", pathname: "/v1", protocol: "https:" }; }
  return {
    host: u.hostname,
    port: u.port ? parseInt(u.port, 10) : (u.protocol === "https:" ? 443 : 80),
    basePath: (u.pathname || "/v1").replace(/\/+$/, "") || "/v1",
  };
}
// routeRequest -> { host, port, path, upstreamProto, fingerprint, translate }
//   upstreamProto: "anthropic" | "openai"  — what the upstream endpoint speaks
//   translate:      true if client proto != upstream proto (proxy must translate)
//   fingerprint:    true if the Claude Code fingerprint must be injected (freemodel cc)
function routeRequest(p, clientProto, model) {
  if (p.kind === "freemodel") {
    if (isClaudeModel(model)) {
      return { host: p.claudeUpstream, port: 443, path: "/v1/messages?beta=true", upstreamProto: "anthropic", fingerprint: !!p.needsFingerprint, translate: clientProto !== "anthropic" };
    }
    return { host: p.openaiUpstream, port: 443, path: "/v1/chat/completions", upstreamProto: "openai", fingerprint: false, translate: clientProto !== "openai" };
  }
  // openai-compat: one baseUrl, always OpenAI upstream; claude-* is served by
  // translating Anthropic->OpenAI in the proxy (the host has no /v1/messages).
  const e = openAICompatEndpoint(p);
  return { host: e.host, port: e.port, path: e.basePath + "/chat/completions", upstreamProto: "openai", fingerprint: false, translate: clientProto !== "openai" };
}
function modelsEndpoints(p) {
  // returns array of {host, port, path} to fetch /models from for this provider
  if (p.kind === "freemodel") {
    return [
      { host: p.claudeUpstream, port: 443, path: "/v1/models", anthropic: true },
      { host: p.openaiUpstream, port: 443, path: "/v1/models", anthropic: false },
    ];
  }
  const e = openAICompatEndpoint(p);
  return [{ host: e.host, port: e.port, path: e.basePath + "/models", anthropic: false }];
}

// ─── upstream request helpers ────────────────────────────────────────────
// Headers for an Anthropic-shape upstream (freemodel cc host). Carries the
// Claude Code fingerprint markers the gate checks. key goes both as x-api-key
// and Authorization: Bearer (cc.freemodel.dev accepts either).
function buildAnthropicUpstreamHeaders(bodyBuf, key, host) {
  const headers = {
    "host": host,
    "user-agent": UA,
    "x-app": "cli",
    "anthropic-dangerous-direct-browser-access": "true",
    "anthropic-version": FP.anthropicVersion,
    "anthropic-beta": ANTHROPIC_BETA,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  if (key) { headers["x-api-key"] = key; headers["authorization"] = "Bearer " + key; }
  return headers;
}
// Headers for an OpenAI-shape upstream (freemodel api host OR openai-compat
// baseUrl). Bearer only.
function buildOpenAIUpstreamHeaders(bodyBuf, key, host) {
  const headers = {
    "host": host,
    "user-agent": UA,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  if (key) headers["authorization"] = "Bearer " + key;
  return headers;
}
function buildHeadersForRoute(route, bodyBuf, key) {
  return route.upstreamProto === "anthropic"
    ? buildAnthropicUpstreamHeaders(bodyBuf, key, route.host)
    : buildOpenAIUpstreamHeaders(bodyBuf, key, route.host);
}

// Probe one key against a provider with the right shape for its kind. Used by
// findFirstWorking. Minimal request (max_tokens:1).
function probeProviderOnce(p, key) {
  return new Promise((resolve) => {
    if (p.kind === "freemodel") {
      const body = Buffer.from(injectFingerprint(JSON.stringify({
        model: "claude-haiku-4-5-20251001", max_tokens: 1, stream: false,
        messages: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
      })).body);
      const up = https.request({ host: p.claudeUpstream, port: 443, method: "POST", path: "/v1/messages?beta=true", headers: buildAnthropicUpstreamHeaders(body, key, p.claudeUpstream) }, (upres) => {
        upres.resume(); upres.on("end", () => resolve({ status: upres.statusCode || 0 }));
      });
      up.on("error", () => resolve({ status: 0 }));
      up.setTimeout(15000, () => { try { up.destroy(); } catch {} resolve({ status: 0 }); });
      up.write(body); up.end();
      return;
    }
    // openai-compat: tiny chat completion on the baseUrl
    const e = openAICompatEndpoint(p);
    const probeModel = p.probeModel || "glm-5.2";
    const body = Buffer.from(JSON.stringify({ model: probeModel, max_tokens: 1, stream: false, messages: [{ role: "user", content: "hi" }] }));
    const up = https.request({ host: e.host, port: e.port, method: "POST", path: e.basePath + "/chat/completions", headers: buildOpenAIUpstreamHeaders(body, key, e.host) }, (upres) => {
      upres.resume(); upres.on("end", () => resolve({ status: upres.statusCode || 0 }));
    });
    up.on("error", () => resolve({ status: 0 }));
    up.setTimeout(15000, () => { try { up.destroy(); } catch {} resolve({ status: 0 }); });
    up.write(body); up.end();
  });
}

// ─── response safety + error normalization ───────────────────────────────
// cres can throw if the client already disconnected; never let that crash the
// process. Every client write goes through these.
function safeWriteHead(cres, code, headers) { try { cres.writeHead(code, headers || {}); return true; } catch { return false; } }
function safeEnd(cres, buf) { try { if (buf != null) cres.end(buf); else cres.end(); } catch {} }
function safeWrite(cres, chunk) { try { cres.write(chunk); return true; } catch { return false; } }

// Strip hop-by-hop headers from an upstream response before forwarding. Node
// re-frames the body (chunked/content-length) for the client; copying the
// upstream's transfer-encoding/connection/content-length verbatim can confuse
// it (duplicate/contradictory framing).
function filterHeaders(hdrs) {
  const out = {};
  for (const [k, v] of Object.entries(hdrs || {})) {
    const lk = k.toLowerCase();
    if (lk === "connection" || lk === "keep-alive" || lk === "transfer-encoding" ||
        lk === "content-length" || lk === "host") continue;
    out[k] = v;
  }
  return out;
}

// Normalize an upstream error body into the shape each client SDK expects.
// cc/api.freemodel.dev send `{"error":"Unauthorized - Invalid token"}` (a
// STRING, not an object), which breaks both Anthropic and OpenAI SDK error
// parsers. These always return the canonical shape for the wire protocol.
function normalizeAnthropicError(upstreamText, status) {
  let message = upstreamText, type = "api_error";
  try {
    const j = JSON.parse(upstreamText);
    if (j && j.error) {
      if (typeof j.error === "string") message = j.error;
      else { message = j.error.message || JSON.stringify(j.error); type = j.error.type || type; }
    } else if (typeof j === "string") message = j;
  } catch {}
  if (status === 401 || status === 403) type = "authentication_error";
  else if (status === 429) type = "rate_limit_error";
  else if (status >= 500) type = "api_error";
  else if (status >= 400) type = "invalid_request_error";
  return { type: "error", error: { type, message } };
}
function normalizeOpenAIError(upstreamText, status) {
  let message = upstreamText, type = "upstream_error", code = status;
  try {
    const j = JSON.parse(upstreamText);
    if (j && j.error) {
      if (typeof j.error === "string") message = j.error;
      else { message = j.error.message || JSON.stringify(j.error); type = j.error.type || type; code = j.error.code || status; }
    } else if (typeof j === "string") message = j;
  } catch {}
  if (status === 401 || status === 403) type = "invalid_request_error";
  else if (status === 429) type = "rate_limit_exceeded";
  return { error: { message, type, code } };
}

function proxyError(cres, logEntry, e, shape) {
  logEntry.status = 502; logEntry.note = e.message; pushLog(logEntry);
  const body = shape === "openai"
    ? normalizeOpenAIError(e.message, 502)
    : normalizeAnthropicError(e.message, 502);
  const buf = Buffer.from(JSON.stringify(body));
  safeWriteHead(cres, 502, { "content-type": "application/json", "content-length": buf.length });
  safeEnd(cres, buf);
}

// Pipe the upstream SSE straight through to an Anthropic streaming client.
// Upstream SSE error events are piped verbatim — Anthropic SDK reads them.
// Headers are filtered (hop-by-hop stripped) so Node re-frames cleanly, and all
// writes are safe against a client that already hung up.
function respondAnthropicStream(upres, cres, logEntry) {
  logEntry.status = upres.statusCode;
  if (!safeWriteHead(cres, upres.statusCode || 502, filterHeaders(upres.headers))) { try { upres.destroy(); } catch {} return; }
  upres.pipe(cres);
  upres.on("error", () => { try { cres.destroy(); upres.destroy(); } catch {} });
  cres.on("error", () => { try { upres.destroy(); } catch {} });
  upres.on("end", () => pushLog(logEntry));
}

// Buffer upstream SSE and emit one non-stream Anthropic Messages JSON object.
function respondAnthropicNonStream(upres, cres, logEntry) {
  const data = [];
  upres.on("data", (c) => data.push(c));
  upres.on("end", () => {
    const text = Buffer.concat(data).toString("utf8");
    if (upres.statusCode !== 200) {
      logEntry.status = upres.statusCode; pushLog(logEntry);
      const norm = normalizeAnthropicError(text, upres.statusCode);
      const buf = Buffer.from(JSON.stringify(norm));
      safeWriteHead(cres, upres.statusCode || 502, { "content-type": "application/json", "content-length": buf.length });
      safeEnd(cres, buf);
      return;
    }
    const r = collectAnthropicMessage(text);
    logEntry.status = r.ok ? 200 : 500;
    pushLog(logEntry);
    const out = r.ok ? r.message : { type: "error", error: r.error || { type: "proxy_error", message: "failed to assemble non-stream response" } };
    const buf = Buffer.from(JSON.stringify(out));
    const code = r.ok ? 200 : 500;
    safeWriteHead(cres, code, { "content-type": "application/json", "content-length": buf.length });
    safeEnd(cres, buf);
  });
}

// Translate upstream Anthropic SSE → OpenAI Chat Completions SSE chunks.
function respondOpenAIStream(upres, cres, model, logEntry) {
  const data = [];
  if (upres.statusCode !== 200) {
    upres.on("data", (c) => data.push(c));
    upres.on("end", () => {
      logEntry.status = upres.statusCode; pushLog(logEntry);
      const text = Buffer.concat(data).toString("utf8");
      const norm = normalizeOpenAIError(text, upres.statusCode);
      const buf = Buffer.from(JSON.stringify(norm));
      safeWriteHead(cres, upres.statusCode || 502, { "content-type": "application/json", "content-length": buf.length });
      safeEnd(cres, buf);
    });
    return;
  }
  logEntry.status = 200;
  if (!safeWriteHead(cres, 200, { "content-type": "text/event-stream", "cache-control": "no-cache", "connection": "keep-alive" })) { try { upres.destroy(); } catch {} return; }
  const id = "chatcmpl-" + crypto.randomUUID().replace(/-/g, "").slice(0, 24);
  const created = Math.floor(Date.now() / 1000);
  const emit = (delta, finish) => {
    const obj = { id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta, finish_reason: finish == null ? null : finish }] };
    if (!safeWrite(cres, "data: " + JSON.stringify(obj) + "\n\n")) { try { upres.destroy(); } catch {} }
  };
  emit({ role: "assistant", content: "" }, null);
  const tcIndex = {};
  let tcCounter = 0;
  let finish = "stop";
  let buf = "";
  upres.on("data", (chunk) => {
    buf += chunk.toString("utf8");
    const lines = buf.split("\n");
    buf = lines.pop();
    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line.startsWith("data:")) continue;
      const payload = line.slice(5).trim();
      if (!payload || payload === "[DONE]") continue;
      let ev;
      try { ev = JSON.parse(payload); } catch { continue; }
      switch (ev.type) {
        case "content_block_start": {
          const b = ev.content_block || {};
          if (b.type === "tool_use") {
            const idx = tcCounter++;
            tcIndex[ev.index] = idx;
            emit({ tool_calls: [{ index: idx, id: b.id, type: "function", function: { name: b.name, arguments: "" } }] }, null);
          }
          break;
        }
        case "content_block_delta": {
          const d = ev.delta;
          if (!d) break;
          if (d.type === "text_delta") emit({ content: d.text || "" }, null);
          else if (d.type === "input_json_delta") {
            const idx = tcIndex[ev.index];
            if (idx != null) emit({ tool_calls: [{ index: idx, function: { arguments: d.partial_json || "" } }] }, null);
          }
          break;
        }
        case "message_delta":
          if (ev.delta && ev.delta.stop_reason) finish = mapFinishReason(ev.delta.stop_reason);
          break;
        default: break;
      }
    }
  });
  upres.on("end", () => {
    emit({}, finish);
    safeWrite(cres, "data: [DONE]\n\n");
    safeEnd(cres);
    pushLog(logEntry);
  });
}

// Buffer upstream SSE, collect, translate to one OpenAI Chat Completions JSON object.
function respondOpenAINonStream(upres, cres, model, logEntry) {
  const data = [];
  upres.on("data", (c) => data.push(c));
  upres.on("end", () => {
    const text = Buffer.concat(data).toString("utf8");
    if (upres.statusCode !== 200) {
      logEntry.status = upres.statusCode; pushLog(logEntry);
      const norm = normalizeOpenAIError(text, upres.statusCode);
      const buf = Buffer.from(JSON.stringify(norm));
      safeWriteHead(cres, upres.statusCode || 502, { "content-type": "application/json", "content-length": buf.length });
      safeEnd(cres, buf);
      return;
    }
    const r = collectAnthropicMessage(text);
    logEntry.status = r.ok ? 200 : 500; pushLog(logEntry);
    const out = r.ok ? anthropicMessageToOpenAI(r.message, model) : { error: { message: JSON.stringify(r.error), type: "proxy_error" } };
    const buf = Buffer.from(JSON.stringify(out));
    const code = r.ok ? 200 : 500;
    safeWriteHead(cres, code, { "content-type": "application/json", "content-length": buf.length });
    safeEnd(cres, buf);
  });
}

// OpenAI-shape upstream -> OpenAI-shape client: pipe the response straight
// through (the host already speaks OpenAI, stream + non-stream). Only the
// error path is normalized; on 200 we just filter hop-by-hop headers and pipe.
function respondOpenAIPassthrough(upres, cres, logEntry) {
  const st = upres.statusCode || 502;
  if (st >= 200 && st < 300) {
    if (!safeWriteHead(cres, st, filterHeaders(upres.headers))) { try { upres.destroy(); } catch {} return; }
    upres.pipe(cres);
    upres.on("error", () => { try { cres.destroy(); upres.destroy(); } catch {} });
    cres.on("error", () => { try { upres.destroy(); } catch {} });
    upres.on("end", () => pushLog(logEntry));
    return;
  }
  const errBuf = [];
  upres.on("data", (c) => errBuf.push(c));
  upres.on("end", () => {
    pushLog(logEntry);
    const norm = normalizeOpenAIError(Buffer.concat(errBuf).toString("utf8"), st);
    const b = Buffer.from(JSON.stringify(norm));
    safeWriteHead(cres, st, { "content-type": "application/json", "content-length": b.length });
    safeEnd(cres, b);
  });
}

// OpenAI-shape upstream -> Anthropic-shape client, non-stream: buffer the
// OpenAI JSON, translate one-shot to an Anthropic Message, normalize errors.
function respondOpenAIToAnthropicNonStream(upres, cres, model, logEntry) {
  const data = [];
  upres.on("data", (c) => data.push(c));
  upres.on("end", () => {
    const text = Buffer.concat(data).toString("utf8");
    const st = upres.statusCode || 502;
    logEntry.status = st; pushLog(logEntry);
    if (st !== 200) {
      const norm = normalizeAnthropicError(text, st);
      const b = Buffer.from(JSON.stringify(norm));
      safeWriteHead(cres, st, { "content-type": "application/json", "content-length": b.length });
      safeEnd(cres, b);
      return;
    }
    let oai; try { oai = JSON.parse(text); } catch {
      const b = Buffer.from(JSON.stringify({ type: "error", error: { type: "proxy_error", message: "bad upstream json" } }));
      safeWriteHead(cres, 502, { "content-type": "application/json", "content-length": b.length }); safeEnd(cres, b); return;
    }
    const ant = openAIResponseToAnthropic(oai);
    const buf = Buffer.from(JSON.stringify(ant));
    safeWriteHead(cres, 200, { "content-type": "application/json", "content-length": buf.length });
    safeEnd(cres, buf);
  });
}

// Unified forwarding with per-request key rotation. This replaces the old
// postWithRetry / postOpenAIDirectWithRetry / gpt-via-anthropic inline loops:
// one function, parameterized by provider + route, that picks the right
// responder for the (upstreamProto x clientProto x stream) matrix.
//   route: from routeRequest(provider, clientProto, model)
//   bodyBuf: already-shaped upstream body (translated + fingerprinted if needed)
//   clientProto: "anthropic" | "openai"  — for error-shape normalization
function forwardWithRetry(provider, route, bodyBuf, clientProto, clientStream, model, cres, logEntry) {
  const pool = poolOf(provider.id);
  const maxAttempts = Math.max(1, pool.keys.length);
  let attempts = 0;
  let cursor = pool.idx; // per-request snapshot; commit pool.idx only on 200
  const errShape = clientProto === "openai" ? "openai" : "anthropic";

  function dispatch(upres) {
    const upProto = route.upstreamProto;
    if (upProto === "anthropic" && clientProto === "anthropic") {
      return clientStream ? respondAnthropicStream(upres, cres, logEntry) : respondAnthropicNonStream(upres, cres, logEntry);
    }
    if (upProto === "anthropic" && clientProto === "openai") {
      return clientStream ? respondOpenAIStream(upres, cres, model, logEntry) : respondOpenAINonStream(upres, cres, model, logEntry);
    }
    if (upProto === "openai" && clientProto === "openai") {
      return respondOpenAIPassthrough(upres, cres, logEntry);
    }
    // openai upstream -> anthropic client
    return clientStream ? openAIStreamToAnthropic(upres, cres, model, logEntry) : respondOpenAIToAnthropicNonStream(upres, cres, model, logEntry);
  }

  function attempt() {
    if (!pool.keys.length) { proxyError(cres, logEntry, new Error("no keys in pool"), errShape); return; }
    const key = pool.keys[cursor];
    attempts++;
    const headers = buildHeadersForRoute(route, bodyBuf, key);
    const up = https.request({ host: route.host, port: route.port, method: "POST", path: route.path, headers }, (upres) => {
      const st = upres.statusCode || 0;
      const s = ensureStats(provider.id, key);
      s.lastStatus = st; s.lastUsed = new Date().toISOString(); s.count++;
      if (ROTATE_STATUSES.has(st) && attempts < maxAttempts) {
        upres.resume();
        if (st === 401 || st === 402 || st === 403) s.bad = true; else if (st === 429) s.limited++;
        const note = maskKey(key) + "->" + st;
        cursor = nextUsable(provider.id, cursor);
        logEntry.note = logEntry.note ? logEntry.note + " " + note + "->retry" : note + "->retry";
        attempt();
        return;
      }
      logEntry.status = st;
      if (st >= 200 && st < 300) { s.ok++; pool.idx = cursor; }
      dispatch(upres);
    });
    up.on("error", (e) => {
      if (attempts < maxAttempts) { cursor = nextUsable(provider.id, cursor); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "neterr->retry"; attempt(); }
      else proxyError(cres, logEntry, e, errShape);
    });
    up.setTimeout(180000, () => { try { up.destroy(); } catch {}
      if (attempts < maxAttempts) { cursor = nextUsable(provider.id, cursor); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "timeout->retry"; attempt(); }
      else proxyError(cres, logEntry, new Error("upstream timeout"), errShape);
    });
    up.write(bodyBuf);
    up.end();
  }
  attempt();
}

// Anthropic Messages request → OpenAI Chat Completions request (for /v1/messages
// with a GPT model). Mirrors oaiToAnthropic in reverse.
function anthropicToOpenAIRequest(ant) {
  const out = {
    model: ant.model || "gpt-5.4-mini",
    max_tokens: ant.max_tokens || 4096,
    messages: [],
    stream: ant.stream === true,
  };
  // Ask the OpenAI host to include a final usage chunk in the stream so we can
  // populate Anthropic `usage.output_tokens` for streaming GPT-via-Anthropic
  // (otherwise usage is always 0 in stream mode).
  if (out.stream) out.stream_options = { include_usage: true };
  if (ant.temperature != null) out.temperature = ant.temperature;
  if (ant.top_p != null) out.top_p = ant.top_p;
  if (Array.isArray(ant.stop_sequences)) out.stop = ant.stop_sequences;
  const systemParts = [];
  if (Array.isArray(ant.system)) {
    for (const b of ant.system) systemParts.push(typeof b === "string" ? b : (b.text || ""));
  } else if (typeof ant.system === "string") {
    systemParts.push(ant.system);
  }
  const conv = [];
  if (systemParts.length) conv.push({ role: "system", content: systemParts.join("\n\n") });
  for (const m of ant.messages || []) {
    if (m.role === "assistant" && Array.isArray(m.content) && m.content.some((b) => b.type === "tool_use")) {
      // Assistant turn that called tools: OpenAI carries these as `tool_calls`
      // on the assistant message (NOT as content parts). Without this the next
      // `tool_result` references a tool_call the API never saw → 400.
      const blocks = m.content;
      const txt = blocks.filter((b) => b.type === "text").map((b) => b.text || "").join("");
      const toolCalls = blocks.filter((b) => b.type === "tool_use").map((b) => ({
        id: b.id, type: "function", function: { name: b.name, arguments: JSON.stringify(b.input || {}) },
      }));
      const msg = { role: "assistant", content: txt || null };
      if (toolCalls.length) msg.tool_calls = toolCalls;
      conv.push(msg);
    } else if (m.role === "user" || m.role === "assistant") {
      conv.push({ role: m.role, content: anthropicContentToOpenAI(m.content) });
    } else if (m.role === "tool" || (Array.isArray(m.content) && m.content.some((b) => b.type === "tool_result"))) {
      const blocks = Array.isArray(m.content) ? m.content : [];
      for (const b of blocks) {
        if (b.type === "tool_result") {
          conv.push({ role: "tool", tool_call_id: b.tool_use_id, content: typeof b.content === "string" ? b.content : JSON.stringify(b.content) });
        }
      }
    }
  }
  out.messages = conv;
  if (Array.isArray(ant.tools)) {
    out.tools = ant.tools.map((t) => ({ type: "function", function: { name: t.name, description: t.description || "", parameters: t.input_schema || { type: "object", properties: {} } } }));
  }
  if (ant.tool_choice) {
    if (ant.tool_choice.type === "auto" || ant.tool_choice.type === "none") out.tool_choice = ant.tool_choice.type;
    else if (ant.tool_choice.type === "any") out.tool_choice = "required";
    else if (ant.tool_choice.type === "tool") out.tool_choice = { type: "function", function: { name: ant.tool_choice.name } };
  }
  return out;
}

function anthropicContentToOpenAI(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  const parts = [];
  for (const b of content) {
    if (b.type === "text") parts.push({ type: "text", text: b.text || "" });
    else if (b.type === "tool_use") {
      // Standalone tool_use outside an assistant turn (shouldn't normally
      // happen) — drop it; assistant tool_use is handled in the message loop.
      continue;
    }
    else if (b.type === "image") {
      const src = b.source || {};
      if (src.type === "base64") parts.push({ type: "image_url", image_url: { url: `data:${src.media_type};base64,${src.data}` } });
      else if (src.type === "url") parts.push({ type: "image_url", image_url: { url: src.url } });
    }
  }
  return parts.filter(Boolean);
}

// OpenAI Chat Completions non-stream response → Anthropic Messages object.
function openAIResponseToAnthropic(oai) {
  const choice = (oai.choices && oai.choices[0]) || {};
  const msg = choice.message || {};
  const content = [];
  if (typeof msg.content === "string" && msg.content) content.push({ type: "text", text: msg.content });
  if (Array.isArray(msg.tool_calls)) {
    for (const tc of msg.tool_calls) {
      let input = {};
      try { input = tc.function && tc.function.arguments ? JSON.parse(tc.function.arguments) : {}; } catch {}
      content.push({ type: "tool_use", id: tc.id, name: (tc.function || {}).name, input });
    }
  }
  const sr = choice.finish_reason;
  const stopReason = sr === "stop" ? "end_turn" : sr === "tool_calls" ? "tool_use" : sr === "length" ? "max_tokens" : sr || "end_turn";
  const u = oai.usage || {};
  return {
    id: oai.id || ("msg_" + crypto.randomUUID()),
    type: "message", role: "assistant", model: oai.model || "gpt",
    content: content.length ? content : [{ type: "text", text: "" }],
    stop_reason: stopReason, stop_sequence: null,
    usage: { input_tokens: u.prompt_tokens || 0, output_tokens: u.completion_tokens || 0 },
  };
}

// Translate OpenAI Chat Completions SSE → Anthropic Messages SSE, so an
// Anthropic-protocol client streaming a GPT model gets native events.
function openAIStreamToAnthropic(upres, cres, model, logEntry) {
  const errBuf = [];
  if (upres.statusCode !== 200) {
    upres.on("data", (c) => errBuf.push(c));
    upres.on("end", () => {
      logEntry.status = upres.statusCode; pushLog(logEntry);
      const norm = normalizeAnthropicError(Buffer.concat(errBuf).toString("utf8"), upres.statusCode);
      const b = Buffer.from(JSON.stringify(norm));
      safeWriteHead(cres, upres.statusCode || 502, { "content-type": "application/json", "content-length": b.length });
      safeEnd(cres, b);
    });
    return;
  }
  logEntry.status = 200;
  if (!safeWriteHead(cres, 200, { "content-type": "text/event-stream", "cache-control": "no-cache", "connection": "keep-alive" })) { try { upres.destroy(); } catch {} return; }
  const msgId = "msg_" + crypto.randomUUID();
  let alive = true;
  const emit = (obj) => { if (!alive) return; if (!safeWrite(cres, "event: " + obj.type + "\ndata: " + JSON.stringify(obj) + "\n\n")) { alive = false; try { upres.destroy(); } catch {} } };
  emit({ type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 0, output_tokens: 0 } } });

  let blockIdx = -1;        // current Anthropic content-block index
  let textOpen = false;     // is there an open text block?
  const tcMap = {};         // OpenAI tool_call.index -> { block: anthropicIdx }
  let inputTokens = 0, outputTokens = 0;
  let finish = "end_turn";
  let buf = "";

  function openText() { if (textOpen) return; blockIdx++; emit({ type: "content_block_start", index: blockIdx, content_block: { type: "text", text: "" } }); textOpen = true; }
  function closeText() { if (!textOpen) return; emit({ type: "content_block_stop", index: blockIdx }); textOpen = false; }

  upres.on("data", (chunk) => {
    if (!alive) return;
    buf += chunk.toString("utf8");
    const lines = buf.split("\n");
    buf = lines.pop();
    for (const rawLine of lines) {
      const line = rawLine.trim();
      if (!line.startsWith("data:")) continue;
      const payload = line.slice(5).trim();
      if (!payload || payload === "[DONE]") continue;
      let ev;
      try { ev = JSON.parse(payload); } catch { continue; }
      const choice = ev.choices && ev.choices[0];
      const d = choice && choice.delta;
      if (d) {
        if (typeof d.content === "string" && d.content) {
          if (!textOpen) openText();
          emit({ type: "content_block_delta", index: blockIdx, delta: { type: "text_delta", text: d.content } });
        }
        // tool_calls arrive as deltas: first chunk carries id+name, later
        // chunks carry function.arguments fragments. Emit them as Anthropic
        // tool_use blocks with input_json_delta so the client can assemble input.
        if (Array.isArray(d.tool_calls)) {
          for (const tc of d.tool_calls) {
            const oi = tc.index || 0;
            if (!tcMap[oi]) {
              closeText();
              blockIdx++;
              tcMap[oi] = { block: blockIdx };
              emit({ type: "content_block_start", index: blockIdx, content_block: { type: "tool_use", id: tc.id || ("tool_" + oi), name: (tc.function && tc.function.name) || "", input: {} } });
            }
            const fn = tc.function || {};
            if (typeof fn.arguments === "string" && fn.arguments) {
              emit({ type: "content_block_delta", index: tcMap[oi].block, delta: { type: "input_json_delta", partial_json: fn.arguments } });
            }
          }
        }
      }
      if (choice && choice.finish_reason) {
        finish = choice.finish_reason === "stop" ? "end_turn" : choice.finish_reason === "tool_calls" ? "tool_use" : choice.finish_reason === "length" ? "max_tokens" : "end_turn";
      }
      if (ev.usage) {
        inputTokens = ev.usage.prompt_tokens || inputTokens;
        outputTokens = ev.usage.completion_tokens || outputTokens;
      }
    }
  });
  upres.on("end", () => {
    for (const oi of Object.keys(tcMap)) emit({ type: "content_block_stop", index: tcMap[oi].block });
    if (textOpen) emit({ type: "content_block_stop", index: blockIdx });
    if (blockIdx < 0) {
      // nothing streamed at all — emit an empty text block so the client gets content
      emit({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } });
      emit({ type: "content_block_stop", index: 0 });
    }
    emit({ type: "message_delta", delta: { stop_reason: finish, stop_sequence: null }, usage: { output_tokens: outputTokens } });
    emit({ type: "message_stop" });
    safeEnd(cres);
    pushLog(logEntry);
  });
  upres.on("error", () => { alive = false; try { cres.destroy(); } catch {} });
  cres.on("error", () => { alive = false; try { upres.destroy(); } catch {} });
}

// (postWithRetry was replaced by the unified forwardWithRetry above.)

// ─── UI helpers ───────────────────────────────────────────────────────────
function sendJson(cres, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  safeWriteHead(cres, code, { "content-type": "application/json", "content-length": buf.length, "access-control-allow-origin": "*" });
  safeEnd(cres, buf);
}
function readJsonBody(creq, cb) {
  const ch = [];
  creq.on("data", (c) => ch.push(c));
  creq.on("end", () => {
    const text = Buffer.concat(ch).toString("utf8");
    if (!text) return cb({}, null);
    let b; let err = null;
    try { b = JSON.parse(text); } catch (e) { err = e.message; b = {}; }
    cb(b, err);
  });
}

// ─── server ───────────────────────────────────────────────────────────────
const server = http.createServer((creq, cres) => {
  const url = creq.url.split("?")[0];

  // CORS: allow browser clients from any origin (UI is same-origin, but this
  // keeps the LLM endpoints usable from web clients too). Preflight short-circuit.
  try { cres.setHeader("access-control-allow-origin", "*"); } catch {}
  if (creq.method === "OPTIONS") {
    safeWriteHead(cres, 204, {
      "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
      "access-control-allow-headers": "content-type, authorization, x-api-key, anthropic-version, anthropic-beta",
      "access-control-max-age": "86400",
    });
    safeEnd(cres);
    return;
  }

  // UI
  if (url === "/" || url === "/ui" || url === "/index.html") {
    try {
      const html = fs.readFileSync(UI_FILE, "utf8");
      const buf = Buffer.from(html);
      safeWriteHead(cres, 200, { "content-type": "text/html; charset=utf-8", "content-length": buf.length });
      safeEnd(cres, buf);
    } catch (e) { sendJson(cres, 500, { error: "ui.html missing: " + e.message }); }
    return;
  }

  // Profile disabled → block LLM traffic, but NOT /v1/models (metadata) so the
  // UI model list and test picker keep working while the profile is off.
  if (!profileEnabled() && url.startsWith("/v1/") && url !== "/v1/models" && !url.startsWith("/v1/models")) {
    const ap = activeProvider();
    const buf = Buffer.from(JSON.stringify({
      type: "error", error: { type: "profile_disabled",
        message: "Profile is disabled. Enable it in the proxy UI or PUT /api/profile { enabled: true }." }
    }));
    safeWriteHead(cres, 503, { "content-type": "application/json", "content-length": buf.length, "x-profile-enabled": "false", "x-active-provider": ap ? ap.id : "" });
    safeEnd(cres, buf);
    return;
  }

  // query param helper (for ?provider= on key endpoints)
  const qIdx = creq.url.indexOf("?");
  const queryStr = qIdx >= 0 ? creq.url.slice(qIdx + 1) : "";
  function queryParam(name) {
    for (const pair of queryStr.split("&")) {
      const eq = pair.indexOf("="); if (eq < 0) continue;
      if (decodeURIComponent(pair.slice(0, eq)) === name) return decodeURIComponent(pair.slice(eq + 1));
    }
    return null;
  }
  function providerForKeys() { const id = queryParam("provider"); return (id && providerById(id)) || activeProvider(); }

  // UI backend: global profile enable + active provider selector (compat alias)
  if (url === "/api/profile") {
    if (creq.method === "GET") {
      const ap = activeProvider();
      sendJson(cres, 200, { id: ap.id, name: ap.name, enabled: profileEnabled(), activeId: PROVIDERS.activeId, providers: PROVIDERS.providers, profileFile: PROVIDERS_FILE });
      return;
    }
    if (creq.method === "PUT") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        if (typeof body.enabled === "boolean") PROVIDERS.enabled = body.enabled;
        if (typeof body.activeId === "string" && providerById(body.activeId)) PROVIDERS.activeId = body.activeId;
        persistProviders();
        const ap = activeProvider();
        pushLog({ kind: "api", method: "PUT", path: "/api/profile", status: 200, note: "enabled=" + PROVIDERS.enabled + " active=" + ap.id });
        sendJson(cres, 200, { ok: true, profile: { id: ap.id, name: ap.name, enabled: profileEnabled(), activeId: PROVIDERS.activeId }, providers: PROVIDERS.providers });
      });
      return;
    }
  }

  // Multi-provider management
  if (url === "/api/providers" && creq.method === "GET") {
    sendJson(cres, 200, { activeId: PROVIDERS.activeId, enabled: profileEnabled(), providers: PROVIDERS.providers });
    return;
  }
  if (url === "/api/providers/active" && creq.method === "PUT") {
    readJsonBody(creq, (body, err) => {
      if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
      if (typeof body.id === "string" && providerById(body.id)) {
        PROVIDERS.activeId = body.id; persistProviders();
        pushLog({ kind: "api", method: "PUT", path: "/api/providers/active", status: 200, note: "active=" + body.id });
        sendJson(cres, 200, { ok: true, activeId: body.id, provider: providerById(body.id) });
      } else sendJson(cres, 404, { ok: false, error: "unknown provider id" });
    });
    return;
  }
  if (url === "/api/providers" && creq.method === "POST") {
    readJsonBody(creq, (body, err) => {
      if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
      const id = (typeof body.id === "string" && body.id) || ("prov" + crypto.randomBytes(3).toString("hex"));
      if (providerById(id)) { sendJson(cres, 409, { ok: false, error: "provider id already exists" }); return; }
      const kind = body.kind === "freemodel" ? "freemodel" : "openai-compat";
      const p = {
        id, name: typeof body.name === "string" ? body.name : id, kind, enabled: true,
        keyPrefix: typeof body.keyPrefix === "string" ? body.keyPrefix : "",
        needsFingerprint: kind === "freemodel",
        note: typeof body.note === "string" ? body.note : "",
      };
      if (kind === "freemodel") {
        p.claudeUpstream = body.claudeUpstream || "cc.freemodel.dev";
        p.openaiUpstream = body.openaiUpstream || "api.freemodel.dev";
      } else {
        p.baseUrl = body.baseUrl || "";
        p.probeModel = body.probeModel || "";
        if (!p.baseUrl) { sendJson(cres, 400, { ok: false, error: "baseUrl required for openai-compat provider" }); return; }
      }
      PROVIDERS.providers.push(p); persistProviders();
      pushLog({ kind: "api", method: "POST", path: "/api/providers", status: 200, note: "added " + id });
      sendJson(cres, 200, { ok: true, provider: p });
    });
    return;
  }
  // PUT /api/providers/<id> — edit mutable fields (name, note, baseUrl, upstreams, probeModel, keyPrefix)
  if (url.startsWith("/api/providers/") && creq.method === "PUT") {
    const id = url.slice("/api/providers/".length).split("/")[0];
    const p = providerById(id);
    if (!p) { sendJson(cres, 404, { ok: false, error: "unknown provider id" }); return; }
    readJsonBody(creq, (body, err) => {
      if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
      if (typeof body.name === "string") p.name = body.name;
      if (typeof body.note === "string") p.note = body.note;
      if (typeof body.keyPrefix === "string") p.keyPrefix = body.keyPrefix;
      if (typeof body.claudeUpstream === "string") p.claudeUpstream = body.claudeUpstream;
      if (typeof body.openaiUpstream === "string") p.openaiUpstream = body.openaiUpstream;
      if (typeof body.baseUrl === "string") p.baseUrl = body.baseUrl;
      if (typeof body.probeModel === "string") p.probeModel = body.probeModel;
      if (typeof body.needsFingerprint === "boolean") p.needsFingerprint = body.needsFingerprint;
      persistProviders();
      pushLog({ kind: "api", method: "PUT", path: "/api/providers/" + id, status: 200, note: "edited" });
      sendJson(cres, 200, { ok: true, provider: p });
    });
    return;
  }
  if (url.startsWith("/api/providers/") && creq.method === "DELETE") {
    const id = url.slice("/api/providers/".length).split("/")[0];
    if (PROVIDERS.providers.length <= 1) { sendJson(cres, 400, { ok: false, error: "cannot delete the last provider" }); return; }
    const i = PROVIDERS.providers.findIndex((p) => p.id === id);
    if (i < 0) { sendJson(cres, 404, { ok: false, error: "unknown provider id" }); return; }
    PROVIDERS.providers.splice(i, 1);
    if (PROVIDERS.activeId === id) PROVIDERS.activeId = PROVIDERS.providers[0].id;
    persistProviders();
    pushLog({ kind: "api", method: "DELETE", path: "/api/providers/" + id, status: 200, note: "removed" });
    sendJson(cres, 200, { ok: true, activeId: PROVIDERS.activeId });
    return;
  }

  if (url === "/api/status") {
    const ap = activeProvider();
    const pool = poolOf(ap.id);
    sendJson(cres, 200, {
      ok: true,
      port: CFG.port,
      enabled: profileEnabled(),
      activeId: PROVIDERS.activeId,
      activeProvider: { id: ap.id, name: ap.name, kind: ap.kind, needsFingerprint: !!ap.needsFingerprint,
        baseUrl: ap.baseUrl || null, claudeUpstream: ap.claudeUpstream || null, openaiUpstream: ap.openaiUpstream || null },
      providers: PROVIDERS.providers.map((p) => ({ id: p.id, name: p.name, kind: p.kind, enabled: p.enabled !== false, keys: poolOf(p.id).keys.length })),
      keys: { total: pool.keys.length, current: pool.idx, currentMasked: maskKey(currentKey(ap.id)) },
      device_id: DEVICE_ID,
      session_id: SESSION_ID,
      version: require("./package.json").version,
      uptime_s: Math.round(process.uptime()),
      endpoints: ["POST /v1/messages (Anthropic)", "POST /v1/chat/completions (OpenAI)", "GET /v1/models (active provider)"],
      fingerprint: { capturedFrom: FP.capturedFrom, userAgent: FP.userAgent, profileFile: FINGERPRINT_FILE },
    });
    return;
  }
  if (url === "/api/logs") { sendJson(cres, 200, { logs: LOGS.slice().reverse() }); return; }

  // Fingerprint profile (read + update without editing code). Only meaningful
  // for freemodel-style providers, but stored globally.
  if (url === "/api/fingerprint") {
    if (creq.method === "GET") {
      sendJson(cres, 200, { ...FP, profileFile: FINGERPRINT_FILE });
      return;
    }
    if (creq.method === "PUT") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        const next = { ...FP };
        for (const k of Object.keys(FINGERPRINT_DEFAULTS)) if (body[k] != null && typeof body[k] === "string") next[k] = body[k];
        try {
          fs.mkdirSync(CONFIG_DIR, { recursive: true });
          fs.writeFileSync(FINGERPRINT_FILE, JSON.stringify(next, null, 2) + "\n");
          Object.assign(FP, next);
          pushLog({ kind: "api", method: "PUT", path: "/api/fingerprint", status: 200, note: "profile updated" });
          sendJson(cres, 200, { ok: true, profile: { ...FP, profileFile: FINGERPRINT_FILE }, note: "applied live; restart to re-read file if edited externally" });
        } catch (e) { sendJson(cres, 500, { ok: false, error: e.message }); }
      });
      return;
    }
  }

  // Key pool management — operates on the active provider by default, or
  // ?provider=<id> to target another. Alias keeps keys.js CLI and old UI working.
  if (url === "/api/keys") {
    const p = providerForKeys();
    const pool = poolOf(p.id);
    if (creq.method === "GET") {
      reloadKeyStats(p.id);
      sendJson(cres, 200, {
        provider: p.id, total: pool.keys.length, current: pool.idx,
        keys: pool.keys.map((k, i) => {
          const s = (keyStats[p.id] || {})[k] || {};
          return { index: i, masked: maskKey(k), current: i === pool.idx, ok: s.ok || 0, limited: s.limited || 0, bad: !!s.bad, count: s.count || 0, lastStatus: s.lastStatus, lastUsed: s.lastUsed };
        }),
      });
      return;
    }
    if (creq.method === "POST") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        const added = [];
        const add = (v) => { if (typeof v === "string" && v.trim() && !pool.keys.includes(v.trim())) { pool.keys.push(v.trim()); ensureStats(p.id, v.trim()); added.push(v.trim()); } };
        if (Array.isArray(body.keys)) body.keys.forEach(add); else if (body.key) add(body.key);
        persistPool(p.id);
        sendJson(cres, 200, { ok: true, provider: p.id, added: added.length, total: pool.keys.length });
      });
      return;
    }
    if (creq.method === "DELETE") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        let removed = false;
        if (typeof body.index === "number" && pool.keys[body.index] !== undefined) { pool.keys.splice(body.index, 1); removed = true; }
        else if (typeof body.key === "string") { const i = pool.keys.indexOf(body.key.trim()); if (i >= 0) { pool.keys.splice(i, 1); removed = true; } }
        if (removed) { pool.idx = pool.keys.length ? pool.idx % pool.keys.length : 0; persistPool(p.id); sendJson(cres, 200, { ok: true, provider: p.id, removed: true, total: pool.keys.length }); }
        else sendJson(cres, 404, { ok: false, error: "key not found" });
      });
      return;
    }
    return;
  }
  if (url === "/api/keys/find" && creq.method === "POST") {
    const p = providerForKeys();
    readJsonBody(creq, async () => { const r = await findFirstWorking(p.id); sendJson(cres, 200, Object.assign({ provider: p.id }, r)); });
    return;
  }

  if (url === "/api/test" && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      let reqBody;
      try { reqBody = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { reqBody = {}; }
      const model = reqBody.model || "claude-opus-4-8";
      const prompt = reqBody.prompt || "Reply with exactly: ok";
      const proto = reqBody.proto === "anthropic" ? "anthropic" : "openai";
      const ap = activeProvider();
      const route = routeRequest(ap, proto, model);
      // Route through the REAL endpoint (loopback) so the test exercises the
      // exact path a client would use: chosen protocol × active provider.
      const path = proto === "anthropic" ? "/v1/messages" : "/v1/chat/completions";
      const body = proto === "anthropic"
        ? { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: [{ type: "text", text: prompt }] }] }
        : { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: prompt }] };
      const buf = Buffer.from(JSON.stringify(body));
      const logEntry = { kind: "test", method: "POST", path: path + "(" + proto + ")", status: null, note: model + " via " + ap.id };
      const up = http.request({ host: "127.0.0.1", port: CFG.port, method: "POST", path, headers: { "content-type": "application/json", "content-length": buf.length } }, (upres) => {
        const data = []; upres.on("data", (c) => data.push(c));
        upres.on("end", () => {
          const text = Buffer.concat(data).toString("utf8");
          const st = upres.statusCode || 502;
          logEntry.status = st; pushLog(logEntry);
          if (st !== 200) { sendJson(cres, st, { ok: false, model, proto, path, provider: ap.id, upstream: route.host, status: st, raw: text }); return; }
          let out = text;
          try {
            const d = JSON.parse(text);
            if (proto === "anthropic") { const txt = (d.content || []).filter((b) => b.type === "text").map((b) => b.text).join(""); if (txt) out = txt; }
            else { const c = d.choices && d.choices[0] && d.choices[0].message && d.choices[0].message.content; if (c) out = c; }
          } catch {}
          sendJson(cres, 200, { ok: true, model, proto, path, provider: ap.id, upstream: route.host, status: st, raw: out });
        });
      });
      up.on("error", (e) => { logEntry.status = 502; logEntry.note = e.message; pushLog(logEntry); sendJson(cres, 502, { ok: false, error: e.message }); });
      up.write(buf); up.end();
    });
    return;
  }

  // API: /v1/messages  (Anthropic Messages, stream + non-stream)
  if (url === "/v1/messages" && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      let parsed;
      try { parsed = JSON.parse(raw); } catch { sendJson(cres, 400, { type: "error", error: { type: "invalid_request", message: "body is not valid JSON" } }); return; }
      const provider = activeProvider();
      const model = parsed.model || "claude";
      const route = routeRequest(provider, "anthropic", model);
      let bodyBuf, clientStream;
      if (route.translate) {
        // Anthropic client -> OpenAI upstream: translate the body.
        const oai = anthropicToOpenAIRequest(parsed);
        clientStream = oai.stream === true;
        bodyBuf = Buffer.from(JSON.stringify(oai));
      } else if (route.fingerprint) {
        // Anthropic client -> freemodel cc host: inject the Claude Code fingerprint.
        const inj = injectFingerprint(raw);
        if (!inj.ok) { sendJson(cres, 400, { type: "error", error: { type: "invalid_request", message: "body is not valid JSON" } }); return; }
        clientStream = inj.clientStream; bodyBuf = Buffer.from(inj.body);
      } else {
        // Anthropic client -> anthropic upstream, no fingerprint (not currently used).
        clientStream = parsed.stream === true; bodyBuf = Buffer.from(raw);
      }
      const logEntry = { kind: route.upstreamProto === "openai" ? "openai" : "anthropic", method: "POST", path: "/v1/messages", status: null, note: model + " via " + provider.id + (clientStream ? " stream" : " nonstream") };
      forwardWithRetry(provider, route, bodyBuf, "anthropic", clientStream, model, cres, logEntry);
    });
    return;
  }

  // API: /v1/chat/completions  (OpenAI Chat Completions, stream + non-stream)
  if ((url === "/v1/chat/completions" || url === "/chat/completions") && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      let oai;
      try { oai = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch (e) {
        sendJson(cres, 400, { error: { message: "invalid JSON body: " + e.message, type: "invalid_request_error" } });
        return;
      }
      const provider = activeProvider();
      const model = oai.model || "claude";
      const route = routeRequest(provider, "openai", model);
      let bodyBuf, clientStream = oai.stream === true;
      if (route.translate) {
        // OpenAI client -> Anthropic upstream (freemodel cc): translate + fingerprint.
        const ant = oaiToAnthropic(oai);
        if (route.fingerprint) {
          const inj = injectFingerprint(JSON.stringify(ant));
          if (!inj.ok) { sendJson(cres, 400, { error: { message: "failed to translate request", type: "invalid_request_error" } }); return; }
          bodyBuf = Buffer.from(inj.body);
        } else {
          bodyBuf = Buffer.from(JSON.stringify(ant));
        }
      } else {
        // OpenAI client -> OpenAI upstream: forward as-is (freemodel api host or openai-compat baseUrl).
        bodyBuf = Buffer.from(JSON.stringify(oai));
      }
      const logEntry = { kind: route.upstreamProto === "openai" ? "openai" : "anthropic", method: "POST", path: "/v1/chat/completions", status: null, note: model + " via " + provider.id + (clientStream ? " stream" : " nonstream") };
      forwardWithRetry(provider, route, bodyBuf, "openai", clientStream, model, cres, logEntry);
    });
    return;
  }

  // API: /v1/models — models served by the ACTIVE provider (freemodel: merged
  // claude+gpt from two hosts; openai-compat: one /models on baseUrl).
  if (url === "/v1/models" || url.startsWith("/v1/models")) {
    const logEntry = { kind: "api", method: "GET", path: "/v1/models", status: null };
    const ap = activeProvider();
    function fetchModelsFrom(ep) {
      return new Promise((resolve) => {
        const k = currentKey(ap.id);
        const headers = ep.anthropic
          ? buildAnthropicUpstreamHeaders(null, k, ep.host)
          : buildOpenAIUpstreamHeaders(null, k, ep.host);
        headers["accept"] = "application/json";
        const up = https.request({ host: ep.host, port: ep.port, method: "GET", path: ep.path, headers }, (upres) => {
          const data = []; upres.on("data", (c) => data.push(c));
          upres.on("end", () => { try { resolve(JSON.parse(Buffer.concat(data).toString("utf8")).data || []); } catch { resolve([]); } });
        });
        up.on("error", () => resolve([]));
        up.setTimeout(10000, () => { try { up.destroy(); } catch {} resolve([]); });
        up.end();
      });
    }
    Promise.all(modelsEndpoints(ap).map(fetchModelsFrom)).then((lists) => {
      logEntry.status = 200; pushLog(logEntry);
      const data = [].concat(...lists);
      const buf = Buffer.from(JSON.stringify({ object: "list", data }));
      safeWriteHead(cres, 200, { "content-type": "application/json", "content-length": buf.length });
      safeEnd(cres, buf);
    });
    return;
  }

  sendJson(cres, 404, { error: { message: "Not found: " + url, type: "not_found_error" } });
});

server.listen(CFG.port, "127.0.0.1", () => {
  const ap = activeProvider();
  console.log("freemodel-cc-proxy " + require("./package.json").version);
  console.log("  Anthropic: http://127.0.0.1:" + CFG.port + "/v1/messages");
  console.log("  OpenAI    : http://127.0.0.1:" + CFG.port + "/v1/chat/completions");
  console.log("  Models    : http://127.0.0.1:" + CFG.port + "/v1/models");
  console.log("  UI        : http://127.0.0.1:" + CFG.port + "/");
  console.log("  active    : " + ap.id + " (" + ap.name + ", kind=" + ap.kind + ", keys=" + poolOf(ap.id).keys.length + ")");
  console.log("  providers : " + PROVIDERS.providers.map((p) => p.id + "[" + p.kind + "," + poolOf(p.id).keys.length + "k]").join(" "));
  console.log("  rotate on 401/403/429; 5xx returned (no pool burn)");
});
