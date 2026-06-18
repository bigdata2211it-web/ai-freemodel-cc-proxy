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
  const cfg = { port: 11440, upstream: "cc.freemodel.dev", upstreamOpenai: "api.freemodel.dev", key: "" };
  try { Object.assign(cfg, JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"))); } catch {}
  if (process.env.FMCC_PORT) cfg.port = parseInt(process.env.FMCC_PORT, 10);
  if (process.env.FMCC_UPSTREAM) cfg.upstream = process.env.FMCC_UPSTREAM;
  if (process.env.FMCC_UPSTREAM_OPENAI) cfg.upstreamOpenai = process.env.FMCC_UPSTREAM_OPENAI;
  if (process.env.FMCC_KEY) cfg.key = process.env.FMCC_KEY;
  return cfg;
}
const CFG = loadConfig();

// ─── key pool (multi-key rotation: drain key #1 until exhausted, then #2, …) ─
// On 401/402/429/5xx the proxy advances to the next usable key and retries the
// SAME client request, so the client sees no break — only a final error if every
// key in the pool is dead. The pointer only moves forward (sequential drain).
const KEYS_FILE = path.join(CONFIG_DIR, "keys.json");
function loadKeyPool() {
  let arr = [];
  try { const j = JSON.parse(fs.readFileSync(KEYS_FILE, "utf8")); if (Array.isArray(j.keys)) arr = j.keys.filter((k) => typeof k === "string" && k); } catch {}
  // bootstrap from legacy single key (env FMCC_KEY or config.json "key")
  if (!arr.length && CFG.key) arr = [CFG.key];
  return arr;
}
let KEY_POOL = loadKeyPool();
let keyIdx = 0;
const keyStats = {};
const ROTATE_STATUSES = new Set([401, 402, 429, 500, 502, 503, 504]);
function ensureStats(k) { if (!keyStats[k]) keyStats[k] = { ok: 0, limited: 0, bad: false, count: 0, lastStatus: null, lastUsed: null }; return keyStats[k]; }
KEY_POOL.forEach(ensureStats);
function persistKeys() {
  try { fs.mkdirSync(CONFIG_DIR, { recursive: true }); fs.writeFileSync(KEYS_FILE, JSON.stringify({ keys: KEY_POOL }, null, 2) + "\n"); } catch {}
}
function maskKey(k) { return k ? k.slice(0, 8) + "…" + k.slice(-4) : "(none)"; }
function currentKey() { return KEY_POOL[keyIdx]; }
// advance the persistent pointer to the next non-bad key (forward, circular)
function advancePointer() {
  if (KEY_POOL.length === 0) return;
  for (let i = 0; i < KEY_POOL.length; i++) {
    keyIdx = (keyIdx + 1) % KEY_POOL.length;
    const s = keyStats[KEY_POOL[keyIdx]];
    if (!s || !s.bad) return;
  }
}
function reloadKeyStats() { KEY_POOL.forEach(ensureStats); }

// Sequential forward health probe: find the FIRST working key, lock the pointer
// onto it, leave the rest untouched. Stops at the first 200 (minimal requests,
// IP-friendly). Dead keys encountered are marked (401 -> bad, others -> limited).
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function probeOnce(body, key) {
  return new Promise((resolve) => {
    const up = https.request({ host: CFG.upstream, port: 443, method: "POST", path: "/v1/messages?beta=true", headers: buildUpstreamHeaders(body, key) }, (upres) => {
      upres.resume();
      upres.on("end", () => resolve({ status: upres.statusCode || 0 }));
    });
    up.on("error", () => resolve({ status: 0 }));
    up.setTimeout(15000, () => { try { up.destroy(); } catch {} resolve({ status: 0 }); });
    up.write(body);
    up.end();
  });
}
async function findFirstWorking() {
  if (!KEY_POOL.length) return { ok: false, error: "empty pool" };
  const probeBody = Buffer.from(injectFingerprint(JSON.stringify({
    model: "claude-haiku-4-5-20251001", max_tokens: 1, stream: false,
    messages: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
  })).body);
  const start = keyIdx;
  const tried = [];
  for (let step = 0; step < KEY_POOL.length; step++) {
    const i = (start + step) % KEY_POOL.length;
    const key = KEY_POOL[i];
    const s = ensureStats(key);
    if (s.bad) { tried.push({ index: i, masked: maskKey(key), status: "skip(bad)" }); continue; }
    const res = await probeOnce(probeBody, key);
    s.lastStatus = res.status; s.lastUsed = new Date().toISOString(); s.count++;
    if (res.status === 200) { s.ok++; keyIdx = i; return { ok: true, current: i, masked: maskKey(key), probed: step + 1, tried }; }
    if (res.status === 401) s.bad = true; else if (ROTATE_STATUSES.has(res.status)) s.limited++;
    tried.push({ index: i, masked: maskKey(key), status: res.status || "neterr" });
    await sleep(400); // gentle pacing so the IP doesn't look abusive
  }
  return { ok: false, current: keyIdx, probed: tried.length, tried, error: "no working key in pool" };
}

if (!KEY_POOL.length) {
  console.error("No FreeModel keys configured. Add one (or many):");
  console.error("  node keys.js add fe_oa_...");
  console.error("  or: FMCC_KEY=fe_oa_... node index.js");
  console.error("  or put {\"keys\":[\"fe_oa_...\"]} in " + KEYS_FILE);
  process.exit(1);
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

// ─── upstream request helpers ────────────────────────────────────────────
function buildUpstreamHeaders(bodyBuf, key, extra) {
  const headers = {
    "host": CFG.upstream,
    "user-agent": UA,
    "x-app": "cli",
    "anthropic-dangerous-direct-browser-access": "true",
    "anthropic-version": FP.anthropicVersion,
    "anthropic-beta": ANTHROPIC_BETA,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  const k = key || CFG.key;
  if (k) { headers["x-api-key"] = k; headers["authorization"] = "Bearer " + k; }
  return Object.assign(headers, extra || {});
}

function proxyError(cres, logEntry, e) {
  logEntry.status = 502; logEntry.note = e.message; pushLog(logEntry);
  try { cres.writeHead(502, { "content-type": "application/json" }); } catch {}
  cres.end(JSON.stringify({ type: "error", error: { type: "proxy_error", message: e.message } }));
}

// Pipe the upstream SSE straight through to an Anthropic streaming client.
function respondAnthropicStream(upres, cres, logEntry) {
  logEntry.status = upres.statusCode;
  cres.writeHead(upres.statusCode || 502, upres.headers);
  upres.pipe(cres);
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
      cres.writeHead(upres.statusCode || 502, { "content-type": "application/json" });
      cres.end(text);
      return;
    }
    const r = collectAnthropicMessage(text);
    logEntry.status = r.ok ? 200 : 500;
    pushLog(logEntry);
    const out = r.ok ? r.message : { type: "error", error: r.error || { type: "proxy_error", message: "failed to assemble non-stream response" } };
    const buf = Buffer.from(JSON.stringify(out));
    const code = r.ok ? 200 : 500;
    cres.writeHead(code, { "content-type": "application/json", "content-length": buf.length });
    cres.end(buf);
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
      let message = text;
      try { const j = JSON.parse(text); message = (j.error && j.error.message) || text; } catch {}
      const buf = Buffer.from(JSON.stringify({ error: { message, type: "upstream_error", code: upres.statusCode } }));
      cres.writeHead(upres.statusCode || 502, { "content-type": "application/json", "content-length": buf.length });
      cres.end(buf);
    });
    return;
  }
  logEntry.status = 200;
  cres.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", "connection": "keep-alive" });
  const id = "chatcmpl-" + crypto.randomUUID().replace(/-/g, "").slice(0, 24);
  const created = Math.floor(Date.now() / 1000);
  const emit = (delta, finish) => {
    const obj = { id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta, finish_reason: finish == null ? null : finish }] };
    cres.write("data: " + JSON.stringify(obj) + "\n\n");
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
    cres.write("data: [DONE]\n\n");
    cres.end();
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
      let message = text;
      try { const j = JSON.parse(text); message = (j.error && j.error.message) || text; } catch {}
      const buf = Buffer.from(JSON.stringify({ error: { message, type: "upstream_error", code: upres.statusCode } }));
      cres.writeHead(upres.statusCode || 502, { "content-type": "application/json", "content-length": buf.length });
      cres.end(buf);
      return;
    }
    const r = collectAnthropicMessage(text);
    logEntry.status = r.ok ? 200 : 500; pushLog(logEntry);
    const out = r.ok ? anthropicMessageToOpenAI(r.message, model) : { error: { message: JSON.stringify(r.error), type: "proxy_error" } };
    const buf = Buffer.from(JSON.stringify(out));
    const code = r.ok ? 200 : 500;
    cres.writeHead(code, { "content-type": "application/json", "content-length": buf.length });
    cres.end(buf);
  });
}

// Generic GET passthrough (used by /v1/models).
function passthroughGet(reqUrl, clientHeaders, cres) {
  const headers = buildUpstreamHeaders(null, currentKey(), { accept: "application/json" });
  const logEntry = { kind: "api", method: "GET", path: reqUrl, status: null };
  const up = https.request({ host: CFG.upstream, port: 443, method: "GET", path: reqUrl, headers }, (upres) => {
    logEntry.status = upres.statusCode;
    cres.writeHead(upres.statusCode || 502, upres.headers);
    upres.pipe(cres);
    upres.on("end", () => pushLog(logEntry));
  });
  up.on("error", (e) => proxyError(cres, logEntry, e));
  up.end();
}

// ─── GPT / OpenAI-host (api.freemodel.dev) routing ────────────────────────
// FreeModel's GPT models (gpt-5.5, gpt-5.4, gpt-5.4-mini, gpt-5.3-codex) live
// on api.freemodel.dev, which speaks OpenAI Chat Completions natively with no
// client fingerprint gate. The same fe_oa key works. We route by model id:
// gpt-* goes straight to the OpenAI host (no Anthropic translation, no
// fingerprint); claude-* keeps the cc.freemodel.dev fingerprint path.
function isGptModel(id) {
  if (!id) return false;
  return /^gpt-/i.test(id) || /gpt-5/i.test(id);
}

function buildOpenAIUpstreamHeaders(bodyBuf, key) {
  const headers = {
    "host": CFG.upstreamOpenai,
    "user-agent": UA,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  const k = key || CFG.key || currentKey();
  if (k) { headers["authorization"] = "Bearer " + k; }
  return headers;
}

// Forward an OpenAI Chat Completions body as-is to api.freemodel.dev and pipe
// the response straight back (the host already speaks OpenAI, stream + non-
// stream). Key rotation on 401/402/429/5xx, same as the Claude path.
function postOpenAIDirectWithRetry(bodyBuf, cres, model, logEntry) {
  const maxAttempts = Math.max(1, KEY_POOL.length);
  let attempts = 0;
  function attempt() {
    if (!KEY_POOL.length) { proxyError(cres, logEntry, new Error("no keys in pool")); return; }
    const key = currentKey();
    attempts++;
    const headers = buildOpenAIUpstreamHeaders(bodyBuf, key);
    const up = https.request({ host: CFG.upstreamOpenai, port: 443, method: "POST", path: "/v1/chat/completions", headers }, (upres) => {
      const st = upres.statusCode || 0;
      const s = ensureStats(key);
      s.lastStatus = st; s.lastUsed = new Date().toISOString(); s.count++;
      if (ROTATE_STATUSES.has(st) && attempts < maxAttempts) {
        upres.resume();
        if (st === 401) s.bad = true; else s.limited++;
        const note = maskKey(key) + "->" + st;
        advancePointer();
        logEntry.note = logEntry.note ? logEntry.note + " " + note + "->retry" : note + "->retry";
        attempt();
        return;
      }
      if (st >= 200 && st < 300) s.ok++;
      logEntry.status = st;
      // pass upstream headers through but fix framing for the client
      cres.writeHead(st || 502, upres.headers);
      upres.pipe(cres);
      upres.on("end", () => pushLog(logEntry));
    });
    up.on("error", (e) => {
      if (attempts < maxAttempts) { advancePointer(); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "neterr->retry"; attempt(); }
      else proxyError(cres, logEntry, e);
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
    if (m.role === "user" || m.role === "assistant") {
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
    else if (b.type === "tool_use") parts.push(null); // handled via assistant tool_calls below
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
  const data = [];
  if (upres.statusCode !== 200) {
    upres.on("data", (c) => data.push(c));
    upres.on("end", () => {
      logEntry.status = upres.statusCode; pushLog(logEntry);
      const text = Buffer.concat(data).toString("utf8");
      cres.writeHead(upres.statusCode || 502, { "content-type": "application/json" });
      cres.end(text);
    });
    return;
  }
  logEntry.status = 200;
  cres.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", "connection": "keep-alive" });
  const msgId = "msg_" + crypto.randomUUID();
  const emit = (obj) => cres.write("event: " + obj.type + "\ndata: " + JSON.stringify(obj) + "\n\n");
  emit({ type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 0, output_tokens: 0 } } });
  emit({ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } });
  let buf = "";
  let finish = "end_turn";
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
      const d = ev.choices && ev.choices[0] && ev.choices[0].delta;
      if (d && typeof d.content === "string" && d.content) {
        emit({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: d.content } });
      }
      const fr = ev.choices && ev.choices[0] && ev.choices[0].finish_reason;
      if (fr) finish = fr === "stop" ? "end_turn" : fr === "tool_calls" ? "tool_use" : fr === "length" ? "max_tokens" : "end_turn";
    }
  });
  upres.on("end", () => {
    emit({ type: "content_block_stop", index: 0 });
    emit({ type: "message_delta", delta: { stop_reason: finish, stop_sequence: null }, usage: { output_tokens: 0 } });
    emit({ type: "message_stop" });
    cres.end();
    pushLog(logEntry);
  });
}

// POST to upstream /v1/messages?beta=true with key rotation. On a rotatable
// status (401/402/429/5xx) it advances to the next key and retries the SAME
// request — at most once per key in the pool — so the client sees no break.
function postWithRetry(bodyBuf, cres, mode, model, logEntry) {
  const maxAttempts = Math.max(1, KEY_POOL.length);
  let attempts = 0;
  function dispatch(upres) {
    if (mode === "anthropic-stream") respondAnthropicStream(upres, cres, logEntry);
    else if (mode === "anthropic-nonstream") respondAnthropicNonStream(upres, cres, logEntry);
    else if (mode === "openai-stream") respondOpenAIStream(upres, cres, model, logEntry);
    else respondOpenAINonStream(upres, cres, model, logEntry); // openai-nonstream
  }
  function attempt() {
    if (!KEY_POOL.length) { proxyError(cres, logEntry, new Error("no keys in pool")); return; }
    const key = currentKey();
    attempts++;
    const headers = buildUpstreamHeaders(bodyBuf, key);
    const up = https.request({ host: CFG.upstream, port: 443, method: "POST", path: "/v1/messages?beta=true", headers }, (upres) => {
      const st = upres.statusCode || 0;
      const s = ensureStats(key);
      s.lastStatus = st; s.lastUsed = new Date().toISOString(); s.count++;
      if (ROTATE_STATUSES.has(st) && attempts < maxAttempts) {
        upres.resume(); // drain & discard the error body, free the socket
        if (st === 401) s.bad = true; else s.limited++;
        const note = maskKey(key) + "->" + st;
        advancePointer();
        logEntry.note = logEntry.note ? logEntry.note + " " + note + "->retry" : note + "->retry";
        attempt();
        return;
      }
      if (st >= 200 && st < 300) s.ok++;
      dispatch(upres);
    });
    up.on("error", (e) => {
      if (attempts < maxAttempts) { advancePointer(); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "neterr->retry"; attempt(); }
      else proxyError(cres, logEntry, e);
    });
    up.write(bodyBuf);
    up.end();
  }
  attempt();
}

// ─── UI helpers ───────────────────────────────────────────────────────────
function sendJson(cres, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  cres.writeHead(code, { "content-type": "application/json", "content-length": buf.length });
  cres.end(buf);
}
function readJsonBody(creq, cb) {
  const ch = [];
  creq.on("data", (c) => ch.push(c));
  creq.on("end", () => {
    let b;
    try { b = JSON.parse(Buffer.concat(ch).toString("utf8") || "{}"); } catch { b = {}; }
    cb(b);
  });
}

// ─── server ───────────────────────────────────────────────────────────────
const server = http.createServer((creq, cres) => {
  const url = creq.url.split("?")[0];

  // UI
  if (url === "/" || url === "/ui" || url === "/index.html") {
    try {
      const html = fs.readFileSync(UI_FILE, "utf8");
      const buf = Buffer.from(html);
      cres.writeHead(200, { "content-type": "text/html; charset=utf-8", "content-length": buf.length });
      cres.end(buf);
    } catch (e) { sendJson(cres, 500, { error: "ui.html missing: " + e.message }); }
    return;
  }

  // UI backend
  if (url === "/api/status") {
    sendJson(cres, 200, {
      ok: true,
      port: CFG.port,
      upstream: CFG.upstream,
      keys: { total: KEY_POOL.length, current: keyIdx, currentMasked: maskKey(currentKey()) },
      device_id: DEVICE_ID,
      session_id: SESSION_ID,
      version: require("./package.json").version,
      uptime_s: Math.round(process.uptime()),
      endpoints: ["POST /v1/messages (Anthropic; claude-* → cc host, gpt-* → api host)", "POST /v1/chat/completions (OpenAI; gpt-* → api host direct, claude-* → cc host)", "GET /v1/models (merged: Claude + GPT)"],
      upstreams: { claude: CFG.upstream, openai: CFG.upstreamOpenai },
      fingerprint: { capturedFrom: FP.capturedFrom, userAgent: FP.userAgent, profileFile: FINGERPRINT_FILE },
    });
    return;
  }
  if (url === "/api/logs") { sendJson(cres, 200, { logs: LOGS.slice().reverse() }); return; }

  // Fingerprint profile (read + update without editing code)
  if (url === "/api/fingerprint") {
    if (creq.method === "GET") {
      sendJson(cres, 200, { ...FP, profileFile: FINGERPRINT_FILE });
      return;
    }
    if (creq.method === "PUT") {
      readJsonBody(creq, (body) => {
        const next = { ...FP };
        for (const k of Object.keys(FINGERPRINT_DEFAULTS)) if (body[k] != null && typeof body[k] === "string") next[k] = body[k];
        try {
          fs.mkdirSync(CONFIG_DIR, { recursive: true });
          fs.writeFileSync(FINGERPRINT_FILE, JSON.stringify(next, null, 2) + "\n");
          // live-apply in-process
          Object.assign(FP, next);
          pushLog({ kind: "api", method: "PUT", path: "/api/fingerprint", status: 200, note: "profile updated" });
          sendJson(cres, 200, { ok: true, profile: { ...FP, profileFile: FINGERPRINT_FILE }, note: "applied live; restart to re-read file if edited externally" });
        } catch (e) { sendJson(cres, 500, { ok: false, error: e.message }); }
      });
      return;
    }
  }

  // Key pool management (UI + CLI keys.js + AI agents)
  if (url === "/api/keys") {
    if (creq.method === "GET") {
      reloadKeyStats();
      sendJson(cres, 200, {
        total: KEY_POOL.length,
        current: keyIdx,
        keys: KEY_POOL.map((k, i) => {
          const s = keyStats[k] || {};
          return {
            index: i,
            masked: maskKey(k),
            current: i === keyIdx,
            ok: s.ok || 0,
            limited: s.limited || 0,
            bad: !!s.bad,
            count: s.count || 0,
            lastStatus: s.lastStatus,
            lastUsed: s.lastUsed,
          };
        }),
      });
      return;
    }
    if (creq.method === "POST") {
      readJsonBody(creq, (body) => {
        const added = [];
        const add = (v) => {
          if (typeof v === "string" && v.trim() && !KEY_POOL.includes(v.trim())) { KEY_POOL.push(v.trim()); ensureStats(v.trim()); added.push(v.trim()); }
        };
        if (Array.isArray(body.keys)) body.keys.forEach(add);
        else if (body.key) add(body.key);
        persistKeys();
        sendJson(cres, 200, { ok: true, added: added.length, total: KEY_POOL.length });
      });
      return;
    }
    if (creq.method === "DELETE") {
      readJsonBody(creq, (body) => {
        let removed = false;
        if (typeof body.index === "number" && KEY_POOL[body.index] !== undefined) {
          KEY_POOL.splice(body.index, 1); removed = true;
        } else if (typeof body.key === "string") {
          const i = KEY_POOL.indexOf(body.key.trim());
          if (i >= 0) { KEY_POOL.splice(i, 1); removed = true; }
        }
        if (removed) {
          keyIdx = KEY_POOL.length ? keyIdx % KEY_POOL.length : 0;
          persistKeys();
          sendJson(cres, 200, { ok: true, removed: true, total: KEY_POOL.length });
        } else {
          sendJson(cres, 404, { ok: false, error: "key not found" });
        }
      });
      return;
    }
    return;
  }
  // Find the first working key (forward probe, stop on first 200, lock pointer).
  if (url === "/api/keys/find" && creq.method === "POST") {
    readJsonBody(creq, async () => { const r = await findFirstWorking(); sendJson(cres, 200, r); });
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
      const isGpt = isGptModel(model);
      // Route through the REAL endpoint (loopback) so the test exercises the
      // exact path a client would use: chosen protocol × model family.
      const path = proto === "anthropic" ? "/v1/messages" : "/v1/chat/completions";
      const body = proto === "anthropic"
        ? { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: [{ type: "text", text: prompt }] }] }
        : { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: prompt }] };
      const buf = Buffer.from(JSON.stringify(body));
      const logEntry = { kind: "test", method: "POST", path: path + "(" + proto + ")", status: null, note: model + (isGpt ? " [gpt]" : " [claude]") };
      const up = http.request({ host: "127.0.0.1", port: CFG.port, method: "POST", path, headers: { "content-type": "application/json", "content-length": buf.length } }, (upres) => {
        const data = []; upres.on("data", (c) => data.push(c));
        upres.on("end", () => {
          const text = Buffer.concat(data).toString("utf8");
          const st = upres.statusCode || 502;
          logEntry.status = st; pushLog(logEntry);
          if (st !== 200) { sendJson(cres, st, { ok: false, model, proto, path, status: st, raw: text }); return; }
          // extract text from either response shape
          let out = text;
          try {
            const d = JSON.parse(text);
            if (proto === "anthropic") {
              const txt = (d.content || []).filter((b) => b.type === "text").map((b) => b.text).join("");
              if (txt) out = txt;
            } else {
              const c = d.choices && d.choices[0] && d.choices[0].message && d.choices[0].message.content;
              if (c) out = c;
            }
          } catch {}
          sendJson(cres, 200, { ok: true, model, proto, path, upstream: isGpt ? CFG.upstreamOpenai : CFG.upstream, status: st, raw: out });
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
      // GPT model via Anthropic protocol → translate to OpenAI, hit api.freemodel.dev
      if (isGptModel(parsed.model)) {
        const oai = anthropicToOpenAIRequest(parsed);
        const clientStream = oai.stream === true;
        const obuf = Buffer.from(JSON.stringify(oai));
        const logEntry = { kind: "openai", method: "POST", path: "/v1/messages→gpt", status: null, note: parsed.model + (clientStream ? " stream" : " nonstream") };
        if (clientStream) {
          // stream OpenAI response → translate SSE to Anthropic events
          const headers = buildOpenAIUpstreamHeaders(obuf, currentKey());
          const up = https.request({ host: CFG.upstreamOpenai, port: 443, method: "POST", path: "/v1/chat/completions", headers }, (upres) => openAIStreamToAnthropic(upres, cres, parsed.model, logEntry));
          up.on("error", (e) => proxyError(cres, logEntry, e));
          up.write(obuf); up.end();
        } else {
          // non-stream: buffer OpenAI JSON, translate one-shot to Anthropic Message
          const maxAttempts = Math.max(1, KEY_POOL.length);
          let attempts = 0;
          (function attempt() {
            if (!KEY_POOL.length) { proxyError(cres, logEntry, new Error("no keys in pool")); return; }
            const key = currentKey(); attempts++;
            const up = https.request({ host: CFG.upstreamOpenai, port: 443, method: "POST", path: "/v1/chat/completions", headers: buildOpenAIUpstreamHeaders(obuf, key) }, (upres) => {
              const st = upres.statusCode || 0;
              const s = ensureStats(key); s.lastStatus = st; s.lastUsed = new Date().toISOString(); s.count++;
              if (ROTATE_STATUSES.has(st) && attempts < maxAttempts) {
                upres.resume(); if (st === 401) s.bad = true; else s.limited++;
                logEntry.note = maskKey(key) + "->" + st + "->retry"; advancePointer(); attempt(); return;
              }
              if (st >= 200 && st < 300) s.ok++;
              const data = []; upres.on("data", (c) => data.push(c));
              upres.on("end", () => {
                const text = Buffer.concat(data).toString("utf8");
                logEntry.status = st; pushLog(logEntry);
                if (st !== 200) { cres.writeHead(st || 502, { "content-type": "application/json" }); cres.end(text); return; }
                let oai; try { oai = JSON.parse(text); } catch { cres.writeHead(502, { "content-type": "application/json" }); cres.end(JSON.stringify({ type: "error", error: { type: "proxy_error", message: "bad upstream json" } })); return; }
                const ant = openAIResponseToAnthropic(oai);
                const buf = Buffer.from(JSON.stringify(ant));
                cres.writeHead(200, { "content-type": "application/json", "content-length": buf.length });
                cres.end(buf);
              });
            });
            up.on("error", (e) => { if (attempts < maxAttempts) { advancePointer(); attempt(); } else proxyError(cres, logEntry, e); });
            up.write(obuf); up.end();
          })();
        }
        return;
      }
      // Claude model → fingerprint + cc.freemodel.dev (existing path)
      const inj = injectFingerprint(raw);
      if (!inj.ok) { sendJson(cres, 400, { type: "error", error: { type: "invalid_request", message: "body is not valid JSON" } }); return; }
      const buf = Buffer.from(inj.body);
      const mode = inj.clientStream ? "anthropic-stream" : "anthropic-nonstream";
      const logEntry = { kind: "anthropic", method: "POST", path: "/v1/messages", status: null, note: inj.clientStream ? "stream" : "nonstream" };
      postWithRetry(buf, cres, mode, inj.model, logEntry);
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
      // GPT model → forward OpenAI body as-is to api.freemodel.dev (no translation, no fingerprint)
      if (isGptModel(oai.model)) {
        const obuf = Buffer.from(JSON.stringify(oai));
        const logEntry = { kind: "openai", method: "POST", path: "/v1/chat/completions→gpt", status: null, note: oai.model + (oai.stream === true ? " stream" : " nonstream") };
        postOpenAIDirectWithRetry(obuf, cres, oai.model, logEntry);
        return;
      }
      // Claude model → translate OpenAI→Anthropic, fingerprint, cc.freemodel.dev
      const ant = oaiToAnthropic(oai);
      const inj = injectFingerprint(JSON.stringify(ant));
      if (!inj.ok) { sendJson(cres, 400, { error: { message: "failed to translate request", type: "invalid_request_error" } }); return; }
      const buf = Buffer.from(inj.body);
      const mode = oai.stream === true ? "openai-stream" : "openai-nonstream";
      const logEntry = { kind: "openai", method: "POST", path: "/v1/chat/completions", status: null, note: oai.stream === true ? "stream" : "nonstream" };
      postWithRetry(buf, cres, mode, oai.model || "claude", logEntry);
    });
    return;
  }

  // API: /v1/models — merged: Claude from cc.freemodel.dev + GPT from api.freemodel.dev
  if (url === "/v1/models" || url.startsWith("/v1/models")) {
    const logEntry = { kind: "api", method: "GET", path: "/v1/models", status: null };
    function fetchModels(host) {
      return new Promise((resolve) => {
        const headers = { host, "user-agent": UA, "accept": "application/json" };
        const k = currentKey();
        if (k) headers["authorization"] = "Bearer " + k;
        if (host === CFG.upstream) headers["x-api-key"] = k;
        const up = https.request({ host, port: 443, method: "GET", path: "/v1/models", headers }, (upres) => {
          const data = []; upres.on("data", (c) => data.push(c));
          upres.on("end", () => {
            try { resolve(JSON.parse(Buffer.concat(data).toString("utf8")).data || []); }
            catch { resolve([]); }
          });
        });
        up.on("error", () => resolve([]));
        up.setTimeout(10000, () => { try { up.destroy(); } catch {} resolve([]); });
        up.end();
      });
    }
    Promise.all([fetchModels(CFG.upstream), fetchModels(CFG.upstreamOpenai)]).then(([claude, gpt]) => {
      logEntry.status = 200; pushLog(logEntry);
      const data = [].concat(claude, gpt);
      const buf = Buffer.from(JSON.stringify({ object: "list", data }));
      cres.writeHead(200, { "content-type": "application/json", "content-length": buf.length });
      cres.end(buf);
    });
    return;
  }

  sendJson(cres, 404, { error: { message: "Not found: " + url, type: "not_found_error" } });
});

server.listen(CFG.port, "127.0.0.1", () => {
  console.log("freemodel-cc-proxy " + require("./package.json").version);
  console.log("  Anthropic: http://127.0.0.1:" + CFG.port + "/v1/messages  (claude-* → cc, gpt-* → api)");
  console.log("  OpenAI    : http://127.0.0.1:" + CFG.port + "/v1/chat/completions  (gpt-* → api direct, claude-* → cc)");
  console.log("  Models    : http://127.0.0.1:" + CFG.port + "/v1/models  (merged Claude + GPT)");
  console.log("  UI        : http://127.0.0.1:" + CFG.port + "/");
  console.log("  upstreams : claude=https://" + CFG.upstream + "  openai=https://" + CFG.upstreamOpenai);
  console.log("  keys      : " + KEY_POOL.length + " (active: " + maskKey(currentKey()) + ", rotate on 401/402/429/5xx)");
});
