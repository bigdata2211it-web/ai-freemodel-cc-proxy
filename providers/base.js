// providers/base.js — shared infrastructure + ProviderBase.
//
// Everything that is NOT provider-specific lives here: safe client IO, error
// normalization, hop-by-hop header filtering, the request log, the OpenAI⇄
// Anthropic translators, the SSE collectors/responders, and the generic
// key-rotation + forwarding loop. Provider drivers (freemodel.js,
// openai-compat.js) extend ProviderBase and only implement the parts that are
// true to THEIR backend: route(), headersFor(), prepareAnthropicBody(),
// prepareOpenAIBody(), listModelsEndpoints(), probeKey().
//
// The shell (index.js) never branches on provider kind. It calls provider.X().
"use strict";

const https = require("https");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

// Rotate to another key only on auth/rate-limit errors. 5xx is an upstream
// outage, not a key problem — rotating would burn the whole pool in one
// request and mark every key "limited" for nothing. 5xx is returned to the
// client as-is (normalized to the right error shape).
const ROTATE_STATUSES = new Set([401, 402, 403, 429]);

// ─── safe client IO ──────────────────────────────────────────────────────
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

// ─── error normalization ─────────────────────────────────────────────────
// Upstream error bodies (e.g. cc/api.freemodel.dev send {"error":"..."} as a
// STRING) are coerced into the canonical shape each client SDK expects.
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
  const body = shape === "openai" ? normalizeOpenAIError(e.message, 502) : normalizeAnthropicError(e.message, 502);
  const buf = Buffer.from(JSON.stringify(body));
  safeWriteHead(cres, 502, { "content-type": "application/json", "content-length": buf.length });
  safeEnd(cres, buf);
}

// ─── request log (one global ring buffer + disk) ─────────────────────────
const LOGS = [];
const MAX_LOGS = 200;
let _LOG_FILE = null;
function setLogFile(p) { _LOG_FILE = p; }
function pushLog(entry) {
  entry.ts = new Date().toISOString();
  LOGS.push(entry);
  if (LOGS.length > MAX_LOGS) LOGS.shift();
  try {
    if (_LOG_FILE) {
      fs.mkdirSync(path.dirname(_LOG_FILE), { recursive: true });
      fs.appendFileSync(_LOG_FILE, `[${entry.ts}] ${entry.kind} ${entry.method} ${entry.path} ${entry.status || ""} ${entry.note || ""}\n`);
    }
  } catch {}
}
function getLogs() { return LOGS.slice().reverse(); }

function maskKey(k) { return k ? k.slice(0, 8) + "…" + k.slice(-4) : "(none)"; }

// ─── upstream header builders (parameterized by ua + fingerprint) ────────
// Anthropic-shape upstream (freemodel cc host): carries the Claude Code
// fingerprint markers the gate checks. key goes both as x-api-key and
// Authorization: Bearer.
function buildAnthropicUpstreamHeaders(bodyBuf, key, host, ua, fp) {
  const headers = {
    "host": host,
    "user-agent": ua,
    "x-app": "cli",
    "anthropic-dangerous-direct-browser-access": "true",
    "anthropic-version": fp.anthropicVersion,
    "anthropic-beta": fp.anthropicBeta,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  if (key) { headers["x-api-key"] = key; headers["authorization"] = "Bearer " + key; }
  return headers;
}
// OpenAI-shape upstream (freemodel api host OR openai-compat baseUrl). Bearer only.
function buildOpenAIUpstreamHeaders(bodyBuf, key, host, ua) {
  const headers = {
    "host": host,
    "user-agent": ua,
    "accept": "application/json",
    "content-type": "application/json",
  };
  if (bodyBuf && bodyBuf.length) headers["content-length"] = Buffer.byteLength(bodyBuf);
  if (key) headers["authorization"] = "Bearer " + key;
  return headers;
}

// ─── SSE collector: reassemble a non-stream Anthropic Message from SSE ────
function collectAnthropicMessage(sseText) {
  const blocks = {};
  const order = [];
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
      default: break;
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

// Anthropic Messages request → OpenAI Chat Completions request (for an
// Anthropic-protocol client hitting an OpenAI-shape upstream).
function anthropicToOpenAIRequest(ant) {
  const out = {
    model: ant.model || "gpt-5.4-mini",
    max_tokens: ant.max_tokens || 4096,
    messages: [],
    stream: ant.stream === true,
  };
  // Ask the OpenAI host to include a final usage chunk in the stream so we can
  // populate Anthropic `usage.output_tokens` for streaming (otherwise 0).
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
    else if (b.type === "tool_use") continue; // handled in the assistant message loop
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

// ─── responders: (upstreamProto x clientProto x stream) matrix ───────────
function respondAnthropicStream(upres, cres, logEntry) {
  logEntry.status = upres.statusCode;
  if (!safeWriteHead(cres, upres.statusCode || 502, filterHeaders(upres.headers))) { try { upres.destroy(); } catch {} return; }
  upres.pipe(cres);
  upres.on("error", () => { try { cres.destroy(); upres.destroy(); } catch {} });
  cres.on("error", () => { try { upres.destroy(); } catch {} });
  upres.on("end", () => pushLog(logEntry));
}

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

// OpenAI-shape upstream -> OpenAI-shape client: pipe straight through. Only
// the error path is normalized; on 200 we filter hop-by-hop headers and pipe.
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

// OpenAI-shape upstream -> Anthropic-shape client, non-stream.
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

// Translate OpenAI Chat Completions SSE → Anthropic Messages SSE.
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

  let blockIdx = -1;
  let textOpen = false;
  const tcMap = {};
  let outputTokens = 0;
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
      if (ev.usage) { outputTokens = ev.usage.completion_tokens || outputTokens; }
    }
  });
  upres.on("end", () => {
    for (const oi of Object.keys(tcMap)) emit({ type: "content_block_stop", index: tcMap[oi].block });
    if (textOpen) emit({ type: "content_block_stop", index: blockIdx });
    if (blockIdx < 0) {
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── ProviderBase: per-provider key pool + generic forwarding ────────────
// A provider instance owns its OWN key pool, rotation cursor, and stats — no
// shared global KEY_POOL/keyIdx. The shell picks a provider and calls its
// methods; it never touches the pool directly.
class ProviderBase {
  constructor(config, opts) {
    this.configDir = opts.configDir;
    this.ua = opts.ua;
    // copy serializable fields (id, name, kind, keyPrefix, needsFingerprint, note, ...)
    Object.assign(this, config);
    this.pool = this._loadPool();
    this.stats = {};
    this.pool.keys.forEach((k) => this.ensureStats(k));
  }

  // serializable config (written to providers.json)
  toJSON() {
    const out = { id: this.id, name: this.name, kind: this.kind, enabled: this.enabled !== false,
                  keyPrefix: this.keyPrefix, needsFingerprint: !!this.needsFingerprint, note: this.note };
    if (this.kind === "freemodel") { out.claudeUpstream = this.claudeUpstream; out.openaiUpstream = this.openaiUpstream; }
    else { out.baseUrl = this.baseUrl; out.probeModel = this.probeModel; }
    return out;
  }

  _keysFile() { return path.join(this.configDir, "keys-" + this.id + ".json"); }
  _loadPool() {
    let keys = [];
    try { const j = JSON.parse(fs.readFileSync(this._keysFile(), "utf8")); if (Array.isArray(j.keys)) keys = j.keys.filter((k) => typeof k === "string" && k); } catch {}
    return { keys, idx: 0 };
  }
  persistPool() {
    try { fs.mkdirSync(this.configDir, { recursive: true }); fs.writeFileSync(this._keysFile(), JSON.stringify({ keys: this.pool.keys }, null, 2) + "\n"); } catch {}
  }
  ensureStats(k) { if (!this.stats[k]) this.stats[k] = { ok: 0, limited: 0, bad: false, count: 0, lastStatus: null, lastUsed: null }; return this.stats[k]; }
  currentKey() { return this.pool.keys[this.pool.idx]; }
  reloadStats() { this.pool.keys.forEach((k) => this.ensureStats(k)); }
  // Per-request cursor advance: next non-bad key index after `from`, WITHOUT
  // mutating the shared pool.idx. Commits pool.idx only on a 200.
  nextUsable(from) {
    if (this.pool.keys.length === 0) return from;
    for (let i = 0; i < this.pool.keys.length; i++) {
      const idx = (from + 1 + i) % this.pool.keys.length;
      const s = this.stats[this.pool.keys[idx]];
      if (!s || !s.bad) return idx;
    }
    return (from + 1) % this.pool.keys.length;
  }

  // ── subclass interface (override these) ──
  route(/* clientProto, model */) { throw new Error("route() not implemented"); }
  headersFor(/* route, bodyBuf, key */) { throw new Error("headersFor() not implemented"); }
  prepareAnthropicBody(/* parsed, raw */) { throw new Error("prepareAnthropicBody() not implemented"); }
  prepareOpenAIBody(/* oai */) { throw new Error("prepareOpenAIBody() not implemented"); }
  listModelsEndpoints() { throw new Error("listModelsEndpoints() not implemented"); }
  // Cheap key validation: GET /v1/models (metadata, costs 0 tokens). Override
  // only if a provider has a cheaper/different health endpoint.
  probeKey(/* key */) { throw new Error("probeKey() not implemented"); }

  // ── model catalog cache (for union routing + /v1/models) ──
  // Each provider fetches its own /v1/models and caches the id list. The shell
  // builds a {modelId -> providerId} map from all enabled providers' caches to
  // route /v1/* by model id without any "active provider" concept.
  _modelsCache = null;
  _modelsAt = 0;
  async fetchModels(ttlMs = 60000) {
    if (this._modelsCache && (Date.now() - this._modelsAt) < ttlMs) return this._modelsCache;
    const eps = this.listModelsEndpoints();
    const results = await Promise.all(eps.map((ep) => new Promise((resolve) => {
      const key = this.currentKey();
      const route = { host: ep.host, port: ep.port, upstreamProto: ep.anthropic ? "anthropic" : "openai" };
      const headers = this.headersFor(route, null, key); headers["accept"] = "application/json";
      const up = https.request({ host: ep.host, port: ep.port, method: "GET", path: ep.path, headers }, (upres) => {
        const data = []; upres.on("data", (c) => data.push(c));
        upres.on("end", () => { try { resolve(JSON.parse(Buffer.concat(data).toString("utf8")).data || []); } catch { resolve([]); } });
      });
      up.on("error", () => resolve([]));
      up.setTimeout(10000, () => { try { up.destroy(); } catch {} resolve([]); });
      up.end();
    })));
    const ids = [].concat(...results).map((m) => (m && m.id) || m).filter(Boolean);
    this._modelsCache = ids; this._modelsAt = Date.now();
    return ids;
  }
  servesModel(id) { return !!id && !!this._modelsCache && this._modelsCache.includes(id); }
  invalidateModels() { this._modelsCache = null; this._modelsAt = 0; }

  // ── generic forwarding with per-request key rotation ──
  // route + bodyBuf come from route()/prepareX(); the responder is picked from
  // the (upstreamProto x clientProto x stream) matrix. No provider-specific
  // logic here — this loop is identical for every provider.
  forwardWithRetry(route, bodyBuf, clientProto, clientStream, model, cres, logEntry) {
    const pool = this.pool;
    const maxAttempts = Math.max(1, pool.keys.length);
    let attempts = 0;
    let cursor = pool.idx;
    const errShape = clientProto === "openai" ? "openai" : "anthropic";

    const dispatch = (upres) => {
      const up = route.upstreamProto;
      if (up === "anthropic" && clientProto === "anthropic")
        return clientStream ? respondAnthropicStream(upres, cres, logEntry) : respondAnthropicNonStream(upres, cres, logEntry);
      if (up === "anthropic" && clientProto === "openai")
        return clientStream ? respondOpenAIStream(upres, cres, model, logEntry) : respondOpenAINonStream(upres, cres, model, logEntry);
      if (up === "openai" && clientProto === "openai")
        return respondOpenAIPassthrough(upres, cres, logEntry);
      return clientStream ? openAIStreamToAnthropic(upres, cres, model, logEntry) : respondOpenAIToAnthropicNonStream(upres, cres, model, logEntry);
    };

    const attempt = () => {
      if (!pool.keys.length) { proxyError(cres, logEntry, new Error("no keys in pool"), errShape); return; }
      const key = pool.keys[cursor];
      attempts++;
      const headers = this.headersFor(route, bodyBuf, key);
      const up = https.request({ host: route.host, port: route.port, method: "POST", path: route.path, headers }, (upres) => {
        const st = upres.statusCode || 0;
        const s = this.ensureStats(key);
        s.lastStatus = st; s.lastUsed = new Date().toISOString(); s.count++;
        if (ROTATE_STATUSES.has(st) && attempts < maxAttempts) {
          upres.resume();
          if (st === 401 || st === 402 || st === 403) s.bad = true; else if (st === 429) s.limited++;
          const note = maskKey(key) + "->" + st;
          cursor = this.nextUsable(cursor);
          logEntry.note = logEntry.note ? logEntry.note + " " + note + "->retry" : note + "->retry";
          attempt();
          return;
        }
        logEntry.status = st;
        if (st >= 200 && st < 300) { s.ok++; pool.idx = cursor; }
        dispatch(upres);
      });
      up.on("error", (e) => {
        if (attempts < maxAttempts) { cursor = this.nextUsable(cursor); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "neterr->retry"; attempt(); }
        else proxyError(cres, logEntry, e, errShape);
      });
      up.setTimeout(180000, () => { try { up.destroy(); } catch {}
        if (attempts < maxAttempts) { cursor = this.nextUsable(cursor); logEntry.note = (logEntry.note ? logEntry.note + " " : "") + "timeout->retry"; attempt(); }
        else proxyError(cres, logEntry, new Error("upstream timeout"), errShape);
      });
      up.write(bodyBuf);
      up.end();
    };
    attempt();
  }

  // ── cheap health probe: REMOVED. `/v1/models` is open (no auth) on both
  // FreeModel and OpenCode, and a bogus-model POST returns the same 401 for a
  // bad key and a good key (model check runs before/with auth). There is no
  // zero-token way to validate a key. So: key health is decided LAZILY by real
  // client requests in forwardWithRetry (401->bad, 429->limited, 200->ok), and
  // the UI offers "reset marks" (no requests) instead of a probe.
  resetStats() {
    for (const k of this.pool.keys) {
      const s = this.ensureStats(k);
      s.bad = false; s.limited = 0;
    }
  }
}

module.exports = {
  ROTATE_STATUSES,
  safeWriteHead, safeEnd, safeWrite,
  filterHeaders,
  normalizeAnthropicError, normalizeOpenAIError, proxyError,
  setLogFile, pushLog, getLogs,
  maskKey,
  buildAnthropicUpstreamHeaders, buildOpenAIUpstreamHeaders,
  collectAnthropicMessage,
  oaiToAnthropic, anthropicToOpenAIRequest, openAIResponseToAnthropic,
  respondAnthropicStream, respondAnthropicNonStream,
  respondOpenAIStream, respondOpenAINonStream,
  respondOpenAIPassthrough, respondOpenAIToAnthropicNonStream, openAIStreamToAnthropic,
  ProviderBase,
};
