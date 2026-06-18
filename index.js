// freemodel-cc-proxy
// Local Anthropic Messages API backed by FreeModel's cc.freemodel.dev, which
// serves real Claude (Opus/Sonnet/Haiku) but gates access to the official
// Claude Code client. This proxy impersonates that client's request fingerprint
// so any Anthropic-compatible tool (Factory Droid, Claude Code, Cline, Cursor,
// Anthropic SDK) can use FreeModel's Claude through one local endpoint.
//
// Run:  FMCC_KEY=fe_oa_<...> node index.js
// API:  http://127.0.0.1:11440  (/v1/messages, /v1/models)
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
  const cfg = { port: 11440, upstream: "cc.freemodel.dev", key: "" };
  try { Object.assign(cfg, JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"))); } catch {}
  if (process.env.FMCC_PORT) cfg.port = parseInt(process.env.FMCC_PORT, 10);
  if (process.env.FMCC_UPSTREAM) cfg.upstream = process.env.FMCC_UPSTREAM;
  if (process.env.FMCC_KEY) cfg.key = process.env.FMCC_KEY;
  return cfg;
}
const CFG = loadConfig();
if (!CFG.key) {
  console.error("FMCC_KEY not set. Put your FreeModel key (fe_oa_...) in FMCC_KEY env");
  console.error("or in " + CONFIG_FILE + ' as {"key":"fe_oa_..."}.');
  process.exit(1);
}

// ─── fingerprint (captured from official Claude Code via mitmproxy) ───────
// cc.freemodel.dev validates the request BODY shape, not TLS or key.
// Required: stream:true, two leading system blocks, metadata.user_id as a
// JSON string with device_id/account_uuid/session_id, plus Claude Code headers.
const BILLING_HDR = "x-anthropic-billing-header: cc_version=2.1.179.efd; cc_entrypoint=sdk-cli; cch=fa71b;";
const AGENT_HDR = "You are a Claude agent, built on Anthropic's Claude Agent SDK.";
const ANTHROPIC_BETA =
  "claude-code-20250219,interleaved-thinking-2025-05-14,context-management-2025-06-27," +
  "prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24";
const UA = "claude-cli/2.1.179 (external, sdk-cli)";
const DEVICE_ID = crypto.randomBytes(32).toString("hex");
const SESSION_ID = crypto.randomUUID();

function injectFingerprint(bodyStr) {
  let d;
  try { d = JSON.parse(bodyStr); } catch { return { ok: false, body: bodyStr }; }
  d.stream = true; // gate requires streaming
  const sys = Array.isArray(d.system)
    ? d.system
    : typeof d.system === "string" ? [{ type: "text", text: d.system }] : [];
  d.system = [{ type: "text", text: BILLING_HDR }, { type: "text", text: AGENT_HDR }, ...sys];
  d.metadata = {
    user_id: JSON.stringify({ device_id: DEVICE_ID, account_uuid: "", session_id: SESSION_ID }),
  };
  return { ok: true, body: JSON.stringify(d), model: d.model };
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

// ─── upstream forward ─────────────────────────────────────────────────────
function forward(req, bodyBuf, cres, { inject }) {
  const headers = { ...req.headers };
  delete headers.host;
  delete headers["content-length"];
  delete headers.connection;
  delete headers["accept-encoding"]; // keep plaintext for logging
  headers["host"] = CFG.upstream;
  headers["user-agent"] = UA;
  headers["x-app"] = "cli";
  headers["anthropic-dangerous-direct-browser-access"] = "true";
  headers["anthropic-version"] = headers["anthropic-version"] || "2023-06-01";
  headers["anthropic-beta"] = ANTHROPIC_BETA;
  headers["accept"] = "application/json";
  const key = CFG.key || headers["x-api-key"];
  if (key) { headers["x-api-key"] = key; headers["authorization"] = "Bearer " + key; }
  headers["content-length"] = Buffer.byteLength(bodyBuf);

  const logEntry = { kind: "api", method: req.method, path: req.url, status: null };
  const up = https.request({ host: CFG.upstream, port: 443, method: req.method, path: req.url, headers }, (upres) => {
    logEntry.status = upres.statusCode;
    cres.writeHead(upres.statusCode || 502, upres.headers);
    upres.pipe(cres);
    upres.on("end", () => pushLog(logEntry));
  });
  up.on("error", (e) => {
    logEntry.status = 502; logEntry.note = e.message; pushLog(logEntry);
    try { cres.writeHead(502, { "content-type": "application/json" }); } catch {}
    cres.end(JSON.stringify({ type: "error", error: { type: "proxy_error", message: e.message } }));
  });
  up.write(bodyBuf);
  up.end();
}

// ─── UI helpers ───────────────────────────────────────────────────────────
function sendJson(cres, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj));
  cres.writeHead(code, { "content-type": "application/json", "content-length": buf.length });
  cres.end(buf);
}
function maskKey(k) { return k ? k.slice(0, 8) + "…" + k.slice(-4) : "(none)"; }

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
      key: maskKey(CFG.key),
      device_id: DEVICE_ID,
      session_id: SESSION_ID,
      version: require("./package.json").version,
      uptime_s: Math.round(process.uptime()),
    });
    return;
  }
  if (url === "/api/logs") { sendJson(cres, 200, { logs: LOGS.slice().reverse() }); return; }

  if (url === "/api/test" && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      let reqBody;
      try { reqBody = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { reqBody = {}; }
      const model = reqBody.model || "claude-opus-4-8";
      const prompt = reqBody.prompt || "Reply with exactly: ok";
      const body = JSON.stringify({
        model,
        max_tokens: 64,
        stream: false,
        messages: [{ role: "user", content: [{ type: "text", text: prompt }] }],
      });
      const injected = injectFingerprint(body);
      const buf = Buffer.from(injected.body);
      const headers = {
        "host": CFG.upstream,
        "user-agent": UA,
        "x-app": "cli",
        "anthropic-dangerous-direct-browser-access": "true",
        "anthropic-version": "2023-06-01",
        "anthropic-beta": ANTHROPIC_BETA,
        "accept": "application/json",
        "content-type": "application/json",
        "content-length": Buffer.byteLength(buf),
      };
      if (CFG.key) { headers["x-api-key"] = CFG.key; headers["authorization"] = "Bearer " + CFG.key; }
      const up = https.request({ host: CFG.upstream, port: 443, method: "POST", path: "/v1/messages?beta=true", headers }, (upres) => {
        let data = [];
        upres.on("data", (c) => data.push(c));
        upres.on("end", () => {
          const text = Buffer.concat(data).toString("utf8");
          pushLog({ kind: "test", method: "POST", path: "/v1/messages", status: upres.statusCode, note: model });
          const isErr = upres.statusCode !== 200;
          let out = text;
          if (!isErr) {
            // upstream always streams (gate forces stream:true); parse SSE deltas into plain text.
            const deltas = [];
            for (const line of text.split("\n")) {
              if (!line.startsWith("data:")) continue;
              const payload = line.slice(5).trim();
              if (!payload || payload === "[DONE]") continue;
              try {
                const ev = JSON.parse(payload);
                if (ev.type === "content_block_delta" && ev.delta && ev.delta.type === "text_delta") deltas.push(ev.delta.text);
                else if (ev.type === "error" && ev.error) { out = JSON.stringify(ev.error); return; }
              } catch {}
            }
            if (deltas.length) out = deltas.join("");
          }
          sendJson(cres, upres.statusCode, { ok: !isErr, model, status: upres.statusCode, raw: out });
        });
      });
      up.on("error", (e) => { pushLog({ kind: "test", method: "POST", path: "/v1/messages", status: 502, note: e.message }); sendJson(cres, 502, { ok: false, error: e.message }); });
      up.write(buf); up.end();
    });
    return;
  }

  // API: /v1/messages
  if (url === "/v1/messages") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      const inj = injectFingerprint(raw);
      const buf = Buffer.from(inj.body);
      // ?beta=true is what Claude Code sends; harmless on plain /v1/messages too
      const fwdUrl = creq.url; // keep client query string
      const fakeReq = { method: creq.method, url: fwdUrl, headers: creq.headers };
      forward(fakeReq, buf, cres, { inject: true });
    });
    return;
  }

  // API: /v1/models (passthrough, no injection)
  if (url === "/v1/models" || url.startsWith("/v1/models")) {
    const buf = Buffer.alloc(0);
    const fakeReq = { method: creq.method, url: creq.url, headers: creq.headers };
    forward(fakeReq, buf, cres, { inject: false });
    return;
  }

  sendJson(cres, 404, { type: "error", error: { type: "not_found", message: "Not found: " + url } });
});

server.listen(CFG.port, "127.0.0.1", () => {
  console.log("freemodel-cc-proxy " + require("./package.json").version);
  console.log("  API: http://127.0.0.1:" + CFG.port + "/v1/messages  (Anthropic Messages)");
  console.log("  API: http://127.0.0.1:" + CFG.port + "/v1/models");
  console.log("  UI : http://127.0.0.1:" + CFG.port + "/");
  console.log("  upstream: https://" + CFG.upstream + "  key: " + maskKey(CFG.key));
});
