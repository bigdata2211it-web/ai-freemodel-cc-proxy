// freemodel-cc-proxy — shell.
//
// HTTP shell ONLY. All provider-specific logic lives in providers/ (freemodel,
// openai-compat, sharing base.js). There is NO provider-kind branching here.
//
// Model: ALL enabled providers are active at once. /v1/* routes by model id —
// the shell finds the enabled provider that serves that model (via each
// provider's /v1/models cache) and forwards through it. There is no "active
// provider" selector; the only switch is per-provider enabled on/off. /v1/models
// returns the union of every enabled provider's catalog.
//
// Run:  node index.js      (keys live in ~/.freemodel-cc-proxy/keys-<id>.json)
// API:  http://127.0.0.1:11440  (/v1/messages, /v1/chat/completions, /v1/models)
// UI:   http://127.0.0.1:11440/
"use strict";

const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");

const {
  setLogFile, pushLog, getLogs, proxyError, maskKey,
  safeWriteHead, safeEnd,
} = require("./providers/base");
const { loadFingerprint, FINGERPRINT_DEFAULTS } = require("./providers/freemodel");
const { FreeModelProvider } = require("./providers/freemodel");
const { OpenAICompatProvider } = require("./providers/openai-compat");

// ─── config ──────────────────────────────────────────────────────────────
const CONFIG_DIR = process.env.FMCC_CONFIG_DIR || path.join(os.homedir(), ".freemodel-cc-proxy");
const CONFIG_FILE = path.join(CONFIG_DIR, "config.json");
const LOG_FILE = process.env.FMCC_LOG_FILE || path.join(CONFIG_DIR, "proxy.log");
const UI_FILE = path.join(__dirname, "ui.html");
const PROVIDERS_FILE = path.join(CONFIG_DIR, "providers.json");
const FINGERPRINT_FILE = process.env.FMCC_FINGERPRINT_FILE || path.join(CONFIG_DIR, "fingerprint.json");
const LEGACY_KEYS_FILE = path.join(CONFIG_DIR, "keys.json");
const LEGACY_PROFILE_FILE = path.join(CONFIG_DIR, "profile.json");

function loadCfg() {
  const cfg = { port: 11440 };
  try { Object.assign(cfg, JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"))); } catch {}
  if (process.env.FMCC_PORT) cfg.port = parseInt(process.env.FMCC_PORT, 10);
  return cfg;
}
const CFG = loadCfg();
setLogFile(LOG_FILE);

// ─── shared identity (used by the freemodel driver for its fingerprint) ───
const FP = loadFingerprint(FINGERPRINT_FILE);
const UA = FP.userAgent;
const DEVICE_ID = crypto.randomBytes(32).toString("hex");
const SESSION_ID = crypto.randomUUID();

// ─── providers registry ──────────────────────────────────────────────────
const DRIVERS = { freemodel: FreeModelProvider, "openai-compat": OpenAICompatProvider };

function defaultProviderConfigs() {
  const legacyCfg = (() => { try { return JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")); } catch { return {}; } })();
  const legacyProf = (() => { try { return JSON.parse(fs.readFileSync(LEGACY_PROFILE_FILE, "utf8")); } catch { return null; } })();
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

function loadProvidersState() {
  let d = { providers: defaultProviderConfigs() };
  let loaded = null;
  try { loaded = JSON.parse(fs.readFileSync(PROVIDERS_FILE, "utf8")); } catch {}
  if (loaded) {
    if (Array.isArray(loaded.providers) && loaded.providers.length) d.providers = loaded.providers;
  }
  return d;
}
const PROVIDERS = loadProvidersState();
function persistProviders() {
  try { fs.mkdirSync(CONFIG_DIR, { recursive: true }); fs.writeFileSync(PROVIDERS_FILE, JSON.stringify(PROVIDERS, null, 2) + "\n"); } catch {}
}

// One-time migration: legacy single-provider keys.json -> keys-freemodel.json.
(function migrateLegacyKeys() {
  try {
    const fmPath = path.join(CONFIG_DIR, "keys-freemodel.json");
    if (!fs.existsSync(fmPath) && fs.existsSync(LEGACY_KEYS_FILE)) {
      const j = JSON.parse(fs.readFileSync(LEGACY_KEYS_FILE, "utf8"));
      if (Array.isArray(j.keys) && j.keys.length) fs.writeFileSync(fmPath, JSON.stringify({ keys: j.keys }, null, 2) + "\n");
    }
  } catch {}
})();

const SHARED_OPTS = { configDir: CONFIG_DIR, ua: UA, fp: FP, fingerprintFile: FINGERPRINT_FILE, deviceId: DEVICE_ID, sessionId: SESSION_ID };
const INSTANCES = {}; // id -> provider instance
function instantiate(c) { const Drv = DRIVERS[c.kind] || OpenAICompatProvider; return new Drv(c, SHARED_OPTS); }
function rebuildInstances() {
  for (const id of Object.keys(INSTANCES)) delete INSTANCES[id];
  PROVIDERS.providers.forEach((c) => { INSTANCES[c.id] = instantiate(c); });
}
rebuildInstances();
function configOf(id) { return PROVIDERS.providers.find((p) => p.id === id); }
function isEnabled(inst) { const c = configOf(inst.id); return c ? c.enabled !== false : true; }
function enabledProviders() { return PROVIDERS.providers.map((c) => INSTANCES[c.id]).filter((p) => p && isEnabled(p)); }
function getProvider(id) { return INSTANCES[id]; }

// ─── model catalog: union of all enabled providers, for routing + /v1/models ─
// { modelId -> providerId }. Refreshed on boot, on a timer, and after key/toggle
// changes. Optional "providerId/model" syntax picks a specific provider even
// when several serve the same model id.
let ROUTE_MAP = {};        // modelId -> providerId
let ROUTE_MAP_AT = 0;
async function refreshRouteMap() {
  const map = {};
  await Promise.all(enabledProviders().map(async (p) => {
    const ids = await p.fetchModels(0); // force refresh
    ids.forEach((mid) => { if (!map[mid]) map[mid] = p.id; }); // first enabled wins
  }));
  ROUTE_MAP = map; ROUTE_MAP_AT = Date.now();
}
function providerForModel(modelId) {
  if (!modelId) return null;
  // explicit "provider/model" override
  if (modelId.includes("/")) {
    const [pid, mid] = modelId.split(/\/(.+)/);
    const p = INSTANCES[pid];
    if (p && isEnabled(p)) return { provider: p, model: mid };
  }
  const pid = ROUTE_MAP[modelId];
  if (pid) { const p = INSTANCES[pid]; if (p && isEnabled(p)) return { provider: p, model: modelId }; }
  return null;
}
// boot + periodic refresh of the route map
refreshRouteMap();
setInterval(refreshRouteMap, 60000);

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

  // CORS: allow browser clients from any origin; preflight short-circuit.
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

  // query param helper (?provider= on key endpoints)
  const qIdx = creq.url.indexOf("?");
  const queryStr = qIdx >= 0 ? creq.url.slice(qIdx + 1) : "";
  function queryParam(name) {
    for (const pair of queryStr.split("&")) {
      const eq = pair.indexOf("="); if (eq < 0) continue;
      if (decodeURIComponent(pair.slice(0, eq)) === name) return decodeURIComponent(pair.slice(eq + 1));
    }
    return null;
  }
  // For key endpoints: ?provider=<id> required; default = first enabled.
  function providerForKeys() {
    const id = queryParam("provider");
    if (id && INSTANCES[id]) return INSTANCES[id];
    return enabledProviders()[0] || Object.values(INSTANCES)[0];
  }

  // ── providers management ──
  if (url === "/api/providers" && creq.method === "GET") {
    sendJson(cres, 200, {
      providers: PROVIDERS.providers.map((c) => ({
        ...c, keys: INSTANCES[c.id] ? INSTANCES[c.id].pool.keys.length : 0,
        models: INSTANCES[c.id] ? (INSTANCES[c.id]._modelsCache || []) : [],
      })),
      routeMap: ROUTE_MAP,
    });
    return;
  }
  if (url === "/api/providers" && creq.method === "POST") {
    readJsonBody(creq, async (body, err) => {
      if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
      const id = (typeof body.id === "string" && body.id) || ("prov" + crypto.randomBytes(3).toString("hex"));
      if (INSTANCES[id]) { sendJson(cres, 409, { ok: false, error: "provider id already exists" }); return; }
      const kind = body.kind === "freemodel" ? "freemodel" : "openai-compat";
      const c = { id, name: typeof body.name === "string" ? body.name : id, kind, enabled: body.enabled !== false,
        keyPrefix: typeof body.keyPrefix === "string" ? body.keyPrefix : "",
        needsFingerprint: kind === "freemodel", note: typeof body.note === "string" ? body.note : "" };
      if (kind === "freemodel") { c.claudeUpstream = body.claudeUpstream || "cc.freemodel.dev"; c.openaiUpstream = body.openaiUpstream || "api.freemodel.dev"; }
      else { c.baseUrl = body.baseUrl || ""; c.probeModel = body.probeModel || "";
        if (!c.baseUrl) { sendJson(cres, 400, { ok: false, error: "baseUrl required for openai-compat provider" }); return; } }
      PROVIDERS.providers.push(c); INSTANCES[id] = instantiate(c); persistProviders();
      await refreshRouteMap();
      pushLog({ kind: "api", method: "POST", path: "/api/providers", status: 200, note: "added " + id });
      sendJson(cres, 200, { ok: true, provider: c });
    });
    return;
  }
  if (url.startsWith("/api/providers/") && creq.method === "PUT") {
    const id = url.slice("/api/providers/".length).split("/")[0];
    const inst = INSTANCES[id]; const cfg = configOf(id);
    if (!inst || !cfg) { sendJson(cres, 404, { ok: false, error: "unknown provider id" }); return; }
    readJsonBody(creq, async (body, err) => {
      if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
      for (const k of ["name", "note", "keyPrefix", "claudeUpstream", "openaiUpstream", "baseUrl", "probeModel"]) {
        if (typeof body[k] === "string") { inst[k] = body[k]; cfg[k] = body[k]; }
      }
      if (typeof body.needsFingerprint === "boolean") { inst.needsFingerprint = body.needsFingerprint; cfg.needsFingerprint = body.needsFingerprint; }
      // per-provider enable/disable — the ONLY on/off switch (no global profile)
      if (typeof body.enabled === "boolean") { cfg.enabled = body.enabled; }
      persistProviders();
      await refreshRouteMap();
      pushLog({ kind: "api", method: "PUT", path: "/api/providers/" + id, status: 200, note: "enabled=" + cfg.enabled });
      sendJson(cres, 200, { ok: true, provider: cfg });
    });
    return;
  }
  if (url.startsWith("/api/providers/") && creq.method === "DELETE") {
    (async () => {
      const id = url.slice("/api/providers/".length).split("/")[0];
      if (PROVIDERS.providers.length <= 1) { sendJson(cres, 400, { ok: false, error: "cannot delete the last provider" }); return; }
      const i = PROVIDERS.providers.findIndex((p) => p.id === id);
      if (i < 0) { sendJson(cres, 404, { ok: false, error: "unknown provider id" }); return; }
      PROVIDERS.providers.splice(i, 1); delete INSTANCES[id];
      persistProviders(); await refreshRouteMap();
      pushLog({ kind: "api", method: "DELETE", path: "/api/providers/" + id, status: 200, note: "removed" });
      sendJson(cres, 200, { ok: true });
    })();
    return;
  }

  // ── status (global shell health) ──
  if (url === "/api/status") {
    const en = enabledProviders();
    sendJson(cres, 200, {
      ok: true, port: CFG.port,
      version: require("./package.json").version, uptime_s: Math.round(process.uptime()),
      enabledCount: en.length, totalProviders: PROVIDERS.providers.length,
      providers: PROVIDERS.providers.map((c) => ({
        id: c.id, name: c.name, kind: c.kind, enabled: c.enabled !== false,
        keys: INSTANCES[c.id] ? INSTANCES[c.id].pool.keys.length : 0,
        models: INSTANCES[c.id] ? (INSTANCES[c.id]._modelsCache || []) : [],
        baseUrl: c.baseUrl || null, claudeUpstream: c.claudeUpstream || null, openaiUpstream: c.openaiUpstream || null,
        keyPrefix: c.keyPrefix || null, needsFingerprint: !!c.needsFingerprint,
      })),
      endpoints: ["POST /v1/messages (Anthropic)", "POST /v1/chat/completions (OpenAI)", "GET /v1/models (union of enabled providers)"],
      fingerprint: { capturedFrom: FP.capturedFrom, userAgent: FP.userAgent, profileFile: FINGERPRINT_FILE },
      device_id: DEVICE_ID, session_id: SESSION_ID,
    });
    return;
  }
  if (url === "/api/logs") { sendJson(cres, 200, { logs: getLogs() }); return; }

  // ── fingerprint (a FreeModel concept; values are shared) ──
  if (url === "/api/fingerprint") {
    if (creq.method === "GET") { sendJson(cres, 200, { ...FP, profileFile: FINGERPRINT_FILE }); return; }
    if (creq.method === "PUT") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        const next = { ...FP };
        for (const k of Object.keys(FINGERPRINT_DEFAULTS)) if (body[k] != null && typeof body[k] === "string") next[k] = body[k];
        try {
          fs.mkdirSync(CONFIG_DIR, { recursive: true });
          fs.writeFileSync(FINGERPRINT_FILE, JSON.stringify(next, null, 2) + "\n");
          Object.assign(FP, next);
          for (const id of Object.keys(INSTANCES)) { const p = INSTANCES[id]; if (p.fp) Object.assign(p.fp, next); }
          pushLog({ kind: "api", method: "PUT", path: "/api/fingerprint", status: 200, note: "profile updated" });
          sendJson(cres, 200, { ok: true, profile: { ...FP, profileFile: FINGERPRINT_FILE }, note: "applied live" });
        } catch (e) { sendJson(cres, 500, { ok: false, error: e.message }); }
      });
      return;
    }
  }

  // ── per-provider key pool (?provider=<id>; default first enabled) ──
  if (url === "/api/keys") {
    const p = providerForKeys();
    if (!p) { sendJson(cres, 404, { ok: false, error: "no provider" }); return; }
    if (creq.method === "GET") {
      p.reloadStats();
      sendJson(cres, 200, {
        provider: p.id, enabled: isEnabled(p), total: p.pool.keys.length, current: p.pool.idx,
        keys: p.pool.keys.map((k, i) => {
          const s = p.stats[k] || {};
          return { index: i, masked: maskKey(k), current: i === p.pool.idx, ok: s.ok || 0, limited: s.limited || 0, bad: !!s.bad, count: s.count || 0, lastStatus: s.lastStatus, lastUsed: s.lastUsed };
        }),
      });
      return;
    }
    if (creq.method === "POST") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        const added = [];
        const add = (v) => { if (typeof v === "string" && v.trim() && !p.pool.keys.includes(v.trim())) { p.pool.keys.push(v.trim()); p.ensureStats(v.trim()); added.push(v.trim()); } };
        if (Array.isArray(body.keys)) body.keys.forEach(add); else if (body.key) add(body.key);
        p.persistPool();
        sendJson(cres, 200, { ok: true, provider: p.id, added: added.length, total: p.pool.keys.length });
      });
      return;
    }
    if (creq.method === "DELETE") {
      readJsonBody(creq, (body, err) => {
        if (err) { sendJson(cres, 400, { ok: false, error: "invalid JSON body: " + err }); return; }
        let removed = false;
        if (typeof body.index === "number" && p.pool.keys[body.index] !== undefined) { p.pool.keys.splice(body.index, 1); removed = true; }
        else if (typeof body.key === "string") { const i = p.pool.keys.indexOf(body.key.trim()); if (i >= 0) { p.pool.keys.splice(i, 1); removed = true; } }
        if (removed) { p.pool.idx = p.pool.keys.length ? p.pool.idx % p.pool.keys.length : 0; p.persistPool(); sendJson(cres, 200, { ok: true, provider: p.id, removed: true, total: p.pool.keys.length }); }
        else sendJson(cres, 404, { ok: false, error: "key not found" });
      });
      return;
    }
    return;
  }
  // Reset key marks (no requests): clears bad/limited on every key so they
  // get re-evaluated by the next real request. Replaces the old probe-based
  // "find first working" which burned generation tokens for nothing.
  if (url === "/api/keys/reset" && creq.method === "POST") {
    const p = providerForKeys();
    if (!p) { sendJson(cres, 404, { ok: false, error: "no provider" }); return; }
    p.resetStats();
    pushLog({ kind: "api", method: "POST", path: "/api/keys/reset", status: 200, note: p.id });
    sendJson(cres, 200, { ok: true, provider: p.id, total: p.pool.keys.length });
    return;
  }

  // ── test panel (loopback through the real /v1/* path) ──
  if (url === "/api/test" && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      let reqBody; try { reqBody = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { reqBody = {}; }
      const model = reqBody.model || "claude-opus-4-8";
      const prompt = reqBody.prompt || "Reply with exactly: ok";
      const proto = reqBody.proto === "anthropic" ? "anthropic" : "openai";
      const tpath = proto === "anthropic" ? "/v1/messages" : "/v1/chat/completions";
      const body = proto === "anthropic"
        ? { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: [{ type: "text", text: prompt }] }] }
        : { model, max_tokens: 64, stream: false, messages: [{ role: "user", content: prompt }] };
      const buf = Buffer.from(JSON.stringify(body));
      const found = providerForModel(model);
      const upstream = found ? found.provider.route(proto, found.model).host : "(no provider)";
      const logEntry = { kind: "test", method: "POST", path: tpath + "(" + proto + ")", status: null, note: model + (found ? " via " + found.provider.id : " (unrouted)") };
      const up = http.request({ host: "127.0.0.1", port: CFG.port, method: "POST", path: tpath, headers: { "content-type": "application/json", "content-length": buf.length } }, (upres) => {
        const data = []; upres.on("data", (c) => data.push(c));
        upres.on("end", () => {
          const text = Buffer.concat(data).toString("utf8");
          const st = upres.statusCode || 502;
          logEntry.status = st; pushLog(logEntry);
          if (st !== 200) { sendJson(cres, st, { ok: false, model, proto, path: tpath, provider: found ? found.provider.id : null, upstream, status: st, raw: text }); return; }
          let out = text;
          try {
            const d = JSON.parse(text);
            if (proto === "anthropic") { const txt = (d.content || []).filter((b) => b.type === "text").map((b) => b.text).join(""); if (txt) out = txt; }
            else { const c = d.choices && d.choices[0] && d.choices[0].message && d.choices[0].message.content; if (c) out = c; }
          } catch {}
          sendJson(cres, 200, { ok: true, model, proto, path: tpath, provider: found ? found.provider.id : null, upstream, status: st, raw: out });
        });
      });
      up.on("error", (e) => { logEntry.status = 502; logEntry.note = e.message; pushLog(logEntry); sendJson(cres, 502, { ok: false, error: e.message }); });
      up.write(buf); up.end();
    });
    return;
  }

  // ── /v1/messages (Anthropic) ──
  if (url === "/v1/messages" && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      let parsed; try { parsed = JSON.parse(raw); } catch { sendJson(cres, 400, { type: "error", error: { type: "invalid_request", message: "body is not valid JSON" } }); return; }
      const model = parsed.model || "claude";
      const found = providerForModel(model);
      if (!found) { sendJson(cres, 404, { type: "error", error: { type: "not_found", message: "No enabled provider serves model '" + model + "'. Enable a provider that has it, or use 'provider/model' syntax." } }); return; }
      const provider = found.provider;
      const route = provider.route("anthropic", found.model);
      const pre = provider.prepareAnthropicBody({ ...parsed, model: found.model }, raw);
      if (!pre.ok) { sendJson(cres, 400, { type: "error", error: { type: "invalid_request", message: pre.error } }); return; }
      const logEntry = { kind: route.upstreamProto === "openai" ? "openai" : "anthropic", method: "POST", path: "/v1/messages", status: null, note: found.model + " via " + provider.id + (pre.clientStream ? " stream" : " nonstream") };
      provider.forwardWithRetry(route, pre.bodyBuf, "anthropic", pre.clientStream, found.model, cres, logEntry);
    });
    return;
  }

  // ── /v1/chat/completions (OpenAI) ──
  if ((url === "/v1/chat/completions" || url === "/chat/completions") && creq.method === "POST") {
    let chunks = [];
    creq.on("data", (c) => chunks.push(c));
    creq.on("end", () => {
      let oai; try { oai = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch (e) {
        sendJson(cres, 400, { error: { message: "invalid JSON body: " + e.message, type: "invalid_request_error" } }); return;
      }
      const model = oai.model || "claude";
      const found = providerForModel(model);
      if (!found) { sendJson(cres, 404, { error: { message: "No enabled provider serves model '" + model + "'. Enable a provider that has it, or use 'provider/model' syntax.", type: "invalid_request_error" } }); return; }
      const provider = found.provider;
      const route = provider.route("openai", found.model);
      const pre = provider.prepareOpenAIBody({ ...oai, model: found.model });
      if (!pre.ok) { sendJson(cres, 400, { error: { message: pre.error, type: "invalid_request_error" } }); return; }
      const logEntry = { kind: route.upstreamProto === "openai" ? "openai" : "anthropic", method: "POST", path: "/v1/chat/completions", status: null, note: found.model + " via " + provider.id + (pre.clientStream ? " stream" : " nonstream") };
      provider.forwardWithRetry(route, pre.bodyBuf, "openai", pre.clientStream, found.model, cres, logEntry);
    });
    return;
  }

  // ── /v1/models — union of all ENABLED providers ──
  if (url === "/v1/models" || url.startsWith("/v1/models")) {
    const logEntry = { kind: "api", method: "GET", path: "/v1/models", status: null };
    Promise.all(enabledProviders().map(async (p) => {
      const ids = await p.fetchModels(60000);
      return ids.map((mid) => ({ id: mid, object: "model", owned_by: p.id }));
    })).then((lists) => {
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
  const en = enabledProviders();
  console.log("freemodel-cc-proxy " + require("./package.json").version);
  console.log("  Anthropic: http://127.0.0.1:" + CFG.port + "/v1/messages");
  console.log("  OpenAI    : http://127.0.0.1:" + CFG.port + "/v1/chat/completions");
  console.log("  Models    : http://127.0.0.1:" + CFG.port + "/v1/models  (union of enabled providers)");
  console.log("  UI        : http://127.0.0.1:" + CFG.port + "/");
  console.log("  enabled   : " + (en.length ? en.map((p) => p.id + "[" + p.pool.keys.length + "k]").join(" ") : "(none — enable one in the UI)"));
  console.log("  routing   : by model id (first enabled provider that serves it); use 'provider/model' to force");
  console.log("  key check : GET /v1/models (0 tokens); rotation on 401/403/429; 5xx returned (no pool burn)");
});
