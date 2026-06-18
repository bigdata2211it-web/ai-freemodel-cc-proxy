#!/usr/bin/env node
// freemodel-cc-proxy key pool manager — talks to the running proxy.
// Manages the live in-memory pool (and persists to ~/.freemodel-cc-proxy/keys.json).
//
//   keys.js                          list keys (masked + status)
//   keys.js add fe_oa_...            add one key (or several, space-separated)
//   keys.js rm 3                     remove by index (from `list`)
//   keys.js rm fe_oa_...             remove by exact key
//   keys.js status                   pool summary
//
// Base URL: FMCC_BASE (default http://127.0.0.1:11440).
"use strict";

const BASE = (process.env.FMCC_BASE || "http://127.0.0.1:11440").replace(/\/$/, "");
const [,, cmd, ...args] = process.argv;

async function jget(p) { const r = await fetch(BASE + p); return [r.status, await r.json().catch(() => ({}))]; }
async function jsend(p, method, body) {
  const r = await fetch(BASE + p, { method, headers: { "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  return [r.status, await r.json().catch(() => ({}))];
}

const HELP = `freemodel-cc-proxy key pool manager
  keys.js              list keys
  keys.js add <key...> add key(s)
  keys.js rm <idx|key> remove by index or exact key
  keys.js status       pool summary
  keys.js current      show current (active) key, masked
base: ${BASE} (override with FMCC_BASE)`;

(async () => {
  try {
    switch (cmd) {
      case undefined:
      case "list":
      case "ls": {
        const [s, d] = await jget("/api/keys");
        if (s !== 200) { console.error("error", s, d); process.exit(1); }
        if (!d.keys.length) { console.log("(empty)"); break; }
        console.log(`pool: ${d.keys.length} key(s) · current #${d.current}`);
        for (const k of d.keys) {
          const flag = k.current ? "★" : " ";
          const st = [k.bad ? "BAD" : "", k.limited ? `lim×${k.limited}` : "", k.ok ? `ok×${k.ok}` : ""].filter(Boolean).join(" ");
          const ls = k.lastStatus ? ` last=${k.lastStatus}` : "";
          console.log(`  ${flag} #${k.index}  ${k.masked}  ${st}${ls}  reqs=${k.count}`);
        }
        break;
      }
      case "add": {
        if (!args.length) { console.error("usage: keys.js add fe_oa_..."); process.exit(1); }
        const [s, d] = await jsend("/api/keys", "POST", { keys: args });
        console.log(s === 200 ? `added ${d.added}, total ${d.total}` : `failed (${s}): ${JSON.stringify(d)}`);
        process.exit(s === 200 ? 0 : 1);
      }
      case "rm":
      case "remove": {
        if (!args.length) { console.error("usage: keys.js rm <index|key>"); process.exit(1); }
        const a = args[0];
        const body = /^\d+$/.test(a) ? { index: parseInt(a, 10) } : { key: a };
        const [s, d] = await jsend("/api/keys", "DELETE", body);
        console.log(s === 200 ? `removed, total ${d.total}` : `not found (${s}): ${JSON.stringify(d)}`);
        process.exit(s === 200 ? 0 : 1);
      }
      case "status": {
        const [s, d] = await jget("/api/status");
        if (s !== 200) { console.error("error", s); process.exit(1); }
        console.log(`keys: ${d.keys.total} · current #${d.keys.current} (${d.keys.currentMasked})`);
        break;
      }
      default:
        console.log(HELP);
    }
  } catch (e) {
    if (e.code === "ECONNREFUSED") { console.error("proxy not running at " + BASE + " — start it first (systemctl --user start freemodel-cc-proxy)"); }
    else console.error(e.message);
    process.exit(1);
  }
})();
