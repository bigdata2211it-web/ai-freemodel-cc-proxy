# freemodel-cc-proxy

Local **LLM gateway** over FreeModel — one endpoint, **both** model families, both
protocols, using your existing FreeModel `fe_oa` key:

- **Real Claude** (Opus 4.8, Sonnet 4.6, Haiku 4.5) from `cc.freemodel.dev`
- **GPT-5.x** (gpt-5.5/5.4/5.4-mini/5.3-codex) from `api.freemodel.dev`

Speaks **both** wire protocols on the same port:

- **Anthropic Messages API** (`POST /v1/messages`)
- **OpenAI Chat Completions API** (`POST /v1/chat/completions`)

…both **streaming and non-streaming**, with full tool/function-calling. The proxy
routes by model id (`claude-*` → fingerprint + cc host, `gpt-*` → api host direct)
and translates between protocols as needed, so any client works against any model.

Works with **Factory `droid`** (BYOK, with `FACTORY_AIRGAP_ENABLED=1`), **Claude Code**,
**Cline**, **Cursor**, **Pi CLI**, the **Anthropic SDK**, the **OpenAI SDK**, and any
tool that talks either API.

> FreeModel exposes Claude only on `cc.freemodel.dev`, and that endpoint is gated to the
> **official Claude Code client** — it returns `403 "This service is restricted to the
> official Claude Code client"` for anything else. This proxy impersonates Claude Code's
> request fingerprint (captured via mitmproxy) so ordinary clients get through. GPT
> models on `api.freemodel.dev` have no such gate. No binary patching, no token theft —
> just request-shape normalization on your own machine, with your own key.

## Profile & enable toggle

FreeModel is a **profile** — a named, toggleable provider config. The web UI has an
ON/OFF switch in the header; `PUT /api/profile { "enabled": false }` does the same
programmatically. When disabled, `/v1/*` returns `503 profile_disabled` with a clear
message, while the UI and `/api/*` stay up so you can re-enable. State persists in
`~/.freemodel-cc-proxy/profile.json`.

## What it does

- `POST /v1/messages` — Anthropic Messages API (**streaming SSE + non-streaming**).
  `claude-*` → fingerprint inject → `cc.freemodel.dev`; `gpt-*` → translate to OpenAI →
  `api.freemodel.dev` → translate back.
- `POST /v1/chat/completions` — OpenAI Chat Completions API (**streaming + non-streaming**,
  tool/function calling). `gpt-*` → forward as-is to `api.freemodel.dev`; `claude-*` →
  translate to Anthropic + fingerprint → `cc.freemodel.dev` → translate back.
- `GET /v1/models` — merged list of both families (6 Claude + 4 GPT, OpenAI-shape).
- `GET /` — web UI: profile toggle + **routing diagram**, metrics, model list, **key
  pool**, fingerprint editor, live test panel (protocol × model), request log, docs.
- `GET/PUT /api/profile` — read/toggle the profile.
- `GET/PUT /api/fingerprint` — read/update the Claude Code fingerprint profile.
- `GET/POST/DELETE /api/keys`, `POST /api/keys/find` — manage the key pool.

> The FreeModel gate forces `stream:true` on every upstream request. So even when a
> client asks for a single JSON object (`stream:false`, the Anthropic SDK default, or
> any non-streaming client), the proxy **buffers the SSE and reassembles one JSON
> object** before returning it. Non-streaming clients work as if the upstream weren't
> gated.

## Available models (merged `/v1/models`)

**Claude** — from `cc.freemodel.dev` (fingerprint-gated):

| Model | Context |
|---|---|
| `claude-opus-4-8` | 1M |
| `claude-opus-4-7` | 1M |
| `claude-sonnet-4-6` | 1M |
| `claude-opus-4-6` | 200K |
| `claude-fable-5` | — |
| `claude-haiku-4-5-20251001` | 200K |

**GPT** — from `api.freemodel.dev` (no gate):

| Model |
|---|
| `gpt-5.5` · `gpt-5.4` · `gpt-5.4-mini` · `gpt-5.3-codex` |

## Requirements

- **Node.js 18+**
- A **FreeModel** key (`fe_oa_...`). Get one at https://freemodel.dev (sign up, verify,
  create an API key). The free tier key works — `cc.freemodel.dev` serves real Claude
  through it once the client fingerprint passes.
- Linux, macOS, or Windows.

## Quick install (autostart)

One-liner, no git clone needed.

**Linux / macOS**

```bash
curl -fsSL https://raw.githubusercontent.com/bigdata2211it-web/ai-freemodel-cc-proxy/main/install/install.sh | FMCC_KEY=fe_oa_YOURKEY bash
```

**Windows (PowerShell)**

```powershell
$env:FMCC_KEY = "fe_oa_YOURKEY"
iwr -useb https://raw.githubusercontent.com/bigdata2211it-web/ai-freemodel-cc-proxy/main/install/install.ps1 | iex
```

The installer:
- checks Node 18+,
- places the proxy in `~/Documents/freemodel-cc-proxy`,
- writes your key to `~/.freemodel-cc-proxy/config.json` (chmod 600),
- registers autostart (systemd user unit on Linux, LaunchAgent on macOS, Scheduled
  Task on Windows),
- smoke-tests `http://127.0.0.1:11440/v1/models`.

## Manual install & run

```bash
git clone https://github.com/bigdata2211it-web/ai-freemodel-cc-proxy.git
cd ai-freemodel-cc-proxy
FMCC_KEY=fe_oa_YOURKEY node index.js
```

Listens on `http://127.0.0.1:11440`. Override port via `FMCC_PORT`.

## Config

Precedence: env vars > `~/.freemodel-cc-proxy/config.json` > defaults.

| Source | Keys |
|---|---|
| env | `FMCC_KEY`, `FMCC_PORT` (default `11440`), `FMCC_UPSTREAM` (default `cc.freemodel.dev`), `FMCC_LOG_FILE` |
| config.json | `{ "port": 11440, "upstream": "cc.freemodel.dev", "key": "fe_oa_..." }` |
| keys.json | `{ "keys": ["fe_oa_...", "fe_oa_..."] }` — the key pool (managed via CLI/UI) |

## Key pool — multi-key rotation (drain #1 → #2 → …)

The proxy holds a **pool of FreeModel keys** and drains them in order. It serves from
the current key; when that key hits `401`/`402`/`429`/`5xx`, it **advances to the next
key and retries the same request** — at most once per key — so the client sees no
break. `401` marks a key `bad` (skipped until restart); `402`/`429`/`5xx` mark it
`limited` and move the pointer forward (sequential drain). When every key is dead you
finally get an error. A single legacy key (`FMCC_KEY` / config.json `key`) is used to
bootstrap the pool the first time.

Manage keys live (updates the running proxy + persists `~/.freemodel-cc-proxy/keys.json`):

```bash
node keys.js                       # list keys (masked, status, reqs)
node keys.js find                  # probe forward, lock onto the FIRST working key
node keys.js add fe_oa_...         # add one
node keys.js add fe_oa_... fe_oa_...   # add several
node keys.js rm 3                  # remove by index
node keys.js rm fe_oa_...          # remove by exact key
node keys.js status                # pool summary
```

`find` does a sequential forward health probe (tiny `haiku` request, `max_tokens:1`),
**stops at the first `200`**, marks dead keys (`401`→bad, `402/429`→limited) and locks
the pointer onto the working one — leaving the rest untouched. Minimal requests, with a
short pause between probes, so your IP doesn't look abusive. Use it after adding a batch
of keys to immediately land on the first good one without your workload triggering rotation.

Or from the web UI (`/` → "Key pool" card: **Find first working** button), or via HTTP:

```bash
curl localhost:11440/api/keys                                  # GET  — list
curl -X POST localhost:11440/api/keys/find                    # POST — find first working
curl -X POST localhost:11440/api/keys -H 'content-type: application/json' \
  -d '{"keys":["fe_oa_...","fe_oa_..."]}'                      # POST — add
curl -X DELETE localhost:11440/api/keys -H 'content-type: application/json' \
  -d '{"index":0}'                                             # DELETE — by index or {key}
```

`keys.json` is gitignored. AI agents can add keys by `POST`ing to `/api/keys`.

## Use with Factory `droid`

Add to `~/.factory/settings.json` `customModels`:

```json
{
  "model": "claude-opus-4-8",
  "displayName": "FreeModel · Claude Opus 4.8 (cc-proxy)",
  "baseUrl": "http://127.0.0.1:11440",
  "apiKey": "dummy",
  "provider": "anthropic",
  "maxOutputTokens": 16384
}
```

Keep `FACTORY_AIRGAP_ENABLED=1` so Droid skips the Factory platform auth gate (see the
[`droid-byok`](https://github.com/bigdata2211it-web/ai-droid-skills) skill). Then:

```bash
droid exec -m claude-opus-4-8 --auto low "say ok"
```

## Use with Claude Code

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:11440
export ANTHROPIC_API_KEY=dummy
claude "refactor this function"
```

## Use with the Anthropic SDK (Python)

```python
from anthropic import Anthropic
c = Anthropic(base_url="http://127.0.0.1:11440", api_key="dummy")
print(c.messages.create(model="claude-opus-4-8", max_tokens=64,
      messages=[{"role":"user","content":"say ok"}]).content[0].text)
```

## Use with the OpenAI SDK (Python)

Point the OpenAI SDK at the proxy — same Claude models, OpenAI shape in and out:

```python
from openai import OpenAI
c = OpenAI(base_url="http://127.0.0.1:11440/v1", api_key="dummy")
r = c.chat.completions.create(
    model="claude-opus-4-8",
    messages=[{"role":"user","content":"say ok"}],
)
print(r.choices[0].message.content)

# streaming + tool calling work the same way as with OpenAI's own API
stream = c.chat.completions.create(
    model="claude-opus-4-8",
    stream=True,
    tools=[{"type":"function","function":{
        "name":"get_weather",
        "parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}],
    messages=[{"role":"user","content":"weather in Paris"}],
)
for chunk in stream:
    print(chunk.choices[0].delta)
```

Also works with **LiteLLM**, **LangChain** (`ChatOpenAI(base_url=...)`), **curl**, and any
OpenAI-compatible client — set base URL `http://127.0.0.1:11440/v1`, any API key, model
`claude-opus-4-8` / `claude-sonnet-4-6` / `claude-haiku-4-5-20251001`.

## How the fingerprint works

`cc.freemodel.dev` validates the request **body shape**, not TLS/JA3 and not the key
(the same `fe_oa_` key works for both FreeModel's GPT host and the Claude host). The
gate requires:

- `stream: true`
- two leading `system` blocks: a billing header
  (`x-anthropic-billing-header: cc_version=2.1.179.efd; cc_entrypoint=sdk-cli; cch=fa71b;`)
  and `You are a Claude agent, built on Anthropic's Claude Agent SDK.`
- `metadata.user_id` as a JSON-encoded string with `device_id` / `account_uuid` /
  `session_id`
- Claude Code headers: `User-Agent: claude-cli/2.1.179 (external, sdk-cli)`,
  `x-app: cli`, `anthropic-beta: claude-code-20250219,…`,
  `anthropic-version: 2023-06-01`, `anthropic-dangerous-direct-browser-access: true`

The proxy injects all of the above into every upstream request, preserves the client's
actual `system`/`messages`/`tools`, and:

- **Anthropic streaming clients** → SSE piped through unchanged.
- **Anthropic non-streaming clients** → SSE buffered and reassembled into one
  `Message` JSON object.
- **OpenAI clients** (`/v1/chat/completions`) → the request is translated to Anthropic,
  sent upstream, then the response is translated back: SSE→OpenAI chunks for streaming,
  one `chat.completion` object for non-streaming. Tool calling is mapped both ways
  (`tools`/`tool_choice`/`tool_calls`/`tool` results).

## systemd (Linux, manual)

`install/install.sh` writes this for you. To do it by hand:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/freemodel-cc-proxy.service ~/.config/systemd/user/
# edit the unit: set ExecStart path and Environment=FMCC_KEY=...
systemctl --user daemon-reload
systemctl --user enable --now freemodel-cc-proxy
journalctl --user -u freemodel-cc-proxy -f
```

Lingering (so the user unit runs without an active login session):

```bash
sudo loginctl enable-linger $USER
```

## Uninstall

```bash
# Linux / macOS
bash install/uninstall.sh
# Windows
powershell -File install\uninstall.ps1
```

## Limitations

- The fingerprint was captured from Claude Code `2.1.179`. If FreeModel tightens the
  gate in a future Claude Code release, re-capture with mitmproxy (see the skill) and
  update the constants in `index.js`.
- `cc.freemodel.dev` may rate-limit or tier-gate heavily; this proxy forwards
  upstream errors verbatim.
- Proxied endpoints: `/v1/messages` (Anthropic), `/v1/chat/completions` (OpenAI),
  `/v1/models`. Embeddings, files, batches are not exposed by the upstream.
- OpenAI translation covers chat, system messages, tool/function calling, images
  (base64 + URL), `temperature`/`top_p`/`stop`. Streaming-only OpenAI features like
  `logprobs` are not mapped. Claude's `thinking` blocks are dropped on the OpenAI side
  (OpenAI has no equivalent).

## License

MIT
