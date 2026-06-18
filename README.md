# freemodel-cc-proxy

Local **Anthropic Messages API** backed by FreeModel's `cc.freemodel.dev` — gives any
Anthropic-compatible client access to **real Claude Opus 4.8, Sonnet 4.6, Haiku 4.5**
through one local endpoint, using your existing FreeModel key.

Works with **Factory `droid`** (BYOK, with `FACTORY_AIRGAP_ENABLED=1`), **Claude Code**,
**Cline**, **Cursor**, the **Anthropic SDK**, and any tool that talks the Anthropic
Messages API.

> FreeModel exposes Claude only on `cc.freemodel.dev`, and that endpoint is gated to the
> **official Claude Code client** — it returns `403 "This service is restricted to the
> official Claude Code client"` for anything else. This proxy impersonates Claude Code's
> request fingerprint (captured via mitmproxy) so ordinary clients get through. No
> binary patching, no token theft — just request-shape normalization on your own
> machine, with your own key.

## What it does

- `POST /v1/messages` — full Anthropic Messages API (streaming SSE + non-streaming),
  with the Claude Code fingerprint injected before forwarding to `cc.freemodel.dev`.
- `GET /v1/models` — lists the Claude models FreeModel actually serves.
- `GET /` — web UI: status, model list, live test panel, request log.

## Available models (from `cc.freemodel.dev/v1/models`)

| Model | Context |
|---|---|
| `claude-opus-4-8` | 1M |
| `claude-opus-4-7` | 1M |
| `claude-sonnet-4-6` | 1M |
| `claude-opus-4-6` | 200K |
| `claude-fable-5` | — |
| `claude-haiku-4-5-20251001` | 200K |

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

The proxy injects all of the above into every `/v1/messages` request, preserves the
client's actual `system`/`messages`/`tools`, and streams the SSE response back
unchanged. `/v1/models` is a passthrough.

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
- Only `/v1/messages` and `/v1/models` are proxied. Embeddings, files, batches are
  not exposed by the upstream.

## License

MIT
