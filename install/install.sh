#!/bin/bash
# freemodel-cc-proxy installer — Linux / macOS
# Clones/copies the proxy into place, writes the FreeModel key to config,
# registers a systemd user unit (Linux) or LaunchAgent (macOS), and smoke-tests.
set -e

PORT="${FMCC_PORT:-11440}"
INSTALL_DIR="${FMCC_INSTALL_DIR:-$HOME/Documents/freemodel-cc-proxy}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "freemodel-cc-proxy installer"
echo "  target: $INSTALL_DIR"
echo "  port:   $PORT"
echo

# 1. Node check
if ! command -v node >/dev/null || [ "$(node -v | cut -d. -f1 | tr -d v)" -lt 18 ]; then
  echo "Node.js 18+ required. Install from https://nodejs.org and re-run." >&2
  exit 1
fi

# 2. Place files (if running from inside the repo, just use it; else copy)
if [ "$SCRIPT_DIR" != "$INSTALL_DIR/install" ] && [ "$SCRIPT_DIR" != "$INSTALL_DIR" ]; then
  mkdir -p "$INSTALL_DIR"
  cp -r "$SCRIPT_DIR"/../*.js "$SCRIPT_DIR"/../*.html "$SCRIPT_DIR"/../*.json "$SCRIPT_DIR"/../*.md "$SCRIPT_DIR"/../*.sh "$SCRIPT_DIR"/../*.cmd "$SCRIPT_DIR"/../*.ps1 "$INSTALL_DIR"/ 2>/dev/null || true
  cp -r "$SCRIPT_DIR/../install" "$INSTALL_DIR/" 2>/dev/null || true
  cp -r "$SCRIPT_DIR/../systemd" "$INSTALL_DIR/" 2>/dev/null || true
fi

# 3. Key
KEY="${FMCC_KEY:-}"
if [ -z "$KEY" ]; then
  printf "Paste your FreeModel key (fe_oa_...): "
  read -r KEY
fi
[ -z "$KEY" ] && { echo "No key provided. Set FMCC_KEY and re-run."; exit 1; }
CFG_DIR="$HOME/.freemodel-cc-proxy"
mkdir -p "$CFG_DIR"
printf '{"port":%s,"upstream":"cc.freemodel.dev","key":"%s"}\n' "$PORT" "$KEY" > "$CFG_DIR/config.json"
chmod 600 "$CFG_DIR/config.json"
echo "  config: $CFG_DIR/config.json"

# 4. Autostart
OS="$(uname -s)"
if [ "$OS" = "Linux" ]; then
  U="$HOME/.config/systemd/user/freemodel-cc-proxy.service"
  mkdir -p "$(dirname "$U")"
  ESC_DIR="${INSTALL_DIR/#$HOME/\%h}"
  ESC_DIR="${ESC_DIR// /\\ }"
  cat > "$U" <<EOF
[Unit]
Description=FreeModel CC Proxy — local Anthropic Messages API backed by cc.freemodel.dev
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/node $ESC_DIR/index.js
Restart=on-failure
RestartSec=5
Environment=FMCC_PORT=$PORT
Environment=FMCC_KEY=$KEY

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now freemodel-cc-proxy.service
  echo "  systemd: $U  (enabled, started)"
elif [ "$OS" = "Darwin" ]; then
  LA="$HOME/Library/LaunchAgents/dev.freemodel.cc-proxy.plist"
  cat > "$LA" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>dev.freemodel.cc-proxy</string>
  <key>ProgramArguments</key><array>
    <string>/usr/local/bin/node</string><string>$INSTALL_DIR/index.js</string>
  </array>
  <key>EnvironmentVariables</key><dict>
    <key>FMCC_PORT</key><string>$PORT</string>
    <key>FMCC_KEY</key><string>$KEY</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$HOME/.freemodel-cc-proxy/proxy.log</string>
  <key>StandardErrorPath</key><string>$HOME/.freemodel-cc-proxy/proxy.log</string>
</dict></plist>
EOF
  launchctl load "$LA"
  echo "  LaunchAgent: $LA"
fi

# 5. Smoke test
echo
echo "Smoke test..."
sleep 2
if curl -sS -m 15 "http://127.0.0.1:$PORT/v1/models" -H "x-api-key: dummy" | grep -q claude-opus-4-8; then
  echo "  /v1/models OK — Claude models visible."
else
  echo "  /v1/models did not return expected models. Check: $HOME/.freemodel-cc-proxy/proxy.log" >&2
  exit 1
fi
echo
echo "Done. UI:  http://127.0.0.1:$PORT/"
echo "     API: http://127.0.0.1:$PORT/v1/messages"
echo
echo "Use with Factory Droid — add to ~/.factory/settings.json customModels:"
echo "  {\"model\":\"claude-opus-4-8\",\"baseUrl\":\"http://127.0.0.1:$PORT\",\"apiKey\":\"dummy\",\"provider\":\"anthropic\",\"maxOutputTokens\":16384}"
echo "and keep FACTORY_AIRGAP_ENABLED=1 (see the droid-byok skill)."
