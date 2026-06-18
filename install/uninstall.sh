#!/bin/bash
# Uninstaller — Linux / macOS
set -e
OS="$(uname -s)"
if [ "$OS" = "Linux" ]; then
  systemctl --user disable --now freemodel-cc-proxy.service 2>/dev/null || true
  rm -f "$HOME/.config/systemd/user/freemodel-cc-proxy.service"
  systemctl --user daemon-reload 2>/dev/null || true
elif [ "$OS" = "Darwin" ]; then
  launchctl unload "$HOME/Library/LaunchAgents/dev.freemodel.cc-proxy.plist" 2>/dev/null || true
  rm -f "$HOME/Library/LaunchAgents/dev.freemodel.cc-proxy.plist"
fi
echo "Stopped and removed autostart. Config kept at ~/.freemodel-cc-proxy/config.json."
echo "Remove the project dir manually if desired: ~/Documents/freemodel-cc-proxy"
