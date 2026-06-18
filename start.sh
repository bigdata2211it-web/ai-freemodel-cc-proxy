#!/bin/bash
# Linux / macOS launcher
export FMCC_KEY="${FMCC_KEY:-$(node -e "try{console.log(require('./.freemodel-cc-proxy/config.json').key||'')}catch(e){}" 2>/dev/null)}"
cd "$(dirname "$0")"
exec node index.js
