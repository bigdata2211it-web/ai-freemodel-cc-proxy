# freemodel-cc-proxy installer — Windows (PowerShell)
#Requires -Version 5
$ErrorActionPreference = "Stop"

$Port = if ($env:FMCC_PORT) { $env:FMCC_PORT } else { "11440" }
$InstallDir = if ($env:FMCC_INSTALL_DIR) { $env:FMCC_INSTALL_DIR } else { "$HOME\Documents\freemodel-cc-proxy" }
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "freemodel-cc-proxy installer"
Write-Host "  target: $InstallDir"
Write-Host "  port:   $Port"

# Node check
try { $nv = (node -v) } catch { Write-Error "Node.js 18+ required. Install from https://nodejs.org"; exit 1 }
if ([int]($nv -replace '^v(\d+).*','$1') -lt 18) { Write-Error "Node.js 18+ required."; exit 1 }

# Place files
if ($ScriptDir -ne "$InstallDir\install" -and $ScriptDir -ne $InstallDir) {
  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
  Copy-Item "$ScriptDir\..\*.js","$ScriptDir\..\*.html","$ScriptDir\..\*.json","$ScriptDir\..\*.md","$ScriptDir\..\*.cmd","$ScriptDir\..\*.ps1" $InstallDir -ErrorAction SilentlyContinue
  Copy-Item "$ScriptDir\..\install","$ScriptDir\..\systemd" $InstallDir -Recurse -ErrorAction SilentlyContinue
}

# Key
$Key = $env:FMCC_KEY
if (-not $Key) { $Key = Read-Host "Paste your FreeModel key (fe_oa_...)" }
if (-not $Key) { Write-Error "No key provided. Set FMCC_KEY and re-run." }
$CfgDir = "$HOME\.freemodel-cc-proxy"
New-Item -ItemType Directory -Force -Path $CfgDir | Out-Null
@{port=[int]$Port; upstream="cc.freemodel.dev"; key=$Key} | ConvertTo-Json | Set-Content "$CfgDir\config.json"
Write-Host "  config: $CfgDir\config.json"

# Scheduled Task (autostart on login)
$Action = New-ScheduledTaskAction -Execute "node" -Argument "$InstallDir\index.js"
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
$Env = @{ FMCC_PORT=[string]$Port; FMCC_KEY=$Key }
Register-ScheduledTask -TaskName "freemodel-cc-proxy" -Action $Action -Trigger $Trigger -Settings $Settings -Env $Env -Force | Out-Null
Start-ScheduledTask -TaskName "freemodel-cc-proxy"
Write-Host "  Scheduled Task: freemodel-cc-proxy (registered, started)"

# Smoke test
Start-Sleep -Seconds 2
$r = try { Invoke-RestMethod "http://127.0.0.1:$Port/v1/models" -Headers @{ "x-api-key"="dummy" } -TimeoutSec 15 } catch { $null }
if ($r -and ($r.data | Where-Object id -eq "claude-opus-4-8")) {
  Write-Host "  /v1/models OK — Claude models visible."
} else {
  Write-Error "Smoke test failed. Check $CfgDir\proxy.log"
}
Write-Host ""
Write-Host "Done. UI:  http://127.0.0.1:$Port/"
Write-Host "     API: http://127.0.0.1:$Port/v1/messages"
