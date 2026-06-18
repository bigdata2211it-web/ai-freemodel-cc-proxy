# Uninstaller — Windows (PowerShell)
$ErrorActionPreference = "SilentlyContinue"
Unregister-ScheduledTask -TaskName "freemodel-cc-proxy" -Confirm:$false
Write-Host "Stopped and removed Scheduled Task. Config kept at $HOME\.freemodel-cc-proxy\config.json."
Write-Host "Remove the project dir manually if desired: $HOME\Documents\freemodel-cc-proxy"
