# PowerShell launcher
if (-not $env:FMCC_KEY) {
  try { $c = Get-Content "$PSScriptRoot/.freemodel-cc-proxy/config.json" -Raw -ErrorAction Stop | ConvertFrom-Json; $env:FMCC_KEY = $c.key } catch {}
}
Set-Location $PSScriptRoot
node index.js
