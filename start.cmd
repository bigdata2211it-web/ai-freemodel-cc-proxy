@echo off
if "%FMCC_KEY%"=="" (
  node -e "try{const c=require('./.freemodel-cc-proxy/config.json');process.stdout.write(c.key||'')}catch(e){}" > %TEMP%\fmcc_key.txt 2>nul
  set /p FMCC_KEY=<%TEMP%\fmcc_key.txt
  del %TEMP%\fmcc_key.txt
)
node "%~dp0index.js"
