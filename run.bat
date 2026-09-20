@echo off
title Dhurta Sync — Local Server
color 0A

echo.
echo  ====================================================
echo   DHURTA Sync — Offline P2P Server Launcher
echo  ====================================================
echo.

:: Get local IPv4 address (Wi-Fi / Hotspot interface)
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /C:"IPv4 Address" ^| findstr /V "127.0.0.1"') do (
    set "LOCAL_IP=%%a"
    goto :found_ip
)
:found_ip
:: Trim leading spaces
set "LOCAL_IP=%LOCAL_IP: =%"

if "%LOCAL_IP%"=="" (
    set "LOCAL_IP=127.0.0.1"
    echo  [WARN] Could not detect LAN IP. Using localhost.
)

set PORT=8080

echo  Local  : http://localhost:%PORT%
echo  Network: http://%LOCAL_IP%:%PORT%
echo.
echo  Phone/tablet: connect to same Wi-Fi or hotspot,
echo  then open http://%LOCAL_IP%:%PORT%
echo.
echo  ====================================================
echo.

:: Check for Python 3
python --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo  [OK] Python found — starting http.server on port %PORT%...
    echo.
    start "" /B python -m http.server %PORT% --bind 0.0.0.0
    goto :launch_browser
)

:: Check for Python 3 as python3
python3 --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo  [OK] Python3 found — starting http.server on port %PORT%...
    echo.
    start "" /B python3 -m http.server %PORT% --bind 0.0.0.0
    goto :launch_browser
)

:: Check for Node.js / npx serve
npx --version >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo  [OK] Node.js/npx found — starting npx serve on port %PORT%...
    echo.
    start "" /B npx serve -l %PORT% -s
    goto :launch_browser
)

:: Fallback: Pure PowerShell zero-dependency TCP server
echo  [WARN] No Python or Node found. Using PowerShell TCP server...
echo.
start "" /B powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$listener = New-Object System.Net.HttpListener;" ^
  "$listener.Prefixes.Add('http://+:%PORT%/');" ^
  "$listener.Start();" ^
  "Write-Host 'PowerShell HTTP server running on port %PORT%';" ^
  "while ($listener.IsListening) {" ^
  "  $ctx = $listener.GetContext();" ^
  "  $req = $ctx.Request; $res = $ctx.Response;" ^
  "  $path = $req.Url.LocalPath.TrimStart('/');" ^
  "  if ($path -eq '') { $path = 'index.html' };" ^
  "  $file = Join-Path (Get-Location) $path;" ^
  "  $mime = switch ([System.IO.Path]::GetExtension($file)) {" ^
  "    '.html' {'text/html'} '.css' {'text/css'} '.js' {'application/javascript'}" ^
  "    '.json' {'application/json'} '.png' {'image/png'} '.svg' {'image/svg+xml'}" ^
  "    default {'application/octet-stream'}};" ^
  "  if (Test-Path $file) {" ^
  "    $bytes = [System.IO.File]::ReadAllBytes($file);" ^
  "    $res.ContentType = $mime;" ^
  "    $res.ContentLength64 = $bytes.Length;" ^
  "    $res.OutputStream.Write($bytes, 0, $bytes.Length);" ^
  "  } else {" ^
  "    $res.StatusCode = 404; $msg = [System.Text.Encoding]::UTF8.GetBytes('Not Found');" ^
  "    $res.OutputStream.Write($msg, 0, $msg.Length);" ^
  "  };" ^
  "  $res.Close();" ^
  "}"

:launch_browser
:: Wait 2 seconds then open browser
timeout /t 2 /nobreak >nul
start http://localhost:%PORT%

echo.
echo  Press Ctrl+C to stop the server.
echo.
pause
