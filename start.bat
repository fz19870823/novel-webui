@echo off
title novel-webui launcher
setlocal EnableDelayedExpansion

rem ============================================================
rem  novel-webui one-click launcher
rem   - default binds 0.0.0.0:8000 (LAN / remote access)
rem   - to listen locally only, edit HOST to 127.0.0.1 or pass arg
rem   - confirm countdown defaults to 5s
rem   - API key priority: env NOVEL_AI_API_KEY > key.env file
rem  Usage: start.bat [host] [port] [confirm-seconds]
rem ============================================================

cd /d "%~dp0"

rem ---- args: host / port / confirm ----
set "HOST=0.0.0.0"
set "PORT=8000"
set "CONFIRM=5"

if not "%~1"=="" set "HOST=%~1"
if not "%~2"=="" set "PORT=%~2"
if not "%~3"=="" set "CONFIRM=%~3"

echo.
echo  ==========================================
echo   novel-webui
echo   Host   : !HOST!
echo   Port   : !PORT!
echo   Confirm: !CONFIRM!s (auto-confirm countdown)
echo  ==========================================
echo.

rem ---- venv self-check ----
if not exist "venv\Scripts\python.exe" (
    echo [ERROR] venv\Scripts\python.exe not found.
    echo Please run:
    echo   python -m venv venv
    echo   venv\Scripts\python -m pip install -r requirements.txt
    pause
    exit /b 1
)

rem ---- API key from key.env (optional, only if env var not set) ----
if not defined NOVEL_AI_API_KEY (
    if exist "key.env" (
        for /f "usebackq tokens=1,* delims==" %%a in ("key.env") do (
            if /i "%%a"=="NOVEL_AI_API_KEY" set "NOVEL_AI_API_KEY=%%b"
        )
    )
)

echo [INFO ] venv python OK
if defined NOVEL_AI_API_KEY (
    echo [KEY  ] %NOVEL_AI_API_KEY:~0,4%***%NOVEL_AI_API_KEY:~-4% (masked)
) else (
    echo [KEY  ] NOT set. Recommend env NOVEL_AI_API_KEY or key.env
)
echo [INFO ] Browser will open. Keep this window open while serving.
echo.
if "!HOST!"=="0.0.0.0" (
    echo   LAN access   : http://THIS-PC-IP:!PORT!
    echo   Local access : http://127.0.0.1:!PORT!
    echo   NOTE: 0.0.0.0 has NO auth - trusted network only
) else (
    echo   Access       : http://!HOST!:!PORT!
)
echo.

rem ---- open browser ----
start "" "http://127.0.0.1:!PORT!"

rem ---- run server ----
"venv\Scripts\python.exe" server.py --host "!HOST!" --port "!PORT!" --confirm "!CONFIRM!"

echo.
echo [server exited] code: %ERRORLEVEL%
pause
