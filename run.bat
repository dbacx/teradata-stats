@echo off
setlocal enabledelayedexpansion
title VantageOps — Teradata Assessment Suite

REM ── Get client name ──────────────────────────────────────────
set CLIENT=%1
if "%CLIENT%"=="" (
    echo.
    echo  Available clients:
    for %%f in (config\*.env) do (
        set fname=%%~nf
        if not "!fname!"==".env" (
            echo    - !fname!
        )
    )
    echo.
    set /p CLIENT="Enter client name (e.g. EPM): "
)

if "%CLIENT%"=="" (
    echo [ERROR] No client name provided.
    pause
    exit /b 1
)

REM ── Check config exists ───────────────────────────────────────
if not exist "config\%CLIENT%.env" (
    echo [ERROR] Configuration not found: config\%CLIENT%.env
    echo Create the file from the template: config\.env.example
    pause
    exit /b 1
)

REM ── Activate venv ────────────────────────────────────────────
if not exist venv (
    echo [ERROR] Virtual environment not found. Run install.bat first.
    pause
    exit /b 1
)
call venv\Scripts\activate.bat

REM ── Launch Streamlit ─────────────────────────────────────────
echo.
echo  Starting VantageOps for client: %CLIENT%
echo  Opening browser at http://localhost:8501
echo  Press Ctrl+C to stop.
echo.
cd ui
streamlit run main.py -- --client %CLIENT%
endlocal
