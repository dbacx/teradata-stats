@echo off
setlocal enabledelayedexpansion
title VantageOps — Teradata Assessment Suite

REM ── Check connection config ────────────────────────────────
if not exist config\connections.csv (
    echo [ERROR] Connection configuration not found: config\connections.csv
    echo Run install.bat first, or: python scripts\create_connections_template.py
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
echo  Starting VantageOps...
echo  Opening browser at http://localhost:8501
echo  Press Ctrl+C to stop.
echo.
streamlit run ui/main.py
endlocal
