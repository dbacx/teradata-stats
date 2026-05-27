@echo off
setlocal enabledelayedexpansion
title VantageOps — Installer

echo.
echo  ================================================================
echo   VantageOps — Teradata Assessment Suite
echo   Managed Services DBA Tool — Installation
echo  ================================================================
echo.

REM ── Check Python ─────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found. Please install Python 3.10+
    echo  Download: https://www.python.org/downloads/
    echo  Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PY_VER=%%v
echo  [OK] Python found: %PY_VER%

REM ── Create virtual environment ────────────────────────────────
echo.
echo  [1/4] Creating virtual environment...
if exist venv (
    echo  [INFO] Virtual environment already exists. Skipping creation.
) else (
    python -m venv venv
    echo  [OK] Virtual environment created.
)

REM ── Activate venv ────────────────────────────────────────────
call venv\Scripts\activate.bat

REM ── Upgrade pip ──────────────────────────────────────────────
echo.
echo  [2/4] Upgrading pip...
python -m pip install --upgrade pip --quiet

REM ── Install dependencies ─────────────────────────────────────
echo.
echo  [3/4] Installing dependencies (this may take 2-3 minutes)...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo  [ERROR] Dependency installation failed.
    echo  Check your internet connection and try again.
    pause
    exit /b 1
)
echo  [OK] Dependencies installed.

REM ── Create connection config if missing ─────────────────────
echo.
echo  [4/4] Checking configuration...
if not exist config\connections.csv (
    python scripts\create_connections_template.py
    echo.
    echo  ============================================================
    echo   ACTION REQUIRED: Configure your connections
    echo  ============================================================
    echo   A template was created at: config\connections.csv
    echo   Edit this file with your Teradata credentials before running.
    echo  ============================================================
) else (
    echo  [OK] Connection configuration found.
)

REM ── Create run shortcut ──────────────────────────────────────
echo.
echo  [OK] Installation complete!
echo.
echo  ================================================================
echo   TO RUN VantageOps:
echo     Option A: Double-click run.bat
echo     Option B: run.bat CLIENTNAME
echo     Example:  run.bat EPM
echo  ================================================================
echo.
pause
endlocal
