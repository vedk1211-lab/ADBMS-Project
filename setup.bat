@echo off
title ADBMS Smart Retail — Setup

echo ============================================================
echo  ADBMS Smart Retail Analytics — Windows Setup Script
echo ============================================================
echo.

:: Step 1 — Python deps
echo [1/4] Installing Python dependencies...
pip install -r backend\requirements.txt
if %ERRORLEVEL% NEQ 0 ( echo ERROR: pip install failed. & pause & exit /b 1 )
echo      Done.
echo.

:: Step 2 — Create DB (SQLite is file-based, no separate create command needed)
echo [2/4] SQLite database will be initialized by Python scripts...
echo      Done.
echo.

:: Step 3 — Schema + Data + ML
echo [3/4] Initialising schema, data, and ML models...
cd backend
python database.py
if %ERRORLEVEL% NEQ 0 ( echo ERROR: database.py failed. & cd .. & pause & exit /b 1 )

python generate_data.py
if %ERRORLEVEL% NEQ 0 ( echo ERROR: generate_data.py failed. & cd .. & pause & exit /b 1 )

python train_model.py
if %ERRORLEVEL% NEQ 0 ( echo ERROR: train_model.py failed. & cd .. & pause & exit /b 1 )
cd ..
echo      Done.
echo.

:: Step 4 — Start server
echo [4/4] Starting FastAPI backend on http://localhost:8001 ...
echo       Press Ctrl+C to stop.
echo.
cd backend
python -m uvicorn server:app --host 0.0.0.0 --port 8001 --reload
