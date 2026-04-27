@echo off
title ADBMS Backend Server
cd /d "%~dp0backend"
echo Starting FastAPI backend on http://localhost:8001 ...
python -m uvicorn server:app --host 0.0.0.0 --port 8001 --reload
pause
