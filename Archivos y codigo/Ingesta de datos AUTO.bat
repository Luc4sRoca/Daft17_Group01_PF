@echo off
REM ===============================
REM Script de ejecución automática
REM Ejecuta watchdog_ingestion.py con Python desde PowerShell
REM ===============================

cd %~dp0
powershell -WindowStyle Hidden -ExecutionPolicy Bypass -Command "python \"%~dp0watchdog_ingestion.py\""
