@echo off
:: ============================================================
::  FraudShield — One-Click Startup Script
::  Starts Zookeeper, Kafka, trains model, and opens dashboard
:: ============================================================
setlocal

set KAFKA_HOME=C:\kafka\kafka_2.13-3.7.1
set PROJECT_DIR=%~dp0
set PYTHON=python

echo.
echo  ========================================
echo    FraudShield Real-Time Fraud Detection
echo  ========================================
echo.

:: ── Step 1: Train model ──────────────────────────────────────
echo [1/5] Training ML model...
cd /d "%PROJECT_DIR%"
%PYTHON% train_model.py
if %errorlevel% neq 0 (
    echo ERROR: Model training failed. Check Python/sklearn installation.
    pause
    exit /b 1
)
echo Model ready!
echo.

:: ── Step 2: Start Zookeeper ───────────────────────────────────
echo [2/5] Starting Zookeeper...
start "Zookeeper" cmd /k "cd /d %KAFKA_HOME% && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
ping 127.0.0.1 -n 7 > nul
echo Zookeeper started.
echo.

:: ── Step 3: Start Kafka broker ────────────────────────────────
echo [3/5] Starting Kafka broker...
start "Kafka Broker" cmd /k "cd /d %KAFKA_HOME% && bin\windows\kafka-server-start.bat config\server.properties"
ping 127.0.0.1 -n 9 > nul
echo Kafka started.
echo.

:: ── Step 4: Create the topic (ignore if already exists) ───────
echo [4/5] Creating 'transactions' topic...
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>nul
echo Topic ready.
echo.

:: ── Step 5: Start producer ────────────────────────────────────
echo [5/5] Starting transaction producer...
start "Producer" cmd /k "cd /d %PROJECT_DIR% && %PYTHON% producer.py"
echo Producer started.
echo.

:: ── Launch dashboard ──────────────────────────────────────────
echo Launching Streamlit dashboard...
ping 127.0.0.1 -n 3 > nul
start "Dashboard" cmd /k "cd /d %PROJECT_DIR% && %PYTHON% -m streamlit run app.py"

echo.
echo  ========================================
echo    All components started!
echo    Dashboard: http://localhost:8501
echo    Click 'Start' in the sidebar to begin.
echo  ========================================
echo.
pause
