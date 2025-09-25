@echo off
REM Airflow Pipeline Runner Script for Windows
REM This script helps manage and run the Airflow pipeline

setlocal enabledelayedexpansion

REM Check if virtual environment exists
if not exist "airflow_env" (
    echo [ERROR] Virtual environment not found. Please run setup_airflow.bat first.
    pause
    exit /b 1
)

REM Activate virtual environment
call airflow_env\Scripts\activate.bat

REM Set environment variables
set AIRFLOW_HOME=C:\temp\airflow
set AIRFLOW__CORE__LOAD_EXAMPLES=False
set AIRFLOW__CORE__EXECUTOR=LocalExecutor

if "%1"=="" goto :show_help
if "%1"=="help" goto :show_help
if "%1"=="--help" goto :show_help
if "%1"=="-h" goto :show_help

if "%1"=="start" goto :start_services
if "%1"=="stop" goto :stop_services
if "%1"=="status" goto :show_status
if "%1"=="trigger" goto :trigger_dag
if "%1"=="list" goto :list_dags
if "%1"=="logs" goto :show_logs
if "%1"=="test" goto :test_task
if "%1"=="clean" goto :cleanup

echo [ERROR] Unknown command: %1
echo.
goto :show_help

:show_help
echo Airflow Pipeline Runner
echo =======================
echo.
echo Usage: %0 [COMMAND]
echo.
echo Commands:
echo   start       Start Airflow webserver and scheduler
echo   stop        Stop all Airflow processes
echo   status      Show status of Airflow services
echo   trigger     Trigger a DAG run
echo   list        List all DAGs
echo   logs        Show logs for a specific task
echo   test        Test a specific task
echo   clean       Clean up temporary files
echo   help        Show this help message
echo.
echo Examples:
echo   %0 start
echo   %0 trigger data_engineering_pipeline
echo   %0 test data_engineering_pipeline extract_api_data 2024-01-01
echo   %0 logs data_engineering_pipeline extract_api_data 2024-01-01
goto :eof

:start_services
echo [HEADER] Starting Airflow Services

REM Check if services are already running
tasklist /FI "IMAGENAME eq python.exe" /FI "WINDOWTITLE eq airflow webserver" 2>nul | find /I "python.exe" >nul
if not errorlevel 1 (
    echo [WARNING] Webserver might already be running
) else (
    echo [INFO] Starting Airflow webserver...
    start "Airflow Webserver" /MIN cmd /c "airflow webserver --port 8080"
    timeout /t 3 /nobreak >nul
    echo [INFO] Webserver started on http://localhost:8080
)

tasklist /FI "IMAGENAME eq python.exe" /FI "WINDOWTITLE eq airflow scheduler" 2>nul | find /I "python.exe" >nul
if not errorlevel 1 (
    echo [WARNING] Scheduler might already be running
) else (
    echo [INFO] Starting Airflow scheduler...
    start "Airflow Scheduler" /MIN cmd /c "airflow scheduler"
    timeout /t 3 /nobreak >nul
    echo [INFO] Scheduler started
)

echo.
echo ✅ Airflow services started successfully!
echo 🌐 Web UI: http://localhost:8080
echo 👤 Username: admin
echo 🔑 Password: admin
goto :eof

:stop_services
echo [HEADER] Stopping Airflow Services

echo [INFO] Stopping Airflow processes...
taskkill /F /IM python.exe /FI "WINDOWTITLE eq airflow webserver" 2>nul
taskkill /F /IM python.exe /FI "WINDOWTITLE eq airflow scheduler" 2>nul

echo [INFO] ✅ All Airflow services stopped
goto :eof

:show_status
echo [HEADER] Airflow Services Status

echo Webserver:
tasklist /FI "IMAGENAME eq python.exe" /FI "WINDOWTITLE eq airflow webserver" 2>nul | find /I "python.exe" >nul
if not errorlevel 1 (
    echo [INFO] ✅ Running
    echo    URL: http://localhost:8080
) else (
    echo [ERROR] ❌ Not running
)

echo.
echo Scheduler:
tasklist /FI "IMAGENAME eq python.exe" /FI "WINDOWTITLE eq airflow scheduler" 2>nul | find /I "python.exe" >nul
if not errorlevel 1 (
    echo [INFO] ✅ Running
) else (
    echo [ERROR] ❌ Not running
)

echo.
echo DAGs:
airflow dags list | findstr "data_engineering_pipeline data_backup_pipeline data_quality_monitoring"
goto :eof

:trigger_dag
if "%2"=="" (
    echo [ERROR] Please specify a DAG ID
    echo Available DAGs:
    airflow dags list
    goto :eof
)

echo [INFO] Triggering DAG: %2
airflow dags trigger %2
echo [INFO] ✅ DAG triggered successfully
goto :eof

:list_dags
echo [HEADER] Available DAGs
airflow dags list
goto :eof

:show_logs
if "%2"=="" (
    echo [ERROR] Usage: %0 logs ^<dag_id^> ^<task_id^> ^<execution_date^>
    echo Example: %0 logs data_engineering_pipeline extract_api_data 2024-01-01
    goto :eof
)

echo [INFO] Showing logs for %2.%3 on %4
airflow tasks log %2 %3 %4
goto :eof

:test_task
if "%2"=="" (
    echo [ERROR] Usage: %0 test ^<dag_id^> ^<task_id^> ^<execution_date^>
    echo Example: %0 test data_engineering_pipeline extract_api_data 2024-01-01
    goto :eof
)

echo [INFO] Testing task: %2.%3 on %4
airflow tasks test %2 %3 %4
goto :eof

:cleanup
echo [HEADER] Cleaning Up Temporary Files

echo [INFO] Cleaning up temporary data files...
if exist "C:\temp\airflow\extracted\*.tmp" del "C:\temp\airflow\extracted\*.tmp"
if exist "C:\temp\airflow\transformed\*.tmp" del "C:\temp\airflow\transformed\*.tmp"
if exist "C:\temp\airflow\backups\*.tmp" del "C:\temp\airflow\backups\*.tmp"

echo [INFO] ✅ Cleanup completed
goto :eof
