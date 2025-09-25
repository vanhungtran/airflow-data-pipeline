@echo off
REM Airflow Setup Script for Windows
REM This script sets up Apache Airflow with the data pipeline

echo 🚀 Setting up Apache Airflow Data Pipeline...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed. Please install Python 3.8+ first.
    pause
    exit /b 1
)

echo [INFO] Python check passed

REM Create virtual environment
echo [INFO] Creating virtual environment...
python -m venv airflow_env
call airflow_env\Scripts\activate.bat

REM Upgrade pip
echo [INFO] Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo [INFO] Installing Python packages...
pip install -r requirements.txt

REM Set environment variables
echo [INFO] Setting up environment variables...
set AIRFLOW_HOME=C:\temp\airflow
set AIRFLOW__CORE__LOAD_EXAMPLES=False
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
set AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///C:\temp\airflow\airflow.db

REM Create necessary directories
echo [INFO] Creating Airflow directories...
if not exist "C:\temp\airflow" mkdir "C:\temp\airflow"
if not exist "C:\temp\airflow\dags" mkdir "C:\temp\airflow\dags"
if not exist "C:\temp\airflow\logs" mkdir "C:\temp\airflow\logs"
if not exist "C:\temp\airflow\plugins" mkdir "C:\temp\airflow\plugins"
if not exist "C:\temp\airflow\backups" mkdir "C:\temp\airflow\backups"
if not exist "C:\temp\airflow\extracted" mkdir "C:\temp\airflow\extracted"
if not exist "C:\temp\airflow\transformed" mkdir "C:\temp\airflow\transformed"
if not exist "C:\temp\airflow\loaded" mkdir "C:\temp\airflow\loaded"
if not exist "C:\temp\airflow\quality_checks" mkdir "C:\temp\airflow\quality_checks"
if not exist "C:\temp\airflow\monitoring" mkdir "C:\temp\airflow\monitoring"
if not exist "C:\temp\airflow\summary" mkdir "C:\temp\airflow\summary"
if not exist "C:\temp\airflow\archived" mkdir "C:\temp\airflow\archived"

REM Copy DAGs to Airflow directory
echo [INFO] Copying DAG files...
copy "dags\*.py" "C:\temp\airflow\dags\"

REM Initialize Airflow database
echo [INFO] Initializing Airflow database...
airflow db init

REM Create admin user
echo [INFO] Creating admin user...
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

REM Create sample input data
echo [INFO] Creating sample input data...
if not exist "C:\temp\airflow\input" mkdir "C:\temp\airflow\input"
echo sample,data,file > "C:\temp\airflow\input\sample.csv"
echo another,sample,file > "C:\temp\airflow\input\another.csv"

echo.
echo ✅ Airflow setup completed successfully!
echo.
echo 🎉 Setup Summary:
echo ==================
echo • Airflow Home: C:\temp\airflow
echo • Admin Username: admin
echo • Admin Password: admin
echo • Web UI: http://localhost:8080
echo • Database: SQLite
echo.
echo 🚀 Next Steps:
echo ==============
echo 1. Activate the virtual environment:
echo    airflow_env\Scripts\activate.bat
echo.
echo 2. Start the webserver (in one terminal):
echo    airflow webserver --port 8080
echo.
echo 3. Start the scheduler (in another terminal):
echo    airflow scheduler
echo.
echo 4. Access the web UI at: http://localhost:8080
echo.
echo 📚 Available DAGs:
echo ==================
echo • data_engineering_pipeline (Daily at 6:00 AM UTC)
echo • data_backup_pipeline (Weekly on Sundays at 2:00 AM UTC)
echo • data_quality_monitoring (Every 4 hours)
echo.
echo Happy Data Engineering! 🎉
pause
