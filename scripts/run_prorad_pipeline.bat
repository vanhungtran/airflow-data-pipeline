@echo off
echo =================================================
echo PRORAD Study Data Processing Pipeline
echo =================================================
echo.

cd /d "C:\Users\tralucck\OneDrive\airflow"

echo Activating Python environment...
call "airflow_env\Scripts\activate.bat"

echo.
echo Starting PRORAD pipeline execution...
echo.

python run_prorad_pipeline.py

echo.
echo Pipeline execution completed!
echo Check results in: C:\temp\airflow\prorad_processed\
echo.

pause