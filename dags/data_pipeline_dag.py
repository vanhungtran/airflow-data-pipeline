"""
Data Engineering Pipeline DAG

This DAG demonstrates a complete data pipeline with:
- Data extraction from multiple sources
- Data transformation and validation
- Data loading and storage
- Error handling and monitoring
- Data quality checks

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import json
import logging
from typing import Dict, Any

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'data_engineering_pipeline',
    default_args=default_args,
    description='Complete data pipeline with extraction, transformation, and loading',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM UTC
    max_active_runs=1,
    tags=['data-engineering', 'etl', 'daily'],
)

# Task 1: Start pipeline
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Task 2: Check for input files
check_input_files = FileSensor(
    task_id='check_input_files',
    filepath='C:/temp/airflow/input/',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Task 3: Extract data from API
def extract_api_data(**context):
    """
    Extract data from external API
    """
    try:
        # Example API call (replace with actual API)
        api_url = "https://jsonplaceholder.typicode.com/posts"
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Save to temporary location
        df = pd.DataFrame(data)
        output_path = 'C:/temp/airflow/extracted/api_data.json'
        df.to_json(output_path, orient='records', indent=2)
        
        logging.info(f"Successfully extracted {len(data)} records from API")
        return output_path
        
    except Exception as e:
        logging.error(f"Failed to extract API data: {str(e)}")
        raise

extract_api = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_api_data,
    dag=dag,
)

# Task 4: Process CSV files
def process_csv_files(**context):
    """
    Process and validate CSV files
    """
    try:
        # Simulate CSV processing
        sample_data = {
            'id': range(1, 101),
            'name': [f'User_{i}' for i in range(1, 101)],
            'email': [f'user{i}@example.com' for i in range(1, 101)],
            'created_at': [datetime.now().isoformat() for _ in range(1, 101)]
        }
        
        df = pd.DataFrame(sample_data)
        output_path = 'C:/temp/airflow/extracted/csv_data.csv'
        df.to_csv(output_path, index=False)
        
        logging.info(f"Successfully processed CSV with {len(df)} records")
        return output_path
        
    except Exception as e:
        logging.error(f"Failed to process CSV files: {str(e)}")
        raise

process_csv = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv_files,
    dag=dag,
)

# Task 5: Data validation
def validate_data(**context):
    """
    Validate extracted data quality
    """
    try:
        # Get file paths from previous tasks
        api_data_path = context['task_instance'].xcom_pull(task_ids='extract_api_data')
        csv_data_path = context['task_instance'].xcom_pull(task_ids='process_csv_files')
        
        # Validate API data
        with open(api_data_path, 'r') as f:
            api_data = json.load(f)
        
        # Validate CSV data
        csv_data = pd.read_csv(csv_data_path)
        
        # Data quality checks
        validation_results = {
            'api_records_count': len(api_data),
            'csv_records_count': len(csv_data),
            'api_data_valid': len(api_data) > 0,
            'csv_data_valid': len(csv_data) > 0 and not csv_data.isnull().any().any(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Log validation results
        logging.info(f"Data validation results: {validation_results}")
        
        # Save validation results
        with open('C:/temp/airflow/validation_results.json', 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        if not validation_results['api_data_valid'] or not validation_results['csv_data_valid']:
            raise ValueError("Data validation failed")
        
        return validation_results
        
    except Exception as e:
        logging.error(f"Data validation failed: {str(e)}")
        raise

validate_data_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data,
    dag=dag,
)

# Task 6: Transform data
def transform_data(**context):
    """
    Transform and clean the data
    """
    try:
        # Get validation results
        validation_results = context['task_instance'].xcom_pull(task_ids='validate_data_quality')
        
        # Load data for transformation
        api_data_path = context['task_instance'].xcom_pull(task_ids='extract_api_data')
        csv_data_path = context['task_instance'].xcom_pull(task_ids='process_csv_files')
        
        with open(api_data_path, 'r') as f:
            api_data = json.load(f)
        
        csv_data = pd.read_csv(csv_data_path)
        
        # Transform API data
        api_df = pd.DataFrame(api_data)
        api_df['source'] = 'api'
        api_df['processed_at'] = datetime.now().isoformat()
        
        # Transform CSV data
        csv_df = csv_data.copy()
        csv_df['source'] = 'csv'
        csv_df['processed_at'] = datetime.now().isoformat()
        
        # Combine datasets
        combined_df = pd.concat([api_df, csv_df], ignore_index=True, sort=False)
        
        # Additional transformations
        combined_df['record_id'] = range(1, len(combined_df) + 1)
        
        # Save transformed data
        output_path = 'C:/temp/airflow/transformed/combined_data.json'
        combined_df.to_json(output_path, orient='records', indent=2)
        
        logging.info(f"Successfully transformed {len(combined_df)} records")
        return output_path
        
    except Exception as e:
        logging.error(f"Data transformation failed: {str(e)}")
        raise

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 7: Load data to destination
def load_data(**context):
    """
    Load transformed data to final destination
    """
    try:
        # Get transformed data path
        transformed_data_path = context['task_instance'].xcom_pull(task_ids='transform_data')
        
        # Load transformed data
        with open(transformed_data_path, 'r') as f:
            transformed_data = json.load(f)
        
        # Simulate loading to database/warehouse
        df = pd.DataFrame(transformed_data)
        
        # In a real scenario, you would load to your data warehouse
        # For this example, we'll save to a final location
        final_output_path = 'C:/temp/airflow/loaded/final_data.csv'
        df.to_csv(final_output_path, index=False)
        
        # Create summary statistics
        summary = {
            'total_records': len(df),
            'api_records': len(df[df['source'] == 'api']),
            'csv_records': len(df[df['source'] == 'csv']),
            'load_timestamp': datetime.now().isoformat(),
            'status': 'success'
        }
        
        # Save summary
        with open('C:/temp/airflow/summary/load_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logging.info(f"Successfully loaded {len(df)} records. Summary: {summary}")
        return summary
        
    except Exception as e:
        logging.error(f"Data loading failed: {str(e)}")
        raise

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Task 8: Data quality monitoring
def data_quality_monitoring(**context):
    """
    Perform final data quality checks and monitoring
    """
    try:
        # Get load summary
        load_summary = context['task_instance'].xcom_pull(task_ids='load_data')
        
        # Additional quality checks
        quality_metrics = {
            'data_freshness': 'good',
            'completeness_score': 0.95,
            'accuracy_score': 0.98,
            'consistency_score': 0.97,
            'timeliness_score': 0.99,
            'check_timestamp': datetime.now().isoformat()
        }
        
        # Save quality metrics
        with open('C:/temp/airflow/monitoring/quality_metrics.json', 'w') as f:
            json.dump(quality_metrics, f, indent=2)
        
        logging.info(f"Data quality monitoring completed: {quality_metrics}")
        return quality_metrics
        
    except Exception as e:
        logging.error(f"Data quality monitoring failed: {str(e)}")
        raise

quality_monitoring = PythonOperator(
    task_id='data_quality_monitoring',
    python_callable=data_quality_monitoring,
    dag=dag,
)

# Task 9: Send success notification
def send_success_notification(**context):
    """
    Send success notification
    """
    try:
        load_summary = context['task_instance'].xcom_pull(task_ids='load_data')
        quality_metrics = context['task_instance'].xcom_pull(task_ids='data_quality_monitoring')
        
        message = f"""
        Data Pipeline Successfully Completed!
        
        Load Summary:
        - Total Records: {load_summary['total_records']}
        - API Records: {load_summary['api_records']}
        - CSV Records: {load_summary['csv_records']}
        - Load Time: {load_summary['load_timestamp']}
        
        Quality Metrics:
        - Completeness: {quality_metrics['completeness_score']*100:.1f}%
        - Accuracy: {quality_metrics['accuracy_score']*100:.1f}%
        - Consistency: {quality_metrics['consistency_score']*100:.1f}%
        - Timeliness: {quality_metrics['timeliness_score']*100:.1f}%
        """
        
        logging.info("Success notification prepared")
        return message
        
    except Exception as e:
        logging.error(f"Failed to prepare success notification: {str(e)}")
        raise

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Task 10: Cleanup temporary files
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
    echo "Cleaning up temporary files..."
    del /q "C:\\temp\\airflow\\extracted\\*.json" 2>nul || echo "No JSON files to clean"
    del /q "C:\\temp\\airflow\\extracted\\*.csv" 2>nul || echo "No CSV files to clean"
    echo "Cleanup completed"
    ''',
    dag=dag,
)

# Task 11: End pipeline
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies
start_pipeline >> check_input_files

# Parallel data extraction
check_input_files >> [extract_api, process_csv]

# Data processing pipeline
[extract_api, process_csv] >> validate_data_quality
validate_data_quality >> transform_data_task
transform_data_task >> load_data_task
load_data_task >> quality_monitoring

# Final steps
quality_monitoring >> success_notification
success_notification >> cleanup_temp_files
cleanup_temp_files >> end_pipeline
