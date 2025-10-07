#!/usr/bin/env python3
"""
Medical Data Pipeline DAG
Apache Airflow DAG for SecuTrial-based medical data processing
Includes treatment duration analysis and patient record management
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import json
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'medical-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'medical_data_pipeline',
    default_args=default_args,
    description='Medical data processing pipeline with SecuTrial preprocessing logic',
    schedule_interval='@daily',  # Run daily at midnight
    max_active_runs=1,
    tags=['medical', 'secutrial', 'treatment-analysis']
)

def extract_medical_data(**context):
    """Extract medical data from SecuTrial-like sources"""
    logging.info("🏥 Starting medical data extraction...")
    
    try:
        # Import the medical pipeline
        import sys
        import os
        sys.path.append('/tmp/airflow/dags')
        
        from medical_data_pipeline import MedicalDataPipeline
        
        # Initialize pipeline
        pipeline = MedicalDataPipeline()
        
        # Generate sample medical data
        df = pipeline.generate_medical_sample_data()
        
        # Store data for next task
        output_path = "/tmp/airflow/extracted/medical_raw_data.json"
        df.to_json(output_path, orient='records', indent=2, date_format='iso')
        
        logging.info(f"✅ Extracted {len(df)} patient records")
        return {
            'status': 'success',
            'records_extracted': len(df),
            'output_path': output_path
        }
        
    except Exception as e:
        logging.error(f"❌ Medical data extraction failed: {e}")
        raise

def transform_medical_data(**context):
    """Transform medical data with SecuTrial preprocessing logic"""
    logging.info("🔄 Starting medical data transformation...")
    
    try:
        # Get data from previous task
        ti = context['ti']
        extract_result = ti.xcom_pull(task_ids='extract_medical_data')
        
        if not extract_result or extract_result['status'] != 'success':
            raise ValueError("Previous extraction task failed")
        
        # Load the extracted data
        df = pd.read_json(extract_result['output_path'])
        
        # Import the medical pipeline for transformation functions
        import sys
        sys.path.append('/tmp/airflow/dags')
        from medical_data_pipeline import MedicalDataPipeline
        
        pipeline = MedicalDataPipeline()
        
        # Apply transformations
        df = pipeline.process_dates_and_calculations(df)
        df = pipeline.apply_data_corrections(df)
        
        # Save transformed data
        output_path = "/tmp/airflow/transformed/medical_transformed_data.json"
        df.to_json(output_path, orient='records', indent=2, date_format='iso')
        
        logging.info(f"✅ Transformed {len(df)} patient records")
        return {
            'status': 'success',
            'records_transformed': len(df),
            'output_path': output_path
        }
        
    except Exception as e:
        logging.error(f"❌ Medical data transformation failed: {e}")
        raise

def analyze_treatment_data(**context):
    """Analyze treatment duration and therapy effectiveness"""
    logging.info("📊 Starting treatment analysis...")
    
    try:
        # Get data from previous task
        ti = context['ti']
        transform_result = ti.xcom_pull(task_ids='transform_medical_data')
        
        if not transform_result or transform_result['status'] != 'success':
            raise ValueError("Previous transformation task failed")
        
        # Load the transformed data
        df = pd.read_json(transform_result['output_path'])
        
        # Import the medical pipeline for analysis functions
        import sys
        sys.path.append('/tmp/airflow/dags')
        from medical_data_pipeline import MedicalDataPipeline
        
        pipeline = MedicalDataPipeline()
        
        # Create therapy analysis
        therapy_df = pipeline.create_therapy_analysis(df)
        
        # Save analysis results
        output_path = "/tmp/airflow/treatment_analysis/therapy_results.json"
        therapy_df.to_json(output_path, orient='records', indent=2, date_format='iso')
        
        logging.info(f"✅ Analyzed {len(therapy_df)} treatment records")
        return {
            'status': 'success',
            'treatment_records': len(therapy_df),
            'output_path': output_path
        }
        
    except Exception as e:
        logging.error(f"❌ Treatment analysis failed: {e}")
        raise

def validate_medical_quality(**context):
    """Validate medical data quality and completeness"""
    logging.info("✅ Starting medical data quality validation...")
    
    try:
        # Get data from previous tasks
        ti = context['ti']
        transform_result = ti.xcom_pull(task_ids='transform_medical_data')
        
        if not transform_result or transform_result['status'] != 'success':
            raise ValueError("Previous transformation task failed")
        
        # Load the data
        df = pd.read_json(transform_result['output_path'])
        
        # Import the medical pipeline for validation
        import sys
        sys.path.append('/tmp/airflow/dags')
        from medical_data_pipeline import MedicalDataPipeline
        
        pipeline = MedicalDataPipeline()
        
        # Run validation
        validation_results = pipeline.validate_medical_data(df)
        
        # Save validation results
        output_path = "/tmp/airflow/quality_checks/medical_validation_results.json"
        with open(output_path, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        # Check if validation passes threshold
        quality_threshold = 80.0
        if validation_results['data_quality_score'] < quality_threshold:
            logging.warning(f"⚠️ Data quality score {validation_results['data_quality_score']:.1f}% below threshold {quality_threshold}%")
            # Could trigger alerts or additional processing here
        
        logging.info(f"✅ Validation completed. Quality score: {validation_results['data_quality_score']:.1f}%")
        return {
            'status': 'success',
            'quality_score': validation_results['data_quality_score'],
            'validation_passed': validation_results['validation_passed'],
            'output_path': output_path
        }
        
    except Exception as e:
        logging.error(f"❌ Medical data validation failed: {e}")
        raise

def load_final_medical_data(**context):
    """Load final medical data to destination"""
    logging.info("💾 Loading final medical data...")
    
    try:
        # Get data from previous tasks
        ti = context['ti']
        transform_result = ti.xcom_pull(task_ids='transform_medical_data')
        analysis_result = ti.xcom_pull(task_ids='analyze_treatment_data')
        validation_result = ti.xcom_pull(task_ids='validate_medical_quality')
        
        # Load and combine data
        df = pd.read_json(transform_result['output_path'])
        therapy_df = pd.read_json(analysis_result['output_path'])
        
        # Save final datasets
        final_patient_path = "/tmp/airflow/loaded/final_patient_data.csv"
        final_therapy_path = "/tmp/airflow/loaded/final_therapy_data.csv"
        
        df.to_csv(final_patient_path, index=False)
        therapy_df.to_csv(final_therapy_path, index=False)
        
        # Create summary
        summary = {
            'pipeline_completed': datetime.now().isoformat(),
            'patients_processed': len(df),
            'treatment_records': len(therapy_df),
            'data_quality_score': validation_result['quality_score'],
            'validation_passed': validation_result['validation_passed'],
            'output_files': {
                'patient_data': final_patient_path,
                'therapy_data': final_therapy_path
            }
        }
        
        # Save summary
        summary_path = "/tmp/airflow/summary/medical_pipeline_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logging.info(f"✅ Final medical data loaded successfully")
        logging.info(f"📊 Patients: {len(df)}, Treatments: {len(therapy_df)}")
        logging.info(f"📈 Quality Score: {validation_result['quality_score']:.1f}%")
        
        return summary
        
    except Exception as e:
        logging.error(f"❌ Final data loading failed: {e}")
        raise

def create_medical_backup(**context):
    """Create backup of medical data"""
    logging.info("💾 Creating medical data backup...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"/tmp/airflow/backups/medical_backup_{timestamp}"
        
        # Create backup directory
        import os
        os.makedirs(backup_dir, exist_ok=True)
        
        # Copy files to backup
        import shutil
        source_files = [
            "/tmp/airflow/loaded/final_patient_data.csv",
            "/tmp/airflow/loaded/final_therapy_data.csv",
            "/tmp/airflow/summary/medical_pipeline_summary.json"
        ]
        
        for file_path in source_files:
            if os.path.exists(file_path):
                filename = os.path.basename(file_path)
                shutil.copy2(file_path, os.path.join(backup_dir, filename))
        
        logging.info(f"✅ Medical data backup created: {backup_dir}")
        return {
            'status': 'success',
            'backup_location': backup_dir,
            'backup_timestamp': timestamp
        }
        
    except Exception as e:
        logging.error(f"❌ Medical data backup failed: {e}")
        raise

# Define DAG tasks
start_task = DummyOperator(
    task_id='start_medical_pipeline',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_medical_data',
    python_callable=extract_medical_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_medical_data',
    python_callable=transform_medical_data,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_treatment_data',
    python_callable=analyze_treatment_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_medical_quality',
    python_callable=validate_medical_quality,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_final_medical_data',
    python_callable=load_final_medical_data,
    dag=dag
)

backup_task = PythonOperator(
    task_id='create_medical_backup',
    python_callable=create_medical_backup,
    dag=dag
)

# Data quality monitoring task
quality_check_task = BashOperator(
    task_id='monitor_data_quality',
    bash_command='''
    echo "🔍 Monitoring medical data quality..."
    QUALITY_FILE="/tmp/airflow/quality_checks/medical_validation_results.json"
    if [ -f "$QUALITY_FILE" ]; then
        echo "✅ Quality validation file found"
        python3 -c "
import json
with open('$QUALITY_FILE', 'r') as f:
    data = json.load(f)
    print(f'Quality Score: {data[\"data_quality_score\"]:.1f}%')
    print(f'Validation Passed: {data[\"validation_passed\"]}')
        "
    else
        echo "❌ Quality validation file not found"
        exit 1
    fi
    ''',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_medical_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> extract_task >> transform_task >> [analyze_task, validate_task]
[analyze_task, validate_task] >> load_task >> quality_check_task >> backup_task >> end_task

# Task documentation
extract_task.doc_md = """
## Medical Data Extraction Task

This task extracts medical data from SecuTrial-like sources, generating sample patient records
with the following features:

- Patient demographics and medical history
- Time-series examination data (t0-t4)
- Treatment information including medications and therapies
- Disease severity scores (SCORAD)
- Physical measurements (weight, height, BMI)

**Output**: Raw medical data in JSON format
"""

transform_task.doc_md = """
## Medical Data Transformation Task

Applies SecuTrial preprocessing logic including:

- Date processing and validation
- BMI calculations for each time point
- Patient age calculations
- Treatment duration analysis
- Data corrections and standardizations
- Boolean indicators for medical conditions

**Output**: Transformed medical data ready for analysis
"""

analyze_task.doc_md = """
## Treatment Analysis Task

Performs comprehensive treatment analysis:

- Treatment duration calculations
- Therapy effectiveness analysis  
- Medication compliance tracking
- Visit-to-treatment distance calculations
- Therapy type categorization

**Output**: Treatment analysis results and therapy dataframe
"""

validate_task.doc_md = """
## Medical Data Quality Validation

Validates medical data quality including:

- Patient ID completeness
- Birth date validity
- Physical measurement ranges
- BMI calculations accuracy
- Treatment duration consistency

**Output**: Data quality metrics and validation report
"""

# DAG documentation
dag.doc_md = """
# Medical Data Pipeline DAG

This DAG processes medical data using SecuTrial preprocessing logic for treatment duration analysis.

## Features

- **Patient Record Processing**: Handles patient demographics, medical history, and examination data
- **Treatment Analysis**: Calculates treatment durations, effectiveness, and compliance metrics
- **Quality Validation**: Comprehensive data quality checks specific to medical data
- **Time Series Processing**: Handles multiple visit time points (t0-t4)
- **Backup & Recovery**: Automated backup creation for data recovery

## Data Flow

1. **Extract**: Generate/load medical data from SecuTrial-like sources
2. **Transform**: Apply medical data preprocessing and calculations
3. **Analyze**: Perform treatment duration and therapy analysis
4. **Validate**: Check data quality and medical data consistency
5. **Load**: Save final datasets (patient data + therapy analysis)
6. **Monitor**: Quality monitoring and alerting
7. **Backup**: Create data backups for recovery

## Output Files

- `final_patient_data.csv`: Processed patient records
- `final_therapy_data.csv`: Treatment analysis results
- Medical data quality reports and validation metrics
- Automated backups with timestamps

## Schedule

Runs daily at midnight to process new medical data and update treatment analyses.
"""