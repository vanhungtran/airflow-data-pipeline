#!/usr/bin/env python3
"""
PRORAD Study Airflow DAG
Complete pipeline for PRORAD study data processing with longitudinal analysis
Processes entire dataset with variable mapping, language normalization, and quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import numpy as np
import json
import logging
import os
import sys

# Add the airflow directory to Python path
sys.path.append('/c/Users/tralucck/OneDrive/airflow')

# Default arguments for the DAG
default_args = {
    'owner': 'prorad-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
    'max_active_runs': 1
}

# Create the DAG
dag = DAG(
    'prorad_study_pipeline',
    default_args=default_args,
    description='Complete PRORAD study data processing pipeline with longitudinal analysis',
    schedule_interval='@weekly',  # Run weekly to process new data
    max_active_runs=1,
    tags=['prorad', 'medical', 'longitudinal', 'german-english', 'quality-checks']
)

def validate_input_data(**context):
    """Validate input data availability and structure"""
    logging.info("🔍 Validating input data...")
    
    input_path = "C:/Users/tralucck/OneDrive/airflow/input"
    
    # Check required files
    required_files = ['input.csv', 'vars_dict.csv']
    missing_files = []
    
    for file in required_files:
        file_path = os.path.join(input_path, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
        else:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logging.info(f"✅ Found {file}: {file_size:.1f} MB")
    
    if missing_files:
        raise FileNotFoundError(f"Missing required files: {missing_files}")
    
    # Basic validation of input.csv
    try:
        df_sample = pd.read_csv(os.path.join(input_path, 'input.csv'), nrows=5)
        logging.info(f"📊 Input data preview: {len(df_sample)} rows × {len(df_sample.columns)} columns")
        
        # Store metadata for downstream tasks
        context['task_instance'].xcom_push(key='input_columns', value=len(df_sample.columns))
        context['task_instance'].xcom_push(key='validation_status', value='success')
        
    except Exception as e:
        logging.error(f"❌ Error validating input data: {e}")
        raise
    
    logging.info("✅ Input data validation completed successfully")

def run_variable_mapping(**context):
    """Execute variable mapping from VAR_ORIGINAL to VAR_NEW"""
    logging.info("🏷️ Starting variable mapping process...")
    
    try:
        from prorad_pipeline import PRORADProcessor
        
        # Initialize processor
        processor = PRORADProcessor()
        
        # Load data
        input_file = "C:/Users/tralucck/OneDrive/airflow/input/input.csv"
        df = pd.read_csv(input_file, low_memory=False)
        
        logging.info(f"📥 Loaded dataset: {len(df)} rows × {len(df.columns)} columns")
        
        # Apply variable mapping
        df_mapped = processor.apply_variable_mapping(df)
        
        # Store intermediate result
        temp_path = "C:/temp/airflow/prorad_processed/intermediate_mapped.csv"
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        df_mapped.to_csv(temp_path, index=False)
        
        # Push metadata to XCom
        mapping_stats = {
            'total_columns': len(df.columns),
            'mapped_columns': processor.processing_stats['mapped_columns'],
            'unmapped_columns': processor.processing_stats['unmapped_columns'],
            'mapping_success_rate': processor.processing_stats['mapped_columns'] / len(df.columns) * 100
        }
        
        context['task_instance'].xcom_push(key='mapping_stats', value=mapping_stats)
        context['task_instance'].xcom_push(key='intermediate_file', value=temp_path)
        
        logging.info(f"✅ Variable mapping completed: {mapping_stats['mapping_success_rate']:.1f}% success rate")
        
    except Exception as e:
        logging.error(f"❌ Error in variable mapping: {e}")
        raise

def run_language_normalization(**context):
    """Execute German to English translation and spell checking"""
    logging.info("🌍 Starting language normalization process...")
    
    try:
        from prorad_pipeline import PRORADProcessor
        
        # Get intermediate file from previous task
        intermediate_file = context['task_instance'].xcom_pull(task_ids='variable_mapping', key='intermediate_file')
        
        if not intermediate_file or not os.path.exists(intermediate_file):
            raise FileNotFoundError("Intermediate mapped file not found")
        
        # Load mapped data
        df_mapped = pd.read_csv(intermediate_file)
        logging.info(f"📥 Loaded mapped dataset: {len(df_mapped)} rows × {len(df_mapped.columns)} columns")
        
        # Initialize processor and apply language normalization
        processor = PRORADProcessor()
        df_normalized = processor.apply_language_normalization(df_mapped)
        
        # Store normalized result
        normalized_path = "C:/temp/airflow/prorad_processed/intermediate_normalized.csv"
        df_normalized.to_csv(normalized_path, index=False)
        
        # Push translation statistics
        translation_stats = {
            'translated_values': processor.processing_stats['translated_values'],
            'inconsistencies_found': processor.processing_stats['inconsistencies_found']
        }
        
        context['task_instance'].xcom_push(key='translation_stats', value=translation_stats)
        context['task_instance'].xcom_push(key='normalized_file', value=normalized_path)
        
        logging.info(f"✅ Language normalization completed: {translation_stats['translated_values']} values translated")
        
    except Exception as e:
        logging.error(f"❌ Error in language normalization: {e}")
        raise

def run_quality_checks(**context):
    """Execute comprehensive data quality checks"""
    logging.info("🔍 Starting quality checks...")
    
    try:
        from prorad_pipeline import PRORADProcessor
        
        # Get normalized file from previous task
        normalized_file = context['task_instance'].xcom_pull(task_ids='language_normalization', key='normalized_file')
        
        if not normalized_file or not os.path.exists(normalized_file):
            raise FileNotFoundError("Normalized file not found")
        
        # Load normalized data
        df_normalized = pd.read_csv(normalized_file)
        logging.info(f"📥 Loaded normalized dataset: {len(df_normalized)} rows × {len(df_normalized.columns)} columns")
        
        # Initialize processor and run quality checks
        processor = PRORADProcessor()
        quality_report = processor.run_quality_checks(df_normalized)
        
        # Push quality metrics
        quality_summary = {
            'total_rows': quality_report['dataset_overview']['total_rows'],
            'total_columns': quality_report['dataset_overview']['total_columns'],
            'memory_usage_mb': quality_report['dataset_overview']['memory_usage_mb'],
            'time_points_detected': len(quality_report['longitudinal_structure']['detected_time_points']),
            'max_time_point': quality_report['longitudinal_structure']['max_time_point'],
            'longitudinal_columns': quality_report['longitudinal_structure']['longitudinal_columns']
        }
        
        context['task_instance'].xcom_push(key='quality_summary', value=quality_summary)
        context['task_instance'].xcom_push(key='quality_report', value=quality_report)
        context['task_instance'].xcom_push(key='final_file', value=normalized_file)
        
        logging.info(f"✅ Quality checks completed: {quality_summary['time_points_detected']} time points detected")
        
    except Exception as e:
        logging.error(f"❌ Error in quality checks: {e}")
        raise

def save_final_dataset(**context):
    """Save the final processed dataset in multiple formats"""
    logging.info("💾 Saving final processed dataset...")
    
    try:
        from prorad_pipeline import PRORADProcessor
        
        # Get final file from previous task
        final_file = context['task_instance'].xcom_pull(task_ids='quality_checks', key='final_file')
        
        if not final_file or not os.path.exists(final_file):
            raise FileNotFoundError("Final processed file not found")
        
        # Load final data
        df_final = pd.read_csv(final_file)
        logging.info(f"📥 Loaded final dataset: {len(df_final)} rows × {len(df_final.columns)} columns")
        
        # Initialize processor and save dataset
        processor = PRORADProcessor()
        processor.processing_stats = {
            'total_rows': len(df_final),
            'total_columns': len(df_final.columns),
            'mapped_columns': context['task_instance'].xcom_pull(task_ids='variable_mapping', key='mapping_stats')['mapped_columns'],
            'unmapped_columns': context['task_instance'].xcom_pull(task_ids='variable_mapping', key='mapping_stats')['unmapped_columns'],
            'translated_values': context['task_instance'].xcom_pull(task_ids='language_normalization', key='translation_stats')['translated_values'],
            'inconsistencies_found': context['task_instance'].xcom_pull(task_ids='language_normalization', key='translation_stats')['inconsistencies_found'],
            'spell_corrections_made': 0
        }
        
        processor.save_processed_dataset(df_final)
        
        # Get quality report for final report
        quality_report = context['task_instance'].xcom_pull(task_ids='quality_checks', key='quality_report')
        processor.generate_processing_report(quality_report)
        
        # Push final statistics
        final_stats = {
            'processing_timestamp': datetime.now().isoformat(),
            'total_rows': len(df_final),
            'total_columns': len(df_final.columns),
            'processing_success': True
        }
        
        context['task_instance'].xcom_push(key='final_stats', value=final_stats)
        
        logging.info("✅ Final dataset saved successfully")
        
    except Exception as e:
        logging.error(f"❌ Error saving final dataset: {e}")
        raise

def generate_processing_summary(**context):
    """Generate and log processing summary"""
    logging.info("📋 Generating processing summary...")
    
    try:
        # Collect statistics from all tasks
        mapping_stats = context['task_instance'].xcom_pull(task_ids='variable_mapping', key='mapping_stats')
        translation_stats = context['task_instance'].xcom_pull(task_ids='language_normalization', key='translation_stats')
        quality_summary = context['task_instance'].xcom_pull(task_ids='quality_checks', key='quality_summary')
        final_stats = context['task_instance'].xcom_pull(task_ids='save_final_dataset', key='final_stats')
        
        # Create comprehensive summary
        summary = {
            'prorad_pipeline_summary': {
                'execution_date': context['execution_date'].isoformat(),
                'dag_run_id': context['dag_run'].run_id,
                'pipeline_status': 'SUCCESS',
                'dataset_statistics': {
                    'total_rows': final_stats['total_rows'],
                    'total_columns': final_stats['total_columns'],
                    'variable_mapping': mapping_stats,
                    'language_normalization': translation_stats,
                    'quality_metrics': quality_summary
                },
                'processing_timestamps': {
                    'pipeline_start': context['dag_run'].start_date.isoformat() if context['dag_run'].start_date else None,
                    'pipeline_end': datetime.now().isoformat()
                }
            }
        }
        
        # Save summary to file
        summary_path = "C:/temp/airflow/prorad_processed/reports/pipeline_execution_summary.json"
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Log key metrics
        logging.info("🎉 PRORAD Pipeline Execution Summary:")
        logging.info(f"   📊 Dataset: {final_stats['total_rows']:,} rows × {final_stats['total_columns']:,} columns")
        logging.info(f"   🏷️ Variable Mapping: {mapping_stats['mapping_success_rate']:.1f}% success rate")
        logging.info(f"   🌍 Language Normalization: {translation_stats['translated_values']:,} values translated")
        logging.info(f"   📈 Longitudinal Structure: {quality_summary['time_points_detected']} time points (T0-T{quality_summary['max_time_point']})")
        logging.info(f"   💾 Output Location: C:/temp/airflow/prorad_processed/")
        
        context['task_instance'].xcom_push(key='pipeline_summary', value=summary)
        
        logging.info("✅ Processing summary generated successfully")
        
    except Exception as e:
        logging.error(f"❌ Error generating processing summary: {e}")
        raise

def send_notification_email(**context):
    """Send email notification about pipeline completion"""
    logging.info("📧 Preparing notification email...")
    
    try:
        final_stats = context['task_instance'].xcom_pull(task_ids='save_final_dataset', key='final_stats')
        pipeline_summary = context['task_instance'].xcom_pull(task_ids='generate_summary', key='pipeline_summary')
        
        if not final_stats or not pipeline_summary:
            logging.warning("⚠️ Missing statistics for email notification")
            return
        
        # Email content
        subject = f"PRORAD Pipeline Completed - {datetime.now().strftime('%Y-%m-%d')}"
        
        body = f"""
        PRORAD Study Data Processing Pipeline Completed Successfully
        
        Execution Details:
        - Pipeline Run ID: {context['dag_run'].run_id}
        - Execution Date: {context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}
        - Dataset Processed: {final_stats['total_rows']:,} rows × {final_stats['total_columns']:,} columns
        
        Processing Results:
        - Variable Mapping: Completed with {pipeline_summary['prorad_pipeline_summary']['dataset_statistics']['variable_mapping']['mapping_success_rate']:.1f}% success rate
        - Language Normalization: {pipeline_summary['prorad_pipeline_summary']['dataset_statistics']['language_normalization']['translated_values']:,} values translated
        - Quality Checks: {pipeline_summary['prorad_pipeline_summary']['dataset_statistics']['quality_metrics']['time_points_detected']} time points detected
        
        Output Location: C:/temp/airflow/prorad_processed/
        
        The processed PRORAD dataset is ready for analysis.
        """
        
        # Log email content (actual email sending would require SMTP configuration)
        logging.info(f"📧 Email notification prepared:")
        logging.info(f"   Subject: {subject}")
        logging.info(f"   Body preview: {body[:200]}...")
        
        logging.info("✅ Notification email prepared (configure SMTP for actual sending)")
        
    except Exception as e:
        logging.error(f"❌ Error preparing notification email: {e}")

# Define DAG structure
start_task = DummyOperator(
    task_id='start_prorad_pipeline',
    dag=dag
)

validate_input_task = PythonOperator(
    task_id='validate_input_data',
    python_callable=validate_input_data,
    dag=dag
)

variable_mapping_task = PythonOperator(
    task_id='variable_mapping',
    python_callable=run_variable_mapping,
    dag=dag
)

language_normalization_task = PythonOperator(
    task_id='language_normalization',
    python_callable=run_language_normalization,
    dag=dag
)

quality_checks_task = PythonOperator(
    task_id='quality_checks',
    python_callable=run_quality_checks,
    dag=dag
)

save_dataset_task = PythonOperator(
    task_id='save_final_dataset',
    python_callable=save_final_dataset,
    dag=dag
)

generate_summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_processing_summary,
    dag=dag
)

notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification_email,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_prorad_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> validate_input_task >> variable_mapping_task >> language_normalization_task >> quality_checks_task >> save_dataset_task >> generate_summary_task >> notification_task >> end_task

# Optional: Add parallel quality monitoring branch
quality_monitoring_task = BashOperator(
    task_id='monitor_processing_resources',
    bash_command='echo "Monitoring system resources during PRORAD processing"; df -h; free -m',
    dag=dag
)

# Connect monitoring task to run in parallel with main processing
validate_input_task >> quality_monitoring_task
quality_monitoring_task >> save_dataset_task