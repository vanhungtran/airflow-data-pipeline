"""
Data Quality Monitoring DAG

This DAG runs continuous data quality checks and monitoring.
Runs every 4 hours to ensure data quality standards are maintained.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import json
import logging
import numpy as np
import os

default_args = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'data_quality_monitoring',
    default_args=default_args,
    description='Continuous data quality monitoring and alerting',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    max_active_runs=1,
    tags=['monitoring', 'data-quality', 'alerting'],
)

def check_data_completeness(**context):
    """
    Check for missing or null values in datasets
    """
    try:
        # Load recent data files
        data_files = ['C:/temp/airflow/loaded/final_data.csv']
        completeness_results = {}
        
        for file_path in data_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                
                # Calculate completeness metrics
                total_records = len(df)
                null_counts = df.isnull().sum()
                completeness_score = 1 - (null_counts.sum() / (total_records * len(df.columns)))
                
                completeness_results[os.path.basename(file_path)] = {
                    'total_records': total_records,
                    'completeness_score': float(completeness_score),
                    'null_counts': null_counts.to_dict(),
                    'threshold_met': completeness_score >= 0.95
                }
        
        # Save results
        os.makedirs('C:/temp/airflow/quality_checks', exist_ok=True)
        with open('C:/temp/airflow/quality_checks/completeness_check.json', 'w') as f:
            json.dump(completeness_results, f, indent=2)
        
        logging.info(f"Completeness check completed: {completeness_results}")
        return completeness_results
        
    except Exception as e:
        logging.error(f"Completeness check failed: {str(e)}")
        raise

def check_data_accuracy(**context):
    """
    Check data accuracy and consistency
    """
    try:
        accuracy_results = {}
        data_files = ['C:/temp/airflow/loaded/final_data.csv']
        
        for file_path in data_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                
                # Accuracy checks
                accuracy_checks = {
                    'duplicate_records': len(df) - len(df.drop_duplicates()),
                    'invalid_emails': 0,  # Would check email format if email column exists
                    'negative_ids': len(df[df.get('id', pd.Series([0])) < 0]) if 'id' in df.columns else 0,
                    'future_dates': 0  # Would check for future dates if date columns exist
                }
                
                accuracy_score = 1 - sum(accuracy_checks.values()) / len(df)
                
                accuracy_results[os.path.basename(file_path)] = {
                    'accuracy_score': float(accuracy_score),
                    'accuracy_checks': accuracy_checks,
                    'threshold_met': accuracy_score >= 0.98
                }
        
        # Save results
        with open('C:/temp/airflow/quality_checks/accuracy_check.json', 'w') as f:
            json.dump(accuracy_results, f, indent=2)
        
        logging.info(f"Accuracy check completed: {accuracy_results}")
        return accuracy_results
        
    except Exception as e:
        logging.error(f"Accuracy check failed: {str(e)}")
        raise

def check_data_freshness(**context):
    """
    Check if data is fresh and up-to-date
    """
    try:
        freshness_results = {}
        current_time = datetime.now()
        
        # Check file modification times
        data_files = ['C:/temp/airflow/loaded/final_data.csv']
        
        for file_path in data_files:
            if os.path.exists(file_path):
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                hours_since_update = (current_time - file_mtime).total_seconds() / 3600
                
                # Data is considered fresh if updated within last 24 hours
                is_fresh = hours_since_update <= 24
                
                freshness_results[os.path.basename(file_path)] = {
                    'last_updated': file_mtime.isoformat(),
                    'hours_since_update': hours_since_update,
                    'is_fresh': is_fresh,
                    'threshold_met': is_fresh
                }
        
        # Save results
        with open('C:/temp/airflow/quality_checks/freshness_check.json', 'w') as f:
            json.dump(freshness_results, f, indent=2)
        
        logging.info(f"Freshness check completed: {freshness_results}")
        return freshness_results
        
    except Exception as e:
        logging.error(f"Freshness check failed: {str(e)}")
        raise

def generate_quality_report(**context):
    """
    Generate comprehensive data quality report
    """
    try:
        # Load all quality check results
        quality_files = [
            'C:/temp/airflow/quality_checks/completeness_check.json',
            'C:/temp/airflow/quality_checks/accuracy_check.json',
            'C:/temp/airflow/quality_checks/freshness_check.json'
        ]
        
        quality_report = {
            'report_timestamp': datetime.now().isoformat(),
            'overall_score': 0,
            'checks_passed': 0,
            'total_checks': 0,
            'details': {}
        }
        
        scores = []
        
        for quality_file in quality_files:
            if os.path.exists(quality_file):
                with open(quality_file, 'r') as f:
                    check_results = json.load(f)
                    quality_report['details'][os.path.basename(quality_file)] = check_results
                    
                    # Extract scores and pass/fail status
                    for file_name, results in check_results.items():
                        if 'completeness_score' in results:
                            scores.append(results['completeness_score'])
                            quality_report['total_checks'] += 1
                            if results.get('threshold_met', False):
                                quality_report['checks_passed'] += 1
                        
                        if 'accuracy_score' in results:
                            scores.append(results['accuracy_score'])
                            quality_report['total_checks'] += 1
                            if results.get('threshold_met', False):
                                quality_report['checks_passed'] += 1
                        
                        if 'is_fresh' in results:
                            scores.append(1.0 if results['is_fresh'] else 0.0)
                            quality_report['total_checks'] += 1
                            if results.get('threshold_met', False):
                                quality_report['checks_passed'] += 1
        
        # Calculate overall score
        if scores:
            quality_report['overall_score'] = float(np.mean(scores))
        
        # Save comprehensive report
        with open('C:/temp/airflow/quality_checks/quality_report.json', 'w') as f:
            json.dump(quality_report, f, indent=2)
        
        logging.info(f"Quality report generated: Overall Score: {quality_report['overall_score']:.2f}")
        return quality_report
        
    except Exception as e:
        logging.error(f"Quality report generation failed: {str(e)}")
        raise

def send_quality_alert(**context):
    """
    Send alert if data quality thresholds are not met
    """
    try:
        quality_report = context['task_instance'].xcom_pull(task_ids='generate_quality_report')
        
        overall_score = quality_report['overall_score']
        checks_passed = quality_report['checks_passed']
        total_checks = quality_report['total_checks']
        
        # Define alert thresholds
        critical_threshold = 0.8
        warning_threshold = 0.9
        
        if overall_score < critical_threshold:
            alert_level = "CRITICAL"
            subject = f"🚨 CRITICAL: Data Quality Alert - Score: {overall_score:.2f}"
        elif overall_score < warning_threshold:
            alert_level = "WARNING"
            subject = f"⚠️ WARNING: Data Quality Alert - Score: {overall_score:.2f}"
        else:
            alert_level = "INFO"
            subject = f"✅ Data Quality Check Passed - Score: {overall_score:.2f}"
        
        message = f"""
        Data Quality Monitoring Report
        
        Overall Quality Score: {overall_score:.2f}
        Checks Passed: {checks_passed}/{total_checks}
        Alert Level: {alert_level}
        
        Detailed Results:
        {json.dumps(quality_report['details'], indent=2)}
        
        Timestamp: {quality_report['report_timestamp']}
        """
        
        logging.info(f"Quality alert prepared: {alert_level}")
        return {'level': alert_level, 'subject': subject, 'message': message}
        
    except Exception as e:
        logging.error(f"Quality alert preparation failed: {str(e)}")
        raise

start_monitoring = DummyOperator(
    task_id='start_monitoring',
    dag=dag,
)

completeness_check = PythonOperator(
    task_id='check_data_completeness',
    python_callable=check_data_completeness,
    dag=dag,
)

accuracy_check = PythonOperator(
    task_id='check_data_accuracy',
    python_callable=check_data_accuracy,
    dag=dag,
)

freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

generate_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag,
)

send_alert = PythonOperator(
    task_id='send_quality_alert',
    python_callable=send_quality_alert,
    dag=dag,
)

end_monitoring = DummyOperator(
    task_id='end_monitoring',
    dag=dag,
)

# Define task dependencies
start_monitoring >> [completeness_check, accuracy_check, freshness_check]
[completeness_check, accuracy_check, freshness_check] >> generate_report
generate_report >> send_alert >> end_monitoring
