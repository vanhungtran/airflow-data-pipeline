"""
Data Backup DAG

This DAG handles automated data backups and archival.
Runs weekly on Sundays at 2:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import shutil
import os
import logging

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'data_backup_pipeline',
    default_args=default_args,
    description='Weekly data backup and archival pipeline',
    schedule_interval='0 2 * * 0',  # Weekly on Sundays at 2:00 AM UTC
    max_active_runs=1,
    tags=['backup', 'weekly', 'data-engineering'],
)

def create_backup(**context):
    """
    Create backup of processed data
    """
    try:
        backup_dir = f'C:/temp/airflow/backups/backup_{context["ds"]}'
        os.makedirs(backup_dir, exist_ok=True)
        
        # Copy data files to backup location
        source_dirs = ['C:/temp/airflow/loaded', 'C:/temp/airflow/transformed']
        
        for source_dir in source_dirs:
            if os.path.exists(source_dir):
                dest_dir = os.path.join(backup_dir, os.path.basename(source_dir))
                shutil.copytree(source_dir, dest_dir)
                logging.info(f"Backed up {source_dir} to {dest_dir}")
        
        return backup_dir
        
    except Exception as e:
        logging.error(f"Backup failed: {str(e)}")
        raise

def archive_old_data(**context):
    """
    Archive data older than 90 days
    """
    try:
        import glob
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.now() - timedelta(days=90)
        archive_dir = 'C:/temp/airflow/archived'
        os.makedirs(archive_dir, exist_ok=True)
        
        # Find old backup files
        old_backups = glob.glob('C:/temp/airflow/backups/backup_*')
        archived_count = 0
        
        for backup in old_backups:
            backup_date_str = backup.split('_')[-1]
            try:
                backup_date = datetime.strptime(backup_date_str, '%Y-%m-%d')
                if backup_date < cutoff_date:
                    archive_path = os.path.join(archive_dir, os.path.basename(backup))
                    shutil.move(backup, archive_path)
                    archived_count += 1
                    logging.info(f"Archived {backup} to {archive_path}")
            except ValueError:
                continue
        
        logging.info(f"Archived {archived_count} old backups")
        return archived_count
        
    except Exception as e:
        logging.error(f"Archive failed: {str(e)}")
        raise

start_backup = DummyOperator(
    task_id='start_backup',
    dag=dag,
)

create_backup_task = PythonOperator(
    task_id='create_backup',
    python_callable=create_backup,
    dag=dag,
)

archive_old_data_task = PythonOperator(
    task_id='archive_old_data',
    python_callable=archive_old_data,
    dag=dag,
)

cleanup_temp_backups = BashOperator(
    task_id='cleanup_temp_backups',
    bash_command='''
    echo "Cleaning up temporary backup files..."
    del /q "C:\\temp\\airflow\\backups\\*.tmp" 2>nul || echo "No temp files to clean"
    echo "Cleanup completed"
    ''',
    dag=dag,
)

end_backup = DummyOperator(
    task_id='end_backup',
    dag=dag,
)

# Define task dependencies
start_backup >> create_backup_task >> archive_old_data_task >> cleanup_temp_backups >> end_backup
