# Apache Airflow Data Pipeline

This repository contains a comprehensive Apache Airflow data pipeline implementation with multiple DAGs for different data engineering tasks.

## 🚀 Pipeline Overview

The pipeline consists of three main DAGs:

### 1. Data Engineering Pipeline (`data_pipeline_dag.py`)
- **Schedule**: Daily at 6:00 AM UTC
- **Purpose**: Complete ETL pipeline with data extraction, transformation, and loading
- **Features**:
  - Multi-source data extraction (API, CSV files)
  - Data validation and quality checks
  - Data transformation and cleaning
  - Error handling and notifications
  - Automated cleanup

### 2. Data Backup Pipeline (`data_backup_dag.py`)
- **Schedule**: Weekly on Sundays at 2:00 AM UTC
- **Purpose**: Automated data backup and archival
- **Features**:
  - Automated backup creation
  - Data archival for old files (>90 days)
  - Cleanup of temporary files

### 3. Data Quality Monitoring (`data_quality_monitoring_dag.py`)
- **Schedule**: Every 4 hours
- **Purpose**: Continuous data quality monitoring and alerting
- **Features**:
  - Data completeness checks
  - Data accuracy validation
  - Data freshness monitoring
  - Quality score calculation
  - Automated alerting

## 📋 Prerequisites

- Python 3.8+
- Apache Airflow 2.7.3+
- Required Python packages (see `requirements.txt`)

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/vanhungtran/airflow-data-pipeline.git
   cd airflow-data-pipeline
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize Airflow**:
   ```bash
   # Set Airflow home directory
   export AIRFLOW_HOME=/tmp/airflow
   
   # Initialize the database
   airflow db init
   
   # Create admin user
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

4. **Start Airflow services**:
   ```bash
   # Start the webserver (in one terminal)
   airflow webserver --port 8080
   
   # Start the scheduler (in another terminal)
   airflow scheduler
   ```

## 🏗️ Project Structure

```
airflow/
├── dags/                          # DAG definitions
│   ├── data_pipeline_dag.py      # Main ETL pipeline
│   ├── data_backup_dag.py        # Backup pipeline
│   └── data_quality_monitoring_dag.py  # Quality monitoring
├── logs/                          # Airflow logs
├── plugins/                       # Custom Airflow plugins
├── requirements.txt               # Python dependencies
├── airflow.cfg                   # Airflow configuration
└── README.md                     # This file
```

## 📊 Pipeline Features

### Data Extraction
- **API Integration**: Fetches data from external APIs with error handling
- **File Processing**: Processes CSV files with validation
- **Sensor Integration**: File system sensors for dependency management

### Data Transformation
- **Data Cleaning**: Removes duplicates, handles missing values
- **Data Validation**: Ensures data quality and consistency
- **Data Enrichment**: Adds metadata and timestamps

### Data Loading
- **Multi-format Support**: JSON, CSV output formats
- **Batch Processing**: Efficient handling of large datasets
- **Storage Management**: Organized file structure

### Monitoring & Alerting
- **Quality Metrics**: Completeness, accuracy, freshness scores
- **Automated Alerts**: Email notifications for failures
- **Performance Monitoring**: Task execution tracking

## 🔧 Configuration

### Environment Variables
```bash
export AIRFLOW_HOME=/tmp/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
```

### Customizing the Pipeline

1. **Modify API endpoints** in `extract_api_data()` function
2. **Adjust data validation rules** in `validate_data()` function
3. **Update alert recipients** in email configurations
4. **Change schedule intervals** in DAG definitions

## 📈 Monitoring

### Airflow UI
Access the Airflow web interface at `http://localhost:8080`

### Key Metrics to Monitor
- DAG run success rates
- Task execution times
- Data quality scores
- Error frequencies

### Logs
- Task logs: Available in Airflow UI
- System logs: `/tmp/airflow/logs/`
- Quality reports: `/tmp/airflow/quality_checks/`

## 🚨 Troubleshooting

### Common Issues

1. **DAG not appearing**:
   - Check DAG syntax with `airflow dags list`
   - Verify file permissions
   - Check Airflow logs for errors

2. **Task failures**:
   - Review task logs in Airflow UI
   - Check data file paths and permissions
   - Verify API connectivity

3. **Performance issues**:
   - Monitor resource usage
   - Adjust parallelism settings
   - Optimize data processing logic

### Useful Commands

```bash
# List all DAGs
airflow dags list

# Test a specific task
airflow tasks test data_engineering_pipeline extract_api_data 2024-01-01

# Trigger a DAG run
airflow dags trigger data_engineering_pipeline

# Check DAG structure
airflow dags show data_engineering_pipeline
```

## 🔒 Security Considerations

- Change default passwords and secret keys
- Use proper file permissions
- Implement network security for API calls
- Regular security updates

## 🚀 Quick Start (Without Airflow)

If you want to run the pipeline immediately without setting up Airflow:

```bash
# Run the offline pipeline demo
python offline_pipeline.py

# View results
python view_results.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📦 GitHub Setup

To push this project to GitHub:

1. **Create a new repository on GitHub**
2. **Initialize git locally**:
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Airflow data pipeline"
   ```

3. **Connect to GitHub**:
   ```bash
   git remote add origin https://github.com/yourusername/your-repo-name.git
   git branch -M main
   git push -u origin main
   ```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions or issues:
- Check the troubleshooting section
- Review Airflow documentation
- Create an issue in the repository

---

**Happy Data Engineering! 🎉**
