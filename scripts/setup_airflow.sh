#!/bin/bash

# Airflow Setup Script
# This script sets up Apache Airflow with the data pipeline

set -e

echo "🚀 Setting up Apache Airflow Data Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.8"

if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" != "$REQUIRED_VERSION" ]; then
    print_error "Python 3.8+ is required. Current version: $PYTHON_VERSION"
    exit 1
fi

print_status "Python version check passed: $PYTHON_VERSION"

# Create virtual environment
print_status "Creating virtual environment..."
python3 -m venv airflow_env
source airflow_env/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip

# Install requirements
print_status "Installing Python packages..."
pip install -r requirements.txt

# Set environment variables
print_status "Setting up environment variables..."
export AIRFLOW_HOME=/tmp/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow/airflow.db

# Create necessary directories
print_status "Creating Airflow directories..."
mkdir -p /tmp/airflow/{dags,logs,plugins,backups,extracted,transformed,loaded,quality_checks,monitoring,summary,archived}

# Copy DAGs to Airflow directory
print_status "Copying DAG files..."
cp dags/*.py /tmp/airflow/dags/

# Initialize Airflow database
print_status "Initializing Airflow database..."
airflow db init

# Create admin user
print_status "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || print_warning "Admin user might already exist"

# Create sample input data
print_status "Creating sample input data..."
mkdir -p /tmp/airflow/input
echo "sample,data,file" > /tmp/airflow/input/sample.csv
echo "another,sample,file" > /tmp/airflow/input/another.csv

print_status "✅ Airflow setup completed successfully!"

echo ""
echo "🎉 Setup Summary:"
echo "=================="
echo "• Airflow Home: /tmp/airflow"
echo "• Admin Username: admin"
echo "• Admin Password: admin"
echo "• Web UI: http://localhost:8080"
echo "• Database: SQLite"
echo ""
echo "🚀 Next Steps:"
echo "=============="
echo "1. Activate the virtual environment:"
echo "   source airflow_env/bin/activate"
echo ""
echo "2. Start the webserver (in one terminal):"
echo "   airflow webserver --port 8080"
echo ""
echo "3. Start the scheduler (in another terminal):"
echo "   airflow scheduler"
echo ""
echo "4. Access the web UI at: http://localhost:8080"
echo ""
echo "📚 Available DAGs:"
echo "=================="
echo "• data_engineering_pipeline (Daily at 6:00 AM UTC)"
echo "• data_backup_pipeline (Weekly on Sundays at 2:00 AM UTC)"
echo "• data_quality_monitoring (Every 4 hours)"
echo ""
echo "Happy Data Engineering! 🎉"
