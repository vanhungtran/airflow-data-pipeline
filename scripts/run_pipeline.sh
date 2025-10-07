#!/bin/bash

# Airflow Pipeline Runner Script
# This script helps manage and run the Airflow pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

print_header() {
    echo -e "${BLUE}[HEADER]${NC} $1"
}

# Check if virtual environment exists
if [ ! -d "airflow_env" ]; then
    print_error "Virtual environment not found. Please run setup_airflow.sh first."
    exit 1
fi

# Activate virtual environment
source airflow_env/bin/activate

# Set environment variables
export AIRFLOW_HOME=/tmp/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Function to show help
show_help() {
    echo "Airflow Pipeline Runner"
    echo "======================="
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start       Start Airflow webserver and scheduler"
    echo "  stop        Stop all Airflow processes"
    echo "  status      Show status of Airflow services"
    echo "  trigger     Trigger a DAG run"
    echo "  list        List all DAGs"
    echo "  logs        Show logs for a specific task"
    echo "  test        Test a specific task"
    echo "  clean       Clean up temporary files"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 trigger data_engineering_pipeline"
    echo "  $0 test data_engineering_pipeline extract_api_data 2024-01-01"
    echo "  $0 logs data_engineering_pipeline extract_api_data 2024-01-01"
}

# Function to start Airflow services
start_services() {
    print_header "Starting Airflow Services"
    
    # Check if services are already running
    if pgrep -f "airflow webserver" > /dev/null; then
        print_warning "Webserver is already running"
    else
        print_status "Starting Airflow webserver..."
        nohup airflow webserver --port 8080 > /tmp/airflow/webserver.log 2>&1 &
        sleep 3
        print_status "Webserver started on http://localhost:8080"
    fi
    
    if pgrep -f "airflow scheduler" > /dev/null; then
        print_warning "Scheduler is already running"
    else
        print_status "Starting Airflow scheduler..."
        nohup airflow scheduler > /tmp/airflow/scheduler.log 2>&1 &
        sleep 3
        print_status "Scheduler started"
    fi
    
    echo ""
    print_status "✅ Airflow services started successfully!"
    echo "🌐 Web UI: http://localhost:8080"
    echo "👤 Username: admin"
    echo "🔑 Password: admin"
}

# Function to stop Airflow services
stop_services() {
    print_header "Stopping Airflow Services"
    
    # Stop webserver
    if pgrep -f "airflow webserver" > /dev/null; then
        print_status "Stopping webserver..."
        pkill -f "airflow webserver"
        sleep 2
        print_status "Webserver stopped"
    else
        print_warning "Webserver is not running"
    fi
    
    # Stop scheduler
    if pgrep -f "airflow scheduler" > /dev/null; then
        print_status "Stopping scheduler..."
        pkill -f "airflow scheduler"
        sleep 2
        print_status "Scheduler stopped"
    else
        print_warning "Scheduler is not running"
    fi
    
    print_status "✅ All Airflow services stopped"
}

# Function to show status
show_status() {
    print_header "Airflow Services Status"
    
    echo "Webserver:"
    if pgrep -f "airflow webserver" > /dev/null; then
        print_status "✅ Running (PID: $(pgrep -f "airflow webserver"))"
        echo "   URL: http://localhost:8080"
    else
        print_error "❌ Not running"
    fi
    
    echo ""
    echo "Scheduler:"
    if pgrep -f "airflow scheduler" > /dev/null; then
        print_status "✅ Running (PID: $(pgrep -f "airflow scheduler"))"
    else
        print_error "❌ Not running"
    fi
    
    echo ""
    echo "DAGs:"
    airflow dags list | grep -E "(data_engineering_pipeline|data_backup_pipeline|data_quality_monitoring)" || print_warning "No DAGs found"
}

# Function to trigger a DAG
trigger_dag() {
    local dag_id=$1
    if [ -z "$dag_id" ]; then
        print_error "Please specify a DAG ID"
        echo "Available DAGs:"
        airflow dags list
        exit 1
    fi
    
    print_status "Triggering DAG: $dag_id"
    airflow dags trigger "$dag_id"
    print_status "✅ DAG triggered successfully"
}

# Function to list DAGs
list_dags() {
    print_header "Available DAGs"
    airflow dags list
}

# Function to show logs
show_logs() {
    local dag_id=$1
    local task_id=$2
    local execution_date=$3
    
    if [ -z "$dag_id" ] || [ -z "$task_id" ] || [ -z "$execution_date" ]; then
        print_error "Usage: $0 logs <dag_id> <task_id> <execution_date>"
        echo "Example: $0 logs data_engineering_pipeline extract_api_data 2024-01-01"
        exit 1
    fi
    
    print_status "Showing logs for $dag_id.$task_id on $execution_date"
    airflow tasks log "$dag_id" "$task_id" "$execution_date"
}

# Function to test a task
test_task() {
    local dag_id=$1
    local task_id=$2
    local execution_date=$3
    
    if [ -z "$dag_id" ] || [ -z "$task_id" ] || [ -z "$execution_date" ]; then
        print_error "Usage: $0 test <dag_id> <task_id> <execution_date>"
        echo "Example: $0 test data_engineering_pipeline extract_api_data 2024-01-01"
        exit 1
    fi
    
    print_status "Testing task: $dag_id.$task_id on $execution_date"
    airflow tasks test "$dag_id" "$task_id" "$execution_date"
}

# Function to clean up
cleanup() {
    print_header "Cleaning Up Temporary Files"
    
    print_status "Cleaning up old log files..."
    find /tmp/airflow/logs -name "*.log" -mtime +7 -delete 2>/dev/null || true
    
    print_status "Cleaning up temporary data files..."
    find /tmp/airflow/extracted -name "*.tmp" -delete 2>/dev/null || true
    find /tmp/airflow/transformed -name "*.tmp" -delete 2>/dev/null || true
    
    print_status "Cleaning up old backup files..."
    find /tmp/airflow/backups -name "*.tmp" -delete 2>/dev/null || true
    
    print_status "✅ Cleanup completed"
}

# Main script logic
case "${1:-help}" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    status)
        show_status
        ;;
    trigger)
        trigger_dag "$2"
        ;;
    list)
        list_dags
        ;;
    logs)
        show_logs "$2" "$3" "$4"
        ;;
    test)
        test_task "$2" "$3" "$4"
        ;;
    clean)
        cleanup
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
