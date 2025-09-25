#!/usr/bin/env python3
"""
Pipeline Results Viewer
Simple dashboard to view pipeline execution results
"""

import json
import os
import csv
from datetime import datetime

def load_json_file(file_path):
    """Load JSON file safely"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        return f"Error loading {file_path}: {str(e)}"

def display_pipeline_summary():
    """Display pipeline execution summary"""
    print("🎉 PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    
    # Load pipeline summary
    summary_path = "C:/temp/airflow/summary/pipeline_summary.json"
    if os.path.exists(summary_path):
        summary = load_json_file(summary_path)
        if isinstance(summary, dict):
            print(f"📊 Pipeline Status: {summary.get('pipeline_status', 'Unknown')}")
            print(f"⏰ Execution Time: {summary.get('execution_time', 'Unknown')}")
            print(f"🔧 Pipeline Type: {summary.get('pipeline_type', 'Unknown')}")
            
            # Load summary details
            load_summary = summary.get('load_summary', {})
            print(f"📈 Total Records: {load_summary.get('total_records', 0)}")
            print(f"🔗 API Records: {load_summary.get('api_records', 0)}")
            print(f"📄 CSV Records: {load_summary.get('csv_records', 0)}")
            print(f"⭐ Average Quality Score: {load_summary.get('average_quality_score', 0)*100:.1f}%")
            
            # Quality metrics
            quality_metrics = summary.get('quality_metrics', {})
            print(f"📊 Completeness: {quality_metrics.get('completeness_score', 0)*100:.1f}%")
            print(f"🎯 Accuracy: {quality_metrics.get('accuracy_score', 0)*100:.1f}%")
            print(f"🔄 Consistency: {quality_metrics.get('consistency_score', 0)*100:.1f}%")
            print(f"⚡ Timeliness: {quality_metrics.get('timeliness_score', 0)*100:.1f}%")
            
            backup_location = summary.get('backup_location')
            if backup_location:
                print(f"💾 Backup Location: {backup_location}")
        else:
            print(summary)
    else:
        print("❌ Pipeline summary not found")

def display_data_sample():
    """Display a sample of the processed data"""
    print("\n📋 DATA SAMPLE")
    print("=" * 60)
    
    # Load final data
    final_data_path = "C:/temp/airflow/loaded/final_data.csv"
    if os.path.exists(final_data_path):
        try:
            with open(final_data_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
            if rows:
                print(f"📊 Total records in final dataset: {len(rows)}")
                print("\n🔍 Sample records (first 3):")
                print("-" * 40)
                
                for i, row in enumerate(rows[:3]):
                    print(f"\nRecord {i+1}:")
                    for key, value in row.items():
                        if len(str(value)) > 50:
                            value = str(value)[:47] + "..."
                        print(f"  {key}: {value}")
            else:
                print("❌ No data found in final dataset")
                
        except Exception as e:
            print(f"❌ Error reading final data: {str(e)}")
    else:
        print("❌ Final data file not found")

def display_quality_report():
    """Display detailed quality report"""
    print("\n🔍 DATA QUALITY REPORT")
    print("=" * 60)
    
    # Load quality metrics
    quality_path = "C:/temp/airflow/monitoring/quality_metrics.json"
    if os.path.exists(quality_path):
        quality_data = load_json_file(quality_path)
        if isinstance(quality_data, dict):
            print(f"📅 Check Timestamp: {quality_data.get('check_timestamp', 'Unknown')}")
            print(f"🌡️ Data Freshness: {quality_data.get('data_freshness', 'Unknown')}")
            print(f"📊 Completeness Score: {quality_data.get('completeness_score', 0)*100:.1f}%")
            print(f"🎯 Accuracy Score: {quality_data.get('accuracy_score', 0)*100:.1f}%")
            print(f"🔄 Consistency Score: {quality_data.get('consistency_score', 0)*100:.1f}%")
            print(f"⚡ Timeliness Score: {quality_data.get('timeliness_score', 0)*100:.1f}%")
            print(f"🔄 Duplicate Records: {quality_data.get('duplicate_records', 0)}")
            print(f"📝 Null Value Percentage: {quality_data.get('null_value_percentage', 0):.2f}%")
        else:
            print(quality_data)
    else:
        print("❌ Quality report not found")

def display_validation_results():
    """Display validation results"""
    print("\n✅ VALIDATION RESULTS")
    print("=" * 60)
    
    # Load validation results
    validation_path = "C:/temp/airflow/quality_checks/validation_results.json"
    if os.path.exists(validation_path):
        validation_data = load_json_file(validation_path)
        if isinstance(validation_data, dict):
            print(f"📊 API Records Count: {validation_data.get('api_records_count', 0)}")
            print(f"📄 CSV Records Count: {validation_data.get('csv_records_count', 0)}")
            print(f"✅ API Data Valid: {validation_data.get('api_data_valid', False)}")
            print(f"✅ CSV Data Valid: {validation_data.get('csv_data_valid', False)}")
            print(f"✅ API Required Fields: {validation_data.get('api_required_fields', False)}")
            print(f"✅ CSV Required Fields: {validation_data.get('csv_required_fields', False)}")
            print(f"⏰ Validation Timestamp: {validation_data.get('timestamp', 'Unknown')}")
        else:
            print(validation_data)
    else:
        print("❌ Validation results not found")

def display_file_structure():
    """Display the generated file structure"""
    print("\n📁 GENERATED FILE STRUCTURE")
    print("=" * 60)
    
    base_path = "C:/temp/airflow"
    if os.path.exists(base_path):
        for root, dirs, files in os.walk(base_path):
            level = root.replace(base_path, '').count(os.sep)
            indent = ' ' * 2 * level
            print(f"{indent}📁 {os.path.basename(root)}/")
            subindent = ' ' * 2 * (level + 1)
            for file in files:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                size_str = f"({file_size:,} bytes)" if file_size > 0 else "(empty)"
                print(f"{subindent}📄 {file} {size_str}")
    else:
        print("❌ Pipeline output directory not found")

def main():
    """Main function"""
    print("🚀 AIRFLOW PIPELINE RESULTS VIEWER")
    print("=" * 60)
    print("This dashboard shows the results of your data pipeline execution")
    print("=" * 60)
    
    # Display all sections
    display_pipeline_summary()
    display_validation_results()
    display_quality_report()
    display_data_sample()
    display_file_structure()
    
    print("\n" + "=" * 60)
    print("🎉 Pipeline execution completed successfully!")
    print("📁 All files are saved in: C:/temp/airflow")
    print("=" * 60)

if __name__ == "__main__":
    main()
