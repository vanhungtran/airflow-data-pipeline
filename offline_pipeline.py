#!/usr/bin/env python3
"""
Offline Data Pipeline (No Network Required)
This script demonstrates the pipeline logic using only local operations
"""

import json
import os
import logging
import csv
from datetime import datetime
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OfflineDataPipeline:
    def __init__(self):
        self.base_path = "C:/temp/airflow"
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            'extracted', 'transformed', 'loaded', 'quality_checks',
            'monitoring', 'summary', 'input', 'backups', 'archived'
        ]
        
        for directory in directories:
            path = os.path.join(self.base_path, directory)
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
    
    def generate_sample_api_data(self):
        """Generate sample API-like data"""
        try:
            logger.info("Generating sample API data...")
            
            # Generate sample data similar to what an API would return
            sample_api_data = []
            for i in range(1, 51):  # 50 records
                sample_api_data.append({
                    'id': i,
                    'title': f'Sample Post {i}',
                    'body': f'This is the body content for post number {i}. It contains some sample text.',
                    'userId': random.randint(1, 10),
                    'category': random.choice(['tech', 'business', 'lifestyle', 'news']),
                    'created_at': datetime.now().isoformat()
                })
            
            # Save to file
            output_path = os.path.join(self.base_path, 'extracted', 'api_data.json')
            with open(output_path, 'w') as f:
                json.dump(sample_api_data, f, indent=2)
            
            logger.info(f"Successfully generated {len(sample_api_data)} API-like records")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to generate API data: {str(e)}")
            raise
    
    def create_sample_csv_data(self):
        """Create sample CSV data"""
        try:
            logger.info("Creating sample CSV data...")
            
            # Create sample data
            sample_data = []
            for i in range(1, 101):
                sample_data.append({
                    'id': i,
                    'name': f'User_{i}',
                    'email': f'user{i}@example.com',
                    'department': random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales']),
                    'salary': random.randint(30000, 120000),
                    'created_at': datetime.now().isoformat()
                })
            
            # Save as CSV
            output_path = os.path.join(self.base_path, 'extracted', 'csv_data.csv')
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                if sample_data:
                    writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
                    writer.writeheader()
                    writer.writerows(sample_data)
            
            logger.info(f"Successfully created CSV with {len(sample_data)} records")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to create CSV data: {str(e)}")
            raise
    
    def load_csv_data(self, csv_path):
        """Load CSV data into memory"""
        data = []
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
            return data
        except Exception as e:
            logger.error(f"Failed to load CSV data: {str(e)}")
            raise
    
    def validate_data(self, api_path, csv_path):
        """Validate extracted data quality"""
        try:
            logger.info("Starting data validation...")
            
            # Load and validate API data
            with open(api_path, 'r') as f:
                api_data = json.load(f)
            
            csv_data = self.load_csv_data(csv_path)
            
            # Data quality checks
            validation_results = {
                'api_records_count': len(api_data),
                'csv_records_count': len(csv_data),
                'api_data_valid': len(api_data) > 0,
                'csv_data_valid': len(csv_data) > 0,
                'api_required_fields': all('id' in item for item in api_data),
                'csv_required_fields': all('id' in item for item in csv_data),
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Data validation results: {validation_results}")
            
            # Save validation results
            validation_path = os.path.join(self.base_path, 'quality_checks', 'validation_results.json')
            with open(validation_path, 'w') as f:
                json.dump(validation_results, f, indent=2)
            
            if not validation_results['api_data_valid'] or not validation_results['csv_data_valid']:
                raise ValueError("Data validation failed")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            raise
    
    def transform_data(self, api_path, csv_path):
        """Transform and clean the data"""
        try:
            logger.info("Starting data transformation...")
            
            # Load data
            with open(api_path, 'r') as f:
                api_data = json.load(f)
            
            csv_data = self.load_csv_data(csv_path)
            
            # Transform API data
            for item in api_data:
                item['source'] = 'api'
                item['processed_at'] = datetime.now().isoformat()
                # Add some data enrichment
                item['data_quality_score'] = random.uniform(0.8, 1.0)
            
            # Transform CSV data
            for item in csv_data:
                item['source'] = 'csv'
                item['processed_at'] = datetime.now().isoformat()
                # Add some data enrichment
                item['data_quality_score'] = random.uniform(0.85, 1.0)
            
            # Combine datasets
            combined_data = api_data + csv_data
            
            # Add record IDs and additional metadata
            for i, item in enumerate(combined_data):
                item['record_id'] = i + 1
                item['pipeline_version'] = '1.0'
                item['batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save transformed data
            output_path = os.path.join(self.base_path, 'transformed', 'combined_data.json')
            with open(output_path, 'w') as f:
                json.dump(combined_data, f, indent=2)
            
            logger.info(f"Successfully transformed {len(combined_data)} records")
            return output_path
            
        except Exception as e:
            logger.error(f"Data transformation failed: {str(e)}")
            raise
    
    def load_data(self, transformed_path):
        """Load transformed data to final destination"""
        try:
            logger.info("Starting data loading...")
            
            # Load transformed data
            with open(transformed_path, 'r') as f:
                transformed_data = json.load(f)
            
            # Save as CSV for final output
            final_output_path = os.path.join(self.base_path, 'loaded', 'final_data.csv')
            
            if transformed_data:
                # Get all unique fieldnames from all records
                all_fieldnames = set()
                for record in transformed_data:
                    all_fieldnames.update(record.keys())
                all_fieldnames = sorted(list(all_fieldnames))
                
                with open(final_output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=all_fieldnames)
                    writer.writeheader()
                    writer.writerows(transformed_data)
            
            # Count records by source and calculate statistics
            api_count = sum(1 for item in transformed_data if item.get('source') == 'api')
            csv_count = sum(1 for item in transformed_data if item.get('source') == 'csv')
            
            # Calculate average quality score
            quality_scores = [float(item.get('data_quality_score', 0)) for item in transformed_data]
            avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
            
            # Create summary statistics
            summary = {
                'total_records': len(transformed_data),
                'api_records': api_count,
                'csv_records': csv_count,
                'average_quality_score': round(avg_quality_score, 3),
                'load_timestamp': datetime.now().isoformat(),
                'status': 'success',
                'pipeline_version': '1.0'
            }
            
            # Save summary
            summary_path = os.path.join(self.base_path, 'summary', 'load_summary.json')
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"Successfully loaded {len(transformed_data)} records. Summary: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            raise
    
    def quality_monitoring(self):
        """Perform data quality monitoring"""
        try:
            logger.info("Starting quality monitoring...")
            
            # Simulate quality metrics based on current data
            quality_metrics = {
                'data_freshness': 'excellent',
                'completeness_score': round(random.uniform(0.92, 0.99), 3),
                'accuracy_score': round(random.uniform(0.95, 0.99), 3),
                'consistency_score': round(random.uniform(0.90, 0.98), 3),
                'timeliness_score': round(random.uniform(0.95, 1.0), 3),
                'duplicate_records': random.randint(0, 5),
                'null_value_percentage': round(random.uniform(0.0, 2.0), 2),
                'check_timestamp': datetime.now().isoformat()
            }
            
            # Save quality metrics
            quality_path = os.path.join(self.base_path, 'monitoring', 'quality_metrics.json')
            with open(quality_path, 'w') as f:
                json.dump(quality_metrics, f, indent=2)
            
            logger.info(f"Data quality monitoring completed: {quality_metrics}")
            return quality_metrics
            
        except Exception as e:
            logger.error(f"Data quality monitoring failed: {str(e)}")
            raise
    
    def create_backup(self):
        """Create a backup of processed data"""
        try:
            logger.info("Creating data backup...")
            
            backup_dir = os.path.join(self.base_path, 'backups', f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            os.makedirs(backup_dir, exist_ok=True)
            
            # Copy final data to backup
            import shutil
            source_file = os.path.join(self.base_path, 'loaded', 'final_data.csv')
            if os.path.exists(source_file):
                shutil.copy2(source_file, backup_dir)
                logger.info(f"Backup created at: {backup_dir}")
            
            return backup_dir
            
        except Exception as e:
            logger.error(f"Backup creation failed: {str(e)}")
            # Don't fail the pipeline for backup issues
            return None
    
    def run_pipeline(self):
        """Run the complete data pipeline"""
        try:
            logger.info("🚀 Starting Offline Data Pipeline...")
            
            # Step 1: Extract data
            api_path = self.generate_sample_api_data()
            csv_path = self.create_sample_csv_data()
            
            # Step 2: Validate data
            validation_results = self.validate_data(api_path, csv_path)
            
            # Step 3: Transform data
            transformed_path = self.transform_data(api_path, csv_path)
            
            # Step 4: Load data
            load_summary = self.load_data(transformed_path)
            
            # Step 5: Quality monitoring
            quality_metrics = self.quality_monitoring()
            
            # Step 6: Create backup
            backup_path = self.create_backup()
            
            # Final summary
            final_summary = {
                'pipeline_status': 'SUCCESS',
                'execution_time': datetime.now().isoformat(),
                'validation_results': validation_results,
                'load_summary': load_summary,
                'quality_metrics': quality_metrics,
                'backup_location': backup_path,
                'pipeline_type': 'offline_demo'
            }
            
            # Save final summary
            final_path = os.path.join(self.base_path, 'summary', 'pipeline_summary.json')
            with open(final_path, 'w') as f:
                json.dump(final_summary, f, indent=2)
            
            logger.info("✅ Pipeline completed successfully!")
            logger.info(f"📊 Total records processed: {load_summary['total_records']}")
            logger.info(f"📈 Average Quality Score: {load_summary['average_quality_score']*100:.1f}%")
            logger.info(f"🔗 API Records: {load_summary['api_records']}")
            logger.info(f"📄 CSV Records: {load_summary['csv_records']}")
            
            return final_summary
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise

def main():
    """Main function to run the pipeline"""
    print("🎉 Welcome to the Offline Data Pipeline!")
    print("=" * 50)
    print("This pipeline demonstrates data processing without network dependencies")
    print("=" * 50)
    
    # Create pipeline instance
    pipeline = OfflineDataPipeline()
    
    # Run the pipeline
    try:
        result = pipeline.run_pipeline()
        print("\n🎉 Pipeline completed successfully!")
        print(f"📁 Results saved in: {pipeline.base_path}")
        print(f"📊 Processed {result['load_summary']['total_records']} records")
        print(f"📈 Average Quality Score: {result['load_summary']['average_quality_score']*100:.1f}%")
        print(f"🔗 API Records: {result['load_summary']['api_records']}")
        print(f"📄 CSV Records: {result['load_summary']['csv_records']}")
        print(f"💾 Backup created at: {result['backup_location']}")
        
        # Show file locations
        print("\n📂 Generated Files:")
        print(f"   • Final Data: {pipeline.base_path}/loaded/final_data.csv")
        print(f"   • Summary: {pipeline.base_path}/summary/pipeline_summary.json")
        print(f"   • Quality Report: {pipeline.base_path}/monitoring/quality_metrics.json")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
