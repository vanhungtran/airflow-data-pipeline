#!/usr/bin/env python3
"""
PRORAD Study Pipeline Executor
Simple execution script for the PRORAD data processing pipeline
"""

import os
import sys
import subprocess
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_environment():
    """Check if required packages are available"""
    try:
        import pandas as pd
        import numpy as np
        logger.info("✅ Required packages available")
        return True
    except ImportError as e:
        logger.error(f"❌ Missing required package: {e}")
        return False

def install_requirements():
    """Install required packages if missing"""
    logger.info("📦 Installing required packages...")
    
    requirements = [
        'pandas>=1.3.0',
        'numpy>=1.20.0',
        'openpyxl>=3.0.0'
    ]
    
    for package in requirements:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            logger.info(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to install {package}: {e}")
            return False
    
    return True

def run_prorad_pipeline():
    """Execute the PRORAD processing pipeline"""
    logger.info("🚀 Starting PRORAD Study Data Processing Pipeline...")
    
    try:
        # Import and run the pipeline
        from prorad_pipeline import PRORADProcessor
        
        logger.info("📊 Initializing PRORAD processor...")
        processor = PRORADProcessor()
        
        logger.info("🔄 Processing complete dataset...")
        processed_df = processor.process_full_dataset()
        
        logger.info(f"✅ Pipeline completed successfully!")
        logger.info(f"📈 Processed dataset: {len(processed_df)} rows × {len(processed_df.columns)} columns")
        logger.info(f"📁 Output location: C:/temp/airflow/prorad_processed/")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        return False

def main():
    """Main execution function"""
    logger.info("🏥 PRORAD Study Pipeline Executor")
    logger.info("=" * 50)
    
    # Check environment
    if not check_environment():
        logger.info("🔧 Installing missing packages...")
        if not install_requirements():
            logger.error("❌ Failed to install required packages")
            return False
        
        # Re-check environment
        if not check_environment():
            logger.error("❌ Environment setup failed")
            return False
    
    # Run the pipeline
    success = run_prorad_pipeline()
    
    if success:
        logger.info("🎉 PRORAD pipeline execution completed successfully!")
        logger.info("📋 Check the processing reports in C:/temp/airflow/prorad_processed/reports/")
    else:
        logger.error("💥 PRORAD pipeline execution failed!")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)