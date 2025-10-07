#!/usr/bin/env python3
"""
Enhanced Medical Data Pipeline based on SecuTrial Jupyter Notebook
This script incorporates the data extraction and processing logic from the medical notebook
for comprehensive medical data processing capabilities
"""

import json
import os
import logging
import csv
import pandas as pd
import numpy as np
from datetime import datetime
import random
import re
from functools import reduce

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MedicalDataProcessor:
    """Medical data processing based on SecuTrial notebook logic"""
    
    def __init__(self):
        self.filter_times = ['t0', 't1', 't2', 't3', 't4']
        
    def change_format(self, df, key_col, id_col, exact=True):
        """
        Transform data from wide to long format - key function from notebook
        """
        listdf = []
        for k in key_col:
            if exact:
                temp_df = df.filter(regex=k + '$' + '|' + id_col)
                temp_df = temp_df.melt(
                    id_vars=id_col, 
                    value_vars=list(df.filter(regex=k + '$').columns)
                ).sort_values(id_col)
            else:
                temp_df = df.filter(regex=k + '|' + id_col)
                temp_df = temp_df.melt(
                    id_vars=id_col, 
                    value_vars=list(df.filter(regex=k).columns)
                ).sort_values(id_col)

            # Extract visit information
            temp_df['visit'] = temp_df['variable'].str.split("_", n=1, expand=True)[0].str.upper()
            del temp_df["variable"]
            temp_df.rename(columns={"value": k}, inplace=True)
            temp_df.sort_values([id_col, "visit"], inplace=True)
            temp_df.drop_duplicates(subset=[id_col, "visit"], inplace=True)
            listdf.append(temp_df)
            
        return reduce(lambda df1, df2: pd.merge(df1, df2, on=[id_col, "visit"], how='outer'), listdf)
    
    def compute_bmi(self, df, weight_col, height_col, time_point):
        """Compute BMI for given time point"""
        bmi_col = f'{time_point}_bmi'
        if weight_col in df.columns and height_col in df.columns:
            # Convert to numeric, handling potential string values
            weight = pd.to_numeric(df[weight_col], errors='coerce')
            height = pd.to_numeric(df[height_col], errors='coerce')
            
            # BMI calculation: weight(kg) / height(m)^2
            # Assuming height might be in cm, convert to meters
            height_m = np.where(height > 10, height / 100, height)  # Convert cm to m if needed
            df[bmi_col] = weight / (height_m ** 2)
            
            logger.info(f"Computed BMI for {time_point}: {bmi_col}")
        return df
    
    def extract_medical_conditions(self, df, time_point):
        """Extract medical conditions for a time point - based on notebook logic"""
        conditions = {}
        
        # Define condition patterns based on notebook
        condition_patterns = {
            'anamese': ['anamar', 'anamnma', 'anamast', 'anampso', 'anamdm', 'anamms', 'anamre', 'anamca'],
            'AD': ['anamad'],
            'Neurodermitis_Andauernd': ['anamadand'],
            'Psoriasis': ['anampso'],
            'Psoriasis_Andauernd': ['anampsoand'],
            'Allergische_Rhinitis': ['anamar'],
            'Allergische_Rhinitis_Andauernd': ['anamarand'],
            'Food_allergy': ['anamnma'],
            'Food_allergy_Andauernd': ['anamnmaand'],
            'Asthma': ['anamast'],
            'Asthma_Andauernd': ['anamastand'],
            'Diabetes_mellitus': ['anamdm']
        }
        
        for condition, patterns in condition_patterns.items():
            condition_cols = []
            for pattern in patterns:
                matching_cols = [col for col in df.columns if f'{time_point}_fw_{pattern}' in col]
                condition_cols.extend(matching_cols)
            
            if condition_cols:
                # Check for 'ja' or 'yes' values (case insensitive)
                condition_key = f'{time_point}_{condition}'
                df[condition_key] = df[condition_cols].apply(
                    lambda x: x.astype(str).str.contains('ja|yes|1', case=False, na=False)
                ).any(axis=1).astype(int)
                conditions[condition_key] = df[condition_key].sum()
        
        return df, conditions
    
    def process_laboratory_data(self, df, time_points):
        """Process hematology and IgE data - based on notebook logic"""
        # Define laboratory parameters from notebook
        key_hem = ["leuko", "heamglob", "ery", "heamkrit", "lympho", "neutro", "mono", "eos", "baso", "ige_gesamt"]
        key_hem_meanings = {
            'leuko': "leukocytes",
            'heamglob': "hemoglobin", 
            'ery': "erythrocytes",
            'heamkrit': "hematocrit",
            'lympho': "lymphocytes",
            'neutro': "neutrophils",
            'mono': "monocytes",
            'eos': "eosinophils",
            'baso': "basophils",
            'ige_gesamt': "IgE"
        }
        
        # Filter hematology and IgE columns
        haem_cols = []
        for time_point in time_points:
            for param in key_hem:
                matching_cols = [col for col in df.columns if f'{time_point}_fw_{param}' in col]
                haem_cols.extend(matching_cols)
        
        if haem_cols:
            haem_cols.append('aid')  # Add patient ID
            df_haem = df[haem_cols].copy()
            
            # Transform to long format if we have data
            if len(df_haem.columns) > 1:
                try:
                    df_haem_long = self.change_format(df_haem, key_col=key_hem, id_col="aid")
                    df_haem_long.rename(columns=key_hem_meanings, inplace=True)
                    return df_haem_long
                except Exception as e:
                    logger.warning(f"Could not transform hematology data: {e}")
        
        return pd.DataFrame()
    
    def process_severity_scores(self, df, time_points):
        """Process SCORAD, EASI, and DLQI scores - based on notebook logic"""
        score_dfs = []
        
        try:
            # SCORAD scores
            scorad_cols = ['aid']
            for tp in time_points:
                scorad_cols.extend([col for col in df.columns if f'{tp}' in col and 'scorad' in col.lower()])
            
            if len(scorad_cols) > 1:
                df_scorad = self.change_format(df[scorad_cols], key_col=["scorad"], id_col="aid", exact=False)
                score_dfs.append(df_scorad)
            
            # EASI scores  
            easi_cols = ['aid']
            for tp in time_points:
                easi_cols.extend([col for col in df.columns if f'{tp}' in col and 'easi' in col.lower()])
                
            if len(easi_cols) > 1:
                df_easi = self.change_format(df[easi_cols], key_col=["easi"], id_col="aid", exact=False)
                score_dfs.append(df_easi)
            
            # DLQI scores
            dlqi_cols = ['aid']
            for tp in time_points:
                dlqi_cols.extend([col for col in df.columns if f'{tp}' in col and 'dlqi' in col.lower()])
                
            if len(dlqi_cols) > 1:
                df_dlqi = self.change_format(df[dlqi_cols], key_col=["dlqi"], id_col="aid", exact=False)
                score_dfs.append(df_dlqi)
            
            # Merge all scores
            if score_dfs:
                df_scores = reduce(lambda df1, df2: pd.merge(df1, df2, on=['aid', 'visit'], how='outer'), score_dfs)
                return df_scores
                
        except Exception as e:
            logger.warning(f"Could not process severity scores: {e}")
        
        return pd.DataFrame()
    
    def standardize_medical_values(self, df):
        """Standardize medical terminology - based on notebook replacements"""
        replacements = {
            # German to English translations
            "Ja": "Yes",
            "Nein": "No", 
            "ja": "Yes",
            "nein": "No",
            "Weiblich": "Female",
            "Männlich": "Male",
            "Unbekannt": "unknown",
            "UNBEKANNT": "unknown",
            
            # Medical conditions
            "Während Kindheit und Erwachsenenalter": "During childhood and adulthood",
            "Nur im Kindesalter": "Only in childhood",
            "Nur im Erwachsenenalter": "Adulthood only",
            "Ja, aber wann ist unbekannt": "Yes, but when is unknown",
            
            # Treatment-related
            "Ja, vor mehr als 12 Monate": "Yes, more than 12 months ago",
            "Ja, während der letzten 12 Monaten": "Yes, within the last 12 months",
            "Weniger als einmal pro Woche": "Less than once a week",
            "Mehr als 1x täglich": "More than 1 time a day",
            "2-3x wöchentlich": "2-3 times a week",
            "4-6x wöchentlich": "4-6 times a week",
            "1x täglich": "Once per day",
            "1x wöchentlich": "Once per week"
        }
        
        for old, new in replacements.items():
            df = df.replace(old, new, regex=True)
        
        return df

class EnhancedOfflineDataPipeline:
    def __init__(self):
        self.base_path = "C:/temp/airflow"
        self.medical_processor = MedicalDataProcessor()
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories including medical data directories"""
        directories = [
            'extracted', 'transformed', 'loaded', 'quality_checks',
            'monitoring', 'summary', 'input', 'backups', 'archived',
            'medical_data', 'treatment_analysis', 'patient_records',
            'exports', 'exports/clean_data', 'exports/reports',
            'exports/medical_reports', 'laboratory_data', 'severity_scores'
        ]
        
        for directory in directories:
            path = os.path.join(self.base_path, directory)
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
    
    def read_medical_input_data(self):
        """Read medical data with SecuTrial-like structure"""
        try:
            logger.info("Reading medical input data...")
            
            # Try to read input.csv first
            input_file_path = os.path.join(os.path.dirname(__file__), 'input', 'input.csv')
            
            if os.path.exists(input_file_path):
                df = pd.read_csv(input_file_path, encoding='utf-8')
                logger.info(f"Loaded medical data with {len(df)} records and {len(df.columns)} columns")
                
                # Add patient ID if not present
                if 'aid' not in df.columns:
                    df['aid'] = range(1, len(df) + 1)
                
                return df
            else:
                logger.info("No input file found, generating sample medical data...")
                return self.generate_sample_medical_data()
                
        except Exception as e:
            logger.error(f"Failed to read medical input data: {e}")
            return self.generate_sample_medical_data()
    
    def generate_sample_medical_data(self):
        """Generate sample medical data similar to SecuTrial structure"""
        logger.info("Generating sample medical data...")
        
        n_patients = 100
        time_points = ['t0', 't1', 't2', 't3', 't4']
        
        # Generate base patient data
        patients_data = []
        
        for i in range(1, n_patients + 1):
            patient_base = {
                'aid': i,
                'mnpaid': f'MNP{i:06d}',
                'ctrid': random.randint(1, 5),  # Center ID
                't0_auf_gebj_1': random.randint(1950, 2010),  # Birth year
                't0_auf_gebm_1': random.randint(1, 12),  # Birth month
                't0_fw_geschl': random.choice(['Männlich', 'Weiblich']),  # Gender
            }
            
            # Add time-point specific data
            for tp in time_points:
                # Medical history
                patient_base[f'{tp}_fw_anamad'] = random.choice(['ja', 'nein', 'Unbekannt'])
                patient_base[f'{tp}_fw_anamar'] = random.choice(['ja', 'nein'])
                patient_base[f'{tp}_fw_anamnma'] = random.choice(['ja', 'nein'])
                patient_base[f'{tp}_fw_anamast'] = random.choice(['ja', 'nein'])
                patient_base[f'{tp}_fw_anampso'] = random.choice(['ja', 'nein'])
                
                # Physical measurements
                patient_base[f'{tp}_fw_kunt_weight'] = round(random.uniform(50, 120), 1)
                patient_base[f'{tp}_fw_kunt_size'] = round(random.uniform(1.5, 2.0), 2)
                
                # Laboratory values
                patient_base[f'{tp}_fw_haem_leuko'] = round(random.uniform(4, 12), 1)
                patient_base[f'{tp}_fw_haem_heamglob'] = round(random.uniform(12, 18), 1)
                patient_base[f'{tp}_fw_haem_ery'] = round(random.uniform(4.0, 6.0), 2)
                patient_base[f'{tp}_fw_ige_gesamt'] = round(random.uniform(10, 1000), 1)
                
                # Severity scores
                patient_base[f'{tp}_fw_scorad_tot'] = round(random.uniform(0, 100), 1)
                patient_base[f'{tp}_fw_easi_tot'] = round(random.uniform(0, 72), 1)
                patient_base[f'{tp}_fw_dlqi_tot'] = round(random.uniform(0, 30), 1)
                
                # Visit dates
                patient_base[f'{tp}_fw_untersuchdat'] = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            
            patients_data.append(patient_base)
        
        df = pd.DataFrame(patients_data)
        
        # Save sample data
        output_path = os.path.join(self.base_path, 'extracted', 'medical_input_data.csv')
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Generated {len(df)} sample medical records")
        return df
    
    def process_medical_data(self, df):
        """Process medical data using SecuTrial notebook logic"""
        try:
            logger.info("Processing medical data with SecuTrial logic...")
            
            processed_data = {}
            time_points = self.medical_processor.filter_times
            
            # 1. Compute BMI for each time point
            for tp in time_points:
                weight_col = f'{tp}_fw_kunt_weight'
                height_col = f'{tp}_fw_kunt_size'
                if weight_col in df.columns and height_col in df.columns:
                    df = self.medical_processor.compute_bmi(df, weight_col, height_col, tp)
            
            # 2. Extract medical conditions
            conditions_summary = {}
            for tp in time_points:
                df, conditions = self.medical_processor.extract_medical_conditions(df, tp)
                conditions_summary[tp] = conditions
            
            # 3. Process laboratory data
            df_lab = self.medical_processor.process_laboratory_data(df, time_points)
            if not df_lab.empty:
                lab_path = os.path.join(self.base_path, 'laboratory_data', 'hematology_ige_data.csv')
                df_lab.to_csv(lab_path, index=False)
                processed_data['laboratory_records'] = len(df_lab)
            
            # 4. Process severity scores
            df_scores = self.medical_processor.process_severity_scores(df, time_points)
            if not df_scores.empty:
                scores_path = os.path.join(self.base_path, 'severity_scores', 'clinical_scores.csv')
                df_scores.to_csv(scores_path, index=False)
                processed_data['severity_records'] = len(df_scores)
            
            # 5. Standardize values
            df = self.medical_processor.standardize_medical_values(df)
            
            # 6. Save processed main dataset
            processed_path = os.path.join(self.base_path, 'transformed', 'processed_medical_data.csv')
            df.to_csv(processed_path, index=False, encoding='utf-8')
            
            # Generate summary
            processed_data.update({
                'total_patients': len(df),
                'time_points_processed': len(time_points),
                'conditions_summary': conditions_summary,
                'columns_processed': len(df.columns),
                'processing_timestamp': datetime.now().isoformat()
            })
            
            logger.info(f"Medical data processing completed: {processed_data['total_patients']} patients")
            return processed_data, df
            
        except Exception as e:
            logger.error(f"Medical data processing failed: {e}")
            raise
    
    def generate_medical_quality_report(self, df, processed_data):
        """Generate comprehensive medical data quality report"""
        try:
            logger.info("Generating medical quality report...")
            
            # Calculate quality metrics specific to medical data
            quality_metrics = {
                'data_completeness': {},
                'value_ranges': {},
                'patient_demographics': {},
                'clinical_scores': {},
                'laboratory_values': {}
            }
            
            # Demographics analysis
            if 't0_fw_geschl' in df.columns:
                gender_dist = df['t0_fw_geschl'].value_counts().to_dict()
                quality_metrics['patient_demographics']['gender_distribution'] = gender_dist
            
            # BMI analysis
            bmi_cols = [col for col in df.columns if '_bmi' in col]
            if bmi_cols:
                bmi_stats = {}
                for col in bmi_cols:
                    bmi_values = pd.to_numeric(df[col], errors='coerce').dropna()
                    if len(bmi_values) > 0:
                        bmi_stats[col] = {
                            'mean': round(bmi_values.mean(), 2),
                            'std': round(bmi_values.std(), 2),
                            'min': round(bmi_values.min(), 2),
                            'max': round(bmi_values.max(), 2),
                            'count': len(bmi_values)
                        }
                quality_metrics['clinical_scores']['bmi_statistics'] = bmi_stats
            
            # Laboratory values analysis
            lab_cols = [col for col in df.columns if any(lab in col for lab in ['leuko', 'heamglob', 'ery', 'ige'])]
            if lab_cols:
                lab_stats = {}
                for col in lab_cols:
                    lab_values = pd.to_numeric(df[col], errors='coerce').dropna()
                    if len(lab_values) > 0:
                        lab_stats[col] = {
                            'mean': round(lab_values.mean(), 2),
                            'median': round(lab_values.median(), 2),
                            'count': len(lab_values),
                            'missing': len(df) - len(lab_values)
                        }
                quality_metrics['laboratory_values'] = lab_stats
            
            # Medical conditions prevalence
            condition_cols = [col for col in df.columns if any(cond in col for cond in ['AD', 'Asthma', 'Rhinitis', 'allergy'])]
            if condition_cols:
                conditions_prev = {}
                for col in condition_cols:
                    if df[col].dtype in ['int64', 'float64']:
                        prevalence = (df[col] == 1).sum()
                        conditions_prev[col] = {
                            'prevalence_count': int(prevalence),
                            'prevalence_rate': round(prevalence / len(df) * 100, 1)
                        }
                quality_metrics['patient_demographics']['conditions_prevalence'] = conditions_prev
            
            # Overall quality score
            total_cells = len(df) * len(df.columns)
            non_null_cells = total_cells - df.isnull().sum().sum()
            completeness_rate = non_null_cells / total_cells
            
            quality_metrics['overall'] = {
                'completeness_rate': round(completeness_rate, 3),
                'total_patients': len(df),
                'total_columns': len(df.columns),
                'processing_success': True,
                'report_generated': datetime.now().isoformat()
            }
            
            # Save quality report
            report_path = os.path.join(self.base_path, 'exports', 'medical_reports', 'medical_quality_report.json')
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(quality_metrics, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Medical quality report generated: {completeness_rate*100:.1f}% data completeness")
            return quality_metrics
            
        except Exception as e:
            logger.error(f"Failed to generate medical quality report: {e}")
            return {}
    
    def run_enhanced_medical_pipeline(self):
        """Run the complete enhanced medical data pipeline"""
        try:
            logger.info("🏥 Starting Enhanced Medical Data Pipeline...")
            
            # Step 1: Read medical data
            df_medical = self.read_medical_input_data()
            
            # Step 2: Process medical data with SecuTrial logic
            processed_data, df_processed = self.process_medical_data(df_medical)
            
            # Step 3: Generate medical quality report
            quality_report = self.generate_medical_quality_report(df_processed, processed_data)
            
            # Step 4: Save final medical dataset
            final_path = os.path.join(self.base_path, 'loaded', 'final_medical_data.csv')
            df_processed.to_csv(final_path, index=False, encoding='utf-8')
            
            # Step 5: Create comprehensive summary
            pipeline_summary = {
                'pipeline_type': 'enhanced_medical',
                'status': 'SUCCESS',
                'execution_time': datetime.now().isoformat(),
                'processed_data': processed_data,
                'quality_metrics': quality_report,
                'output_files': {
                    'final_dataset': final_path,
                    'laboratory_data': os.path.join(self.base_path, 'laboratory_data', 'hematology_ige_data.csv'),
                    'severity_scores': os.path.join(self.base_path, 'severity_scores', 'clinical_scores.csv'),
                    'quality_report': os.path.join(self.base_path, 'exports', 'medical_reports', 'medical_quality_report.json')
                }
            }
            
            # Save pipeline summary
            summary_path = os.path.join(self.base_path, 'summary', 'medical_pipeline_summary.json')
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(pipeline_summary, f, indent=2, ensure_ascii=False)
            
            logger.info("✅ Enhanced Medical Pipeline completed successfully!")
            logger.info(f"📊 Processed {processed_data['total_patients']} patients")
            logger.info(f"🧬 Laboratory records: {processed_data.get('laboratory_records', 0)}")
            logger.info(f"📈 Severity score records: {processed_data.get('severity_records', 0)}")
            logger.info(f"🎯 Data completeness: {quality_report.get('overall', {}).get('completeness_rate', 0)*100:.1f}%")
            
            return pipeline_summary
            
        except Exception as e:
            logger.error(f"❌ Enhanced Medical Pipeline failed: {e}")
            raise

def main():
    """Main function to run the enhanced medical pipeline"""
    print("🏥 Enhanced Medical Data Pipeline")
    print("=" * 60)
    print("Based on SecuTrial Jupyter Notebook Processing Logic")
    print("Includes: BMI calculation, medical conditions, lab values, severity scores")
    print("=" * 60)
    
    # Create pipeline instance
    pipeline = EnhancedOfflineDataPipeline()
    
    try:
        # Run the enhanced medical pipeline
        result = pipeline.run_enhanced_medical_pipeline()
        
        print("\n🎉 Enhanced Medical Pipeline completed successfully!")
        print(f"📁 Results saved in: {pipeline.base_path}")
        print(f"📊 Patients processed: {result['processed_data']['total_patients']}")
        print(f"⏱️ Time points: {result['processed_data']['time_points_processed']}")
        print(f"🧬 Laboratory records: {result['processed_data'].get('laboratory_records', 0)}")
        print(f"📈 Clinical scores: {result['processed_data'].get('severity_records', 0)}")
        
        # Show key output files
        print("\n📂 Key Output Files:")
        for file_type, file_path in result['output_files'].items():
            if os.path.exists(file_path):
                print(f"   • {file_type}: {file_path}")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())