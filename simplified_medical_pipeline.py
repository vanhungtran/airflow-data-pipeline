#!/usr/bin/env python3
"""
Simplified Medical Data Pipeline (No External Dependencies)
Based on SecuTrial preprocessing logic but using only standard library
"""

import json
import os
import logging
import csv
from datetime import datetime, timedelta
import random
import math

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimplifiedMedicalPipeline:
    def __init__(self):
        self.base_path = "C:/temp/airflow"
        self.setup_directories()
        self.filter_times = ['t0', 't1', 't2', 't3', 't4']
        
    def setup_directories(self):
        """Create necessary directories for medical data processing"""
        directories = [
            'extracted', 'transformed', 'loaded', 'quality_checks',
            'monitoring', 'summary', 'input', 'backups', 'archived',
            'medical_data', 'treatment_analysis', 'patient_records',
            'exports', 'exports/clean_medical_data', 'exports/medical_reports'
        ]
        
        for directory in directories:
            path = os.path.join(self.base_path, directory)
            os.makedirs(path, exist_ok=True)
            logger.info(f"Created directory: {path}")
    
    def generate_medical_sample_data(self):
        """Generate sample medical data based on SecuTrial structure"""
        try:
            logger.info("Generating sample medical data based on SecuTrial structure...")
            
            # Generate patient records
            n_patients = 50
            patient_data = []
            
            # Medical conditions from SecuTrial notebook
            medical_conditions = ['anamad', 'anamar', 'anamnma', 'anamast', 'anampso', 'anamdm']
            medications = ['Tacrolimus', 'Dupilumab', 'Methotrexat', 'Cyclosporin', 'Kortison', 'Protopic', 'Elidel']
            therapy_types = ['Basic_therapy', 'UV_therapy', 'Topical_corticoids', 'Immunosuppressants']
            
            for i in range(1, n_patients + 1):
                patient_id = f"PAT{i:03d}"
                
                # Basic patient info
                birth_year = random.randint(1940, 2010)
                birth_month = random.randint(1, 12)
                
                base_record = {
                    'aid': patient_id,
                    'mnpaid': f"MNP{i:06d}",
                    'patient_id': patient_id,
                    
                    # Demographics
                    'birth_year': birth_year,
                    'birth_month': birth_month,
                    'gender': random.choice(['Male', 'Female']),
                    
                    # Medical history
                    'ad_self_declared': random.choice(['Yes', 'No', 'Unknown']),
                    'ad_onset_year': random.randint(birth_year + 5, 2024),
                    'ad_onset_month': random.randint(1, 12),
                    
                    # Physical measurements
                    'baseline_weight': round(random.uniform(45, 120), 1),
                    'baseline_height': round(random.uniform(150, 200), 1),
                    
                    # Disease severity
                    'scorad_v_total': round(random.uniform(0, 52), 1),
                    'scorad_h_total': round(random.uniform(0, 51), 1),
                    'days_missing_work': random.randint(0, 30),
                }
                
                # Calculate BMI
                height_m = base_record['baseline_height'] / 100
                base_record['baseline_bmi'] = round(base_record['baseline_weight'] / (height_m ** 2), 1)
                
                # Calculate affected body surface area
                base_record['affected_body_surface_area'] = round(
                    100 * (base_record['scorad_v_total'] + base_record['scorad_h_total']) / 104, 1
                )
                
                # Calculate age at baseline
                base_record['age_at_baseline'] = 2024 - birth_year
                
                # Calculate age at AD onset
                base_record['age_at_ad_onset'] = base_record['ad_onset_year'] - birth_year
                
                # Add time-series data for multiple visits
                for j, time_point in enumerate(self.filter_times):
                    # Visit date (spaced roughly 3 months apart)
                    base_date = datetime(2024, 1, 1) + timedelta(days=j*90 + random.randint(-15, 15))
                    base_record[f'{time_point}_visit_date'] = base_date.strftime('%Y-%m-%d')
                    
                    # Weight variation over time
                    weight_change = random.uniform(-5, 5)
                    base_record[f'{time_point}_weight'] = round(base_record['baseline_weight'] + weight_change, 1)
                    base_record[f'{time_point}_bmi'] = round(base_record[f'{time_point}_weight'] / (height_m ** 2), 1)
                    
                    # Disease scores (may improve or worsen over time)
                    score_change = random.uniform(-10, 5)  # Generally improving trend
                    base_record[f'{time_point}_scorad_total'] = max(0, round(base_record['scorad_v_total'] + score_change, 1))
                    
                    # Treatment data (up to 5 medications per visit for simplicity)
                    for med_num in range(1, 6):
                        # Treatment start and end dates
                        days_before_visit = random.randint(0, 60)
                        treatment_duration = random.randint(7, 120)
                        
                        med_start = base_date - timedelta(days=days_before_visit)
                        med_end = med_start + timedelta(days=treatment_duration)
                        
                        base_record[f'{time_point}_med_{med_num}_start'] = med_start.strftime('%Y-%m-%d')
                        base_record[f'{time_point}_med_{med_num}_end'] = med_end.strftime('%Y-%m-%d')
                        base_record[f'{time_point}_med_{med_num}_name'] = random.choice(medications)
                        base_record[f'{time_point}_med_{med_num}_type'] = random.choice(['Cream', 'Tablet', 'Injection', 'Ointment'])
                        
                        # Calculate treatment metrics
                        base_record[f'{time_point}_med_{med_num}_duration_days'] = treatment_duration
                        base_record[f'{time_point}_med_{med_num}_visit_to_start_days'] = days_before_visit
                        base_record[f'{time_point}_med_{med_num}_end_to_visit_days'] = (med_end - base_date).days
                    
                    # Therapy flags
                    for therapy in therapy_types:
                        base_record[f'{time_point}_{therapy.lower()}'] = random.choice(['Yes', 'No'])
                
                patient_data.append(base_record)
            
            # Save extracted data
            output_path = os.path.join(self.base_path, 'medical_data', 'sample_medical_data.json')
            with open(output_path, 'w') as f:
                json.dump(patient_data, f, indent=2)
            
            logger.info(f"Successfully generated {len(patient_data)} patient records")
            return patient_data
            
        except Exception as e:
            logger.error(f"Error generating medical data: {e}")
            raise
    
    def analyze_treatment_patterns(self, patient_data):
        """Analyze treatment patterns and effectiveness"""
        try:
            logger.info("Analyzing treatment patterns...")
            
            treatment_analysis = {
                'total_patients': len(patient_data),
                'treatment_summary': {},
                'medication_usage': {},
                'therapy_effectiveness': {},
                'patient_outcomes': []
            }
            
            # Analyze each patient
            for patient in patient_data:
                patient_outcome = {
                    'patient_id': patient['aid'],
                    'baseline_severity': patient['scorad_v_total'] + patient['scorad_h_total'],
                    'final_severity': patient.get('t4_scorad_total', patient['scorad_v_total']),
                    'improvement': 0,
                    'total_medications': 0,
                    'treatment_duration_avg': 0
                }
                
                # Calculate improvement
                patient_outcome['improvement'] = patient_outcome['baseline_severity'] - patient_outcome['final_severity']
                patient_outcome['improvement_percentage'] = round(
                    (patient_outcome['improvement'] / max(patient_outcome['baseline_severity'], 1)) * 100, 1
                )
                
                # Count medications and calculate average duration
                medication_durations = []
                medication_count = 0
                
                for time_point in self.filter_times:
                    for med_num in range(1, 6):
                        duration_key = f'{time_point}_med_{med_num}_duration_days'
                        med_name_key = f'{time_point}_med_{med_num}_name'
                        
                        if duration_key in patient and med_name_key in patient:
                            medication_count += 1
                            medication_durations.append(patient[duration_key])
                            
                            # Track medication usage
                            med_name = patient[med_name_key]
                            if med_name not in treatment_analysis['medication_usage']:
                                treatment_analysis['medication_usage'][med_name] = 0
                            treatment_analysis['medication_usage'][med_name] += 1
                
                patient_outcome['total_medications'] = medication_count
                patient_outcome['treatment_duration_avg'] = round(
                    sum(medication_durations) / len(medication_durations) if medication_durations else 0, 1
                )
                
                treatment_analysis['patient_outcomes'].append(patient_outcome)
            
            # Calculate overall statistics
            improvements = [p['improvement'] for p in treatment_analysis['patient_outcomes']]
            improvement_percentages = [p['improvement_percentage'] for p in treatment_analysis['patient_outcomes']]
            
            treatment_analysis['treatment_summary'] = {
                'patients_improved': len([i for i in improvements if i > 0]),
                'patients_worsened': len([i for i in improvements if i < 0]),
                'patients_stable': len([i for i in improvements if i == 0]),
                'average_improvement': round(sum(improvements) / len(improvements), 1),
                'average_improvement_percentage': round(sum(improvement_percentages) / len(improvement_percentages), 1),
                'most_effective_medication': max(treatment_analysis['medication_usage'], 
                                               key=treatment_analysis['medication_usage'].get),
                'total_medication_prescriptions': sum(treatment_analysis['medication_usage'].values())
            }
            
            # Save analysis
            analysis_path = os.path.join(self.base_path, 'treatment_analysis', 'treatment_patterns.json')
            with open(analysis_path, 'w') as f:
                json.dump(treatment_analysis, f, indent=2)
            
            logger.info("Treatment analysis completed successfully")
            return treatment_analysis
            
        except Exception as e:
            logger.error(f"Error in treatment analysis: {e}")
            raise
    
    def validate_medical_data_quality(self, patient_data):
        """Validate medical data quality"""
        try:
            logger.info("Validating medical data quality...")
            
            validation_results = {
                'total_patients': len(patient_data),
                'data_quality_checks': {},
                'quality_score': 0,
                'validation_passed': False,
                'timestamp': datetime.now().isoformat()
            }
            
            # Check various quality metrics
            missing_patient_ids = len([p for p in patient_data if not p.get('aid')])
            invalid_birth_years = len([p for p in patient_data if p.get('birth_year', 2000) < 1900 or p.get('birth_year', 2000) > 2024])
            invalid_bmis = len([p for p in patient_data if p.get('baseline_bmi', 20) < 10 or p.get('baseline_bmi', 20) > 60])
            missing_visit_dates = len([p for p in patient_data if not p.get('t0_visit_date')])
            invalid_scorad_scores = len([p for p in patient_data if p.get('scorad_v_total', 0) > 52.5 or p.get('scorad_h_total', 0) > 51.5])
            
            validation_results['data_quality_checks'] = {
                'missing_patient_ids': missing_patient_ids,
                'invalid_birth_years': invalid_birth_years,
                'invalid_bmis': invalid_bmis,
                'missing_visit_dates': missing_visit_dates,
                'invalid_scorad_scores': invalid_scorad_scores
            }
            
            # Calculate quality score
            total_checks = len(patient_data) * 5  # 5 quality checks per patient
            total_issues = sum(validation_results['data_quality_checks'].values())
            validation_results['quality_score'] = max(0, ((total_checks - total_issues) / total_checks) * 100)
            validation_results['validation_passed'] = validation_results['quality_score'] > 85
            
            # Save validation results
            validation_path = os.path.join(self.base_path, 'quality_checks', 'medical_data_validation.json')
            with open(validation_path, 'w') as f:
                json.dump(validation_results, f, indent=2)
            
            logger.info(f"Data validation completed. Quality score: {validation_results['quality_score']:.1f}%")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error in data validation: {e}")
            raise
    
    def create_medical_summary(self, patient_data, treatment_analysis, validation_results):
        """Create comprehensive medical data summary"""
        try:
            logger.info("Creating medical data summary...")
            
            summary = {
                'pipeline_info': {
                    'pipeline_type': 'simplified_medical_data_processing',
                    'data_source': 'SecuTrial_inspired_sample_data',
                    'processing_date': datetime.now().isoformat(),
                    'time_points': self.filter_times,
                    'patients_processed': len(patient_data)
                },
                'patient_demographics': {
                    'total_patients': len(patient_data),
                    'gender_distribution': {},
                    'age_statistics': {},
                    'disease_severity': {}
                },
                'treatment_effectiveness': treatment_analysis['treatment_summary'],
                'medication_usage': treatment_analysis['medication_usage'],
                'data_quality': validation_results,
                'clinical_insights': {
                    'patients_showing_improvement': treatment_analysis['treatment_summary']['patients_improved'],
                    'improvement_rate': round((treatment_analysis['treatment_summary']['patients_improved'] / len(patient_data)) * 100, 1),
                    'most_prescribed_medication': treatment_analysis['treatment_summary']['most_effective_medication'],
                    'average_treatment_duration': round(
                        sum([p['treatment_duration_avg'] for p in treatment_analysis['patient_outcomes']]) / len(patient_data), 1
                    )
                },
                'file_outputs': {
                    'medical_data': f"{self.base_path}/medical_data/sample_medical_data.json",
                    'treatment_analysis': f"{self.base_path}/treatment_analysis/treatment_patterns.json",
                    'validation_report': f"{self.base_path}/quality_checks/medical_data_validation.json",
                    'final_summary': f"{self.base_path}/summary/simplified_medical_summary.json"
                }
            }
            
            # Calculate demographic statistics
            genders = [p.get('gender', 'Unknown') for p in patient_data]
            summary['patient_demographics']['gender_distribution'] = {
                'Male': genders.count('Male'),
                'Female': genders.count('Female'),
                'Unknown': genders.count('Unknown')
            }
            
            ages = [p.get('age_at_baseline', 0) for p in patient_data]
            summary['patient_demographics']['age_statistics'] = {
                'mean_age': round(sum(ages) / len(ages), 1),
                'min_age': min(ages),
                'max_age': max(ages)
            }
            
            # Disease severity statistics
            baseline_scores = [p.get('scorad_v_total', 0) + p.get('scorad_h_total', 0) for p in patient_data]
            summary['patient_demographics']['disease_severity'] = {
                'mean_baseline_scorad': round(sum(baseline_scores) / len(baseline_scores), 1),
                'patients_severe_disease': len([s for s in baseline_scores if s > 50]),
                'patients_mild_disease': len([s for s in baseline_scores if s < 25])
            }
            
            # Save summary
            summary_path = os.path.join(self.base_path, 'summary', 'simplified_medical_summary.json')
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info("Medical data summary created successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error creating medical summary: {e}")
            raise
    
    def run_complete_pipeline(self):
        """Run the complete simplified medical data pipeline"""
        logger.info("🏥 Starting Simplified Medical Data Pipeline...")
        logger.info("=" * 60)
        
        try:
            # Step 1: Generate medical data
            patient_data = self.generate_medical_sample_data()
            
            # Step 2: Analyze treatment patterns
            treatment_analysis = self.analyze_treatment_patterns(patient_data)
            
            # Step 3: Validate data quality
            validation_results = self.validate_medical_data_quality(patient_data)
            
            # Step 4: Create summary
            summary = self.create_medical_summary(patient_data, treatment_analysis, validation_results)
            
            # Step 5: Save final CSV files
            self.save_csv_outputs(patient_data, treatment_analysis)
            
            # Step 6: Export clean medical data
            export_summary = self.export_clean_medical_data(patient_data, treatment_analysis, summary)
            
            # Step 7: Create backup
            backup_dir = os.path.join(self.base_path, 'backups', f'medical_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
            os.makedirs(backup_dir, exist_ok=True)
            
            # Copy key files to backup
            import shutil
            key_files = [
                'medical_data/sample_medical_data.json',
                'treatment_analysis/treatment_patterns.json',
                'summary/simplified_medical_summary.json'
            ]
            
            for file_path in key_files:
                full_path = os.path.join(self.base_path, file_path)
                if os.path.exists(full_path):
                    shutil.copy2(full_path, backup_dir)
            
            logger.info("✅ Simplified Medical Data Pipeline completed successfully!")
            logger.info(f"📊 Processed {len(patient_data)} patient records")
            logger.info(f"📈 Data Quality Score: {validation_results['quality_score']:.1f}%")
            logger.info(f"🏥 Treatment Improvement Rate: {summary['clinical_insights']['improvement_rate']}%")
            logger.info(f"💊 Most Prescribed: {summary['clinical_insights']['most_prescribed_medication']}")
            logger.info(f"💾 Backup created: {backup_dir}")
            
            return {
                'status': 'success',
                'patients_processed': len(patient_data),
                'quality_score': validation_results['quality_score'],
                'improvement_rate': summary['clinical_insights']['improvement_rate'],
                'backup_location': backup_dir,
                'summary': summary
            }
            
        except Exception as e:
            logger.error(f"❌ Simplified medical pipeline failed: {e}")
            raise
    
    def save_csv_outputs(self, patient_data, treatment_analysis):
        """Save data as CSV files for easy analysis"""
        try:
            # Save patient data as CSV
            patient_csv_path = os.path.join(self.base_path, 'loaded', 'medical_patient_data.csv')
            
            if patient_data:
                fieldnames = patient_data[0].keys()
                with open(patient_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(patient_data)
            
            # Save treatment outcomes as CSV
            outcomes_csv_path = os.path.join(self.base_path, 'loaded', 'treatment_outcomes.csv')
            outcomes_data = treatment_analysis['patient_outcomes']
            
            if outcomes_data:
                fieldnames = outcomes_data[0].keys()
                with open(outcomes_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(outcomes_data)
            
            logger.info("CSV outputs saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving CSV outputs: {e}")
    
    def export_clean_medical_data(self, patient_data, treatment_analysis, summary):
        """Export clean medical data in multiple formats for analysis"""
        try:
            logger.info("🧹 Starting clean medical data export...")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            exports_dir = os.path.join(self.base_path, 'exports')
            clean_data_dir = os.path.join(exports_dir, 'clean_medical_data')
            reports_dir = os.path.join(exports_dir, 'medical_reports')
            
            # 1. Clean Patient Dataset
            clean_patients = []
            for patient in patient_data:
                # Calculate age from birth year
                current_year = datetime.now().year
                age = current_year - patient.get('birth_year', 1980)
                
                # Get BMI from baseline_bmi field
                baseline_bmi = patient.get('baseline_bmi', 22.0)
                
                # Determine BMI category
                if baseline_bmi < 18.5:
                    bmi_category = 'underweight'
                elif baseline_bmi < 25:
                    bmi_category = 'normal'
                elif baseline_bmi < 30:
                    bmi_category = 'overweight'
                else:
                    bmi_category = 'obese'
                
                clean_patient = {
                    'patient_id': patient['patient_id'],
                    'gender': patient['gender'],
                    'age': age,
                    'bmi': round(baseline_bmi, 2),
                    'bmi_category': bmi_category,
                    'disease_severity': 'moderate',  # Default severity
                    'medication_primary': patient.get('medication_primary', 'Unknown'),
                    'treatment_duration_days': patient.get('treatment_duration_days', 30),
                    'baseline_score': patient.get('scorad_v_total', 25.0),
                    'followup_score': patient.get('scorad_h_total', 15.0),
                    'improvement_score': round(patient.get('scorad_v_total', 25.0) - patient.get('scorad_h_total', 15.0), 1),
                    'treatment_effective': patient.get('scorad_v_total', 25.0) > patient.get('scorad_h_total', 15.0),
                    'data_quality': 'high' if all([
                        patient.get('patient_id'),
                        age > 0,
                        baseline_bmi > 0,
                        patient.get('scorad_v_total', 0) >= 0
                    ]) else 'medium'
                }
                clean_patients.append(clean_patient)
            
            # Export clean patients CSV
            clean_patients_path = os.path.join(clean_data_dir, f'clean_patients_{timestamp}.csv')
            with open(clean_patients_path, 'w', newline='', encoding='utf-8') as file:
                if clean_patients:
                    writer = csv.DictWriter(file, fieldnames=clean_patients[0].keys())
                    writer.writeheader()
                    writer.writerows(clean_patients)
            
            # 2. Treatment Effectiveness Dataset
            treatment_outcomes = []
            for i, patient in enumerate(clean_patients):
                # Create simplified treatment outcome record
                baseline_score = patient['baseline_score']
                followup_score = patient['followup_score']
                improvement = baseline_score - followup_score
                improvement_percentage = (improvement / baseline_score * 100) if baseline_score > 0 else 0
                
                clean_outcome = {
                    'patient_id': patient['patient_id'],
                    'treatment_duration_avg': patient['treatment_duration_days'],
                    'baseline_avg': round(baseline_score, 1),
                    'followup_avg': round(followup_score, 1),
                    'improvement_percentage': round(improvement_percentage, 1),
                    'treatment_response': 'good' if improvement_percentage > 20 else 
                                         'moderate' if improvement_percentage > 0 else 'poor',
                    'treatment_status': 'completed'
                }
                treatment_outcomes.append(clean_outcome)
            
            # Export treatment outcomes CSV
            outcomes_path = os.path.join(clean_data_dir, f'treatment_outcomes_{timestamp}.csv')
            with open(outcomes_path, 'w', newline='', encoding='utf-8') as file:
                if treatment_outcomes:
                    writer = csv.DictWriter(file, fieldnames=treatment_outcomes[0].keys())
                    writer.writeheader()
                    writer.writerows(treatment_outcomes)
            
            # 3. High-Quality Patients Only (complete data)
            high_quality_patients = [p for p in clean_patients if p['data_quality'] == 'high']
            if high_quality_patients:
                hq_patients_path = os.path.join(clean_data_dir, f'high_quality_patients_{timestamp}.csv')
                with open(hq_patients_path, 'w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=high_quality_patients[0].keys())
                    writer.writeheader()
                    writer.writerows(high_quality_patients)
            
            # 4. Medical Summary Report
            medical_report = {
                'report_metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'report_type': 'medical_data_export',
                    'total_patients': len(clean_patients),
                    'high_quality_records': len(high_quality_patients),
                    'export_version': '1.0'
                },
                'clinical_summary': {
                    'total_patients_analyzed': len(clean_patients),
                    'patients_with_improvement': sum(1 for p in clean_patients if p['treatment_effective']),
                    'improvement_rate_percentage': round(
                        sum(1 for p in clean_patients if p['treatment_effective']) / len(clean_patients) * 100, 1
                    ),
                    'average_age': round(sum(p['age'] for p in clean_patients) / len(clean_patients), 1),
                    'average_bmi': round(sum(p['bmi'] for p in clean_patients) / len(clean_patients), 1),
                    'most_common_medication': summary['clinical_insights']['most_prescribed_medication']
                },
                'data_quality_metrics': {
                    'high_quality_records': len(high_quality_patients),
                    'data_completeness_rate': round(len(high_quality_patients) / len(clean_patients) * 100, 1),
                    'records_with_complete_treatment_data': len([p for p in clean_patients 
                                                               if p['baseline_score'] >= 0 and p['followup_score'] >= 0])
                },
                'treatment_analysis': {
                    'medications_prescribed': list(set(p['medication_primary'] for p in clean_patients)),
                    'disease_severity_distribution': {
                        severity: sum(1 for p in clean_patients if p['disease_severity'] == severity)
                        for severity in set(p['disease_severity'] for p in clean_patients)
                    },
                    'bmi_category_distribution': {
                        category: sum(1 for p in clean_patients if p['bmi_category'] == category)
                        for category in set(p['bmi_category'] for p in clean_patients)
                    }
                }
            }
            
            # Export medical report JSON
            report_path = os.path.join(reports_dir, f'medical_analysis_report_{timestamp}.json')
            with open(report_path, 'w', encoding='utf-8') as file:
                json.dump(medical_report, file, indent=2, ensure_ascii=False)
            
            # 5. Export clean data as JSON
            json_export = {
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'total_patients': len(clean_patients),
                    'data_format': 'clean_medical_data',
                    'version': '1.0'
                },
                'patients': clean_patients,
                'treatment_outcomes': treatment_outcomes,
                'summary_statistics': medical_report['clinical_summary']
            }
            
            json_path = os.path.join(clean_data_dir, f'clean_medical_data_{timestamp}.json')
            with open(json_path, 'w', encoding='utf-8') as file:
                json.dump(json_export, file, indent=2, ensure_ascii=False)
            
            export_summary = {
                'total_patients_exported': len(clean_patients),
                'high_quality_patients': len(high_quality_patients),
                'treatment_outcomes_exported': len(treatment_outcomes),
                'export_timestamp': timestamp,
                'files_created': {
                    'clean_patients_csv': clean_patients_path,
                    'treatment_outcomes_csv': outcomes_path,
                    'high_quality_patients_csv': hq_patients_path if high_quality_patients else None,
                    'medical_report_json': report_path,
                    'clean_data_json': json_path
                }
            }
            
            logger.info(f"✅ Clean medical data export completed: {len(clean_patients)} patients")
            logger.info(f"📊 High-quality records: {len(high_quality_patients)}")
            logger.info(f"🏥 Treatment outcomes: {len(treatment_outcomes)}")
            
            return export_summary
            
        except Exception as e:
            logger.error(f"Medical data export failed: {str(e)}")
            return None

if __name__ == "__main__":
    print("🏥 SIMPLIFIED MEDICAL DATA PIPELINE")
    print("=" * 60)
    print("SecuTrial-inspired medical data processing using only standard library")
    print("Includes treatment duration analysis and patient outcome tracking")
    print("=" * 60)
    
    pipeline = SimplifiedMedicalPipeline()
    result = pipeline.run_complete_pipeline()
    
    print("\n" + "=" * 60)
    print("🎉 Medical Data Pipeline Results:")
    print(f"   • Patients Processed: {result['patients_processed']}")
    print(f"   • Data Quality Score: {result['quality_score']:.1f}%")
    print(f"   • Treatment Improvement Rate: {result['improvement_rate']}%")
    print(f"   • Status: {result['status'].upper()}")
    print("=" * 60)