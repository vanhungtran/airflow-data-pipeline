# -*- coding: utf-8 -*-
"""
FeaturesSecuTrial preprocessing functions adapted for the airflow pipeline
Classes and functions to manually extract variables (features) from SecuTrial
"""

import re
from datetime import date
import logging

# Set up logging
logger = logging.getLogger(__name__)

class FeaturesSecuTrialPreprocessing:
    """
    Simplified version of FeaturesSecuTrial for use in the airflow pipeline
    Uses standard library only to avoid dependency issues
    """
    
    # Dictionary for hospital location mapping
    dict_hospital_location = {
        '01': 'Berlin',
        '02': 'Munich', 
        '03': 'Hamburg',
        '04': 'Cologne',
        '05': 'Frankfurt',
        '06': 'Stuttgart',
        '07': 'Dresden',
        '08': 'Hannover',
        '09': 'Nuremberg',
        '10': 'Dortmund'
    }
    
    def __init__(self, data):
        """
        Initialize with data (list of dictionaries)
        data -- processed SecuTrial data in dictionary format
        """
        self.data = data
        self.processed_features = {}
        logger.info(f"Initialized FeaturesSecuTrial preprocessing with {len(data)} records")

    def extract_pattern_from_vars(self, var_patterns, regex_pattern, records=None):
        """
        Extract pattern from variables using regex
        
        var_patterns -- list of variable patterns to search for
        regex_pattern -- regex pattern to match (e.g., r'\bja\b' for 'ja')
        records -- specific records to process (default: all)
        """
        if records is None:
            records = self.data
            
        results = []
        pattern = re.compile(regex_pattern, re.IGNORECASE)
        
        for record in records:
            found_any = False
            for var_pattern in var_patterns:
                # Check if any field matches the variable pattern
                for field_name, field_value in record.items():
                    if var_pattern in field_name and field_value:
                        if pattern.search(str(field_value)):
                            found_any = True
                            break
                if found_any:
                    break
            results.append(found_any)
            
        return results

    def extract_location_from_hospital(self, var_name):
        """
        Extract location from hospital ID variable
        
        var_name -- name of the variable containing hospital ID
        """
        logger.info(f"Extracting location from hospital variable: {var_name}")
        
        locations = []
        for record in self.data:
            location = 'Unknown'
            if var_name in record and record[var_name]:
                hospital_id = str(record[var_name])
                
                # Extract location code from hospital ID
                for code, loc in self.dict_hospital_location.items():
                    if code in hospital_id:
                        location = loc
                        break
                        
            locations.append(location)
            
        self.processed_features['location'] = locations
        logger.info(f"Extracted locations: {set(locations)}")
        return locations

    def extract_date(self, var_name):
        """
        Extract date components from YYYYMMDD format
        
        var_name -- variable name containing date as YYYYMMDD
        """
        logger.info(f"Extracting date components from: {var_name}")
        
        years, months, days = [], [], []
        
        for record in self.data:
            year, month, day = None, None, None
            
            if var_name in record and record[var_name]:
                try:
                    date_value = int(float(record[var_name]))
                    year = date_value // 10000
                    month = (date_value % 10000) // 100
                    day = date_value % 100
                except (ValueError, TypeError):
                    pass
                    
            years.append(year)
            months.append(month)
            days.append(day)
            
        self.processed_features[f'{var_name}_year'] = years
        self.processed_features[f'{var_name}_month'] = months
        self.processed_features[f'{var_name}_day'] = days
        
        return years, months, days

    def reformat_date(self, var_name):
        """
        Reformat date variable to standard format
        
        var_name -- variable name containing date
        """
        logger.info(f"Reformatting date variable: {var_name}")
        
        formatted_dates = []
        for record in self.data:
            formatted_date = None
            
            if var_name in record and record[var_name]:
                date_str = str(record[var_name]).replace('.0', '')
                
                # Try different date formats
                formats = ['%Y%m%d', '%Y%m', '%Y']
                for fmt in formats:
                    try:
                        if len(date_str) == len(fmt.replace('%', '')):
                            from datetime import datetime
                            parsed_date = datetime.strptime(date_str, fmt)
                            formatted_date = parsed_date.strftime('%Y-%m-%d')
                            break
                    except ValueError:
                        continue
                        
            formatted_dates.append(formatted_date)
            
        self.processed_features[f'{var_name}_formatted'] = formatted_dates
        return formatted_dates

    def compute_BMI(self, weight_var, height_var, time_point='t0'):
        """
        Calculate BMI from weight and height variables
        
        weight_var -- variable name for weight (kg)
        height_var -- variable name for height (cm)
        time_point -- time point identifier
        """
        logger.info(f"Computing BMI from {weight_var} and {height_var}")
        
        bmi_values = []
        for record in self.data:
            bmi = None
            
            try:
                weight = float(record.get(weight_var, 0)) if record.get(weight_var) else None
                height = float(record.get(height_var, 0)) if record.get(height_var) else None
                
                if weight and height and height > 0:
                    height_meters = height / 100  # Convert cm to meters
                    bmi = weight / (height_meters ** 2)
                    bmi = round(bmi, 2)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
                
            bmi_values.append(bmi)
            
        feature_name = f'{time_point}_BMI'
        self.processed_features[feature_name] = bmi_values
        logger.info(f"Computed BMI for {len([b for b in bmi_values if b])} records")
        
        return bmi_values

    def extract_sys_therapy(self, regex=r'\bja\b', time_point='t0'):
        """
        Extract systemic therapy information
        
        regex -- pattern to match (default: 'ja')
        time_point -- time point identifier
        """
        logger.info(f"Extracting systemic therapy for {time_point}")
        
        # Look for therapy-related variables
        therapy_patterns = ['thraea', 'therapy', 'behandlung', 'medikament']
        
        results = []
        pattern = re.compile(regex, re.IGNORECASE)
        
        for record in self.data:
            has_therapy = False
            for field_name, field_value in record.items():
                if any(pat in field_name.lower() for pat in therapy_patterns):
                    if field_value and pattern.search(str(field_value)):
                        has_therapy = True
                        break
            results.append(has_therapy)
            
        feature_name = f'{time_point}_sys_therapy'
        self.processed_features[feature_name] = results
        
        return results

    def extract_AD(self, time_point='t0'):
        """
        Extract Atopic Dermatitis status
        
        time_point -- time point identifier
        """
        logger.info(f"Extracting AD status for {time_point}")
        
        ad_var = f'{time_point}_fw_anamad'
        ad_results = []
        
        for record in self.data:
            ad_status = None
            
            if ad_var in record:
                value = record[ad_var]
                if value:
                    if re.search(r'\bja\b', str(value), re.IGNORECASE):
                        ad_status = True
                    elif re.search(r'\bnein\b', str(value), re.IGNORECASE):
                        ad_status = False
                        
            ad_results.append(ad_status)
            
        feature_name = f'{time_point}_AD'
        self.processed_features[feature_name] = ad_results
        
        return ad_results

    def extract_AD_severity(self, time_point='t0', score='scorad'):
        """
        Extract AD severity based on EASI or SCORAD scores
        
        time_point -- time point identifier
        score -- score type ('easi' or 'scorad')
        """
        logger.info(f"Extracting AD severity using {score} for {time_point}")
        
        score_var = f'{time_point}_fw_{score}_tot'
        severity_results = []
        
        # First ensure we have AD status
        ad_feature = f'{time_point}_AD'
        if ad_feature not in self.processed_features:
            self.extract_AD(time_point)
            
        ad_status = self.processed_features[ad_feature]
        
        for i, record in enumerate(self.data):
            severity = None
            
            try:
                score_value = float(record.get(score_var, 0)) if record.get(score_var) else None
                patient_ad = ad_status[i] if i < len(ad_status) else None
                
                if patient_ad is False:
                    severity = 'healthy'
                elif score_value is not None and patient_ad is True:
                    if score == 'easi':
                        if score_value <= 7:
                            severity = 'mild'
                        elif score_value <= 21:
                            severity = 'moderate'
                        else:
                            severity = 'severe'
                    elif score == 'scorad':
                        if score_value <= 25:
                            severity = 'mild'
                        elif score_value <= 60:
                            severity = 'moderate'
                        else:
                            severity = 'severe'
            except (ValueError, TypeError):
                pass
                
            severity_results.append(severity)
            
        feature_name = f'{time_point}_AD_severity_{score}'
        self.processed_features[feature_name] = severity_results
        
        return severity_results

    def extract_dupilumab(self, regex=r'\bD11AH05\b', time_point='t0'):
        """
        Extract dupilumab usage information
        
        regex -- pattern to match dupilumab code
        time_point -- time point identifier
        """
        logger.info(f"Extracting dupilumab usage for {time_point}")
        
        dupilumab_patterns = ['dupilumab', 'D11AH05']
        results = []
        pattern = re.compile(regex, re.IGNORECASE)
        
        for record in self.data:
            has_dupilumab = False
            for field_name, field_value in record.items():
                if any(pat in field_name for pat in dupilumab_patterns):
                    if field_value and pattern.search(str(field_value)):
                        has_dupilumab = True
                        break
            results.append(has_dupilumab)
            
        feature_name = f'{time_point}_dupilumab'
        self.processed_features[feature_name] = results
        
        return results

    def compute_patient_age(self, exam_year_var, exam_month_var, born_year_var, born_month_var, time_point='t0'):
        """
        Compute patient age at examination
        
        exam_year_var -- examination year variable
        exam_month_var -- examination month variable  
        born_year_var -- birth year variable
        born_month_var -- birth month variable
        time_point -- time point identifier
        """
        logger.info(f"Computing patient age for {time_point}")
        
        ages = []
        for record in self.data:
            age = None
            
            try:
                exam_year = int(record.get(exam_year_var, 0)) if record.get(exam_year_var) else None
                exam_month = int(record.get(exam_month_var, 0)) if record.get(exam_month_var) else None
                born_year = int(record.get(born_year_var, 0)) if record.get(born_year_var) else None
                born_month = int(record.get(born_month_var, 0)) if record.get(born_month_var) else None
                
                if all([exam_year, exam_month, born_year, born_month]):
                    age = exam_year - born_year
                    if exam_month < born_month:
                        age -= 1
                        
            except (ValueError, TypeError):
                pass
                
            ages.append(age)
            
        feature_name = f'{time_point}_patient_age'
        self.processed_features[feature_name] = ages
        
        return ages

    def get_processing_summary(self):
        """
        Get summary of all processed features
        """
        summary = {
            'total_records': len(self.data),
            'processed_features': len(self.processed_features),
            'feature_names': list(self.processed_features.keys())
        }
        
        return summary

    def export_processed_features(self, output_path):
        """
        Export processed features to JSON file
        
        output_path -- path to save the processed features
        """
        import json
        
        summary = self.get_processing_summary()
        
        export_data = {
            'summary': summary,
            'features': self.processed_features,
            'timestamp': date.today().isoformat()
        }
        
        with open(output_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
            
        logger.info(f"Exported processed features to: {output_path}")
        return output_path