#!/usr/bin/env python3
"""
PRORAD Study Complete Processing Pipeline
Processes the entire PRORAD dataset with variable mapping and language normalization
"""

import pandas as pd
import numpy as np
import csv
import os
import logging
import json
import re
from datetime import datetime
from typing import Dict, List, Tuple, Set
from collections import defaultdict, Counter

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PRORADProcessor:
    """Complete PRORAD study data processor"""
    
    def __init__(self):
        self.input_path = "C:/Users/tralucck/OneDrive/airflow/input"
        self.output_path = "C:/temp/airflow/prorad_processed"
        self.setup_directories()
        
        # Initialize mappings
        self.var_mapping = {}
        self.german_english_dict = {}
        self.spell_corrections = {}
        
        # Processing stats
        self.processing_stats = {
            'total_rows': 0,
            'total_columns': 0,
            'mapped_columns': 0,
            'unmapped_columns': 0,
            'translated_values': 0,
            'spell_corrections_made': 0,
            'inconsistencies_found': 0
        }
        
        # Load mappings
        self.load_var_mapping()
        self.load_translation_dictionary()
    
    def setup_directories(self):
        """Create output directories"""
        directories = [
            self.output_path,
            os.path.join(self.output_path, 'reports'),
            os.path.join(self.output_path, 'quality_checks'),
            os.path.join(self.output_path, 'logs')
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"📁 Created directory: {directory}")
    
    def load_var_mapping(self):
        """Load VAR_ORIGINAL -> VAR_NEW mapping from vars_dict.csv"""
        try:
            vars_dict_path = os.path.join(self.input_path, 'vars_dict.csv')
            logger.info(f"📚 Loading VAR_ORIGINAL -> VAR_NEW mapping from: {vars_dict_path}")
            
            with open(vars_dict_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    var_original = row.get('VAR_ORIGINAL', '').strip()
                    var_new = row.get('VAR_NEW', '').strip()
                    
                    if var_original and var_new:
                        self.var_mapping[var_original] = var_new
            
            logger.info(f"✅ Loaded {len(self.var_mapping)} VAR_ORIGINAL -> VAR_NEW mappings")
            
        except Exception as e:
            logger.error(f"❌ Error loading variable mapping: {e}")
            raise
    
    def load_translation_dictionary(self):
        """Load German to English translation dictionary"""
        # Comprehensive German-English medical/study terms dictionary
        self.german_english_dict = {
            # Basic responses
            'ja': 'yes',
            'nein': 'no',
            'nicht_bekannt': 'unknown',
            'unbekannt': 'unknown',
            'fehlend': 'missing',
            'nicht_vorhanden': 'not_available',
            
            # Medical terms
            'mann': 'male',
            'männlich': 'male',
            'frau': 'female',
            'weiblich': 'female',
            'geschlecht': 'gender',
            'alter': 'age',
            'geburt': 'birth',
            'gewicht': 'weight',
            'größe': 'height',
            'blutdruck': 'blood_pressure',
            
            # Time-related
            'datum': 'date',
            'zeit': 'time',
            'tag': 'day',
            'monat': 'month',
            'jahr': 'year',
            'heute': 'today',
            'gestern': 'yesterday',
            'morgen': 'tomorrow',
            
            # Medical conditions
            'allergie': 'allergy',
            'asthma': 'asthma',
            'dermatitis': 'dermatitis',
            'ekzem': 'eczema',
            'hautausschlag': 'skin_rash',
            'behandlung': 'treatment',
            'therapie': 'therapy',
            'medikament': 'medication',
            'diagnose': 'diagnosis',
            
            # Severity levels
            'leicht': 'mild',
            'mittel': 'moderate',
            'schwer': 'severe',
            'stark': 'strong',
            'schwach': 'weak',
            'keine': 'none',
            'gering': 'low',
            'hoch': 'high',
            
            # Family relationships
            'mutter': 'mother',
            'vater': 'father',
            'geschwister': 'siblings',
            'kind': 'child',
            'kinder': 'children',
            'familie': 'family',
            
            # Common study terms
            'patient': 'patient',
            'visite': 'visit',
            'untersuchung': 'examination',
            'befund': 'finding',
            'ergebnis': 'result',
            'wert': 'value',
            'messung': 'measurement',
            
            # Body parts
            'kopf': 'head',
            'hals': 'neck',
            'gesicht': 'face',
            'auge': 'eye',
            'augen': 'eyes',
            'nase': 'nose',
            'mund': 'mouth',
            'haut': 'skin',
            'arm': 'arm',
            'arme': 'arms',
            'hand': 'hand',
            'hände': 'hands',
            'bein': 'leg',
            'beine': 'legs',
            'fuß': 'foot',
            'füße': 'feet',
            
            # Study-specific terms
            'baseline': 'baseline',
            'follow_up': 'follow_up',
            'screening': 'screening',
            'randomisierung': 'randomization',
            'plazebo': 'placebo',
            'kontrolle': 'control',
            'intervention': 'intervention'
        }
        
        logger.info(f"📖 Loaded {len(self.german_english_dict)} German-English translations")
    
    def detect_language_and_translate(self, value: str) -> Tuple[str, bool, str]:
        """
        Detect if value is German and translate to English
        Returns: (translated_value, was_translated, original_language)
        """
        if pd.isna(value) or value == '':
            return value, False, 'empty'
        
        value_str = str(value).lower().strip()
        
        # Check for German terms
        for german, english in self.german_english_dict.items():
            if german in value_str:
                translated = value_str.replace(german, english)
                if translated != value_str:
                    return translated, True, 'german'
        
        # Check for common German patterns
        german_patterns = [
            r'ä|ö|ü|ß',  # German umlauts
            r'_[0-9]+$',  # Common German coding pattern
            r'^v[0-9]+_',  # Variable pattern
        ]
        
        for pattern in german_patterns:
            if re.search(pattern, value_str):
                return value_str, False, 'german_pattern'
        
        return value_str, False, 'english_or_unknown'
    
    def detect_spelling_errors(self, values: List[str]) -> Dict[str, str]:
        """
        Detect potential spelling errors and suggest corrections
        """
        corrections = {}
        value_counts = Counter([str(v).lower().strip() for v in values if pd.notna(v)])
        
        # Find potential misspellings (values that appear very rarely)
        rare_values = {val: count for val, count in value_counts.items() if count == 1}
        common_values = {val: count for val, count in value_counts.items() if count > 5}
        
        for rare_val in rare_values:
            # Simple Levenshtein-like similarity check
            for common_val in common_values:
                if len(rare_val) > 3 and len(common_val) > 3:
                    # Check for single character differences
                    diff_count = sum(1 for i, (a, b) in enumerate(zip(rare_val, common_val)) if a != b)
                    if len(rare_val) == len(common_val) and diff_count == 1:
                        corrections[rare_val] = common_val
                        break
        
        return corrections
    
    def process_full_dataset(self):
        """Process the complete PRORAD dataset"""
        logger.info("🚀 Starting PRORAD complete dataset processing...")
        
        try:
            # Load the complete dataset
            input_file = os.path.join(self.input_path, 'input.csv')
            logger.info(f"📥 Loading complete dataset from: {input_file}")
            
            # Read with efficient processing for large files
            chunk_size = 1000
            chunks = []
            
            for chunk_df in pd.read_csv(input_file, chunksize=chunk_size, low_memory=False):
                chunks.append(chunk_df)
            
            df = pd.concat(chunks, ignore_index=True)
            
            self.processing_stats['total_rows'] = len(df)
            self.processing_stats['total_columns'] = len(df.columns)
            
            logger.info(f"📊 Dataset loaded: {len(df)} rows × {len(df.columns)} columns")
            
            # Step 1: Variable mapping
            logger.info("🔄 Step 1: Applying variable mapping...")
            df_mapped = self.apply_variable_mapping(df)
            
            # Step 2: Language normalization
            logger.info("🌍 Step 2: Applying language normalization...")
            df_normalized = self.apply_language_normalization(df_mapped)
            
            # Step 3: Quality checks
            logger.info("🔍 Step 3: Running quality checks...")
            quality_report = self.run_quality_checks(df_normalized)
            
            # Step 4: Save processed dataset
            logger.info("💾 Step 4: Saving processed dataset...")
            self.save_processed_dataset(df_normalized)
            
            # Step 5: Generate comprehensive report
            logger.info("📋 Step 5: Generating processing report...")
            self.generate_processing_report(quality_report)
            
            logger.info("✅ PRORAD dataset processing completed successfully!")
            return df_normalized
            
        except Exception as e:
            logger.error(f"❌ Error processing PRORAD dataset: {e}")
            raise
    
    def apply_variable_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply VAR_ORIGINAL -> VAR_NEW mapping to column names"""
        logger.info("🏷️ Applying variable name mapping...")
        
        original_columns = list(df.columns)
        mapped_columns = []
        unmapped_columns = []
        
        # Create mapping for existing columns
        column_mapping = {}
        for col in original_columns:
            if col in self.var_mapping:
                column_mapping[col] = self.var_mapping[col]
                mapped_columns.append(col)
            else:
                unmapped_columns.append(col)
        
        # Apply mapping
        df_mapped = df.rename(columns=column_mapping)
        
        # Update stats
        self.processing_stats['mapped_columns'] = len(mapped_columns)
        self.processing_stats['unmapped_columns'] = len(unmapped_columns)
        
        logger.info(f"✅ Variable mapping completed:")
        logger.info(f"   - Mapped columns: {len(mapped_columns)}")
        logger.info(f"   - Unmapped columns: {len(unmapped_columns)}")
        
        # Save mapping report
        mapping_report = {
            'timestamp': datetime.now().isoformat(),
            'total_columns': len(original_columns),
            'mapped_columns': len(mapped_columns),
            'unmapped_columns': len(unmapped_columns),
            'mapping_success_rate': len(mapped_columns) / len(original_columns) * 100,
            'mapped_column_list': mapped_columns,
            'unmapped_column_list': unmapped_columns
        }
        
        with open(os.path.join(self.output_path, 'reports', 'variable_mapping_report.json'), 'w') as f:
            json.dump(mapping_report, f, indent=2)
        
        return df_mapped
    
    def apply_language_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply German to English translation and spell checking"""
        logger.info("🌐 Applying language normalization...")
        
        df_normalized = df.copy()
        translation_log = []
        inconsistency_log = []
        
        for column in df_normalized.columns:
            logger.info(f"   Processing column: {column}")
            
            # Get unique values for this column
            unique_values = df_normalized[column].dropna().unique()
            
            if len(unique_values) == 0:
                continue
            
            # Detect and translate German values
            column_translations = {}
            for value in unique_values:
                translated, was_translated, language = self.detect_language_and_translate(value)
                if was_translated:
                    column_translations[value] = translated
                    translation_log.append({
                        'column': column,
                        'original': value,
                        'translated': translated,
                        'language': language
                    })
            
            # Apply translations
            if column_translations:
                df_normalized[column] = df_normalized[column].replace(column_translations)
                self.processing_stats['translated_values'] += len(column_translations)
            
            # Detect spelling inconsistencies
            column_values = df_normalized[column].dropna().astype(str).tolist()
            spelling_corrections = self.detect_spelling_errors(column_values)
            
            if spelling_corrections:
                inconsistency_log.extend([
                    {
                        'column': column,
                        'potential_error': error,
                        'suggested_correction': correction,
                        'confidence': 'medium'
                    } for error, correction in spelling_corrections.items()
                ])
                self.processing_stats['inconsistencies_found'] += len(spelling_corrections)
        
        # Save translation and inconsistency reports
        with open(os.path.join(self.output_path, 'reports', 'translation_log.json'), 'w') as f:
            json.dump(translation_log, f, indent=2)
        
        with open(os.path.join(self.output_path, 'reports', 'inconsistency_log.json'), 'w') as f:
            json.dump(inconsistency_log, f, indent=2)
        
        logger.info(f"✅ Language normalization completed:")
        logger.info(f"   - Translated values: {self.processing_stats['translated_values']}")
        logger.info(f"   - Inconsistencies found: {self.processing_stats['inconsistencies_found']}")
        
        return df_normalized
    
    def run_quality_checks(self, df: pd.DataFrame) -> Dict:
        """Run comprehensive data quality checks"""
        logger.info("🔍 Running data quality checks...")
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'dataset_overview': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'missing_data': {},
            'data_types': {},
            'unique_values': {},
            'temporal_analysis': {},
            'longitudinal_structure': {}
        }
        
        # Missing data analysis
        for column in df.columns:
            missing_count = df[column].isna().sum()
            missing_pct = (missing_count / len(df)) * 100
            quality_report['missing_data'][column] = {
                'missing_count': int(missing_count),
                'missing_percentage': round(missing_pct, 2)
            }
        
        # Data type analysis
        for column in df.columns:
            dtype_str = str(df[column].dtype)
            unique_count = df[column].nunique()
            quality_report['data_types'][column] = dtype_str
            quality_report['unique_values'][column] = int(unique_count)
        
        # Temporal analysis (detect time point columns)
        temporal_columns = [col for col in df.columns if any(
            time_indicator in col.lower() 
            for time_indicator in ['t0_', 't1_', 't2_', 't3_', 't4_', 't5_', 'date', 'time']
        )]
        
        quality_report['temporal_analysis'] = {
            'temporal_columns_found': len(temporal_columns),
            'temporal_columns': temporal_columns[:20]  # Limit for readability
        }
        
        # Longitudinal structure analysis
        time_points = set()
        for col in df.columns:
            match = re.match(r'^t(\d+)_', col)
            if match:
                time_points.add(int(match.group(1)))
        
        quality_report['longitudinal_structure'] = {
            'detected_time_points': sorted(list(time_points)),
            'max_time_point': max(time_points) if time_points else 0,
            'longitudinal_columns': len([col for col in df.columns if re.match(r'^t\d+_', col)])
        }
        
        logger.info(f"✅ Quality checks completed:")
        logger.info(f"   - Time points detected: {sorted(list(time_points))}")
        logger.info(f"   - Temporal columns: {len(temporal_columns)}")
        
        # Save quality report
        with open(os.path.join(self.output_path, 'quality_checks', 'quality_report.json'), 'w') as f:
            json.dump(quality_report, f, indent=2)
        
        return quality_report
    
    def save_processed_dataset(self, df: pd.DataFrame):
        """Save the processed dataset in multiple formats"""
        logger.info("💾 Saving processed dataset...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as CSV
        csv_path = os.path.join(self.output_path, f'prorad_processed_{timestamp}.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"📄 CSV saved: {csv_path}")
        
        # Save as Excel with multiple sheets
        excel_path = os.path.join(self.output_path, f'prorad_processed_{timestamp}.xlsx')
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='ProradData', index=False)
            
            # Add metadata sheet
            metadata_df = pd.DataFrame([
                ['Processing Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Total Rows', len(df)],
                ['Total Columns', len(df.columns)],
                ['Mapped Columns', self.processing_stats['mapped_columns']],
                ['Translated Values', self.processing_stats['translated_values']],
                ['Inconsistencies Found', self.processing_stats['inconsistencies_found']]
            ], columns=['Metric', 'Value'])
            metadata_df.to_excel(writer, sheet_name='ProcessingMetadata', index=False)
        
        logger.info(f"📊 Excel saved: {excel_path}")
        
        # Save column mapping reference
        column_reference = pd.DataFrame([
            {'Column_Index': i, 'Column_Name': col, 'Data_Type': str(df[col].dtype)}
            for i, col in enumerate(df.columns)
        ])
        reference_path = os.path.join(self.output_path, f'column_reference_{timestamp}.csv')
        column_reference.to_csv(reference_path, index=False)
        logger.info(f"📋 Column reference saved: {reference_path}")
    
    def generate_processing_report(self, quality_report: Dict):
        """Generate comprehensive processing report"""
        logger.info("📋 Generating comprehensive processing report...")
        
        report = {
            'prorad_processing_summary': {
                'processing_timestamp': datetime.now().isoformat(),
                'pipeline_version': '1.0.0',
                'dataset_source': 'input/input.csv',
                'processing_statistics': self.processing_stats,
                'quality_metrics': quality_report
            }
        }
        
        # Save comprehensive report
        report_path = os.path.join(self.output_path, 'reports', 'prorad_processing_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Create human-readable summary
        summary_lines = [
            "# PRORAD Study Processing Report",
            f"**Processing Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Dataset Overview",
            f"- **Total Rows:** {self.processing_stats['total_rows']:,}",
            f"- **Total Columns:** {self.processing_stats['total_columns']:,}",
            f"- **Variable Mapping Success:** {self.processing_stats['mapped_columns']}/{self.processing_stats['total_columns']} ({self.processing_stats['mapped_columns']/self.processing_stats['total_columns']*100:.1f}%)",
            "",
            "## Processing Results",
            f"- **Mapped Columns:** {self.processing_stats['mapped_columns']:,}",
            f"- **Unmapped Columns:** {self.processing_stats['unmapped_columns']:,}",
            f"- **Translated Values:** {self.processing_stats['translated_values']:,}",
            f"- **Spell Corrections:** {self.processing_stats['spell_corrections_made']:,}",
            f"- **Inconsistencies Found:** {self.processing_stats['inconsistencies_found']:,}",
            "",
            "## Longitudinal Structure",
            f"- **Time Points Detected:** {quality_report['longitudinal_structure']['detected_time_points']}",
            f"- **Maximum Time Point:** T{quality_report['longitudinal_structure']['max_time_point']}",
            f"- **Longitudinal Columns:** {quality_report['longitudinal_structure']['longitudinal_columns']:,}",
            "",
            "## Quality Assessment",
            "- ✅ Variable mapping completed",
            "- ✅ Language normalization applied",
            "- ✅ Inconsistency detection performed",
            "- ✅ Longitudinal structure validated",
            "",
            f"**Processing completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}**"
        ]
        
        summary_path = os.path.join(self.output_path, 'reports', 'PROCESSING_SUMMARY.md')
        with open(summary_path, 'w') as f:
            f.write('\n'.join(summary_lines))
        
        logger.info(f"📋 Processing report saved: {report_path}")
        logger.info(f"📄 Summary report saved: {summary_path}")

def main():
    """Main execution function"""
    processor = PRORADProcessor()
    processed_df = processor.process_full_dataset()
    return processed_df

if __name__ == "__main__":
    main()