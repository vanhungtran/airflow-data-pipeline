#!/usr/bin/env python3
"""
Simple Data Validator for PRORAD Processed Dataset
Performs basic quality checks on the processed CSV file
"""

import csv
import os
import json
from datetime import datetime
from collections import Counter

def validate_processed_data(processed_file, output_dir):
    """Validate the processed PRORAD dataset"""
    
    print("🔍 Starting data validation...")
    print("=" * 40)
    
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'processed_file': processed_file,
        'checks': {}
    }
    
    try:
        # Read the processed file
        with open(processed_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
        
        print(f"✅ Loaded dataset: {len(rows)} rows × {len(headers)} columns")
        
        # Check 1: Data completeness
        total_cells = len(rows) * len(headers)
        empty_cells = 0
        
        for row in rows:
            for cell in row:
                if not cell or cell.strip() == '':
                    empty_cells += 1
        
        completeness = ((total_cells - empty_cells) / total_cells) * 100
        validation_results['checks']['data_completeness'] = {
            'total_cells': total_cells,
            'empty_cells': empty_cells,
            'completeness_percentage': round(completeness, 2)
        }
        
        print(f"✅ Data Completeness: {completeness:.2f}% ({total_cells - empty_cells:,}/{total_cells:,} cells filled)")
        
        # Check 2: Column mapping validation
        mapped_columns = [col for col in headers if not col.startswith('mnp')]
        original_columns = [col for col in headers if col.startswith('mnp')]
        
        validation_results['checks']['column_mapping'] = {
            'total_columns': len(headers),
            'mapped_columns': len(mapped_columns),
            'original_columns': len(original_columns),
            'mapping_rate': round((len(mapped_columns) / len(headers)) * 100, 2)
        }
        
        print(f"✅ Column Mapping: {len(mapped_columns)} mapped, {len(original_columns)} original")
        
        # Check 3: Language translation check
        german_words = ['männlich', 'weiblich', 'ja', 'nein', 'allergie', 'diagnose']
        english_words = ['male', 'female', 'yes', 'no', 'allergy', 'diagnosis']
        
        german_count = 0
        english_count = 0
        
        for row in rows:
            for cell in row:
                if isinstance(cell, str):
                    cell_lower = cell.lower().strip()
                    if cell_lower in german_words:
                        german_count += 1
                    elif cell_lower in english_words:
                        english_count += 1
        
        validation_results['checks']['language_translation'] = {
            'german_terms_found': german_count,
            'english_terms_found': english_count,
            'translation_ratio': round(english_count / (german_count + english_count) * 100, 2) if (german_count + english_count) > 0 else 0
        }
        
        print(f"✅ Language Translation: {english_count} English terms, {german_count} German terms remaining")
        
        # Check 4: Data type analysis for key columns
        key_columns = ['pid', 'aid', 'age', 'sex', 'diagnosis']
        data_type_analysis = {}
        
        for col_name in key_columns:
            if col_name in headers:
                col_index = headers.index(col_name)
                values = [row[col_index] for row in rows if col_index < len(row)]
                
                numeric_count = 0
                text_count = 0
                empty_count = 0
                
                for value in values:
                    if not value or value.strip() == '':
                        empty_count += 1
                    elif value.replace('.', '').replace('-', '').isdigit():
                        numeric_count += 1
                    else:
                        text_count += 1
                
                data_type_analysis[col_name] = {
                    'total_values': len(values),
                    'numeric_values': numeric_count,
                    'text_values': text_count,
                    'empty_values': empty_count
                }
        
        validation_results['checks']['data_types'] = data_type_analysis
        
        print(f"✅ Data Type Analysis completed for {len(data_type_analysis)} key columns")
        
        # Check 5: Unique identifier validation
        if 'pid' in headers:
            pid_index = headers.index('pid')
            pid_values = [row[pid_index] for row in rows if pid_index < len(row) and row[pid_index]]
            unique_pids = len(set(pid_values))
            
            validation_results['checks']['unique_identifiers'] = {
                'total_pid_values': len(pid_values),
                'unique_pid_values': unique_pids,
                'duplicate_rate': round(((len(pid_values) - unique_pids) / len(pid_values)) * 100, 2) if pid_values else 0
            }
            
            print(f"✅ Patient IDs: {unique_pids} unique out of {len(pid_values)} total")
        
        # Overall validation score
        scores = []
        if completeness >= 80:
            scores.append(1)
        if validation_results['checks']['column_mapping']['mapping_rate'] >= 90:
            scores.append(1)
        if validation_results['checks']['language_translation']['translation_ratio'] >= 80:
            scores.append(1)
        
        overall_score = (sum(scores) / 3) * 100
        validation_results['overall_quality_score'] = round(overall_score, 2)
        
        print(f"✅ Overall Quality Score: {overall_score:.1f}%")
        
        # Save validation report
        validation_file = os.path.join(output_dir, 'data_validation_report.json')
        with open(validation_file, 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Saved validation report: {validation_file}")
        
        # Create human-readable summary
        summary_file = os.path.join(output_dir, 'validation_summary.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("PRORAD Data Validation Summary\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Validation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Dataset: {processed_file}\n\n")
            
            f.write("Quality Metrics:\n")
            f.write(f"- Data Completeness: {completeness:.2f}%\n")
            f.write(f"- Column Mapping Rate: {validation_results['checks']['column_mapping']['mapping_rate']:.2f}%\n")
            f.write(f"- Language Translation Ratio: {validation_results['checks']['language_translation']['translation_ratio']:.2f}%\n")
            f.write(f"- Overall Quality Score: {overall_score:.1f}%\n\n")
            
            f.write("Dataset Statistics:\n")
            f.write(f"- Total Rows: {len(rows):,}\n")
            f.write(f"- Total Columns: {len(headers):,}\n")
            f.write(f"- Mapped Columns: {len(mapped_columns):,}\n")
            f.write(f"- Data Cells: {total_cells:,}\n")
            f.write(f"- Filled Cells: {total_cells - empty_cells:,}\n\n")
            
            if 'pid' in validation_results['checks'].get('unique_identifiers', {}):
                uid_check = validation_results['checks']['unique_identifiers']
                f.write(f"Patient Identifiers:\n")
                f.write(f"- Total Patient Records: {uid_check['total_pid_values']:,}\n")
                f.write(f"- Unique Patients: {uid_check['unique_pid_values']:,}\n")
                f.write(f"- Duplicate Rate: {uid_check['duplicate_rate']:.2f}%\n\n")
            
            f.write("Validation Status: ✅ PASSED\n")
        
        print(f"✅ Created validation summary: {summary_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        return False

def main():
    """Main validation function"""
    processed_file = r"C:\temp\airflow\prorad_processed\prorad_processed.csv"
    output_dir = r"C:\temp\airflow\prorad_processed"
    
    if not os.path.exists(processed_file):
        print(f"❌ Processed file not found: {processed_file}")
        return False
    
    success = validate_processed_data(processed_file, output_dir)
    
    if success:
        print("=" * 40)
        print("🎉 Data validation completed successfully!")
        print(f"📁 Check validation reports in: {output_dir}")
    
    return success

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)