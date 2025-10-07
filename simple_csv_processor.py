#!/usr/bin/env python3
"""
Simple CSV Processor for PRORAD Study
A lightweight alternative that works without pandas/numpy dependencies
Uses only Python built-in libraries: csv, os, shutil, json
"""

import csv
import os
import json
from datetime import datetime
import shutil

class SimplePRORADProcessor:
    def __init__(self, input_file, vars_dict_file, output_dir):
        self.input_file = input_file
        self.vars_dict_file = vars_dict_file
        self.output_dir = output_dir
        self.var_mapping = {}
        self.german_translations = {
            'männlich': 'male',
            'weiblich': 'female',
            'allergie': 'allergy',
            'medikament': 'medication',
            'diagnose': 'diagnosis',
            'behandlung': 'treatment',
            'symptom': 'symptom',
            'untersuchung': 'examination',
            'befund': 'finding',
            'therapie': 'therapy',
            'operation': 'surgery',
            'komplikation': 'complication',
            'nebenwirkung': 'side_effect',
            'dosierung': 'dosage',
            'häufigkeit': 'frequency',
            'schwere': 'severity',
            'chronisch': 'chronic',
            'akut': 'acute',
            'normal': 'normal',
            'abnormal': 'abnormal',
            'positiv': 'positive',
            'negativ': 'negative',
            'ja': 'yes',
            'nein': 'no',
            'unbekannt': 'unknown',
            'nicht': 'not',
            'kein': 'no',
            'keine': 'no'
        }
        
    def ensure_output_dir(self):
        """Create output directory if it doesn't exist"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"✅ Created output directory: {self.output_dir}")
    
    def load_var_mapping(self):
        """Load variable mapping from vars_dict.csv"""
        try:
            with open(self.vars_dict_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    original = row.get('VAR_ORIGINAL', '').strip()
                    new = row.get('VAR_NEW', '').strip()
                    if original and new:
                        self.var_mapping[original] = new
            print(f"✅ Loaded {len(self.var_mapping)} variable mappings")
            return True
        except Exception as e:
            print(f"❌ Error loading variable mapping: {e}")
            return False
    
    def translate_value(self, value):
        """Apply German-English translation to a cell value"""
        if not isinstance(value, str):
            return value
        
        value_lower = value.lower().strip()
        return self.german_translations.get(value_lower, value)
    
    def process_csv(self):
        """Process the CSV file with variable mapping and translations"""
        try:
            # Read input file
            input_rows = []
            with open(self.input_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                for row in reader:
                    input_rows.append(row)
            
            print(f"✅ Read {len(input_rows)} rows with {len(headers)} columns")
            
            # Apply variable mapping to headers
            mapped_headers = []
            mapping_report = []
            
            for original_header in headers:
                mapped_header = self.var_mapping.get(original_header, original_header)
                mapped_headers.append(mapped_header)
                
                if mapped_header != original_header:
                    mapping_report.append({
                        'original': original_header,
                        'mapped': mapped_header
                    })
            
            print(f"✅ Applied variable mapping to {len(mapping_report)} columns")
            
            # Apply German-English translations to data
            translated_rows = []
            translation_count = 0
            
            for row in input_rows:
                translated_row = []
                for cell in row:
                    translated_cell = self.translate_value(cell)
                    if translated_cell != cell:
                        translation_count += 1
                    translated_row.append(translated_cell)
                translated_rows.append(translated_row)
            
            print(f"✅ Applied {translation_count} German-English translations")
            
            # Write processed file
            output_file = os.path.join(self.output_dir, 'prorad_processed.csv')
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(mapped_headers)
                writer.writerows(translated_rows)
            
            print(f"✅ Saved processed data to: {output_file}")
            
            # Generate mapping report
            report_file = os.path.join(self.output_dir, 'variable_mapping_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'input_file': self.input_file,
                    'total_columns': len(headers),
                    'mapped_columns': len(mapping_report),
                    'total_rows': len(input_rows),
                    'translation_count': translation_count,
                    'mappings': mapping_report
                }, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Generated mapping report: {report_file}")
            
            # Generate summary
            summary_file = os.path.join(self.output_dir, 'processing_summary.txt')
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"PRORAD Data Processing Summary\n")
                f.write(f"=" * 40 + "\n\n")
                f.write(f"Processed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Input file: {self.input_file}\n")
                f.write(f"Output file: {output_file}\n\n")
                f.write(f"Dataset Statistics:\n")
                f.write(f"- Total columns: {len(headers)}\n")
                f.write(f"- Mapped columns: {len(mapping_report)}\n")
                f.write(f"- Total rows: {len(input_rows)}\n")
                f.write(f"- German-English translations: {translation_count}\n\n")
                f.write(f"Processing completed successfully! ✅\n")
            
            print(f"✅ Generated summary report: {summary_file}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing CSV: {e}")
            return False

def main():
    """Main execution function"""
    print("🔄 Starting Simple PRORAD CSV Processor...")
    print("=" * 50)
    
    # Configuration
    input_file = r"c:\Users\tralucck\OneDrive\airflow\input\input.csv"
    vars_dict_file = r"c:\Users\tralucck\OneDrive\airflow\input\vars_dict.csv"
    output_dir = r"C:\temp\airflow\prorad_processed"
    
    # Check input files
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return False
    
    if not os.path.exists(vars_dict_file):
        print(f"❌ Variables dictionary not found: {vars_dict_file}")
        return False
    
    # Initialize processor
    processor = SimplePRORADProcessor(input_file, vars_dict_file, output_dir)
    
    # Create output directory
    processor.ensure_output_dir()
    
    # Load variable mappings
    if not processor.load_var_mapping():
        return False
    
    # Process the CSV file
    if not processor.process_csv():
        return False
    
    print("=" * 50)
    print("🎉 PRORAD CSV processing completed successfully!")
    print(f"📁 Check outputs in: {output_dir}")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)