#!/usr/bin/env python3
"""
Enhanced PRORAD CSV Processor with Spelling Detection and Comprehensive Transformations
Includes spelling mistake detection and the complete medical terminology transformations
Uses only Python built-in libraries for compatibility
"""

import csv
import os
import json
import re
from datetime import datetime
from collections import Counter
import difflib

class EnhancedPRORADProcessor:
    def __init__(self, input_file, vars_dict_file, output_dir):
        self.input_file = input_file
        self.vars_dict_file = vars_dict_file
        self.output_dir = output_dir
        self.var_mapping = {}
        self.spelling_suggestions = []
        self.transformation_stats = {
            'spelling_corrections': 0,
            'transformations_applied': 0,
            'suspicious_spellings': 0
        }
        
        # Comprehensive German-English and medical transformations dictionary
        self.comprehensive_transformations = {
            # Time-related transformations
            "Ja, (Während Kindheit und Erwachsenenalter)": "Yes, (During childhood and adulthood)", 
            "Während Kindheit und Erwachsenenalter": "During childhood and adulthood",
            "Yes, während der letzten 12 Monaten": "Yes, during the last 12 months",  
            "Yes, wÃ¤hrend UND vor mehr als 12 Monaten": "Yes, during AND more than 12 months",  
            "Yes, es wird Basistherapie/Hautpflege verwendet.": "Yes, basic therapy/skin care is used.", 
            "Nur im Kindesalter": "Only in childhood", 
            "Ja, aber wann ist unbekannt": "Yes, but when is unknown",
            "Nur im Erwachsenenalter": "Adulthood only", 
            "No, bisher keine Therapie verwendet.": "No, no therapy used so far.",
            "Ja, vor mehr als 12 Monate": "Yes, more than 12 months ago", 
            "Ja, während UND vor mehr als 12 Monaten": "Yes, during AND more than 12 months ago", 
            "Ja, während der letzten 12 Monaten": "Yes, within the last 12 months",
            
            # Gender and basic responses
            "Weiblich": "Female", 
            "Männlich": "Male", 
            "MÃ¤nnlich": "Male", 
            "Ja": "Yes", 
            "Nein": "No",
            "ja": "Yes", 
            "yes": "Yes",
            "nein": "No", 
            "no ": "No ",
            
            # Unknown/unclear responses
            "unknown": "Unknown", 
            "Unbekannt": "Unknown", 
            "UNBEKANNT": "Unknown",
            "Unk": "Unknown",
            
            # Contact/appointment related
            "nicht erschienen": "do not understand", 
            "NICHT ERSCHIENEN": "do not understand", 
            "nicht mehr erreicht": "not reached anymore", 
            "NICHT MEHR ERREICHT": "not reached anymore", 
            "kein Kontakt möglich": "no contact possible", 
            "Termin abgesagt": "canceled appointment", 
            "nicht erreicht": "not reached", 
            "keine Kontaktaufnahm": "no contact", 
            "Kontaktaufnahme": "contact",
            "KONTAKTAUFNAHME": "contact",
            "KEIN KONTAKT MÖGLICH": "no contact possible",
            "KONTAKTAUF N MÖGLICH": "contact impossible",
            "KONTAKTAUFNAHME N MÖ": "contact impossible",
            "Adresse nicht korrek": "address not correct",
            "keinen Kontakt mehr": "no more contact", 
            "kein Kontakt mehr": "no more contact",
            
            # Medical conditions
            "PSORIASIS": "Psoriasis", 
            "Gürtelrose": "shingles", 
            "Akne": "acne", 
            "AGNE INVERSA": "Acne inversa", 
            "raue Haut an Augenlider": "rough skin on eyelids", 
            "Basaliom": "basalioma",
            "aber die Neurodermitis besteht nicht mehr seit:": "but the neurodermatitis has not existed since:",
            "die Neurodermitis ist konstant da": "the neurodermatitis is constantly there",
            
            # Medications and treatments
            "NEUES ANTIHISTAMINIK": "New Antihistamin", 
            "Kieselerdeextrakt": "Silica extract", 
            "PLACEBO GRUPPE": "Placebo Group", 
            "PROToPIC": "PROTOPIC", 
            "PRTOPIC": "PROTOPIC", 
            "TACROLImUS": "TACROLIMUS", 
            "Tacrolimus": "TACROLIMUS",
            "Dupilumab": "DUPILUMAB", 
            "SANTOLEO ÖL": "SANTOLEO OIL", 
            "FUMARSÄURE": "FUMACIC ACID", 
            "Homöopathie": "Homeopathie",
            "HOMÖOPATHIE": "Homeopathie",
            "mISCHBESTRAHLUNg": "MIXED RADIATION", 
            "DEseNSIBILISIERUNG": "Desensitization",
            "Cetaphil": "CETAPHIL",
            "PEnICILLIN": "PENICILLIN", 
            "Penicilin": "PENICILLIN",  # Spelling correction
            "Paracethamol": "PARACETAMOL",  # Spelling correction
            "Paracetamol": "PARACETAMOL", 
            "Amoxicillin": "AMOXICILLIN", 
            "Aspirin": "ASPIRIN", 
            "Betäubungsmittel": "NARCOTICS",
            
            # Frequency/dosage
            "Weniger als einmal pro Woche": "Less than once a week", 
            "Mehr als 1x täglich": "More than 1 time a day", 
            "2-3x wöchentlich": "2-3 times a week", 
            "4-6x wöchentlich": "4-6 times a week",
            "2 - 3x wöchentlich": "2-3 times a week", 
            "4 - 7x wöchentlich": "4-7 times a week",
            "mehr als 7x pro Woche": ">7 times per week",
            "1x täglich": "Once per day", 
            "1x wöchentlich": "Once per week",
            
            # Administrative/status
            "ABGEGEbEn": "ABGEGEBEN", 
            "abgEgEBEN": "ABGEGEBEN", 
            "WURDE NICHT NACHGEREICHT": "WAS NOT SUBMITTED",  
            "NICHT ZU ERMITTELN": "CAN NOT BE DETERMINED",
            "KEINE REAKTION MEHR": "NO MORE RESPONSE",
            
            # Education levels
            "CH: Primarschule; D: Hauptschule, Volksschule": "CH: Primary school; D: Secondary school, elementary school",    
            "CH: Berufsausbildung; D: Berufsausbildung, Lehre": "CH: Vocational training; D: Vocational training, apprenticeship",
            "CH: Fachhochschule; D: Fachhochschule, Hochschule": "CH: University of applied sciences; D: University of applied sciences, college",
            "CH: Universität; D: Universität, Hochschule": "CH: University; D: University, college",
            "Hochschule, Fachhochschule, UniversitÃ¤t": "College, University of applied sciences, University",
            "Keine oder noch keine abgeschlossene Schulbildung": "No or no completed education",
            "CH: Sekundarschule; D: Realschule, Mittlere Reife": "CH: Secondary school; D: Secondary school, middle school",
            "Gymnasium, MaturitÃ¤tsschulen, Abitur, Fachabitur": "High school, Maturity schools, high school graduation, university entry exam"
        }
        
        # Common medical terms for spell checking
        self.medical_dictionary = [
            'male', 'female', 'yes', 'no', 'unknown', 'allergy', 'medication', 
            'diagnosis', 'treatment', 'symptom', 'examination', 'finding', 
            'therapy', 'surgery', 'complication', 'side_effect', 'dosage', 
            'frequency', 'severity', 'chronic', 'acute', 'normal', 'abnormal', 
            'positive', 'negative', 'psoriasis', 'dermatitis', 'eczema',
            'antihistamine', 'corticosteroid', 'topical', 'systemic',
            'penicillin', 'paracetamol', 'amoxicillin', 'aspirin', 'tacrolimus',
            'dupilumab', 'protopic', 'cetaphil', 'homeopathy', 'radiation',
            'desensitization', 'placebo', 'control', 'baseline', 'followup'
        ]
        
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
    
    def detect_spelling_mistakes(self, text):
        """Detect potential spelling mistakes and suggest corrections"""
        if not isinstance(text, str) or len(text.strip()) == 0:
            return text, False
        
        text_clean = text.strip().lower()
        
        # Check for mixed case patterns that might indicate typos
        mixed_case_pattern = re.compile(r'[a-z][A-Z]|[A-Z][a-z][A-Z]')
        has_mixed_case = bool(mixed_case_pattern.search(text))
        
        # Check for suspicious character combinations
        suspicious_patterns = [
            r'[ÃÄÖÜäöü]',  # UTF-8 encoding issues
            r'[A-Z]{2,}[a-z]{1,2}[A-Z]',  # Irregular capitalization
            r'\w{2,}[A-Z][a-z]*[A-Z]',  # CamelCase in medical terms
        ]
        
        is_suspicious = any(re.search(pattern, text) for pattern in suspicious_patterns)
        
        # Look for close matches in our transformations dictionary
        close_matches = difflib.get_close_matches(text, self.comprehensive_transformations.keys(), n=1, cutoff=0.8)
        
        if close_matches:
            suggested = self.comprehensive_transformations[close_matches[0]]
            self.spelling_suggestions.append({
                'original': text,
                'suggested': suggested,
                'confidence': difflib.SequenceMatcher(None, text, close_matches[0]).ratio()
            })
            return suggested, True
        
        # Check against medical dictionary
        words = re.findall(r'\b\w+\b', text_clean)
        corrected_words = []
        was_corrected = False
        
        for word in words:
            if len(word) > 3:  # Only check longer words
                close_medical = difflib.get_close_matches(word, self.medical_dictionary, n=1, cutoff=0.8)
                if close_medical and word != close_medical[0]:
                    corrected_words.append(close_medical[0])
                    was_corrected = True
                    self.spelling_suggestions.append({
                        'original': word,
                        'suggested': close_medical[0],
                        'confidence': difflib.SequenceMatcher(None, word, close_medical[0]).ratio(),
                        'type': 'medical_term'
                    })
                else:
                    corrected_words.append(word)
            else:
                corrected_words.append(word)
        
        if was_corrected:
            return ' '.join(corrected_words), True
        
        # Mark as suspicious if patterns detected but no correction made
        if is_suspicious or has_mixed_case:
            self.transformation_stats['suspicious_spellings'] += 1
            return text, False
            
        return text, False
    
    def apply_comprehensive_transformations(self, value):
        """Apply comprehensive German-English and medical transformations"""
        if not isinstance(value, str):
            return value
        
        original_value = value
        
        # First check for exact matches in our comprehensive dictionary
        if value in self.comprehensive_transformations:
            self.transformation_stats['transformations_applied'] += 1
            return self.comprehensive_transformations[value]
        
        # Check for partial matches (case-insensitive)
        value_lower = value.lower().strip()
        for key, replacement in self.comprehensive_transformations.items():
            if key.lower() == value_lower:
                self.transformation_stats['transformations_applied'] += 1
                return replacement
        
        # Check for spelling mistakes and corrections
        corrected_value, was_corrected = self.detect_spelling_mistakes(value)
        if was_corrected:
            self.transformation_stats['spelling_corrections'] += 1
            return corrected_value
        
        return value
    
    def process_csv(self):
        """Process the CSV file with comprehensive transformations and spell checking"""
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
            
            # Apply comprehensive transformations to data
            transformed_rows = []
            
            for row_idx, row in enumerate(input_rows):
                transformed_row = []
                for cell in row:
                    transformed_cell = self.apply_comprehensive_transformations(cell)
                    transformed_row.append(transformed_cell)
                transformed_rows.append(transformed_row)
                
                # Progress indicator for large datasets
                if (row_idx + 1) % 500 == 0:
                    print(f"🔄 Processed {row_idx + 1:,} rows...")
            
            print(f"✅ Applied {self.transformation_stats['transformations_applied']} comprehensive transformations")
            print(f"✅ Applied {self.transformation_stats['spelling_corrections']} spelling corrections")
            print(f"⚠️  Detected {self.transformation_stats['suspicious_spellings']} suspicious spellings")
            
            # Write processed file
            output_file = os.path.join(self.output_dir, 'prorad_enhanced_processed.csv')
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(mapped_headers)
                writer.writerows(transformed_rows)
            
            print(f"✅ Saved enhanced processed data to: {output_file}")
            
            # Generate comprehensive reports
            self.generate_reports(mapping_report, len(input_rows), len(headers))
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing CSV: {e}")
            return False
    
    def generate_reports(self, mapping_report, total_rows, total_columns):
        """Generate comprehensive processing and spelling reports"""
        
        # Main processing report
        report_file = os.path.join(self.output_dir, 'enhanced_processing_report.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'input_file': self.input_file,
                'total_columns': total_columns,
                'mapped_columns': len(mapping_report),
                'total_rows': total_rows,
                'transformation_stats': self.transformation_stats,
                'transformations_available': len(self.comprehensive_transformations),
                'column_mappings': mapping_report[:50],  # First 50 for brevity
                'transformation_dictionary_size': len(self.comprehensive_transformations)
            }, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Generated enhanced processing report: {report_file}")
        
        # Spelling suggestions report
        if self.spelling_suggestions:
            spelling_report_file = os.path.join(self.output_dir, 'spelling_corrections_report.json')
            with open(spelling_report_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'total_suggestions': len(self.spelling_suggestions),
                    'spelling_corrections': self.spelling_suggestions
                }, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Generated spelling corrections report: {spelling_report_file}")
        
        # Human-readable summary
        summary_file = os.path.join(self.output_dir, 'enhanced_processing_summary.txt')
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"Enhanced PRORAD Data Processing Summary\n")
            f.write(f"=" * 45 + "\n\n")
            f.write(f"Processed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Input file: {self.input_file}\n")
            f.write(f"Output file: prorad_enhanced_processed.csv\n\n")
            
            f.write(f"Dataset Statistics:\n")
            f.write(f"- Total columns: {total_columns:,}\n")
            f.write(f"- Mapped columns: {len(mapping_report):,}\n")
            f.write(f"- Total rows: {total_rows:,}\n\n")
            
            f.write(f"Transformation Statistics:\n")
            f.write(f"- Comprehensive transformations: {self.transformation_stats['transformations_applied']:,}\n")
            f.write(f"- Spelling corrections: {self.transformation_stats['spelling_corrections']:,}\n")
            f.write(f"- Suspicious spellings detected: {self.transformation_stats['suspicious_spellings']:,}\n")
            f.write(f"- Available transformation rules: {len(self.comprehensive_transformations):,}\n\n")
            
            if self.spelling_suggestions:
                f.write(f"Spelling Corrections Sample:\n")
                for i, suggestion in enumerate(self.spelling_suggestions[:10]):
                    f.write(f"- '{suggestion['original']}' → '{suggestion['suggested']}' (confidence: {suggestion.get('confidence', 0):.2f})\n")
                if len(self.spelling_suggestions) > 10:
                    f.write(f"... and {len(self.spelling_suggestions) - 10} more corrections\n")
                f.write("\n")
            
            f.write(f"Processing Status: ✅ COMPLETED WITH ENHANCEMENTS\n")
        
        print(f"✅ Generated enhanced summary report: {summary_file}")

def main():
    """Main execution function"""
    print("🔄 Starting Enhanced PRORAD CSV Processor with Spelling Detection...")
    print("=" * 65)
    
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
    
    # Initialize enhanced processor
    processor = EnhancedPRORADProcessor(input_file, vars_dict_file, output_dir)
    
    # Create output directory
    processor.ensure_output_dir()
    
    # Load variable mappings
    if not processor.load_var_mapping():
        return False
    
    print(f"📚 Loaded {len(processor.comprehensive_transformations)} comprehensive transformation rules")
    print(f"🔍 Medical dictionary contains {len(processor.medical_dictionary)} terms for spell checking")
    
    # Process the CSV file
    if not processor.process_csv():
        return False
    
    print("=" * 65)
    print("🎉 Enhanced PRORAD CSV processing completed successfully!")
    print(f"📁 Check enhanced outputs in: {output_dir}")
    print("🔍 Spelling corrections and comprehensive transformations applied!")
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)