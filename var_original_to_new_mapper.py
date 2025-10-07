#!/usr/bin/env python3
"""
VAR_ORIGINAL to VAR_NEW Column Mapper
Maps column names in input.csv from VAR_ORIGINAL to VAR_NEW using vars_dict.csv
"""

import csv
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VarOriginalToNewMapper:
    """Maps column names from VAR_ORIGINAL to VAR_NEW using vars_dict.csv"""
    
    def __init__(self):
        self.input_path = "C:/Users/tralucck/OneDrive/airflow/input"
        self.output_path = "C:/temp/airflow/var_new_mapped"
        self.setup_directories()
        
        # Load VAR_ORIGINAL -> VAR_NEW mapping
        self.var_mapping = {}
        self.load_var_mapping()
    
    def setup_directories(self):
        """Create output directory"""
        os.makedirs(self.output_path, exist_ok=True)
    
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
            
            # Show sample mappings
            logger.info("🔍 Sample mappings:")
            for i, (original, new) in enumerate(list(self.var_mapping.items())[:5]):
                logger.info(f"   {i+1}. {original} → {new}")
            
        except Exception as e:
            logger.error(f"❌ Error loading var mapping: {e}")
            self.var_mapping = {}
    
    def map_input_csv_columns(self, sample_size=None):
        """Map column names in input.csv from VAR_ORIGINAL to VAR_NEW"""
        
        input_file = os.path.join(self.input_path, 'input.csv')
        
        if not os.path.exists(input_file):
            logger.error(f"❌ Input file not found: {input_file}")
            return None
        
        # Get file info
        file_size = os.path.getsize(input_file)
        logger.info(f"📁 Processing: {input_file}")
        logger.info(f"📊 File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        
        try:
            # Read headers and determine mapping
            with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                original_headers = reader.fieldnames
                
                logger.info(f"📋 Total columns in input.csv: {len(original_headers)}")
                
                # Create mapping for headers
                mapped_headers = []
                mapping_stats = {'mapped': 0, 'unmapped': 0}
                header_mapping = {}
                
                for header in original_headers:
                    if header in self.var_mapping:
                        new_header = self.var_mapping[header]
                        mapped_headers.append(new_header)
                        header_mapping[header] = new_header
                        mapping_stats['mapped'] += 1
                    else:
                        mapped_headers.append(header)  # Keep original if no mapping
                        header_mapping[header] = header
                        mapping_stats['unmapped'] += 1
                
                logger.info(f"🗺️ Mapping Results:")
                logger.info(f"   • Mapped columns: {mapping_stats['mapped']}")
                logger.info(f"   • Unmapped columns: {mapping_stats['unmapped']}")
                logger.info(f"   • Mapping rate: {mapping_stats['mapped']/len(original_headers)*100:.1f}%")
                
                # Show sample mappings applied
                logger.info("🔍 Sample column mappings applied:")
                mapped_count = 0
                for original, new in header_mapping.items():
                    if original != new and mapped_count < 10:  # Show first 10 actual mappings
                        logger.info(f"   {mapped_count+1:2d}. {original} → {new}")
                        mapped_count += 1
                
                if mapped_count == 0:
                    logger.warning("⚠️ No column mappings were applied - check if VAR_ORIGINAL names match input.csv headers")
                
                # Process data with sample if requested
                if sample_size:
                    output_file = os.path.join(self.output_path, f'input_mapped_var_new_sample_{sample_size}.csv')
                    self.create_mapped_sample(input_file, original_headers, mapped_headers, header_mapping, sample_size)
                else:
                    output_file = os.path.join(self.output_path, 'input_mapped_var_new_full.csv')
                    self.create_mapped_full(input_file, original_headers, mapped_headers, header_mapping)
                
                return output_file, header_mapping, mapping_stats
                
        except Exception as e:
            logger.error(f"❌ Error processing input.csv: {e}")
            return None, {}, {}
    
    def create_mapped_sample(self, input_file, original_headers, mapped_headers, header_mapping, sample_size):
        """Create sample mapped CSV file"""
        output_file = os.path.join(self.output_path, f'input_mapped_var_new_sample_{sample_size}.csv')
        
        logger.info(f"📝 Creating mapped sample ({sample_size} rows): {output_file}")
        
        with open(input_file, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=mapped_headers)
                writer.writeheader()
                
                # Write sample rows with mapped headers
                for i, row in enumerate(reader):
                    if i >= sample_size:
                        break
                    
                    mapped_row = {}
                    for old_header, new_header in zip(original_headers, mapped_headers):
                        mapped_row[new_header] = row.get(old_header, '')
                    writer.writerow(mapped_row)
        
        logger.info(f"✅ Sample mapped file created: {output_file}")
        return output_file
    
    def create_mapped_full(self, input_file, original_headers, mapped_headers, header_mapping):
        """Create full mapped CSV file (for smaller files)"""
        output_file = os.path.join(self.output_path, 'input_mapped_var_new_full.csv')
        
        logger.info(f"📝 Creating full mapped file: {output_file}")
        logger.warning("⚠️ This will process the entire large file - may take time...")
        
        try:
            with open(input_file, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                
                with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=mapped_headers)
                    writer.writeheader()
                    
                    # Process in batches for large files
                    batch_size = 1000
                    batch_count = 0
                    
                    batch_rows = []
                    for row in reader:
                        mapped_row = {}
                        for old_header, new_header in zip(original_headers, mapped_headers):
                            mapped_row[new_header] = row.get(old_header, '')
                        batch_rows.append(mapped_row)
                        
                        if len(batch_rows) >= batch_size:
                            writer.writerows(batch_rows)
                            batch_count += 1
                            logger.info(f"   Processed batch {batch_count} ({batch_count * batch_size:,} rows)")
                            batch_rows = []
                    
                    # Write remaining rows
                    if batch_rows:
                        writer.writerows(batch_rows)
                        batch_count += 1
                        logger.info(f"   Final batch {batch_count} ({len(batch_rows)} rows)")
            
            logger.info(f"✅ Full mapped file created: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"❌ Error creating full mapped file: {e}")
            return None
    
    def create_mapping_report(self, header_mapping, mapping_stats, output_file):
        """Create detailed mapping report"""
        
        report_file = os.path.join(self.output_path, 'var_original_to_var_new_mapping_report.txt')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("VAR_ORIGINAL → VAR_NEW COLUMN MAPPING REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("📁 INPUT FILES:\n")
            f.write("   • Source data: C:/Users/tralucck/OneDrive/airflow/input/input.csv\n")
            f.write("   • Mapping dict: C:/Users/tralucck/OneDrive/airflow/input/vars_dict.csv\n")
            f.write(f"   • Output file: {output_file}\n\n")
            
            f.write("📊 MAPPING STATISTICS:\n")
            f.write(f"   • Total columns: {mapping_stats['mapped'] + mapping_stats['unmapped']}\n")
            f.write(f"   • Successfully mapped: {mapping_stats['mapped']}\n")
            f.write(f"   • Unmapped (kept original): {mapping_stats['unmapped']}\n")
            f.write(f"   • Mapping success rate: {mapping_stats['mapped']/(mapping_stats['mapped'] + mapping_stats['unmapped'])*100:.1f}%\n\n")
            
            f.write("🗺️ COLUMN MAPPINGS APPLIED:\n")
            f.write("-" * 60 + "\n")
            f.write(f"{'VAR_ORIGINAL':<30} → {'VAR_NEW':<30}\n")
            f.write("-" * 60 + "\n")
            
            # Show actual mappings (not identity mappings)
            actual_mappings = [(orig, new) for orig, new in header_mapping.items() if orig != new]
            
            if actual_mappings:
                for original, new in actual_mappings[:50]:  # Show first 50 mappings
                    f.write(f"{original:<30} → {new:<30}\n")
                
                if len(actual_mappings) > 50:
                    f.write(f"... and {len(actual_mappings) - 50} more mappings\n")
            else:
                f.write("No column mappings were applied.\n")
                f.write("This might indicate that column names in input.csv don't match VAR_ORIGINAL values.\n")
            
            f.write("\n" + "="*80 + "\n")
        
        logger.info(f"📄 Mapping report created: {report_file}")
        return report_file
    
    def run_var_new_mapping(self, sample_size=1000):
        """Run the complete VAR_ORIGINAL → VAR_NEW mapping process"""
        
        logger.info("🚀 Starting VAR_ORIGINAL → VAR_NEW column mapping...")
        
        # Map the input CSV
        result = self.map_input_csv_columns(sample_size=sample_size)
        
        if result[0]:  # If successful
            output_file, header_mapping, mapping_stats = result
            
            # Create mapping report
            report_file = self.create_mapping_report(header_mapping, mapping_stats, output_file)
            
            # Print summary
            print("\n" + "="*80)
            print("🎯 VAR_ORIGINAL → VAR_NEW MAPPING COMPLETED!")
            print("="*80)
            print(f"📁 Input file: C:/Users/tralucck/OneDrive/airflow/input/input.csv")
            print(f"📁 Mapping dict: C:/Users/tralucck/OneDrive/airflow/input/vars_dict.csv")
            print(f"💾 Output file: {output_file}")
            print(f"📄 Report: {report_file}")
            print("")
            print(f"📊 RESULTS:")
            print(f"   • Columns mapped: {mapping_stats['mapped']:,}")
            print(f"   • Columns unmapped: {mapping_stats['unmapped']:,}")
            print(f"   • Success rate: {mapping_stats['mapped']/(mapping_stats['mapped'] + mapping_stats['unmapped'])*100:.1f}%")
            print("="*80)
            
            return output_file, report_file
        else:
            logger.error("❌ Mapping process failed")
            return None, None

def main():
    """Main function to run VAR_ORIGINAL → VAR_NEW mapping"""
    
    print("🗺️ VAR_ORIGINAL → VAR_NEW Column Mapper")
    print("Mapping column names in input.csv using vars_dict.csv")
    
    # Initialize mapper
    mapper = VarOriginalToNewMapper()
    
    # Run mapping with sample (change sample_size=None for full file)
    output_file, report_file = mapper.run_var_new_mapping(sample_size=5000)  # 5000 row sample
    
    if output_file:
        print(f"\n✅ SUCCESS! VAR_NEW mapped dataset created!")
        print(f"📊 Check the output files for your mapped data with VAR_NEW column names.")
    else:
        print(f"\n❌ FAILED! Check the error messages above.")

if __name__ == "__main__":
    main()