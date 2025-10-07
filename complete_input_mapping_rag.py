#!/usr/bin/env python3
"""
Complete Input Data Mapping and RAG Analysis System
Final integrated system that processes all input data with comprehensive mapping and RAG visualization
"""

import json
import os
import logging
import csv
from datetime import datetime
import statistics

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteDataMappingRAGSystem:
    """Complete system for input data mapping and RAG analysis"""
    
    def __init__(self):
        self.base_path = "C:/temp/airflow"
        self.input_path = "C:/Users/tralucck/OneDrive/airflow/input"  
        self.output_path = os.path.join(self.base_path, "complete_rag_analysis")
        self.setup_directories()
        
        # Load comprehensive variable mappings
        self.vars_mappings = {}
        self.load_vars_dictionary()
        
        # Medical data mappings from existing system
        self.key_hem_meanings = {
            'leuko': "Leukocytes (White Blood Cells)",
            'heamglob': "Hemoglobin Level", 
            'ery': "Erythrocytes (Red Blood Cells)",
            'heamkrit': "Hematocrit",
            'lympho': "Lymphocytes",
            'neutro': "Neutrophils",
            'mono': "Monocytes",
            'eos': "Eosinophils",
            'baso': "Basophils",
            'ige_gesamt': "Total IgE Level"
        }
        
        # Therapy mappings from existing system  
        self.therapy_meanings = {
            "thraebasis_": "Basic Therapy Protocol",
            "thraelokkort": "Topical Corticosteroids",
            "thraelokpime": "Pimecrolimus Cream",
            "thraeloktacr": "Tacrolimus Ointment (Protopic)",
            "thraebalneo": "Balneo Phototherapy",
            "thraelicht": "Light Therapy",
            "thraepuva": "PUVA Therapy",
            "thraeuva": "UVA Therapy",
            "thraeuvab": "UVA/UVB Combined Therapy",
            "thraeuvb": "UVB Therapy",
            "thrspkun": "Unknown Spectrum Therapy",
            "thraekort": "Systemic Corticosteroids",
            "thraecycl": "Cyclosporine",
            "thraeazat": "Azathioprine",
            "thraemtx": "Methotrexate",
            "thraemmf": "Mycophenolate Mofetil",
            "thraea": "Other Treatment",
            "thraeaw": "Other Systemic Treatment",
            "untersuchdat": "Visit Date"
        }
        
        # Extended RAG thresholds for comprehensive analysis
        self.rag_thresholds = {
            'completeness': {'green': 0.9, 'amber': 0.7},  
            'quality': {'green': 0.85, 'amber': 0.6},      
            'compliance': {'green': 0.95, 'amber': 0.8},   
            'hemoglobin': {'green': 12.0, 'amber': 8.0},   
            'treatment_response': {'green': 70, 'amber': 40},
            'data_consistency': {'green': 0.95, 'amber': 0.85},
            'missing_critical': {'green': 0.02, 'amber': 0.05}  # Lower is better
        }
        
        # RAG colors for text output
        self.rag_colors_text = {
            'Green': '🟢',
            'Amber': '🟡', 
            'Red': '🔴',
            'Gray': '⚪'
        }
    
    def setup_directories(self):
        """Create comprehensive directory structure"""
        directories = [
            self.output_path,
            os.path.join(self.output_path, 'mapped_data'),
            os.path.join(self.output_path, 'rag_reports'),
            os.path.join(self.output_path, 'rag_tables'),
            os.path.join(self.output_path, 'visualizations'),
            os.path.join(self.output_path, 'summary')
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def load_vars_dictionary(self):
        """Load comprehensive variable mappings from vars_dict.csv"""
        try:
            vars_dict_path = os.path.join(self.input_path, 'vars_dict.csv')
            if os.path.exists(vars_dict_path):
                logger.info("📚 Loading comprehensive variable dictionary...")
                
                with open(vars_dict_path, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        original_var = row.get('VAR_ORIGINAL', '')
                        meaning = row.get('MEANING', '')
                        meaning_details = row.get('MEANING_DETAILS', '')
                        var_type = row.get('TYPE', '')
                        
                        if original_var and meaning:
                            # Create comprehensive mapping with type info
                            full_meaning = meaning
                            if meaning_details and meaning_details.strip():
                                full_meaning += f" ({meaning_details})"
                            if var_type:
                                full_meaning += f" [{var_type}]"
                            
                            self.vars_mappings[original_var] = full_meaning
                
                logger.info(f"✅ Loaded {len(self.vars_mappings)} comprehensive variable mappings")
            else:
                logger.warning("⚠️ vars_dict.csv not found - using limited mappings")
                
        except Exception as e:
            logger.error(f"❌ Error loading variable dictionary: {e}")
    
    def map_all_columns_comprehensive(self, headers):
        """Apply comprehensive column mapping using all available dictionaries"""
        logger.info("🗺️ Applying comprehensive column mapping...")
        
        mapping_applied = {}
        mapping_sources = {}
        
        # Priority 1: vars_dict mappings (most comprehensive)
        for original_col in headers:
            if original_col in self.vars_mappings:
                new_name = self.vars_mappings[original_col]
                mapping_applied[original_col] = new_name
                mapping_sources[original_col] = 'vars_dict'
        
        # Priority 2: Hemoglobin mappings
        for original_col in headers:
            if original_col in self.key_hem_meanings and original_col not in mapping_applied:
                new_name = self.key_hem_meanings[original_col]
                mapping_applied[original_col] = new_name
                mapping_sources[original_col] = 'hemoglobin'
        
        # Priority 3: Therapy mappings
        for original_col in headers:
            if original_col in self.therapy_meanings and original_col not in mapping_applied:
                new_name = self.therapy_meanings[original_col]
                mapping_applied[original_col] = new_name
                mapping_sources[original_col] = 'therapy'
        
        # Create summary by source
        source_summary = {}
        for source in mapping_sources.values():
            source_summary[source] = source_summary.get(source, 0) + 1
        
        logger.info(f"📝 Applied {len(mapping_applied)} column mappings:")
        for source, count in source_summary.items():
            logger.info(f"   • {source}: {count} mappings")
        
        return mapping_applied, mapping_sources
    
    def calculate_rag_status_advanced(self, value, metric_type):
        """Calculate RAG status with advanced thresholds"""
        if value is None or value == '':
            return 'Gray'  # Missing data
        
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return 'Gray'  # Non-numeric data
        
        thresholds = self.rag_thresholds.get(metric_type, {'green': 0.8, 'amber': 0.5})
        
        # For "lower is better" metrics like missing_critical
        if metric_type in ['missing_critical']:
            if numeric_value <= thresholds['green']:
                return 'Green'
            elif numeric_value <= thresholds['amber']:
                return 'Amber'
            else:
                return 'Red'
        else:
            # For "higher is better" metrics
            if numeric_value >= thresholds['green']:
                return 'Green'
            elif numeric_value >= thresholds['amber']:
                return 'Amber'
            else:
                return 'Red'
    
    def analyze_data_quality_comprehensive(self, file_path):
        """Comprehensive data quality analysis with advanced metrics"""
        logger.info(f"🔍 Comprehensive quality analysis: {os.path.basename(file_path)}")
        
        quality_metrics = {}
        
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                headers = reader.fieldnames
                rows = list(reader)
                
                total_rows = len(rows)
                total_cells = total_rows * len(headers) if headers else 0
                
                if total_rows == 0:
                    logger.warning(f"⚠️ No data rows found in {file_path}")
                    return {}
                
                # 1. Column-wise completeness analysis
                for col in headers:
                    non_empty_count = sum(1 for row in rows if row.get(col, '').strip())
                    completeness = non_empty_count / total_rows
                    
                    quality_metrics[f"{col}_completeness"] = {
                        'value': completeness,
                        'rag_status': self.calculate_rag_status_advanced(completeness, 'completeness'),
                        'metric_type': 'completeness'
                    }
                
                # 2. Overall data completeness
                non_empty_cells = sum(1 for row in rows for col in headers if row.get(col, '').strip())
                overall_completeness = non_empty_cells / total_cells if total_cells > 0 else 0
                
                quality_metrics['overall_completeness'] = {
                    'value': overall_completeness,
                    'rag_status': self.calculate_rag_status_advanced(overall_completeness, 'completeness'),
                    'metric_type': 'completeness'
                }
                
                # 3. Critical fields missing rate
                critical_fields = [col for col in headers if any(term in col.lower() 
                                 for term in ['id', 'date', 'patient', 'subject'])]
                if critical_fields:
                    missing_critical = 0
                    for field in critical_fields:
                        missing_critical += sum(1 for row in rows if not row.get(field, '').strip())
                    
                    missing_critical_rate = missing_critical / (len(critical_fields) * total_rows)
                    quality_metrics['missing_critical_fields'] = {
                        'value': missing_critical_rate,
                        'rag_status': self.calculate_rag_status_advanced(missing_critical_rate, 'missing_critical'),
                        'metric_type': 'missing_critical'
                    }
                
                # 4. Medical metrics analysis
                self.analyze_medical_metrics(headers, rows, quality_metrics)
                
                # 5. Data consistency analysis
                self.analyze_data_consistency(headers, rows, quality_metrics)
                
                logger.info(f"📊 Analyzed {total_rows} rows, {len(headers)} columns, {len(quality_metrics)} metrics")
                
        except Exception as e:
            logger.error(f"❌ Error in comprehensive analysis of {file_path}: {e}")
            return {}
        
        return quality_metrics
    
    def analyze_medical_metrics(self, headers, rows, quality_metrics):
        """Analyze medical-specific metrics"""
        # Hemoglobin analysis
        hb_columns = [col for col in headers if any(term in col.lower() 
                     for term in ['hemoglobin', 'heamglob', 'hb'])]
        if hb_columns:
            hb_col = hb_columns[0]
            hb_values = []
            for row in rows:
                try:
                    val = float(row.get(hb_col, ''))
                    if 0 < val < 50:  # Valid hemoglobin range
                        hb_values.append(val)
                except (ValueError, TypeError):
                    continue
            
            if hb_values:
                avg_hb = statistics.mean(hb_values)
                quality_metrics['avg_hemoglobin'] = {
                    'value': avg_hb,
                    'rag_status': self.calculate_rag_status_advanced(avg_hb, 'hemoglobin'),
                    'metric_type': 'hemoglobin'
                }
                
                # Hemoglobin variability
                if len(hb_values) > 1:
                    hb_stdev = statistics.stdev(hb_values)
                    quality_metrics['hemoglobin_variability'] = {
                        'value': hb_stdev,
                        'rag_status': self.calculate_rag_status_advanced(hb_stdev, 'quality'),
                        'metric_type': 'quality'
                    }
        
        # Treatment response analysis
        treatment_columns = [col for col in headers if any(term in col.lower() 
                           for term in ['response', 'improvement', 'efficacy'])]
        if treatment_columns:
            for treat_col in treatment_columns[:1]:  # Analyze first treatment column
                treat_values = []
                for row in rows:
                    try:
                        val = float(row.get(treat_col, ''))
                        if 0 <= val <= 100:  # Percentage range
                            treat_values.append(val)
                    except (ValueError, TypeError):
                        continue
                
                if treat_values:
                    avg_response = statistics.mean(treat_values)
                    quality_metrics['avg_treatment_response'] = {
                        'value': avg_response,
                        'rag_status': self.calculate_rag_status_advanced(avg_response, 'treatment_response'),
                        'metric_type': 'treatment_response'
                    }
    
    def analyze_data_consistency(self, headers, rows, quality_metrics):
        """Analyze data consistency metrics"""
        # Check for duplicate records based on ID fields
        id_columns = [col for col in headers if any(term in col.lower() 
                     for term in ['id', 'patient', 'subject', 'participant'])]
        
        if id_columns:
            id_col = id_columns[0]
            id_values = [row.get(id_col, '') for row in rows if row.get(id_col, '').strip()]
            
            if id_values:
                unique_ids = len(set(id_values))
                total_ids = len(id_values)
                consistency_rate = unique_ids / total_ids if total_ids > 0 else 0
                
                quality_metrics['id_consistency'] = {
                    'value': consistency_rate,
                    'rag_status': self.calculate_rag_status_advanced(consistency_rate, 'data_consistency'),
                    'metric_type': 'data_consistency'
                }
    
    def create_comprehensive_rag_dashboard(self, quality_metrics, filename, mapping_applied, mapping_sources):
        """Create comprehensive RAG dashboard with advanced visualizations"""
        dashboard_lines = []
        
        # Header
        dashboard_lines.append("\n" + "="*100)
        dashboard_lines.append("🎯 COMPREHENSIVE INPUT DATA MAPPING & RAG ANALYSIS DASHBOARD")
        dashboard_lines.append("="*100)
        dashboard_lines.append(f"📄 File: {filename}")
        dashboard_lines.append(f"⏰ Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        dashboard_lines.append("="*100)
        
        # Summary statistics
        rag_counts = {'Green': 0, 'Amber': 0, 'Red': 0, 'Gray': 0}
        metric_types = {}
        
        for metric, data in quality_metrics.items():
            rag_counts[data['rag_status']] += 1
            metric_type = data.get('metric_type', 'other')
            metric_types[metric_type] = metric_types.get(metric_type, 0) + 1
        
        total_metrics = sum(rag_counts.values())
        
        # RAG Distribution with enhanced visualization
        dashboard_lines.append("\n🚦 RAG STATUS OVERVIEW")
        dashboard_lines.append("="*60)
        dashboard_lines.append("     ┌─────────────────────────────────────┐")
        
        for status, count in rag_counts.items():
            if total_metrics > 0:
                percentage = (count / total_metrics) * 100
                blocks = int(percentage / 2.5)  # Each block = 2.5%
                visual_blocks = '█' * blocks + '░' * (40 - blocks)
                symbol = self.rag_colors_text[status]
                dashboard_lines.append(f"{symbol} {status:6} │{visual_blocks}│ {count:3d} ({percentage:5.1f}%)")
        
        dashboard_lines.append("     └─────────────────────────────────────┘")
        dashboard_lines.append(f"     Total metrics analyzed: {total_metrics}")
        
        # Metric types breakdown
        if metric_types:
            dashboard_lines.append("\n📊 METRICS BY CATEGORY")
            dashboard_lines.append("="*40)
            for mtype, count in metric_types.items():
                dashboard_lines.append(f"  • {mtype.replace('_', ' ').title()}: {count} metrics")
        
        # Column mapping summary with sources
        if mapping_applied:
            dashboard_lines.append("\n🗺️  COMPREHENSIVE COLUMN MAPPING")
            dashboard_lines.append("="*60)
            
            # Summary by source
            source_counts = {}
            for source in mapping_sources.values():
                source_counts[source] = source_counts.get(source, 0) + 1
            
            dashboard_lines.append(f"Total columns mapped: {len(mapping_applied)}")
            dashboard_lines.append("Mapping sources:")
            for source, count in source_counts.items():
                dashboard_lines.append(f"  • {source.replace('_', ' ').title()}: {count} mappings")
            
            # Show sample mappings
            dashboard_lines.append("\nSample mappings:")
            for i, (original, mapped) in enumerate(mapping_applied.items()):
                if i < 5:  # Show first 5 mappings
                    source = mapping_sources.get(original, 'unknown')
                    dashboard_lines.append(f"  {i+1}. {original} → {mapped} [{source}]")
                elif i == 5:
                    remaining = len(mapping_applied) - 5
                    dashboard_lines.append(f"  ... and {remaining} more mappings")
                    break
        
        # Quality metrics heatmap by category
        self.add_quality_heatmap_to_dashboard(dashboard_lines, quality_metrics)
        
        # Key insights and recommendations
        self.add_insights_and_recommendations(dashboard_lines, quality_metrics, rag_counts, total_metrics)
        
        # RAG thresholds reference
        dashboard_lines.append("\n📋 RAG THRESHOLDS REFERENCE")
        dashboard_lines.append("="*50)
        for metric_type, thresholds in self.rag_thresholds.items():
            dashboard_lines.append(f"{metric_type.replace('_', ' ').title()}:")
            dashboard_lines.append(f"  🟢 Green: ≥{thresholds['green']}")
            dashboard_lines.append(f"  🟡 Amber: ≥{thresholds['amber']}")
        
        dashboard_lines.append("\n" + "="*100)
        
        return '\n'.join(dashboard_lines)
    
    def add_quality_heatmap_to_dashboard(self, dashboard_lines, quality_metrics):
        """Add quality heatmap section to dashboard"""
        dashboard_lines.append("\n📈 QUALITY METRICS HEATMAP")
        dashboard_lines.append("="*70)
        
        # Group metrics by type
        completeness_metrics = {k: v for k, v in quality_metrics.items() 
                              if v.get('metric_type') == 'completeness'}
        medical_metrics = {k: v for k, v in quality_metrics.items() 
                          if v.get('metric_type') in ['hemoglobin', 'treatment_response']}
        consistency_metrics = {k: v for k, v in quality_metrics.items() 
                             if v.get('metric_type') in ['data_consistency', 'missing_critical']}
        
        if completeness_metrics:
            dashboard_lines.append("\n📊 COMPLETENESS METRICS:")
            dashboard_lines.append("─" * 50)
            for metric, data in list(completeness_metrics.items())[:10]:  # Show top 10
                symbol = self.rag_colors_text[data['rag_status']]
                metric_name = metric.replace('_completeness', '').replace('_', ' ').title()
                value = data['value']
                
                bar_length = int(value * 20) if isinstance(value, (int, float)) else 0
                bar = '█' * bar_length + '░' * (20 - bar_length)
                
                dashboard_lines.append(f"{symbol} {metric_name:25} |{bar}| {value:6.1%}")
        
        if medical_metrics:
            dashboard_lines.append("\n🩺 MEDICAL METRICS:")
            dashboard_lines.append("─" * 30)
            for metric, data in medical_metrics.items():
                symbol = self.rag_colors_text[data['rag_status']]
                metric_name = metric.replace('_', ' ').title()
                value = data['value']
                
                if isinstance(value, float):
                    dashboard_lines.append(f"{symbol} {metric_name:25} {value:8.2f}")
                else:
                    dashboard_lines.append(f"{symbol} {metric_name:25} {value}")
        
        if consistency_metrics:
            dashboard_lines.append("\n🔍 CONSISTENCY METRICS:")
            dashboard_lines.append("─" * 35)
            for metric, data in consistency_metrics.items():
                symbol = self.rag_colors_text[data['rag_status']]
                metric_name = metric.replace('_', ' ').title()
                value = data['value']
                
                if isinstance(value, float):
                    if 'missing' in metric:
                        dashboard_lines.append(f"{symbol} {metric_name:25} {value:6.1%}")
                    else:
                        dashboard_lines.append(f"{symbol} {metric_name:25} {value:6.1%}")
                else:
                    dashboard_lines.append(f"{symbol} {metric_name:25} {value}")
    
    def add_insights_and_recommendations(self, dashboard_lines, quality_metrics, rag_counts, total_metrics):
        """Add insights and recommendations section"""
        dashboard_lines.append("\n🔍 KEY INSIGHTS & RECOMMENDATIONS")
        dashboard_lines.append("="*60)
        
        if total_metrics > 0:
            green_pct = (rag_counts['Green'] / total_metrics) * 100
            red_pct = (rag_counts['Red'] / total_metrics) * 100
            amber_pct = (rag_counts['Amber'] / total_metrics) * 100
            
            # Overall assessment
            if green_pct >= 80:
                dashboard_lines.append("✅ EXCELLENT: Data quality is outstanding")
                dashboard_lines.append("   Most metrics are performing above target thresholds")
            elif green_pct >= 60:
                dashboard_lines.append("🟡 GOOD: Data quality is acceptable")
                dashboard_lines.append("   Majority of metrics are performing well")
            else:
                dashboard_lines.append("🔴 ATTENTION NEEDED: Data quality requires improvement")
                dashboard_lines.append("   Many metrics are below acceptable thresholds")
            
            # Specific recommendations
            if red_pct > 25:
                dashboard_lines.append("\n⚠️  HIGH PRIORITY ACTIONS:")
                red_metrics = [k for k, v in quality_metrics.items() if v['rag_status'] == 'Red']
                for metric in red_metrics[:3]:
                    metric_name = metric.replace('_', ' ').title()
                    dashboard_lines.append(f"   • Address {metric_name}")
            
            if amber_pct > 30:
                dashboard_lines.append("\n🟡 MONITORING RECOMMENDED:")
                amber_metrics = [k for k, v in quality_metrics.items() if v['rag_status'] == 'Amber']
                for metric in amber_metrics[:3]:
                    metric_name = metric.replace('_', ' ').title()
                    dashboard_lines.append(f"   • Monitor {metric_name}")
            
            # Best performing metrics
            green_metrics = [k for k, v in quality_metrics.items() if v['rag_status'] == 'Green']
            if green_metrics:
                dashboard_lines.append("\n🟢 WELL PERFORMING:")
                for metric in green_metrics[:3]:
                    metric_name = metric.replace('_', ' ').title()
                    dashboard_lines.append(f"   • {metric_name}")
    
    def process_all_input_files(self):
        """Process all input files with comprehensive analysis"""
        logger.info("🚀 Starting complete input data mapping and RAG analysis...")
        
        results = {}
        overall_summary = {
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_columns_mapped': 0,
            'total_metrics_analyzed': 0,
            'overall_rag_distribution': {'Green': 0, 'Amber': 0, 'Red': 0, 'Gray': 0}
        }
        
        # Process each CSV file in input directory
        for filename in os.listdir(self.input_path):
            if filename.endswith('.csv') and filename not in ['input.csv']:  # Skip large input.csv
                file_path = os.path.join(self.input_path, filename)
                overall_summary['total_files'] += 1
                
                try:
                    logger.info(f"📁 Processing: {filename}")
                    
                    # Get headers for mapping
                    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        headers = reader.fieldnames
                    
                    if not headers:
                        logger.warning(f"⚠️ No headers found in {filename}")
                        continue
                    
                    # Apply comprehensive column mapping
                    mapping_applied, mapping_sources = self.map_all_columns_comprehensive(headers)
                    
                    # Comprehensive data quality analysis
                    quality_metrics = self.analyze_data_quality_comprehensive(file_path)
                    
                    # Create comprehensive RAG dashboard
                    dashboard = self.create_comprehensive_rag_dashboard(
                        quality_metrics, filename, mapping_applied, mapping_sources
                    )
                    
                    # Save dashboard
                    dashboard_path = os.path.join(self.output_path, 'rag_reports', 
                                                f'comprehensive_rag_{filename.replace(".csv", "")}.txt')
                    with open(dashboard_path, 'w', encoding='utf-8') as f:
                        f.write(dashboard)
                    
                    # Create mapped CSV
                    mapped_file_path = self.create_mapped_csv_comprehensive(file_path, mapping_applied)
                    
                    # Update overall summary
                    overall_summary['successful_files'] += 1
                    overall_summary['total_columns_mapped'] += len(mapping_applied)
                    overall_summary['total_metrics_analyzed'] += len(quality_metrics)
                    
                    for metric, data in quality_metrics.items():
                        overall_summary['overall_rag_distribution'][data['rag_status']] += 1
                    
                    # Store results
                    results[filename] = {
                        'status': 'success',
                        'total_columns': len(headers),
                        'mapped_columns': len(mapping_applied),
                        'mapping_sources': dict(Counter(mapping_sources.values())),
                        'quality_metrics_count': len(quality_metrics),
                        'dashboard_path': dashboard_path,
                        'mapped_file_path': mapped_file_path,
                        'rag_distribution': {
                            status: sum(1 for data in quality_metrics.values() 
                                      if data['rag_status'] == status)
                            for status in ['Green', 'Amber', 'Red', 'Gray']
                        }
                    }
                    
                    logger.info(f"✅ Successfully processed {filename}")
                    
                    # Print dashboard to console for immediate review
                    print(dashboard)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing {filename}: {e}")
                    overall_summary['failed_files'] += 1
                    results[filename] = {'status': 'failed', 'error': str(e)}
        
        # Generate final summary
        self.generate_final_summary_report(results, overall_summary)
        
        return results, overall_summary
    
    def create_mapped_csv_comprehensive(self, input_file, mapping_applied):
        """Create CSV with comprehensive mapped column names"""
        try:
            filename = os.path.basename(input_file)
            output_file = os.path.join(self.output_path, 'mapped_data', f'comprehensive_mapped_{filename}')
            
            with open(input_file, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                
                # Map headers
                new_headers = []
                for header in reader.fieldnames:
                    if header in mapping_applied:
                        new_headers.append(mapping_applied[header])
                    else:
                        new_headers.append(header)
                
                with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=new_headers)
                    writer.writeheader()
                    
                    # Write data with mapped headers
                    for row in reader:
                        mapped_row = {}
                        for old_header, new_header in zip(reader.fieldnames, new_headers):
                            mapped_row[new_header] = row[old_header]
                        writer.writerow(mapped_row)
            
            logger.info(f"💾 Comprehensive mapped CSV: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"❌ Error creating comprehensive mapped CSV: {e}")
            return None
    
    def generate_final_summary_report(self, results, overall_summary):
        """Generate final comprehensive summary report"""
        logger.info("📋 Generating final comprehensive summary...")
        
        # Create detailed JSON report
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'system_info': {
                'total_variable_mappings': len(self.vars_mappings),
                'hemoglobin_mappings': len(self.key_hem_meanings),
                'therapy_mappings': len(self.therapy_meanings),
                'rag_thresholds': self.rag_thresholds
            },
            'overall_summary': overall_summary,
            'file_results': results
        }
        
        report_path = os.path.join(self.output_path, 'summary', 'final_comprehensive_report.json')
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        # Create executive summary text
        exec_summary = self.create_executive_summary(overall_summary, results)
        exec_summary_path = os.path.join(self.output_path, 'summary', 'executive_summary.txt')
        with open(exec_summary_path, 'w', encoding='utf-8') as f:
            f.write(exec_summary)
        
        logger.info(f"📄 Final reports saved:")
        logger.info(f"   • JSON: {report_path}")
        logger.info(f"   • Executive Summary: {exec_summary_path}")
        
        # Print executive summary
        print(exec_summary)
    
    def create_executive_summary(self, overall_summary, results):
        """Create executive summary text"""
        lines = []
        lines.append("\n" + "="*100)
        lines.append("📈 EXECUTIVE SUMMARY - COMPLETE INPUT DATA MAPPING & RAG ANALYSIS")
        lines.append("="*100)
        lines.append(f"⏰ Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # High-level metrics
        lines.append("🎯 KEY METRICS")
        lines.append("="*40)
        lines.append(f"📁 Files analyzed: {overall_summary['total_files']}")
        lines.append(f"✅ Successful: {overall_summary['successful_files']}")
        lines.append(f"❌ Failed: {overall_summary['failed_files']}")
        lines.append(f"🗺️ Total columns mapped: {overall_summary['total_columns_mapped']}")
        lines.append(f"📊 Total metrics analyzed: {overall_summary['total_metrics_analyzed']}")
        
        # Overall RAG distribution
        total_rag = sum(overall_summary['overall_rag_distribution'].values())
        if total_rag > 0:
            lines.append("\n🚦 OVERALL RAG DISTRIBUTION")
            lines.append("="*40)
            for status, count in overall_summary['overall_rag_distribution'].items():
                percentage = (count / total_rag) * 100
                symbol = self.rag_colors_text[status]
                lines.append(f"{symbol} {status}: {count} ({percentage:.1f}%)")
        
        # Success rate analysis
        if overall_summary['total_files'] > 0:
            success_rate = (overall_summary['successful_files'] / overall_summary['total_files']) * 100
            lines.append(f"\n📈 Processing success rate: {success_rate:.1f}%")
        
        # Mapping efficiency
        if overall_summary['successful_files'] > 0:
            avg_mappings = overall_summary['total_columns_mapped'] / overall_summary['successful_files']
            lines.append(f"🗺️ Average mappings per file: {avg_mappings:.1f}")
        
        lines.append("\n" + "="*100)
        lines.append("✅ Complete input data mapping and RAG analysis finished successfully!")
        lines.append(f"📊 All results available in: {self.output_path}")
        lines.append("="*100)
        
        return '\n'.join(lines)

# Add Counter import for mapping sources counting
from collections import Counter

def main():
    """Main execution function"""
    print("🚀 COMPLETE INPUT DATA MAPPING & RAG ANALYSIS SYSTEM")
    print("📊 Processing all input data with comprehensive mapping and RAG visualization...")
    
    # Initialize complete system
    complete_system = CompleteDataMappingRAGSystem()
    
    # Process all input files
    results, overall_summary = complete_system.process_all_input_files()
    
    print("\n" + "="*80)
    print("🎉 COMPLETE ANALYSIS FINISHED!")
    print("="*80)
    print("📁 Generated comprehensive outputs:")
    print("   • Mapped CSV files with meaningful column names")
    print("   • RAG analysis dashboards for each file")
    print("   • Comprehensive quality metrics")
    print("   • Executive summary report")
    print("   • Final JSON report with all details")
    print(f"📊 All results available in: {complete_system.output_path}")
    print("="*80)

if __name__ == "__main__":
    main()