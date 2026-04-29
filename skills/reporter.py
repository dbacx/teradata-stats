"""
Excel Reporter for Teradata Statistics Analysis

This module provides automated generation of Excel reports for statistics
analysis results, creating audit-ready evidence with multiple worksheets.
"""

import logging
import pandas as pd
import io
from typing import Dict, Any
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class ExcelReporter:
    """
    Generates Excel reports from analysis results.
    
    This class processes analysis results from the RulesEngine and creates
    a comprehensive Excel file with separate worksheets for each rule's results,
    providing audit-ready evidence for statistics optimization decisions.
    """
    
    def __init__(self):
        """Initialize the ExcelReporter with default configuration."""
        self.generation_date = datetime.now()
        logger.info(f"ExcelReporter initialized with generation date: {self.generation_date}")
    
    def generate_excel_bytes(self, analysis_results: Dict[str, Any]) -> bytes:
        """
        Generate Excel report from analysis results.
        
        Creates an Excel file in memory with separate worksheets for each
        non-empty DataFrame in the analysis results dictionary.
        
        Args:
            analysis_results: Dictionary containing analysis results from RulesEngine.
                            Expected structure: {'rule_id': DataFrame, ...}
        
        Returns:
            Bytes containing the generated Excel file
        
        Raises:
            ValueError: If analysis_results is empty or invalid
        """
        if not analysis_results:
            logger.warning("Empty analysis_results provided to generate_excel_bytes")
            return b''
        
        # Create BytesIO buffer for in-memory Excel file
        buffer = io.BytesIO()
        
        try:
            # Use ExcelWriter with openpyxl engine
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                # Add summary sheet with metadata
                self._add_summary_sheet(writer, analysis_results)
                
                # Add a worksheet for each non-empty DataFrame
                worksheet_count = 0
                for key, value in analysis_results.items():
                    # Skip execution summary and empty DataFrames
                    if key == '_execution_summary':
                        continue
                    
                    if isinstance(value, pd.DataFrame) and not value.empty:
                        # Sanitize sheet name (Excel sheet names have restrictions)
                        sheet_name = self._sanitize_sheet_name(key)
                        
                        # Write DataFrame to worksheet
                        value.to_excel(writer, sheet_name=sheet_name, index=False)
                        worksheet_count += 1
                        
                        logger.debug(f"Added worksheet '{sheet_name}' with {len(value)} rows")
                    elif isinstance(value, dict) and key == 'df_stats':
                        # Handle the main stats DataFrame separately
                        sheet_name = self._sanitize_sheet_name('full_statistics')
                        value.to_excel(writer, sheet_name=sheet_name, index=False)
                        worksheet_count += 1
                        logger.debug(f"Added worksheet '{sheet_name}' with {len(value)} rows")
            
            # Get the bytes value
            excel_bytes = buffer.getvalue()
            
            logger.info(f"Generated Excel report with {worksheet_count} worksheets, size: {len(excel_bytes)} bytes")
            return excel_bytes
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {str(e)}")
            raise
    
    def _add_summary_sheet(self, writer: pd.ExcelWriter, analysis_results: Dict[str, Any]) -> None:
        """
        Add a summary worksheet with metadata and statistics.
        
        Args:
            writer: ExcelWriter object
            analysis_results: Dictionary containing analysis results
        """
        # Create summary data
        summary_data = {
            'Report Metadata': [
                ('Generation Date', self.generation_date.strftime('%Y-%m-%d %H:%M:%S')),
                ('Report Type', 'Teradata Statistics Optimization Analysis'),
                ('Total Rules Executed', len(analysis_results)),
            ]
        }
        
        # Add execution summary if available
        if '_execution_summary' in analysis_results:
            exec_summary = analysis_results['_execution_summary']
            summary_data['Execution Summary'] = [
                ('Total Rules', exec_summary.get('total_rules', 'N/A')),
                ('Successful Rules', exec_summary.get('successful_rules', 'N/A')),
                ('Failed Rules', exec_summary.get('failed_rules', 'N/A')),
                ('Total Execution Time (s)', f"{exec_summary.get('total_execution_time', 0):.2f}"),
            ]
        
        # Add rule-specific statistics
        rule_stats = []
        for key, value in analysis_results.items():
            if key == '_execution_summary' or key == 'df_stats':
                continue
            
            if isinstance(value, pd.DataFrame):
                rule_stats.append((key, len(value)))
        
        if rule_stats:
            summary_data['Rule Results'] = rule_stats
        
        # Create DataFrame for summary
        summary_df = pd.DataFrame()
        
        # Flatten summary data into a single DataFrame
        summary_rows = []
        for section, items in summary_data.items():
            for item in items:
                summary_rows.append({
                    'Section': section,
                    'Metric': item[0],
                    'Value': str(item[1])
                })
        
        if summary_rows:
            summary_df = pd.DataFrame(summary_rows)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            logger.debug("Added Summary worksheet")
    
    def _sanitize_sheet_name(self, name: str, max_length: int = 31) -> str:
        """
        Sanitize a string to be a valid Excel sheet name.
        
        Excel sheet names have the following restrictions:
        - Maximum 31 characters
        - Cannot contain: \ / * ? : [ ]
        - Cannot be empty
        
        Args:
            name: Original sheet name
            max_length: Maximum length for sheet name (default: 31)
        
        Returns:
            Sanitized sheet name
        """
        # Remove invalid characters
        invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
        sanitized = name
        for char in invalid_chars:
            sanitized = sanitized.replace(char, '_')
        
        # Truncate to max length
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
        
        # Ensure not empty
        if not sanitized:
            sanitized = 'Sheet'
        
        return sanitized
    
    def generate_excel_with_ddl(self, analysis_results: Dict[str, Any], 
                               ddl_recommendations: Dict[str, list]) -> bytes:
        """
        Generate Excel report including DDL recommendations.
        
        Creates an Excel file with analysis results and DDL recommendations
        in separate worksheets.
        
        Args:
            analysis_results: Dictionary containing analysis results
            ddl_recommendations: Dictionary containing DDL statements by category
        
        Returns:
            Bytes containing the generated Excel file
        """
        if not analysis_results:
            logger.warning("Empty analysis_results provided to generate_excel_with_ddl")
            return b''
        
        # Create BytesIO buffer
        buffer = io.BytesIO()
        
        try:
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                # Add analysis results worksheets
                for key, value in analysis_results.items():
                    if key == '_execution_summary':
                        continue
                    
                    if isinstance(value, pd.DataFrame) and not value.empty:
                        sheet_name = self._sanitize_sheet_name(key)
                        value.to_excel(writer, sheet_name=sheet_name, index=False)
                        logger.debug(f"Added worksheet '{sheet_name}'")
                
                # Add DDL recommendations worksheets
                if ddl_recommendations:
                    for ddl_type, ddl_statements in ddl_recommendations.items():
                        if ddl_statements:
                            # Filter out None values
                            valid_statements = [stmt for stmt in ddl_statements if stmt is not None]
                            if valid_statements:
                                ddl_df = pd.DataFrame({
                                    'DDL_Statement': valid_statements
                                })
                                sheet_name = self._sanitize_sheet_name(f'DDL_{ddl_type}')
                                ddl_df.to_excel(writer, sheet_name=sheet_name, index=False)
                                logger.debug(f"Added DDL worksheet '{sheet_name}' with {len(valid_statements)} statements")
                
                # Add summary sheet
                self._add_summary_sheet(writer, analysis_results)
            
            excel_bytes = buffer.getvalue()
            logger.info(f"Generated Excel report with DDL, size: {len(excel_bytes)} bytes")
            return excel_bytes
            
        except Exception as e:
            logger.error(f"Error generating Excel report with DDL: {str(e)}")
            raise


if __name__ == "__main__":
    # Example usage
    try:
        # Create sample data
        sample_results = {
            'rule_01_unused': pd.DataFrame({
                'DatabaseName': ['DB1', 'DB2'],
                'TableName': ['Table1', 'Table2'],
                'ColumnName': ['Col1', 'Col2'],
                'days_since_last_access': [45, 60]
            }),
            'rule_06_stale': pd.DataFrame({
                'DatabaseName': ['DB1', 'DB3'],
                'TableName': ['Table1', 'Table3'],
                'ColumnName': ['Col1', 'Col3'],
                'days_since_collection': [25, 30]
            }),
            '_execution_summary': {
                'total_rules': 2,
                'successful_rules': 2,
                'failed_rules': 0,
                'total_execution_time': 1.5
            }
        }
        
        # Initialize reporter
        reporter = ExcelReporter()
        
        # Generate Excel bytes
        excel_bytes = reporter.generate_excel_bytes(sample_results)
        
        print(f"Generated Excel report: {len(excel_bytes)} bytes")
        
        # Test with DDL
        sample_ddl = {
            'collect_stats': ['COLLECT STATISTICS ON DB1.Table1;', 'COLLECT STATISTICS ON DB2.Table2;'],
            'drop_stats': ['DROP STATISTICS ON DB3.Table3;']
        }
        
        excel_with_ddl = reporter.generate_excel_with_ddl(sample_results, sample_ddl)
        print(f"Generated Excel with DDL: {len(excel_with_ddl)} bytes")
        
    except Exception as e:
        print(f"Error in example usage: {str(e)}")
