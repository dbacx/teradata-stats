"""
PPT Reporter for Executive Summary Generation

This module provides functionality to generate PowerPoint presentations
with executive summaries of statistics analysis results.
"""

import logging
import pandas as pd
import io
from typing import Dict, Any, Optional
from datetime import datetime
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor

# Configure logging
logger = logging.getLogger(__name__)


class PPTReporter:
    """
    Generate PowerPoint presentations for executive reporting.
    
    Creates professional presentations with title slide, summary slides
    for each rule with findings, and detailed tables for top findings.
    """
    
    def __init__(self):
        """
        Initialize the PPT Reporter.
        
        Will attempt to load template.pptx from project root, fallback to blank presentation.
        """
        logger.info("Initialized PPTReporter")
    
    def _load_presentation(self):
        """Load template.pptx from root or create new blank presentation."""
        import os
        # Get project root (parent of skills directory)
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        template_path = os.path.join(project_root, 'template.pptx')
        
        if os.path.exists(template_path):
            try:
                prs = Presentation(template_path)
                logger.info(f"Loaded template from {template_path}")
                return prs
            except Exception as e:
                logger.warning(f"Failed to load template, using blank presentation: {str(e)}")
                return Presentation()
        else:
            logger.info("template.pptx not found, using blank presentation")
            return Presentation()
    
    def generate_ppt_bytes(self, analysis_results: Dict[str, Any]) -> bytes:
        """
        Generate PowerPoint presentation as bytes.
        
        Args:
            analysis_results: Dictionary containing analysis results.
                            Expected keys: 'rule_results', 'analysis_level',
                            'database_name', 'table_name'
        
        Returns:
            Bytes of the PowerPoint presentation
        """
        try:
            # Load presentation (template or blank)
            prs = self._load_presentation()
            
            # Extract data
            rule_results = analysis_results.get('rule_results', {})
            analysis_level = analysis_results.get('analysis_level', 'Sistema Completo')
            database_name = analysis_results.get('database_name', 'N/A')
            table_name = analysis_results.get('table_name', None)
            
            # Create title slide
            self._create_title_slide(prs, analysis_level, database_name, table_name)
            
            # Create slides for each rule with findings
            rule_names = {
                'rule_01_unused': 'Objetos Sin Uso',
                'rule_02_sample': 'Candidatos a Sample',
                'rule_03_partition_missing': 'Missing PARTITION Stats',
                'rule_04_table_missing': 'Missing Table-Level Stats',
                'rule_05_index_missing': 'Missing Index-Level Stats',
                'rule_06_stale': 'Estadísticas Desactualizadas',
                'rule_07_zero_stats': 'Zero Count Statistics',
                'rule_08_multicolumn': 'Multicolumn MaxValueLength',
                'rule_09_skipped_sample': 'Skipped and Sample Stats',
                'rule_10_dbc_missing': 'Missing DBC/PDCR Stats',
                'rule_11_redundant': 'Redundant Statistics',
                'rule_12_extrapolation': 'Extrapolation Risk',
                'rule_13_analyze': 'Hardcoded Samples',
                'rule_14_join_columns': 'Missing Key Columns',
                'rule_15_bloat': 'Dictionary Bloat',
                'rule_16_urgent_missing': 'Urgent Missing Stats'
            }
            
            for rule_id, rule_name in rule_names.items():
                result_df = rule_results.get(rule_id, pd.DataFrame())
                if not result_df.empty:
                    self._create_rule_slide(prs, rule_name, result_df)
            
            # Save to bytes
            ppt_bytes = io.BytesIO()
            prs.save(ppt_bytes)
            ppt_bytes.seek(0)
            
            logger.info("Successfully generated PowerPoint presentation")
            return ppt_bytes.getvalue()
            
        except Exception as e:
            logger.error(f"Error generating PowerPoint: {str(e)}")
            raise
    
    def _create_title_slide(self, prs: Presentation, analysis_level: str, 
                           database_name: str, table_name: Optional[str]):
        """Create title slide with analysis information."""
        # Remove default blank slide if present
        if len(prs.slides) > 0:
            rId = prs.slides._sldIdLst[0].rId
            prs.part.drop_rel(rId)
            del prs.slides._sldIdLst[0]
        
        slide_layout = prs.slide_layouts[0]  # Title slide
        slide = prs.slides.add_slide(slide_layout)
        
        # Set title
        title = slide.shapes.title
        title.text = "Reporte de Optimización de Estadísticas Teradata"
        
        # Set subtitle with analysis details
        subtitle = slide.placeholders[1]
        subtitle_text = f"Análisis: {analysis_level}"
        if database_name and database_name != 'N/A':
            subtitle_text += f"\nBase de Datos: {database_name}"
        if table_name:
            subtitle_text += f"\nTabla: {table_name}"
        subtitle_text += f"\nFecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subtitle.text = subtitle_text
        
        logger.info("Created title slide")
    
    def _create_rule_slide(self, prs: Presentation, rule_name: str, result_df: pd.DataFrame):
        """Create slide for a specific rule with findings."""
        slide_layout = prs.slide_layouts[1]  # Title and Content
        slide = prs.slides.add_slide(slide_layout)
        
        # Set title
        title = slide.shapes.title
        title.text = rule_name
        
        # Add summary text
        left = Inches(1)
        top = Inches(1.5)
        width = Inches(8)
        height = Inches(0.5)
        
        text_box = slide.shapes.add_textbox(left, top, width, height)
        text_frame = text_box.text_frame
        p = text_frame.paragraphs[0]
        p.text = f"Total de Hallazgos: {len(result_df)}"
        p.font.size = Pt(18)
        p.font.bold = True
        
        # Add table with top 10 findings
        if len(result_df) > 0:
            top_10 = result_df.head(10)
            
            # Determine columns to display
            display_columns = []
            if 'DatabaseName' in top_10.columns:
                display_columns.append('DatabaseName')
            if 'TableName' in top_10.columns:
                display_columns.append('TableName')
            if 'ColumnName' in top_10.columns:
                display_columns.append('ColumnName')
            if 'recommendation' in top_10.columns:
                display_columns.append('recommendation')
            if 'reason' in top_10.columns:
                display_columns.append('reason')
            
            # Fallback if no columns found
            if not display_columns:
                display_columns = top_10.columns[:4].tolist()
            
            table_data = top_10[display_columns].fillna('N/A').head(10)
            
            # Create table
            rows = len(table_data) + 1  # +1 for header
            cols = len(display_columns)
            
            left = Inches(0.5)
            top = Inches(2.5)
            width = Inches(9)
            height = Inches(4.5)
            
            table = slide.shapes.add_table(rows, cols, left, top, width, height).table
            
            # Set header row
            for col_idx, col_name in enumerate(display_columns):
                cell = table.cell(0, col_idx)
                cell.text = col_name
                cell.text_frame.paragraphs[0].font.bold = True
                cell.text_frame.paragraphs[0].font.size = Pt(12)
            
            # Fill data rows
            for row_idx, (_, row_data) in enumerate(table_data.iterrows(), start=1):
                for col_idx, col_name in enumerate(display_columns):
                    cell = table.cell(row_idx, col_idx)
                    cell.text = str(row_data[col_name])[:50]  # Truncate long text
                    cell.text_frame.paragraphs[0].font.size = Pt(10)
        
        logger.info(f"Created slide for rule: {rule_name}")


if __name__ == "__main__":
    # Example usage for testing
    try:
        sample_results = {
            'rule_01_unused': pd.DataFrame({
                'DatabaseName': ['DB1', 'DB2'],
                'TableName': ['Table1', 'Table2'],
                'recommendation': ['DROP', 'DROP'],
                'reason': ['No usage', 'No usage']
            }),
            'rule_06_stale': pd.DataFrame({
                'DatabaseName': ['DB3'],
                'TableName': ['Table3'],
                'recommendation': ['COLLECT'],
                'reason': ['Old stats']
            })
        }
        
        analysis_data = {
            'rule_results': sample_results,
            'analysis_level': 'Base de Datos',
            'database_name': 'TEST_DB',
            'table_name': None
        }
        
        reporter = PPTReporter(template_path='Teradata_Template_2026.pptx')
        ppt_bytes = reporter.generate_ppt_bytes(analysis_data)
        
        # Save to file for testing
        with open('test_report.pptx', 'wb') as f:
            f.write(ppt_bytes)
        
        print("Test presentation generated: test_report.pptx")
        
    except Exception as e:
        print(f"Error in example usage: {str(e)}")
