"""
Module 1: System Information Page

This page provides a dedicated interface for System Information module,
using SystemInformationCollector and SystemInformationAnalyzer classes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.connection import TeradataConnection
from collectors.mod1_health_collector import SystemInformationCollector
from analyzers.mod1_health_analyzer import SystemInformationAnalyzer
from utils.csv_logger import log_execution

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def inject_custom_css():
    """Inject custom CSS for compact mode."""
    css = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    
    * {
        font-family: 'Inter', sans-serif !important;
    }
    
    /* Compact Mode - Reduce margins */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    
    /* Compact metrics */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.85rem !important;
    }
    
    /* Reduce gaps */
    .stGap {
        gap: 0.3rem !important;
    }
    
    /* Compact general text */
    p, div, span {
        font-size: 0.9rem;
    }
    
    /* Compact tables */
    [data-testid="stDataFrame"] {
        font-size: 0.85rem;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'mod1_collected_data' not in st.session_state:
        st.session_state.mod1_collected_data = None
    if 'mod1_analyzed_data' not in st.session_state:
        st.session_state.mod1_analyzed_data = None


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("System Information")
    st.markdown("*Información de la capacidad del sistema*")
    st.markdown("---")
    
    # Execute Analysis Button
    if st.button("Ejecutar Analisis", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using SystemInformationCollector
            with st.spinner("Recolectando información del sistema..."):
                collector = SystemInformationCollector()
                collected_data = collector.collect(connection, params={})
                st.session_state.mod1_collected_data = collected_data
                logger.info(f"Collected {len(collected_data)} rows from system information")
            
            # Step 3: Analyze data using SystemInformationAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = SystemInformationAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod1_analyzed_data = analyzed_data
                logger.info("Analysis complete")
            
            connection.close()
            
            # Log execution
            log_execution(
                ticket="System_Information",
                project_name="Teradata DBA Services",
                system_name="Production",
                resource_name="TD_PROD",
                issue="System Information Query",
                request_by="System",
                comments="System information analysis completed"
            )
            
            # Custom success message
            st.markdown(
                "<div style='background-color: #d1e7dd; color: #0f5132; padding: 6px 12px; "
                "border-radius: 4px; font-size: 0.85rem; border: 1px solid #badbcc; "
                "margin-top: 10px;'>Análisis completado exitosamente.</div>", 
                unsafe_allow_html=True
            )
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod1_analyzed_data is not None:
        df = st.session_state.mod1_analyzed_data
        
        if not df.empty:
            st.markdown("## Resultados del Análisis")
            
            # Single consolidated table
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Descargar CSV",
                data=csv,
                file_name=f"system_information_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No se encontraron datos de información del sistema")


if __name__ == "__main__":
    main()
