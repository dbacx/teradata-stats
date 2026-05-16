"""
Module 10: Monthly Report Page

This page provides a dedicated interface for the Monthly Report module,
using the MonthlyReportCollector and MonthlyReportAnalyzer classes.
"""

import sys
import os
from pathlib import Path
import streamlit as st
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import logging

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
# Sube exactamente 2 niveles desde ui/pages/9_Module_...py hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

# Ahora sí, importaciones locales
from core.connection import TeradataConnection
from collectors.mod9_monthly_collector import MonthlyReportCollector
from analyzers.mod9_monthly_analyzer import MonthlyReportAnalyzer
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
    if 'mod10_collected_data' not in st.session_state:
        st.session_state.mod10_collected_data = None
    if 'mod10_analyzed_data' not in st.session_state:
        st.session_state.mod10_analyzed_data = None
    if 'mod10_selected_report' not in st.session_state:
        st.session_state.mod10_selected_report = None


def get_default_dates():
    """
    Calculate default dates for monthly report.
    Returns first day of previous month and last day of previous month.
    """
    today = date.today()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - relativedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return first_day_prev_month, last_day_prev_month


def display_execution_form():
    """Display execution registration form in sidebar."""
    st.sidebar.subheader("Registro de Ejecución")
    
    with st.sidebar.form("execution_form"):
        ticket = st.text_input("Ticket")
        project_name = st.text_input("Project Name", value="Teradata DBA Services")
        system_name = st.text_input("System Name", value="Production")
        resource_name = st.text_input("Resource Name", value="TD_PROD")
        issue = st.text_input("Issue")
        request_by = st.text_input("Request By")
        comments = st.text_area("Comments")
        
        submitted = st.form_submit_button("Registrar Ejecución")
        
        if submitted:
            log_execution(
                ticket=ticket,
                project_name=project_name,
                system_name=system_name,
                resource_name=resource_name,
                issue=issue,
                request_by=request_by,
                comments=comments
            )
            st.sidebar.success("Ejecución registrada exitosamente")


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Monthly Report")
    st.markdown("*Generación Automatizada de Reportes Mensuales en PowerPoint*")
    st.markdown("---")
    
    # Date selection
    col1, col2 = st.columns(2)
    
    # Calculate default dates
    default_start, default_end = get_default_dates()
    
    with col1:
        start_date = st.date_input(
            "Fecha Inicial",
            value=default_start,
            format="YYYY-MM-DD"
        )
    
    with col2:
        end_date = st.date_input(
            "Fecha Final",
            value=default_end,
            format="YYYY-MM-DD"
        )
    
    st.markdown("---")
    
    # Execute button for batch execution
    if st.sidebar.button("Ejecutar Analisis", type="primary"):
        try:
            # Get connection
            teradata_conn = TeradataConnection()
            conn = teradata_conn.connect()
            
            # Initialize collector
            collector = MonthlyReportCollector()
            
            # Collect data for all reports
            with st.spinner("Ejecutando 69 reportes en modo batch..."):
                collected_data = collector.collect_all(
                    conn,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d")
                )
            
            # Analyze data
            analyzer = MonthlyReportAnalyzer()
            analyzed_data = analyzer.analyze_all(collected_data)
            
            # Store in session state
            st.session_state.mod10_collected_data = collected_data
            st.session_state.mod10_analyzed_data = analyzed_data
            
            conn.close()
            
            st.success(f"Ejecución batch completada: {len(analyzed_data)} reportes procesados")
            
        except Exception as e:
            st.error(f"Error ejecutando reportes: {str(e)}")
            logger.error(f"Error executing batch reports: {str(e)}")
    
    # Display results if available
    if st.session_state.mod10_analyzed_data is not None:
        analyzed_data = st.session_state.mod10_analyzed_data
        
        if analyzed_data:
            # Extract report names for dynamic tabs
            nombres_reportes = list(analyzed_data.keys())
            
            # Create dynamic tabs
            tabs = st.tabs(nombres_reportes)
            
            # Iterate over tabs and results simultaneously
            for tab, report_name in zip(tabs, nombres_reportes):
                with tab:
                    df = analyzed_data[report_name]
                    
                    if not df.empty:
                        st.subheader(f"Reporte: {report_name}")
                        
                        # Render table
                        st.dataframe(df, use_container_width=True)
                        
                        # Automatic chart rendering for numeric columns
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.markdown("**Tendencia/Distribución (Columnas Numéricas)**")
                            st.bar_chart(df[numeric_cols])
                        
                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label=f"Descargar CSV - {report_name}",
                            data=csv,
                            file_name=f"{report_name}_{start_date}_to_{end_date}.csv",
                            mime="text/csv",
                            key=f"download_{report_name}"
                        )
                    else:
                        st.warning(f"No se encontraron datos para el reporte: {report_name}")
    
    # Display execution form in sidebar
    display_execution_form()


if __name__ == "__main__":
    main()
