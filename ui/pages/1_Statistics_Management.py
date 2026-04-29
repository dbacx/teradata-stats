"""
Module 2: Statistics Management Page

This page provides a dedicated interface for the Statistics Management module,
using the new StatsCollector and StatsAnalyzer classes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.connection import TeradataConnection
from collectors.mod2_stats_collector import StatsCollector
from analyzers.mod2_stats_analyzer import StatsAnalyzer
from core.config import THRESHOLDS, SYSTEM_DATABASES

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def inject_custom_css():
    """Inject custom CSS for fonts and icons."""
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
    if 'mod2_collected_data' not in st.session_state:
        st.session_state.mod2_collected_data = None
    if 'mod2_analyzed_data' not in st.session_state:
        st.session_state.mod2_analyzed_data = None
    if 'mod2_findings' not in st.session_state:
        st.session_state.mod2_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards for all 10 components."""
    st.subheader("KPI Cards - Statistics Management")
    
    component_names = [
        ("Unused Objects", "01_unused_objects"),
        ("Sample Candidates", "02_sample_candidates"),
        ("Missing PARTITION", "03_missing_partition"),
        ("Missing Table Stats", "04_missing_table"),
        ("Missing Index Stats", "05_missing_index"),
        ("Stale Statistics", "06_stale_stats"),
        ("Zero Statistics", "07_zero_stats"),
        ("Multicolumn Issues", "08_multicolumn"),
        ("Skipped/Sample", "09_skipped_sample"),
        ("DBC Recommendations", "10_dbc_recommendations")
    ]
    
    # Calculate severity counts
    severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
    total_findings = 0
    
    for component_name, component_key in component_names:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if not df.empty and 'Severity' in df.columns:
            for severity in severity_counts:
                severity_counts[severity] += len(df[df['Severity'] == severity])
            total_findings += len(df)
    
    # Display severity summary
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("CRITICAL", severity_counts['CRITICAL'])
    with col2:
        st.metric("HIGH", severity_counts['HIGH'])
    with col3:
        st.metric("MEDIUM", severity_counts['MEDIUM'])
    with col4:
        st.metric("LOW", severity_counts['LOW'])
    with col5:
        st.metric("INFO", severity_counts['INFO'])
    
    st.markdown("---")
    
    # Display component counts
    cols = st.columns(5)
    for i, (component_name, component_key) in enumerate(component_names):
        df = analyzed_data.get(component_key, pd.DataFrame())
        count = len(df)
        with cols[i % 5]:
            st.metric(component_name, count)
    
    st.markdown(f"**Total de Hallazgos:** {total_findings}")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting and pagination."""
    st.subheader("Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_unused_objects": "Unused Objects",
        "02_sample_candidates": "Sample Candidates",
        "03_missing_partition": "Missing PARTITION",
        "04_missing_table": "Missing Table Stats",
        "05_missing_index": "Missing Index Stats",
        "06_stale_stats": "Stale Statistics",
        "07_zero_stats": "Zero Statistics",
        "08_multicolumn": "Multicolumn Issues",
        "09_skipped_sample": "Skipped/Sample",
        "10_dbc_recommendations": "DBC Recommendations"
    }
    
    for component_key, component_name in component_names.items():
        df = analyzed_data.get(component_key, pd.DataFrame())
        if not df.empty:
            df_copy = df.copy()
            df_copy['Component'] = component_name
            all_findings.append(df_copy)
    
    if not all_findings:
        st.success("No se encontraron hallazgos")
        return
    
    combined_df = pd.concat(all_findings, ignore_index=True)
    
    # Severity filter
    severity_filter = st.multiselect(
        "Filtrar por Severidad",
        options=['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'],
        default=['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']
    )
    
    if severity_filter:
        combined_df = combined_df[combined_df['Severity'].isin(severity_filter)]
    
    # Component filter
    component_filter = st.multiselect(
        "Filtrar por Componente",
        options=list(component_names.values()),
        default=list(component_names.values())
    )
    
    if component_filter:
        combined_df = combined_df[combined_df['Component'].isin(component_filter)]
    
    # Display with conditional formatting
    def highlight_severity(val):
        if val == 'CRITICAL':
            return 'background-color: #FF6B6B; color: white; font-weight: bold'
        elif val == 'HIGH':
            return 'background-color: #FFA500; color: white; font-weight: bold'
        elif val == 'MEDIUM':
            return 'background-color: #FFD700; color: black'
        elif val == 'LOW':
            return 'background-color: #90EE90; color: black'
        else:
            return 'background-color: #E0E0E0; color: black'
    
    styled_df = combined_df.style.applymap(highlight_severity, subset=['Severity'])
    
    st.dataframe(
        styled_df,
        width='stretch',
        height=500
    )
    
    # CSV Download
    csv = combined_df.to_csv(index=False)
    st.download_button(
        label="Descargar CSV",
        data=csv,
        file_name=f"stats_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements."""
    st.subheader("Acciones DDL de Remediación")
    
    component_names = {
        "01_unused_objects": "Unused Objects",
        "02_sample_candidates": "Sample Candidates",
        "03_missing_partition": "Missing PARTITION",
        "04_missing_table": "Missing Table Stats",
        "05_missing_index": "Missing Index Stats",
        "06_stale_stats": "Stale Statistics",
        "07_zero_stats": "Zero Statistics",
        "08_multicolumn": "Multicolumn Issues",
        "09_skipped_sample": "Skipped/Sample",
        "10_dbc_recommendations": "DBC Recommendations"
    }
    
    # Group by DDL_Action
    ddl_actions = {'COLLECT': [], 'DROP': [], 'REFRESH': [], 'RECREATE': [], 'REVIEW': []}
    
    for component_key, component_name in component_names.items():
        df = analyzed_data.get(component_key, pd.DataFrame())
        if not df.empty and 'DDL_Statement' in df.columns and 'DDL_Action' in df.columns:
            for _, row in df.iterrows():
                action = row['DDL_Action']
                statement = row['DDL_Statement']
                if action in ddl_actions:
                    ddl_actions[action].append(statement)
    
    # Display each action type
    for action, statements in ddl_actions.items():
        if statements:
            st.write(f"**{action} ({len(statements)} statements):**")
            for stmt in statements:
                st.code(stmt, language='sql')
            st.markdown("---")
    
    # Download all DDL
    all_ddl = []
    for action, statements in ddl_actions.items():
        all_ddl.extend(statements)
    
    if all_ddl:
        combined_ddl = "\n".join(all_ddl)
        st.download_button(
            label="Descargar Todas las Acciones DDL",
            data=combined_ddl,
            file_name=f"stats_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Statistics Management")
    st.markdown("*Gestión y Optimización de Estadísticas Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    # Database filter
    database_name = st.sidebar.text_input(
        "Filtrar por Base de Datos (opcional)",
        placeholder="Ej: USER_DB, ALL para todas"
    )
    
    # Threshold configuration
    stale_days = st.sidebar.slider(
        "Umbral Días para Stats Desactualizadas",
        min_value=1,
        max_value=90,
        value=THRESHOLDS['stats_stale_days'],
        help="Estadísticas más antiguas que este número de días se considerarán desactualizadas"
    )
    
    max_value_length = st.sidebar.slider(
        "Umbral MaxValueLength para Multicolumn",
        min_value=10,
        max_value=50,
        value=25,
        help="Valor máximo para considerar un problema de MaxValueLength"
    )
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Análisis Módulo 2", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using StatsCollector
            with st.spinner("Recolectando datos de estadísticas..."):
                collector = StatsCollector()
                params = {
                    'stale_days_threshold': stale_days,
                    'max_value_length_threshold': max_value_length
                }
                collected_data = collector.collect(connection, params=params)
                st.session_state.mod2_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using StatsAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = StatsAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod2_analyzed_data = analyzed_data
                st.session_state.mod2_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod2_findings)}")
            
            connection.close()
            st.success(f"Análisis completado. Total hallazgos: {len(st.session_state.mod2_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod2_analyzed_data:
        st.markdown("## Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod2_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod2_analyzed_data)
        st.markdown("---")
        display_ddl_actions(st.session_state.mod2_analyzed_data)


if __name__ == "__main__":
    main()
