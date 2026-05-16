"""
Module 3: Space Assessment Page

This page provides a dedicated interface for the Space Assessment module,
using the SpaceCollector and SpaceAnalyzer classes.
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
from collectors.mod3_space_collector import SpaceCollector
from analyzers.mod3_space_analyzer import SpaceAnalyzer, COMPONENT_LABELS, COMPONENT_SEVERITY, DDL_COLUMNS
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
    if 'mod3_collected_data' not in st.session_state:
        st.session_state.mod3_collected_data = None
    if 'mod3_analyzed_data' not in st.session_state:
        st.session_state.mod3_analyzed_data = None
    if 'mod3_findings' not in st.session_state:
        st.session_state.mod3_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards based on severity counts across all components."""
    st.subheader("KPI Cards - Space Assessment")
    
    severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
    total_rows = 0
    
    for component_key, df in analyzed_data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            total_rows += len(df)
            if 'Severity' in df.columns:
                for sev in severity_counts:
                    severity_counts[sev] += int((df['Severity'] == sev).sum())
    
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
        st.metric("Total Hallazgos", total_rows)
    
    st.markdown("---")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")
    
    all_findings = []
    
    for component_key, df in analyzed_data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df_copy = df.copy()
            df_copy['Component'] = COMPONENT_LABELS.get(component_key, component_key)
            all_findings.append(df_copy)
    
    if not all_findings:
        st.success("No se encontraron hallazgos")
        return
    
    combined_df = pd.concat(all_findings, ignore_index=True)
    
    available_severities = sorted(combined_df['Severity'].unique().tolist()) if 'Severity' in combined_df.columns else []
    severity_filter = st.multiselect(
        "Filtrar por Severidad",
        options=available_severities,
        default=available_severities
    )
    
    if severity_filter and 'Severity' in combined_df.columns:
        combined_df = combined_df[combined_df['Severity'].isin(severity_filter)]
    
    available_components = sorted(combined_df['Component'].unique().tolist())
    component_filter = st.multiselect(
        "Filtrar por Componente",
        options=available_components,
        default=available_components
    )
    
    if component_filter:
        combined_df = combined_df[combined_df['Component'].isin(component_filter)]
    
    def highlight_severity(val):
        colors = {
            'CRITICAL': 'background-color: #FF6B6B; color: white; font-weight: bold',
            'HIGH': 'background-color: #FFA500; color: white; font-weight: bold',
            'MEDIUM': 'background-color: #FFD700; color: black',
            'LOW': 'background-color: #90EE90; color: black',
            'INFO': 'background-color: #E0E0E0; color: black',
        }
        return colors.get(val, '')
    
    if 'Severity' in combined_df.columns:
        styled_df = combined_df.style.map(highlight_severity, subset=['Severity'])
    else:
        styled_df = combined_df.style
    
    st.dataframe(styled_df, use_container_width=True, height=500)
    
    csv = combined_df.to_csv(index=False)
    st.download_button(
        label="Descargar CSV",
        data=csv,
        file_name=f"space_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements dynamically."""
    st.subheader("Scripts de Remediación")
    
    for component_key in COMPONENT_LABELS:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            ddl_col = None
            for col_name in DDL_COLUMNS:
                if col_name in df.columns:
                    ddl_col = col_name
                    break
            
            if ddl_col is None:
                continue
            
            statements = df[ddl_col].dropna().unique().tolist()
            if not statements:
                continue
            
            label = COMPONENT_LABELS.get(component_key, component_key)
            with st.expander(f"{label} ({len(statements)} scripts)", expanded=False):
                for stmt in statements[:50]:
                    st.code(str(stmt), language='sql')
                if len(statements) > 50:
                    st.info(f"... y {len(statements) - 50} scripts más (descarga el archivo completo)")
    
    all_ddl = []
    for component_key in COMPONENT_LABELS:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            for col_name in DDL_COLUMNS:
                if col_name in df.columns:
                    all_ddl.extend(df[col_name].dropna().unique().tolist())
                    break
    
    if all_ddl:
        combined_ddl = "\n".join(str(s) for s in all_ddl)
        st.download_button(
            label="Descargar Todos los Scripts DDL",
            data=combined_ddl,
            file_name=f"space_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Space")
    st.markdown("*Evaluación y Optimización de Espacio en Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    database_name = st.sidebar.text_input(
        "Filtrar por Base de Datos (opcional)",
        placeholder="Ej: USER_DB, ALL para todas"
    )
    
    unused_days = st.sidebar.slider(
        "Umbral Días para Tablas Sin Uso",
        min_value=30,
        max_value=365,
        value=THRESHOLDS['unused_object_days'],
        help="Tablas sin acceso por más de este número de días se considerarán sin uso"
    )
    
    skew_threshold = st.sidebar.slider(
        "Umbral % Skew para Tablas",
        min_value=10,
        max_value=100,
        value=THRESHOLDS['pi_skew_pct'],
        help="Porcentaje de skew para identificar tablas desbalanceadas"
    )
    
    if st.sidebar.button("Ejecutar Analisis", type="primary"):
        try:
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            with st.spinner("Recolectando datos de espacio..."):
                collector = SpaceCollector()
                params = {
                    'unused_days_threshold': unused_days,
                    'skew_pct_threshold': skew_threshold
                }
                collected_data = collector.collect(connection, params=params)
                st.session_state.mod3_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            with st.spinner("Analizando datos..."):
                analyzer = SpaceAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod3_analyzed_data = analyzed_data
                st.session_state.mod3_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod3_findings)}")
            
            connection.close()
            st.success(f"Análisis completado. Total hallazgos: {len(st.session_state.mod3_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    if st.session_state.mod3_analyzed_data:
        st.markdown("## Resultados del Análisis")
        
        tab_hallazgos, tab_scripts = st.tabs(["Hallazgos", "Scripts de Remediación"])
        
        with tab_hallazgos:
            display_kpi_cards(st.session_state.mod3_analyzed_data)
            st.markdown("---")
            display_findings_table(st.session_state.mod3_analyzed_data)
        
        with tab_scripts:
            display_ddl_actions(st.session_state.mod3_analyzed_data)


if __name__ == "__main__":
    main()
