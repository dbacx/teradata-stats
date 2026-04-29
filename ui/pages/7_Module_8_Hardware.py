"""
Module 8: Hardware Utilization Page

This page provides a dedicated interface for the Hardware Utilization module,
using the HardwareCollector and HardwareAnalyzer classes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.connection import TeradataConnection
from collectors.mod8_hardware_collector import HardwareCollector
from analyzers.mod8_hardware_analyzer import HardwareAnalyzer

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
    if 'mod8_collected_data' not in st.session_state:
        st.session_state.mod8_collected_data = None
    if 'mod8_analyzed_data' not in st.session_state:
        st.session_state.mod8_analyzed_data = None
    if 'mod8_findings' not in st.session_state:
        st.session_state.mod8_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards for all 2 components."""
    st.subheader("KPI Cards - Hardware Utilization")
    
    # Extract AMP skew data
    amp_skew_df = analyzed_data.get('01_amp_space_skew', pd.DataFrame())
    total_amps = 0
    skew_percentage = 0.0
    
    if not amp_skew_df.empty and 'AMP_ID' in amp_skew_df.columns:
        total_amps = len(amp_skew_df)
        if 'Skew_Percentage' in amp_skew_df.columns:
            skew_percentage = amp_skew_df['Skew_Percentage'].iloc[0] if len(amp_skew_df) > 0 else 0.0
    
    # Calculate severity counts
    severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
    total_findings = 0
    
    for component_key in ['01_amp_space_skew', '02_node_cpu']:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if not df.empty and 'Severity' in df.columns:
            for severity in severity_counts:
                severity_counts[severity] += len(df[df['Severity'] == severity])
            total_findings += len(df)
    
    # Display KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de AMPs", total_amps)
    with col2:
        st.metric("Varianza de Espacio (Skew)", f"{skew_percentage:.2f}%")
    with col3:
        st.metric("Total Hallazgos", total_findings)
    with col4:
        st.metric("HIGH/MEDIUM", severity_counts['HIGH'] + severity_counts['MEDIUM'])
    
    st.markdown("---")
    
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


def display_amp_space_chart(analyzed_data: dict):
    """Display bar chart of space per AMP."""
    st.subheader("Espacio por AMP")
    
    amp_skew_df = analyzed_data.get('01_amp_space_skew', pd.DataFrame())
    
    if not amp_skew_df.empty and 'AMP_ID' in amp_skew_df.columns and 'TotalSpace_Bytes' in amp_skew_df.columns:
        # Convert bytes to GB for better readability
        chart_df = amp_skew_df.copy()
        chart_df['Space_GB'] = chart_df['TotalSpace_Bytes'] / (1024**3)
        chart_df = chart_df.set_index('AMP_ID')
        
        st.bar_chart(chart_df['Space_GB'])
        
        st.markdown(f"**Total AMPs:** {len(chart_df)}")
        st.markdown(f"**Max Space:** {chart_df['Space_GB'].max():.2f} GB")
        st.markdown(f"**Min Space:** {chart_df['Space_GB'].min():.2f} GB")
        st.markdown(f"**Avg Space:** {chart_df['Space_GB'].mean():.2f} GB")
    else:
        st.info("No hay datos de espacio por AMP disponibles")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_amp_space_skew": "AMP Space Skew",
        "02_node_cpu": "Node CPU Usage"
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
        file_name=f"hardware_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_amp_details(analyzed_data: dict):
    """Display AMP space details table."""
    st.subheader("Detalles de Espacio por AMP")
    
    amp_skew_df = analyzed_data.get('01_amp_space_skew', pd.DataFrame())
    
    if not amp_skew_df.empty:
        display_df = amp_skew_df.copy()
        if 'TotalSpace_Bytes' in display_df.columns:
            display_df['Space_GB'] = display_df['TotalSpace_Bytes'] / (1024**3)
        
        st.dataframe(
            display_df,
            width='stretch',
            height=300
        )
    else:
        st.info("No hay detalles de AMP disponibles")


def display_node_cpu_details(analyzed_data: dict):
    """Display node CPU details table."""
    st.subheader("Detalles de CPU por Nodo")
    
    node_cpu_df = analyzed_data.get('02_node_cpu', pd.DataFrame())
    
    if not node_cpu_df.empty:
        display_df = node_cpu_df.copy()
        if 'CPU_Utilization_Pct' in display_df.columns:
            display_df['CPU_Utilization_Pct'] = display_df['CPU_Utilization_Pct'].round(2)
        
        st.dataframe(
            display_df,
            width='stretch',
            height=300
        )
    else:
        st.info("No hay detalles de CPU por nodo disponibles (ResUsage logging puede estar desactivado)")


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Hardware Utilization")
    st.markdown("*Monitoreo de Balanceo de Carga en AMPs y Uso de CPU en Nodos*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Análisis Módulo 8", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using HardwareCollector
            with st.spinner("Recolectando datos de hardware..."):
                collector = HardwareCollector()
                collected_data = collector.collect(connection, params={})
                st.session_state.mod8_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using HardwareAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = HardwareAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod8_analyzed_data = analyzed_data
                st.session_state.mod8_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod8_findings)}")
            
            connection.close()
            st.success(f"Análisis completado. Total hallazgos: {len(st.session_state.mod8_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod8_analyzed_data:
        st.markdown("## Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod8_analyzed_data)
        st.markdown("---")
        display_amp_space_chart(st.session_state.mod8_analyzed_data)
        st.markdown("---")
        display_amp_details(st.session_state.mod8_analyzed_data)
        st.markdown("---")
        display_node_cpu_details(st.session_state.mod8_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod8_analyzed_data)


if __name__ == "__main__":
    main()
