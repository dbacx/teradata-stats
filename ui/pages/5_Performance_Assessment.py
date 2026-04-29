"""
Module 3: Performance & DDL Assessment Page

This page provides a dedicated interface for the Performance Assessment module,
using the new PerformanceCollector and PerformanceAnalyzer classes.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.connection import TeradataConnection
from collectors.mod3_performance_collector import PerformanceCollector
from analyzers.mod3_performance_analyzer import PerformanceAnalyzer
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
    """Display KPI cards for performance assessment."""
    st.subheader("KPI Cards - Performance Assessment")
    
    # Calculate metrics
    skewed_df = analyzed_data.get('02_highly_skewed_queries', pd.DataFrame())
    spool_df = analyzed_data.get('03_spool_usage_alerts', pd.DataFrame())
    fts_df = analyzed_data.get('01_full_table_scans', pd.DataFrame())
    
    # Queries with Skew
    skewed_count = len(skewed_df) if not skewed_df.empty else 0
    
    # Spool Errors
    spool_errors = 0
    if not spool_df.empty and 'ErrorCode' in spool_df.columns:
        spool_errors = len(spool_df[spool_df['ErrorCode'] == 2646])
    
    # High Spool Usage
    high_spool = 0
    if not spool_df.empty and 'NormalizedSpoolUsage' in spool_df.columns:
        high_spool = len(spool_df[spool_df['NormalizedSpoolUsage'] > 80])
    
    # Tables with High I/O
    high_io_tables = len(fts_df) if not fts_df.empty else 0
    
    # Unused Indexes
    unused_df = analyzed_data.get('04_unused_indexes', pd.DataFrame())
    unused_indexes = len(unused_df) if not unused_df.empty else 0
    
    # Display KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Queries con Skew", skewed_count)
    with col2:
        st.metric("Errores de Spool", spool_errors)
    with col3:
        st.metric("Uso Spool Alto", high_spool)
    with col4:
        st.metric("Tablas Alto I/O", high_io_tables)
    with col5:
        st.metric("Indices a Revisar", unused_indexes)
    
    st.markdown("---")


def display_charts(analyzed_data: dict):
    """Display charts for performance assessment."""
    st.subheader("Visualizaciones")
    
    col1, col2 = st.columns(2)
    
    # Scatter Plot: TotalIOCount vs AmpCPUSkew
    with col1:
        st.write("**Scatter Plot: I/O vs CPU Skew**")
        skewed_df = analyzed_data.get('02_highly_skewed_queries', pd.DataFrame())
        
        if not skewed_df.empty and 'TotalIOCount' in skewed_df.columns and 'AmpCPUSkew' in skewed_df.columns:
            fig = go.Figure(data=go.Scatter(
                x=skewed_df['TotalIOCount'],
                y=skewed_df['AmpCPUSkew'],
                mode='markers',
                marker=dict(
                    size=8,
                    color=skewed_df['AmpCPUSkew'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="CPU Skew %")
                ),
                text=skewed_df['UserName'],
                hovertemplate='<b>%{text}</b><br>I/O: %{x:,.0f}<br>Skew: %{y:.2f}%<extra></extra>'
            ))
            
            fig.update_layout(
                title="Queries: I/O vs CPU Skew",
                xaxis_title="Total I/O Count",
                yaxis_title="CPU Skew (%)",
                height=500,
                hovermode='closest'
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos suficientes o faltan columnas para generar el scatter plot.")
    
    # Bar Chart: Top 10 Tables by I/O
    with col2:
        st.write("**Top 10 Tablas por I/O**")
        fts_df = analyzed_data.get('01_full_table_scans', pd.DataFrame())
        
        if not fts_df.empty and 'DatabaseName' in fts_df.columns and 'TableName' in fts_df.columns and 'TotalIO' in fts_df.columns:
            top_10 = fts_df.head(10).copy()
            top_10['Table_Label'] = top_10['DatabaseName'] + '.' + top_10['TableName']
            top_10 = top_10.sort_values('TotalIO', ascending=True)
            
            st.bar_chart(
                data=top_10,
                x='TotalIO',
                y='Table_Label',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("No hay datos suficientes o faltan columnas para generar el gráfico de tablas por I/O.")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_full_table_scans": "Full Table Scans",
        "02_highly_skewed_queries": "Highly Skewed Queries",
        "03_spool_usage_alerts": "Spool Usage Alerts",
        "04_unused_indexes": "Unused Indexes"
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
    
    # Check if Severity column exists before applying filters
    if combined_df.empty or 'Severity' not in combined_df.columns:
        st.info("No hay hallazgos de configuración para mostrar con los filtros actuales.")
        return
    
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
        file_name=f"performance_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements."""
    st.subheader("Acciones DDL de Remediación")
    
    component_names = {
        "01_full_table_scans": "Full Table Scans",
        "02_highly_skewed_queries": "Highly Skewed Queries",
        "03_spool_usage_alerts": "Spool Usage Alerts",
        "04_unused_indexes": "Unused Indexes"
    }
    
    # Group by DDL_Action
    ddl_actions = {'REVIEW_INDEX': [], 'OPTIMIZE_QUERY': [], 'ADJUST_SPOOL': [], 'DROP_INDEX': []}
    
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
            for stmt in statements[:20]:  # Limit to first 20 for display
                st.code(stmt, language='sql')
            if len(statements) > 20:
                st.info(f"... y {len(statements) - 20} statements más (descarga el archivo completo)")
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
            file_name=f"performance_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Performance Assessment")
    st.markdown("*Evaluación y Optimización de Rendimiento en Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Análisis Módulo 3", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using PerformanceCollector
            with st.spinner("Recolectando datos de rendimiento..."):
                collector = PerformanceCollector()
                collected_data = collector.collect(connection)
                st.session_state.mod3_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using PerformanceAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = PerformanceAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod3_analyzed_data = analyzed_data
                st.session_state.mod3_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod3_findings)}")
            
            connection.close()
            st.success(f"Análisis completado. Total hallazgos: {len(st.session_state.mod3_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod3_analyzed_data:
        st.markdown("## Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod3_analyzed_data)
        st.markdown("---")
        display_charts(st.session_state.mod3_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod3_analyzed_data)
        st.markdown("---")
        display_ddl_actions(st.session_state.mod3_analyzed_data)


if __name__ == "__main__":
    main()
