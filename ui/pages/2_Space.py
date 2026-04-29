"""
Module 4: Space Assessment Page

This page provides a dedicated interface for the Space Assessment module,
using the new SpaceCollector and SpaceAnalyzer classes.
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
from collectors.mod4_space_collector import SpaceCollector
from analyzers.mod4_space_analyzer import SpaceAnalyzer
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
    
    [data-testid="stMetricValue"] {
        font-size: 3.5rem !important;
        font-weight: 600 !important;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'mod4_collected_data' not in st.session_state:
        st.session_state.mod4_collected_data = None
    if 'mod4_analyzed_data' not in st.session_state:
        st.session_state.mod4_analyzed_data = None
    if 'mod4_findings' not in st.session_state:
        st.session_state.mod4_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards for space assessment."""
    st.subheader("📊 KPI Cards - Space Assessment")
    
    # Calculate metrics
    db_util_df = analyzed_data.get('01_db_space_utilization', pd.DataFrame())
    unused_df = analyzed_data.get('02_unused_tables_space', pd.DataFrame())
    mvc_df = analyzed_data.get('03_mvc_candidates', pd.DataFrame())
    top_df = analyzed_data.get('04_top_tables', pd.DataFrame())
    
    # Terabytes Analyzed
    total_tb = 0
    if not db_util_df.empty:
        total_tb = db_util_df['CurrentPerm_TB'].sum()
    
    # GB Recoverable (unused tables)
    recoverable_gb = 0
    if not unused_df.empty:
        recoverable_gb = unused_df['Size_GB'].sum()
    
    # Critical Databases
    critical_dbs = 0
    if not db_util_df.empty:
        critical_dbs = len(db_util_df[db_util_df['Usage_Pct'] > 80])
    
    # MVC Candidates
    mvc_count = len(mvc_df)
    
    # Skewed Tables
    skewed_df = analyzed_data.get('05_skewed_tables', pd.DataFrame())
    skewed_count = len(skewed_df)
    
    # Display KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("📦 TB Analizados", f"{total_tb:.2f}")
    with col2:
        st.metric("♻️ GB Recuperables", f"{recoverable_gb:.2f}")
    with col3:
        st.metric("🔴 BDs Críticas", critical_dbs)
    with col4:
        st.metric("🗜️ MVC Candidatos", mvc_count)
    with col5:
        st.metric("⚡ Tablas Skewed", skewed_count)
    
    st.markdown("---")


def display_charts(analyzed_data: dict):
    """Display charts for top tables and database usage."""
    st.subheader("📈 Visualizaciones")
    
    col1, col2 = st.columns(2)
    
    # Top 10 Tables Chart
    with col1:
        st.write("**Top 10 Tablas por Tamaño**")
        top_df = analyzed_data.get('04_top_tables', pd.DataFrame())
        if not top_df.empty and 'DatabaseName' in top_df.columns and 'TableName' in top_df.columns and 'Size_GB' in top_df.columns:
            top_10 = top_df.head(10).copy()
            top_10['Table_Label'] = top_10['DatabaseName'] + '.' + top_10['TableName']
            top_10 = top_10.sort_values('Size_GB', ascending=True)
            
            st.bar_chart(
                data=top_10,
                x='Size_GB',
                y='Table_Label',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("⚠️ No hay datos suficientes o faltan columnas para generar el gráfico del Top 10 de Tablas.")
    
    # Database Usage Chart
    with col2:
        st.write("**Uso de Bases de Datos (Top 10)**")
        db_df = analyzed_data.get('01_db_space_utilization', pd.DataFrame())
        if not db_df.empty and 'DatabaseName' in db_df.columns and 'Usage_Pct' in db_df.columns:
            top_10_db = db_df.head(10).copy()
            top_10_db = top_10_db.sort_values('Usage_Pct', ascending=True)
            
            st.bar_chart(
                data=top_10_db,
                x='Usage_Pct',
                y='DatabaseName',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("⚠️ No hay datos suficientes o faltan columnas para generar el gráfico de Uso de Bases de Datos.")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("📋 Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_db_space_utilization": "DB Space Utilization",
        "02_unused_tables_space": "Unused Tables Space",
        "03_mvc_candidates": "MVC Candidates",
        "04_top_tables": "Top Tables",
        "05_skewed_tables": "Skewed Tables"
    }
    
    for component_key, component_name in component_names.items():
        df = analyzed_data.get(component_key, pd.DataFrame())
        if not df.empty:
            df_copy = df.copy()
            df_copy['Component'] = component_name
            all_findings.append(df_copy)
    
    if not all_findings:
        st.success("✅ No se encontraron hallazgos")
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
        label="📥 Descargar CSV",
        data=csv,
        file_name=f"space_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements."""
    st.subheader("🔧 Acciones DDL de Remediación")
    
    component_names = {
        "01_db_space_utilization": "DB Space Utilization",
        "02_unused_tables_space": "Unused Tables Space",
        "03_mvc_candidates": "MVC Candidates",
        "04_top_tables": "Top Tables",
        "05_skewed_tables": "Skewed Tables"
    }
    
    # Group by DDL_Action
    ddl_actions = {'DROP': [], 'ADD_COMPRESS': [], 'MONITOR': [], 'REDESIGN': []}
    
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
            label="📥 Descargar Todas las Acciones DDL",
            data=combined_ddl,
            file_name=f"space_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.sidebar.header("� Módulos")
    
    st.title("🗄️ Space")
    st.markdown("*Evaluación y Optimización de Espacio en Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuración")
    
    # Database filter
    database_name = st.sidebar.text_input(
        "Filtrar por Base de Datos (opcional)",
        placeholder="Ej: USER_DB, ALL para todas"
    )
    
    # Threshold configuration
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
    
    # Execute Analysis Button
    if st.sidebar.button("🚀 Ejecutar Análisis Módulo 4", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("🔌 Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using SpaceCollector
            with st.spinner("📊 Recolectando datos de espacio..."):
                collector = SpaceCollector()
                params = {
                    'unused_days_threshold': unused_days,
                    'skew_pct_threshold': skew_threshold
                }
                collected_data = collector.collect(connection, params=params)
                st.session_state.mod4_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using SpaceAnalyzer
            with st.spinner("🔍 Analizando datos..."):
                analyzer = SpaceAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod4_analyzed_data = analyzed_data
                st.session_state.mod4_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod4_findings)}")
            
            connection.close()
            st.success(f"✅ Análisis completado. Total hallazgos: {len(st.session_state.mod4_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod4_analyzed_data:
        st.markdown("## 📈 Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod4_analyzed_data)
        st.markdown("---")
        display_charts(st.session_state.mod4_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod4_analyzed_data)
        st.markdown("---")
        display_ddl_actions(st.session_state.mod4_analyzed_data)


if __name__ == "__main__":
    main()
