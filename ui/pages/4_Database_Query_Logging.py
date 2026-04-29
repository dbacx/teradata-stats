"""
Module 5: Data Collection & Logging Config Page

This page provides a dedicated interface for the Config Assessment module,
using the new ConfigCollector and ConfigAnalyzer classes.
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
from collectors.mod5_config_collector import ConfigCollector
from analyzers.mod5_config_analyzer import ConfigAnalyzer
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
    
    .checklist-item {
        padding: 10px;
        margin: 5px 0;
        border-radius: 5px;
        display: flex;
        align-items: center;
    }
    
    .checklist-success {
        background-color: #d4edda;
        color: #155724;
    }
    
    .checklist-warning {
        background-color: #fff3cd;
        color: #856404;
    }
    
    .checklist-danger {
        background-color: #f8d7da;
        color: #721c24;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'mod5_collected_data' not in st.session_state:
        st.session_state.mod5_collected_data = None
    if 'mod5_analyzed_data' not in st.session_state:
        st.session_state.mod5_analyzed_data = None
    if 'mod5_findings' not in st.session_state:
        st.session_state.mod5_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards for config assessment."""
    st.subheader("📊 KPI Cards - Config Assessment")
    
    # Calculate metrics
    dbql_rules_df = analyzed_data.get('02_dbql_rules', pd.DataFrame())
    dbql_tables_df = analyzed_data.get('04_dbql_tables_health', pd.DataFrame())
    resusage_df = analyzed_data.get('01_resusage_rules', pd.DataFrame())
    
    # Active DBQL Rules
    active_dbql_rules = len(dbql_rules_df) if not dbql_rules_df.empty else 0
    
    # Critical Log Tables (missing or empty)
    critical_tables = 0
    if not dbql_tables_df.empty and 'Severity' in dbql_tables_df.columns:
        critical_tables = len(dbql_tables_df[dbql_tables_df['Severity'] == 'CRITICAL'])
    
    # ResUsage Status (check if NodeLoggingRate > 10)
    resusage_status = "OK"
    if not resusage_df.empty and 'RuleName' in resusage_df.columns:
        node_rate = resusage_df[resusage_df['RuleName'] == 'NodeLoggingRate']
        if not node_rate.empty and 'RuleValue' in node_rate.columns:
            try:
                rate = int(node_rate.iloc[0]['RuleValue'])
                resusage_status = "⚠️" if rate > 10 else "✅"
            except (ValueError, TypeError):
                resusage_status = "❓"
    
    # DBQL Thresholds Configured
    thresholds_df = analyzed_data.get('03_dbql_thresholds', pd.DataFrame())
    thresholds_configured = len(thresholds_df) if not thresholds_df.empty else 0
    
    # Display KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📋 Reglas DBQL Activas", active_dbql_rules)
    with col2:
        st.metric("🗄️ Tablas de Log Críticas", critical_tables)
    with col3:
        st.metric("⚙️ Estado de ResUsage", resusage_status)
    with col4:
        st.metric("🎯 Thresholds Configurados", thresholds_configured)
    
    st.markdown("---")


def display_config_checklist(analyzed_data: dict):
    """Display configuration status as a visual checklist."""
    st.subheader("✅ Checklist de Configuración")
    
    checklist_items = []
    
    # Check 1: DBQL Rules Exist
    dbql_rules_df = analyzed_data.get('02_dbql_rules', pd.DataFrame())
    if not dbql_rules_df.empty:
        checklist_items.append({
            'check': 'Reglas DBQL Configuradas',
            'status': 'success',
            'message': f"{len(dbql_rules_df)} reglas activas encontradas"
        })
    else:
        checklist_items.append({
            'check': 'Reglas DBQL Configuradas',
            'status': 'danger',
            'message': 'No se encontraron reglas DBQL activas'
        })
    
    # Check 2: DBQL Thresholds Configured
    thresholds_df = analyzed_data.get('03_dbql_thresholds', pd.DataFrame())
    if not thresholds_df.empty:
        checklist_items.append({
            'check': 'Thresholds DBQL Configurados',
            'status': 'success',
            'message': f"{len(thresholds_df)} reglas con thresholds"
        })
    else:
        checklist_items.append({
            'check': 'Thresholds DBQL Configurados',
            'status': 'warning',
            'message': 'No se encontraron thresholds configurados'
        })
    
    # Check 3: DBQL Tables Exist
    dbql_tables_df = analyzed_data.get('04_dbql_tables_health', pd.DataFrame())
    
    # Diagnostic Mode: Show raw DBQL Tables data
    with st.expander("🔍 Modo Diagnóstico: Datos crudos DBQL Tables"):
        st.dataframe(dbql_tables_df, width='stretch')
    
    if not dbql_tables_df.empty:
        expected_tables = ['DBQLogTbl', 'DBQLogTbl_Hst', 'DBQLObjTbl', 'DBQLObjTbl_Hst']
        # Detect dynamically the column name (ViewName or TableName)
        col_name = 'ViewName' if 'ViewName' in dbql_tables_df.columns else 'TableName'
        
        if col_name in dbql_tables_df.columns:
            existing = set(dbql_tables_df[col_name].tolist())
        else:
            existing = set()
        
        missing = set(expected_tables) - existing
        if not missing:
            checklist_items.append({
                'check': 'Tablas DBQL Existentes',
                'status': 'success',
                'message': 'Todas las tablas críticas existen'
            })
        else:
            checklist_items.append({
                'check': 'Tablas DBQL Existentes',
                'status': 'danger',
                'message': f"Faltan tablas: {', '.join(missing)}"
            })
    else:
        checklist_items.append({
            'check': 'Tablas DBQL Existentes',
            'status': 'danger',
            'message': 'No se pudo verificar las tablas DBQL'
        })
    
    # Check 4: ResUsage Configuration
    resusage_df = analyzed_data.get('01_resusage_rules', pd.DataFrame())
    if not resusage_df.empty and 'RuleName' in resusage_df.columns:
        node_rate = resusage_df[resusage_df['RuleName'] == 'NodeLoggingRate']
        if not node_rate.empty and 'RuleValue' in node_rate.columns:
            try:
                rate = int(node_rate.iloc[0]['RuleValue'])
                if rate <= 10:
                    checklist_items.append({
                        'check': 'ResUsage NodeLoggingRate',
                        'status': 'success',
                        'message': f"Intervalo: {rate} minutos (≤ 10 min)"
                    })
                else:
                    checklist_items.append({
                        'check': 'ResUsage NodeLoggingRate',
                        'status': 'warning',
                        'message': f"Intervalo: {rate} minutos (> 10 min recomendado)"
                    })
            except (ValueError, TypeError):
                checklist_items.append({
                    'check': 'ResUsage NodeLoggingRate',
                    'status': 'warning',
                    'message': 'No se pudo interpretar el valor'
                })
        else:
            checklist_items.append({
                'check': 'ResUsage NodeLoggingRate',
                'status': 'warning',
                'message': 'No configurado'
            })
    else:
        checklist_items.append({
            'check': 'ResUsage NodeLoggingRate',
            'status': 'info',
            'message': 'No disponible (vista ResUsageRules)'
        })
    
    # Display checklist
    for item in checklist_items:
        icon = '✅' if item['status'] == 'success' else '⚠️' if item['status'] == 'warning' else '❌' if item['status'] == 'danger' else '❓'
        css_class = f"checklist-{item['status']}"
        st.markdown(
            f"""
            <div class="checklist-item {css_class}">
                <span style="font-size: 1.5em; margin-right: 10px;">{icon}</span>
                <div>
                    <strong>{item['check']}</strong><br/>
                    <small>{item['message']}</small>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown("---")


def display_charts(analyzed_data: dict):
    """Display charts for config assessment."""
    st.subheader("📈 Visualizaciones")
    
    col1, col2 = st.columns(2)
    
    # Bar Chart: DBQL Rules by User
    with col1:
        st.write("**Reglas DBQL por Usuario**")
        dbql_rules_df = analyzed_data.get('02_dbql_rules', pd.DataFrame())
        
        if not dbql_rules_df.empty and 'UserName' in dbql_rules_df.columns:
            rules_by_user = dbql_rules_df.groupby('UserName').size().reset_index(name='RuleCount')
            rules_by_user = rules_by_user.sort_values('RuleCount', ascending=True)
            
            st.bar_chart(
                data=rules_by_user,
                x='RuleCount',
                y='UserName',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("⚠️ No hay datos suficientes o faltan columnas para generar el gráfico de reglas DBQL.")
    
    # Bar Chart: DBQL Tables Size
    with col2:
        st.write("**Tamaño de Tablas DBQL (GB)**")
        dbql_tables_df = analyzed_data.get('04_dbql_tables_health', pd.DataFrame())
        
        if not dbql_tables_df.empty and 'TableName' in dbql_tables_df.columns and 'Size_GB' in dbql_tables_df.columns:
            tables_df = dbql_tables_df[['TableName', 'Size_GB']].copy()
            tables_df = tables_df.sort_values('Size_GB', ascending=True)
            
            st.bar_chart(
                data=tables_df,
                x='Size_GB',
                y='TableName',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("⚠️ No hay datos suficientes o faltan columnas para generar el gráfico de tamaño de tablas.")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("📋 Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_resusage_rules": "ResUsage Rules",
        "02_dbql_rules": "DBQL Rules",
        "03_dbql_thresholds": "DBQL Thresholds",
        "04_dbql_tables_health": "DBQL Tables Health"
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
        label="📥 Descargar CSV",
        data=csv,
        file_name=f"config_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements."""
    st.subheader("🔧 Acciones DDL de Remediación")
    
    component_names = {
        "01_resusage_rules": "ResUsage Rules",
        "02_dbql_rules": "DBQL Rules",
        "03_dbql_thresholds": "DBQL Thresholds",
        "04_dbql_tables_health": "DBQL Tables Health"
    }
    
    # Group by DDL_Action
    ddl_actions = {'MODIFY_RESUSAGE': [], 'REPLACE_LOGGING': [], 'CONFIGURE_THRESHOLD': [], 'MAINTENANCE': []}
    
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
            file_name=f"config_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.sidebar.header("📁 Módulos")
    
    st.title("⚙️ Database Query Logging")
    st.markdown("*Evaluación y Optimización de Configuración de Logging en Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuración")
    
    # Info about ResUsageRules
    st.sidebar.info(
        "ℹ️ **Nota:** La vista DBC.ResUsageRules puede no estar disponible "
        "en todas las versiones de Teradata o puede requerir permisos especiales."
    )
    
    # Execute Analysis Button
    if st.sidebar.button("🚀 Ejecutar Análisis Módulo 5", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("🔌 Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using ConfigCollector
            with st.spinner("📊 Recolectando datos de configuración..."):
                collector = ConfigCollector()
                collected_data = collector.collect(connection)
                st.session_state.mod5_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using ConfigAnalyzer
            with st.spinner("🔍 Analizando datos..."):
                analyzer = ConfigAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod5_analyzed_data = analyzed_data
                st.session_state.mod5_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod5_findings)}")
            
            connection.close()
            st.success(f"✅ Análisis completado. Total hallazgos: {len(st.session_state.mod5_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod5_analyzed_data:
        st.markdown("## 📈 Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod5_analyzed_data)
        st.markdown("---")
        display_config_checklist(st.session_state.mod5_analyzed_data)
        st.markdown("---")
        display_charts(st.session_state.mod5_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod5_analyzed_data)
        st.markdown("---")
        display_ddl_actions(st.session_state.mod5_analyzed_data)


if __name__ == "__main__":
    main()
