"""
Module 6: User & Security Management Page

This page provides a dedicated interface for the Security Assessment module,
using the new SecurityCollector and SecurityAnalyzer classes.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging
import sys
import os
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.connection import TeradataConnection
from collectors.mod10_security_collector import SecurityCollector
from analyzers.mod10_security_analyzer import SecurityAnalyzer
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
    if 'mod6_collected_data' not in st.session_state:
        st.session_state.mod6_collected_data = None
    if 'mod6_analyzed_data' not in st.session_state:
        st.session_state.mod6_analyzed_data = None
    if 'mod6_findings' not in st.session_state:
        st.session_state.mod6_findings = None


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards for security assessment."""
    st.subheader("KPI Cards - Security Assessment")
    
    # Calculate metrics
    password_df = analyzed_data.get('01_password_expiry', pd.DataFrame())
    profile_df = analyzed_data.get('02_users_without_profile', pd.DataFrame())
    direct_grants_df = analyzed_data.get('03_direct_grants', pd.DataFrame())
    stagnant_df = analyzed_data.get('04_stagnant_users', pd.DataFrame())
    role_df = analyzed_data.get('05_users_without_role', pd.DataFrame())
    
    # Users at Risk (password expiry + stagnant users)
    users_at_risk = len(password_df) + len(stagnant_df)
    
    # Direct Grants Detected
    direct_grants_count = len(direct_grants_df)
    
    # Inactive Accounts (stagnant users)
    inactive_accounts = len(stagnant_df)
    
    # Users Without Profile
    without_profile = len(profile_df)
    
    # Users Without Role
    without_role = len(role_df)
    
    # Display KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Usuarios en Riesgo", users_at_risk)
    with col2:
        st.metric("Permisos Directos", direct_grants_count)
    with col3:
        st.metric("Cuentas Inactivas", inactive_accounts)
    with col4:
        st.metric("Sin Profile", without_profile)
    with col5:
        st.metric("Sin Rol", without_role)
    
    st.markdown("---")


def display_charts(analyzed_data: dict):
    """Display charts for security assessment."""
    st.subheader("Visualizaciones")
    
    col1, col2 = st.columns(2)
    
    # Donut Chart: Healthy vs At-Risk Users
    with col1:
        st.write("**Usuarios Sanos vs en Riesgo**")
        password_df = analyzed_data.get('01_password_expiry', pd.DataFrame())
        stagnant_df = analyzed_data.get('04_stagnant_users', pd.DataFrame())
        
        if not password_df.empty and not stagnant_df.empty:
            at_risk = len(password_df) + len(stagnant_df)
            # Assume total users estimate (this would ideally come from a total count query)
            total_users = max(at_risk + 50, 100)  # Placeholder for total users
            healthy = total_users - at_risk
            
            fig = go.Figure(data=[go.Pie(
                labels=['Sanos', 'En Riesgo'],
                values=[healthy, at_risk],
                hole=0.4,
                marker=dict(colors=['#90EE90', '#FF6B6B'])
            )])
            
            fig.update_layout(
                title="Distribución de Usuarios",
                showlegend=True,
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos suficientes para generar el gráfico de distribución de usuarios.")
    
    # Bar Chart: Direct Grants by User
    with col2:
        st.write("**Permisos Directos por Usuario (Top 10)**")
        direct_grants_df = analyzed_data.get('03_direct_grants', pd.DataFrame())
        
        if not direct_grants_df.empty and 'UserName' in direct_grants_df.columns:
            grants_by_user = direct_grants_df.groupby('UserName').size().reset_index(name='GrantCount')
            top_10 = grants_by_user.nlargest(10, 'GrantCount')
            top_10 = top_10.sort_values('GrantCount', ascending=True)
            
            st.bar_chart(
                data=top_10,
                x='GrantCount',
                y='UserName',
                horizontal=True,
                width='stretch'
            )
        else:
            st.warning("No hay datos suficientes o faltan columnas para generar el gráfico de permisos directos.")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")
    
    # Combine all findings into a single DataFrame
    all_findings = []
    
    component_names = {
        "01_password_expiry": "Password Expiry",
        "02_users_without_profile": "Users Without Profile",
        "03_direct_grants": "Direct Grants",
        "04_stagnant_users": "Stagnant Users",
        "05_users_without_role": "Users Without Role"
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
        file_name=f"security_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements."""
    st.subheader("Acciones DDL de Remediación")
    
    component_names = {
        "01_password_expiry": "Password Expiry",
        "02_users_without_profile": "Users Without Profile",
        "03_direct_grants": "Direct Grants",
        "04_stagnant_users": "Stagnant Users",
        "05_users_without_role": "Users Without Role"
    }
    
    # Group by DDL_Action
    ddl_actions = {'EXPIRE_PASSWORD': [], 'ASSIGN_PROFILE': [], 'USE_ROLES': [], 'REVOKE_LOGON': [], 'ASSIGN_ROLE': []}
    
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
            file_name=f"security_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("Security")
    st.markdown("*Evaluación y Optimización de Seguridad en Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    # Threshold configuration
    password_expiry_days = st.sidebar.slider(
        "Umbral Días para Expiración de Password",
        min_value=30,
        max_value=365,
        value=THRESHOLDS.get('password_expiry_days', 90),
        help="Usuarios con password sin cambio por más de este número de días se considerarán en riesgo"
    )
    
    unused_days = st.sidebar.slider(
        "Umbral Días para Usuarios Inactivos",
        min_value=30,
        max_value=365,
        value=THRESHOLDS.get('unused_object_days', 30),
        help="Usuarios sin acceso por más de este número de días se considerarán inactivos"
    )
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Analisis", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using SecurityCollector
            with st.spinner("Recolectando datos de seguridad..."):
                collector = SecurityCollector()
                params = {
                    'password_expiry_days_threshold': password_expiry_days,
                    'unused_days_threshold': unused_days
                }
                collected_data = collector.collect(connection, params=params)
                st.session_state.mod6_collected_data = collected_data
                
                total_rows = sum(len(df) for df in collected_data.values())
                logger.info(f"Collected {total_rows} rows from {len(collected_data)} components")
            
            # Step 3: Analyze data using SecurityAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = SecurityAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod6_analyzed_data = analyzed_data
                st.session_state.mod6_findings = analyzer.get_findings()
                
                logger.info(f"Analysis complete. Total findings: {len(st.session_state.mod6_findings)}")
            
            connection.close()
            st.success(f"Análisis completado. Total hallazgos: {len(st.session_state.mod6_findings)}")
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod6_analyzed_data:
        st.markdown("## Resultados del Análisis")
        
        display_kpi_cards(st.session_state.mod6_analyzed_data)
        st.markdown("---")
        display_charts(st.session_state.mod6_analyzed_data)
        st.markdown("---")
        display_findings_table(st.session_state.mod6_analyzed_data)
        st.markdown("---")
        display_ddl_actions(st.session_state.mod6_analyzed_data)


if __name__ == "__main__":
    main()
