"""
VantageOps - Teradata Assessment Suite

This module serves as the primary entry point for the observability and auditing platform.
It provides an enterprise-grade web interface to evaluate cluster health, manage database
statistics, and generate actionable remediation insights for Managed Services and Data Platform operations.
"""


import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.dictionary_ext import extract_database_stats
from analyzers.engine import RulesEngine
from analyzers.rules.rule_01_unused import Rule01Unused
from analyzers.rules.rule_02_sample import Rule02Sample
from analyzers.rules.rule_06_stale import Rule06Stale
from analyzers.rules.rule_15_bloat import Rule15Bloat
from skills.recommender import DDLRecommender


# Configure logging
from core.logging_config import configure_logging
configure_logging()
logger = logging.getLogger(__name__)


def main_app():
    """Main application function for the Home page."""
    inject_custom_css()
    initialize_session_state()
    
    # Corporate Banner
    st.markdown("""
    <div style="background-color: #F0F2F6; padding: 2rem 1.5rem; border-radius: 8px; text-align: center; margin-bottom: 1.5rem;">
        <h2 style="margin: 0 0 0.25rem 0; color: #1C1C1E;">VantageOps</h2>
        <p style="margin: 0; color: #555; font-size: 1rem;">Managed Services &mdash; Teradata DBA Optimization Suite</p>
    </div>
    """, unsafe_allow_html=True)

    # Corporate Welcome Section
    st.markdown("## Teradata DBA Services Framework")
    st.markdown("""
Este framework integral proporciona capacidades de auditoría de base de datos, optimización de estadísticas,
gestión de espacio, evaluación de seguridad y recolección de datos para entornos Teradata.

La herramienta permite a los administradores de bases de datos e ingenieros de datos:
- Analizar y optimizar estadísticas de base de datos para mejorar el rendimiento de consultas
- Monitorear y gestionar la utilización de espacio en base de datos
- Evaluar configuraciones de seguridad y patrones de acceso de usuarios
- Evaluar el registro de consultas y métricas de rendimiento
- Generar recomendaciones DDL accionables para remediación

Para más información sobre soluciones Teradata, visite el [Sitio Web Oficial de Teradata](https://www.teradata.com/).
""")
    st.markdown("---")

    # Copyright
    st.markdown(
        "<p style='text-align: center; font-size: 0.85rem; color: rgba(0,0,0,0.6); margin-top: 2rem;'>"
        "Copyright Teradata Corporation 2026</p>",
        unsafe_allow_html=True,
    )
    
    # Sidebar configuration
    st.sidebar.header("Configuración")
    
    # Analysis Level Selection
    analysis_level = st.sidebar.radio(
        "Nivel de Análisis",
        options=["Sistema Completo", "Base de Datos", "Tabla Específica"],
        help="Selecciona el alcance del análisis"
    )
    
    # Conditional inputs based on analysis level
    database_name = None
    table_name = None
    
    if analysis_level == "Base de Datos":
        database_name = st.sidebar.text_input(
            "Nombre de la Base de Datos",
            value=st.session_state.last_database,
            placeholder="Ej: DBC, USER_DB, etc."
        )
    elif analysis_level == "Tabla Específica":
        database_name = st.sidebar.text_input(
            "Nombre de la Base de Datos",
            value=st.session_state.last_database,
            placeholder="Ej: DBC, USER_DB, etc."
        )
        table_name = st.sidebar.text_input(
            "Nombre de la Tabla",
            placeholder="Ej: Table1, Customer_Table, etc."
        )
    # For "Sistema Completo", both remain None
    
    unused_threshold = st.sidebar.slider(
        "Umbral de Días para Objetos Sin Uso",
        min_value=1,
        max_value=90,
        value=30,
        help="Tablas sin acceso por más de este número de días se considerarán sin uso"
    )
    
    stale_threshold = st.sidebar.slider(
        "Umbral de Días para Stats Desactualizadas",
        min_value=1,
        max_value=90,
        value=15,
        help="Estadísticas más antiguas que este número de días se considerarán desactualizadas"
    )
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Análisis", type="primary"):
        if analysis_level == "Base de Datos" and (not database_name or not database_name.strip()):
            st.sidebar.error("Por favor, ingrese un nombre de base de datos válido")
            return
        if analysis_level == "Tabla Específica" and (not database_name or not database_name.strip() or not table_name or not table_name.strip()):
            st.sidebar.error("Por favor, ingrese nombre de base de datos y tabla válidos")
            return
        
        # Store database name in session state
        st.session_state.last_database = database_name
        
        # Main analysis workflow
        try:
            # Step 1: Extract statistics metadata with multi-level support
            with st.spinner("Extrayendo metadata del diccionario..."):
                logger.info(f"Starting extraction for level: {analysis_level}, database: {database_name}, table: {table_name}")
                df_stats = extract_database_stats(database_name=database_name, table_name=table_name)
            
            if df_stats.empty:
                scope_msg = f"base de datos '{database_name}'" if database_name else "sistema completo"
                st.error(f"No se encontraron estadísticas para {scope_msg}")
                st.session_state.analysis_results = None
                st.session_state.ddl_recommendations = None
                return
            
            # Step 2: Initialize RulesEngine and register all rules
            with st.spinner("Inicializando motor de reglas..."):
                engine = RulesEngine()
                
                # Configure rule-specific parameters
                rule_config = {
                    'rule_01_unused': {'days_threshold': unused_threshold},
                    'rule_06_stale': {'days_threshold': stale_threshold}
                }
                
                # Register all 16 rules automatically
                engine.register_all_rules(config=rule_config)
                
                logger.info(f"Registered {len(engine)} rules")
            
            # Step 3: Execute all rules with timing
            with st.spinner("Ejecutando análisis de reglas..."):
                context = {'stats_df': df_stats}
                start_time = time.time()
                rule_results = engine.run_all(context)
                execution_time = time.time() - start_time
                
                # Store results in session state with all 16 rule results
                st.session_state.analysis_results = {
                    'df_stats': df_stats,
                    'rule_results': rule_results,
                    'analysis_level': analysis_level,
                    'database_name': database_name,
                    'table_name': table_name,
                    'execution_time': execution_time
                }
            
            # Step 4: Generate DDL recommendations
            with st.spinner("Generando recomendaciones DDL..."):
                recommender = DDLRecommender()
                
                # Use rule_16_urgent_missing for COLLECT DDL
                urgent_results = rule_results.get('rule_16_urgent_missing', pd.DataFrame())
                stale_results = rule_results.get('rule_06_stale', pd.DataFrame())
                bloat_results = rule_results.get('rule_15_bloat', pd.DataFrame())
                
                # Generate DDL from urgent results if available
                collect_ddls = []
                if not urgent_results.empty and 'Comando_Collect' in urgent_results.columns:
                    collect_ddls = urgent_results['Comando_Collect'].dropna().tolist()
                elif not stale_results.empty:
                    collect_ddls = recommender.generate_collect_stats(stale_results)
                
                drop_ddls = recommender.generate_drop_stats(bloat_results)
                
                st.session_state.ddl_recommendations = {
                    'collect_stats': collect_ddls,
                    'drop_stats': drop_ddls
                }
            
            scope_msg = f"'{database_name}'" if database_name else "sistema completo"
            if table_name:
                scope_msg += f", tabla '{table_name}'"
            st.success(f"Análisis completado para {scope_msg}")
            
        except ValueError as e:
            st.error(f"Error de validación: {str(e)}")
            logger.error(f"Validation error: {str(e)}")
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.analysis_results:
        st.markdown("## Resultados del Análisis")
        
        results = st.session_state.analysis_results
        df_stats = results['df_stats']
        rule_results = results['rule_results']
        
        # Display execution time
        if 'execution_time' in results:
            execution_time = results['execution_time']
            st.success(f"Tiempo de análisis: {execution_time:.2f} segundos")
        
        # Display overall metrics
        display_overall_metrics(df_stats, rule_results)
        
        st.markdown("---")
        
        # Display results in tabs - all 16 rules plus main stats and DDL
        tabs = st.tabs([
            "Estadísticas",
            "SIN USO",
            "DESACTUALIZADAS",
            "PARTITION",
            "ZERO STATS",
            "TABLE MISSING",
            "INDEX MISSING",
            "SAMPLE",
            "MULTICOLUMN",
            "SKIPPED/SAMPLE",
            "DBC MISSING",
            "REDUNDANT",
            "EXTRAPOLATION",
            "HARDCODED SAMPLE",
            "JOIN COLUMNS",
            "DICTIONARY BLOAT",
            "URGENTES",
            "DDL"
        ])
        
        # Tab 0: Main Statistics
        with tabs[0]:
            st.subheader("Estadísticas Completas")
            if not df_stats.empty:
                display_df = df_stats.copy()
                timestamp_columns = display_df.select_dtypes(include=['datetime64[ns]']).columns
                for col in timestamp_columns:
                    display_df[col] = display_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                st.dataframe(
                    display_df,
                    width='stretch',
                    height=500
                )
            else:
                st.info("No hay estadísticas disponibles para mostrar")
        
        # Tabs 1-16: Individual Rules - reordered by priority
        rule_tabs = [
            (1, 'rule_01_unused', 'Objetos Sin Uso'),
            (2, 'rule_06_stale', 'Estadísticas Desactualizadas'),
            (3, 'rule_03_partition_missing', 'Missing PARTITION Stats'),
            (4, 'rule_07_zero_stats', 'Zero Count Statistics'),
            (5, 'rule_04_table_missing', 'Missing Table-Level Stats'),
            (6, 'rule_05_index_missing', 'Missing Index-Level Stats'),
            (7, 'rule_02_sample', 'Candidatos a Sample'),
            (8, 'rule_08_multicolumn', 'Multicolumn MaxValueLength'),
            (9, 'rule_09_skipped_sample', 'Skipped and Sample Stats'),
            (10, 'rule_10_dbc_missing', 'Missing DBC/PDCR Stats'),
            (11, 'rule_11_redundant', 'Redundant Statistics'),
            (12, 'rule_12_extrapolation', 'Extrapolation Risk'),
            (13, 'rule_13_analyze', 'Hardcoded Samples'),
            (14, 'rule_14_join_columns', 'Missing Key Columns'),
            (15, 'rule_15_bloat', 'Dictionary Bloat'),
            (16, 'rule_16_urgent_missing', 'Urgent Missing Stats')
        ]
        
        for tab_idx, rule_key, rule_name in rule_tabs:
            with tabs[tab_idx]:
                rule_result = rule_results.get(rule_key, pd.DataFrame())
                display_rule_results(rule_key, rule_name, rule_result)
        
        # Tab 17: DDL Recommendations
        with tabs[17]:
            if st.session_state.ddl_recommendations:
                display_ddl_recommendations(st.session_state.ddl_recommendations)
            else:
                st.info("No hay recomendaciones DDL disponibles")
        

    
    # Footer
    st.markdown(
        """
        <hr style="margin-top: 3rem; margin-bottom: 1rem; border-color: rgba(255,255,255,0.1);">
        <div style="text-align: center; font-size: 0.75rem; color: rgba(255,255,255,0.6); line-height: 1.5;">
            © 2026 Teradata. All rights reserved<br>
            Internal use only
        </div>
        """,
        unsafe_allow_html=True
    )


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

# Streamlit page configuration
st.set_page_config(
    page_title="TD Stats Optimizer",
    layout="wide",
    initial_sidebar_state="expanded"
)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'ddl_recommendations' not in st.session_state:
        st.session_state.ddl_recommendations = None
    if 'last_database' not in st.session_state:
        st.session_state.last_database = ""


def display_overall_metrics(df_stats: pd.DataFrame, rule_results: dict):
    """Display overall metrics summary with all 16 rules."""
    # Base metrics
    col1, col2 = st.columns(2)
    
    with col1:
        total_tables = df_stats['TableName'].nunique() if not df_stats.empty else 0
        st.metric("Total Tablas", total_tables)
    
    with col2:
        total_stats = len(df_stats)
        st.metric("Total Estadísticas", total_stats)
    
    st.markdown("---")
    
    # Executive Health KPIs Block
    st.subheader("HEALTH STATUS")
    
    # Calculate health metrics
    from datetime import datetime, timedelta
    
    unused_count = len(rule_results.get('rule_01_unused', pd.DataFrame()))
    stale_count = len(rule_results.get('rule_06_stale', pd.DataFrame()))
    zero_count = len(rule_results.get('rule_07_zero_stats', pd.DataFrame()))
    
    # Calculate percentages
    if total_stats > 0:
        healthy_stats_pct = ((total_stats - stale_count - zero_count) / total_stats) * 100
    else:
        healthy_stats_pct = 0
    
    if total_tables > 0:
        active_tables_pct = ((total_tables - unused_count) / total_tables) * 100
    else:
        active_tables_pct = 0
    
    # Health score (0-100 based on healthy stats percentage)
    health_score = int(healthy_stats_pct)
    
    # Determine color based on score
    if health_score > 80:
        health_status = "EXCELLENT"
    elif health_score >= 50:
        health_status = "WARNING"
    else:
        health_status = "CRITICAL"
    
    # Stats requiring attention
    stats_requiring_attention = stale_count + zero_count
    
    # Next action date (current date + 4 days)
    next_action_date = (datetime.now() + timedelta(days=4)).strftime('%Y-%m-%d')
    
    # Display health block
    st.markdown(f"""
    <div style="background-color: rgba(255,255,255,0.05); padding: 1.5rem; border-radius: 0.5rem; border-left: 4px solid {'#00FF00' if health_score > 80 else '#FFA500' if health_score >= 50 else '#FF0000'};">
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem;">
            <div style="display: flex; align-items: center; gap: 1rem;">
                <div>
                    <div style="font-size: 0.85rem; color: rgba(255,255,255,0.7); margin-bottom: 0.25rem;">SYSTEM HEALTH</div>
                    <div style="font-size: 2rem; font-weight: 600;">{health_score}/100</div>
                </div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 1.25rem; font-weight: 600; color: {'#00FF00' if health_score > 80 else '#FFA500' if health_score >= 50 else '#FF0000'};">{health_status}</div>
            </div>
        </div>
        <div style="font-size: 0.9rem; color: rgba(255,255,255,0.8); line-height: 1.6;">
            <strong>{stats_requiring_attention} statistics require attention</strong> | Next action: {next_action_date}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display percentage metrics
    col3, col4 = st.columns(2)
    with col3:
        st.metric("% Estadísticas Saludables", f"{healthy_stats_pct:.1f}%")
    with col4:
        st.metric("% Tablas Activas", f"{active_tables_pct:.1f}%")
    
    st.markdown("---")
    st.subheader("DASHBOARD STATISTICS STATUS")
    
    # 4x4 grid for 16 rule metrics - reordered by priority
    rule_metrics = [
        ("SIN USO", 'rule_01_unused'),
        ("DESACTUALIZADAS", 'rule_06_stale'),
        ("PARTITION", 'rule_03_partition_missing'),
        ("ZERO STATS", 'rule_07_zero_stats'),
        ("TABLE MISSING", 'rule_04_table_missing'),
        ("INDEX MISSING", 'rule_05_index_missing'),
        ("SAMPLE", 'rule_02_sample'),
        ("MULTICOLUMN", 'rule_08_multicolumn'),
        ("SKIPPED/SAMPLE", 'rule_09_skipped_sample'),
        ("DBC MISSING", 'rule_10_dbc_missing'),
        ("REDUNDANT", 'rule_11_redundant'),
        ("EXTRAPOLATION", 'rule_12_extrapolation'),
        ("HARDCODED SAMPLE", 'rule_13_analyze'),
        ("JOIN COLUMNS", 'rule_14_join_columns'),
        ("DICTIONARY BLOAT", 'rule_15_bloat'),
        ("URGENTES", 'rule_16_urgent_missing')
    ]
    
    # Create 4 rows of 4 columns each
    for row in range(4):
        cols = st.columns(4)
        for col_idx in range(4):
            rule_idx = row * 4 + col_idx
            if rule_idx < len(rule_metrics):
                rule_name, rule_key = rule_metrics[rule_idx]
                count = len(rule_results.get(rule_key, pd.DataFrame()))
                with cols[col_idx]:
                    # Highlight urgent rules with different color
                    if rule_key == 'rule_16_urgent_missing' and count > 0:
                        st.metric(rule_name, count, delta_color="inverse")
                    else:
                        st.metric(rule_name, count)


def display_rule_results(rule_id: str, rule_name: str, result_df: pd.DataFrame):
    """Display results for a specific rule."""
    if result_df.empty:
        st.success(f"No se encontraron hallazgos para {rule_name}")
        return
    
    st.subheader(f"{rule_name}")
    
    # Format timestamps for better display
    display_df = result_df.copy()
    timestamp_columns = display_df.select_dtypes(include=['datetime64[ns]']).columns
    for col in timestamp_columns:
        display_df[col] = display_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    st.dataframe(
        display_df,
        width='stretch',
        height=400
    )
    
    # Show summary statistics
    with st.expander(f"Resumen de {rule_name}"):
        if 'DatabaseName' in display_df.columns:
            db_count = display_df['DatabaseName'].nunique()
            st.write(f"**Bases de datos afectadas:** {db_count}")
        
        if 'TableName' in display_df.columns:
            table_count = display_df['TableName'].nunique()
            st.write(f"**Tablas afectadas:** {table_count}")
        
        st.write(f"**Total de hallazgos:** {len(display_df)}")


def display_ddl_recommendations(ddl_recommendations: dict):
    """Display generated DDL recommendations."""
    st.subheader("Recomendaciones DDL")
    
    if not ddl_recommendations:
        st.info("No hay recomendaciones DDL disponibles")
        return
    
    # Display COLLECT STATISTICS
    if ddl_recommendations.get('collect_stats'):
        st.write("**COLLECT STATISTICS (Actualización):**")
        # Filter out None values before joining
        collect_statements = [stmt for stmt in ddl_recommendations['collect_stats'] if stmt is not None]
        if collect_statements:
            collect_sql = "\n".join(collect_statements)
            st.code(collect_sql, language='sql')
            st.download_button(
                label="Descargar COLLECT STATS",
                data=collect_sql,
                file_name=f"collect_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                mime="text/plain"
            )
        else:
            st.info("No hay sentencias COLLECT STATISTICS válidas")
    
    # Display DROP STATISTICS
    if ddl_recommendations.get('drop_stats'):
        st.write("**DROP STATISTICS (Limpieza):**")
        # Filter out None values before joining
        drop_statements = [stmt for stmt in ddl_recommendations['drop_stats'] if stmt is not None]
        if drop_statements:
            drop_sql = "\n".join(drop_statements)
            st.code(drop_sql, language='sql')
            st.download_button(
                label="Descargar DROP STATS",
                data=drop_sql,
                file_name=f"drop_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                mime="text/plain"
            )
        else:
            st.info("No hay sentencias DROP STATISTICS válidas")
    
    # Display combined recommendations
    if ddl_recommendations.get('collect_stats') or ddl_recommendations.get('drop_stats'):
        st.write("**Todas las Recomendaciones:**")
        recommender = DDLRecommender()
        combined_output = recommender.format_ddl_output(ddl_recommendations)
        st.code(combined_output, language='sql')
        st.download_button(
            label="Descargar Todas las Recomendaciones",
            data=combined_output,
            file_name=f"td_stats_recommendations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


if __name__ == "__main__":
    main_app()
