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
import time
import json
from pathlib import Path

# ---------------------------------------------------------
# BULLETPROOF PATH ROUTING
# ---------------------------------------------------------
# Sube exactamente 2 niveles desde ui/pages/2_Module_...py hasta teradata-stats/
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)  # insert(0) fuerza a Python a buscar aquí primero

# Ahora sí, importaciones locales
from core.connection import create_connection_from_params
from collectors.mod2_stats_collector import StatsCollector
from analyzers.mod2_stats_analyzer import StatsAnalyzer, COMPONENT_LABELS, DDL_COLUMNS
from core.config import THRESHOLDS, SYSTEM_DATABASES
from utils.csv_logger import log_execution, log_analysis_result

# Configure logging
from core.logging_config import configure_logging
configure_logging()
logger = logging.getLogger(__name__)

# Contrato de severidades — orden estricto de criticidad
SEVERITY_ORDER = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO']

# Orden de renderizado de las 15 categorías (criticidad descendente)
COMPONENT_RENDER_ORDER = [
    ("Zero Statistics",        "07_statistics_zero_stats"),           # CRITICAL
    ("Missing Table Stats",    "04_statistics_missing_table"),        # CRITICAL
    ("Sampled Skew",           "13_statistics_sampled_skew"),         # CRITICAL
    ("Stale by Volume",        "14_statistics_stale_by_volume"),      # CRITICAL
    ("Missing Index Stats",    "05_statistics_missing_index"),        # CRITICAL
    ("Missing PARTITION",      "03_statistics_missing_partition"),    # HIGH
    ("MLPPI Missing Levels",   "11_statistics_mlppi_missing_levels"), # HIGH
    ("DBC Recommendations",    "10_statistics_dbc_recommendations"),  # HIGH
    ("Multicolumn Issues",     "08_statistics_multicolumn"),          # MEDIUM
    ("Statistics Bloat",       "12_statistics_bloat"),                # MEDIUM
    ("Sample Candidates",      "02_statistics_sample_candidates"),    # MEDIUM
    ("Skipped/Sample",         "09_statistics_skipped_sample"),       # MEDIUM
    ("Unused Objects",         "01_statistics_unused_objects"),       # LOW
    ("Stale Statistics",       "06_statistics_stale_stats"),          # LOW
    ("TDStats Recommendations","15_tdstats_recommendations"),         # INFO
]


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


def _normalize_to_dataframe(result) -> pd.DataFrame:
    """Normalize a result (DataFrame, list, or dict) into a single DataFrame."""
    if isinstance(result, pd.DataFrame):
        return result
    if isinstance(result, list):
        return pd.DataFrame(result) if result else pd.DataFrame()
    if isinstance(result, dict):
        frames = [v for v in result.values() if isinstance(v, pd.DataFrame) and not v.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return pd.DataFrame()


def _get_ddl_values(df: pd.DataFrame) -> list:
    """Extract DDL values from a DataFrame, checking both RemediationDDL and Action_SQL."""
    for col in DDL_COLUMNS:
        if col in df.columns:
            return df[col].dropna().tolist()
    return []


def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards — strictly 5 severity columns in criticality order."""
    st.subheader("KPI Cards - Statistics Management")
    
    severity_counts = {sev: 0 for sev in SEVERITY_ORDER}
    total_findings = 0
    
    for _, component_key in COMPONENT_RENDER_ORDER:
        df = _normalize_to_dataframe(analyzed_data.get(component_key, pd.DataFrame()))
        if not df.empty and 'Severity' in df.columns:
            for severity in SEVERITY_ORDER:
                severity_counts[severity] += len(df[df['Severity'] == severity])
            total_findings += len(df)
    
    # Contrato AC-01: exactamente 5 columnas (st.columns(5))
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
    
    # Component counts — 15 categories in 5-column grid
    cols = st.columns(5)
    for i, (component_name, component_key) in enumerate(COMPONENT_RENDER_ORDER):
        df = _normalize_to_dataframe(analyzed_data.get(component_key, pd.DataFrame()))
        count = len(df)
        with cols[i % 5]:
            st.metric(component_name, count)
    
    st.markdown(f"**Total de Hallazgos:** {total_findings}")


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")
    
    all_findings = []
    
    for component_name, component_key in COMPONENT_RENDER_ORDER:
        result = analyzed_data.get(component_key, pd.DataFrame())
        if isinstance(result, dict):
            for query_name, df_result in result.items():
                if isinstance(df_result, pd.DataFrame) and not df_result.empty:
                    df_copy = df_result.copy()
                    df_copy['Component'] = f"{component_name} - {query_name}"
                    all_findings.append(df_copy)
        else:
            df = _normalize_to_dataframe(result)
            if not df.empty:
                df_copy = df.copy()
                df_copy['Component'] = component_name
                all_findings.append(df_copy)
    
    if not all_findings:
        st.success("No se encontraron hallazgos")
        return
    
    combined_df = pd.concat(all_findings, ignore_index=True)
    
    # Severity filter — only 5 levels
    severity_filter = st.multiselect(
        "Filtrar por Severidad",
        options=SEVERITY_ORDER,
        default=SEVERITY_ORDER
    )
    
    if severity_filter and 'Severity' in combined_df.columns:
        combined_df = combined_df[combined_df['Severity'].isin(severity_filter)]
    
    # Component filter
    component_labels = [name for name, _ in COMPONENT_RENDER_ORDER]
    component_filter = st.multiselect(
        "Filtrar por Componente",
        options=component_labels,
        default=component_labels
    )
    
    if component_filter and 'Component' in combined_df.columns:
        combined_df = combined_df[combined_df['Component'].isin(component_filter)]
    
    # Conditional formatting
    def highlight_severity(val):
        if val == 'CRITICAL':
            return 'background-color: #FF6B6B; color: white; font-weight: bold'
        elif val == 'HIGH':
            return 'background-color: #FFA500; color: white; font-weight: bold'
        elif val == 'MEDIUM':
            return 'background-color: #FFD700; color: black'
        elif val == 'LOW':
            return 'background-color: #90EE90; color: black'
        return ''
    
    if 'Severity' in combined_df.columns:
        styled_df = combined_df.style.applymap(highlight_severity, subset=['Severity'])
    else:
        styled_df = combined_df.style

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


def _load_kb_index() -> dict:
    """Load knowledge_base/index.json mapping categories to KB metadata."""
    kb_index_path = Path(__file__).resolve().parent.parent.parent / "knowledge_base" / "index.json"
    try:
        with open(kb_index_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"KB index not loaded: {e}")
        return {}


def _load_kb_script(script_file: str) -> str:
    """Read a KB SQL script from knowledge_base/[script_file]."""
    kb_script_path = Path(__file__).resolve().parent.parent.parent / "knowledge_base" / script_file
    try:
        with open(kb_script_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning(f"KB script not found: {kb_script_path}")
        return ""


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation scripts grouped by component (read-only).

    For categories registered in knowledge_base/index.json, the KB script
    is rendered once inside an st.expander (deduplicated, not per row).
    For categories not in the KB, the per-row RemediationDDL from the
    DataFrame is rendered as before.
    """
    st.subheader("Scripts de Remediación")

    kb_index = _load_kb_index()
    rendered_kb_categories = set()

    has_any_ddl = False
    all_ddl_lines = []

    for component_name, component_key in COMPONENT_RENDER_ORDER:
        df = _normalize_to_dataframe(analyzed_data.get(component_key, pd.DataFrame()))
        if df.empty:
            continue

        # --- KB path: render once per category via st.expander ---
        if component_name in kb_index and component_name not in rendered_kb_categories:
            kb_entry = kb_index[component_name]
            script_content = _load_kb_script(kb_entry["script_file"])
            if script_content:
                has_any_ddl = True
                rendered_kb_categories.add(component_name)
                expander_title = f"{kb_entry['id']} - {kb_entry['description']}"
                with st.expander(expander_title):
                    st.code(script_content, language="sql")
                all_ddl_lines.append(f"-- {expander_title}")
                all_ddl_lines.append(script_content)
            continue

        # --- Fallback: per-row DDL from DataFrame ---
        ddl_values = _get_ddl_values(df)
        if not ddl_values:
            continue

        has_any_ddl = True
        st.markdown(f"#### {component_name}")
        component_ddl = "\n".join(ddl_values)
        st.code(component_ddl, language="sql")

        all_ddl_lines.extend(ddl_values)

    if has_any_ddl:
        st.markdown("---")
        combined_ddl = "\n".join(all_ddl_lines)
        st.download_button(
            label="Descargar Todos los Scripts DDL",
            data=combined_ddl,
            file_name=f"stats_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )
    else:
        st.info("No se generaron scripts de remediación.")


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
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar", type="primary"):
        try:
            start_time = time.time()

            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                params = st.session_state.get("td_params", {})
                if not params:
                    st.error("Conecta primero desde el sidebar.")
                    st.stop()
                connection = create_connection_from_params(params)
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using StatsCollector
            with st.spinner("Recolectando datos de estadísticas..."):
                collector = StatsCollector()
                params = {
                    'stale_days_threshold': stale_days,
                    'max_value_length_threshold': 25
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
            
            # Log execution
            log_execution(
                ticket="Statistics_Management",
                project_name="Teradata DBA Services",
                system_name="Production",
                resource_name="TD_PROD",
                issue="Statistics Analysis",
                request_by="System",
                comments=f"Statistics analysis completed with {len(st.session_state.mod2_findings)} findings"
            )
            
            elapsed_time = time.time() - start_time

            # Custom success message
            st.markdown(
                "<div style='background-color: #d1e7dd; color: #0f5132; padding: 6px 12px; "
                "border-radius: 4px; font-size: 0.85rem; border: 1px solid #badbcc; "
                "margin-top: 10px;'>Análisis completado exitosamente.</div>", 
                unsafe_allow_html=True
            )

            # --- Log analysis result to history CSV ---
            findings = st.session_state.mod2_findings

            def _count_by_type(findings_list, finding_type):
                return sum(1 for f in findings_list if f.get("finding_type") == finding_type)

            def _count_by_severity(findings_list, severity):
                return sum(1 for f in findings_list if f.get("severity", "").upper() == severity)

            # Count COLLECT / DROP DDLs across all analyzed components
            collect_ddl_count = 0
            drop_ddl_count = 0
            for _comp_key, _comp_df in analyzed_data.items():
                for ddl_val in _get_ddl_values(_normalize_to_dataframe(_comp_df)):
                    upper_ddl = ddl_val.strip().upper()
                    if upper_ddl.startswith("COLLECT"):
                        collect_ddl_count += 1
                    elif upper_ddl.startswith("DROP"):
                        drop_ddl_count += 1

            analysis_record = {
                "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "customer":         st.session_state.get("td_params", {}).get("customer", "unknown"),
                "site_id":          st.session_state.get("td_params", {}).get("host", "unknown"),
                "system":           st.session_state.get("td_params", {}).get("system", "unknown"),
                "database_filter":  database_name if database_name else "ALL",
                "total_tables":     total_rows,
                "zero_stats":       _count_by_type(findings, "Zero Statistics"),
                "missing_table":    _count_by_type(findings, "Missing Table Stats"),
                "stale_stats":      _count_by_type(findings, "Stale Statistics"),
                "bloat":            _count_by_type(findings, "Statistics Bloat"),
                "total_findings":   len(findings),
                "critical_count":   _count_by_severity(findings, "CRITICAL"),
                "high_count":       _count_by_severity(findings, "HIGH"),
                "medium_count":     _count_by_severity(findings, "MEDIUM"),
                "low_count":        _count_by_severity(findings, "LOW"),
                "collect_ddls":     collect_ddl_count,
                "drop_ddls":        drop_ddl_count,
                "execution_seconds": round(elapsed_time, 2),
            }

            try:
                log_analysis_result(analysis_record)
            except Exception as log_err:
                st.warning(f"Historial no registrado: {log_err}")

        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod2_analyzed_data:
        findings = st.session_state.mod2_findings

        # Validacion segura independiente del tipo de dato (isinstance)
        if isinstance(findings, pd.DataFrame):
            has_findings = not findings.empty
        elif isinstance(findings, (list, dict)):
            has_findings = len(findings) > 0
        else:
            has_findings = bool(findings)

        if has_findings:
            st.markdown("## Resultados del Análisis")
            
            # Contrato AC-02: ESTRICTAMENTE 2 tabs, sin "Datos Analizados"
            tab1, tab2 = st.tabs(["Hallazgos", "Scripts de Remediación"])
            
            with tab1:
                display_kpi_cards(st.session_state.mod2_analyzed_data)
                display_findings_table(st.session_state.mod2_analyzed_data)
            
            with tab2:
                display_ddl_actions(st.session_state.mod2_analyzed_data)


if __name__ == "__main__":
    main()
