"""
Module 1: System Information Page

This page provides a dedicated interface for System Information module,
using SystemInformationCollector and SystemInformationAnalyzer classes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
current_file_path = Path(__file__).resolve()
project_root = str(current_file_path.parent.parent.parent)

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.connection import TeradataConnection
from collectors.mod1_health_collector import SystemInformationCollector
from analyzers.mod1_health_analyzer import SystemInformationAnalyzer
from utils.csv_logger import log_execution

# Configure logging
from core.logging_config import configure_logging
configure_logging()
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
    if 'mod1_collected_data' not in st.session_state:
        st.session_state.mod1_collected_data = None
    if 'mod1_analyzed_data' not in st.session_state:
        st.session_state.mod1_analyzed_data = None


def main():
    """Main page entry point."""
    inject_custom_css()
    initialize_session_state()
    
    st.title("System Information")
    st.markdown("*Información de la capacidad del sistema*")
    st.markdown("---")
    
    # Execute Analysis Button
    if st.sidebar.button("Ejecutar Analisis", type="primary"):
        try:
            # Step 1: Connect to database
            with st.spinner("Conectando a Teradata..."):
                td_conn = TeradataConnection()
                connection = td_conn.connect()
                logger.info("Connected to Teradata")
            
            # Step 2: Collect data using SystemInformationCollector
            with st.spinner("Recolectando información del sistema..."):
                collector = SystemInformationCollector()
                collected_data = collector.collect(connection, params={})
                st.session_state.mod1_collected_data = collected_data
                logger.info(f"Collected {len(collected_data)} rows from system information")
            
            # Step 3: Analyze data using SystemInformationAnalyzer
            with st.spinner("Analizando datos..."):
                analyzer = SystemInformationAnalyzer()
                analyzed_data = analyzer.run(collected_data)
                st.session_state.mod1_analyzed_data = analyzed_data
                logger.info("Analysis complete")
            
            connection.close()
            
            # Log execution
            log_execution(
                ticket="System_Information",
                project_name="Teradata DBA Services",
                system_name="Production",
                resource_name="TD_PROD",
                issue="System Information Query",
                request_by="System",
                comments="System information analysis completed"
            )
            
            # Custom success message
            st.markdown(
                "<div style='background-color: #d1e7dd; color: #0f5132; padding: 6px 12px; "
                "border-radius: 4px; font-size: 0.85rem; border: 1px solid #badbcc; "
                "margin-top: 10px;'>Análisis completado exitosamente.</div>", 
                unsafe_allow_html=True
            )
            
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.mod1_analyzed_data is not None:
        df = st.session_state.mod1_analyzed_data
        
        if not df.empty:
            st.markdown("## Resultados del Análisis")

            # ── METRIC CARD DISPLAY ──────────────────────────────────────
            METRIC_CONFIG = [
                ("NodeType",        "memory",                   "Versión / Tipo de Nodo",   "Sistema"),
                ("Logdate",         "calendar_today",           "Fecha de Registro",        "Fecha"),
                ("Nodes",           "device_hub",               "Nodos",                    "Infraestructura"),
                ("AMPs",            "settings_input_component", "AMPs Totales",             "Procesamiento"),
                ("NodeAMPs",        "calculate",                "AMPs por Nodo",            "Procesamiento"),
                ("PEs",             "sync_alt",                 "Parsing Engines (PEs)",    "Procesamiento"),
                ("Gateways",        "router",                   "Gateways",                 "Red"),
                ("SystemSpace_TBs", "storage",                  "Espacio del Sistema (TB)", "Almacenamiento"),
                ("AMPSpace_GBs",    "sd_storage",               "Espacio por AMP (GB)",     "Almacenamiento"),
                ("CPUSec_Hr",       "speed",                    "CPU Seg/Hora",             "Rendimiento"),
            ]
            known_cols = {c[0] for c in METRIC_CONFIG}

            st.markdown("""
            <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@48,400,0,0&display=block" rel="stylesheet" />
            <style>
            .td-metric-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                gap: 1.2rem;
                margin-top: 1rem;
            }
            .td-metric-card {
                background: #ffffff;
                border: 1px solid #e8e8e8;
                border-radius: 12px;
                padding: 1.2rem 1rem 1rem 1rem;
                display: flex;
                flex-direction: column;
                gap: 0.4rem;
            }
            .td-metric-card .td-icon {
                font-family: 'Material Symbols Outlined';
                font-size: 2rem;
                color: #F37021;
                line-height: 1;
            }
            .td-metric-card .td-value {
                font-size: 1.6rem;
                font-weight: 600;
                color: #1C1C1E;
                line-height: 1.2;
                word-break: break-all;
            }
            .td-metric-card .td-label {
                font-size: 0.78rem;
                color: #6b6b6b;
                line-height: 1.3;
            }
            .td-metric-card .td-category {
                font-size: 0.68rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.06em;
                color: #F37021;
                margin-top: 0.2rem;
            }
            </style>
            """, unsafe_allow_html=True)

            row = df.iloc[0] if not df.empty else {}
            cards_html = '<div class="td-metric-grid">'
            for col, icon, label, category in METRIC_CONFIG:
                if col in df.columns:
                    value = row.get(col, "—")
                    if value is None or str(value).strip() == "":
                        value = "—"
                    cards_html += f"""
                    <div class="td-metric-card">
                        <span class="td-icon">{icon}</span>
                        <div class="td-value">{value}</div>
                        <div class="td-label">{label}</div>
                        <div class="td-category">{category}</div>
                    </div>"""

            for col in df.columns:
                if col not in known_cols:
                    value = row.get(col, "—")
                    if value is None or str(value).strip() == "":
                        value = "—"
                    cards_html += f"""
                    <div class="td-metric-card">
                        <span class="td-icon">info</span>
                        <div class="td-value">{value}</div>
                        <div class="td-label">{col}</div>
                        <div class="td-category">Otros</div>
                    </div>"""

            cards_html += '</div>'
            st.markdown(cards_html, unsafe_allow_html=True)

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Descargar CSV",
                data=csv,
                file_name=f"system_information_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No se encontraron datos de información del sistema")


if __name__ == "__main__":
    main()
