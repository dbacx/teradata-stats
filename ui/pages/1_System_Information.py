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

from core.connection import create_connection_from_params
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
                params = st.session_state.get("td_params", {})
                if not params:
                    st.error("Conecta primero desde el sidebar.")
                    st.stop()
                connection = create_connection_from_params(params)
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

            # ── TERADATA CORPORATE DASHBOARD ─────────────────────────────

            row = df.iloc[0]

            def val(col, default="—"):
                v = row.get(col, default)
                if v is None or str(v).strip() == "":
                    return default
                try:
                    n = float(str(v).replace(",", ""))
                    if n >= 1000:
                        return f"{n:,.0f}"
                except Exception:
                    pass
                return str(v)

            # Step 1: inject CSS separately (no f-string)
            st.markdown("""
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=block" rel="stylesheet"/>
            <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@48,400,0,0&display=block" rel="stylesheet"/>
            <style>
            .td-dash * { box-sizing: border-box; font-family: 'Inter', sans-serif; }
            .td-dash { width: 100%; padding: 0.5rem 0 1.5rem 0; }
            .td-top-row {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 1rem;
                margin-bottom: 1.4rem;
            }
            .td-info-card {
                background: #FFFFFF;
                border: 1.5px solid #e2e8f0;
                border-top: 3px solid #FF5F02;
                border-radius: 10px;
                padding: 1rem 1.1rem;
                display: flex;
                align-items: flex-start;
                gap: 0.75rem;
            }
            .td-info-card .td-icon {
                font-family: 'Material Symbols Outlined';
                font-size: 1.8rem;
                color: #FF5F02;
                flex-shrink: 0;
                line-height: 1;
                margin-top: 2px;
            }
            .td-info-card .td-ic-label {
                font-size: 0.68rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: #6b7280;
                margin-bottom: 0.2rem;
            }
            .td-info-card .td-ic-value {
                font-size: 1.15rem;
                font-weight: 700;
                color: #00233C;
                line-height: 1.2;
            }
            .td-body-row {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 1.1rem;
            }
            .td-panel {
                background: #f0f4f8;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                overflow: hidden;
            }
            .td-panel-header {
                background: #00233C;
                padding: 0.65rem 1rem;
                display: flex;
                align-items: center;
                gap: 0.5rem;
            }
            .td-panel-header .td-ph-icon {
                font-family: 'Material Symbols Outlined';
                font-size: 1.1rem;
                color: #FF5F02;
                line-height: 1;
            }
            .td-panel-header .td-ph-title {
                font-size: 0.78rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                color: #FFFFFF;
            }
            .td-panel-body {
                padding: 0.85rem;
                display: flex;
                flex-direction: column;
                gap: 0.65rem;
            }
            .td-kpi {
                background: #FFFFFF;
                border: 1px solid #e9edf2;
                border-radius: 8px;
                padding: 0.75rem 0.9rem;
                display: flex;
                align-items: center;
                gap: 0.75rem;
            }
            .td-kpi .td-kpi-icon {
                font-family: 'Material Symbols Outlined';
                font-size: 1.5rem;
                color: #FF5F02;
                flex-shrink: 0;
                line-height: 1;
            }
            .td-kpi .td-kpi-label {
                font-size: 0.65rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.07em;
                color: #9ca3af;
                line-height: 1;
                margin-bottom: 0.25rem;
            }
            .td-kpi .td-kpi-value {
                font-size: 1.3rem;
                font-weight: 700;
                color: #00233C;
                line-height: 1.1;
            }
            @media (max-width: 900px) {
                .td-top-row, .td-body-row { grid-template-columns: 1fr; }
            }
            </style>
            """, unsafe_allow_html=True)

            # Step 2: inject HTML with Python variables (f-string)
            html = f"""
            <div class="td-dash">
              <div class="td-top-row">
                <div class="td-info-card">
                  <span class="td-icon">info</span>
                  <div>
                    <div class="td-ic-label">Teradata Version</div>
                    <div class="td-ic-value">{val('VERSION')}</div>
                  </div>
                </div>
                <div class="td-info-card">
                  <span class="td-icon">dns</span>
                  <div>
                    <div class="td-ic-label">Instance Type (Node)</div>
                    <div class="td-ic-value">{val('NodeType')}</div>
                  </div>
                </div>
                <div class="td-info-card">
                  <span class="td-icon">calendar_today</span>
                  <div>
                    <div class="td-ic-label">Logdate</div>
                    <div class="td-ic-value">{val('Logdate')}</div>
                  </div>
                </div>
              </div>
              <div class="td-body-row">
                <div class="td-panel">
                  <div class="td-panel-header">
                    <span class="td-ph-icon">hub</span>
                    <span class="td-ph-title">Cluster Topology</span>
                  </div>
                  <div class="td-panel-body">
                    <div class="td-kpi">
                      <span class="td-kpi-icon">storage</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Nodes</div>
                        <div class="td-kpi-value">{val('Nodes')}</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">settings_input_component</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Total AMPs</div>
                        <div class="td-kpi-value">{val('AMPs')}</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">memory</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Total PEs</div>
                        <div class="td-kpi-value">{val('PEs')}</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">router</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Gateways</div>
                        <div class="td-kpi-value">{val('Gateways')}</div>
                      </div>
                    </div>
                  </div>
                </div>
                <div class="td-panel">
                  <div class="td-panel-header">
                    <span class="td-ph-icon">speed</span>
                    <span class="td-ph-title">Capacity &amp; Performance</span>
                  </div>
                  <div class="td-panel-body">
                    <div class="td-kpi">
                      <span class="td-kpi-icon">hard_drive</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Total System Space</div>
                        <div class="td-kpi-value">{val('SystemSpace_TBs')} TB</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">sd_storage</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">AMP Space</div>
                        <div class="td-kpi-value">{val('AMPSpace_GBs')} GB</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">bolt</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Compute Capacity (CPU Sec/Hr)</div>
                        <div class="td-kpi-value">{val('CPUSec_Hr')}</div>
                      </div>
                    </div>
                  </div>
                </div>
                <div class="td-panel">
                  <div class="td-panel-header">
                    <span class="td-ph-icon">developer_board</span>
                    <span class="td-ph-title">Node Specifications</span>
                  </div>
                  <div class="td-panel-body">
                    <div class="td-kpi">
                      <span class="td-kpi-icon">memory_alt</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">Memoria por Nodo</div>
                        <div class="td-kpi-value">{val('NodeMemSize_GBs')} GB</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">cpu</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">CPUs por Nodo</div>
                        <div class="td-kpi-value">{val('NodeCPUs')}</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">account_tree</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">AMPs por Nodo</div>
                        <div class="td-kpi-value">{val('NodeAMPs')}</div>
                      </div>
                    </div>
                    <div class="td-kpi">
                      <span class="td-kpi-icon">sync_alt</span>
                      <div class="td-kpi-text">
                        <div class="td-kpi-label">PEs por Nodo</div>
                        <div class="td-kpi-value">{val('NodePEs')}</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            """

            st.markdown(html, unsafe_allow_html=True)

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
