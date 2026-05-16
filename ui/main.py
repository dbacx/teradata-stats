"""
TD Stats Optimizer - Main Navigation Entry Point

This module provides the main navigation using Streamlit's st.navigation API
to control the order of pages in the sidebar.
"""

import pandas as pd
import streamlit as st
import sys
import os
import argparse
from dotenv import load_dotenv

# --- CONFIGURACIÓN DINÁMICA DE ENTORNO (MÚLTIPLES CLIENTES) ---
# 1. Capturar argumentos de la línea de comandos
parser = argparse.ArgumentParser(description="Teradata Stats Optimizer")
parser.add_argument(
    "--client", 
    type=str, 
    default="EPM",  # Cliente por defecto si no se especifica en consola
    help="Nombre del cliente para cargar credenciales (ej: EPM, BCI)"
)

# parse_known_args evita que Streamlit colapse con sus comandos internos
args, _ = parser.parse_known_args()

# 2. Add parent directory to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# 3. Construir la ruta al archivo .env y cargar
env_file = os.path.join(project_root, f"{args.client.upper()}.env")

if os.path.exists(env_file):
    # override=True fuerza a que las variables se actualicen si cambias de cliente
    load_dotenv(env_file, override=True)
else:
    # Mostramos el error en la UI de Streamlit en lugar de colapsar la terminal
    st.error(f"🚨 **Error Crítico de Configuración:** No se encontró el archivo de credenciales `{args.client.upper()}.env` en la raíz del proyecto.")
    st.stop()
# -------------------------------------------------------------

# Blindaje contra dataframes masivos en Streamlit
pd.set_option("styler.render.max_elements", 2000000)

# Streamlit page configuration
st.set_page_config(
    page_title=f"TD Stats Optimizer - {args.client.upper()}", # Añadimos el cliente al título
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add logo to sidebar using native st.logo
st.logo("logo.jpg")

# Inject CSS for offline/firewall-proof icon fallback
offline_icons_css = """
<style>
    /* 1. Ocultar el texto crudo de los iconos rotos */
    .material-symbols-rounded, 
    .material-symbols-outlined,
    [data-testid="stSidebarCollapseButton"] span,
    [data-testid="stExpanderToggleIcon"] {
        color: transparent !important;
        font-size: 0px !important;
    }

    /* 2. Reemplazo para el botón del menú lateral (keyboard_double_arrow...) */
    [data-testid="stSidebarCollapseButton"] span::after {
        content: "☰" !important;
        font-size: 1.2rem !important;
        color: #333333 !important;
        visibility: visible !important;
        display: block !important;
    }

    /* 3. Reemplazo para la flecha de los acordeones/expanders (expand_more) */
    [data-testid="stExpanderToggleIcon"]::after {
        content: "▼" !important;
        font-size: 0.8rem !important;
        color: #333333 !important;
        visibility: visible !important;
        display: block !important;
    }

    /* Estilo corporativo global para botones primarios (Teradata Orange) */
    [data-testid="baseButton-primary"] {
        background-color: #FD6724 !important;
        color: white !important;
        border-color: #FD6724 !important;
    }
    [data-testid="baseButton-primary"]:hover {
        background-color: #e55a1d !important;
        border-color: #e55a1d !important;
    }
</style>
"""
st.markdown(offline_icons_css, unsafe_allow_html=True)

# Define pages in the desired order
# Home first, then Health & Connectivity, then other modules
pg_home = st.Page("app.py", title="Home")
pg_health = st.Page("pages/1_System_Information.py", title="System Information")
pg_statistics = st.Page("pages/2_Module_2_Statistics.py", title="Statistics Management")
pg_space = st.Page("pages/2_Space.py", title="Space")
pg_security = st.Page("pages/3_Security.py", title="Security")
pg_dbql = st.Page("pages/4_Database_Query_Logging.py", title="Database Query Logging")
pg_performance = st.Page("pages/5_Performance_Assessment.py", title="Performance Assessment")
pg_schema = st.Page("pages/6_Module_7_Schema.py", title="Schema Design")
pg_hardware = st.Page("pages/7_Module_8_Hardware.py", title="Hardware Utilization")
pg_cleanup = st.Page("pages/8_Module_9_Cleanup.py", title="Cleanup & Cost Optimization")
pg_monthly_report = st.Page("pages/9_Module_10_Monthly_Report.py", title="Monthly Report")

# Create navigation with grouped sections
pages = {
    "": [pg_home],
    "Módulos": [
        pg_health,
        pg_statistics,
        pg_space,
        pg_security,
        pg_dbql,
        pg_performance,
        pg_schema,
        pg_hardware,
        pg_cleanup,
        pg_monthly_report
    ]
}

navigation = st.navigation(pages)

# Run navigation
navigation.run()