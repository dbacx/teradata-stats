"""
VantageOps - Main Navigation Entry Point

This module provides the main navigation using Streamlit's st.navigation API
to control the order of pages in the sidebar.
"""

import pandas as pd
import streamlit as st
import sys
import os
import argparse
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Cached helpers (executed once per process, not on every rerun)
# ---------------------------------------------------------------------------

@st.cache_resource
def load_client_environment():
    """Parse CLI args, configure sys.path, and load the client .env file.

    Returns the client name (e.g. "EPM") on success, or None if the
    credentials file is missing.
    """
    parser = argparse.ArgumentParser(description="VantageOps")
    parser.add_argument(
        "--client",
        type=str,
        default="EPM",
        help="Nombre del cliente para cargar credenciales (ej: EPM_PRD, EPM_DEV, BCI_PRD)",
    )
    args, _ = parser.parse_known_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.append(project_root)

    env_file = os.path.join(project_root, f"{args.client.upper()}.env")
    if os.path.exists(env_file):
        load_dotenv(env_file, override=True)
        return args.client.upper()
    return None


@st.cache_resource
def get_global_css():
    """Return the global CSS string (offline icon fallback + corporate buttons)."""
    return """
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


# ---------------------------------------------------------------------------
# Bootstrap sequence
# ---------------------------------------------------------------------------

# 1. Pandas config (pure Python, no Streamlit rendering)
pd.set_option("styler.render.max_elements", 2000000)

# 2. Load client environment (cached — runs once per process)
client = load_client_environment()

# 3. FIRST Streamlit rendering command
st.set_page_config(
    page_title=f"VantageOps - {client or 'UNKNOWN'}",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 4. Validate environment loaded successfully
if client is None:
    st.error(
        "**Error Critico de Configuracion:** No se encontro el archivo de "
        "credenciales `.env` en la raiz del proyecto."
    )
    st.stop()

# 5. Sidebar logo
st.logo("logo.jpg")

# 6. Inject cached global CSS
st.markdown(get_global_css(), unsafe_allow_html=True)

# Define pages in the desired order
# Home first, then Health & Connectivity, then other modules
pg_home = st.Page("app.py", title="Home")
pg_dbinfo = st.Page("pages/1_System_Information.py", title="System Information")
pg_statistics = st.Page("pages/2_Statistics_Management.py", title="Statistics Management")
#pg_space = st.Page("pages/3_Space.py", title="Space")
#pg_dbql = st.Page("pages/4_Database_Query_Logging.py", title="Database Query Logging")
#pg_performance = st.Page("pages/5_Performance_Assessment.py", title="Performance Assessment")
#pg_schema = st.Page("pages/6_Schema_Design.py", title="Schema Design")
#pg_hardware = st.Page("pages/7_Hardware_Utilization.py", title="Hardware Utilization")
#pg_cleanup = st.Page("pages/8_Cleanup_Cost_Optimization.py", title="Cleanup & Cost Optimization")
#pg_monthly_report = st.Page("pages/9_Monthly_Report.py", title="Monthly Report")
#pg_security = st.Page("pages/10_Security.py", title="Security")

# Create navigation with grouped sections
pages = {
    "MODULOS": [
        pg_home,           # Home
        pg_dbinfo,         # 1
        pg_statistics,     # 2
#        pg_space,          # 3
#        pg_dbql,           # 4
#        pg_performance,    # 5
#        pg_schema,         # 6
#        pg_hardware,       # 7
#        pg_cleanup,        # 8
#        pg_monthly_report, # 9
#        pg_security        # 10
    ]
}

navigation = st.navigation(pages)

# Run navigation
navigation.run()