"""
TD Stats Optimizer - Main Navigation Entry Point

This module provides the main navigation using Streamlit's st.navigation API
to control the order of pages in the sidebar.
"""

import streamlit as st
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Streamlit page configuration
st.set_page_config(
    page_title="TD Stats Optimizer",
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
