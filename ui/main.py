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

# Add logo to sidebar
st.sidebar.image("logo.jpg", use_container_width=True)

# Define pages in the desired order
# Home first, then Health & Connectivity, then other modules
pg_home = st.Page("app.py", title="Home")
pg_health = st.Page("pages/1_Module_1_Health.py", title="Health & Connectivity")
pg_statistics = st.Page("pages/1_Statistics_Management.py", title="Statistics Management")
pg_space = st.Page("pages/2_Space.py", title="Space")
pg_security = st.Page("pages/3_Security.py", title="Security")
pg_dbql = st.Page("pages/4_Database_Query_Logging.py", title="Database Query Logging")
pg_performance = st.Page("pages/5_Performance_Assessment.py", title="Performance Assessment")
pg_schema = st.Page("pages/6_Module_7_Schema.py", title="Schema Design")
pg_hardware = st.Page("pages/7_Module_8_Hardware.py", title="Hardware Utilization")

# Create navigation with the specified order
navigation = st.navigation([
    pg_home,
    pg_health,
    pg_statistics,
    pg_space,
    pg_security,
    pg_dbql,
    pg_performance,
    pg_schema,
    pg_hardware
])

# Add sidebar header
st.sidebar.header("Módulos")

# Run navigation
navigation.run()
