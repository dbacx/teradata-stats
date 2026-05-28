"""
VantageOps - Main Navigation Entry Point

This module provides the main navigation using Streamlit's st.navigation API
to control the order of pages in the sidebar.
"""

import sys
import os

# Ensure project root is on sys.path BEFORE any local imports
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pandas as pd
import streamlit as st

from core.connections_manager import (
    load_connections, get_customers, get_systems, get_connection_params,
)
from core.connection import create_connection_from_params


# ---------------------------------------------------------------------------
# Cached helpers (executed once per process, not on every rerun)
# ---------------------------------------------------------------------------


@st.cache_data
def load_css(css_file_path: str):
    """Read the static CSS file and inject it into the app."""
    if os.path.exists(css_file_path):
        with open(css_file_path, "r", encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        st.warning(f"⚠️ No se encontró el archivo de estilos: {css_file_path}")


# ---------------------------------------------------------------------------
# Bootstrap sequence
# ---------------------------------------------------------------------------

# 1. Pandas config (pure Python, no Streamlit rendering)
pd.set_option("styler.render.max_elements", 2000000)

# 2. Page config — FIRST Streamlit rendering command
st.set_page_config(
    page_title="VantageOps",
    page_icon="🔶",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 3. Inject cached global CSS (from ui/static/style.css)
css_path = os.path.join(_project_root, "ui", "static", "style.css")
load_css(css_path)

# 4. Sidebar logo (custom HTML — no white background box)
st.markdown("""
<div class="td-logo-wrapper">
    <img src="app/static/logo.png" alt="teradata.">
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar — Connection selector (connections.csv)
# ---------------------------------------------------------------------------
df_conn = load_connections()

if df_conn.empty:
    with st.sidebar:
        st.warning(
            "⚠️ No se encontró config/connections.csv\n\n"
            "Ejecuta: `python scripts/create_connections_template.py`\n\n"
            "Luego edita el archivo con tus credenciales."
        )
else:
    with st.sidebar:
        st.sidebar.header("Conexión")

        customers = get_customers(df_conn)
        selected_customer = st.selectbox("Cliente", customers, key="sb_customer")

        systems_df = get_systems(df_conn, selected_customer)
        system_options = {
            f"{row['System Name']} ({row['Site ID']})": row["Site ID"]
            for _, row in systems_df.iterrows()
        }
        selected_label = st.selectbox("Sistema", list(system_options.keys()), key="sb_system")
        selected_site_id = system_options.get(selected_label, "")

        if st.button("Conectar", type="primary"):
            params = get_connection_params(df_conn, selected_customer, selected_site_id)
            if not params:
                st.error("No se encontraron parámetros para esta combinación.")
            else:
                try:
                    connection = create_connection_from_params(params)
                    st.session_state["td_conn"] = connection    # ← guardar el objeto
                    st.session_state["td_params"] = params
                    st.session_state["td_connected"] = True
                    st.success(f"Conectado a {params['host']}")
                except Exception as exc:
                    st.session_state["td_connected"] = False
                    st.error(f"Error de conexión: {exc}")

        # Connection status indicator
        if st.session_state.get("td_connected"):
            params = st.session_state.get("td_params", {})
            st.markdown(
                f"<div style='background:#d1e7dd;color:#0f5132;padding:6px 10px;"
                f"border-radius:4px;font-size:0.8rem;border:1px solid #badbcc;'>"
                f"🟢 {params.get('customer','')}/{params.get('system','')}"
                f"</div>",
                unsafe_allow_html=True,
            )

        st.sidebar.markdown("---")

# ---------------------------------------------------------------------------
# Page navigation
# ---------------------------------------------------------------------------
pg_home = st.Page("app.py", title="Home")
pg_dbinfo = st.Page("pages/1_System_Information.py", title="System Information")
pg_statistics = st.Page("pages/2_Statistics_Management.py", title="Statistics Management")

pages = {
    "MODULOS": [
        pg_home,
        pg_dbinfo,
        pg_statistics,
    ]
}

navigation = st.navigation(pages)
navigation.run()
