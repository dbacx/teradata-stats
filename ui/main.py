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


@st.cache_resource
def get_global_css():
    """Return the global CSS string (offline icon fallback + corporate buttons)."""
    return """
<style>
    /* 1. Hide broken icon text ONLY in Streamlit UI chrome elements */
    [data-testid="stSidebarCollapseButton"] .material-symbols-outlined,
    [data-testid="stSidebarCollapseButton"] .material-symbols-rounded,
    [data-testid="stExpanderToggleIcon"] .material-symbols-outlined,
    [data-testid="stExpanderToggleIcon"] .material-symbols-rounded,
    [data-testid="stSidebarCollapseButton"] span:not(.td-icon),
    [data-testid="stExpanderToggleIcon"] span:not(.td-icon) {
        color: transparent !important;
        font-size: 0px !important;
    }

    /* 2. Reemplazo para el botón del menú lateral (keyboard_double_arrow...) */
    [data-testid="stSidebarCollapseButton"] span::after {
        content: "☰" !important;
        font-size: 1.2rem !important;
        color: #1a2b38 !important;
        visibility: visible !important;
        display: block !important;
    }

    /* 3. Reemplazo para la flecha de los acordeones/expanders (expand_more) */
    [data-testid="stExpanderToggleIcon"]::after {
        content: "▼" !important;
        font-size: 0.8rem !important;
        color: #1a2b38 !important;
        visibility: visible !important;
        display: block !important;
    }

    /* 4. Botones primarios — Teradata Navy oficial */
    [data-testid="baseButton-primary"] {
        background-color: #00233C !important;
        color: #FFFFFF !important;
        border: none !important;
        font-weight: 600 !important;
        border-radius: 6px !important;
    }
    [data-testid="baseButton-primary"]:hover {
        background-color: #001828 !important;
        transition: background-color 0.2s ease;
    }

    /* 5. Sidebar navigation — tipografía unificada */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a div {
        font-size: 1.1rem !important;
        font-weight: 500 !important;
    }

    /* 6. Tipografía global — párrafos y listas */
    p, li {
        font-size: 1rem !important;
        line-height: 1.6 !important;
        color: #1a2b38 !important;
    }

    /* 7. Títulos — contraste fuerte */
    h1, h2, h3 {
        color: #00233C !important;
    }

    /* 8. Ocultar iconos de anchor link en títulos */
    [data-testid="stHeaderActionElements"],
    .st-emotion-cache-10trblm a,
    h1 a, h2 a, h3 a,
    [data-testid="StyledLinkIconContainer"] {
        display: none !important;
    }

    /* 9. Sidebar nav — bold dark module names */
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a div p {
        font-size: 1.05rem !important;
        font-weight: 700 !important;
        color: #00233C !important;
        letter-spacing: 0.01em;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover div p {
        color: #C24B00 !important;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] div p {
        color: #C24B00 !important;
        font-weight: 700 !important;
    }
    /* 10. Section label MODULOS — unified with sidebar headers */
    [data-testid="stNavSectionHeader"],
    [data-testid="stNavSectionHeader"] p {
        font-size: 1rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.05em !important;
        color: #1a2b38 !important;
        text-transform: uppercase !important;
        text-align: center !important;
        display: block !important;
    }

    /* 11. Findings tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        flex-wrap: wrap;
        border-bottom: 2px solid #00233C;
    }
    .stTabs [data-baseweb="tab"] {
        background: #f0f4f8;
        border-radius: 6px 6px 0 0;
        padding: 6px 14px;
        font-size: 0.78rem;
        font-weight: 600;
        color: #00233C;
        border: 1px solid #e2e8f0;
        border-bottom: none;
        white-space: nowrap;
    }
    .stTabs [aria-selected="true"] {
        background: #00233C !important;
        color: #FFFFFF !important;
        border-color: #00233C !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        background: #1a3a52 !important;
        color: #FFFFFF !important;
    }
    /* 12. Sidebar section titles: unified size, weight, alignment */

    /* "Configuración" and "Conexión" — st.sidebar.header() → h2 */
    [data-testid="stSidebar"] h2 {
        font-size: 1rem !important;
        font-weight: 700 !important;
        color: #1a2b38 !important;
        text-align: center !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        margin-top: 0.8rem !important;
        margin-bottom: 0.4rem !important;
    }

    /* "MODULOS" — generated by st.navigation() */
    [data-testid="stNavSectionHeader"] [data-testid="stMarkdownContainer"],
    [data-testid="stNavSectionHeader"] [data-testid="stMarkdownContainer"] p {
        font-size: 1rem !important;
        font-weight: 700 !important;
        color: #1a2b38 !important;
        text-align: center !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        display: block !important;
        margin-top: 0.8rem !important;
        margin-bottom: 0.4rem !important;
    }
</style>
"""


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

# 3. Sidebar logo (custom HTML — no white background box)
st.markdown("""
<style>
[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.5rem;
}
.td-logo-wrapper {
    display: flex;
    align-items: center;
    padding: 0 1.2rem 1.2rem 1.2rem;
}
.td-logo-wrapper img {
    width: 160px;
    height: auto;
    mix-blend-mode: multiply;
    opacity: 0.92;
    filter: drop-shadow(0px 0px 0px transparent);
}
</style>
<div class="td-logo-wrapper">
    <img src="app/static/logo.png" alt="teradata.">
</div>
""", unsafe_allow_html=True)

# 4. Inject cached global CSS
st.markdown(get_global_css(), unsafe_allow_html=True)

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
