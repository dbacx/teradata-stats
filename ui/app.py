"""
VantageOps - Teradata Assessment Suite

Home page — shows the corporate banner, description, and footer.
"""

import streamlit as st


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


def main_app():
    """Main application function for the Home page."""
    inject_custom_css()

    # Corporate Banner
    st.markdown("""
    <div style="background-color: #f0f4f8; padding: 2rem 1.5rem; border-radius: 8px; text-align: center; margin-bottom: 1.5rem;">
        <h2 style="margin: 0 0 0.25rem 0; color: #00233C;">VantageOps</h2>
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


if __name__ == "__main__":
    main_app()
