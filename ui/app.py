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
        <p style="margin: 0; color: #555; font-size: 1.5rem; font-weight: 300;">
    Managed Services &mdash; Teradata DBA Optimization Suite
</p>
    </div>
    """, unsafe_allow_html=True)

    # Corporate Welcome Section
    st.markdown("## Vantage Operations")
    st.markdown("""
VantageOps — Statistics Management es una solución de inteligencia operacional diseñada para equipos de Managed Services que administran entornos Teradata de misión crítica.
La plataforma automatiza el ciclo completo de gestión de estadísticas, permitiendo a los DBAs:

Identificar estadísticas obsoletas, ausentes o redundantes que impactan el rendimiento del optimizador
Detectar inflación del diccionario de datos y generar sentencias DROP STATISTICS de remediación
Generar scripts COLLECT STATISTICS priorizados por severidad, listos para ejecución
Obtener visibilidad inmediata del estado de salud estadístico por base de datos y tabla

El resultado es una reducción sostenida en los tiempos de respuesta de consultas y un diccionario Teradata limpio, auditable y alineado con las mejores prácticas de la plataforma.
Para más información sobre soluciones Teradata, visite el Sitio Web Oficial de Teradata.).
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
