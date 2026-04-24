"""
TD Stats Optimizer - Streamlit Web Interface

This module provides a user-friendly web interface for analyzing and optimizing
Teradata database statistics using the core framework components.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.dictionary_ext import extract_database_stats
from analyzers.health_rules import StatsAnalyzer
from skills.recommender import DDLRecommender

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Streamlit page configuration
st.set_page_config(
    page_title="TD Stats Optimizer",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'ddl_recommendations' not in st.session_state:
        st.session_state.ddl_recommendations = None
    if 'last_database' not in st.session_state:
        st.session_state.last_database = ""


def display_metrics_summary(df_stats: pd.DataFrame, stale_stats: pd.DataFrame):
    """Display key metrics in the main interface."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_tables = df_stats['TableName'].nunique() if not df_stats.empty else 0
        st.metric("Total Tablas", total_tables)
    
    with col2:
        total_stats = len(df_stats)
        st.metric("Total Estadísticas", total_stats)
    
    with col3:
        stale_count = len(stale_stats)
        st.metric("Stats Desactualizadas", stale_count)
    
    with col4:
        if not df_stats.empty:
            avg_table_size = df_stats['TableSizeGB'].mean()
            st.metric("Tamaño Promedio (GB)", f"{avg_table_size:.2f}")
        else:
            st.metric("Tamaño Promedio (GB)", "0.00")


def display_dataframes(df_stats: pd.DataFrame, stale_stats: pd.DataFrame):
    """Display analysis results in expandable dataframes."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Estadísticas Completas")
        if not df_stats.empty:
            # Format timestamps for better display
            display_df = df_stats.copy()
            if 'LastCollectTimeStamp' in display_df.columns:
                display_df['LastCollectTimeStamp'] = display_df['LastCollectTimeStamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400
            )
        else:
            st.info("No hay estadísticas disponibles para mostrar")
    
    with col2:
        st.subheader("⚠️ Estadísticas Desactualizadas")
        if not stale_stats.empty:
            # Format for better display
            display_stale = stale_stats.copy()
            if 'LastCollectTimeStamp' in display_stale.columns:
                display_stale['LastCollectTimeStamp'] = display_stale['LastCollectTimeStamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            st.dataframe(
                display_stale,
                use_container_width=True,
                height=400
            )
        else:
            st.success("✅ No se encontraron estadísticas desactualizadas")


def display_ddl_recommendations(ddl_recommendations: dict):
    """Display generated DDL recommendations."""
    st.subheader("🔧 Recomendaciones DDL")
    
    if not ddl_recommendations:
        st.info("No hay recomendaciones DDL disponibles")
        return
    
    # Display COLLECT STATISTICS
    if ddl_recommendations.get('collect_stats'):
        st.write("**🔄 COLLECT STATISTICS (Actualización):**")
        collect_sql = "\n".join(ddl_recommendations['collect_stats'])
        st.code(collect_sql, language='sql')
        st.download_button(
            label="📥 Descargar COLLECT STATS",
            data=collect_sql,
            file_name=f"collect_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )
    
    # Display DROP STATISTICS
    if ddl_recommendations.get('drop_stats'):
        st.write("**🗑️ DROP STATISTICS (Limpieza):**")
        drop_sql = "\n".join(ddl_recommendations['drop_stats'])
        st.code(drop_sql, language='sql')
        st.download_button(
            label="📥 Descargar DROP STATS",
            data=drop_sql,
            file_name=f"drop_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )
    
    # Display combined recommendations
    if ddl_recommendations.get('collect_stats') or ddl_recommendations.get('drop_stats'):
        st.write("**📄 Todas las Recomendaciones:**")
        recommender = DDLRecommender()
        combined_output = recommender.format_ddl_output(ddl_recommendations)
        st.code(combined_output, language='sql')
        st.download_button(
            label="📥 Descargar Todas las Recomendaciones",
            data=combined_output,
            file_name=f"td_stats_recommendations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


def main():
    """Main application entry point."""
    initialize_session_state()
    
    # Header
    st.title("📊 TD Stats Optimizer")
    st.markdown("*Optimización Automatizada de Estadísticas Teradata*")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuración")
    
    database_name = st.sidebar.text_input(
        "Nombre de la Base de Datos",
        value=st.session_state.last_database,
        placeholder="Ej: DBC, USER_DB, etc."
    )
    
    days_threshold = st.sidebar.slider(
        "Umbral de Días para Stats Desactualizadas",
        min_value=1,
        max_value=90,
        value=15,
        help="Estadísticas más antiguas que este número de días se considerarán desactualizadas"
    )
    
    max_stats_per_table = st.sidebar.slider(
        "Máximo de Stats por Tabla (Bloat)",
        min_value=10,
        max_value=100,
        value=50,
        step=5,
        help="Tablas con más estadísticas que este umbral se marcarán como con posible bloat"
    )
    
    # Execute Analysis Button
    if st.sidebar.button("🚀 Ejecutar Análisis", type="primary"):
        if not database_name or not database_name.strip():
            st.sidebar.error("Por favor, ingrese un nombre de base de datos válido")
            return
        
        # Store database name in session state
        st.session_state.last_database = database_name
        
        # Main analysis workflow
        try:
            # Step 1: Extract statistics metadata
            with st.spinner("🔄 Extrayendo metadata del diccionario..."):
                logger.info(f"Starting extraction for database: {database_name}")
                df_stats = extract_database_stats(database_name)
            
            if df_stats.empty:
                st.error(f"No se encontraron estadísticas para la base de datos '{database_name}'")
                st.session_state.analysis_results = None
                st.session_state.ddl_recommendations = None
                return
            
            # Step 2: Analyze statistics health
            with st.spinner("🔍 Analizando salud de las estadísticas..."):
                analyzer = StatsAnalyzer()
                stale_stats = analyzer.detect_stale_stats(df_stats, days_threshold)
                bloat_analysis = analyzer.detect_dictionary_bloat(df_stats, max_stats_per_table)
                
                # Store results in session state
                st.session_state.analysis_results = {
                    'df_stats': df_stats,
                    'stale_stats': stale_stats,
                    'bloat_analysis': bloat_analysis
                }
            
            # Step 3: Generate DDL recommendations
            with st.spinner("🔧 Generando recomendaciones DDL..."):
                recommender = DDLRecommender()
                collect_ddls = recommender.generate_collect_stats(stale_stats)
                drop_ddls = recommender.generate_drop_stats(bloat_analysis)
                
                st.session_state.ddl_recommendations = {
                    'collect_stats': collect_ddls,
                    'drop_stats': drop_ddls
                }
            
            st.success(f"✅ Análisis completado para '{database_name}'")
            
        except ValueError as e:
            st.error(f"Error de validación: {str(e)}")
            logger.error(f"Validation error: {str(e)}")
        except Exception as e:
            st.error(f"Error durante el análisis: {str(e)}")
            logger.error(f"Analysis error: {str(e)}")
    
    # Display results if available
    if st.session_state.analysis_results:
        st.markdown("## 📈 Resultados del Análisis")
        
        results = st.session_state.analysis_results
        df_stats = results['df_stats']
        stale_stats = results['stale_stats']
        
        # Display metrics
        display_metrics_summary(df_stats, stale_stats)
        
        st.markdown("---")
        
        # Display dataframes
        display_dataframes(df_stats, stale_stats)
        
        st.markdown("---")
        
        # Display DDL recommendations
        if st.session_state.ddl_recommendations:
            display_ddl_recommendations(st.session_state.ddl_recommendations)
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        **TD Stats Optimizer** | Framework de Automatización de Estadísticas Teradata  
        *Desarrollado con Python, Pandas, y Streamlit*
        """
    )


if __name__ == "__main__":
    main()
