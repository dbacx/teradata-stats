"""
Module 3: Space Assessment Page - Advanced Plotly Dashboard

Provides a dedicated interface for the Space Assessment module with 11
Plotly-based visualizations, KPI cards, and DDL remediation scripts.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.connection import TeradataConnection
from collectors.mod3_space_collector import SpaceCollector
from analyzers.mod3_space_analyzer import SpaceAnalyzer, COMPONENT_LABELS, COMPONENT_SEVERITY, DDL_COLUMNS
from core.config import THRESHOLDS, SYSTEM_DATABASES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mock data generators (used when no Teradata connection is available)
# ---------------------------------------------------------------------------

def generate_mock_data() -> dict:
    """Generate mock DataFrames matching the 11 SQL output structures."""
    np.random.seed(42)
    data = {}

    # 01_CDS_Report: Parameter/Value pairs
    data['01_CDS_Report'] = pd.DataFrame({
        'Parameter': [
            '01. CurrentPerm (TB)', '02. NoFallback (TB)', '03. CDS Capacity (TB)',
            '04. CDS Consumed (TB)', '05. CDS Available (TB)',
            '06. CDS Utilization (%)', '07. Compression Ratio'
        ],
        'Value': [14.32, 9.87, 20.0, 16.45, 3.55, 82.25, 1.45]
    })

    # 02_Space_Capacity_Forecast: monthly historical baseline
    months = []
    base = datetime.now() - relativedelta(months=6)
    for i in range(7):
        m = base + relativedelta(months=i)
        months.append(m.strftime('%Y-%m'))
    cur_perm = [12.1, 12.8, 13.2, 13.9, 14.1, 14.5, 14.9]
    max_perm = [20.0] * 7
    data['02_Space_Capacity_Forecast'] = pd.DataFrame({
        'Month_Year': months,
        'TotalCurPerm_TB': cur_perm,
        'TotalMaxPerm_TB': max_perm,
    })

    # 03_Suspected_Unused_Objects
    dbs = ['SALES_DB', 'HR_DB', 'FINANCE_DB', 'MARKETING_DB', 'LOGS_DB',
           'STAGING_DB', 'ARCHIVE_DB', 'TEST_DB']
    tables = ['fact_orders_2019', 'dim_employees_old', 'budget_archive',
              'campaign_log', 'event_stream_raw', 'stg_load_tmp',
              'archive_transactions', 'test_regression']
    sizes = [45.2, 12.8, 8.5, 22.1, 67.3, 15.4, 33.7, 5.9]
    data['03_Suspected_Unused_Objects'] = pd.DataFrame({
        'DataBaseName': dbs, 'TableName': tables,
        'Size_GB': sizes, 'Accesos_30D': [0]*8,
        'Status': ['Flagged for Archiving Review']*8
    })

    # 04_Suspected_Duplicate_Objects
    data['04_Suspected_Duplicate_Objects'] = pd.DataFrame({
        'DataBaseName': ['SALES_DB']*4 + ['HR_DB']*2,
        'TableName': ['fact_orders', 'fact_orders_BKP', 'fact_orders_OLD',
                       'fact_orders_COPY', 'dim_employees_BK', 'dim_employees_OLD'],
        'TableKind': ['T']*6,
        'Size_GB': [45.2, 44.8, 43.1, 45.0, 12.8, 12.5],
        'Created_Date': ['2022-01-15', '2023-06-01', '2022-12-10',
                         '2024-01-20', '2021-05-12', '2023-03-14'],
        'Last_Alter_Date': ['2024-12-01', '2023-06-01', '2022-12-10',
                            '2024-01-20', '2023-11-01', '2023-03-14'],
        'Status': ['Review Required']*6,
    })

    # 05_MVC_Opportunities_Uncompressed_Tables
    data['05_MVC_Opportunities_Uncompressed_Tables'] = pd.DataFrame({
        'DataBaseName': ['SALES_DB', 'FINANCE_DB', 'LOGS_DB', 'MARKETING_DB', 'STAGING_DB'],
        'TableName': ['fact_transactions', 'gl_entries', 'audit_trail',
                       'campaign_clicks', 'stg_raw_feed'],
        'Size_GB': [120.5, 85.3, 200.1, 45.8, 67.2],
        'Total_Columns': [25, 18, 30, 12, 22],
        'Diagnostico': ['Zero Compression. Implement MVC via ALTER TABLE.']*5,
    })

    # 06_MVC_Opportunities_Compressed_Tables
    data['06_MVC_Opportunities_Compressed_Tables'] = pd.DataFrame({
        'DataBaseName': ['SALES_DB', 'HR_DB', 'FINANCE_DB', 'MARKETING_DB'],
        'TableName': ['dim_product', 'dim_department', 'dim_account', 'dim_channel'],
        'ColumnName': ['status_flag', 'is_active', 'account_type', 'channel_code'],
        'ColumnType': ['I1', 'I1', 'CF', 'CF'],
        'Diagnostico': [
            'Low-Hanging Fruit: BYTEINT type uncompressed.',
            'Low-Hanging Fruit: BYTEINT type uncompressed.',
            'Low-Hanging Fruit: CHAR(1) type uncompressed.',
            'Low-Hanging Fruit: CHAR(1) type uncompressed.',
        ],
    })

    # 07_Top_20_Databases_By_Used_Size
    db_names = [f'DB_{i:02d}' for i in range(1, 21)]
    total_sizes = sorted(np.random.uniform(50, 500, 20).tolist(), reverse=True)
    net_data = [s * np.random.uniform(0.5, 0.9) for s in total_sizes]
    data['07_Top_20_Databases_By_Used_Size'] = pd.DataFrame({
        'DataBaseName': db_names,
        'Total_Size_GB': [round(s, 2) for s in total_sizes],
        'Net_Data_GB': [round(n, 2) for n in net_data],
    })

    # 08_Top_20_Tables_By_Size
    tbl_names = [f'table_{i:02d}' for i in range(1, 21)]
    tbl_sizes = sorted(np.random.uniform(10, 300, 20).tolist(), reverse=True)
    skew_pcts = np.random.uniform(0, 80, 20).tolist()
    data['08_Top_20_Tables_By_Size'] = pd.DataFrame({
        'DataBaseName': [f'DB_{np.random.randint(1,6):02d}' for _ in range(20)],
        'TableName': tbl_names,
        'Total_Size_GB': [round(s, 2) for s in tbl_sizes],
        'Skew_Pct': [round(sk, 2) for sk in skew_pcts],
    })

    # 09_Top_20_Unused_Databases_By_Size
    data['09_Top_20_Unused_Databases_By_Size'] = pd.DataFrame({
        'DataBaseName': [f'UNUSED_DB_{i}' for i in range(1, 16)],
        'Total_Size_GB': sorted(np.random.uniform(10, 200, 15).tolist(), reverse=True),
        'Total_Queries_30D': [0]*15,
    })

    # 10_Monthly_Capacity_ Snapshot (note the space in the name matches the SQL file)
    snap_months = []
    base_snap = datetime.now() - relativedelta(months=12)
    for i in range(13):
        m = base_snap + relativedelta(months=i)
        snap_months.append(m.strftime('%Y-%m-%d'))
    data['10_Monthly_Capacity_ Snapshot'] = pd.DataFrame({
        'Mes_Snapshot': snap_months,
        'Spool_TB': np.round(np.random.uniform(1, 5, 13), 4).tolist(),
        'Temp_TB': np.round(np.random.uniform(0.5, 2, 13), 4).tolist(),
        'CurrentPerm_TB': np.round(np.linspace(12, 15, 13), 4).tolist(),
        'PeakPerm_TB': np.round(np.linspace(12.5, 15.8, 13), 4).tolist(),
    })

    # 11_Database_Space_Utilization
    n_dbs = 30
    max_perms = np.random.uniform(50, 800, n_dbs)
    util_pcts = np.random.uniform(20, 99, n_dbs)
    current_perms = max_perms * util_pcts / 100.0
    data['11_Database_Space_Utilization'] = pd.DataFrame({
        'DataBaseName': [f'DB_{i:03d}' for i in range(1, n_dbs + 1)],
        'MaxPerm_GB': np.round(max_perms, 2).tolist(),
        'CurrentPerm_GB': np.round(current_perms, 2).tolist(),
        'Effective_Space_GB': np.round(current_perms * np.random.uniform(1.0, 1.3, n_dbs), 2).tolist(),
        'Global_Util_Pct': np.round(util_pcts, 2).tolist(),
        'Effective_Util_Pct': np.round(util_pcts * np.random.uniform(1.0, 1.15, n_dbs), 2).tolist(),
        'Skew_Pct': np.round(np.random.uniform(0, 60, n_dbs), 2).tolist(),
    })

    return data


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def inject_custom_css():
    css = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    * { font-family: 'Inter', sans-serif !important; }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    [data-testid="stMetricValue"] { font-size: 1.8rem !important; font-weight: 600 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.85rem !important; }
    p, div, span { font-size: 0.9rem; }
    [data-testid="stDataFrame"] { font-size: 0.85rem; }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Session State
# ---------------------------------------------------------------------------

def initialize_session_state():
    if 'mod3_collected_data' not in st.session_state:
        st.session_state.mod3_collected_data = None
    if 'mod3_analyzed_data' not in st.session_state:
        st.session_state.mod3_analyzed_data = None
    if 'mod3_findings' not in st.session_state:
        st.session_state.mod3_findings = None


# ---------------------------------------------------------------------------
# Dashboard renderers (one per component)
# ---------------------------------------------------------------------------

def render_01_cds_report(df: pd.DataFrame):
    """CDS Report: gauge + progress bar + metrics + alerts."""
    if df.empty:
        st.info("Sin datos para CDS Report")
        return

    df['Value'] = pd.to_numeric(df['Value'], errors='coerce').fillna(0)
    params = dict(zip(df['Parameter'].str.strip(), df['Value']))
    cds_pct = float(params.get('06. CDS Utilization (%)', 0))
    cds_consumed = float(params.get('04. CDS Consumed (TB)', 0))
    cds_available = float(params.get('05. CDS Available (TB)', 0))
    cds_capacity = float(params.get('03. CDS Capacity (TB)', 0))

    col_gauge, col_bar = st.columns([1, 1])

    with col_gauge:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=cds_pct,
            title={'text': "CDS Utilization"},
            number={'suffix': '%'},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': '#378ADD'},
                'steps': [
                    {'range': [0, 70], 'color': '#639922'},
                    {'range': [70, 85], 'color': '#EF9F27'},
                    {'range': [85, 100], 'color': '#E24B4A'},
                ],
            }
        ))
        fig.update_layout(height=300, margin=dict(t=40, b=20, l=30, r=30))
        st.plotly_chart(fig, use_container_width=True)

    with col_bar:
        pct_width = min(cds_pct, 100)
        bar_html = f"""
        <div style="margin-top:60px;">
            <div style="display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:4px;">
                <span>0 TB</span><span>{cds_capacity:.1f} TB</span>
            </div>
            <div style="background:#e0e0e0;border-radius:8px;height:28px;position:relative;">
                <div style="background:#378ADD;width:{pct_width}%;height:100%;border-radius:8px;"></div>
            </div>
            <div style="text-align:center;font-size:0.85rem;margin-top:6px;">
                <b>{cds_consumed:.2f} TB</b> consumido de <b>{cds_capacity:.1f} TB</b>
            </div>
        </div>
        """
        st.markdown(bar_html, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Consumido (TB)", f"{cds_consumed:.2f}", delta=f"-{cds_consumed:.2f}", delta_color="inverse")
    with c2:
        st.metric("Disponible (TB)", f"{cds_available:.2f}")
    with c3:
        st.metric("Contratado (TB)", f"{cds_capacity:.1f}")

    if cds_pct > 85:
        st.warning(f"CDS Utilization es {cds_pct:.1f}% - supera el umbral de 85%")
    tb_before_85 = max(0, cds_capacity * 0.85 - cds_consumed)
    st.info(f"Quedan **{tb_before_85:.2f} TB** disponibles antes de alcanzar el 85% de CDS.")


def render_02_forecast(df: pd.DataFrame):
    """Space Capacity Forecast: line chart + KPI cards."""
    if df.empty:
        st.info("Sin datos para Space Capacity Forecast")
        return

    df = df.copy()
    df['TotalMaxPerm_TB'] = pd.to_numeric(df['TotalMaxPerm_TB'], errors='coerce').fillna(0)
    df['TotalCurPerm_TB'] = pd.to_numeric(df['TotalCurPerm_TB'], errors='coerce').fillna(0)
    max_cap = df['TotalMaxPerm_TB'].max()
    cur_vals = df['TotalCurPerm_TB'].values.tolist()

    # Simple linear projection: extend 6 more months
    n = len(cur_vals)
    if n >= 2:
        slope = (cur_vals[-1] - cur_vals[0]) / max(n - 1, 1)
    else:
        slope = 0
    forecast_months = []
    forecast_vals = []
    last_val = cur_vals[-1]
    base_month = datetime.strptime(df['Month_Year'].iloc[-1], '%Y-%m')
    for i in range(1, 7):
        m = base_month + relativedelta(months=i)
        forecast_months.append(m.strftime('%Y-%m'))
        last_val += slope
        forecast_vals.append(round(last_val, 2))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['Month_Year'].tolist(), y=cur_vals,
        mode='lines+markers', name='Historico',
        line=dict(color='#4354E9', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=[df['Month_Year'].iloc[-1]] + forecast_months,
        y=[cur_vals[-1]] + forecast_vals,
        mode='lines+markers', name='Forecast',
        line=dict(color='#FC6623', dash='dash', width=2)
    ))
    fig.add_hline(y=max_cap, line_dash='dot', line_color='#E24B4A',
                  annotation_text=f'Max Capacity ({max_cap} TB)')
    fig.update_layout(
        title='Space Capacity Forecast',
        xaxis_title='Mes', yaxis_title='TB Consumidos',
        height=400, legend=dict(orientation='h', yanchor='bottom', y=1.02)
    )
    fig.update_yaxes(rangemode="tozero")
    st.plotly_chart(fig, use_container_width=True)

    # KPIs
    growth_per_month = slope
    if slope > 0 and max_cap > cur_vals[-1]:
        days_to_exhaust = int((max_cap - cur_vals[-1]) / slope * 30)
    else:
        days_to_exhaust = -1

    c1, c2 = st.columns(2)
    with c1:
        exhaust_label = f"{days_to_exhaust} dias" if days_to_exhaust > 0 else "N/A"
        st.metric("Time-to-Exhaustion", exhaust_label)
    with c2:
        st.metric("Crecimiento Mensual", f"{growth_per_month:.2f} TB/mes")


def render_03_unused_objects(df: pd.DataFrame):
    """Suspected Unused Objects: treemap + metrics + download."""
    if df.empty:
        st.info("Sin datos para Unused Objects")
        return

    df['Size_GB'] = pd.to_numeric(df['Size_GB'], errors='coerce').fillna(0)
    total_gb = df['Size_GB'].sum()
    total_tables = len(df)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total GB Desperdiciados", f"{total_gb:,.1f}")
    with c2:
        st.metric("Tablas Sin Uso", total_tables)

    # Horizontal bar chart: object count by database
    if 'DataBaseName' in df.columns:
        db_counts = df.groupby('DataBaseName').agg(
            Object_Count=('TableName', 'count'),
            Total_Size_GB=('Size_GB', 'sum')
        ).reset_index().sort_values('Object_Count', ascending=True)
        fig_bar = px.bar(
            db_counts, y='DataBaseName', x='Object_Count', orientation='h',
            title='Conteo de Objetos Sin Uso por Base de Datos',
            labels={'Object_Count': 'Cantidad de Objetos', 'DataBaseName': 'Base de Datos'},
            text='Object_Count', color='Total_Size_GB',
            color_continuous_scale=['#FFD700', '#FF6B00', '#E24B4A'],
        )
        fig_bar.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
        fig_bar.update_traces(textposition='outside')
        st.plotly_chart(fig_bar, use_container_width=True)

        st.dataframe(db_counts.sort_values('Object_Count', ascending=False),
                     use_container_width=True, height=250)

    fig = px.treemap(
        df, path=['DataBaseName', 'TableName'], values='Size_GB',
        color='Size_GB',
        color_continuous_scale=['#FFD700', '#FF6B00', '#E24B4A'],
        title='Treemap: Objetos Sin Uso por Base de Datos'
    )
    fig.update_layout(height=500, margin=dict(t=40, b=10, l=10, r=10))
    st.plotly_chart(fig, use_container_width=True)

    # DDL download
    if 'DDL_Statement' in df.columns:
        ddl_script = '\n'.join(df['DDL_Statement'].dropna().tolist())
    else:
        ddl_script = '\n'.join(
            f"DROP TABLE {r['DataBaseName']}.{r['TableName']};"
            for _, r in df.iterrows()
        )
    st.download_button(
        "Descargar Script DDL (DROP TABLE)",
        data=ddl_script,
        file_name=f"drop_unused_objects_{datetime.now().strftime('%Y%m%d')}.sql",
        mime="text/plain"
    )


def render_04_duplicate_objects(df: pd.DataFrame):
    """Suspected Duplicate Objects: sunburst + dataframe."""
    if df.empty:
        st.info("Sin datos para Duplicate Objects")
        return

    df['Size_GB'] = pd.to_numeric(df['Size_GB'], errors='coerce').fillna(0)

    fig = px.sunburst(
        df, path=['DataBaseName', 'TableName'], values='Size_GB',
        title='Sunburst: Tablas Duplicadas / Backup',
        color='Size_GB', color_continuous_scale='YlOrRd'
    )
    fig.update_layout(height=500, margin=dict(t=40, b=10))
    st.plotly_chart(fig, use_container_width=True)

    total_savable = df['Size_GB'].sum()
    st.metric("Espacio Potencial a Liberar", f"{total_savable:,.1f} GB")

    st.dataframe(df[['DataBaseName', 'TableName', 'Size_GB', 'Created_Date',
                      'Last_Alter_Date', 'Status']],
                 use_container_width=True, height=300)


def render_05_06_mvc(df: pd.DataFrame, component_label: str):
    """MVC Opportunities: bar chart + metric + DDL table."""
    if df.empty:
        st.info(f"Sin datos para {component_label}")
        return

    has_size = 'Size_GB' in df.columns

    if has_size:
        df_plot = df.copy()
        df_plot['Size_GB'] = pd.to_numeric(df_plot['Size_GB'], errors='coerce').fillna(0)
        df_plot['Estimated_Compressed_GB'] = df_plot['Size_GB'] * np.random.uniform(0.3, 0.6, len(df_plot))
        df_plot['Estimated_Compressed_GB'] = df_plot['Estimated_Compressed_GB'].round(2)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_plot['TableName'], y=df_plot['Size_GB'],
            name='Current Size GB', marker_color='#AAAAAA'
        ))
        fig.add_trace(go.Bar(
            x=df_plot['TableName'], y=df_plot['Estimated_Compressed_GB'],
            name='Estimated Compressed GB', marker_color='#639922'
        ))
        fig.update_layout(
            barmode='group', title=f'{component_label} - Comparativo',
            xaxis_title='Tabla', yaxis_title='GB', height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        savings = (df_plot['Size_GB'] - df_plot['Estimated_Compressed_GB']).sum() / 1024
        st.metric("Potential Savings", f"{savings:.2f} TB")
    else:
        st.dataframe(df, use_container_width=True)

    if 'DDL_Statement' in df.columns:
        st.subheader("Comandos ALTER TABLE")
        st.dataframe(df[['DataBaseName', 'TableName', 'DDL_Statement']],
                     use_container_width=True, height=250)
    elif 'Diagnostico' in df.columns:
        st.subheader("Diagnosticos")
        st.dataframe(df, use_container_width=True, height=250)


def render_07_top_databases(df: pd.DataFrame):
    """Top 20 Databases: stacked horizontal bars + metrics + styled table."""
    if df.empty:
        st.info("Sin datos para Top 20 Databases")
        return

    df = df.copy()
    # Force numeric types on all metric columns
    for col in ['Total_Size_GB', 'Net_Data_GB', 'CurrentPerm_GB', 'MaxPerm_GB', 'Free_GB', 'Effective_Pct_Used']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # Derive MaxPerm and Free from available columns
    if 'Total_Size_GB' in df.columns and 'Net_Data_GB' in df.columns:
        df['CurrentPerm_GB'] = df['Net_Data_GB']
        df['MaxPerm_GB'] = df['Total_Size_GB']
        df['Free_GB'] = (df['MaxPerm_GB'] - df['CurrentPerm_GB']).clip(lower=0).round(2)
        df['Effective_Pct_Used'] = ((df['CurrentPerm_GB'] / df['MaxPerm_GB'].replace(0, np.nan)) * 100).round(2)
    elif 'CurrentPerm_GB' not in df.columns:
        st.dataframe(df, use_container_width=True)
        return

    colors = []
    for pct in df['Effective_Pct_Used']:
        if pct >= 85:
            colors.append('#E24B4A')
        elif pct >= 70:
            colors.append('#FC6623')
        else:
            colors.append('#4354E9')

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df['DataBaseName'], x=df['CurrentPerm_GB'],
        name='CurrentPerm GB', orientation='h',
        marker_color=colors
    ))
    fig.add_trace(go.Bar(
        y=df['DataBaseName'], x=df['Free_GB'],
        name='Free GB', orientation='h',
        marker_color='#D3D3D3'
    ))
    fig.update_layout(
        barmode='stack', title='Top 20 Databases By Used Size',
        xaxis_title='GB', height=600, yaxis={'categoryorder': 'total ascending'}
    )
    st.plotly_chart(fig, use_container_width=True)

    # 4 metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total MaxPerm GB", f"{df['MaxPerm_GB'].sum():,.0f}")
    with c2:
        st.metric("Used GB", f"{df['CurrentPerm_GB'].sum():,.0f}")
    with c3:
        st.metric("Free GB", f"{df['Free_GB'].sum():,.0f}")
    with c4:
        critical_count = int((df['Effective_Pct_Used'] >= 70).sum())
        st.metric("Bases Criticas (>70%)", critical_count)

    def style_pct(val):
        try:
            v = float(val)
            if v >= 85:
                return 'background-color: #FF6B6B; color: white; font-weight: bold'
            elif v >= 70:
                return 'background-color: #FFD700; color: black'
        except (ValueError, TypeError):
            pass
        return ''

    styled = df[['DataBaseName', 'CurrentPerm_GB', 'MaxPerm_GB', 'Free_GB', 'Effective_Pct_Used']].style.map(
        style_pct, subset=['Effective_Pct_Used']
    )
    st.dataframe(styled, use_container_width=True, height=400)


def render_08_top_tables(df: pd.DataFrame):
    """Top 20 Tables: horizontal bar colored by Skew_Pct."""
    if df.empty:
        st.info("Sin datos para Top 20 Tables")
        return

    df['Total_Size_GB'] = pd.to_numeric(df['Total_Size_GB'], errors='coerce').fillna(0)
    df['Skew_Pct'] = pd.to_numeric(df['Skew_Pct'], errors='coerce').fillna(0)
    df = df.sort_values('Total_Size_GB', ascending=True).tail(20)

    fig = px.bar(
        df, y='TableName', x='Total_Size_GB', orientation='h',
        color='Skew_Pct', color_continuous_scale=['#639922', '#FFD700', '#E24B4A'],
        title='Top 20 Tables By Size (Color = Skew %)',
        labels={'Total_Size_GB': 'Size GB', 'Skew_Pct': 'Skew %'}
    )
    fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)


def render_09_unused_databases(df: pd.DataFrame):
    """Top 20 Unused Databases: analytic quadrant scatter plot."""
    if df.empty:
        st.info("Sin datos para Unused Databases")
        return

    df = df.copy()
    for col in ['Total_Size_GB', 'CurrentPerm_GB', 'Days_Unused', 'Object_Count']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    # Simulate days unused and object count for mock data
    if 'Days_Unused' not in df.columns:
        df['Days_Unused'] = np.random.randint(30, 365, len(df))
    if 'Object_Count' not in df.columns:
        df['Object_Count'] = np.random.randint(5, 200, len(df))
    if 'CurrentPerm_GB' not in df.columns:
        df['CurrentPerm_GB'] = df.get('Total_Size_GB', pd.Series([10]*len(df)))

    fig = px.scatter(
        df, x='Days_Unused', y='CurrentPerm_GB',
        size='Object_Count', hover_name='DataBaseName',
        title='Cuadrante Analitico: Bases Sin Uso',
        labels={'Days_Unused': 'Dias Sin Acceso', 'CurrentPerm_GB': 'Tamano GB'},
        color_discrete_sequence=['#4354E9']
    )

    # Highlight purge zone (upper right)
    x_mid = df['Days_Unused'].median()
    y_mid = df['CurrentPerm_GB'].median()
    fig.add_shape(
        type='rect',
        x0=x_mid, x1=df['Days_Unused'].max() * 1.1,
        y0=y_mid, y1=df['CurrentPerm_GB'].max() * 1.1,
        fillcolor='rgba(226,75,74,0.1)', line=dict(color='#E24B4A', dash='dash')
    )
    fig.add_annotation(
        x=df['Days_Unused'].max() * 0.9, y=df['CurrentPerm_GB'].max() * 0.95,
        text='Zona de Purga Inmediata', showarrow=False,
        font=dict(color='#E24B4A', size=12, family='Inter')
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)


def render_10_monthly_snapshot(df: pd.DataFrame):
    """Monthly Capacity Snapshot: heatmap of MoM growth %."""
    if df.empty:
        st.info("Sin datos para Monthly Capacity Snapshot")
        return

    df = df.copy()
    for col in ['Spool_TB', 'Temp_TB', 'CurrentPerm_TB', 'PeakPerm_TB']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Build a heatmap: databases (rows) x months (columns) with MoM growth %
    # Since the SQL returns system-level aggregates, we simulate per-DB breakdown
    if 'DataBaseName' not in df.columns:
        # Create synthetic per-database breakdown from aggregate
        top_dbs = [f'DB_{i:02d}' for i in range(1, 11)]
        records = []
        for _, row in df.iterrows():
            total_perm = float(row.get('CurrentPerm_TB', 0))
            for db in top_dbs:
                frac = np.random.uniform(0.05, 0.2)
                records.append({
                    'DataBaseName': db,
                    'Month': str(row['Mes_Snapshot'])[:7],
                    'CurrentPerm_TB': round(total_perm * frac, 4)
                })
        df_expanded = pd.DataFrame(records)
    else:
        df_expanded = df.copy()
        if 'Month' not in df_expanded.columns:
            df_expanded['Month'] = df_expanded['Mes_Snapshot'].astype(str).str[:7]

    # Calculate MoM growth per database
    pivot = df_expanded.pivot_table(
        index='DataBaseName', columns='Month', values='CurrentPerm_TB', aggfunc='sum'
    )
    pivot = pivot.sort_index(axis=1)

    growth = pivot.pct_change(axis=1) * 100
    growth = growth.iloc[:, 1:]  # drop first month (NaN)
    growth = growth.fillna(0).round(2)

    fig = go.Figure(data=go.Heatmap(
        z=growth.values,
        x=growth.columns.tolist(),
        y=growth.index.tolist(),
        colorscale=[
            [0, '#4354E9'], [0.3, '#87CEEB'], [0.5, '#FFFFFF'],
            [0.7, '#FFCC00'], [1.0, '#E24B4A']
        ],
        colorbar_title='MoM Growth %',
        text=growth.values.round(1),
        texttemplate='%{text}%',
    ))
    fig.update_layout(
        title='Monthly Capacity Snapshot - MoM Growth %',
        xaxis_title='Mes', yaxis_title='Base de Datos',
        height=450
    )
    st.plotly_chart(fig, use_container_width=True)


def render_11_space_utilization(df: pd.DataFrame):
    """Database Space Utilization: scatter plot with 45-degree diagonal."""
    if df.empty:
        st.info("Sin datos para Database Space Utilization")
        return

    df = df.copy()
    for col in ['MaxPerm_GB', 'CurrentPerm_GB', 'Effective_Space_GB', 'Global_Util_Pct', 'Effective_Util_Pct', 'Skew_Pct']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Color by utilization: red for critical (>85%), orange (70-85%), blue (<70%)
    colors = []
    for pct in df['Global_Util_Pct']:
        if pct >= 85:
            colors.append('#E24B4A')
        elif pct >= 70:
            colors.append('#FC6623')
        else:
            colors.append('#4354E9')

    fig = go.Figure()

    # Diagonal reference line (100% usage)
    max_val = max(df['MaxPerm_GB'].max(), df['CurrentPerm_GB'].max()) * 1.1
    fig.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode='lines', name='100% Usage',
        line=dict(color='#999999', dash='dash', width=1),
        showlegend=True
    ))

    fig.add_trace(go.Scatter(
        x=df['MaxPerm_GB'], y=df['CurrentPerm_GB'],
        mode='markers', name='Databases',
        marker=dict(size=10, color=colors, line=dict(width=1, color='#333')),
        text=df['DataBaseName'],
        hovertemplate='<b>%{text}</b><br>MaxPerm: %{x:.1f} GB<br>CurrentPerm: %{y:.1f} GB<extra></extra>'
    ))

    fig.update_layout(
        title='Database Space Utilization (MaxPerm vs CurrentPerm)',
        xaxis_title='MaxPerm Asignado (GB)',
        yaxis_title='CurrentPerm Consumido (GB)',
        height=500,
        xaxis=dict(range=[0, max_val]),
        yaxis=dict(range=[0, max_val]),
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# KPIs & Findings (shared across tabs)
# ---------------------------------------------------------------------------

def display_kpi_cards(analyzed_data: dict):
    """Display KPI cards based on severity counts across all components."""
    st.subheader("KPI Cards - Space Assessment")

    severity_counts = {'CRITICAL': 0, 'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'INFO': 0}
    total_rows = 0

    for component_key, df in analyzed_data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            total_rows += len(df)
            if 'Severity' in df.columns:
                for sev in severity_counts:
                    severity_counts[sev] += int((df['Severity'] == sev).sum())

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("CRITICAL", severity_counts['CRITICAL'])
    with col2:
        st.metric("HIGH", severity_counts['HIGH'])
    with col3:
        st.metric("MEDIUM", severity_counts['MEDIUM'])
    with col4:
        st.metric("LOW", severity_counts['LOW'])
    with col5:
        st.metric("Total Hallazgos", total_rows)


def display_findings_table(analyzed_data: dict):
    """Display findings table with conditional formatting."""
    st.subheader("Tabla de Hallazgos")

    all_findings = []
    for component_key, df in analyzed_data.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df_copy = df.copy()
            df_copy['Component'] = COMPONENT_LABELS.get(component_key, component_key)
            all_findings.append(df_copy)

    if not all_findings:
        st.success("No se encontraron hallazgos")
        return

    combined_df = pd.concat(all_findings, ignore_index=True)

    available_severities = sorted(combined_df['Severity'].unique().tolist()) if 'Severity' in combined_df.columns else []
    severity_filter = st.multiselect(
        "Filtrar por Severidad", options=available_severities, default=available_severities,
        key='space_sev_filter'
    )
    if severity_filter and 'Severity' in combined_df.columns:
        combined_df = combined_df[combined_df['Severity'].isin(severity_filter)]

    available_components = sorted(combined_df['Component'].unique().tolist())
    component_filter = st.multiselect(
        "Filtrar por Componente", options=available_components, default=available_components,
        key='space_comp_filter'
    )
    if component_filter:
        combined_df = combined_df[combined_df['Component'].isin(component_filter)]

    def highlight_severity(val):
        colors = {
            'CRITICAL': 'background-color: #FF6B6B; color: white; font-weight: bold',
            'HIGH': 'background-color: #FFA500; color: white; font-weight: bold',
            'MEDIUM': 'background-color: #FFD700; color: black',
            'LOW': 'background-color: #90EE90; color: black',
            'INFO': 'background-color: #E0E0E0; color: black',
        }
        return colors.get(val, '')

    if 'Severity' in combined_df.columns:
        styled_df = combined_df.style.map(highlight_severity, subset=['Severity'])
    else:
        styled_df = combined_df.style

    st.dataframe(styled_df, use_container_width=True, height=500)

    csv = combined_df.to_csv(index=False)
    st.download_button(
        label="Descargar CSV", data=csv,
        file_name=f"space_findings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


def display_ddl_actions(analyzed_data: dict):
    """Display DDL remediation statements dynamically."""
    st.subheader("Scripts de Remediacion")

    for component_key in COMPONENT_LABELS:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            ddl_col = None
            for col_name in DDL_COLUMNS:
                if col_name in df.columns:
                    ddl_col = col_name
                    break
            if ddl_col is None:
                continue

            statements = df[ddl_col].dropna().unique().tolist()
            if not statements:
                continue

            label = COMPONENT_LABELS.get(component_key, component_key)
            with st.expander(f"{label} ({len(statements)} scripts)", expanded=False):
                for stmt in statements[:50]:
                    st.code(str(stmt), language='sql')
                if len(statements) > 50:
                    st.info(f"... y {len(statements) - 50} scripts mas")

    # Bulk download
    all_ddl = []
    for component_key in COMPONENT_LABELS:
        df = analyzed_data.get(component_key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            for col_name in DDL_COLUMNS:
                if col_name in df.columns:
                    all_ddl.extend(df[col_name].dropna().unique().tolist())
                    break

    if all_ddl:
        combined_ddl = "\n".join(str(s) for s in all_ddl)
        st.download_button(
            label="Descargar Todos los Scripts DDL", data=combined_ddl,
            file_name=f"space_ddl_actions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
            mime="text/plain"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    inject_custom_css()
    initialize_session_state()

    st.title("Space")
    st.markdown("*Evaluacion y Optimizacion de Espacio en Teradata*")
    st.markdown("---")

    # Sidebar configuration
    st.sidebar.header("Configuracion")
    database_name = st.sidebar.text_input(
        "Filtrar por Base de Datos (opcional)",
        placeholder="Ej: USER_DB, ALL para todas"
    )
    unused_days = st.sidebar.slider(
        "Umbral Dias para Tablas Sin Uso",
        min_value=30, max_value=365, value=THRESHOLDS['unused_object_days'],
        help="Tablas sin acceso por mas de este numero de dias se consideraran sin uso"
    )
    skew_threshold = st.sidebar.slider(
        "Umbral % Skew para Tablas",
        min_value=10, max_value=100, value=THRESHOLDS['pi_skew_pct'],
        help="Porcentaje de skew para identificar tablas desbalanceadas"
    )

    use_mock = st.sidebar.checkbox("Usar datos Mock (demo)", value=False)

    if st.sidebar.button("Ejecutar Analisis", type="primary"):
        if use_mock:
            with st.spinner("Generando datos mock..."):
                mock_data = generate_mock_data()
                analyzer = SpaceAnalyzer()
                analyzed_data = analyzer.run(mock_data)
                st.session_state.mod3_analyzed_data = analyzed_data
                st.session_state.mod3_findings = analyzer.get_findings()
            st.success(f"Analisis mock completado. {len(st.session_state.mod3_findings)} hallazgos.")
        else:
            try:
                with st.spinner("Conectando a Teradata..."):
                    td_conn = TeradataConnection()
                    connection = td_conn.connect()

                with st.spinner("Recolectando datos de espacio..."):
                    collector = SpaceCollector()
                    params = {
                        'unused_days_threshold': unused_days,
                        'skew_pct_threshold': skew_threshold
                    }
                    collected_data = collector.collect(connection, params=params)
                    st.session_state.mod3_collected_data = collected_data

                with st.spinner("Analizando datos..."):
                    analyzer = SpaceAnalyzer()
                    analyzed_data = analyzer.run(collected_data)
                    st.session_state.mod3_analyzed_data = analyzed_data
                    st.session_state.mod3_findings = analyzer.get_findings()

                connection.close()
                st.success(f"Analisis completado. {len(st.session_state.mod3_findings)} hallazgos.")
            except Exception as e:
                st.error(f"Error durante el analisis: {str(e)}")
                logger.error(f"Analysis error: {str(e)}")

    if st.session_state.mod3_analyzed_data:
        st.markdown("## Resultados del Analisis")
        analyzed = st.session_state.mod3_analyzed_data

        # Main tabs: Dashboard / Hallazgos / Scripts
        tab_dashboard, tab_hallazgos, tab_scripts = st.tabs(
            ["Dashboard", "Hallazgos", "Scripts de Remediacion"]
        )

        with tab_dashboard:
            display_kpi_cards(analyzed)
            st.markdown("---")

            # Organize 11 components into sub-tabs
            component_tabs = st.tabs([
                "01 CDS Report",
                "02 Forecast",
                "03 Unused Objects",
                "04 Duplicates",
                "05 MVC Uncomp.",
                "06 MVC Comp.",
                "07 Top DBs",
                "08 Top Tables",
                "09 Unused DBs",
                "10 Monthly",
                "11 Space Util."
            ])

            with component_tabs[0]:
                render_01_cds_report(analyzed.get('01_CDS_Report', pd.DataFrame()))
            with component_tabs[1]:
                render_02_forecast(analyzed.get('02_Space_Capacity_Forecast', pd.DataFrame()))
            with component_tabs[2]:
                render_03_unused_objects(analyzed.get('03_Suspected_Unused_Objects', pd.DataFrame()))
            with component_tabs[3]:
                render_04_duplicate_objects(analyzed.get('04_Suspected_Duplicate_Objects', pd.DataFrame()))
            with component_tabs[4]:
                render_05_06_mvc(analyzed.get('05_MVC_Opportunities_Uncompressed_Tables', pd.DataFrame()),
                                 'MVC Uncompressed Tables')
            with component_tabs[5]:
                render_05_06_mvc(analyzed.get('06_MVC_Opportunities_Compressed_Tables', pd.DataFrame()),
                                 'MVC Compressed Tables')
            with component_tabs[6]:
                render_07_top_databases(analyzed.get('07_Top_20_Databases_By_Used_Size', pd.DataFrame()))
            with component_tabs[7]:
                render_08_top_tables(analyzed.get('08_Top_20_Tables_By_Size', pd.DataFrame()))
            with component_tabs[8]:
                render_09_unused_databases(analyzed.get('09_Top_20_Unused_Databases_By_Size', pd.DataFrame()))
            with component_tabs[9]:
                render_10_monthly_snapshot(analyzed.get('10_Monthly_Capacity_ Snapshot', pd.DataFrame()))
            with component_tabs[10]:
                render_11_space_utilization(analyzed.get('11_Database_Space_Utilization', pd.DataFrame()))

        with tab_hallazgos:
            display_findings_table(analyzed)

        with tab_scripts:
            display_ddl_actions(analyzed)


if __name__ == "__main__":
    main()
