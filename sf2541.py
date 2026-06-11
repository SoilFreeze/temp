import streamlit as st
import pandas as pd
import time
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone, date, time as dt_time
import pytz
import traceback
import io
import re
import numpy as np

# 1. CONFIGURATION & STYLING
st.set_page_config(
    page_title="SoilFreeze Data Lab", 
    page_icon="❄️", 
    layout="wide"
)

# Global Database Constants
DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# Migration Targets for Google Sheets Phase
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry_backup"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry_native"

@st.cache_resource
def get_bq_client():
    """Initializes and caches the BigQuery connection."""
    try:
        SCOPES = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/drive" 
        ]
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None
        
############################
# - 2. DATA ENGINE LOGIC - #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data_base(project_id):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    base_job_num = str(project_id).split('-')[0].strip()
    query = f"""
        WITH filtered_base AS (
            SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_REGISTRY_TABLE}` p ON CAST(p.Project AS STRING) = CAST(@project_id AS STRING)
            WHERE (CAST(m.Project AS STRING) = CAST(@project_id AS STRING) OR CAST(m.Project AS STRING) = '{base_job_num}' OR CAST(m.Project AS STRING) LIKE '{base_job_num}%')
              AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)
              AND m.temperature >= -30.0 AND m.temperature <= 120.0
              AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')
        ),
        gap_evaluation AS (
            SELECT *,
                LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp ASC) as prev_timestamp
            FROM filtered_base
        )
        SELECT Project, NodeNum, Bank, Location, Depth, temperature, timestamp, approval_status
        FROM gap_evaluation
        WHERE prev_timestamp IS NULL 
           OR TIMESTAMP_DIFF(timestamp, prev_timestamp, HOUR) <= 12
        ORDER BY timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    return client.query(query, job_config=job_config).to_dataframe()
    
def get_universal_portal_data(project_id, view_mode="engineering"):
    client = get_bq_client()
    if client is None: return pd.DataFrame()

    is_office = "OFFICE" in str(project_id).upper()
    if view_mode == "client":
        filter_sql = "AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')"
    else:
        if is_office:
            filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) != 'BADDATA'"
        else:
            filter_sql = "AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0', 'MASKED')"

    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_REGISTRY_TABLE}` p ON CAST(m.Project AS STRING) = CAST(p.Project AS STRING)
        WHERE CAST(m.Project AS STRING) = CAST(@project_id AS STRING)
        {filter_sql}
        ORDER BY m.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ Data Sync Error: {e}")
        return pd.DataFrame()

###########################
# - SIDEBAR NAVIGATION -  #
###########################

st.sidebar.title("❄️ SoilFreeze Lab")
page = st.sidebar.selectbox("Navigation", ["Summary", "Time vs Temp", "Depth Charts", "Sensor Status", "Node Diagnostics", "Data Processing", "Admin Tools"], key="nav_page")
st.sidebar.divider()

selected_project = "All Projects"
project_metadata = None  
sidebar_client = get_bq_client()

if sidebar_client is not None:
    try:
        proj_q = f"""
            SELECT CAST(Project AS STRING) as Project, ProjectName, Timezone, ProjectStatus, Date_Freezedown, SoilType 
            FROM `{PROJECT_REGISTRY_TABLE}` 
            WHERE Project IS NOT NULL 
              AND TRIM(CAST(Project AS STRING)) != ''
              AND (ProjectStatus != 'Archived' OR UPPER(CAST(Project AS STRING)) LIKE '%OFFICE%')
        """
        proj_df = sidebar_client.query(proj_q).to_dataframe()
        proj_list = sorted([str(p).strip() for p in proj_df['Project'].unique() if p and str(p).strip().lower() not in ['none', 'nan', 'null', '']])
        selected_project = st.sidebar.selectbox("🎯 Active Project", ["All Projects"] + proj_list, key="sidebar_proj_picker_global")
        st.session_state['selected_project'] = selected_project
        
        if selected_project != "All Projects":
            meta_row = proj_df[proj_df['Project'] == selected_project]
            if not meta_row.empty:
                project_metadata = meta_row.iloc[0].to_dict()
                st.session_state['project_metadata'] = project_metadata
        else:
            st.session_state['project_metadata'] = None
    except Exception as e:
        st.sidebar.error(f"Registry Link Offline: {e}")

# DATA AGES TRACKING Engine
st.sidebar.subheader("⏱️ Current Data Ages")
if sidebar_client is not None:
    try:
        if selected_project == "All Projects":
            pulse_q = f"SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view`"
            scope_label = "Last Data"
        else:
            pulse_q = f"SELECT FORMAT_TIMESTAMP('%m/%d/%Y %H:%M UTC', MAX(timestamp)) as last_sync FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` WHERE CAST(Project AS STRING) = '{selected_project}'"
            scope_label = f"Job {selected_project.split('-')[0]} Age"

        pulse_df = sidebar_client.query(pulse_q).to_dataframe()
        if not pulse_df.empty and pulse_df['last_sync'].iloc[0]:
            last_sync_str = str(pulse_df['last_sync'].iloc[0])
            last_sync_ts = pd.to_datetime(last_sync_str, utc=True)
            elapsed_mins = int((pd.Timestamp.now(tz='UTC') - last_sync_ts).total_seconds() / 60)
            pulse_status = f"🟢 **Live** ({elapsed_mins}m ago)" if elapsed_mins <= 60 else f"🟠 **Delayed** ({elapsed_mins}m ago)" if elapsed_mins <= 180 else f"🔴 **Stale** ({elapsed_mins // 60}h ago)"
            st.sidebar.markdown(f"**{scope_label}:** {pulse_status}")
            st.sidebar.caption(f"Last Entry: `{last_sync_str}`")
        else:
            st.sidebar.markdown(f"**{scope_label}:** ❌ No Sync Records")
    except Exception as pulse_err:
        st.sidebar.caption(f"Pulse tracking suspended: {pulse_err}")

if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.toast("System cache completely cleared!", icon="🔄")
    time.sleep(0.5)
    st.rerun()
        
st.sidebar.divider()
st.sidebar.subheader("👁️ Visibility Controls")
st.sidebar.toggle("Show Theoretical Curves", value=True, key="global_show_ref")
st.sidebar.toggle("Show Masked Data", value=False, key="global_show_masked")
st.sidebar.toggle("Mobile Layout", value=False, key="mobile_optimized_toggle")
st.sidebar.divider()

st.sidebar.subheader("⏳ Timeline Navigation")
selected_weeks = st.sidebar.slider("Select History Window (Weeks)", min_value=1, max_value=12, value=5, step=1, key="global_lookback_weeks_slider")
st.session_state["global_lookback_days"] = selected_weeks * 7

# Raw HTML injection style definitions
st.sidebar.markdown("<style>div[data-baseweb=\"slider\"] > div > div { background: linear-gradient(to right, rgb(214, 39, 40) 0%, rgb(214, 39, 40) var(--slider-progress, 100%), rgb(230, 230, 230) var(--slider-progress, 100%)) !important; } div[role=\"slider\"] { background-color: rgb(214, 39, 40) !important; border: 2px solid rgb(214, 39, 40) !important; }</style>", unsafe_allow_html=True)
st.sidebar.markdown("<style>div[data-testid=\"stDataFrame\"] div[role=\"progressbar\"] > div { background-color: rgb(214, 39, 40) !important; }</style>", unsafe_allow_html=True)

st.sidebar.subheader("🌡️ Units")
unit_mode = st.sidebar.radio("Temperature Scale", ["Fahrenheit", "Celsius"], horizontal=True, key="unit_toggle")
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
st.session_state["unit_mode"], st.session_state["unit_label"] = unit_mode, unit_label
st.sidebar.divider()

st.sidebar.subheader("📱 Display & Time")
tz_lookup = {"UTC": "UTC", "Local (US/Eastern)": "US/Eastern", "Local (US/Pacific)": "US/Pacific"}
tz_mode = st.sidebar.selectbox("Timezone Display", list(tz_lookup.keys()), index=1 if project_metadata and project_metadata.get('Timezone') == "US/Eastern" else 2, key="tz_picker")
display_tz = tz_lookup[tz_mode]
st.session_state["display_tz"] = display_tz
st.sidebar.divider()

st.sidebar.subheader("📏 Reference Lines")
active_refs = [] 
if st.sidebar.checkbox("Freezing (32°F)", value=True, key="ref_freezing"): active_refs.append((32.0, "Freezing"))
if st.sidebar.checkbox("Type B (26.6°F)", value=False, key="ref_type_b"): active_refs.append((26.6, "Type B"))
if st.sidebar.checkbox("Type A (10.2°F)", value=False, key="ref_type_a"): active_refs.append((10.2, "Type A"))
st.session_state["active_refs"] = tuple(active_refs)

##################
# Graph Engine   #
##################

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz="UTC", mobile_mode=False, f_start_date=None, curve_id=None):
    if df.empty: return go.Figure().update_layout(title="No data available")
    client = get_bq_client()
    plot_df = df.copy() 

    if plot_df['timestamp'].dt.tz is None: plot_df['timestamp'] = plot_df['timestamp'].dt.tz_localize('UTC')
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(display_tz)
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]
    fig = go.Figure()

    if curve_id and curve_id != "None" and f_start_date:
        try:
            proj_match = re.findall(r'\d+', str(st.session_state.get('selected_project', '')))
            proj_num = proj_match[0] if proj_match else ""
            loc_part = str(curve_id).split('-')[-1].strip() if curve_id else ""
            if proj_num and loc_part:
                target_q = f"SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE REGEXP_CONTAINS(CurveID, r'^{proj_num}.*{loc_part}$') ORDER BY Day"
                target_df = client.query(target_q).to_dataframe()
                if not target_df.empty:
                    dash_styles = ['dashdot', 'dash', 'dot']
                    gray_shades = ['rgba(30,30,30,0.8)', 'rgba(70,70,70,0.75)', 'rgba(110,110,110,0.7)']
                    for c_idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                        c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                        c_df['timestamp'] = c_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert(display_tz)
                        ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                        label_clean = str(cid).replace(f"{proj_num}-", "").replace(f"-{loc_part}", "")
                        display_label = f"Goal: {label_clean}" if label_clean != loc_part else f"Goal: {loc_part}"
                        fig.add_trace(go.Scatter(x=c_df['timestamp'], y=ref_y, name=f"<b>{display_label}</b>", mode='lines', line=dict(color=gray_shades[c_idx % len(gray_shades)], width=3.5, dash=dash_styles[c_idx % len(dash_styles)], shape='spline', smoothing=1.3)))
        except Exception: pass

    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    node_metadata = []
    for sn in plot_df['NodeNum'].unique():
        node_df = plot_df[plot_df['NodeNum'] == sn]
        depth_val, bank_val, loc_val = node_df['Depth'].iloc[0], node_df['Bank'].iloc[0], node_df['Location'].iloc[0]
        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            display_name, sort_val = f"{bank_val} ({sn})", str(bank_val)  
        elif pd.notnull(depth_val) and not pd.isna(depth_val): 
            display_name, sort_val = f"{depth_val}ft ({sn})", f"depth_{float(depth_val):05.1f}" 
        else: 
            display_name, sort_val = f"{loc_val} ({sn})", str(loc_val)
        node_metadata.append({'node_num': sn, 'display_name': display_name, 'sort_key': sort_val})

    sorted_node_configs = sorted(node_metadata, key=lambda x: natural_sort_key(x['sort_key']))
    for i, config in enumerate(sorted_node_configs):
        sn, display_name = config['node_num'], config['display_name']
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp')
        s_df = s_df.set_index('timestamp').resample('1h').first().reset_index()
        fig.add_trace(go.Scatter(x=s_df['timestamp'], y=s_df['temperature'], name=display_name, mode='lines', connectgaps=False, line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]), hovertemplate="<b>%{fullData.name}</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"))

    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    fig.add_vline(x=pd.Timestamp.now(tz=display_tz).to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    for m_dt in pd.date_range(start=start_view, end=end_view, freq='W-MON'):
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    p_name = st.session_state.get('selected_project', 'Project')
    fig.update_layout(title=dict(text=f"<b>{p_name} - Thermal Trend - {title}</b>", x=0.02, y=0.98, font=dict(size=18)), plot_bgcolor='white', hovermode="x unified", height=650, xaxis=dict(range=[start_view, end_view], showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, hoverformat='%A, %b %d, %Y', tickformat='%b %d', minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8')), yaxis=dict(title=f"Temperature ({unit_label})", range=y_range, dtick=10, showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2, minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8')), legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top"))
    return fig

def apply_sanity_filter(df):
    if df.empty: return df
    bad_condition = (df['temperature'] > 120) | (df['temperature'] < -30)
    col = 'approval_status' if 'approval_status' in df.columns else 'approve' if 'approve' in df.columns else None
    if col: df.loc[bad_condition, col] = 'BADDATA'
    return df

##############################
# Page 1 - Dashboard Summary #
##############################

def render_summary_dashboard(unit_label, unit_mode, display_tz):
    st.header("🌐 Global Project Summary")
    client = get_bq_client()
    if client is None: return
    mobile_mode = st.session_state.get("mobile_optimized_toggle", False)

    summary_q = f"""
        WITH active_projects AS (
            SELECT CAST(Project AS STRING) as Project, ProjectName, ProjectStatus, Date_Freezedown
            FROM `{PROJECT_REGISTRY_TABLE}`
            WHERE ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze')
              AND UPPER(CAST(Project AS STRING)) NOT LIKE '%OFFICE%'
        ),
        raw_data AS (
            SELECT 
                CAST(n.Project AS STRING) as Project, n.Bank, n.Location, n.Depth, m.temperature, m.timestamp, n.NodeNum
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{NODE_REGISTRY_TABLE}` n ON TRIM(CAST(m.NodeNum AS STRING)) = TRIM(CAST(n.NodeNum AS STRING))
            WHERE m.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
              AND UPPER(COALESCE(CAST(m.approval_status AS STRING), 'PENDING')) NOT IN ('BADDATA', 'FALSE', '0')
              AND NOT (m.temperature > 100 AND NOT STARTS_WITH(n.NodeNum, 'SP'))
        ),
        MaxTime AS ( SELECT MAX(timestamp) as max_ts FROM raw_data ),
        LatestStats AS (
            SELECT 
                r.Project, r.Bank, r.Location, r.Depth, r.NodeNum,
                AVG(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_now,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 2 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as avg_1h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 7 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 6 HOUR) THEN r.temperature END) as avg_6h,
                AVG(CASE WHEN r.timestamp BETWEEN TIMESTAMP_SUB(m.max_ts, INTERVAL 25 HOUR) AND TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as avg_24h,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as min_now,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR) THEN r.temperature END) as max_now,
                MIN(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as min_24h,
                MAX(CASE WHEN r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR) THEN r.temperature END) as max_24h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 1 HOUR)) as checkins_1h,
                COUNTIF(r.timestamp >= TIMESTAMP_SUB(m.max_ts, INTERVAL 24 HOUR)) as checkins_24h,
                ARRAY_AGG(r.temperature ORDER BY r.timestamp DESC LIMIT 1)[OFFSET(0)] as latest_temp,
                MAX(r.timestamp) as latest_ts
            FROM raw_data r CROSS JOIN MaxTime m GROUP BY 1, 2, 3, 4, 5
        )
        SELECT 
            p.*, ls.*,
            (COUNTIF(ls.Bank LIKE 'S%' AND ls.latest_temp <= -10) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'S%') OVER(PARTITION BY p.Project), 0)) * 100 as supply_kpi,
            (COUNTIF(ls.Bank LIKE 'R%' AND ls.latest_temp <= 0) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Bank LIKE 'R%') OVER(PARTITION BY p.Project), 0)) * 100 as return_kpi,
            (COUNTIF(ls.Depth IS NOT NULL AND ls.latest_temp <= 32) OVER(PARTITION BY p.Project) / NULLIF(COUNTIF(ls.Depth IS NOT NULL) OVER(PARTITION BY p.Project), 0)) * 100 as freeze_kpi
        FROM active_projects p LEFT JOIN LatestStats ls ON p.Project = ls.Project
    """
    try:
        df = client.query(summary_q).to_dataframe()
        df[['Bank', 'Location']] = df[['Bank', 'Location']].fillna('')
    except Exception as e:
        st.error(f"Dashboard Query Failed: {e}")
        return

    for project in sorted(df['Project'].unique()):
        p_df = df[df['Project'] == project]
        p_name = p_df['ProjectName'].iloc[0] or project
        f_date = p_df['Date_Freezedown'].iloc[0]
        day_text, f_date_display = "", "Not Set"
        if pd.notnull(f_date):
            f_date_display = pd.to_datetime(f_date).strftime('%b %d, %Y')
            day_text = f"🗓️ **Day {max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(f_date).date()).days)}**"
        
        with st.container(border=True):
            h1, h2 = st.columns([2, 1])
            h1.subheader(f"🏗️ {p_name}")
            h2.markdown(f"<div style='text-align: right;'>{day_text}<br><small>Start: {f_date_display}</small></div>", unsafe_allow_html=True)
            proj_match = re.search(r'\b(\d{4})\b', str(project))
            if proj_match:
                st.markdown(f"🔗 **External Client Portal:** [{p_name} Portal Site Link](https://sf{proj_match.group(1)}.streamlit.app)")
            
            active_1h, active_24h, total_nodes = p_df[p_df['checkins_1h'] > 0]['NodeNum'].nunique(), p_df[p_df['checkins_24h'] > 0]['NodeNum'].nunique(), p_df['NodeNum'].dropna().nunique()
            st.markdown(f"📡 **Hardware Status:** `{active_1h}` nodes pinged in the last hour | `{active_24h}` nodes pinged in the last 24h (Total Pool: `{total_nodes}` registered)")
            st.divider() 

            is_amb = p_df['Bank'].str.contains('Amb', case=False) | p_df['Location'].str.contains('Amb', case=False)
            is_s = (p_df['Bank'].str.startswith('S') | p_df['Location'].str.startswith('S')) & ~is_amb
            is_r = (p_df['Bank'].str.startswith('R') | p_df['Location'].str.startswith('R')) & ~is_amb
            is_tp = p_df['Depth'].notnull() & ~is_s & ~is_r & ~is_amb
            groups_data = [("📥 Supply", p_df[is_s], "supply_kpi", -10), ("📤 Return", p_df[is_r], "return_kpi", 0), ("📏 TempPipes", p_df[is_tp], "freeze_kpi", 32), ("☁️ Ambient", p_df[is_amb], None, None)]

            if mobile_mode:
                for title, g_df, kpi_col, kpi_val in groups_data:
                    render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)
                    st.markdown("<hr style='border: 1px dashed #ccc; margin: 15px 0;'>", unsafe_allow_html=True)
            else:
                cols = st.columns([1, 0.1, 1, 0.1, 1, 0.1, 1])
                for s_idx in [1, 3, 5]: cols[s_idx].markdown("<div style='border-left: 1px solid #ddd; height: 320px; margin: auto;'></div>", unsafe_allow_html=True)
                for idx, (title, g_df, kpi_col, kpi_val) in enumerate(groups_data):
                    with cols[[0, 2, 4, 6][idx]]: render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label)

def render_dashboard_column(title, g_df, kpi_col, kpi_val, unit_mode, unit_label):
    st.markdown(f"**{title}**")
    if g_df.empty or g_df['latest_temp'].isnull().all():
        st.caption("No recent data"); return
    latest_val = g_df['latest_temp'].mean()
    def convert(v): return None if (pd.isnull(v) or pd.isna(v)) else (v - 32) * 5/9 if unit_mode == "Celsius" else v
    l_conv, c_min, c_max, m24, x24 = map(convert, [latest_val, g_df['min_now'].min(), g_df['max_now'].max(), g_df['min_24h'].min(), g_df['max_24h'].max()])
    st.metric("Avg (Latest)", f"{l_conv:.1f}{unit_label}")
    if kpi_col:
        pct = g_df[kpi_col].iloc[0]
        st.markdown(f"<p style='font-size:0.85rem; color:{'green' if pct == 100 else '#FF8C00' if pct > 0 else 'gray'};'><b>{pct:.0f}%</b> Nodes ≤ {kpi_val}°F</p>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size: 0.8rem; line-height: 1.2; margin-bottom: 10px;'><b>Normal Ranges:</b><br>Current: {f'{c_min:.1f} to {c_max:.1f}{unit_label}' if c_min else 'No Data'}<br>24h Range: {f'{m24:.1f} to {x24:.1f}{unit_label}' if m24 else 'No Data'}</div>", unsafe_allow_html=True)

def get_trend_arrow(current, previous):
    if pd.isnull(current) or pd.isnull(previous): return "N/A"
    return f"🔺 +{current - previous:.1f}" if current - previous > 0.1 else f"🔹 {current - previous:.1f}" if current - previous < -0.1 else "➡️ 0.0"

#############################
# - 2. PAGE: TIME vs TEMP - #
#############################

def render_global_overview(selected_project, project_metadata, display_tz):
    show_ref, show_masked = st.session_state.get("global_show_ref", True), st.session_state.get("global_show_masked", False)
    unit_mode, unit_label, active_refs = st.session_state.get("unit_mode", "Fahrenheit"), st.session_state.get("unit_label", "°F"), st.session_state.get("active_refs", [])
    p_name, status, f_start_date = selected_project, "Active", None

    if project_metadata:
        p_name, status = project_metadata.get('ProjectName', selected_project), project_metadata.get('ProjectStatus', 'Active')
        if pd.notnull(project_metadata.get('Date_Freezedown')): f_start_date = pd.to_datetime(project_metadata.get('Date_Freezedown')).date()

    st.header(f"📈 Time vs Temp: {p_name} [{status}]")
    if f_start_date: st.markdown(f"### 🗓️ Day **{max(0, (pd.Timestamp.now(tz=display_tz).date() - f_start_date).days)}** of Freezedown")
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Select a project in the sidebar to view engineering trends."); return

    with st.spinner(f"Syncing {p_name} telemetry..."): p_df = get_universal_portal_data(selected_project, view_mode="engineering")
    if p_df.empty: st.warning(f"No engineering data found for '{p_name}'."); return
    mask_col = 'approval_status' if 'approval_status' in p_df.columns else 'approve'
    if not show_masked and mask_col in p_df.columns: p_df = p_df[p_df[mask_col].astype(str).str.upper() != 'MASKED'].copy()

    end_view = (pd.Timestamp.now(tz=display_tz) + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_view = end_view - pd.Timedelta(weeks=st.session_state.get("global_lookback_weeks_slider", 5))

    for loc in sorted([str(loc) for loc in p_df['Location'].dropna().unique()], key=natural_sort_key):
        with st.expander(f"📍 Location: {loc}", expanded=True):
            fig = build_high_speed_graph(df=p_df[p_df['Location'] == loc].copy(), title=f"Thermal Trends: {loc}", start_view=start_view, end_view=end_view, active_refs=active_refs, unit_mode=unit_mode, unit_label=unit_label, display_tz=display_tz, f_start_date=f_start_date, curve_id=f"{str(selected_project).split('-')[0]}-{loc}" if (show_ref and any(x in loc.upper() for x in ["TP", "T", "PIPE", "TEMP"])) else None)
            st.plotly_chart(fig, use_container_width=True, key=f"tvt_{selected_project}_{loc}")

#########################
# Page 3 - Depth Charts #
#########################

def render_depth_charts(selected_project, unit_label, display_tz):
    st.header(f"📏 Depth Profile Analysis: {selected_project}")
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view depth profiles."); return
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    with st.spinner("Fetching historical telemetry..."): p_df = get_universal_portal_data(selected_project, view_mode="engineering")
    if p_df is None or p_df.empty: st.warning("No data found for this project."); return
    p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
    p_df = p_df[p_df['temperature'] <= 50.0]
    depth_df = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    if depth_df.empty: st.info("No sensors with valid 'Depth' values under 50°F found."); return

    unit_mode = st.session_state.get("unit_mode", "Fahrenheit")
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=lookback_weeks, freq='W-MON')
    
    for loc in sorted(depth_df['Location'].unique()):
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            if loc_data['timestamp'].dt.tz is None: loc_data['timestamp'] = loc_data['timestamp'].dt.tz_localize('UTC')
            loc_data['timestamp_local'] = loc_data['timestamp'].dt.tz_convert(display_tz)
            fig = go.Figure()

            baseline_ts = loc_data['timestamp_local'].min()
            b_window = loc_data[(loc_data['timestamp_local'] >= baseline_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp_local'] <= baseline_ts + pd.Timedelta(hours=12))]
            snap_base = b_window.assign(diff=(b_window['timestamp_local'] - baseline_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num') if not b_window.empty else pd.DataFrame()

            loc_data['date_str'], loc_data['hour_int'] = loc_data['timestamp_local'].dt.strftime('%Y-%m-%d'), loc_data['timestamp_local'].dt.hour
            recent_profile_rows = []
            if not loc_data.empty:
                for candidate_date in sorted(loc_data['date_str'].unique(), reverse=True):
                    if candidate_date == baseline_ts.strftime('%Y-%m-%d'): continue
                    day_pool = loc_data[loc_data['date_str'] == candidate_date]
                    if day_pool.empty: continue
                    recent_6am_date_str = candidate_date
                    for node_id, node_group in day_pool.groupby('NodeNum'):
                        exact_6am = node_group[node_group['hour_int'] == 6]
                        recent_profile_rows.append(exact_6am.sort_values('timestamp_local').iloc[-1] if not exact_6am.empty else node_group.assign(hour_dist=(node_group['hour_int'] - 6).abs()).sort_values(['hour_dist', 'timestamp_local']).iloc[0])
                    break
            snap_recent = pd.DataFrame(recent_profile_rows).sort_values('Depth_Num') if recent_profile_rows else pd.DataFrame()

            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                if target_ts.strftime('%Y-%m-%d') in [baseline_ts.strftime('%Y-%m-%d'), recent_6am_date_str]: continue
                window = loc_data[(loc_data['timestamp_local'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp_local'] <= target_ts + pd.Timedelta(hours=12))]
                if not window.empty:
                    snap_week = window.assign(diff=(window['timestamp_local'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                    temps = snap_week['temperature'] if unit_mode == "Fahrenheit" else (snap_week['temperature'] - 32) * 5/9
                    fig.add_trace(go.Scatter(x=temps, y=snap_week['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%Y-%m-%d'), line=dict(shape='spline', smoothing=1.1, width=1.5), marker=dict(size=4)))

            if not snap_recent.empty:
                r_temps = snap_recent['temperature'] if unit_mode == "Fahrenheit" else (snap_recent['temperature'] - 32) * 5/9
                fig.add_trace(go.Scatter(x=r_temps, y=snap_recent['Depth_Num'], mode='lines+markers', name=f'<b>Most Recent ({recent_6am_date_str} 6AM*)</b>', line=dict(color='#ff7f0e', width=3.5, shape='spline', smoothing=1.1), marker=dict(size=6, color='#ff7f0e'), text=snap_recent['timestamp_local'].dt.strftime('%b %d, %H:%M')))
            if not snap_base.empty:
                b_temps = snap_base['temperature'] if unit_mode == "Fahrenheit" else (snap_base['temperature'] - 32) * 5/9
                fig.add_trace(go.Scatter(x=b_temps, y=snap_base['Depth_Num'], mode='lines+markers', name=f'<b>Baseline ({baseline_ts.strftime("%Y-%m-%d")})</b>', line=dict(color='black', width=3, dash='dash'), marker=dict(size=5, color='black')))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")
            max_depth = loc_data['Depth_Num'].max()
            fig.update_layout(title=f"<b>Temp vs Depth - {loc}</b>", plot_bgcolor='white', height=800, xaxis=dict(title=f"Temperature ({unit_label})", range=[-20, 80], dtick=10, gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True), yaxis=dict(title="Depth (ft)", range=[int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50, 0], dtick=10, gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True), legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5))
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_{selected_project}_{loc}")

###########################
# PAGE 4: SENSOR STATUS - #
###########################

def fmt_temp(val, unit_mode, unit_label):
    if pd.isnull(val) or pd.isna(val): return "N/A"
    return f"{((val - 32) * 5/9 if unit_mode == "Celsius" else val):.1f}{unit_label}"

def assign_row_color(hours):
    if hours is None or pd.isna(hours) or hours == float('inf'): return "background-color: #d1d5db; color: #1f2937;"
    return "background-color: #d1fae5; color: #065f46;" if hours < 1.0 else "background-color: #fef08a; color: #854d0e;" if hours <= 6.0 else "background-color: #fed7aa; color: #9a3412;" if hours <= 12.0 else "background-color: #fca5a5; color: #991b1b;"

def render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz):
    p_meta = st.session_state.get('project_metadata')
    if not p_meta or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view sensor health."); return
    st.title(f"❄️ {p_meta.get('ProjectName', selected_project)}")
    if pd.notnull(p_meta.get('Date_Freezedown')): st.markdown(f"## 🗓️ Day **{max(0, (pd.Timestamp.now(tz=display_tz).date() - pd.to_datetime(p_meta.get('Date_Freezedown')).date()).days)}** of Freezedown")
    st.divider()

    query = f"""
        WITH BaseReporting AS ( SELECT m.NodeNum, m.timestamp, m.temperature, m.Location, m.Bank, m.Depth FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m WHERE CAST(m.Project AS STRING) = @proj_id ),
        GapAnalysis AS ( SELECT *, LAG(timestamp) OVER (PARTITION BY NodeNum ORDER BY timestamp) AS prev_ts FROM BaseReporting ),
        HistoricalStats AS (
            SELECT NodeNum, Location, Bank, Depth, MAX(timestamp) AS last_ping, ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS current_temp,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN temperature END) as avg_1h,
                AVG(CASE WHEN timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) as avg_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as seen_1h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR) THEN 1 ELSE 0 END) as seen_6h_f,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) as seen_24h_f,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 24.0) * 100 as coverage_24h,
                (COUNT(DISTINCT CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 HOUR) THEN TIMESTAMP_TRUNC(timestamp, HOUR) END) / 168.0) * 100 as coverage_7d,
                MIN(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS low_24h,
                MAX(CASE WHEN timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN temperature END) AS high_24h
            FROM GapAnalysis GROUP BY NodeNum, Location, Bank, Depth
        ) SELECT * FROM HistoricalStats
    """
    try:
        df = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("proj_id", "STRING", selected_project)])).to_dataframe()
        if df.empty: st.warning("No data found in master_data_view."); return
        df['last_seen_hrs'] = df['last_ping'].apply(lambda ts: 999.0 if pd.isnull(ts) else (pd.Timestamp.now(tz=display_tz) - (ts if ts.tzinfo else ts.tz_localize('UTC')).tz_convert(display_tz)).total_seconds() / 3600)
        
        st.subheader("📍 Location Performance Summary")
        summary_rows = [{'Location': loc, 'Total Nodes': len(g), 'Seen 1h': g['seen_1h_f'].sum(), 'Seen 6h': g['seen_6h_f'].sum(), 'Seen 24h': g['seen_24h_f'].sum(), '24h Coverage': f"{g['coverage_24h'].mean():.1f}%", '7d Coverage': f"{g['coverage_7d'].mean():.1f}%", 'Avg Temp': fmt_temp(g['current_temp'].mean(), unit_mode, unit_label), 'Low 24h': fmt_temp(g['low_24h'].min(), unit_mode, unit_label), 'High 24h': fmt_temp(g['high_24h'].max(), unit_mode, unit_label)} for loc, g in df.groupby('Location')]
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("🔍 Detailed Sensor Audit")
        f_loc = st.selectbox("Filter Audit by Location:", ["--- All ---"] + sorted(df['Location'].unique()))
        audit_df = df if f_loc == "--- All ---" else df[df['Location'] == f_loc]
        audit_rows = [{"Node": r['NodeNum'], "Location": r['Location'], "Position": f"{r['Depth']}ft" if pd.notnull(r['Depth']) else f"Bank {r['Bank']}", "Last Seen": f"🟢 {r['last_seen_hrs']:.1f}h" if r['last_seen_hrs'] <= 1.0 else f"🔴 {r['last_seen_hrs']:.1f}h", "24 hour coverage": f"{r['coverage_24h']:.1f}%", "Current Temp": fmt_temp(r['current_temp'], unit_mode, unit_label), "Change for 1 hr": get_trend_arrow(r['current_temp'], r['avg_1h']), "Change for 24 hr": get_trend_arrow(r['current_temp'], r['avg_24h'])} for _, r in audit_df.sort_values(['Location', 'Depth', 'Bank']).iterrows()]
        st.dataframe(audit_rows, use_container_width=True, hide_index=True)
    except Exception as e: st.error(f"Sensor Status Error: {e}")

###########################
# PAGE 5: NODE DIAGNOSTICS#
###########################

def render_node_diagnostics(selected_project, display_tz, unit_label):
    st.header("📡 Commissioning & Diagnostics Audit")
    client = get_bq_client()
    if client is None: return
    diag_q = f"""
        WITH Stats AS ( SELECT NodeNum, MAX(timestamp) as last_ping, ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp, COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) as count_1h, COUNTIF(timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) as count_24h, ARRAY_AGG(rssi ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as rssi_last, AVG(rssi) as rssi_avg FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum )
        SELECT CAST(n.Project AS STRING) as Project, n.Location, n.NodeNum, n.Bank, n.Depth, n.SensorStatus, s.last_ping, s.last_temp, COALESCE(s.count_1h, 0) as count_1h, COALESCE(s.count_24h, 0) as count_24h, s.rssi_last, s.rssi_avg FROM `{NODE_REGISTRY_TABLE}` n LEFT JOIN Stats s ON n.NodeNum = s.NodeNum WHERE n.End_Date IS NULL
    """
    try:
        df = client.query(diag_q).to_dataframe()
        if df.empty: st.warning("No nodes found in registry."); return
        df['hours_hidden'] = df['last_ping'].apply(lambda x: float('inf') if pd.isnull(x) else (pd.Timestamp.now(tz='UTC') - (x if x.tzinfo else x.tz_localize('UTC'))).total_seconds() / 3600.0)
        df = df.sort_values('hours_hidden').reset_index(drop=True)
        
        display_df = pd.DataFrame({"Node ID": df['NodeNum'], "Location": df['Location'].str.slice(0, 5), "Position": df.apply(lambda r: f"{r['Depth']}ft" if pd.notnull(r['Depth']) and r['Depth'] != 0 else str(r['Bank']), axis=1), "Current Temp": df['last_temp'].apply(lambda x: fmt_temp(x, st.session_state.get("unit_mode"), unit_label)), "Last Seen": df['hours_hidden'].apply(lambda h: f"{h:.1f}h ago" if h != float('inf') else "❌ Never"), "Pings (1h)": df['count_1h'].astype(int), "Pings (24h)": df['count_24h'].astype(int), "RSSI Last": df['rssi_last'].apply(lambda x: f"{int(x)} dBm" if pd.notnull(x) else "N/A"), "Reporting Efficiency": ((df['count_24h'] / 96.0) * 100.0).clip(upper=100.0)})
        st.dataframe(display_df, use_container_width=True, hide_index=True, column_config={"Reporting Efficiency": st.column_config.ProgressColumn("Reporting Efficiency", format="%.0f%%", min_value=0, max_value=100)})
    except Exception as e: st.error(f"Diagnostics Audit Failed: {e}")

###########################
# Page: Data Processing   #
###########################

def render_data_processing_page(selected_project):
    st.header("⚙️ Data Processing & Reference Engine")
    client = get_bq_client()
    if client is None: return
    tab_upload, tab_export, tab_ref_library, tab_event_log, tab_chiller_reg = st.tabs(["📄 Upload Telemetry", "📥 Export Report", "📈 Ref Curve Library", "🚨 Log Site Event", "❄️ Register Chiller"])
    EVENTS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.freezedown_events"
    CHILLER_REG_TABLE = f"{PROJECT_ID}.{DATASET_ID}.chiller_registry"

    with tab_upload:
        st.subheader("📄 Manual File Ingestion")
        u_file = st.file_uploader("Select CSV or Excel file", type=['csv', 'xlsx'], key="manual_upload_main")
        if u_file is not None:
            try:
                df_raw = pd.read_csv(u_file, dtype=str) if u_file.name.endswith('.csv') else pd.read_excel(u_file, dtype=str)
                st.success(f"File loaded successfully: {len(df_raw)} records detected.")
            except Exception as e: st.error(f"Read error: {e}")

    with tab_export:
        st.subheader("📥 Wide-Format Data Export")
        if not selected_project or selected_project == "All Projects": st.warning("⚠️ Select a specific project in the sidebar to export data.")
        else:
            full_df = get_universal_portal_data(selected_project, view_mode="engineering")
            if not full_df.empty:
                csv_data = full_df.to_csv(index=False).encode('utf-8')
                st.download_button(label="💾 Download Custom CSV Export", data=csv_data, file_name=f"{selected_project}_Export.csv", mime="text/csv", use_container_width=True)

    with tab_ref_library:
        st.subheader("📚 Theoretical Curve Library")
        try:
            inv_df = client.query(f"SELECT CurveID, COUNT(*) as Data_Points FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` GROUP BY CurveID").to_dataframe()
            st.dataframe(inv_df, use_container_width=True, hide_index=True)
        except Exception: st.info("Reference library table empty or uninitialized.")

    with tab_event_log:
        st.subheader("🚨 Log New Site Event Entry")
        with st.form("site_event_form"):
            e_desc = st.text_input("Operational Event Description*")
            if st.form_submit_button("Save Event") and e_desc.strip():
                sql = f"INSERT INTO `{EVENTS_TABLE}` (event_id, project_id, event_timestamp, event_description) VALUES ('{str(np.random.randint(100000))}', '{selected_project}', CURRENT_TIMESTAMP(), '{e_desc.replace("'", "''")}')"
                client.query(sql).result()
                st.success("Event tracked successfully!"); st.cache_data.clear()

    with tab_chiller_reg:
        st.subheader("❄️ Chiller Plant Infrastructure")
        try:
            ch_df = client.query(f"SELECT * FROM `{CHILLER_REG_TABLE}`").to_dataframe()
            st.dataframe(ch_df, use_container_width=True, hide_index=True)
        except Exception: st.info("Chiller asset database registry empty.")

######################
# Page: Admin Tools  #
######################

def render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header("🛠️ Admin Tools")
    client = get_bq_client()
    if client is None: return
    tab_admin_sum, tab_bulk_app, tab_logistics, tab_recovery, tab_proj_master, tab_bulk_config, tab_chillers = st.tabs(["📋 Admin Summary", "⚡ Bulk Approval", "📋 Node Master", "📡 Data Recovery", "⚙️ Project Master", "📦 Bulk Uploads", "❄️ Chiller Operations"])

    with tab_admin_sum:
        st.subheader("📋 Centralized Infrastructure Status Overview")
        sum_q = f"SELECT CAST(Project AS STRING) as Project, ProjectName, ProjectStatus, Date_Freezedown FROM `{PROJECT_REGISTRY_TABLE}` WHERE ProjectStatus IN ('Freezedown', 'Maintenance', 'Pre-freeze') ORDER BY Project ASC"
        try: st.dataframe(client.query(sum_q).to_dataframe(), use_container_width=True, hide_index=True)
        except Exception as e: st.error(f"Summary Load Failed: {e}")

    with tab_bulk_app:
        st.subheader("⚡ Administrative Mass Record Adjustments")
        if st.checkbox("Authorize System Changes"): st.info("Ready for batch execution routines.")

    with tab_logistics:
        st.subheader("📋 Node Master Registry Panel")
        reg_df = load_lab_node_registry_data(NODE_REGISTRY_TABLE)
        if not reg_df.empty:
            sel_node = st.selectbox("Select Target Node ID:", sorted(reg_df['NodeNum'].unique()))
            if sel_node:
                node_data = reg_df[reg_df['NodeNum'] == sel_node].iloc[0].to_dict()
                render_lab_node_action_manager(client, node_data, reg_df, [str(selected_project)], [], NODE_REGISTRY_TABLE)

    with tab_recovery:
        st.subheader("📡 Data Ingestion & Backfill Engine")
        st.info("Direct SensorPush API streaming engine backfill hooks.")

    with tab_proj_master:
        st.subheader("⚙️ Project Lifecycle Configuration Hub")
        p_q = f"SELECT * FROM `{PROJECT_REGISTRY_TABLE}` WHERE CAST(Project AS STRING) = '{selected_project}'"
        p_res = client.query(p_q).to_dataframe()
        if not p_res.empty:
            p_data = p_res.iloc[0].to_dict()
            with st.form("edit_project_metadata_form"):
                u_name = st.text_input("Project Name", value=p_data.get('ProjectName', ''))
                if st.form_submit_button("Overwrite Metadata"):
                    client.query(f"UPDATE `{PROJECT_REGISTRY_TABLE}` SET ProjectName='{u_name.replace("'", "''")}' WHERE CAST(Project AS STRING)='{selected_project}'").result()
                    st.success("Metadata updated!"); st.cache_data.clear()

    with tab_bulk_config:
        st.subheader("📦 CSV/Excel Spreadsheet Configuration Import Manager")
        u_file = st.file_uploader("Upload Configuration File", type=["csv"], key="bulk_admin_uploader")
        if u_file and st.button("Commit Batch Changes"):
            df = pd.read_csv(u_file)
            client.load_table_from_dataframe(df, NODE_REGISTRY_TABLE).result()
            st.success("Batch parameters synchronized!"); st.cache_data.clear()

    with tab_chillers: st.info("Mechanical cooling systems manifest module.")

def load_lab_node_registry_data(target_table):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    try:
        master_query = f"""
            WITH LatestTelemetry AS ( SELECT NodeNum, MAX(timestamp) as last_ping, ARRAY_AGG(temperature ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] as last_temp FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` GROUP BY NodeNum )
            SELECT CAST(R.Project AS STRING) as Project, R.Location, R.NodeNum, R.Bank, R.Depth, R.SensorStatus, R.Start_Date, R.End_Date, T.last_ping, T.last_temp FROM `{target_table}` R LEFT JOIN LatestTelemetry T ON R.NodeNum = T.NodeNum
        """
        df = client.query(master_query).to_dataframe()
        if not df.empty and 'last_ping' in df.columns:
            df['hours_hidden'] = df['last_ping'].apply(lambda x: (pd.Timestamp.now(tz='UTC') - pd.to_datetime(x).tz_convert('UTC')).total_seconds() / 3600.0 if pd.notnull(x) else float('inf'))
            df['Last Seen'] = df['hours_hidden'].apply(lambda h: f"{h:.1f}h ago" if h != float('inf') else "❌ Never")
        return df
    except Exception as e: st.error(f"Error loading node metrics: {e}"); return pd.DataFrame()

def render_lab_node_action_manager(client, selected_node_data, reg_df, proj_list, known_project_locations, target_registry):
    node_id = str(selected_node_data['NodeNum']).strip()
    with st.form("lab_node_attribute_edit_form"):
        st.write(f"📝 Assignment Form: **{node_id}**")
        edit_proj = st.selectbox("Assign Project ID", proj_list)
        edit_loc = st.text_input("Assign Location", value=selected_node_data.get('Location', ''))
        edit_phase = st.text_input("Phase Designation", value=str(selected_node_data.get('Phase', '1')))
        edit_system = st.text_input("System / Loop Designation", value=str(selected_node_data.get('System', '1')))
        edit_bank = st.text_input("Bank String", value=str(selected_node_data.get('Bank', '')))
        edit_depth = st.number_input("Placement Depth", value=float(selected_node_data.get('Depth', 0.0)))
        
        if st.form_submit_button("Commit Changes Row Line"):
            sql = f"""
                BEGIN TRANSACTION;
                DELETE FROM `{target_registry}` WHERE NodeNum = '{node_id}' AND Start_Date = DATE('{selected_node_data['Start_Date']}');
                INSERT INTO `{target_registry}` (NodeNum, Project, Location, Bank, Depth, SensorStatus, Start_Date, Phase, System)
                VALUES ('{node_id}', '{edit_proj.replace("'", "''")}', '{edit_loc.replace("'", "''")}', '{edit_bank.replace("'", "''")}', {edit_depth}, 'On Project', CURRENT_DATE(), '{edit_phase.replace("'", "''")}', '{edit_system.replace("'", "''")}');
                COMMIT;
            """
            client.query(sql).result()
            st.success("Asset configuration mapped cleanly!"); st.cache_data.clear()

###################
# 12. MAIN ROUTER #
###################

if page == "Summary":
    render_summary_dashboard(unit_label, unit_mode, display_tz)
elif page == "Time vs Temp":
    render_global_overview(selected_project, st.session_state.get('project_metadata'), display_tz) 
elif page == "Depth Charts":
    render_depth_charts(selected_project, unit_label, display_tz)
elif page == "Sensor Status":
    render_sensor_status(client, selected_project, unit_label, unit_mode, display_tz)
elif page == "Node Diagnostics":
    render_node_diagnostics(selected_project, display_tz, unit_label)
elif page in ["Data Processing", "Admin Tools"]:
    if st.session_state.get('authenticated', False):
        if page == "Data Processing": render_data_processing_page(selected_project)
        elif page == "Admin Tools": render_admin_page(selected_project, display_tz, unit_mode, unit_label, active_refs)
    else:
        st.divider()
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.subheader("🔐 Restricted Admin Access")
            pwd = st.text_input("Enter Admin Password", type="password")
            if st.button("Unlock Dashboard", use_container_width=True):
                if pwd == st.secrets["admin_password"]:
                    st.session_state['authenticated'] = True; st.rerun()
                else: st.error("Invalid Password. Access Denied.")
