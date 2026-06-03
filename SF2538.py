import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# ===============================================================
# 1. TARGET CONFIGURATION
# ===============================================================
TARGET_JOB_NUMBER = "2538" 
# ===============================================================

st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 

# --- CORE UTILITIES ---

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Authentication Failed: {e}")
        return None

def ensure_tz_convert(series, target_tz):
    if series.dt.tz is None:
        return series.dt.tz_localize('UTC').dt.tz_convert(target_tz)
    return series.dt.tz_convert(target_tz)

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """Fetches approved client data with a built-in SQL garbage data filter."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    # 🛑 LAYER 1: SQL FILTERING
    # Hard-filters temperatures between -30°F and 120°F at the database level
    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id 
        AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)
        AND m.temperature >= -30.0 AND m.temperature <= 120.0
        AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')
        ORDER BY m.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    return client.query(query, job_config=job_config).to_dataframe()

# --- THE ENGINEERING GRAPHING ENGINE ---

def build_high_speed_graph(df, title, start_view, end_view, unit_mode, unit_label, 
                           display_tz="UTC", f_start_date=None, curve_id=None):
    if df.empty: return go.Figure().update_layout(title="No data available")

    client = get_bq_client()
    plot_df = df.copy() 
    fig = go.Figure()

    plot_df['timestamp'] = ensure_tz_convert(plot_df['timestamp'], display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]

    final_end_view, final_start_view = end_view, start_view
    proj_num = TARGET_JOB_NUMBER
    loc_part = str(curve_id).split('-')[-1] if curve_id else ""

    if f_start_date:
        try:
            ref_q = f"SELECT Day FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CurveID) LIKE UPPER('{proj_num}%') ORDER BY Day DESC LIMIT 1"
            ref_meta = client.query(ref_q).to_dataframe()
            if not ref_meta.empty:
                max_days = int(ref_meta['Day'].max())
                final_start_view = pd.Timestamp(f_start_date) - pd.Timedelta(days=1)
                final_end_view = pd.Timestamp(f_start_date) + pd.Timedelta(days=max_days + 1)
        except: pass

    if curve_id and f_start_date:
        try:
            dash_styles = ['dash', 'dashdot', 'dot', 'longdash', 'longdashdot']
            target_q = f"""
                SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE UPPER(CurveID) LIKE UPPER('%{TARGET_JOB_NUMBER}%') 
                AND UPPER(CurveID) LIKE UPPER('%{loc_part}%')
                ORDER BY Day
            """
            target_df = client.query(target_q).to_dataframe()
            
            if not target_df.empty:
                for idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                    c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                    c_df['timestamp'] = ensure_tz_convert(c_df['timestamp'], display_tz)
                    ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                    soil_label = str(cid).split('-')[-1].strip()
                    
                    fig.add_trace(go.Scatter(
                        x=c_df['timestamp'], y=ref_y, name=f"<b>Goal: {soil_label}</b>", mode='lines',
                        line=dict(color='rgba(80, 80, 80, 0.9)', width=4, dash=dash_styles[idx % len(dash_styles)], shape='spline', smoothing=1.3),
                        legendrank=1 
                    ))
        except: pass
            
    sf_15_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#FF1493', '#00CED1', '#FFD700', '#8A2BE2', '#32CD32']
    
    def get_legend_sort_key(node_id, df):
        row = df[df['NodeNum'] == node_id].iloc[0]
        bank = str(row['Bank']).upper() if pd.notnull(row['Bank']) else ""
        depth = pd.to_numeric(row['Depth'], errors='coerce') if pd.notnull(row['Depth']) else 999
        if 'R' in bank: return (0, bank, node_id) 
        if 'S' in bank: return (1, bank, node_id)
        return (2, depth, node_id)

    unique_nodes = plot_df['NodeNum'].unique()
    sorted_nodes = sorted(unique_nodes, key=lambda x: get_legend_sort_key(x, plot_df))

    for i, sn in enumerate(sorted_nodes):
        s_df = plot_df[plot_df['NodeNum'] == sn].sort_values('timestamp')
        depth_val, bank_val, loc_val = s_df['Depth'].iloc[0], s_df['Bank'].iloc[0], s_df['Location'].iloc[0]
        
        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            display_name = f"{bank_val} ({sn})"
        elif pd.notnull(depth_val): 
            display_name = f"{depth_val}ft ({sn})"
        else: 
            display_name = f"{loc_val} ({sn})"
        
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], y=s_df['temperature'], name=display_name, mode='lines',
            line=dict(shape='spline', smoothing=1.3, width=2, color=sf_15_palette[i % 15]),
            hovertemplate="<b>%{fullData.name}</b><br>Temp: %{y:.1f}" + unit_label + "<extra></extra>"
        ))
        
    fig.add_hline(y=freeze_pt, line_width=2, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F FREEZE", layer="above")
    now_ts = pd.Timestamp.now(tz=display_tz)
    fig.add_vline(x=now_ts.to_pydatetime(), line_width=2, line_color="red", line_dash="dash", layer='above')
    
    m_range = pd.date_range(start=final_start_view, end=final_end_view, freq='W-MON')
    for m_dt in m_range:
        fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)

    fig.update_layout(
        title=dict(text=f"<b>{title}</b>", x=0.02, y=0.98, font=dict(size=18)),
        plot_bgcolor='white', hovermode="x unified", height=650,
        xaxis=dict(
            range=[final_start_view, final_end_view], showgrid=True, gridcolor='Gainsboro',
            showline=True, mirror=True, linecolor='black', linewidth=2,
            minor=dict(dtick=1000*60*60*24, showgrid=True, gridcolor='#f8f8f8'), tickformat='%b %d'
        ),
        yaxis=dict(
            title=f"Temperature ({unit_label})", range=y_range, dtick=10,
            minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
            showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linecolor='black', linewidth=2
        ),
        legend=dict(orientation="v", x=1.02, y=1, xanchor="left", yanchor="top")
    )
    return fig

# --- UI TABS ---

def render_summary_tab(full_p_df, unit_label, local_tz):
    """
    Renders the Thermal Summary split across 4 clean structural groups.
    Matches the exact layout, logic, and latest snapshot calculations 
    of the main global summary dashboard.
    """
    st.subheader("🌐 24 hour Thermal Summary")
    
    # 1. Clean out project local timezones
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    
    # 2. Strict Priority-Based Pipe Classification
    def classify_pipe(row):
        loc = str(row.get('Location', '')).upper()
        bank = str(row.get('Bank', '')).upper()
        
        # Check Ambient FIRST to isolate air temps
        if any(x in loc or x in bank for x in ['AMBIENT', 'AMB', 'AIR', 'OUTSIDE', 'WEATHER']): 
            return 'Ambient'
            
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    df_local['PipeType'] = df_local.apply(classify_pipe, axis=1)
    
    # 3. Calculate 24-hour ranges BEFORE cutting down to the latest snapshot
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
    df_24h_window = df_local[df_local['timestamp'] >= (now_local - pd.Timedelta(days=1))]
    
    # 4. Isolate the absolute LATEST packet entry per individual sensor node
    # This prevents historical data mass from skewing current averages
    latest_snapshot = df_local.sort_values('timestamp').groupby('NodeNum').last().reset_index()

    cols = st.columns(4)
    categories = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)', 'Ambient']
    
    # Target benchmarks for percentage calculations
    kpi_benchmarks = {'Supply (S)': -10, 'Return (R)': 0, 'Temp Pipes (TP)': 32, 'Ambient': None}

    for i, p_type in enumerate(categories):
        with cols[i]:
            st.markdown(f"### {p_type}")
            
            # Isolate data for this category
            snap_type_df = latest_snapshot[latest_snapshot['PipeType'] == p_type]
            hist_type_df = df_24h_window[df_24h_window['PipeType'] == p_type]
            
            if snap_type_df.empty:
                st.caption("No data available.")
                continue

            # Compute current averages from the snapshot (Matches Dashboard)
            avg_val = snap_type_df['temperature'].mean()
            
            # Compute 24-hour boundaries from historical dataset
            if not hist_type_df.empty:
                high_val = hist_type_df['temperature'].max()
                low_val = hist_type_df['temperature'].min()
            else:
                high_val = snap_type_df['temperature'].max()
                low_val = snap_type_df['temperature'].min()

            # Render Main Average Metric
            st.metric("Avg (Latest)", f"{avg_val:.1f}{unit_label}")
            
            # Render Percentage Target KPIs
            kpi_target = kpi_benchmarks[p_type]
            if kpi_target is not None:
                total_nodes = len(snap_type_df)
                nodes_passing = len(snap_type_df[snap_type_df['temperature'] <= kpi_target])
                pct = (nodes_passing / total_nodes) * 100 if total_nodes > 0 else 0
                
                color = "green" if pct == 100 else "#FF8C00" if pct > 0 else "gray"
                st.markdown(f"<p style='font-size:0.85rem; color:{color}; margin-top:-10px;'><b>{pct:.0f}%</b> Nodes ≤ {kpi_target}°F</p>", unsafe_allow_html=True)
            else:
                st.markdown("<div style='height:19px;'></div>", unsafe_allow_html=True)

            # Render Structural Ranges
            sub1, sub2 = st.columns(2)
            sub1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")
            st.divider()

def render_depth_profile_tab(full_p_df):
    st.subheader("📏 Vertical Temperature Profile")
    full_p_df['Depth_Num'] = pd.to_numeric(full_p_df['Depth'], errors='coerce')
    depth_only = full_p_df.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_only.empty:
        st.info("Vertical profile data is not available for this project.")
        return

    for loc in sorted(depth_only['Location'].unique()):
        with st.expander(f"📏 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_only[depth_only['Location'] == loc].copy()
            fig_d = go.Figure()
            
            mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=4, freq='W-MON')
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                  (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                if not window.empty:
                    snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                    fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y'), line=dict(shape='spline', smoothing=0.5)))

            max_sensor_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_sensor_depth // 40) + 1) * 40) if pd.notnull(max_sensor_depth) else 40
            max_temp_seen = loc_data['temperature'].max()
            x_limit = 80 if max_temp_seen > 60 else 60

            fig_d.add_vline(x=32, line_width=2.5, line_dash="dash", line_color="MediumBlue", annotation_text="32°F FREEZE", annotation_position="top left", layer="above")
            
            fig_d.update_layout(
                plot_bgcolor='white', height=750, margin=dict(r=50, l=50, t=50, b=50),
                xaxis=dict(title="Temperature (°F)", range=[-20, x_limit], showgrid=True, gridcolor='Gainsboro', showline=True, mirror=True, linewidth=2, linecolor='black'),
                yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='Silver', showline=True, mirror=True, linewidth=2, linecolor='black'),
                legend=dict(orientation="h", y=-0.15, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_d, use_container_width=True, key=f"depth_profile_{loc}")

def render_client_portal():
    client = get_bq_client()
    if client is None: return

    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', TARGET_JOB_NUMBER)
    local_tz = primary_meta.get('Timezone', 'US/Pacific')
    
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz).date()
    f_start_date = None
    day_count_text = ""
    if pd.notnull(primary_meta.get('Date_Freezedown')):
        f_start_date = pd.to_datetime(primary_meta.get('Date_Freezedown')).date()
        days_since = (now_local - f_start_date).days
        day_count_text = f"🗓️ **Day {max(0, days_since)}** of Freezedown" if days_since >= 0 else f"⏳ **{abs(days_since)} Days** until Start"

    with st.spinner("Synchronizing official records..."):
        all_phases = [get_universal_portal_data(p_id) for p_id in proj_registry['Project']]
        full_p_df = pd.concat(all_phases) if all_phases else pd.DataFrame()

    if full_p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    # 🛑 LAYER 3: BACKUP APPLICATION SPECTRAL CLAMP
    full_p_df = full_p_df[(full_p_df['temperature'] >= -30.0) & (full_p_df['temperature'] <= 120.0)]

    st.title(f"📊 {display_name}")
    
    last_approved_local = ensure_tz_convert(full_p_df['timestamp'], local_tz).max()
    st.info(f"✅ **Official Data Status:** Records approved through **{last_approved_local.strftime('%B %d, %Y at %I:%M %p')}**.")

    head_c1, head_c2 = st.columns(2)
    with head_c1:
        if day_count_text: st.subheader(day_count_text)
    with head_c2:
        if f_start_date: st.write(f"**Freeze Start Date:** {f_start_date.strftime('%B %d, %Y')}")

    tabs = st.tabs(["🏠 Summary", "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As Built"])
    
    with tabs[0]:
        render_summary_tab(full_p_df, "°F", local_tz)

    with tabs[1]:
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        now_local_ts = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
        start_view = now_local_ts - timedelta(weeks=weeks_view)
        
        locations = sorted([str(loc) for loc in full_p_df['Location'].dropna().unique()])
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = full_p_df[full_p_df['Location'] == loc].copy()
                st.plotly_chart(build_high_speed_graph(
                    loc_data, f"{loc} History", start_view, now_local_ts, 
                    "Fahrenheit", "°F", local_tz, f_start_date, f"{TARGET_JOB_NUMBER}-{loc}"
                ), use_container_width=True)

    with tabs[2]:
        render_depth_profile_tab(full_p_df)
    
    with tabs[3]:
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['timestamp'] = ensure_tz_convert(latest['timestamp'], local_tz)
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)
       
    with tabs[4]:
        asbuilt_filename = primary_meta.get('AsBuiltFile')
        if pd.notnull(asbuilt_filename) and str(asbuilt_filename).strip() != "":
            possible_paths = [os.path.join("assets", "asbuilts", asbuilt_filename), asbuilt_filename, os.path.join("assets", asbuilt_filename)]
            img_found = False
            for path in possible_paths:
                if os.path.exists(path):
                    st.image(path, caption=f"Project Plan: {asbuilt_filename}", use_container_width=True)
                    img_found = True
                    break
            if not img_found:
                st.error(f"❌ Drawing Not Found: '{asbuilt_filename}'")
        else:
            st.info("ℹ️ The as-built site plan is currently being processed or has not been assigned in the Project Registry.")

# --- EXECUTION ---
render_client_portal()
