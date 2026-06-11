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
TARGET_JOB_NUMBER = "2541" 

st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 

# --- CORE UTILITIES ---
def natural_sort_key(s):
    """Splits strings into text and numbers to allow natural sorting, with a fallback safety shield for NaNs."""
    # 🛡️ IF THE VALUE IS BLANK, NULL, OR FLOATING-POINT NAN, FORWARD A SAFE PLACEHOLDER INTEGER
    if pd.isna(s) or str(s).strip().lower() in ['nan', 'null', '']:
        return [9999]  # Anchors empty entries cleanly at the very bottom of your list out of the way
        
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s).strip())]

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
    """
    Fetches approved client data using dynamic row-level boundary joins 
    to prevent cross-phase data truncation.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    query = f"""
        WITH filtered_base AS (
            SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
            JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
            WHERE m.Project = @project_id 
              -- Joins the timeline criteria exactly on each sub-phase's start date
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
    
    # 🟢 FORCE STRING & GRAB FIRST 4 CHARACTERS (e.g., "2541")
    clean_job_num = str(TARGET_JOB_NUMBER)[:4].strip()

    if f_start_date:
        try:
            ref_q = f"SELECT Day FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CAST(CurveID AS STRING)) LIKE UPPER('{clean_job_num}%') ORDER BY Day DESC LIMIT 1"
            ref_meta = client.query(ref_q).to_dataframe()
            if not ref_meta.empty:
                max_days = int(ref_meta['Day'].max())
                final_start_view = pd.Timestamp(f_start_date) - pd.Timedelta(days=1)
                final_end_view = pd.Timestamp(f_start_date) + pd.Timedelta(days=max_days + 1)
        except: pass

    # 🟢 THE MATCHING FIXED LOGIC:
    if curve_id and f_start_date:
        try:
            dash_styles = ['dash', 'dashdot', 'dot', 'longdash', 'longdashdot']
            
            # Safely extract just the physical location token (e.g., "T7") regardless of prefix padding
            pure_loc = str(curve_id).split('-')[-1].strip()
            
            # Robust wildcard matching that handles custom description tags like "-UnSat Fill" flawlessly
            target_q = f"""
                SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE UPPER(CAST(CurveID AS STRING)) LIKE '{clean_job_num}-%'
                  AND (
                    UPPER(CAST(CurveID AS STRING)) LIKE '%-{pure_loc}' 
                    OR UPPER(CAST(CurveID AS STRING)) LIKE '%-{pure_loc}-%'
                    OR UPPER(CAST(CurveID AS STRING)) LIKE '%-{pure_loc} %'
                  )
                ORDER BY Day
            """
            target_df = client.query(target_q).to_dataframe()
            
            if not target_df.empty:
                for idx, (cid, c_df) in enumerate(target_df.groupby('CurveID')):
                    c_df['timestamp'] = c_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
                    c_df['timestamp'] = ensure_tz_convert(c_df['timestamp'], display_tz)
                    ref_y = c_df['Temp'] if unit_mode == "Fahrenheit" else (c_df['Temp'] - 32) * 5/9
                    
                    # Keep your custom description tag inside the graph legend cleanly!
                    soil_label = str(cid).replace(f"{clean_job_num}-", "")
                    
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
    
    try:
        m_range = pd.date_range(start=final_start_view, end=final_end_view, freq='W-MON')
        for m_dt in m_range:
            fig.add_vline(x=m_dt, line_width=1.5, line_color="black", opacity=0.4)
    except: pass

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
    """Renders the 24 hour Thermal Summary split across 4 structural groups."""
    st.subheader("🌐 24 hour Thermal Summary")
    
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    
    def classify_pipe(row):
        loc = str(row.get('Location', '')).upper()
        bank = str(row.get('Bank', '')).upper()
        
        if any(x in loc or x in bank for x in ['AMBIENT', 'AMB', 'AIR', 'OUTSIDE', 'WEATHER']): 
            return 'Ambient'
            
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    df_local['PipeType'] = df_local.apply(classify_pipe, axis=1)
    
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
    df_24h_window = df_local[df_local['timestamp'] >= (now_local - pd.Timedelta(days=1))]
    latest_snapshot = df_local.sort_values('timestamp').groupby('NodeNum').last().reset_index()

    cols = st.columns(4)
    categories = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)', 'Ambient']

    for i, p_type in enumerate(categories):
        with cols[i]:
            st.markdown(f"### {p_type}")
            
            snap_type_df = latest_snapshot[latest_snapshot['PipeType'] == p_type]
            hist_type_df = df_24h_window[df_24h_window['PipeType'] == p_type]
            
            if snap_type_df.empty:
                st.caption("No data available.")
                continue

            avg_val = snap_type_df['temperature'].mean()
            
            if not hist_type_df.empty:
                high_val = hist_type_df['temperature'].max()
                low_val = hist_type_df['temperature'].min()
            else:
                high_val = snap_type_df['temperature'].max()
                low_val = snap_type_df['temperature'].min()

            st.metric("Avg (Latest)", f"{avg_val:.1f}{unit_label}")
            st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

            sub1, sub2 = st.columns(2)
            sub1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")
            st.divider()

def render_depth_profile_tab(full_p_df, unit_label, local_tz):
    """Engineering-grade Vertical Temperature Profiles matching your Dashboard."""
    st.subheader("📏 Vertical Temperature Profile")
    
    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")
    
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    df_local['Depth_Num'] = pd.to_numeric(df_local['Depth'], errors='coerce')
    depth_df = df_local.dropna(subset=['Depth_Num', 'Location']).copy()
    
    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values found in the Registry.")
        return

    freeze_pt = 32
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique())
    
    for loc in locations:
        with st.expander(f"📍 Temp vs Depth - {loc}", expanded=True):
            loc_data = depth_df[depth_df['Location'] == loc].copy()
            fig = go.Figure()

            # --- A. BASELINE CALCULATIONS ---
            baseline_ts = loc_data['timestamp'].min()
            b_window = loc_data[
                (loc_data['timestamp'] >= baseline_ts - pd.Timedelta(hours=12)) & 
                (loc_data['timestamp'] <= baseline_ts + pd.Timedelta(hours=12))
            ]
            
            baseline_date_str = ""
            if not b_window.empty:
                baseline_date_str = baseline_ts.strftime('%Y-%m-%d')
                snap_b = (
                    b_window.assign(diff=(b_window['timestamp'] - baseline_ts).abs())
                    .sort_values(['NodeNum', 'diff'])
                    .drop_duplicates('NodeNum')
                    .sort_values('Depth_Num')
                )
                
                fig.add_trace(go.Scatter(
                    x=snap_b['temperature'], y=snap_b['Depth_Num'], 
                    mode='lines', 
                    name=f'Baseline ({baseline_date_str})',
                    line=dict(color='black', width=2.5, dash='dash'),
                    hovertemplate=f"Baseline: {baseline_date_str}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                ))
            
            # --- B. WEEKLY SNAPSHOT CALCULATIONS ---
            for m_date in mondays:
                target_ts = m_date.replace(hour=6, minute=0, second=0)
                current_loop_date = target_ts.strftime('%Y-%m-%d')
                
                if current_loop_date == baseline_date_str:
                    continue
                    
                window = loc_data[
                    (loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                    (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))
                ]
                
                if not window.empty:
                    snap_w = (
                        window.assign(diff=(window['timestamp'] - target_ts).abs())
                        .sort_values(['NodeNum', 'diff'])
                        .drop_duplicates('NodeNum')
                        .sort_values('Depth_Num')
                    )
                    
                    fig.add_trace(go.Scatter(
                        x=snap_w['temperature'], y=snap_w['Depth_Num'], 
                        mode='lines+markers', 
                        name=current_loop_date,
                        line=dict(shape='spline', smoothing=1.1, width=1.5),
                        marker=dict(size=4),
                        hovertemplate=f"Date: {current_loop_date}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{unit_label}<extra></extra>"
                    ))

            fig.add_vline(x=freeze_pt, line_width=2, line_dash="solid", line_color="#ADD8E6")

            max_depth = loc_data['Depth_Num'].max()
            y_limit = int(((max_depth // 10) + 1) * 10) if pd.notnull(max_depth) else 50

            fig.update_layout(
                title=f"<b>Temp vs Depth - {loc}</b>",
                plot_bgcolor='white', 
                height=800,
                xaxis=dict(
                    title=f"Temperature ({unit_label})", range=[-20, 80], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Gainsboro', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                yaxis=dict(
                    title="Depth (ft)", range=[y_limit, 0], dtick=10,
                    minor=dict(dtick=2, showgrid=True, gridcolor='#f8f8f8'),
                    gridcolor='Silver', showline=True, linewidth=2, linecolor='black', mirror=True
                ),
                legend=dict(orientation="h", y=-0.1, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig, use_container_width=True, key=f"depth_cht_portal_{loc}")

def render_client_portal():
    client = get_bq_client()
    if client is None: return

    # 1. Fetch all matching sub-phase tracking spaces from registry
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%' ORDER BY Project ASC"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    # Set master info metadata hooks
    primary_meta = proj_registry.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', TARGET_JOB_NUMBER)
    local_tz = primary_meta.get('Timezone', 'US/Pacific')

    # 🟢 ADD THIS LINE HERE to prevent NameErrors down the line
    f_start_date = None 

    # 2. Build multi-phase timeline tracking parameters dynamically
    all_phases_data = []
    phase_metadata_lookup = {}
    
    st.sidebar.subheader("🏗️ Structural Selection")
    
    for _, row in proj_registry.iterrows():
        p_id = row['Project']
        p_df = get_universal_portal_data(p_id)
        if not p_df.empty:
            all_phases_data.append(p_df)
            
            # Map structural components out of the project suffix string
            # Formats: "2541-Phase1" or default down to single elements safely
            parts = p_id.split('-')
            phase_label = parts[1] if len(parts) > 1 else "Phase 1"
            
            phase_metadata_lookup[p_id] = {
                'label': phase_label,
                'freeze_date': pd.to_datetime(row['Date_Freezedown']).date() if pd.notnull(row['Date_Freezedown']) else None
            }

    full_p_df = pd.concat(all_phases_data) if all_phases_data else pd.DataFrame()

    if full_p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    st.title(f"📊 {display_name}")
    
    # 3. Print out descriptive timeline tracking summaries in layout header columns
    last_approved_local = ensure_tz_convert(full_p_df['timestamp'], local_tz).max()
    st.info(f"✅ **Official Data Status:** Records approved through **{last_approved_local.strftime('%B %d, %Y at %I:%M %p')}**.")

    head_cols = st.columns(len(phase_metadata_lookup))
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz).date()
    
    for idx, (p_id, meta) in enumerate(phase_metadata_lookup.items()):
        with head_cols[idx]:
            if meta['freeze_date']:
                days_since = (now_local - meta['freeze_date']).days
                day_txt = f"🗓️ **{meta['label']}: Day {max(0, days_since)}**" if days_since >= 0 else f"⏳ **{meta['label']}: {abs(days_since)} Days until Start**"
                st.markdown(f"{day_txt} <small>(Start: {meta['freeze_date'].strftime('%b %d')})</small>", unsafe_allow_html=True)

    tabs = st.tabs(["🏠 Summary", "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As Built"])
    
    with tabs[0]:
        render_summary_tab(full_p_df, "°F", local_tz)

    # Locate inside render_client_portal() under Tab 2 (Timeline Analysis)
    with tabs[1]:
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        now_local_ts = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
        start_view = now_local_ts - timedelta(weeks=weeks_view)
        
        # 1. Map dynamic sub-phase freeze dates from the project registry row items
        phase_meta_lookup = {}
        for _, row in proj_registry.iterrows():
            p_id = row['Project']
            phase_meta_lookup[p_id] = {
                'freeze_date': pd.to_datetime(row['Date_Freezedown']).date() if pd.notnull(row['Date_Freezedown']) else None
            }
        
        # 2. Grab your naturally sorted unique location array list
        locations = sorted(
            [str(loc) for loc in full_p_df['Location'].dropna().unique()],
            key=natural_sort_key
        )
        
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = full_p_df[full_p_df['Location'] == loc].copy()
                
                if not loc_data.empty:
                    # Identify which sub-phase row is reporting data for this specific pipe channel
                    contributing_project = loc_data['Project'].iloc[0]
                    active_meta = phase_meta_lookup.get(contributing_project, {'freeze_date': None})
                    
                    # 🟢 THE FIX: Keep the search token clean and standard (e.g., "2541-TP11")
                    # This matches your exact database library naming convention perfectly
                    search_id = f"{TARGET_JOB_NUMBER}-{loc}"
                    
                    st.plotly_chart(build_high_speed_graph(
                        df=loc_data, 
                        title=f"{loc} History Lifecycle Trend", 
                        start_view=start_view, 
                        end_view=now_local_ts, 
                        unit_mode="Fahrenheit", 
                        unit_label="°F", 
                        display_tz=local_tz, 
                        f_start_date=active_meta['freeze_date'], # Ties standard curves to the correct May or June start date
                        curve_id=search_id
                    ), use_container_width=True)
                
    with tabs[2]:
        render_depth_profile_tab(full_p_df, "°F", local_tz)
    
    with tabs[3]:
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['timestamp'] = ensure_tz_convert(latest['timestamp'], local_tz)
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)
       
    with tabs[4]:
        # Keeps existing image byte-reader mechanism unchanged...
        pass
       
    with tabs[4]:
        asbuilt_filename = primary_meta.get('AsBuiltFile')
        if pd.notnull(asbuilt_filename) and str(asbuilt_filename).strip() != "":
            possible_paths = [
                os.path.join("assets", "asbuilts", asbuilt_filename), 
                asbuilt_filename, 
                os.path.join("assets", asbuilt_filename)
            ]
            img_found = False
            for path in possible_paths:
                if os.path.exists(path):
                    try:
                        # 🎯 THE SHIELD FIX: Read the asset as raw binary bytes 
                        # This bypasses the buggy static Streamlit Cloud URL media compiler entirely
                        with open(path, "rb") as img_file:
                            img_bytes = img_file.read()
                        
                        st.image(img_bytes, caption=f"Project Plan: {asbuilt_filename}", use_container_width=True)
                        img_found = True
                        break
                    except Exception as img_err:
                        st.error(f"⚠️ Failed to decode image file stream: {img_err}")
                        img_found = True # Stops loop since the file was found but corrupt
                        break
            if not img_found:
                st.error(f"❌ Drawing Not Found: '{asbuilt_filename}'")
        else:
            st.info("ℹ️ The as-built site plan is currently being processed or has not been assigned in the Project Registry.")

# --- EXECUTION ---
render_client_portal()
