import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import re
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# ===============================================================
# 1. DYNAMIC TARGET CONFIGURATION
# ===============================================================

# 1. Fetch from secrets or URL FIRST (No visual Streamlit commands yet!)
TARGET_JOB_NUMBER = None
if "JOB_NUMBER" in st.secrets:
    TARGET_JOB_NUMBER = str(st.secrets["JOB_NUMBER"])
elif "job_number" in st.secrets:
    TARGET_JOB_NUMBER = str(st.secrets["job_number"])
else:
    TARGET_JOB_NUMBER = st.query_params.get("job", None)

# 2. PAGE CONFIG MUST BE THE VERY FIRST STREAMLIT COMMAND
page_title = f"SoilFreeze Portal #{TARGET_JOB_NUMBER}" if TARGET_JOB_NUMBER else "SoilFreeze Client Portal"
st.set_page_config(page_title=page_title, layout="wide")
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

# 3. If STILL no job number is found, show the manual entry screen
if not TARGET_JOB_NUMBER:
    st.title("🌐 SoilFreeze Client Portal")
    st.info("Please enter your assigned Job Number to view project telemetry.")
    
    manual_job = st.text_input("Job Number:", placeholder="e.g., 2527")
    
    if not manual_job:
        st.stop()  # 🛑 Halts script execution here until a number is entered
        
    # Once they hit enter, update the URL and rerun the script from the top
    st.query_params["job"] = str(manual_job)
    st.rerun()

# ===============================================================
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 

# Migration Targets for Google Sheets / Native Table Migration Phase
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry"
NODE_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.node_registry_synced"

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

def natural_sort_key(s):
    """Splits text and numbers to allow natural sorting (e.g., T1, T2, T3 instead of T1, T10)."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'(\d+)', str(s))]

@st.cache_data(ttl=600)
def get_universal_portal_data(target_job_number):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    root_job_id = str(target_job_number).split('-')[0].strip()
    
    # 1. Fetch the raw, approved telemetry directly from the master view
    query = f"""
        SELECT 
            Project, NodeNum, Bank, Location, Depth, temperature, timestamp, approval_status, SensorStatus
        FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2`
        WHERE TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) = @root_job_id
          
          -- 🔒 STRICT ALLOWLIST
          AND UPPER(TRIM(CAST(approval_status AS STRING))) = 'TRUE'
          
          -- 🎛️ RETIREMENT FILTER
          AND UPPER(TRIM(CAST(SensorStatus AS STRING))) IN ('ON PROJECT', 'AVAILABLE', 'MISSING')
          
          -- 🚫 EXCLUSION RULES
          AND UPPER(TRIM(CAST(Location AS STRING))) NOT LIKE '%OFFICE%'
          AND UPPER(TRIM(CAST(Location AS STRING))) NOT LIKE '%DESK%'
          AND UPPER(TRIM(CAST(Location AS STRING))) NOT LIKE '%TEST%'
          AND UPPER(TRIM(CAST(Project AS STRING))) NOT LIKE '%OFFICE%'
          
          AND temperature >= -30.0 AND temperature <= 120.0
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("root_job_id", "STRING", root_job_id)]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    
    if df.empty: return pd.DataFrame()
    
    # 2. Pull the Registry Dates
    reg_q = f"""
        SELECT Project, NodeNum, Location, Start_Date, End_Date 
        FROM `{NODE_REGISTRY_TABLE}` 
        WHERE TRIM(SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)]) = @root_job_id
    """
    reg_df = client.query(reg_q, job_config=job_config).to_dataframe()
    
    # 3. Process Time Boundaries safely using Pandas
    if not reg_df.empty:
        # Force Project columns to be strings and strip whitespace to prevent merge failures
        df['Project'] = df['Project'].astype(str).str.strip()
        reg_df['Project'] = reg_df['Project'].astype(str).str.strip()
        
        # Pandas effortlessly absorbs ANY date format coming from Google Sheets
        reg_df['Start_Date'] = pd.to_datetime(reg_df['Start_Date'], errors='coerce', utc=True)
        reg_df['End_Date'] = pd.to_datetime(reg_df['End_Date'], errors='coerce', utc=True)
        
        # Merge exactly on Project, Node, AND Location to prevent Cartesian cloning (e.g., TP-0142 at T8 vs T17)
        df = df.merge(reg_df[['Project', 'NodeNum', 'Location', 'Start_Date', 'End_Date']], 
                      on=['Project', 'NodeNum', 'Location'], 
                      how='left')
        
        # Apply historical boundary filters
        df = df[df['Start_Date'].isna() | (df['timestamp'] >= df['Start_Date'])]
        df = df[df['End_Date'].isna() | (df['timestamp'] <= df['End_Date'])]
    
    # 4. Generate the 24-hour gap evaluation
    # Keeps data clean by explicitly terminating the line if a sensor goes offline for >24 hours
    df = df.sort_values(by=['NodeNum', 'Location', 'Depth', 'Bank', 'timestamp'])
    df['prev_timestamp'] = df.groupby(['NodeNum', 'Location', 'Depth', 'Bank'])['timestamp'].shift(1)
    
    df = df[df['prev_timestamp'].isna() | ((df['timestamp'] - df['prev_timestamp']).dt.total_seconds() / 3600 <= 24)]
    
    return df.sort_values('timestamp')


# --- THE ENGINEERING GRAPHING ENGINE ---

def build_high_speed_graph(df, title, start_view, end_view, unit_mode, unit_label, 
                           display_tz="UTC", f_start_date=None, curve_id=None, ambient_df=None, target_phase=None):
    if df.empty: return go.Figure().update_layout(title="No data available")

    client = get_bq_client()
    plot_df = df.copy() 
    fig = go.Figure()

    plot_df['timestamp'] = ensure_tz_convert(plot_df['timestamp'], display_tz)
    
    freeze_pt = 0 if unit_mode == "Celsius" else 32
    y_range = [-30, 30] if unit_mode == "Celsius" else [-20, 80]

    final_end_view, final_start_view = end_view, start_view
    loc_part = str(curve_id).split('-')[-1] if curve_id else ""

    if curve_id and f_start_date:
        try:
            dash_styles = ['dash', 'dashdot', 'dot', 'longdash', 'longdashdot']
            
            # 🛡️ Extract just the numbers from the location (e.g., "T1" -> "1")
            digits = re.findall(r'\d+', loc_part)
            loc_digit = digits[0] if digits else loc_part
            target_phase_str = target_phase if target_phase else TARGET_JOB_NUMBER
            
            target_q = f"""
                SELECT CurveID, Day, Temp 
                FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE UPPER(CurveID) LIKE UPPER('%{target_phase_str}%') 
                -- 🎯 EXACT MATCH: Forces a non-numeric boundary after the number so T1 doesn't match T11
                AND REGEXP_CONTAINS(UPPER(CurveID), r'(?i)T[P]?0?{loc_digit}([^0-9]|$)')
                -- 🚫 BRINE EXCLUSION: Database-level block to keep curves off Brine charts
                AND NOT REGEXP_CONTAINS(UPPER(CurveID), r'(?i)BRINE')
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
    
    def get_position_string(row):
        depth_val, bank_val, loc_val = row['Depth'], row['Bank'], row['Location']
        if pd.notnull(bank_val) and any(x in str(bank_val).upper() for x in ['S', 'R']):
            return str(bank_val)
        elif pd.notnull(depth_val) and str(depth_val).strip() != '' and float(depth_val) != 0: 
            return f"{depth_val}ft"
        else: 
            return str(loc_val)

    plot_df['PositionLabel'] = plot_df.apply(get_position_string, axis=1)

    # Secondary cleaning filter step to make sure no loose office/desk items survive
    plot_df = plot_df[
        (~plot_df['PositionLabel'].str.upper().str.contains('OFFICE')) &
        (~plot_df['PositionLabel'].str.upper().str.contains('DESK')) &
        (~plot_df['PositionLabel'].str.upper().str.contains('TEST'))
    ]

    unique_positions = sorted(plot_df['PositionLabel'].unique(), key=natural_sort_key)
    position_color_map = {pos: sf_15_palette[idx % len(sf_15_palette)] for idx, pos in enumerate(unique_positions)}

    def get_legend_sort_key(pos_str, df):
        sub_df = df[df['PositionLabel'] == pos_str]
        if sub_df.empty: return (3, 0, pos_str)
        row = sub_df.iloc[0]
        bank = str(row['Bank']).upper() if pd.notnull(row['Bank']) else ""
        depth = pd.to_numeric(row['Depth'], errors='coerce') if pd.notnull(row['Depth']) else 999
        if 'R' in bank: return (0, bank, pos_str) 
        if 'S' in bank: return (1, bank, pos_str)
        return (2, depth, pos_str)

    sorted_positions = sorted(unique_positions, key=lambda x: get_legend_sort_key(x, plot_df))

    for pos in sorted_positions:
        pos_df = plot_df[plot_df['PositionLabel'] == pos].sort_values('timestamp')
        if pos_df.empty: continue
        
        # 🛡️ SPLIT BY SENSOR: Draw a separate line for every unique sensor that lived at this position
        for node_id in pos_df['NodeNum'].unique():
            node_pos_df = pos_df[pos_df['NodeNum'] == node_id].copy()
            
            # ⏱️ 24-HOUR CHART GAP BUILDER
            node_pos_df = node_pos_df.sort_values('timestamp').reset_index(drop=True)
            time_deltas = node_pos_df['timestamp'].diff()
            gap_indices = time_deltas[time_deltas > timedelta(hours=24)].index
            
            if not gap_indices.empty:
                inserted_gaps = []
                for idx in gap_indices:
                    gap_row = node_pos_df.loc[idx].copy()
                    prev_ts = node_pos_df.loc[idx - 1]['timestamp']
                    gap_row['timestamp'] = prev_ts + timedelta(seconds=1)
                    gap_row['temperature'] = None  # None kills the connecting segment trace
                    inserted_gaps.append(gap_row)
                
                node_pos_df = pd.concat([node_pos_df, pd.DataFrame(inserted_gaps)]).sort_values('timestamp').reset_index(drop=True)
            
            display_name = f"{pos} ({node_id})"
            
            fig.add_trace(go.Scatter(
                x=node_pos_df['timestamp'], y=node_pos_df['temperature'], 
                name=display_name, 
                mode='lines',
                connectgaps=False,  # Enforces physical segment termination at None rows
                line=dict(shape='spline', smoothing=1.3, width=2, color=position_color_map[pos]),
                showlegend=True,
                hovertemplate=f"<b>{pos}</b> (Node: %{{text}})<br>Temp: %{{y:.1f}}{unit_label}<extra></extra>",
                text=node_pos_df['NodeNum']
            ))

    # --- INJECT AMBIENT DATA ONTO BRINE GRAPHS ---
    if ambient_df is not None and not ambient_df.empty:
        amb_plot_df = ambient_df.copy()
        
        # ⏱️ TIMEZONE FIX: Aligns ambient data with the local time of the current graph
        amb_plot_df['timestamp'] = ensure_tz_convert(amb_plot_df['timestamp'], display_tz)
        
        for sn in amb_plot_df['NodeNum'].unique():
            a_df = amb_plot_df[amb_plot_df['NodeNum'] == sn].sort_values('timestamp')
            
            # 🛡️ VISUAL FIX: Limit ambient trace to the exact view window of the current graph
            a_df = a_df[(a_df['timestamp'] >= final_start_view) & (a_df['timestamp'] <= final_end_view)]
            
            if not a_df.empty:
                fig.add_trace(go.Scatter(
                    x=a_df['timestamp'], y=a_df['temperature'],
                    name=f"Ambient Air ({sn})", mode='lines',
                    connectgaps=False,
                    line=dict(width=2.5, dash='dot', color='orange'),
                    hovertemplate="<b>Ambient Air</b><br>Time: %{x|%H:%M}<br>Temp: %{y:.1f}" + unit_label + "<extra></extra>",
                    legendrank=99 
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

def render_pipe_summary_table(full_p_df, unit_label, local_tz):
    """Renders a granular Current vs 24-hour Extremes Summary for each individual pipe/location."""
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
    df_24h = df_local[df_local['timestamp'] >= (now_local - pd.Timedelta(days=1))]
    
    if df_24h.empty:
        st.info("No approved data available in the last 24 hours.")
        return
        
    summary_data = []
    locations = sorted(df_local['Location'].unique(), key=natural_sort_key)
    
    for loc in locations:
        # Skip Ambient for the granular pipe summary 
        if 'AMBIENT' in str(loc).upper(): continue
        
        loc_df = df_local[df_local['Location'] == loc]
        loc_24h = df_24h[df_24h['Location'] == loc]
        
        if loc_24h.empty: continue
        
        # 1. Calculate Current Extremes from the absolute latest reading of each active node
        latest_nodes = loc_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        if not latest_nodes.empty:
            c_max_idx = latest_nodes['temperature'].idxmax()
            c_min_idx = latest_nodes['temperature'].idxmin()
            
            c_high_temp = latest_nodes.loc[c_max_idx, 'temperature']
            c_high_node = latest_nodes.loc[c_max_idx, 'NodeNum']
            
            c_low_temp = latest_nodes.loc[c_min_idx, 'temperature']
            c_low_node = latest_nodes.loc[c_min_idx, 'NodeNum']
        else:
            c_high_temp, c_high_node, c_low_temp, c_low_node = None, "N/A", None, "N/A"
        
        # 2. Find 24h Extremes across the entire trailing window
        max_row = loc_24h.loc[loc_24h['temperature'].idxmax()]
        min_row = loc_24h.loc[loc_24h['temperature'].idxmin()]
        
        h24_temp, h24_node = max_row['temperature'], max_row['NodeNum']
        l24_temp, l24_node = min_row['temperature'], min_row['NodeNum']
        
        # Helper to neatly format the temperature and node ID
        def fmt_temp_node(t, n):
            if pd.isnull(t): return "N/A"
            return f"{t:.1f}{unit_label} ({n})"
        
        summary_data.append({
            "Pipe / Location": loc,
            "Current High": fmt_temp_node(c_high_temp, c_high_node),
            "Current Low": fmt_temp_node(c_low_temp, c_low_node),
        })
        
    st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

def render_depth_profile_tab(full_p_df, unit_label, local_tz):
    """Engineering-grade Vertical Temperature Profiles matching your Dashboard."""
    st.subheader("📏 Vertical Temperature Profile")

    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")

    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    df_local['Depth_Num'] = pd.to_numeric(df_local['Depth'], errors='coerce')
    depth_df = df_local.dropna(subset=['Depth_Num', 'Location']).copy()

    # Post-extraction sanity pass to keep out any test configurations that slipped through
    depth_df = depth_df[
        (~depth_df['Location'].str.upper().str.contains('OFFICE')) &
        (~depth_df['Location'].str.upper().str.contains('DESK')) &
        (~depth_df['Location'].str.upper().str.contains('TEST'))
    ]

    if depth_df.empty:
        st.info("No sensors with valid 'Depth' values found in the Registry.")
        return

    freeze_pt = 32
    now_utc = pd.Timestamp.now(tz='UTC')
    mondays = pd.date_range(end=now_utc, periods=lookback_weeks, freq='W-MON')
    locations = sorted(depth_df['Location'].unique(), key=natural_sort_key)

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

    # 🔥 Fix: Isolate the root job ID rigorously to pull accurate phases.
    root_job_id = str(TARGET_JOB_NUMBER).split('-')[0].strip()
    proj_q = f"SELECT * FROM `{PROJECT_REGISTRY_TABLE}` WHERE SPLIT(CAST(Project AS STRING), '-')[OFFSET(0)] = '{root_job_id}'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    # --- 🎛️ PHASE SELECTOR UI (BUILT FROM ACTUAL DATA) ---
    
    with st.spinner("Synchronizing official records..."):
        # 1. Fetch data for all phases FIRST so we know exactly what Project IDs exist
        master_df = get_universal_portal_data(TARGET_JOB_NUMBER)

    if master_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    master_df = master_df[(master_df['temperature'] >= -30.0) & (master_df['temperature'] <= 120.0)]
    master_df = master_df[
        (~master_df['Location'].str.upper().str.contains('OFFICE')) &
        (~master_df['Location'].str.upper().str.contains('DESK')) &
        (~master_df['Location'].str.upper().str.contains('TEST'))
    ]

    # 2. Build the dropdown from the official Project Registry so ALL phases show up
    proj_registry['Project'] = proj_registry['Project'].astype(str).str.strip()
    master_df['Project'] = master_df['Project'].astype(str).str.strip()
    
    available_phases = sorted(proj_registry['Project'].dropna().unique(), key=natural_sort_key)
    
    if len(available_phases) > 1:
        st.sidebar.markdown("### 📂 Project Phase")
        selected_phase = st.sidebar.selectbox("Select Phase/System:", available_phases)
    elif len(available_phases) == 1:
        selected_phase = available_phases[0]
    else:
        st.error("No valid phases found in the data.")
        return

    # 3. Isolate data exclusively for the chosen phase
    target_phase_clean = str(selected_phase).strip()
    full_p_df = master_df[master_df['Project'] == target_phase_clean].copy()

    # 🛠️ SMART ID TRANSLATOR v3: Base Project Matcher
    # Connects phase-specific registry names (e.g., "2541-Blackjack Phase 2") 
    # to the master telemetry ID (e.g., "2541-Blackjack")
    if full_p_df.empty:
        available_telemetry_projects = master_df['Project'].astype(str).str.strip().dropna().unique()
        
        for telemetry_proj in available_telemetry_projects:
            # If the telemetry ID is a base string of the dropdown phase (or vice versa), link them!
            if telemetry_proj in target_phase_clean or target_phase_clean in telemetry_proj:
                full_p_df = master_df[master_df['Project'] == telemetry_proj].copy()
                break
                
        # Absolute fallback: just show all data matching the root job number to prevent a blank screen
        if full_p_df.empty:
            root_id = str(TARGET_JOB_NUMBER).split('-')[0].strip()
            full_p_df = master_df[master_df['Project'].str.startswith(root_id, na=False)].copy()

    # --- ☁️ AMBIENT WEATHER SHARING FIX ---
    ambient_mask_master = master_df['Location'].astype(str).str.upper().str.contains('AMBIENT')
    ambient_data_global = master_df[ambient_mask_master].copy()
    
    # Safely check if ambient data exists in the current phase before merging
    if not full_p_df.empty:
        ambient_mask_phase = full_p_df['Location'].astype(str).str.upper().str.contains('AMBIENT')
        if not ambient_data_global.empty and not ambient_mask_phase.any():
            full_p_df = pd.concat([full_p_df, ambient_data_global], ignore_index=True)
            
    # 🚨 DIAGNOSTIC SAFETY NET 
    if full_p_df.empty:
        st.error(f"❌ **Data Mismatch Detected!**")
        st.warning(f"The dropdown is looking for phase: `{target_phase_clean}`")
        st.info("But the telemetry database only contains the following Project IDs:")
        st.write(master_df['Project'].unique())
        st.stop() # Halts the script so you can see the error clearly
            


    # --- ☁️ AMBIENT WEATHER SHARING FIX ---
    ambient_mask_master = master_df['Location'].astype(str).str.upper().str.contains('AMBIENT')
    ambient_data_global = master_df[ambient_mask_master].copy()
    
    ambient_mask_phase = full_p_df['Location'].astype(str).str.upper().str.contains('AMBIENT')
    if not ambient_data_global.empty and not ambient_mask_phase.any():
        full_p_df = pd.concat([full_p_df, ambient_data_global], ignore_index=True)

    # 4. Isolate metadata for UI (Fallback gracefully if data naming doesn't perfectly match registry)
    proj_registry['Project'] = proj_registry['Project'].astype(str).str.strip()
    phase_row = proj_registry[proj_registry['Project'] == target_phase_clean]
    
    if phase_row.empty:
        # Fallback: Just grab the first registry entry that shares the root job ID
        phase_row = proj_registry.iloc[[0]]

    primary_meta = phase_row.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', selected_phase)
    local_tz = primary_meta.get('Timezone', 'US/Pacific')
    
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz).date()
    f_start_date = None
    day_count_text = ""
    if pd.notnull(primary_meta.get('Date_Freezedown')):
        f_start_date = pd.to_datetime(primary_meta.get('Date_Freezedown')).date()
        days_since = (now_local - f_start_date).days
        day_count_text = f"🗓️ **Day {max(0, days_since)}** of Freezedown" if days_since >= 0 else f"⏳ **{abs(days_since)} Days** until Start"

    if full_p_df.empty:
        st.warning(f"⚠️ No approved data records available for phase {selected_phase}.")
        return

    st.title(f"📊 {display_name}")
    
    last_approved_local = ensure_tz_convert(full_p_df['timestamp'], local_tz).max()
    if pd.notnull(last_approved_local):
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
        
        # ☁️ ISOLATE AMBIENT DATA LOCALLY (now guaranteed to exist if the site has an ambient sensor)
        ambient_mask = full_p_df['Location'].astype(str).str.upper().str.contains('AMBIENT')
        ambient_df = full_p_df[ambient_mask].copy()
        
        # Filter locations to remove ambient from creating its own expander tab
        raw_locs = [str(loc) for loc in full_p_df['Location'].dropna().unique()]
        locations = sorted([loc for loc in raw_locs if 'AMBIENT' not in loc.upper()], key=natural_sort_key)
        
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = full_p_df[full_p_df['Location'] == loc].copy()
                
                matched_project_id = loc_data['Project'].iloc[0]
                
                # Fetch phase info specifically for this data split
                current_phase_row = proj_registry[proj_registry['Project'] == matched_project_id]
                
                loc_last_data_ts = ensure_tz_convert(loc_data['timestamp'], local_tz).max()
                loc_start_view = loc_last_data_ts - timedelta(weeks=weeks_view)
                loc_f_start_date = f_start_date
                
                if not current_phase_row.empty:
                    raw_phase_fd = current_phase_row.iloc[0].get('Date_Freezedown')
                    if pd.notnull(raw_phase_fd):
                        loc_f_start_date = pd.to_datetime(raw_phase_fd).date()
                        loc_start_view = pd.Timestamp(loc_f_start_date).tz_localize(local_tz)
                        
                        if weeks_view:
                            loc_start_view = loc_last_data_ts - timedelta(weeks=weeks_view)
                
                # 🛡️ STRICT BRINE CHECK
                loc_upper = str(loc).upper().strip()
                is_brine_pipe = (
                    loc_upper.startswith('S') or 
                    loc_upper.startswith('R') or 
                    any(x in loc_upper for x in ['SUPPLY', 'RETURN', 'BRINE', 'BANK'])
                )
                
                graph_curve_id = None if is_brine_pipe else f"{selected_phase}-{loc}"
                
                # 🎯 TARGETED INJECTION: Pass ambient_df to Brine graphs, ignore for Temp Pipes
                target_ambient = ambient_df if is_brine_pipe else None
                
                st.plotly_chart(build_high_speed_graph(
                    loc_data, 
                    f"{loc} History", 
                    loc_start_view, 
                    loc_last_data_ts + timedelta(hours=2), 
                    "Fahrenheit", 
                    "°F", 
                    local_tz, 
                    loc_f_start_date, 
                    graph_curve_id,
                    target_ambient,
                    selected_phase # Pass the selected phase so the query filters correctly
                ), use_container_width=True)

    with tabs[2]:
        render_depth_profile_tab(full_p_df, "°F", local_tz)
    
    with tabs[3]:
        st.subheader("📋 24-Hour Pipe Summary Table")
        render_pipe_summary_table(full_p_df, "°F", local_tz)
       
    with tabs[4]:
        asbuilt_raw = primary_meta.get('AsBuiltFile')
        if pd.notnull(asbuilt_raw) and str(asbuilt_raw).strip() != "":
            asbuilt_filenames = [f.strip() for f in re.split(r'[,;]', str(asbuilt_raw)) if f.strip()]
            
            if not asbuilt_filenames:
                 st.info("ℹ️ The as-built site plan is currently being processed or has not been assigned in the Project Registry.")
            else:
                for filename in asbuilt_filenames:
                    possible_paths = [
                        os.path.join("assets", "asbuilts", filename), 
                        filename, 
                        os.path.join("assets", filename)
                    ]
                    img_found = False
                    for path in possible_paths:
                        if os.path.exists(path):
                            try:
                                with open(path, "rb") as img_file:
                                    img_bytes = img_file.read()
                                
                                st.image(img_bytes, caption=f"Project Plan: {filename}", use_container_width=True)
                                st.markdown("<br>", unsafe_allow_html=True) 
                                img_found = True
                                break
                            except Exception as img_err:
                                st.error(f"⚠️ Failed to decode image file stream for {filename}: {img_err}")
                                img_found = True 
                                break
                    
                    if not img_found:
                        st.error(f"❌ Drawing Not Found: '{filename}'")
        else:
            st.info("ℹ️ The as-built site plan is currently being processed or has not been assigned in the Project Registry.")

# --- EXECUTION ---
render_client_portal()
