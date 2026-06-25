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
# ===============================================================

st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 

# Migration Targets for Google Sheets / Native Table Migration Phase
PROJECT_REGISTRY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry_backup"

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
def get_universal_portal_data(selected_phase_name):
    """
    Fetches client telemetry directly from the v2 master view, using Python Regex 
    to create an indestructible, space-agnostic phase matching query.
    """
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    # ---------------------------------------------------------
    # SMART PHASE DETECTION
    # ---------------------------------------------------------
    # Check if the selected tab contains a Phase number (e.g. extracts "2" from "Phase2")
    phase_match = re.search(r'Phase\s*(\d+)', selected_phase_name, re.IGNORECASE)
    
    if phase_match:
        p_num = phase_match.group(1)
        # It IS a specific phase (e.g. Phase 2). Search for '2541' AND the digit '2'
        sql_phase_filter = f"""
            UPPER(CAST(m.Project AS STRING)) LIKE '%{TARGET_JOB_NUMBER}%'
            AND (
                UPPER(REPLACE(CAST(m.Phase AS STRING), ' ', '')) LIKE '%PHASE{p_num}%' OR 
                CAST(m.Phase AS STRING) = '{p_num}' OR
                UPPER(REPLACE(CAST(m.Project AS STRING), ' ', '')) LIKE '%PHASE{p_num}%'
            )
        """
    else:
        # It's the Base Project. Search for '2541' but explicitly EXCLUDE Phase 2, 3, etc.
        sql_phase_filter = f"""
            UPPER(CAST(m.Project AS STRING)) LIKE '%{TARGET_JOB_NUMBER}%'
            AND (
                m.Phase IS NULL OR 
                m.Phase = '' OR
                m.Phase = '1' OR
                (UPPER(CAST(m.Phase AS STRING)) NOT LIKE '%PHASE%' AND CAST(m.Phase AS STRING) NOT IN ('2', '3', '4', '5'))
            )
            AND UPPER(REPLACE(CAST(m.Project AS STRING), ' ', '')) NOT LIKE '%PHASE2%'
            AND UPPER(REPLACE(CAST(m.Project AS STRING), ' ', '')) NOT LIKE '%PHASE3%'
        """

    query = f"""
        WITH filtered_base AS (
            SELECT 
                m.Project, 
                m.Phase,
                m.System,
                m.NodeNum, 
                m.Bank, 
                m.Location, 
                m.Depth, 
                m.temperature, 
                m.timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view_v2` m
            WHERE 
              ({sql_phase_filter})
              AND m.temperature >= -30.0 AND m.temperature <= 120.0
              AND UPPER(TRIM(CAST(m.Location AS STRING))) NOT LIKE '%OFFICE%'
              AND UPPER(TRIM(CAST(m.Location AS STRING))) NOT LIKE '%DESK%'
              AND UPPER(TRIM(CAST(m.Location AS STRING))) NOT LIKE '%TEST%'
        ),
        gap_evaluation AS (
            SELECT 
                *,
                LAG(timestamp) OVER (PARTITION BY NodeNum, Location, Depth, Bank ORDER BY timestamp ASC) as prev_timestamp
            FROM filtered_base
        )
        SELECT 
            *
        FROM gap_evaluation
        WHERE prev_timestamp IS NULL 
           OR TIMESTAMP_DIFF(timestamp, prev_timestamp, HOUR) <= 24
        ORDER BY timestamp ASC
    """
    
    return client.query(query).to_dataframe()

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

    if curve_id and f_start_date:
        try:
            dash_styles = ['dash', 'dashdot', 'dot', 'longdash', 'longdashdot']
            # Use LIKE with a trailing wildcard to match the specific T-number
            # while allowing for the extra descriptive text (e.g., '-Sat Silt')
            target_q = f"""
                SELECT CurveID, Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` 
                WHERE REGEXP_CONTAINS(CurveID, r'^{curve_id}([^0-9]|$)')
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
        except Exception as e: 
            pass # Fail silently in production
            
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

    # Secondary cleaning filter step to make sure no loose office/desk items survive in telemetry subsets
    plot_df = plot_df[
        (~plot_df['PositionLabel'].str.upper().str.contains('OFFICE')) &
        (~plot_df['PositionLabel'].str.upper().str.contains('DESK')) &
        (~plot_df['PositionLabel'].str.upper().str.contains('TEST'))
    ]

    # Update the color map logic to give Ambient a permanent distinct color
    unique_positions = sorted(plot_df['PositionLabel'].unique(), key=natural_sort_key)
    position_color_map = {}
    for idx, pos in enumerate(unique_positions):
        if any(x in str(pos).upper() for x in ['AMBIENT', 'AMB', 'AIR', 'OUTSIDE']):
            position_color_map[pos] = 'rgba(100, 100, 100, 0.8)' # Distinct dark grey
        else:
            position_color_map[pos] = sf_15_palette[idx % len(sf_15_palette)]

    # Identify the latest node context checking in for each position to deduplicate legend display
    latest_nodes_by_pos = {}
    for pos in unique_positions:
        pos_df = plot_df[plot_df['PositionLabel'] == pos]
        if not pos_df.empty:
            latest_node = pos_df.sort_values('timestamp').iloc[-1]['NodeNum']
            latest_nodes_by_pos[pos] = latest_node

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
        active_node = latest_nodes_by_pos.get(pos, "Unknown")
        
        display_name = f"{pos} ({active_node})"
        
        # ⏱️ 24-HOUR CHART GAP BUILDER
        pos_df = pos_df.sort_values('timestamp')
        time_deltas = pos_df['timestamp'].diff()
        gap_indices = time_deltas[time_deltas > timedelta(hours=24)].index
        
        if not gap_indices.empty:
            inserted_gaps = []
            for idx in gap_indices:
                gap_row = pos_df.loc[idx].copy()
                prev_ts = pos_df.loc[pos_df.index[pos_df.index.get_loc(idx) - 1]]['timestamp']
                gap_row['timestamp'] = prev_ts + timedelta(seconds=1)
                gap_row['temperature'] = None  
                inserted_gaps.append(gap_row)
            
            pos_df = pd.concat([pos_df, pd.DataFrame(inserted_gaps)]).sort_values('timestamp').reset_index(drop=True)
        
        fig.add_trace(go.Scatter(
            x=pos_df['timestamp'], y=pos_df['temperature'], 
            name=display_name, 
            mode='lines',
            connectgaps=False,  
            line=dict(shape='spline', smoothing=1.3, width=2, color=position_color_map[pos]),
            showlegend=True,
            hovertemplate=f"<b>{pos}</b> (Node: %{{text}})<br>Temp: %{{y:.1f}}{unit_label}<extra></extra>",
            text=pos_df['NodeNum']
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
    st.subheader("📏 Vertical Temperature Profile")
    
    st.sidebar.subheader("📐 Profile Settings")
    lookback_weeks = st.sidebar.slider("Historical Snapshots (Weeks)", 1, 24, 8, key="depth_lookback")
    
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    df_local['Depth_Num'] = pd.to_numeric(df_local['Depth'], errors='coerce')
    depth_df = df_local.dropna(subset=['Depth_Num', 'Location']).copy()
    
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

    # =========================================================
    # 1. FETCH REGISTRY & CREATE MASTER PHASE SELECTOR
    # =========================================================
    proj_q = f"SELECT * FROM `{PROJECT_REGISTRY_TABLE}` WHERE CAST(Project AS STRING) LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    proj_registry = proj_registry.sort_values('Project').reset_index(drop=True)
    phase_list = proj_registry['Project'].tolist()
    
    if len(phase_list) > 1:
        st.markdown("### 🏗️ Select Project Phase")
        selected_phase = st.radio("Phase Selection", options=phase_list, horizontal=True, label_visibility="collapsed")
    else:
        selected_phase = phase_list[0]
        
    st.divider()

    # =========================================================
    # 2. LOCK METADATA TO THE SELECTED PHASE
    # =========================================================
    primary_meta = proj_registry[proj_registry['Project'] == selected_phase].iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', selected_phase)
    local_tz = primary_meta.get('Timezone', 'US/Pacific')
    
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz).date()
    f_start_date = None
    day_count_text = ""
    
    if pd.notnull(primary_meta.get('Date_Freezedown')):
        f_start_date = pd.to_datetime(primary_meta.get('Date_Freezedown')).date()
        days_since = (now_local - f_start_date).days
        day_count_text = f"🗓️ **Day {max(0, days_since)}** of Freezedown" if days_since >= 0 else f"⏳ **{abs(days_since)} Days** until Start"

    # =========================================================
    # 3. FETCH AND CLEAN DATA (STRICTLY FOR ACTIVE PHASE)
    # =========================================================
    with st.spinner(f"Synchronizing official records for {selected_phase}..."):
        full_p_df = get_universal_portal_data(selected_phase)

    if full_p_df.empty:
        st.warning(f"⚠️ No approved data records available yet for {selected_phase}.")
        return

    full_p_df = full_p_df[(full_p_df['temperature'] >= -30.0) & (full_p_df['temperature'] <= 120.0)]

    full_p_df = full_p_df[
        (~full_p_df['Location'].str.upper().str.contains('OFFICE')) &
        (~full_p_df['Location'].str.upper().str.contains('DESK')) &
        (~full_p_df['Location'].str.upper().str.contains('TEST'))
    ]

    # =========================================================
    # 4. RENDER UI HEADER
    # =========================================================
    st.title(f"📊 {display_name}")
    
    last_approved_local = ensure_tz_convert(full_p_df['timestamp'], local_tz).max()
    st.info(f"✅ **Official Data Status:** Records approved through **{last_approved_local.strftime('%B %d, %Y at %I:%M %p')}**.")

    head_c1, head_c2 = st.columns(2)
    with head_c1:
        if day_count_text: st.subheader(day_count_text)
    with head_c2:
        if f_start_date: st.write(f"**Freeze Start Date:** {f_start_date.strftime('%B %d, %Y')}")

    # =========================================================
    # 5. RENDER DASHBOARD TABS
    # =========================================================
    tabs = st.tabs(["🏠 Summary", "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As Built"])
    
    with tabs[0]:
        render_summary_tab(full_p_df, "°F", local_tz)

    with tabs[1]:
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        
        # --- AMBIENT ROUTING LOGIC ---
        ambient_keywords = ['AMBIENT', 'AMB', 'AIR', 'OUTSIDE', 'WEATHER']
        
        # 1. Isolate Ambient Data into its own dataframe
        ambient_mask = full_p_df['Location'].str.upper().apply(
            lambda x: any(k in x for k in ambient_keywords) if pd.notnull(x) else False
        )
        ambient_df = full_p_df[ambient_mask].copy()
        
        # 2. Get standard locations, explicitly filtering OUT ambient so it doesn't get its own tab
        locations = sorted([
            str(loc) for loc in full_p_df['Location'].dropna().unique() 
            if not any(k in str(loc).upper() for k in ambient_keywords)
        ], key=natural_sort_key)
        
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = full_p_df[full_p_df['Location'] == loc].copy()
                
                is_brine_pipe = any(x in str(loc).upper() for x in ['S', 'R', 'SUPPLY', 'RETURN'])
                
                # 3. INJECT AMBIENT DATA: If this is a brine pipe, append the ambient data
                if is_brine_pipe and not ambient_df.empty:
                    loc_data = pd.concat([loc_data, ambient_df])
                
                loc_last_data_ts = ensure_tz_convert(loc_data['timestamp'], local_tz).max()
                loc_start_view = loc_last_data_ts - timedelta(weeks=weeks_view)
                
                graph_curve_id = None if is_brine_pipe else f"{TARGET_JOB_NUMBER}-{loc}"
                
                st.plotly_chart(build_high_speed_graph(
                    loc_data, 
                    f"{loc} History", 
                    loc_start_view, 
                    loc_last_data_ts + timedelta(hours=2), 
                    "Fahrenheit", 
                    "°F", 
                    local_tz, 
                    f_start_date, 
                    graph_curve_id
                ), use_container_width=True)

    with tabs[2]:
        render_depth_profile_tab(full_p_df, "°F", local_tz)
    
    with tabs[3]:
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['timestamp'] = ensure_tz_convert(latest['timestamp'], local_tz)
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        
        latest['sort_idx'] = latest['Location'].apply(natural_sort_key)
        latest = latest.sort_values(by='sort_idx').drop(columns=['sort_idx'])
        
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)
        
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
                        with open(path, "rb") as img_file:
                            img_bytes = img_file.read()
                        
                        st.image(img_bytes, caption=f"Project Plan: {asbuilt_filename}", use_container_width=True)
                        img_found = True
                        break
                    except Exception as img_err:
                        st.error(f"⚠️ Failed to decode image file stream: {img_err}")
                        img_found = True 
                        break
            if not img_found:
                st.error(f"❌ Drawing Not Found: '{asbuilt_filename}'")
        else:
            st.info("ℹ️ The as-built site plan is currently being processed or has not been assigned in the Project Registry.")

# --- EXECUTION ---
render_client_portal()
