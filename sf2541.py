import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os

# ===============================================================
# 1. TARGET CONFIGURATION (CHANGE ONLY THIS LINE)
# ===============================================================
TARGET_JOB_NUMBER = "2527" 
# ===============================================================

st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")

# Hide standard sidebar navigation
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# --- HELPER FUNCTIONS ---

@st.cache_resource
def get_bq_client():
    """Initializes BigQuery client with necessary scopes."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

def ensure_tz_convert(series, target_tz):
    """Safely converts timestamps to the project's local timezone."""
    if series.dt.tz is None:
        return series.dt.tz_localize('UTC').dt.tz_convert(target_tz)
    return series.dt.tz_convert(target_tz)

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """Fetches approved data records strictly for the client portal."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id 
        AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)
        AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')
        ORDER BY m.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    return client.query(query, job_config=job_config).to_dataframe()

# --- UI COMPONENTS ---

def render_summary_tab(full_p_df, unit_label, local_tz):
    """Renders the 24h Thermal Summary split by Pipe Type."""
    st.subheader("🌐 24 hour Thermal Summary")
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    
    def classify_pipe(row):
        loc, bank = str(row['Location']).upper(), str(row['Bank']).upper()
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    df_local['PipeType'] = df_local.apply(classify_pipe, axis=1)
    cols = st.columns(3)
    categories = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)']

    for i, p_type in enumerate(categories):
        with cols[i]:
            st.markdown(f"### {p_type}")
            type_df = df_local[df_local['PipeType'] == p_type]
            if type_df.empty:
                st.caption("No data available.")
                continue

            df_24h = type_df[type_df['timestamp'] >= (now_local - pd.Timedelta(days=1))]
            latest_ts = type_df['timestamp'].max()
            lag_hrs = (now_local - latest_ts).total_seconds() / 3600
            
            target_df = df_24h if not df_24h.empty else type_df
            avg_val, high_val, low_val = target_df['temperature'].mean(), target_df['temperature'].max(), target_df['temperature'].min()

            label_pfx = "" if lag_hrs <= 1.5 else "Last "
            st.metric(f"{label_pfx}24h Average", f"{avg_val:.1f}{unit_label}")
            
            if lag_hrs > 1.5:
                st.error(f"⚠️ Data is {int(lag_hrs)}h old")
            else:
                st.success("🟢 Data is Live")

            sub1, sub2 = st.columns(2)
            sub1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")
            st.divider()

def render_client_portal(selected_project, project_metadata, display_tz, unit_mode, unit_label, active_refs):
    """
    Client-facing portal with approved thermal trends and vertical profiles.
    Includes Theoretical Goal overlays and professional chart borders.
    """
    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar to view client data.")
        return

    # 1. METADATA & HEADER
    meta = project_metadata if isinstance(project_metadata, dict) else {}
    display_name = meta.get('ProjectName', selected_project)
    project_status = meta.get('ProjectStatus', 'Active')
    f_start_date = pd.to_datetime(meta.get('Date_Freezedown')).date() if pd.notnull(meta.get('Date_Freezedown')) else None
    
    asbuilt_filename = meta.get('AsBuiltFile')
    registry_disclaimer = meta.get('ClientDisclaimer') 

    st.markdown(f"## 📊 {display_name}")
    st.markdown(f"<p style='color: #6d6d6d; font-size: 18px; margin-top: -15px;'>Status: {project_status}</p>", unsafe_allow_html=True)

    if pd.notnull(registry_disclaimer) and str(registry_disclaimer).strip() != "":
        st.info(f"ℹ️ {registry_disclaimer}")

    # 2. DATA FETCHING (CLIENT MODE)
    with st.spinner("Synchronizing official records..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No approved data records available for {display_name} yet.")
        return

    # 3. TABS
    tab_time, tab_depth, tab_table, tab_built = st.tabs([
        "📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table", "🗺️ As-Built Plan"
    ])

    # --- TAB 1: TIMELINE ANALYSIS ---
    with tab_time:
        st.sidebar.subheader("📅 Portal View Options")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6, key="client_weeks_slider")
        show_ref = st.sidebar.toggle("Show Progress Goals", value=True)
        
        # Calculate viewport
        now_utc = pd.Timestamp.now(tz='UTC')
        start_view = now_utc - timedelta(weeks=weeks_view)
        
        locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = p_df[p_df['Location'] == loc].copy()
                
                # --- CRITICAL FIX: Build the search ID and pass f_start_date ---
                clean_proj_id = str(selected_project).split('-')[0]
                cid = f"{clean_proj_id}-{loc}" if show_ref else None

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc}: {weeks_view}-Week Trend", 
                    start_view=start_view, 
                    end_view=now_utc, 
                    active_refs=active_refs, 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz,
                    f_start_date=f_start_date, # Passed from metadata
                    curve_id=cid
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")
                
    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not available for this project.")
        else:
            x_range = [-20, 40] if unit_mode == "Celsius" else [-10, 80]
            
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 Temp vs Depth - {loc}", expanded=True):
                    loc_data = depth_only[depth_only['Location'] == loc].copy()
                    fig_d = go.Figure()
                    
                    # Weekly Snapshots
                    mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=4, freq='W-MON')
                    for m_date in mondays:
                        target_ts = m_date.replace(hour=6, minute=0, second=0)
                        window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                         (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                        
                        if not window.empty:
                            snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                            c_temps = snap_df['temperature'] if unit_mode == "Fahrenheit" else (snap_df['temperature'] - 32) * 5/9
                            
                            fig_d.add_trace(go.Scatter(
                                x=c_temps, y=snap_df['Depth_Num'], 
                                mode='lines+markers', name=target_ts.strftime('%m/%d/%y'),
                                line=dict(shape='spline', smoothing=0.5)
                            ))

                    # --- ADD THEORETICAL GOAL TO DEPTH CHART ---
                    if show_ref and f_start_date:
                        try:
                            client = get_bq_client()
                            today_day = (pd.Timestamp.now().date() - f_start_date).days
                            ref_q = f"SELECT Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CurveID) LIKE UPPER('%{selected_project}-{loc}%') AND Day <= {today_day} ORDER BY Day DESC LIMIT 1"
                            res = client.query(ref_q).to_dataframe()
                            if not res.empty:
                                goal_temp = res.iloc[0]['Temp'] if unit_mode == "Fahrenheit" else (res.iloc[0]['Temp'] - 32) * 5/9
                                fig_d.add_vline(x=goal_temp, line_dash="dot", line_color="Red", annotation_text="Target Goal")
                        except: pass

                    max_d = depth_only['Depth_Num'].max()
                    y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50
                    
                    fig_d.update_layout(
                        plot_bgcolor='white', height=600,
                        # FULL BOARDER
                        xaxis=dict(title=f"Temp ({unit_label})", range=x_range, showline=True, mirror=True, linecolor='black'),
                        yaxis=dict(title="Depth (ft)", range=[y_limit, 0], showline=True, mirror=True, linecolor='black'),
                        legend=dict(orientation="h", y=-0.2, xanchor="center", x=0.5)
                    )
                    st.plotly_chart(fig_d, use_container_width=True, key=f"portal_depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else (f"Bank {r['Bank']}" if pd.notnull(r.get('Bank')) else "Surface"), axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']].sort_values(['Location', 'Position']), use_container_width=True, hide_index=True)

    # --- TAB 4: AS-BUILT PLAN ---
    with tab_built:
        if pd.notnull(asbuilt_filename):
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"Site Plan: {display_name}")
        else:
            st.info("The as-built site plan is currently being processed.")

# --- EXECUTION ---
render_client_portal()
