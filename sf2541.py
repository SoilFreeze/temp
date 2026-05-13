import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# ===============================================================
# 1. TARGET CONFIGURATION (CHANGE ONLY THIS LINE)
# ===============================================================
TARGET_JOB_NUMBER = "2527" 
# ===============================================================

# 2. GLOBAL UI CONFIG
# This is where your error was: TARGET_JOB_NUMBER must be defined above this line.
st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")

# Hide Streamlit's default sidebar navigation for a cleaner client look
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

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
        st.error(f"❌ BigQuery Authentication Failed: {e}")
        return None

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="client"):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    
    # Strict Client Logic: Only Approved (TRUE) AND NOT Masked
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
def render_summary_tab(full_p_df, unit_label, display_tz):
    """
    Renders the 24h average, high, and low split by S, R, and Temp Pipes.
    Includes staleness tracking for inactive sensors.
    """
    st.subheader("🌐 Project Thermal Overview (Last 24 Hours)")
    
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # 1. CLASSIFY DATA BY PIPE TYPE
    # S = Supply, R = Return, TP = Everything else with a Depth
    def classify_pipe(row):
        loc = str(row['Location']).upper()
        bank = str(row['Bank']).upper()
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    full_p_df['PipeType'] = full_p_df.apply(classify_pipe, axis=1)

    # 2. CREATE COLUMN LAYOUT
    cols = st.columns(3)
    pipe_types = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)']

    for i, p_type in enumerate(pipe_types):
        with cols[i]:
            st.markdown(f"### {p_type}")
            type_df = full_p_df[full_p_df['PipeType'] == p_type]
            
            if type_df.empty:
                st.caption("No data available for this category.")
                continue

            # Identify Latest Readings per Node
            latest_readings = type_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
            
            # Identify 24h Window for Metrics
            df_24h = type_df[type_df['timestamp'] >= (now_utc - pd.Timedelta(days=1))]

            # CALCULATE METRICS
            if not df_24h.empty:
                avg_val = df_24h['temperature'].mean()
                high_val = df_24h['temperature'].max()
                low_val = df_24h['temperature'].min()
            else:
                # Fallback to latest known if no 24h data
                avg_val = latest_readings['temperature'].mean()
                high_val = latest_readings['temperature'].max()
                low_val = latest_readings['temperature'].min()

            # RENDER MAIN METRIC
            st.metric("24h Average", f"{avg_val:.1f}{unit_label}")
            
            # RENDER HIGH/LOW
            sub_c1, sub_c2 = st.columns(2)
            sub_c1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub_c2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")

            st.divider()

            # 3. STALENESS / CURRENT TEMP LOGIC
            st.markdown("**Latest Readings**")
            for _, row in latest_readings.iterrows():
                # Calculate lag
                ts_check = row['timestamp'] if row['timestamp'].tzinfo else row['timestamp'].tz_localize('UTC')
                lag_hrs = (now_utc - ts_check).total_seconds() / 3600
                
                # Format Display
                pos = f"{row['Depth']}ft" if pd.notnull(row['Depth']) else f"Bank {row['Bank']}"
                
                if lag_hrs > 1.5:
                    # Stale Data Tag
                    st.write(f"⚠️ {row['Location']} ({pos}): **{row['temperature']:.1f}{unit_label}**")
                    st.caption(f"*(Data is {int(lag_hrs)}h old)*")
                else:
                    # Current Data
                    st.write(f"🟢 {row['Location']} ({pos}): **{row['temperature']:.1f}{unit_label}**")

def render_client_portal():
    client = get_bq_client()
    
    # 1. REGISTRY LOOKUP (Find all phases)
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    unit_label = "°F" # Hardcoded for client portal as requested

    # 2. AGGREGATED DATA FETCH (Phased Blackjack Support)
    with st.spinner("Synchronizing official records..."):
        all_data = []
        for p_id in proj_registry['Project']:
            phase_df = get_universal_portal_data(p_id, view_mode="client")
            if not phase_df.empty:
                all_data.append(phase_df)
        
        if not all_data:
            st.warning("⚠️ No approved data available yet.")
            return
        full_p_df = pd.concat(all_data)

    # 3. TABS
    tabs = st.tabs(["🏠 Summary", "📈 Time vs Temp", "📏 Temp vs Depth", "📋 Sensor Status", "🗺️ As Built"])
    
    with tabs[0]:
        render_summary_tab(full_p_df, unit_label, "US/Pacific") # Pass your display_tz here

    with tab_time:
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        for phase in sorted(full_p_df['Project'].unique()):
            st.markdown(f"### {phase}")
            # Insert your build_high_speed_graph call here using phase filtered data

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        # Logic for Depth Snapshot rendering

    with tab_status:
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        st.dataframe(latest[['Project', 'Location', 'Depth', 'Bank', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)

    with tab_built:
        if pd.notnull(asbuilt_filename):
            st.image(f"assets/asbuilts/{asbuilt_filename}", caption=f"As Built: {display_name}")
        else:
            st.info("The as-built site plan is currently being processed.")

# EXECUTE PORTAL
render_client_portal()
