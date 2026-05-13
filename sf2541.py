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

def render_client_portal():
    client = get_bq_client()
    
    # Robust Lookup: Finds all phases/projects associated with that Job Number
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', TARGET_JOB_NUMBER)
    asbuilt_filename = primary_meta.get('AsBuiltFile')

    # Aggregated Data Fetching
    with st.spinner("Synchronizing official records..."):
        full_p_df = pd.concat([get_universal_portal_data(p_id) for p_id in proj_registry['Project']])

    if full_p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    # Tabs for the Dashboard
    tabs = st.tabs(["🏠 Summary", "📈 Time vs Temp", "📏 Temp vs Depth", "📋 Sensor Status", "🗺️ As Built"])
    tab_sum, tab_time, tab_depth, tab_status, tab_built = tabs

    with tab_sum:
        st.subheader("🌐 Global Project Health")
        c1, c2, c3 = st.columns(3)
        last_24h = full_p_df[full_p_df['timestamp'] >= (pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=1))]
        
        c1.metric("Project Avg (24h)", f"{last_24h['temperature'].mean():.1f}°F")
        c2.metric("Coldest Probe", f"{last_24h['temperature'].min():.1f}°F")
        c3.metric("Sensors Online", full_p_df['NodeNum'].nunique())
        
        st.divider()
        st.write("### Phase Comparison")
        st.dataframe(full_p_df.groupby('Project')['temperature'].agg(['mean', 'min', 'max']), use_container_width=True)

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
