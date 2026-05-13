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

def render_client_portal():
    """Main portal logic for Job Number aggregation and tab routing."""
    client = get_bq_client()
    if client is None: return

    # Registry Lookup: Finds all phases for the Job # (e.g., Blackjack Ph 1 & 2)
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    local_tz = primary_meta.get('Timezone', 'US/Pacific')
    asbuilt_filename = primary_meta.get('AsBuiltFile')

    with st.spinner("Synchronizing official records..."):
        all_phases = [get_universal_portal_data(p_id) for p_id in proj_registry['Project']]
        full_p_df = pd.concat(all_phases) if all_phases else pd.DataFrame()

    if full_p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    # Data Approval Notification
    last_approved_local = ensure_tz_convert(full_p_df['timestamp'], local_tz).max()
    st.info(f"✅ **Official Data Status:** Records are approved through **{last_approved_local.strftime('%B %d, %Y at %I:%M %p')}**.")

    st.header(f"📊 {primary_meta.get('ProjectName', TARGET_JOB_NUMBER)}")
    tabs = st.tabs(["🏠 Summary", "📈 Time vs Temp", "📏 Temp vs Depth", "📋 Sensor Status", "🗺️ As Built"])
    
    with tabs[0]:
        render_summary_tab(full_p_df, "°F", local_tz)

    with tabs[1]:
        st.write("### Timeline Analysis") # Insert time_vs_temp function here

    with tabs[2]:
        st.write("### Depth Profile") # Insert depth_profile function here

    with tabs[3]:
        st.subheader("📋 Verified Data Summary")
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['timestamp'] = ensure_tz_convert(latest['timestamp'], local_tz)
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)

    with tabs[4]:
        if pd.notnull(asbuilt_filename):
            img_path = f"assets/asbuilts/{asbuilt_filename}"
            if os.path.exists(img_path):
                st.image(img_path)
            else:
                st.warning(f"As-built file '{asbuilt_filename}' missing from server.")

# --- EXECUTION ---
render_client_portal()
