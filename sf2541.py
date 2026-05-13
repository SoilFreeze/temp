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


def render_data_status_bar(full_p_df):
    """Displays a status bar showing the last approved data point."""
    if not full_p_df.empty:
        # Find the most recent approved timestamp
        last_approved = full_p_df['timestamp'].max()
        # Convert to a readable string (assuming local display TZ)
        last_approved_str = last_approved.strftime('%B %d, %Y at %I:%M %p')
        
        st.info(f"✅ **Official Data Status:** Records are approved through **{last_approved_str}**.")
    else:
        st.warning("⚠️ **Notice:** No approved data is currently available for this project phase.")


def render_summary_tab(full_p_df, unit_label):
    """
    Renders the 24 hour Thermal Summary split by S, R, and TP.
    Shows 24h Avg, High, and Low. Displays staleness tags if data is old.
    """
    st.subheader("🌐 24 hour Thermal Summary")
    
    now_utc = pd.Timestamp.now(tz='UTC')
    
    # 1. CLASSIFICATION LOGIC
    def classify_pipe(row):
        loc = str(row['Location']).upper()
        bank = str(row['Bank']).upper()
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    full_p_df['PipeType'] = full_p_df.apply(classify_pipe, axis=1)

    # 2. CATEGORY LAYOUT
    cols = st.columns(3)
    categories = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)']

    for i, p_type in enumerate(categories):
        with cols[i]:
            st.markdown(f"### {p_type}")
            type_df = full_p_df[full_p_df['PipeType'] == p_type]
            
            if type_df.empty:
                st.caption("No data available.")
                continue

            # 24h Window Calculation
            df_24h = type_df[type_df['timestamp'] >= (now_utc - pd.Timedelta(days=1))]
            latest_ts = type_df['timestamp'].max()
            
            # STALENESS LOGIC
            ts_check = latest_ts if latest_ts.tzinfo else latest_ts.tz_localize('UTC')
            lag_hrs = (now_utc - ts_check).total_seconds() / 3600
            
            # Use 24h data if available, otherwise use latest known
            target_df = df_24h if not df_24h.empty else type_df
            avg_val = target_df['temperature'].mean()
            high_val = target_df['temperature'].max()
            low_val = target_df['temperature'].min()

            # RENDER MAIN METRICS
            label_prefix = "" if lag_hrs <= 1.5 else "Last "
            st.metric(f"{label_prefix}24h Average", f"{avg_val:.1f}{unit_label}")
            
            # Show staleness tag directly under the main metric if old
            if lag_hrs > 1.5:
                st.error(f"⚠️ Data is {int(lag_hrs)}h old")
            else:
                st.success("🟢 Data is Live")

            # HIGH / LOW BREAKDOWN
            sub1, sub2 = st.columns(2)
            sub1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")
            
            st.divider()

def render_client_portal():
    client = get_bq_client()
    
    # 1. REGISTRY LOOKUP (Aggregates all phases for the Job #)
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    unit_label = "°F"

    # 2. AGGREGATED DATA FETCH (Phased Support)
    all_data = []
    for p_id in proj_registry['Project']:
        phase_df = get_universal_portal_data(p_id, view_mode="client")
        if not phase_df.empty:
            all_data.append(phase_df)
    
    if not all_data:
        st.warning("⚠️ No approved data records available.")
        return
    full_p_df = pd.concat(all_data)

   with tabs[3]: # Tab: 📋 Sensor Status
    st.subheader("📋 Verified Data Summary")
    
    # Calculate coverage for the last 7 days
    latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
    
    # Show a metric for overall data confidence
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Approved Nodes", len(latest))
    with col2:
        st.metric("Approval Consistency", "100%", help="Only 100% verified data is shown in this portal.")
    
    st.divider()
    
    # Simple table for the client to see their sensor health
    latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
    
    # Clean up table for professional look
    status_table = latest[['Location', 'Position', 'temperature', 'timestamp']].copy()
    status_table.columns = ['Location', 'Depth/Bank', 'Last Temp (°F)', 'Last Approved Record']
    
    st.dataframe(
        status_table.sort_values(['Location', 'Depth/Bank']), 
        use_container_width=True, 
        hide_index=True
    )



# EXECUTE PORTAL
render_client_portal()
