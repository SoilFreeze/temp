import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time
import re

######################
# 1. CONFIGURATION   #
######################

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"
# Note: Ensure project_registry is the source of truth for metadata
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.project_registry" 

@st.cache_resource
def get_bq_client():
    """Initializes BigQuery client with necessary scopes for federated tables."""
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = [
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/drive.readonly"
            ]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info.get("project_id", PROJECT_ID))
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Authentication Failed: {e}")
        return None

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """Robust data fetcher with strict masking and approval logic."""
    client = get_bq_client()
    if client is None: return pd.DataFrame()

    if view_mode == "client":
        # Only Approved (TRUE) AND NOT Masked
        query_filter = """
            AND rej.approve = 'TRUE'
            AND NOT EXISTS (
                SELECT 1 FROM `{OVERRIDE_TABLE}` m 
                WHERE m.NodeNum = r.NodeNum 
                AND m.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                AND m.approve = 'MASKED'
            )
        """
    else:
        # Engineering view: See all except rejected/masked
        query_filter = "AND (rej.approve IS NULL OR rej.approve NOT IN ('FALSE', 'MASKED'))"

    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth, m.Project
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{METADATA_TABLE}` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE m.Project = @project_id
        {query_filter}
        ORDER BY r.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"⚠️ BigQuery Query Error: {e}")
        return pd.DataFrame()
    
########################
#  PROJECT BY NUMBER   #
########################

def get_project_by_job_number(job_number):
    """Safely maps a numeric prefix to a full registry record."""
    client = get_bq_client()
    if not job_number or client is None: return None
    # Use parameterization to prevent SQL injection
    query = f"SELECT * FROM `{METADATA_TABLE}` WHERE Project LIKE @job_num LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_num", "STRING", f"{job_number}%")]
    )
    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return df.iloc[0].to_dict() if not df.empty else None
    except:
        return None

# UI INITIALIZATION
st.set_page_config(page_title="SoilFreeze Data Lab", page_icon="❄️", layout="wide")
st.sidebar.title("❄️ SoilFreeze Lab")

page = st.sidebar.selectbox("Navigation", ["Summary", "Time vs Temp", "Sensor Status", "Depth Charts", "Client Portal", "Admin Tools"])

st.sidebar.divider()
st.sidebar.subheader("🎯 Project Finder")
job_input = st.sidebar.text_input("Enter Job Number", placeholder="e.g. 2538")

# Session State for Selection
if job_input:
    match = get_project_by_job_number(job_input)
    if match:
        st.session_state['selected_project'] = match['Project']
        st.session_state['project_metadata'] = match
        st.sidebar.success(f"Connected: {match['Project']}")
    else:
        st.sidebar.error("Job number not recognized.")
        st.session_state['selected_project'] = None
else:
    # Use standard dropdown as fallback
    client = get_bq_client()
    if client:
        proj_list = client.query(f"SELECT DISTINCT Project FROM `{METADATA_TABLE}`").to_dataframe()
        selected = st.sidebar.selectbox("Or Select Manually", ["None"] + sorted(proj_list['Project'].tolist()))
        if selected != "None":
            st.session_state['selected_project'] = selected
            st.session_state['project_metadata'] = get_project_by_job_number(selected.split('-')[0])

# Settings
st.sidebar.divider()
unit_mode = st.sidebar.radio("Display Units", ["Fahrenheit", "Celsius"], horizontal=True)
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"
display_tz = st.sidebar.selectbox("Timezone", ["UTC", "US/Eastern", "US/Pacific"], index=2)

########################
# 3. GRAPHING ENGINE   #
########################

def build_robust_graph(df, title, start_view, end_view, unit_mode, unit_label, display_tz, f_start_date=None, job_num=None):
    if df.empty: return go.Figure().update_layout(title="No data available.")
    
    pdf = df.copy()
    # Unit conversion
    if unit_mode == "Celsius":
        pdf['temperature'] = (pdf['temperature'] - 32) * 5/9
    
    # Timezone conversion
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    fig = go.Figure()

    # Theoretical Goal Curve Lookup
    if job_num and f_start_date:
        client = get_bq_client()
        ref_q = f"SELECT Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE CurveID LIKE @jid ORDER BY Day"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("jid", "STRING", f"{job_num}%")]
        )
        ref_df = client.query(ref_q, job_config=job_config).to_dataframe()
        if not ref_df.empty:
            ref_df['timestamp'] = ref_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
            fig.add_trace(go.Scatter(x=ref_df['timestamp'], y=ref_df['Temp'], name="Theoretical Goal", line=dict(color='gray', dash='dash')))

    # Sensor Trace with Gap Detection
    for sn in sorted(pdf['NodeNum'].unique()):
        s_df = pdf[pdf['NodeNum'] == sn].sort_values('timestamp')
        # Gap detection logic from v1.0
        s_df['gap'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gaps = s_df[s_df['gap'] > 6.0].copy()
        if not gaps.empty:
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        fig.add_trace(go.Scattergl(x=s_df['timestamp'], y=s_df['temperature'], name=f"Node {sn}", mode='lines'))

    # Standardization
    fig.update_layout(
        title=f"<b>{title}</b>", plot_bgcolor='white', hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, linecolor='black', mirror=True),
        yaxis=dict(title=unit_label, gridcolor='Gainsboro', range=[-20, 80] if unit_mode=="Fahrenheit" else [-30, 30]),
        height=600
    )
    # 32 Degree line
    fig.add_hline(y=(32 if unit_mode=="Fahrenheit" else 0), line_dash="dash", line_color="RoyalBlue")
    return fig

####################
#  CLIENT PORTAL   #
####################

def render_client_portal():
    project_id = st.session_state.get('selected_project')
    meta = st.session_state.get('project_metadata')

    if not project_id:
        st.info("💡 Enter a Job Number in the sidebar to load the portal.")
        return

    job_num = project_id.split('-')[0]
    st.header(f"📊 Portal: {meta.get('ProjectName', project_id)}")
    
    f_date = meta.get('Date_Freezedown')
    if f_date: st.caption(f"❄️ Freezedown Start: {f_date}")

    with st.spinner("Fetching approved records..."):
        p_df = get_universal_portal_data(project_id, view_mode="client")

    if p_df.empty:
        st.warning("No approved data records found for this project.")
        return

    tab_time, tab_depth = st.tabs(["📈 Timeline", "📏 Depth"])

    with tab_time:
        weeks = st.slider("View Window (Weeks)", 1, 12, 4)
        end_view = pd.Timestamp.now(tz=display_tz)
        start_view = end_view - timedelta(weeks=weeks)
        
        for loc in sorted(p_df['Location'].unique()):
            with st.expander(f"📍 Location: {loc}", expanded=True):
                loc_data = p_df[p_df['Location'] == loc]
                fig = build_robust_graph(loc_data, f"{loc} History", start_view, end_view, unit_mode, unit_label, display_tz, f_date, job_num)
                st.plotly_chart(fig, use_container_width=True)

    with tab_depth:
        st.write("Vertical depth analysis would follow similar v1.0 logic here.")

# MAIN ROUTER
if page == "Client Portal":
    render_client_portal()
else:
    st.title(page)
    st.write(f"The {page} module is loading for {st.session_state.get('selected_project', 'No Project Selected')}")

