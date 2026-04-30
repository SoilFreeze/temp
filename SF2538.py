import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

#################################################################
# 1. CONFIGURATION: Project 2538 Details                        #
#################################################################
TARGET_PROJECT = "2538-Ferndale"             # Matches the 'Project' column
CLIENT_NAME = "Pump 16 Upgrade"     
LOCATION_STAMP = "Ferndale, WA"     
DISPLAY_TZ = "America/Los_Angeles"  
UNIT_LABEL = "°F"                   

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
# Use the single table revealed in your screenshot
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"

st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = st.secrets.get("scopes", ["https://www.googleapis.com/auth/bigquery"])
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info.get("project_id", PROJECT_ID))
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_portal_data():
    if client is None:
        return pd.DataFrame()

    # We manually join raw data to the SNAPSHOT to avoid Drive 403 errors
    # We use LIKE and TRIM to handle the "2538-Ferndale" vs "2538" mismatch
    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.metadata_snapshot` AS m 
            ON r.NodeNum = m.NodeNum
        INNER JOIN `{PROJECT_ID}.{DATASET_ID}.manual_rejections` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        # This catches both "2538" and "2538-Ferndale" for all locations
        WHERE (m.Project = '2538' OR m.Project LIKE '2538%')
            AND rej.approve = 'TRUE'
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        ORDER BY r.timestamp ASC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Database Query Error: {e}")
        return pd.DataFrame()

########################
# 3. GRAPHING ENGINE   #
########################

def build_custom_graph(df, title, lookback_weeks):
    if df.empty:
        return go.Figure().update_layout(title="No approved data found.")

    plot_df = df.copy()
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(DISPLAY_TZ) 
    
    now_local = pd.Timestamp.now(tz=DISPLAY_TZ)
    start_view = now_local - timedelta(weeks=lookback_weeks)

    fig = go.Figure()
    for loc in sorted(plot_df['Location'].unique()):
        loc_data = plot_df[plot_df['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=loc_data['timestamp'], y=loc_data['temperature'], 
            name=loc, mode='lines', connectgaps=False
        ))

    # Grid Hierarchy: Black Mondays, Dotted Gray Midnights
    grid_days = pd.date_range(start=start_view.floor('D'), end=now_local.ceil('D'), freq='D', tz=DISPLAY_TZ)
    for ts in grid_days:
        color, width, dash = ("rgba(0,0,0,1)", 1.2, "solid") if ts.weekday() == 0 else ("rgba(128,128,128,0.4)", 0.8, "dot")
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.add_vline(x=now_local, line_width=2, line_color="Red", line_dash="dash")
    fig.add_hline(y=32.0, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F Freezing")

    fig.update_layout(
        title=f"<b>{title}</b>", plot_bgcolor='white', hovermode="x unified",
        xaxis=dict(range=[start_view, now_local], showline=True, linecolor='black', mirror=True, tickformat='%b %d'),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=[-20, 80]),
        height=550, margin=dict(r=150)
    )
    return fig

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {CLIENT_NAME}")

try:
    now_ts = pd.Timestamp.now(tz=DISPLAY_TZ).strftime('%m/%d %H:%M')
except:
    now_ts = pd.Timestamp.now(tz='UTC').strftime('%m/%d %H:%M UTC')

st.caption(f"Project ID: {TARGET_PROJECT} | Current Time: {now_ts}")

with st.sidebar:
    st.header("Settings")
    weeks = st.slider("Lookback (Weeks)", 1, 12, 4)
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

df = get_portal_data()

if df.empty:
    st.warning(f"No approved data found for project {TARGET_PROJECT} in the master_data table.")
else:
    tab1, tab2, tab3 = st.tabs(["📈 Timeline", "📏 Profiles", "📋 Table"])
    
    with tab1:
        for loc in sorted(df['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                st.plotly_chart(build_custom_graph(df[df['Location'] == loc], loc, weeks), use_container_width=True)

    with tab2:
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_only = df.dropna(subset=['Depth_Num']).copy()
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} - Snapshots"):
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                for m_date in mondays:
                    target = m_date.replace(hour=6, minute=0)
                    window = depth_only[(depth_only['Location'] == loc) & (depth_only['timestamp'].between(target-timedelta(hours=12), target+timedelta(hours=12)))]
                    if not window.empty:
                        snap = (window.assign(d=(window['timestamp']-target).abs()).sort_values(['NodeNum','d']).drop_duplicates('NodeNum').sort_values('Depth_Num'))
                        fig_d.add_trace(go.Scatter(x=snap['temperature'], y=snap['Depth_Num'], name=m_date.strftime('%m/%d'), mode='lines+markers'))
                fig_d.update_layout(yaxis=dict(autorange="reversed", title="Depth (ft)"), xaxis=dict(title=UNIT_LABEL), height=600, plot_bgcolor='white')
                st.plotly_chart(fig_d, use_container_width=True)

    with tab3:
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Last Sync'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%m/%d %H:%M')
        st.dataframe(latest[['Location', 'Depth', 'temperature', 'Last Sync']].sort_values(['Location', 'Depth']), use_container_width=True, hide_index=True)
