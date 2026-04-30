import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

#################################################################
# 1. CONFIGURATION: Change these for each project deployment    #
#################################################################
TARGET_PROJECT = "2538"             # Must match Project ID in BigQuery
CLIENT_NAME = "Pump 16 Upgrade"     # Large Header
LOCATION_STAMP = "Ferndale, WA"     # Sub-header
DISPLAY_TZ = "US/Pacific"           # Set your custom timezone here
UNIT_LABEL = "°F"                   # Measurement units

# Database Constants
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
# Updated to use your snapshot table to avoid Drive 403 errors
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata_snapshot"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

@st.cache_resource
def get_bq_client():
    """Returns a BigQuery client using default credentials."""
    return bigquery.Client(project=PROJECT_ID)

client = get_bq_client()

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_portal_data():
    """
    Queries only approved data for the specific project.
    Logic: Must be 'TRUE' approved and NOT 'MASKED'.
    """
    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{METADATA_TABLE}` AS m ON r.NodeNum = m.NodeNum
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE m.Project = '{TARGET_PROJECT}'
        AND rej.approve = 'TRUE' 
        AND NOT EXISTS (
            SELECT 1 FROM `{OVERRIDE_TABLE}` m2 
            WHERE m2.NodeNum = r.NodeNum 
            AND m2.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
            AND m2.approve = 'MASKED'
        )
        ORDER BY r.timestamp ASC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame()

########################
# 3. GRAPHING ENGINE   #
########################

def build_custom_graph(df, title, lookback_weeks):
    if df.empty:
        return go.Figure().update_layout(title="No data currently approved.")

    plot_df = df.copy()
    # Apply the configured custom timezone
    plot_df['timestamp'] = plot_df['timestamp'].dt.tz_convert(DISPLAY_TZ) 
    
    now_local = pd.Timestamp.now(tz=DISPLAY_TZ)
    start_view = now_local - timedelta(weeks=lookback_weeks)

    fig = go.Figure()

    for loc in sorted(plot_df['Location'].unique()):
        loc_data = plot_df[plot_df['Location'] == loc]
        fig.add_trace(go.Scattergl(
            x=loc_data['timestamp'], 
            y=loc_data['temperature'], 
            name=loc, 
            mode='lines', 
            connectgaps=False
        ))

    # Grid Hierarchy: Solid Black Mondays, Dotted Gray Midnights
    grid_days = pd.date_range(start=start_view.floor('D'), end=now_local.ceil('D'), freq='D', tz=DISPLAY_TZ)
    for ts in grid_days:
        if ts.weekday() == 0:  # Monday
            color, width, dash = "rgba(0,0,0,1)", 1.5, "solid"
        else:  # Daily Midnight
            color, width, dash = "rgba(128,128,128,0.5)", 1.0, "dot"
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    # Add "Now" line and 32°F Reference
    fig.add_vline(x=now_local, line_width=2, line_color="Red", line_dash="dash", layer='above')
    fig.add_hline(y=32, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F Freezing")

    fig.update_layout(
        title=f"<b>{title}</b>",
        plot_bgcolor='white',
        hovermode="x unified",
        xaxis=dict(range=[start_view, now_local], showline=True, linecolor='black', mirror=True, tickformat='%b %d'),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=[-20, 80]),
        height=550,
        margin=dict(r=150)
    )
    return fig

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {CLIENT_NAME}")
st.caption(f"{LOCATION_STAMP} | Timezone: {DISPLAY_TZ} | Current: {pd.Timestamp.now(tz=DISPLAY_TZ).strftime('%m/%d/%Y %H:%M')}")

with st.sidebar:
    st.header("Controls")
    weeks = st.slider("Historical Window (Weeks)", 1, 12, 6)
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Execute Data Fetch
df = get_portal_data()

if df.empty:
    st.warning(f"No approved data found for project {TARGET_PROJECT}.")
else:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline", "📏 Profiles", "📋 Table"])
    
    with tab_time:
        for loc in sorted(df['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                st.plotly_chart(build_custom_graph(df[df['Location'] == loc], loc, weeks), use_container_width=True)

    with tab_depth:
        # Depth profile logic
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_only = df.dropna(subset=['Depth_Num']).copy()
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} - Weekly Snapshots"):
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0)
                    window = depth_only[(depth_only['Location'] == loc) & (depth_only['timestamp'].between(target_ts-timedelta(hours=12), target_ts+timedelta(hours=12)))]
                    if not window.empty:
                        snap_df = (window.assign(d=(window['timestamp']-target_ts).abs()).sort_values(['NodeNum','d']).drop_duplicates('NodeNum').sort_values('Depth_Num'))
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], name=m_date.strftime('%m/%d'), mode='lines+markers'))
                fig_d.update_layout(yaxis=dict(autorange="reversed", title="Depth (ft)"), xaxis=dict(title=UNIT_LABEL), height=600)
                st.plotly_chart(fig_d, use_container_width=True)

    with tab_table:
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Last Sync'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%m/%d %H:%M')
        st.dataframe(latest[['Location', 'Depth', 'temperature', 'Last Sync']].sort_values(['Location', 'Depth']), use_container_width=True, hide_index=True)
