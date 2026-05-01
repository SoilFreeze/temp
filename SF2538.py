import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re


#################################################################
# 1. CENTRAL PROJECT CONFIGURATION                              #
#################################################################
# CHANGE THIS TO SWITCH PROJECTS
CURRENT_PROJECT_KEY = "2538-Ferndale" 

PROJECT_REGISTRY = {
    "2538-Ferndale": {
        "name": "Pump 16 Upgrade",
        "location": "Ferndale, WA",
        "start_date": "2024-04-22 00:00:00",
        "timezone": "America/Los_Angeles",
        "upload_note": "Data is synced once per business day.",
        "unit": "°F"
    },
    "2527": {
        "name": "SJI Erie St Remediation",
        "location": "Elizabeth, NJ",
        "start_date": "2026-04-24 00:00:00",
        "timezone": "America/New_York",
        "upload_note": "Data will be uploaded once per business day by 4pm Pacific Time.",
        "unit": "°F"
    }
}

# Extract variables for the active project
active = PROJECT_REGISTRY[CURRENT_PROJECT_KEY]
TARGET_PROJECT = CURRENT_PROJECT_KEY
PROJECT_NAME = active["name"]
PROJECT_LOCATION = active["location"]
PROJECT_START_DATE = active["start_date"]
DISPLAY_TZ = active["timezone"]
UPLOAD_NOTE = active["upload_note"]
UNIT_LABEL = active["unit"]

# Database Globals
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"

# Updated to Snapshot as requested
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata_snapshot" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

st.set_page_config(page_title=f"Portal | {PROJECT_NAME}", layout="wide")

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
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
def get_universal_portal_data(project_id, start_date_str):
    if client is None: return pd.DataFrame()
    
    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth, m.Project
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush`
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord`
        ) AS r
        INNER JOIN `{METADATA_TABLE}` AS m 
            ON UPPER(TRIM(r.NodeNum)) = UPPER(TRIM(m.NodeNum))
            -- Logic to handle sensor lifecycle
            AND r.timestamp >= COALESCE(SAFE_CAST(m.Start_Date AS TIMESTAMP), '2000-01-01')
            AND r.timestamp <= COALESCE(SAFE_CAST(m.End_Date AS TIMESTAMP), '2099-12-31')
        WHERE CAST(m.Project AS STRING) = '{project_id}'
        AND r.timestamp >= '{start_date_str}'
        ORDER BY r.timestamp ASC
    """
    try:
        # Check if the table actually has the lifecycle columns
        table_ref = client.get_table(METADATA_TABLE)
        column_names = [field.name for field in table_ref.schema]
        
        # If snapshot doesn't have dates yet, remove that part of query
        if "Start_Date" not in column_names:
            query = query.replace("AND r.timestamp >= COALESCE", "-- AND r.timestamp")
            
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"BQ Error: {e}")
        return pd.DataFrame()

########################
# 3. GRAPHING ENGINE   #
########################

def build_high_speed_graph(df, title, start_view, end_view, display_tz):
    if df.empty: return go.Figure().update_layout(title="No data available.")

    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    # Sorting and Labeling Logic
    def get_sort_info(r):
        b, d = str(r['Bank']).strip(), str(r['Depth']).strip()
        if b and b.lower() not in ['nan', 'none']: return f"Bank {b}", 0.0
        if d and d.lower() not in ['nan', 'none']:
            try:
                num = float(re.findall(r"[-+]?\d*\.\d+|\d+", d)[0])
                return f"{d}ft", num
            except: return f"{d}ft", 999.0
        return f"Node {r['NodeNum']}", 1000.0

    pdf[['depth_label', 'sort_val']] = pdf.apply(lambda x: pd.Series(get_sort_info(x)), axis=1)
    fig = go.Figure()
    
    unique_depths = pdf[['depth_label', 'sort_val']].drop_duplicates().sort_values('sort_val')
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for i, (_, d_row) in enumerate(unique_depths.iterrows()):
        d_lbl = d_row['depth_label']
        depth_data = pdf[pdf['depth_label'] == d_lbl]
        color = colors[i % len(colors)]
        sensors_at_depth = depth_data['NodeNum'].unique()
        
        for j, sn in enumerate(sensors_at_depth):
            s_df = depth_data[depth_data['NodeNum'] == sn].sort_values('timestamp')
            
            # 6h Gap Detection
            s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
            gap_mask = s_df['gap_hrs'] > 6.0
            if gap_mask.any():
                gaps = s_df[gap_mask].copy()
                gaps['temperature'] = None
                gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
                s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

            fig.add_trace(go.Scatter(
                x=s_df['timestamp'], y=s_df['temperature'], 
                name=f"{d_lbl} ({sn})", legendgroup=d_lbl,
                showlegend=True if j == len(sensors_at_depth)-1 else False,
                mode='lines+markers', connectgaps=False, 
                line=dict(color=color, width=1.5),
                marker=dict(size=4, opacity=0.8),
                hovertemplate=f"<b>{d_lbl} ({sn})</b>: %{{y:.1f}}°F<extra></extra>"
            ))

    fig.add_hline(y=32, line_dash="dash", line_color="RoyalBlue", line_width=2, annotation_text="32°F FREEZING")

    # --- GRID HIERARCHY WITH DASHED MINORS ---
    fig.update_layout(
        title=f"<b>{title}</b>", hovermode="x unified", plot_bgcolor='white',
        xaxis=dict(
            range=[start_view, end_view], showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1, 
            minor=dict(
                dtick=6*60*60*1000, 
                showgrid=True, 
                gridcolor='Gainsboro', 
                griddash='dash'  # <--- THIS DASHES THE 6-HOUR LINES
            ),
            tickformat='%b %d\n%H:%M'
        ),
        yaxis=dict(
            title="Temperature (°F)", range=[-20, 80], showline=True, mirror=True, linecolor='black',
            dtick=10, gridcolor='DarkGray',
            minor=dict(dtick=5, showgrid=True, gridcolor='whitesmoke')
        ),
        height=600, margin=dict(r=150, t=50, b=50),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1)
    )

    mondays = pd.date_range(start=start_view.tz_convert(display_tz).floor('D'), 
                             end=end_view.tz_convert(display_tz).ceil('D'), 
                             freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2.5, line_color="dimgray", layer="below")

    return fig

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {PROJECT_NAME}")
st.caption(f"Project {TARGET_PROJECT} Status")
st.caption(f"Location: {PROJECT_LOCATION} | Timezone: {DISPLAY_TZ}")
st.markdown(f"**{UPLOAD_NOTE}**")

data = get_universal_portal_data(TARGET_PROJECT, PROJECT_START_DATE)

if not data.empty:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        project_start_ts = pd.Timestamp(PROJECT_START_DATE, tz='UTC')
        weeks_view = st.slider("Weeks to View", 1, 12, 4, key="weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = max(project_start_ts, end_view - timedelta(weeks=weeks_view))
        
        for loc in sorted(data['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                fig = build_high_speed_graph(data[data['Location'] == loc], f"{loc} Timeline", start_view, end_view, DISPLAY_TZ)
                st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")

    # (Depth and Table tabs use same logic, referencing DISPLAY_TZ and TARGET_PROJECT)
    # ... [Rest of tab logic remains same, substituting hardcoded values for variables]
else:
    st.info(f"Awaiting data for {PROJECT_NAME} (Started {PROJECT_START_DATE})...")
