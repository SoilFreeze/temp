import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

#################################################################
# 1. CONFIGURATION & CLIENT INITIALIZATION                      #
#################################################################
TARGET_PROJECT = "2527"
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
# Ensure this matches your master table exactly
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

@st.cache_resource
def get_bq_client():
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
        st.error(f"Authentication Failed: {e}")
        return None

# CRITICAL: Initialize client globally so functions can see it
client = get_bq_client()

PROJECT_VISIBILITY_MASKS = {
    "2527": "2026-01-01 00:00:00"
}

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    """
    ULTRA-ROBUST FILTER:
    - Handles case-sensitivity (True vs TRUE)
    - Trims spaces from Node IDs
    - Allows 'Pending' (NULL) data so 114 nodes stay visible
    """
    if client is None: return pd.DataFrame()

    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
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
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON UPPER(TRIM(r.NodeNum)) = UPPER(TRIM(rej.NodeNum)) 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE CAST(m.Project AS STRING) = '{project_id}'
        AND r.timestamp >= '{cutoff}'
        AND (UPPER(CAST(rej.approve AS STRING)) != 'FALSE' OR rej.approve IS NULL)
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY r.timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        # Clean metadata strings to prevent labeling errors
        df['Depth'] = df['Depth'].astype(str).replace(['nan', 'None', '<NA>'], '')
        df['Bank'] = df['Bank'].astype(str).replace(['nan', 'None', '<NA>'], '')
        return df
    except Exception as e:
        st.error(f"BQ Query Error: {e}")
        return pd.DataFrame()

########################
# 3. GRAPHING ENGINE   #
########################

def build_high_speed_graph(df, title, start_view, end_view, display_tz):
    if df.empty: return go.Figure().update_layout(title="No data available.")

    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    # Labeling priority: Bank -> Depth -> NodeID
    def create_label(r):
        b, d = str(r['Bank']).strip(), str(r['Depth']).strip()
        if b: return f"Bank {b} ({r['NodeNum']})"
        if d: return f"{d}ft ({r['NodeNum']})"
        return f"Node {r['NodeNum']}"

    pdf['label'] = pdf.apply(create_label, axis=1)
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # Gap Detection (Relaxed to 48h for Project 2527)
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 48.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # Mode lines+markers + connectgaps ensures sparse Elizabeth data is visible
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines+markers', 
            connectgaps=True, marker=dict(size=5)
        ))

    fig.update_layout(
        title=f"<b>{title}</b>", hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, mirror=True, tickformat='%b %d'),
        yaxis=dict(title="°F", gridcolor='Gainsboro', showline=True, mirror=True, range=[-20, 80]),
        height=600, margin=dict(r=150),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1)
    )
    return fig

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 Project {TARGET_PROJECT} Status")
st.caption(f"Location: Elizabeth, NJ | Timezone: America/New_York")

data = get_universal_portal_data(TARGET_PROJECT)

if not data.empty:
    locs = sorted(data['Location'].unique())
    for loc in locs:
        with st.expander(f"📍 {loc}", expanded=True):
            loc_df = data[data['Location'] == loc]
            fig = build_high_speed_graph(
                loc_df, f"{loc} Data", 
                pd.Timestamp.now(tz='UTC') - timedelta(weeks=4), 
                pd.Timestamp.now(tz='UTC'), "America/New_York"
            )
            # Updated to width='stretch' for 2026 standards
            st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")
else:
    st.info("Syncing Project 2527 streams... (Check BQ if this persists)")
