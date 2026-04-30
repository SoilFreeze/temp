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

client = get_bq_client()

PROJECT_VISIBILITY_MASKS = {
    "2527": "2026-01-01 00:00:00"
}

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
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
    
    def create_label(r):
        b, d = str(r.get('Bank', '')).strip(), str(r.get('Depth', '')).strip()
        if b and b.lower() not in ['nan', 'none']: return f"Bank {b} ({r['NodeNum']})"
        if d and d.lower() not in ['nan', 'none']: return f"{d}ft ({r['NodeNum']})"
        return f"Node {r['NodeNum']}"

    pdf['label'] = pdf.apply(create_label, axis=1)
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # 1. GAP DETECTION: (6.0 hours)
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        if gap_mask.any():
            # Insert None values to physically break the line
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # 2. FIX: Set connectgaps=False to respect the gaps inserted above
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], 
            y=s_df['temperature'], 
            name=lbl, 
            mode='lines+markers', 
            connectgaps=False,  # <--- CRITICAL CHANGE
            marker=dict(size=4, opacity=0.8),
            line=dict(width=1.5)
        ))

    fig.update_layout(
        title=f"<b>{title}</b>", 
        hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, mirror=True, tickformat='%b %d'),
        yaxis=dict(title="°F", gridcolor='Gainsboro', showline=True, mirror=True, range=[-20, 80]),
        height=600, 
        margin=dict(r=150, t=50, b=50),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1),
        autosize=True 
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
        with st.expander(f"📍 Location: {loc}", expanded=True):
            loc_df = data[data['Location'] == loc]
            now_utc = pd.Timestamp.now(tz='UTC')
            
            fig = build_high_speed_graph(
                loc_df, 
                f"{loc} Data Pipeline", 
                now_utc - timedelta(weeks=4), 
                now_utc, 
                "America/New_York"
            )
            st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")
else:
    st.info("Loading streams...")
