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
    "2527": "2026-04-24 00:00:00"
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
        b, d = str(r['Bank']).strip(), str(r['Depth']).strip()
        if b and b.lower() not in ['nan', 'none']: return f"Bank {b} ({r['NodeNum']})"
        if d and d.lower() not in ['nan', 'none']: return f"{d}ft ({r['NodeNum']})"
        return f"Node {r['NodeNum']}"

    pdf['label'] = pdf.apply(create_label, axis=1)
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # GAP DETECTION: 6.0 hours
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # connectgaps=False ensures the 6hr breaks are visible
        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines+markers', 
            connectgaps=False, 
            marker=dict(size=4, opacity=0.8),
            line=dict(width=1.5)
        ))

    fig.update_layout(
        title=f"<b>{title}</b>", hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, mirror=True, tickformat='%b %d'),
        yaxis=dict(title="°F", gridcolor='Gainsboro', showline=True, mirror=True, range=[-20, 80]),
        height=600, margin=dict(r=150, t=50, b=50),
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
    # RESTORED: THE THREE TABS
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = end_view - timedelta(weeks=weeks_view)
        
        locs = sorted(data['Location'].unique())
        for loc in locs:
            with st.expander(f"📍 {loc}", expanded=True):
                loc_df = data[data['Location'] == loc]
                fig = build_high_speed_graph(loc_df, f"{loc} Timeline", start_view, end_view, "America/New_York")
                st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        data['Depth_Num'] = pd.to_numeric(data['Depth'], errors='coerce')
        depth_only = data.dropna(subset=['Depth_Num']).copy()
        
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Vertical Profile"):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=4, freq='W-MON')
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0, second=0)
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                      (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                    if not window.empty:
                        snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))
                
                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(plot_bgcolor='white', height=600, yaxis=dict(range=[y_limit, 0], title="Depth (ft)"), xaxis=dict(range=[-20, 80], title="°F"))
                st.plotly_chart(fig_d, width='stretch', key=f"depth_{loc}")

    with tab_table:
        st.subheader("📋 Latest Sensor Readings")
        latest = data.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}°F")
        st.dataframe(latest[['Location', 'NodeNum', 'Current Temp']].sort_values('Location'), width='stretch', hide_index=True)
else:
    st.info("Loading project streams...")
