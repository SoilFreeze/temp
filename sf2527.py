import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import re

#################################################################
# 1. CONFIGURATION & CLIENT INITIALIZATION                      #
#################################################################
TARGET_PROJECT = "2527"
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# SET PROJECT START DATE
PROJECT_START_DATE = "2026-04-24 00:00:00"

st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

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

PROJECT_VISIBILITY_MASKS = {TARGET_PROJECT: PROJECT_START_DATE}

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
    
    def get_sort_info(r):
        b, d = str(r['Bank']).strip(), str(r['Depth']).strip()
        if b and b.lower() not in ['nan', 'none']: return f"Bank {b} ({r['NodeNum']})", 0.0
        if d and d.lower() not in ['nan', 'none']:
            try:
                num = float(re.findall(r"[-+]?\d*\.\d+|\d+", d)[0])
                return f"{d}ft ({r['NodeNum']})", num
            except: return f"{d}ft ({r['NodeNum']})", 999.0
        return f"Node {r['NodeNum']}", 1000.0

    pdf[['label', 'sort_val']] = pdf.apply(lambda x: pd.Series(get_sort_info(x)), axis=1)
    
    fig = go.Figure()
    sorted_labels = pdf[['label', 'sort_val']].drop_duplicates().sort_values('sort_val')

    for _, row in sorted_labels.iterrows():
        lbl = row['label']
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # Gap Detection (6 hours)
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        fig.add_trace(go.Scatter(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines+markers', 
            connectgaps=False, 
            customdata=s_df[['Depth']],
            hovertemplate="<b>%{x|%b %d, %H:00}</b><br>Depth: %{customdata[0]}ft<br>Temp: %{y:.1f}°F<extra></extra>",
            marker=dict(size=4, opacity=0.8), line=dict(width=1.5)
        ))

    fig.add_hline(y=32, line_dash="dash", line_color="RoyalBlue", line_width=2, annotation_text="32°F FREEZING")

    fig.update_layout(
        title=f"<b>{title}</b>", hovermode="closest", plot_bgcolor='white',
        xaxis=dict(
            range=[start_view, end_view], showline=True, mirror=True, linecolor='black',
            showgrid=True, dtick="D1", gridcolor='DarkGray', gridwidth=1, 
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

    # Monday dark grid lines
    mondays = pd.date_range(start=start_view.tz_convert(display_tz).floor('D'), 
                             end=end_view.tz_convert(display_tz).ceil('D'), 
                             freq='W-MON', tz=display_tz)
    for mon in mondays:
        fig.add_vline(x=mon, line_width=2.5, line_color="dimgray", layer="below")

    return fig

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 SJI Erie St Remediation")
st.caption(f"Project {TARGET_PROJECT} Status")
st.caption(f"Location: Elizabeth, NJ | Timezone: America/New_York")
st.markdown("**Data will be uploaded once per business day by 4pm Pacific Time.**")

data = get_universal_portal_data(TARGET_PROJECT)

if not data.empty:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        # Adjusted Start View to your project date: 4/24
        project_start = pd.Timestamp(PROJECT_START_DATE, tz='UTC')
        end_view = pd.Timestamp.now(tz='UTC')
        
        # Slider still works, but we default it to show from project start
        weeks_view = st.slider("Weeks to View", 1, 12, 1, key="weeks_slider")
        start_view = max(project_start, end_view - timedelta(weeks=weeks_view))
        
        for loc in sorted(data['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                fig = build_high_speed_graph(data[data['Location'] == loc], f"{loc} Timeline", start_view, end_view, "America/New_York")
                st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")

    with tab_depth:
        data['Depth_Num'] = pd.to_numeric(data['Depth'], errors='coerce')
        depth_only = data.dropna(subset=['Depth_Num']).copy()
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Vertical Profile"):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                mondays = pd.date_range(start=project_start, end=pd.Timestamp.now(tz='UTC'), freq='W-MON')
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0, second=0)
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                    if not window.empty:
                        snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y'),
                                                 customdata=snap_df[['timestamp']],
                                                 hovertemplate="<b>%{customdata[0]|%b %d, %H:00}</b><br>Depth: %{y}ft<br>Temp: %{x:.1f}°F<extra></extra>"))
                fig_d.add_vline(x=32, line_dash="dash", line_color="RoyalBlue")
                fig_d.update_layout(plot_bgcolor='white', height=600, yaxis=dict(range=[int(((loc_data['Depth_Num'].max()//10)+1)*10), 0], title="Depth (ft)"), xaxis=dict(range=[-20, 80], title="°F"), hovermode="closest")
                st.plotly_chart(fig_d, width='stretch', key=f"depth_{loc}")

    with tab_table:
        latest = data.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Depth_Sort'] = pd.to_numeric(latest['Depth'], errors='coerce').fillna(0)
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}°F")
        display_df = latest.sort_values(['Location', 'Depth_Sort'])[['Location', 'NodeNum', 'Current Temp']]
        st.dataframe(display_df, width='stretch', hide_index=True)
else:
    st.info("Awaiting initial data streams for project start (4/24)...")
