import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

#################################################################
# 1. CONFIGURATION: Project 2527-Elizabeth                      #
#################################################################
TARGET_PROJECT = "2527-Elizabeth, New Jersey"    
CLIENT_NAME = "SJI Erie St"     
LOCATION_STAMP = "Elizabeth, New Jerse"     
DISPLAY_TZ = "America/New_York"  
UNIT_LABEL = "°F"                   

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata_snapshot"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

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
def get_standalone_portal_data():
    """
    STRICT FILTER: Only pulls data where approve = 'TRUE'.
    MASKED data is inherently ignored by only selecting 'TRUE'.
    """
    if client is None: return pd.DataFrame()

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
        INNER JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp
        WHERE (TRIM(CAST(m.Project AS STRING)) = '{TARGET_PROJECT}' 
               OR m.Project LIKE '2527%')
        AND rej.approve = 'TRUE' -- STRICT TRUE FILTER
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

def build_high_speed_graph(df, title, start_view, end_view, display_tz):
    if df.empty: return go.Figure().update_layout(title="No data available.")

    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    # LEGEND LOGIC: Include Bank and Depth
    pdf['label'] = pdf.apply(
        lambda r: f"Bank {r['Bank']} ({r['NodeNum']})" if pd.notnull(r['Bank']) and str(r['Bank']).strip().lower() not in ["", "none", "nan", "null"]
        else f"{r.get('Depth', '??')}ft ({r.get('NodeNum')})", axis=1
    )
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # GAP DETECTION: Break lines if > 6 hours
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 6.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines', connectgaps=False
        ))

    # Grid Hierarchy
    grid_days = pd.date_range(start=start_view.tz_convert(display_tz).floor('D'), 
                             end=end_view.tz_convert(display_tz).ceil('D'), freq='D', tz=display_tz)
    for ts in grid_days:
        color, width, dash = ("rgba(0,0,0,1)", 1.2, "solid") if ts.weekday() == 0 else ("rgba(128,128,128,0.4)", 0.8, "dot")
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.add_vline(x=pd.Timestamp.now(tz=display_tz), line_width=2, line_color="Red", line_dash="dash")
    fig.add_hline(y=32, line_dash="dash", line_color="RoyalBlue", annotation_text="32°F Freezing")

    fig.update_layout(
        title=f"<b>{title}</b>", plot_bgcolor='white', hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, linecolor='black', mirror=True, tickformat='%b %d'),
        yaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=[-20, 80]),
        height=550, margin=dict(r=150),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1, xanchor="left")
    )
    return fig

###########################
# Client Portal #
#################
def render_client_portal(df):
    """
    Standardized Portal UI used in the office app.
    Uses global constants: TARGET_PROJECT, DISPLAY_TZ, UNIT_LABEL
    """
    if df.empty:
        st.warning(f"No approved data found for project {TARGET_PROJECT}.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="portal_weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = end_view - timedelta(weeks=weeks_view)
        
        locations = sorted(df['Location'].dropna().unique())
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = df[df['Location'] == loc].copy()
                fig = build_high_speed_graph(loc_data, f"{loc} Approved Data", start_view, end_view, DISPLAY_TZ)
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
        depth_only = df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Weekly Snapshots", expanded=False):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                mondays = pd.date_range(end=pd.Timestamp.now(tz='UTC'), periods=6, freq='W-MON')
                
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0, second=0)
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                      (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                    
                    if not window.empty:
                        snap_df = (window.assign(diff=(window['timestamp'] - target_ts).abs())
                                   .sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num'))
                        
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], 
                                                 mode='lines+markers', name=target_ts.strftime('%m/%d/%y'),
                                                 line=dict(shape='spline', smoothing=0.5)))

                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(plot_bgcolor='white', height=600,
                                    xaxis=dict(title=UNIT_LABEL, gridcolor='Gainsboro'),
                                    yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                                    legend=dict(orientation="h", y=-0.2))
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_graph_{loc}")

    with tab_table:
        latest = df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}{UNIT_LABEL}")
        latest['Last Sync'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%m/%d %H:%M')
        st.dataframe(latest[['Location', 'Depth', 'Current Temp', 'Last Sync']].sort_values(['Location', 'Depth']), 
                     use_container_width=True, hide_index=True)

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {CLIENT_NAME}")
st.caption(f"{LOCATION_STAMP} | Timezone: {DISPLAY_TZ}")

# Data engine ensures only rej.approve = 'TRUE' is loaded
p_df = get_standalone_portal_data()

# Call the function you just created
render_client_portal(p_df)
