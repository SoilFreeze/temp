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
# CHANGE THESE TO SWITCH PROJECTS
CURRENT_PROJECT_KEY = "2527" 

PROJECT_REGISTRY = {
    "2527": {
        "name": "SJI Erie St Remediation",
        "location": "Elizabeth, NJ",
        "start_date": "2026-04-24 00:00:00",
        "timezone": "America/New_York",
        "upload_note": "Data will be uploaded once per business day by 4pm Pacific Time."
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
UNIT_LABEL = active.get("unit", "°F")

# Database Globals
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata" 
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

    # Check if Start_Date exists in the table schema first to avoid the 400 error
    table_ref = client.get_table(METADATA_TABLE)
    column_names = [field.name for field in table_ref.schema]
    
    # Build conditional join logic
    if "Start_Date" in column_names and "End_Date" in column_names:
        date_filter = """
            AND r.timestamp >= COALESCE(SAFE_CAST(m.Start_Date AS TIMESTAMP), '2000-01-01')
            AND r.timestamp <= COALESCE(SAFE_CAST(m.End_Date AS TIMESTAMP), '2099-12-31')
        """
    else:
        date_filter = "" # Fallback if columns are missing

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
            {date_filter}
        WHERE CAST(m.Project AS STRING) = '{project_id}'
        AND r.timestamp >= '{start_date_str}'
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

# Get data using centralized variables
data = get_universal_portal_data(TARGET_PROJECT, PROJECT_START_DATE)

if not data.empty:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    # --- TAB 1: TIMELINE ---
    with tab_time:
        project_start_ts = pd.Timestamp(PROJECT_START_DATE, tz='UTC')
        weeks_view = st.slider("Weeks to View", 1, 12, 4, key="weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        # Ensure we don't try to view before the project actually started
        start_view = max(project_start_ts, end_view - timedelta(weeks=weeks_view))
        
        for loc in sorted(data['Location'].unique()):
            with st.expander(f"📍 {loc}", expanded=True):
                fig = build_high_speed_graph(data[data['Location'] == loc], f"{loc} Timeline", start_view, end_view, DISPLAY_TZ)
                st.plotly_chart(fig, width='stretch', key=f"graph_{loc}")

    # --- TAB 2: DEPTH PROFILE ---
    with tab_depth:
        # Convert Depth to numbers for vertical plotting
        data['Depth_Num'] = pd.to_numeric(data['Depth'], errors='coerce')
        depth_only = data.dropna(subset=['Depth_Num']).copy()
        
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Vertical Profile"):
                loc_data = depth_only[depth_only['Location'] == loc].copy()
                fig_d = go.Figure()
                
                # Plot "Snapshots" for every Monday since the project started
                project_start_ts = pd.Timestamp(PROJECT_START_DATE, tz='UTC')
                mondays = pd.date_range(start=project_start_ts, end=pd.Timestamp.now(tz='UTC'), freq='W-MON')
                
                for m_date in mondays:
                    target_ts = m_date.replace(hour=6, minute=0, second=0)
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(hours=12)) & 
                                      (loc_data['timestamp'] <= target_ts + pd.Timedelta(hours=12))]
                    
                    if not window.empty:
                        # Get the closest reading for each sensor in that window
                        snap_df = window.assign(diff=(window['timestamp'] - target_ts).abs()).sort_values(['NodeNum', 'diff']).drop_duplicates('NodeNum').sort_values('Depth_Num')
                        
                        fig_d.add_trace(go.Scatter(
                            x=snap_df['temperature'], 
                            y=snap_df['Depth_Num'], 
                            mode='lines+markers', 
                            name=target_ts.strftime('%m/%d/%y'),
                            customdata=snap_df[['timestamp', 'NodeNum']],
                            hovertemplate=f"<b>%{{customdata[0]|%b %d, %H:00}}</b><br>Sensor: %{{customdata[1]}}<br>Depth: %{{y}}ft<br>Temp: %{{x:.1f}}{UNIT_LABEL}<extra></extra>"
                        ))
                
                # Freezing Reference line for Depth Profile
                fig_d.add_vline(x=32, line_dash="dash", line_color="RoyalBlue")
                
                fig_d.update_layout(
                    plot_bgcolor='white', 
                    height=600, 
                    yaxis=dict(range=[int(((loc_data['Depth_Num'].max()//10)+1)*10), 0], title="Depth (ft)"), 
                    xaxis=dict(range=[-20, 80], title=f"Temperature ({UNIT_LABEL})"), 
                    hovermode="closest"
                )
                st.plotly_chart(fig_d, width='stretch', key=f"depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tab_table:
        # Get the very latest reading for every sensor
        latest = data.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        # Format columns for display
        latest['Depth_Sort'] = pd.to_numeric(latest['Depth'], errors='coerce').fillna(0)
        latest['Last Reading'] = latest['timestamp'].dt.tz_convert(DISPLAY_TZ).dt.strftime('%b %d, %H:%M')
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}{UNIT_LABEL}")
        
        # Filter and sort the table
        display_df = latest.sort_values(['Location', 'Bank', 'Depth_Sort'])[['Location', 'Bank', 'Depth', 'NodeNum', 'Current Temp', 'Last Reading']]
        
        st.dataframe(display_df, width='stretch', hide_index=True)

else:
    st.info(f"Awaiting data for {PROJECT_NAME} (Cutoff: {PROJECT_START_DATE})...")
