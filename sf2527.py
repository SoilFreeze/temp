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
TARGET_PROJECT = "2527"    
CLIENT_NAME = "SJI Erie St"     
LOCATION_STAMP = "Elizabeth, New Jersey"     
DISPLAY_TZ = "America/New_York"  
UNIT_LABEL = "°F"                   

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
# UPDATED: Using the master metadata table for all 114 nodes
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata"
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

PROJECT_VISIBILITY_MASKS = {
    "2527": "2026-01-01 00:00:00",
    "2538-Ferndale": "2024-01-01 00:00:00" 
}

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

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    ULTRA-ROBUST FILTER:
    1. Casts Project to STRING to prevent TypeErrors.
    2. Uses TRIM to handle hidden spaces.
    3. Handles Case-Insensitive 'TRUE'/'True'.
    """
    if client is None: return pd.DataFrame()

    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    # Force project_id to string for the query
    target_pid = str(project_id).strip()

    if view_mode == "client":
        query_filter = f"""
            AND r.timestamp >= '{cutoff}'
            AND (UPPER(CAST(rej.approve AS STRING)) = 'TRUE' OR rej.approve IS NULL)
            AND NOT EXISTS (
                SELECT 1 FROM `{OVERRIDE_TABLE}` m 
                WHERE m.NodeNum = r.NodeNum 
                AND m.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                AND UPPER(CAST(m.approve AS STRING)) = 'MASKED'
            )
        """
    else:
        query_filter = "AND (rej.approve IS NULL OR UPPER(CAST(rej.approve AS STRING)) != 'FALSE')"

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
        WHERE CAST(m.Project AS STRING) LIKE '{target_pid}%'
        {query_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    try:
        # Standardize the conversion to dataframe
        df = client.query(query).to_dataframe()
        
        # FINAL FAILSAFE: If BigQuery returned 'True' as a boolean, convert to string
        if not df.empty and 'is_approved' in df.columns:
            df['is_approved'] = df['is_approved'].astype(str).str.upper()
            
        return df
    except Exception as e:
        st.error(f"BQ Error: {e}")
        return pd.DataFrame()

########################
# 3. GRAPHING ENGINE   #
########################

def build_high_speed_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label, display_tz):
    if df.empty: return go.Figure().update_layout(title="No data available.")

    pdf = df.copy()
    if unit_mode == "Celsius":
        pdf['temperature'] = (pdf['temperature'] - 32) * 5/9
        freezing_line = 0
    else:
        freezing_line = 32

    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    # Clean Labeling Logic
    def clean_label(r):
        bank_val = str(r.get('Bank', '')).strip()
        if bank_val.lower() not in ["", "none", "nan", "null"]:
            prefix = "" if "bank" in bank_val.lower() else "Bank "
            return f"{prefix}{bank_val} ({r['NodeNum']})"
        return f"{r.get('Depth', '??')}ft ({r['NodeNum']})"

    pdf['label'] = pdf.apply(clean_label, axis=1)
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # GAP DETECTION: Relaxed to 48 hours for sparse Elizabeth data
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 48.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # UPDATED: connectgaps=True and mode='lines+markers' to fix invisible lines
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines+markers', 
            connectgaps=True,
            marker=dict(size=4)
        ))

    # Grid Hierarchy
    grid_days = pd.date_range(start=start_view.tz_convert(display_tz).floor('D'), 
                             end=end_view.tz_convert(display_tz).ceil('D'), freq='D', tz=display_tz)
    for ts in grid_days:
        color, width, dash = ("rgba(0,0,0,1)", 1.2, "solid") if ts.weekday() == 0 else ("rgba(128,128,128,0.4)", 0.8, "dot")
        fig.add_vline(x=ts, line_width=width, line_color=color, line_dash=dash, layer='below')

    fig.add_vline(x=pd.Timestamp.now(tz=display_tz), line_width=2, line_color="Red", line_dash="dash")
    fig.add_hline(y=freezing_line, line_dash="dash", line_color="RoyalBlue", 
                 annotation_text=f"{freezing_line}{unit_label} Freezing")
    
    for ref in active_refs:
        try:
            val = float(ref)
            fig.add_hline(y=val, line_dash="dot", line_color="Orange", annotation_text=f"Ref: {val}{unit_label}")
        except: continue

    fig.update_layout(
        title=f"<b>{title}</b>", plot_bgcolor='white', hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, linecolor='black', mirror=True, tickformat='%b %d'),
        yaxis=dict(title=unit_label, gridcolor='Gainsboro', showline=True, linecolor='black', mirror=True, range=[-20, 80]),
        height=550, margin=dict(r=150),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1, xanchor="left")
    )
    return fig

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header(f"📊 Project Status: {selected_project}")
    
    with st.spinner("Loading Elizabeth data..."):
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    if p_df.empty:
        st.warning(f"⚠️ No data found for {selected_project}. Check metadata mapping.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = end_view - timedelta(weeks=weeks_view)
        
        locations = sorted(p_df['Location'].dropna().unique())
        for loc in locations:
            # UNIQUE KEY FIX: Added loc to the key
            with st.expander(f"📍 {loc}", expanded=True):
                loc_data = p_df[p_df['Location'] == loc].copy()
                fig = build_high_speed_graph(
                    df=loc_data, title=f"{loc} Approved Data", 
                    start_view=start_view, end_view=end_view, 
                    active_refs=tuple(active_refs), unit_mode=unit_mode, 
                    unit_label=unit_label, display_tz=display_tz 
                )
                # ADDED KEY HERE
                st.plotly_chart(fig, use_container_width=True, key=f"timeline_{loc}")

    with tab_depth:
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num']).copy()
        for loc in sorted(depth_only['Location'].unique()):
            # UNIQUE KEY FIX: Added loc to the key
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
                        conv_temps = snap_df['temperature'].apply(lambda x: (x-32)*5/9 if unit_mode == "Celsius" else x)
                        fig_d.add_trace(go.Scatter(x=conv_temps, y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%y')))
                
                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(plot_bgcolor='white', height=600, yaxis=dict(range=[y_limit, 0], title="Depth (ft)"), xaxis=dict(range=[-20, 80], title=unit_label))
                # ADDED KEY HERE
                st.plotly_chart(fig_d, use_container_width=True, key=f"depth_{loc}")

    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Current Temp'] = latest['temperature'].apply(lambda x: f"{round((x-32)*5/9 if unit_mode=='Celsius' else x, 1)}{unit_label}")
        st.dataframe(latest[['Location', 'NodeNum', 'Current Temp']].sort_values('Location'), use_container_width=True, hide_index=True)
###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {CLIENT_NAME}")
st.caption(f"{LOCATION_STAMP} | Timezone: {DISPLAY_TZ}")

render_client_portal(
    selected_project=TARGET_PROJECT, 
    display_tz=DISPLAY_TZ, 
    unit_mode="Fahrenheit", 
    unit_label=UNIT_LABEL, 
    active_refs=[]
)
