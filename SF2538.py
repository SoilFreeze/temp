import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz

#################################################################
# 1. CONFIGURATION: Project 2538-Ferndale                       #
#################################################################
TARGET_PROJECT = "2538-Ferndale"    
CLIENT_NAME = "Pump 16 Upgrade"     
LOCATION_STAMP = "Ferndale, WA"     
DISPLAY_TZ = "America/Los_Angeles"  
UNIT_LABEL = "°F"                   

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"

# Updated to use snapshot as requested
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata_snapshot" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

PROJECT_VISIBILITY_MASKS = {
    "2538-Ferndale": "2024-01-01 00:00:00" 
}

st.set_page_config(page_title=f"Project {TARGET_PROJECT} Portal", layout="wide")

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            # Drive scope is required to access metadata_snapshot if it's a linked Sheet
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

# --- CRITICAL FIX: Initialize the client variable ---
client = get_bq_client() 

############################
# 2. DATA ENGINE LOGIC     #
############################

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id, view_mode="engineering"):
    """
    Data Engine using 'approve' status for visibility[cite: 13, 14].
    """
    # Safety check for the global client variable
    if client is None: 
        return pd.DataFrame()

    cutoff = PROJECT_VISIBILITY_MASKS.get(project_id, "2000-01-01 00:00:00")
    
    if view_mode == "client":
        # Logic: Must be Approved (TRUE) AND NOT Masked [cite: 12, 15]
        query_filter = f"""
            AND r.timestamp >= '{cutoff}'
            AND rej.approve = 'TRUE'
            AND NOT EXISTS (
                SELECT 1 FROM `{OVERRIDE_TABLE}` m 
                WHERE m.NodeNum = r.NodeNum 
                AND m.timestamp = TIMESTAMP_TRUNC(r.timestamp, HOUR)
                AND m.approve = 'MASKED'
            )
        """
    else:
        # Engineering sees everything except explicit deletions (FALSE) [cite: 16]
        query_filter = "AND (rej.approve IS NULL OR rej.approve != 'FALSE')"

    query = f"""
        SELECT 
            r.NodeNum, r.timestamp, r.temperature,
            m.Location, m.Bank, m.Depth, m.Project
        FROM (
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_sensorpush` [cite: 2, 3]
            UNION ALL
            SELECT NodeNum, timestamp, temperature FROM `{PROJECT_ID}.{DATASET_ID}.raw_lord` [cite: 2]
        ) AS r
        INNER JOIN `{METADATA_TABLE}` AS m ON r.NodeNum = m.NodeNum [cite: 5, 8]
        LEFT JOIN `{OVERRIDE_TABLE}` AS rej 
            ON r.NodeNum = rej.NodeNum 
            AND TIMESTAMP_TRUNC(r.timestamp, HOUR) = rej.timestamp [cite: 11]
        WHERE m.Project = '{project_id}' [cite: 5, 9]
        {query_filter}
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY m.Location ASC, r.timestamp ASC
    """
    try:
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

def render_client_portal(selected_project, display_tz, unit_mode, unit_label, active_refs):
    st.header(f"📊 Project Status: {selected_project}")
    global client

    if not selected_project or selected_project == "All Projects":
        st.info("💡 Please select a specific project in the sidebar.")
        return
    
    with st.spinner("Loading approved data..."):
        # The portal specifically filters for manual_rejections.status = 'TRUE' [cite: 15, 16]
        p_df = get_universal_portal_data(selected_project, view_mode="client")
    
    # DEBUG: Help identify if data exists but is being filtered out later
    if not p_df.empty:
        st.caption(f"✅ Found {len(p_df)} approved records for {selected_project}.")
    else:
        st.warning(f"⚠️ No data marked as 'Approved' found for {selected_project}. Check the Admin Tools.")
        return

    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Summary Table"])

    with tab_time:
        weeks_view = st.slider("Weeks to View", 1, 12, 6, key="client_weeks_slider")
        end_view = pd.Timestamp.now(tz='UTC')
        start_view = end_view - timedelta(weeks=weeks_view)
        
        # Performance: Pre-sort locations
        locations = sorted(p_df['Location'].dropna().unique())
        
        if not locations:
            st.error("Data loaded, but no 'Location' metadata was found to group the charts.")
        
        for loc in locations:
            with st.expander(f"📍 {loc}", expanded=(len(locations) == 1)):
                loc_data = p_df[p_df['Location'] == loc].copy()
                
                # Check if this specific location has data in the selected time window
                if loc_data.empty:
                    st.write("No data available for this specific location.")
                    continue

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc} Approved Data", 
                    start_view=start_view, 
                    end_view=end_view, 
                    active_refs=tuple(active_refs), 
                    unit_mode=unit_mode, 
                    unit_label=unit_label, 
                    display_tz=display_tz 
                )
                st.plotly_chart(fig, use_container_width=True, key=f"portal_grid_{loc}")

    with tab_depth:
        st.subheader("📏 Vertical Temperature Profile")
        # Ensure Depth is numeric for proper Y-axis scaling [cite: 6, 9]
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
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
                        snap_df = (
                            window.assign(diff=(window['timestamp'] - target_ts).abs())
                            .sort_values(['NodeNum', 'diff'])
                            .drop_duplicates('NodeNum')
                            .sort_values('Depth_Num')
                        )
                        
                        conv_temps = snap_df['temperature'].apply(
                            lambda x: (x - 32) * 5/9 if unit_mode == "Celsius" else x
                        )
                        
                        fig_d.add_trace(go.Scatter(
                            x=conv_temps, 
                            y=snap_df['Depth_Num'], 
                            mode='lines+markers', 
                            name=target_ts.strftime('%m/%d/%y'),
                            line=dict(shape='spline', smoothing=0.5)
                        ))

                y_limit = int(((loc_data['Depth_Num'].max() // 10) + 1) * 10) if not loc_data.empty else 50
                fig_d.update_layout(
                    plot_bgcolor='white', height=600,
                    xaxis=dict(title=f"Temp ({unit_label})", gridcolor='Gainsboro'),
                    yaxis=dict(title="Depth (ft)", range=[y_limit, 0], dtick=10, gridcolor='Silver'),
                    legend=dict(orientation="h", y=-0.2)
                )
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_graph_{loc}")

    with tab_table:
        # Latest Snapshot Table (Fastest way to group latest data)
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        
        # Efficient vector conversion
        latest['Current Temp'] = latest['temperature'].apply(
            lambda x: f"{round((x - 32) * 5/9 if unit_mode == 'Celsius' else x, 1)}{unit_label}"
        )
        
        latest['Position'] = latest.apply(
            lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" 
            else f"{r.get('Depth', '??')} ft", axis=1
        )
        
        st.dataframe(
            latest[['Location', 'Position', 'Current Temp', 'NodeNum']].sort_values(['Location', 'Position']), 
            use_container_width=True, 
            hide_index=True
        )

###########################
# 4. MAIN UI LAYOUT       #
###########################

st.title(f"📊 {CLIENT_NAME}")
st.caption(f"{LOCATION_STAMP} | Timezone: {DISPLAY_TZ}")

# Ensure this is defined so the query doesn't fail on 'cutoff'
PROJECT_VISIBILITY_MASKS = {
    "2538-Ferndale": "2024-01-01 00:00:00" 
}

# The NameError is fixed by calling the function defined in Section 2
# This will now work once the 'Drive' scopes are added to Section 1
render_client_portal(
    selected_project=TARGET_PROJECT, 
    display_tz=DISPLAY_TZ, 
    unit_mode="Fahrenheit", 
    unit_label=UNIT_LABEL, 
    active_refs=[]
)
