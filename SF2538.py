import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

#########################
# --- CONFIGURATION --- #
#########################
# Updated project number and title
ACTIVE_PROJECT = "2538" 
PROJECT_TITLE = "Pump 16 Upgrade Project Ferndale, Washington"
UNIT_LABEL = "°F"
FREEZING_LINE = 32.0

st.set_page_config(page_title=PROJECT_TITLE, layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"Authentication Failed: {e}")
        return None

client = get_bq_client()

###########################
# --- GLOBAL DATA LOAD --- #
###########################
if "data_loaded" not in st.session_state:
    with st.spinner(f"⚡ Fetching Data for Project {ACTIVE_PROJECT}..."):
        query = f"""
            SELECT timestamp, temperature, Depth, Location, Bank, NodeNum, approve
            FROM `{MASTER_TABLE}`
            WHERE CAST(Project AS STRING) = '{ACTIVE_PROJECT}' 
            AND (approve = 'TRUE' OR approve = 'true')
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
            ORDER BY timestamp ASC
        """
        try:
            if client is None:
                st.stop()
            df = client.query(query).to_dataframe()
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert(pytz.UTC) if df['timestamp'].dt.tz else pd.to_datetime(df['timestamp']).dt.tz_localize(pytz.UTC)
                df['Depth_Num'] = pd.to_numeric(df['Depth'], errors='coerce')
                st.session_state.master_df = df
                st.session_state.data_loaded = True
            else:
                st.session_state.master_df = pd.DataFrame()
        except Exception as e:
            st.error(f"Sync Error: {e}")

p_df = st.session_state.get("master_df", pd.DataFrame())

############################
# --- GRAPHING ENGINES --- #
############################

def build_standard_sf_graph(df, start_view, end_view):
    try:
        display_df = df.copy()
        if display_df.empty: return go.Figure()
        
        y_range = [-20, 80]
        display_df['label'] = display_df.apply(lambda r: f"{r.get('Depth', r.get('Bank', 'Unmapped'))}ft ({r.get('NodeNum', 'Unknown')})", axis=1)
        
        fig = go.Figure()
        for lbl in sorted(display_df['label'].unique()):
            sdf = display_df[display_df['label'] == lbl].sort_values('timestamp')
            sdf['gap'] = sdf['timestamp'].diff().dt.total_seconds() / 3600
            if (sdf['gap'] > 6.0).any():
                gaps = sdf[sdf['gap'] > 6.0].copy()
                gaps['temperature'] = None
                gaps['timestamp'] -= pd.Timedelta(seconds=1)
                sdf = pd.concat([sdf, gaps]).sort_values('timestamp')
            fig.add_trace(go.Scatter(x=sdf['timestamp'], y=sdf['temperature'], name=lbl, mode='lines', connectgaps=False))

        for ts in pd.date_range(start=start_view, end=end_view, freq='6h'):
            color, width = ("Black", 2) if (ts.weekday() == 0 and ts.hour == 0) else (("Gray", 1) if ts.hour == 0 else ("LightGray", 0.5))
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        fig.update_yaxes(title=f"Temp ({UNIT_LABEL})", range=y_range, gridcolor='Gainsboro', dtick=5)
        # Always present reference line
        fig.add_hline(y=FREEZING_LINE, line_dash="dash", line_color="RoyalBlue", line_width=2, annotation_text="Freezing (32°F)")
        fig.update_layout(plot_bgcolor='white', height=600, margin=dict(r=150))
        return fig
    except: return go.Figure()

########################
# --- MAIN CONTENT --- #
########################
st.header(f"📊 {PROJECT_TITLE}")

if p_df.empty:
    st.warning(f"No approved data found for Project {ACTIVE_PROJECT} in the last 84 days.")
else:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

    with tab_time:
        weeks = st.select_slider("Weeks to View", options=[1, 2, 4, 6, 8, 12], value=6)
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        for loc in sorted(p_df['Location'].dropna().unique()):
            with st.expander(f"📈 {loc}", expanded=True):
                loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                st.plotly_chart(build_standard_sf_graph(loc_data, start_view, end_view), use_container_width=True, key=f"t_{loc}")

    with tab_depth:
        depth_only = p_df.dropna(subset=['Depth_Num', 'NodeNum']).copy()
        for loc in sorted(depth_only['Location'].unique()):
            with st.expander(f"📏 {loc} Depth Profile", expanded=True):
                loc_data = depth_only[depth_only['Location'] == loc]
                fig_d = go.Figure()
                mondays = pd.date_range(start=start_view, end=now, freq='W-MON')
                
                for target_ts in [m.replace(hour=6) for m in mondays]:
                    window = loc_data[(loc_data['timestamp'] >= target_ts - pd.Timedelta(days=1)) & (loc_data['timestamp'] <= target_ts + pd.Timedelta(days=1))]
                    if not window.empty:
                        snaps = [window[window['NodeNum']==n].sort_values(by='timestamp', key=lambda x: (x-target_ts).abs()).iloc[0] for n in window['NodeNum'].unique()]
                        snap_df = pd.DataFrame(snaps).sort_values('Depth_Num')
                        fig_d.add_trace(go.Scatter(x=snap_df['temperature'], y=snap_df['Depth_Num'], mode='lines+markers', name=target_ts.strftime('%m/%d/%Y')))
                
                y_limit = int(((loc_data['Depth_Num'].max() // 5) + 1) * 5)
                fig_d.update_xaxes(title=f"Temp ({UNIT_LABEL})", range=[-20, 80], dtick=5, showgrid=True, gridcolor='LightGray')
                # Always present vertical reference line
                fig_d.add_vline(x=FREEZING_LINE, line_dash="dash", line_color="RoyalBlue", line_width=2.5)
                fig_d.update_yaxes(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='LightGray')
                fig_d.update_layout(plot_bgcolor='white', height=700)
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_{loc}")

    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
        latest['Temp'] = latest['temperature'].apply(lambda x: f"{round(x, 1)}°F")
        latest['Position'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r['Depth']} ft", axis=1)
        st.dataframe(latest[['Location', 'Position', 'Temp', 'NodeNum']], use_container_width=True, hide_index=True)
