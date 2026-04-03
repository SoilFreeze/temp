import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

#########################
# --- CONFIGURATION --- #
#########################
# Updated project identifier and title
ACTIVE_PROJECT = "2538" 
PROJECT_TITLE = "Pump 16 Upgrade Project Ferndale, Washington"

st.set_page_config(page_title=PROJECT_TITLE, layout="wide")

DATASET_ID = "Temperature" 
PROJECT_ID = "sensorpush-export"
MASTER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.master_data"

@st.cache_resource
def get_bq_client():
    try:
        # Priority 1: Check Streamlit Secrets (for Local or Community Cloud)
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                info, 
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        
        # Priority 2: Try default environment credentials (only works if logged in via gcloud CLI)
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        # This catch provides a clearer explanation to the UI
        st.error("Authentication Error: Service Account secrets not found or invalid.")
        st.info("Check that your .streamlit/secrets.toml file is configured correctly.")
        return None

client = get_bq_client()

###########################
# --- GLOBAL DATA LOAD --- #
###########################
if "data_loaded" not in st.session_state:
    with st.spinner("⚡ Initializing High-Speed Pipeline..."):
        query = f"""
            SELECT timestamp, temperature, Depth, Location, Bank, NodeNum, approve
            FROM `{MASTER_TABLE}`
            WHERE CAST(Project AS STRING) = '{ACTIVE_PROJECT}' 
            AND (approve = 'TRUE' OR approve = 'true')
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
            ORDER BY timestamp ASC
        """
        try:
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

def build_standard_sf_graph(df, title, start_view, end_view, active_refs, unit_mode, unit_label):
    try:
        display_df = df.copy()
        if display_df.empty: return go.Figure()
        
        y_range = [-20, 80] if unit_mode == "Fahrenheit" else [-30, 30]
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
            if ts.weekday() == 0 and ts.hour == 0: color, width = "Black", 2
            elif ts.hour == 0: color, width = "Gray", 1
            else: color, width = "LightGray", 0.5 
            fig.add_vline(x=ts, line_width=width, line_color=color, layer='below')

        fig.update_yaxes(title=f"Temp ({unit_label})", range=y_range, gridcolor='Gainsboro', dtick=5)
        fig.update_layout(plot_bgcolor='white', height=600, margin=dict(r=150))
        
        # Only renders the references passed from the sidebar
        for val, label in active_refs:
            fig.add_hline(y=val, line_dash="dash", line_color="RoyalBlue", line_width=2)
        return fig
    except: return go.Figure()

#######################
# --- SIDEBAR UI --- #
#######################
st.sidebar.title("📏 Dashboard Controls")
unit_mode = st.sidebar.radio("Temperature Unit", ["Fahrenheit", "Celsius"])
unit_label = "°F" if unit_mode == "Fahrenheit" else "°C"

st.sidebar.divider()
active_refs = []
# Simplified to only include the Freezing reference
if st.sidebar.checkbox("Show Freezing Line (32°F)", value=True): 
    active_refs.append((32.0, "Freezing"))

def convert_val(f):
    return (f - 32) * 5/9 if unit_mode == "Celsius" else f

########################
# --- MAIN CONTENT --- #
########################
# Updated Main Header
st.header(f"📊 {PROJECT_TITLE}")

if p_df.empty:
    st.warning(f"No approved data found for Project {ACTIVE_PROJECT}.")
else:
    tab_time, tab_depth, tab_table = st.tabs(["📈 Timeline Analysis", "📏 Depth Profile", "📋 Project Data"])

    with tab_time:
        weeks = st.slider("Weeks to View", 1, 12, 6, key="time_slider")
        now = pd.Timestamp.now(tz=pytz.UTC)
        end_view = (now + pd.Timedelta(days=(7 - now.weekday()) % 7 or 7)).replace(hour=0, minute=0, second=0, microsecond=0)
        start_view = end_view - timedelta(weeks=weeks)
        
        for loc in sorted(p_df['Location'].dropna().unique()):
            with st.expander(f"📈 {loc}", expanded=True):
                loc_data = p_df[(p_df['Location'] == loc) & (p_df['timestamp'] >= start_view)]
                st.plotly_chart(build_standard_sf_graph(loc_data, loc, start_view, end_view, active_refs, unit_mode, unit_label), use_container_width=True, key=f"t_{loc}")

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
                
                fig_d.update_xaxes(title=f"Temp ({unit_label})", range=[-20, 80], dtick=5, showgrid=True, gridcolor='LightGray', gridwidth=0.5)
                for x_v in range(-20, 81, 20): fig_d.add_vline(x=x_v, line_width=2.0, line_color="Black")
                fig_d.update_yaxes(title="Depth (ft)", range=[y_limit, 0], dtick=10, showgrid=True, gridcolor='LightGray', gridwidth=0.7)
                
                for val, label in active_refs:
                    fig_d.add_vline(x=val, line_dash="dash", line_color="RoyalBlue", line_width=2.5)

                fig_d.update_layout(plot_bgcolor='white', height=700)
                st.plotly_chart(fig_d, use_container_width=True, key=f"d_{loc}")

    with tab_table:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').tail(1).copy()
        latest['Temp'] = latest['temperature'].apply(lambda x: f"{round(convert_val(x), 1)}{unit_label}")
        latest['Pos'] = latest.apply(lambda r: f"Bank {r['Bank']}" if pd.notnull(r['Bank']) and str(r['Bank']).strip() != "" else f"{r['Depth']} ft", axis=1)
        st.dataframe(latest[['Location', 'Pos', 'Temp', 'NodeNum']], use_container_width=True, hide_index=True)
