import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os

# ===============================================================
# 1. TARGET CONFIGURATION (CHANGE ONLY THIS LINE)
# ===============================================================
TARGET_JOB_NUMBER = "2527" 
# ===============================================================

st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")

# Hide standard sidebar navigation for client view
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature" 

# --- CORE ENGINE FUNCTIONS ---

@st.cache_resource
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            info = st.secrets["gcp_service_account"]
            SCOPES = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"]
            credentials = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
            return bigquery.Client(credentials=credentials, project=info["project_id"])
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error(f"❌ Authentication Failed: {e}")
        return None

def ensure_tz_convert(series, target_tz):
    """Safely handles conversion for both localized and naive timestamps."""
    if series.dt.tz is None:
        return series.dt.tz_localize('UTC').dt.tz_convert(target_tz)
    return series.dt.tz_convert(target_tz)

@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    client = get_bq_client()
    if client is None: return pd.DataFrame()
    query = f"""
        SELECT m.* FROM `{PROJECT_ID}.{DATASET_ID}.master_data_view` m
        JOIN `{PROJECT_ID}.{DATASET_ID}.project_registry` p ON m.Project = p.Project
        WHERE m.Project = @project_id 
        AND m.timestamp >= CAST(p.Date_Freezedown AS TIMESTAMP)
        AND UPPER(CAST(m.approval_status AS STRING)) IN ('TRUE', '1')
        ORDER BY m.timestamp ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("project_id", "STRING", project_id)])
    return client.query(query, job_config=job_config).to_dataframe()

# --- REUSED GRAPHING ENGINE ---

def build_high_speed_graph(df, title, start_view, end_view, unit_mode, unit_label, display_tz, f_start_date=None, curve_id=None, active_refs=()):
    if df.empty: return go.Figure().update_layout(title="No data available")
    pdf = df.copy()
    fig = go.Figure()

    # Timezone Sync
    pdf['timestamp'] = ensure_tz_convert(pdf['timestamp'], display_tz)
    
    # Reference Curve Logic (Goal Overlay)
    if curve_id and f_start_date:
        client = get_bq_client()
        today_day = (pd.Timestamp.now().date() - f_start_date).days
        ref_q = f"SELECT Day, Temp FROM `{PROJECT_ID}.{DATASET_ID}.reference_curves` WHERE UPPER(CurveID) = UPPER('{curve_id}') ORDER BY Day"
        ref_df = client.query(ref_q).to_dataframe()
        if not ref_df.empty:
            ref_df['timestamp'] = ref_df['Day'].apply(lambda d: pd.Timestamp(f_start_date) + pd.Timedelta(days=d))
            ref_df['timestamp'] = ensure_tz_convert(ref_df['timestamp'], display_tz)
            y_ref = ref_df['Temp'] if unit_mode == "Fahrenheit" else (ref_df['Temp'] - 32) * 5/9
            fig.add_trace(go.Scatter(x=ref_df['timestamp'], y=y_ref, name="Target Goal", line=dict(color='gray', dash='dash')))

    # Sensor Trace
    for sn in sorted(pdf['NodeNum'].unique()):
        s_df = pdf[pdf['NodeNum'] == sn].sort_values('timestamp')
        fig.add_trace(go.Scatter(x=s_df['timestamp'], y=s_df['temperature'], name=f"Node {sn}", mode='lines'))

    fig.update_layout(title=title, template="plotly_white", xaxis_range=[start_view, end_view], yaxis_title=unit_label)
    fig.add_hline(y=(32 if unit_mode == "Fahrenheit" else 0), line_dash="dash", line_color="RoyalBlue")
    return fig

# --- UI TABS ---

def render_summary_tab(full_p_df, unit_label, local_tz):
    st.subheader("🌐 24 hour Thermal Summary")
    now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
    df_local = full_p_df.copy()
    df_local['timestamp'] = ensure_tz_convert(df_local['timestamp'], local_tz)
    
    def classify_pipe(row):
        loc, bank = str(row['Location']).upper(), str(row['Bank']).upper()
        if 'S' in bank or 'SUPPLY' in loc: return 'Supply (S)'
        if 'R' in bank or 'RETURN' in loc: return 'Return (R)'
        return 'Temp Pipes (TP)'

    df_local['PipeType'] = df_local.apply(classify_pipe, axis=1)
    cols = st.columns(3)
    categories = ['Supply (S)', 'Return (R)', 'Temp Pipes (TP)']

    for i, p_type in enumerate(categories):
        with cols[i]:
            st.markdown(f"### {p_type}")
            type_df = df_local[df_local['PipeType'] == p_type]
            if type_df.empty:
                st.caption("No data available.")
                continue

            df_24h = type_df[type_df['timestamp'] >= (now_local - pd.Timedelta(days=1))]
            latest_ts = type_df['timestamp'].max()
            lag_hrs = (now_local - latest_ts).total_seconds() / 3600
            
            target_df = df_24h if not df_24h.empty else type_df
            avg_val, high_val, low_val = target_df['temperature'].mean(), target_df['temperature'].max(), target_df['temperature'].min()

            label_pfx = "" if lag_hrs <= 1.5 else "Last "
            st.metric(f"{label_pfx}24h Average", f"{avg_val:.1f}{unit_label}")
            
            if lag_hrs > 1.5:
                st.error(f"⚠️ Data is {int(lag_hrs)}h old")
            else:
                st.success("🟢 Data is Live")

            sub1, sub2 = st.columns(2)
            sub1.caption(f"**High (24h):**\n{high_val:.1f}{unit_label}")
            sub2.caption(f"**Low (24h):**\n{low_val:.1f}{unit_label}")
            st.divider()

def render_client_portal():
    client = get_bq_client()
    if client is None: return

    # Registry aggregation (Blackjack Ph 1 & 2)
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    primary_meta = proj_registry.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', TARGET_JOB_NUMBER)
    local_tz = primary_meta.get('Timezone', 'US/Pacific')
    f_start_date = pd.to_datetime(primary_meta.get('Date_Freezedown')).date() if pd.notnull(primary_meta.get('Date_Freezedown')) else None
    asbuilt_filename = primary_meta.get('AsBuiltFile')

    with st.spinner("Synchronizing official records..."):
        all_phases = [get_universal_portal_data(p_id) for p_id in proj_registry['Project']]
        p_df = pd.concat(all_phases) if all_phases else pd.DataFrame()

    if p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    # Official Status Bar
    last_approved_local = ensure_tz_convert(p_df['timestamp'], local_tz).max()
    st.info(f"✅ **Official Data Status:** Records are approved through **{last_approved_local.strftime('%B %d, %Y at %I:%M %p')}**.")

    st.header(f"📊 {display_name}")
    tabs = st.tabs(["🏠 Summary", "📈 Time vs Temp", "📏 Temp vs Depth", "📋 Summary Table", "🗺️ As Built"])
    
    # --- TAB 0: SUMMARY ---
    with tabs[0]:
        render_summary_tab(p_df, "°F", local_tz)

    # --- TAB 1: TIME vs TEMP (PASTED LOGIC) ---
    with tabs[1]:
        st.sidebar.subheader("📅 Timeline Controls")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        show_ref = st.sidebar.toggle("Show Progress Goals", value=True)
        
        now_local = pd.Timestamp.now(tz='UTC').tz_convert(local_tz)
        start_view = now_local - timedelta(weeks=weeks_view)
        
        locations = sorted([str(loc) for loc in p_df['Location'].dropna().unique()])
        for loc in locations:
            with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                loc_data = p_df[p_df['Location'] == loc].copy()
                cid = f"{TARGET_JOB_NUMBER}-{loc}" if show_ref else None

                fig = build_high_speed_graph(
                    df=loc_data, 
                    title=f"{loc}: {weeks_view}-Week Trend", 
                    start_view=start_view, 
                    end_view=now_local, 
                    unit_mode="Fahrenheit", 
                    unit_label="°F", 
                    display_tz=local_tz,
                    f_start_date=f_start_date,
                    curve_id=cid
                )
                st.plotly_chart(fig, use_container_width=True, key=f"time_{loc}")

    # --- TAB 2: TEMP vs DEPTH (PASTED LOGIC) ---
    with tabs[2]:
        st.subheader("📏 Vertical Temperature Profile")
        p_df['Depth_Num'] = pd.to_numeric(p_df['Depth'], errors='coerce')
        depth_only = p_df.dropna(subset=['Depth_Num', 'Location']).copy()
        
        if depth_only.empty:
            st.info("Vertical profile data is not available.")
        else:
            for loc in sorted(depth_only['Location'].unique()):
                with st.expander(f"📏 Temp vs Depth - {loc}", expanded=True):
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

                    max_d = depth_only['Depth_Num'].max()
                    y_limit = int(((max_d // 10) + 1) * 10) if pd.notnull(max_d) else 50
                    fig_d.update_layout(plot_bgcolor='white', height=600, xaxis=dict(title="Temp (°F)", range=[-10, 80], showline=True, mirror=True), yaxis=dict(title="Depth (ft)", range=[y_limit, 0], showline=True, mirror=True))
                    st.plotly_chart(fig_d, use_container_width=True, key=f"depth_{loc}")

    # --- TAB 3: SUMMARY TABLE ---
    with tabs[3]:
        latest = p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['timestamp'] = ensure_tz_convert(latest['timestamp'], local_tz)
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        st.dataframe(latest[['Location', 'Position', 'temperature', 'timestamp']], use_container_width=True, hide_index=True)

    # --- TAB 4: AS BUILT ---
    with tabs[4]:
        if pd.notnull(asbuilt_filename):
            img_path = f"assets/asbuilts/{asbuilt_filename}"
            if os.path.exists(img_path):
                st.image(img_path)
            else:
                st.warning(f"File '{asbuilt_filename}' not found.")

# --- EXECUTION ---
render_client_portal()
