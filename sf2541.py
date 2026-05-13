import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- 1. GLOBAL UI CONFIG ---
st.set_page_config(page_title=f"SoilFreeze Portal #{TARGET_JOB_NUMBER}", layout="wide")

# Hide the sidebar navigation to keep it clean for the client
st.markdown("""<style> [data-testid="stSidebarNav"] {display: none;} </style>""", unsafe_allow_html=True)

def render_client_portal():
    # 1. INITIALIZE CLIENT & METADATA
    client = get_bq_client()
    
    # Robust Lookup: Finds all phases/projects associated with that Job Number
    # This ensures "2527-Blackjack-Ph1" and "2527-Blackjack-Ph2" both show up
    proj_q = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.project_registry` WHERE Project LIKE '{TARGET_JOB_NUMBER}%'"
    proj_registry = client.query(proj_q).to_dataframe()

    if proj_registry.empty:
        st.error(f"❌ No registry entry found for Job #{TARGET_JOB_NUMBER}")
        return

    # Use the primary project metadata for the header
    primary_meta = proj_registry.iloc[0].to_dict()
    display_name = primary_meta.get('ProjectName', TARGET_JOB_NUMBER)
    
    # 2. HEADER
    st.header(f"❄️ {display_name} Portal")
    st.caption(f"Secure Client View | Job Number: {TARGET_JOB_NUMBER}")

    # 3. DATA FETCHING (Aggregated for all phases)
    with st.spinner("Fetching official records..."):
        # This pulled data is already "Client Mode" (Approved Only)
        full_p_df = pd.DataFrame()
        for p_id in proj_registry['Project']:
            phase_df = get_universal_portal_data(p_id, view_mode="client")
            full_p_df = pd.concat([full_p_df, phase_df])

    if full_p_df.empty:
        st.warning("⚠️ No approved data records available yet.")
        return

    # 4. DASHBOARD TABS
    tabs = st.tabs(["🏠 Summary", "📈 Timeline", "📏 Depth", "📋 Sensor Status", "🗺️ As-Built"])
    tab_sum, tab_time, tab_depth, tab_table, tab_built = tabs

    # --- TAB: SUMMARY (NEW) ---
    with tab_sum:
        st.subheader("🌐 Global Project Health")
        
        # Calculate Key Metrics across all phases
        now = pd.Timestamp.now(tz='UTC')
        last_24h = full_p_df[full_p_df['timestamp'] >= (now - pd.Timedelta(days=1))]
        
        c1, c2, c3 = st.columns(3)
        with c1:
            avg_temp = last_24h['temperature'].mean()
            st.metric("Avg Project Temp (24h)", f"{avg_temp:.1f}°F")
        with c2:
            min_temp = last_24h['temperature'].min()
            st.metric("Coldest Point", f"{min_temp:.1f}°F")
        with c3:
            total_sensors = full_p_df['NodeNum'].nunique()
            st.metric("Active Sensors", total_sensors)

        st.divider()
        
        # Phase Comparison
        st.write("### Phase Breakdown")
        summary_pivot = full_p_df.groupby('Project').agg({
            'temperature': ['mean', 'min'],
            'timestamp': 'max'
        }).reset_index()
        summary_pivot.columns = ['Phase', 'Avg Temp', 'Min Temp', 'Last Update']
        st.dataframe(summary_pivot, use_container_width=True, hide_index=True)

    # --- TAB: TIMELINE ANALYSIS ---
    with tab_time:
        # Sidebar remains only for display options, not project switching
        st.sidebar.subheader("📅 Display Options")
        weeks_view = st.sidebar.slider("Timeline Span (Weeks)", 1, 12, 6)
        
        # Split display by Project Phase if multiple exist
        for phase in sorted(full_p_df['Project'].unique()):
            st.markdown(f"### {phase}")
            phase_df = full_p_df[full_p_df['Project'] == phase]
            
            locations = sorted(phase_df['Location'].unique())
            for loc in locations:
                with st.expander(f"📍 {loc} Thermal Trend", expanded=True):
                    loc_data = phase_df[phase_df['Location'] == loc].copy()
                    
                    fig = build_high_speed_graph(
                        df=loc_data, 
                        title=f"{loc} History", 
                        start_view=now - timedelta(weeks=weeks_view), 
                        end_view=now, 
                        unit_mode=st.session_state.get('unit_mode', 'Fahrenheit'),
                        unit_label="°F", 
                        display_tz="US/Pacific", # Default or metadata based
                        f_start_date=pd.to_datetime(primary_meta.get('Date_Freezedown')).date(),
                        curve_id=f"{TARGET_JOB_NUMBER}-{loc}"
                    )
                    st.plotly_chart(fig, use_container_width=True, key=f"cht_{phase}_{loc}")

    # --- TAB: DEPTH PROFILE ---
    with tab_depth:
        # Use your existing Vertical Temperature Profile logic here
        # Loop through full_p_df for depth profile rendering
        pass

    # --- TAB: SENSOR STATUS (LATEST TABLE) ---
    with tab_table:
        latest = full_p_df.sort_values('timestamp').groupby('NodeNum').last().reset_index()
        latest['Position'] = latest.apply(lambda r: f"{r['Depth']} ft" if pd.notnull(r.get('Depth')) else f"Bank {r['Bank']}", axis=1)
        st.dataframe(
            latest[['Project', 'Location', 'Position', 'temperature', 'timestamp']].sort_values(['Project', 'Location']), 
            use_container_width=True, 
            hide_index=True
        )

    # --- TAB: AS-BUILT ---
    with tab_built:
        asbuilt = primary_meta.get('AsBuiltFile')
        if pd.notnull(asbuilt):
            st.image(f"assets/asbuilts/{asbuilt}", caption="Official As-Built Plan")
        else:
            st.info("As-built documentation is being finalized.")
