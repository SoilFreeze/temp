import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# --- SECTION 1: CONFIG ---
TARGET_PROJECT = "2527"
PROJECT_ID = "sensorpush-export"
DATASET_ID = "Temperature"
# Ensure this matches your master table exactly
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.metadata" 
OVERRIDE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.manual_rejections"

# --- SECTION 2: ROBUST DATA ENGINE ---
@st.cache_data(ttl=600)
def get_universal_portal_data(project_id):
    if client is None: return pd.DataFrame()

    # We use a broad engineering-style filter first to ensure data loads
    # Then we apply the 'client' logic to hide MASKED data only.
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
        AND (UPPER(CAST(rej.approve AS STRING)) != 'FALSE' OR rej.approve IS NULL)
        AND r.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 84 DAY)
        ORDER BY r.timestamp ASC
    """
    try:
        df = client.query(query).to_dataframe()
        # Fix: Ensure Depth and Bank are strings to prevent labeling errors
        df['Depth'] = df['Depth'].astype(str).replace(['nan', 'None', '<NA>'], '')
        df['Bank'] = df['Bank'].astype(str).replace(['nan', 'None', '<NA>'], '')
        return df
    except Exception as e:
        st.error(f"BQ Error: {e}")
        return pd.DataFrame()

# --- SECTION 3: REPAIRED GRAPHING ENGINE ---
def build_high_speed_graph(df, title, start_view, end_view, display_tz):
    if df.empty: return go.Figure().update_layout(title="No data available.")

    pdf = df.copy()
    pdf['timestamp'] = pdf['timestamp'].dt.tz_convert(display_tz)
    
    # FORCED LABELING: Every sensor MUST have a name to be visible
    def create_label(r):
        b = str(r['Bank']).strip()
        d = str(r['Depth']).strip()
        if b: return f"Bank {b} ({r['NodeNum']})"
        if d: return f"{d}ft ({r['NodeNum']})"
        return f"Node {r['NodeNum']}"

    pdf['label'] = pdf.apply(create_label, axis=1)
    
    fig = go.Figure()
    for lbl in sorted(pdf['label'].unique()):
        s_df = pdf[pdf['label'] == lbl].sort_values('timestamp')
        
        # GAP DETECTION: Relaxed to 48h so sparse Elizabeth data doesn't "vanish"
        s_df['gap_hrs'] = s_df['timestamp'].diff().dt.total_seconds() / 3600
        gap_mask = s_df['gap_hrs'] > 48.0
        if gap_mask.any():
            gaps = s_df[gap_mask].copy()
            gaps['temperature'] = None
            gaps['timestamp'] = gaps['timestamp'] - pd.Timedelta(minutes=1)
            s_df = pd.concat([s_df, gaps]).sort_values('timestamp')

        # FIX: mode='lines+markers' ensures sparse points are visible
        # FIX: connectgaps=True bridges the invisible segments
        fig.add_trace(go.Scattergl(
            x=s_df['timestamp'], y=s_df['temperature'], 
            name=lbl, mode='lines+markers', 
            connectgaps=True, marker=dict(size=5)
        ))

    fig.update_layout(
        title=f"<b>{title}</b>", hovermode="x unified",
        xaxis=dict(range=[start_view, end_view], showline=True, mirror=True),
        yaxis=dict(title="°F", gridcolor='Gainsboro', showline=True, mirror=True, range=[-20, 80]),
        height=600, margin=dict(r=150),
        legend=dict(title="Sensors", orientation="v", x=1.02, y=1)
    )
    return fig

# --- SECTION 4: RENDER ---
st.title(f"📊 Project {TARGET_PROJECT} Status")
data = get_universal_portal_data(TARGET_PROJECT)

if not data.empty:
    locs = sorted(data['Location'].unique())
    for loc in locs:
        with st.expander(f"📍 {loc}", expanded=True):
            loc_df = data[data['Location'] == loc]
            # Use unique keys to prevent StreamlitDuplicateElementId
            fig = build_high_speed_graph(
                loc_df, f"{loc} Data", 
                pd.Timestamp.now(tz='UTC') - timedelta(weeks=4), 
                pd.Timestamp.now(tz='UTC'), "America/New_York"
            )
            st.plotly_chart(fig, use_container_width=True, key=f"graph_{loc}")
else:
    st.error("No data found. Check if NodeNums in 'metadata' match the raw tables.")
