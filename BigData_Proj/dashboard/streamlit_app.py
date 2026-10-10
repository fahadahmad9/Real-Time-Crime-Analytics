import sys
sys.path.insert(0, '/app')

import streamlit as st
import pandas as pd
import psycopg2
import pymongo
import yaml
import folium
from streamlit_folium import st_folium
import matplotlib.pyplot as plt
from datetime import datetime

st.set_page_config(
    page_title="Chicago Crime Analytics",
    page_icon="🚨",
    layout="wide"
)

@st.cache_resource
def load_config():
    with open('config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

@st.cache_resource
def get_pg_conn(config):
    pg = config['postgresql']
    return psycopg2.connect(
        host=pg['host'], port=pg['port'],
        dbname=pg['database'], user=pg['user'],
        password=pg['password']
    )

@st.cache_resource
def get_mongo_db(config):
    mongo = config['mongodb']
    client = pymongo.MongoClient(mongo['host'], mongo['port'])
    return client[mongo['database']]

def query_pg(conn, sql):
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"DB error: {e}")
        return pd.DataFrame()

# --- Load config and connections ---
config = load_config()
pg_conn = get_pg_conn(config)
mongo_db = get_mongo_db(config)

# --- Header ---
st.title("🚨 Chicago Real-Time Crime Analytics")
st.markdown("Lambda Architecture — Batch + Stream Processing Dashboard")

# --- Sidebar ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", [
    "Live Alerts",
    "Crime Trends",
    "Arrest Rates",
    "Crime Hotspots",
    "Violence Stats",
    "Correlations"
])

# =====================
# PAGE: Live Alerts
# =====================
if page == "Live Alerts":
    st.header("🔴 Live Alerts (Storm Output)")

    col1, col2 = st.columns([2, 1])

    with col1:
        alerts_df = query_pg(pg_conn, """
            SELECT district, event_count, threshold, severity, timestamp
            FROM alerts
            ORDER BY timestamp DESC
            LIMIT 50
        """)

        if not alerts_df.empty:
            def color_severity(val):
                colors = {
                    'CRITICAL': 'background-color: #ff4444; color: white',
                    'HIGH':     'background-color: #ff8800; color: white',
                    'MEDIUM':   'background-color: #ffcc00; color: black'
                }
                return colors.get(val, '')

            styled = alerts_df.style.map(
                color_severity, subset=['severity']
            )
            st.dataframe(styled, use_container_width=True)
        else:
            st.info("No alerts yet. Start the Storm topology to generate alerts.")

    with col2:
        st.subheader("Alert Summary")
        if not alerts_df.empty:
            summary = alerts_df['severity'].value_counts()
            fig, ax = plt.subplots()
            colors = ['#ff4444', '#ff8800', '#ffcc00']
            ax.pie(summary.values, labels=summary.index,
                   colors=colors[:len(summary)], autopct='%1.1f%%')
            ax.set_title("Alerts by Severity")
            st.pyplot(fig)

    # MongoDB raw alerts
    st.subheader("MongoDB Alert Logs (Latest 10)")
    try:
        mongo_alerts = list(
            mongo_db[config['mongodb']['alerts_collection']]
            .find({}, {'_id': 0})
            .sort('timestamp', -1)
            .limit(10)
        )
        if mongo_alerts:
            st.dataframe(pd.DataFrame(mongo_alerts), use_container_width=True)
        else:
            st.info("No MongoDB alert logs yet.")
    except Exception as e:
        st.error(f"MongoDB error: {e}")

# =====================
# PAGE: Crime Trends
# =====================
elif page == "Crime Trends":
    st.header("📈 Crime Trends")

    trends_df = query_pg(pg_conn, """
        SELECT year, month, hour, day_of_week, SUM(crime_count) as crime_count
        FROM crime_trends
        GROUP BY year, month, hour, day_of_week
        ORDER BY year, month
    """)

    if not trends_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Crimes by Year")
            yearly = trends_df.groupby('year')['crime_count'].sum().reset_index()
            fig, ax = plt.subplots()
            ax.bar(yearly['year'], yearly['crime_count'], color='steelblue')
            ax.set_xlabel("Year")
            ax.set_ylabel("Crime Count")
            ax.set_title("Annual Crime Count")
            st.pyplot(fig)

        with col2:
            st.subheader("Crimes by Month")
            monthly = trends_df.groupby('month')['crime_count'].sum().reset_index()
            fig, ax = plt.subplots()
            ax.plot(monthly['month'], monthly['crime_count'],
                    marker='o', color='crimson')
            ax.set_xlabel("Month")
            ax.set_ylabel("Crime Count")
            ax.set_title("Monthly Crime Pattern")
            ax.set_xticks(range(1, 13))
            st.pyplot(fig)

        col3, col4 = st.columns(2)

        with col3:
            st.subheader("Crimes by Hour")
            hourly = trends_df.groupby('hour')['crime_count'].sum().reset_index()
            fig, ax = plt.subplots()
            ax.bar(hourly['hour'], hourly['crime_count'], color='darkorange')
            ax.set_xlabel("Hour of Day")
            ax.set_ylabel("Crime Count")
            ax.set_title("Hourly Crime Distribution")
            st.pyplot(fig)

        with col4:
            st.subheader("Crimes by Day of Week")
            days = {1:'Sun',2:'Mon',3:'Tue',4:'Wed',5:'Thu',6:'Fri',7:'Sat'}
            dow = trends_df.groupby('day_of_week')['crime_count'].sum().reset_index()
            dow['day_of_week'] = dow['day_of_week'].astype(int)
            dow['day_name'] = dow['day_of_week'].map(days)
            dow = dow.dropna(subset=['day_name'])
            fig, ax = plt.subplots()
            ax.bar(dow['day_name'], dow['crime_count'], color='purple')
            ax.set_xlabel("Day")
            ax.set_ylabel("Crime Count")
            ax.set_title("Day-of-Week Distribution")
            st.pyplot(fig)
    else:
        st.info("No trend data yet. Run the Spark batch job first.")

# =====================
# PAGE: Arrest Rates
# =====================
elif page == "Arrest Rates":
    st.header("⚖️ Arrest Rate Analysis")

    arrest_df = query_pg(pg_conn, """
        SELECT primary_type, district, arrest_rate, total_crimes, total_arrests
        FROM arrest_rates
        WHERE primary_type IS NOT NULL
        ORDER BY arrest_rate DESC
        LIMIT 20
    """)

    if not arrest_df.empty:
        st.subheader("Top 20 Crime Types by Arrest Rate")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(arrest_df['primary_type'], arrest_df['arrest_rate'],
                color='teal')
        ax.set_xlabel("Arrest Rate")
        ax.set_title("Arrest Rate by Crime Type")
        ax.invert_yaxis()
        st.pyplot(fig)

        st.subheader("Data Table")
        st.dataframe(arrest_df, use_container_width=True)
    else:
        st.info("No arrest rate data yet. Run the Spark batch job first.")

# =====================
# PAGE: Crime Hotspots
# =====================
elif page == "Crime Hotspots":
    st.header("🗺️ Crime Hotspots (K-Means Clusters)")

    hotspots_df = query_pg(pg_conn, """
        SELECT cluster_id, centroid_lat, centroid_lon, crime_count
        FROM hotspots
        ORDER BY crime_count DESC
    """)

    if not hotspots_df.empty:
        # Folium map centered on Chicago
        m = folium.Map(location=[41.8781, -87.6298], zoom_start=11)

        for _, row in hotspots_df.iterrows():
            radius = max(5, min(30, int(row['crime_count'] / 50)))
            folium.CircleMarker(
                location=[row['centroid_lat'], row['centroid_lon']],
                radius=radius,
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.6,
                popup=f"Cluster {int(row['cluster_id'])}: {int(row['crime_count'])} crimes"
            ).add_to(m)

        st_folium(m, width=800, height=500)

        st.subheader("Cluster Details")
        st.dataframe(hotspots_df, use_container_width=True)
    else:
        st.info("No hotspot data yet. Run the Spark ML job first.")

# =====================
# PAGE: Violence Stats
# =====================
elif page == "Violence Stats":
    st.header("🔫 Violence & Gunshot Analysis")

    violence_df = query_pg(pg_conn, """
        SELECT month, district, total_homicides, non_fatal_shootings, gunshot_injuries
        FROM violence_stats
        ORDER BY month
    """)

    if not violence_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Homicides vs Non-Fatal Shootings by Month")
            monthly = violence_df.groupby('month')[
                ['total_homicides', 'non_fatal_shootings']
            ].sum().reset_index()
            fig, ax = plt.subplots()
            ax.plot(monthly['month'], monthly['total_homicides'],
                    marker='o', label='Homicides', color='darkred')
            ax.plot(monthly['month'], monthly['non_fatal_shootings'],
                    marker='s', label='Non-Fatal', color='orange')
            ax.set_xlabel("Month")
            ax.set_ylabel("Count")
            ax.legend()
            ax.set_title("Violence by Month")
            st.pyplot(fig)

        with col2:
            st.subheader("Top Districts by Violence")
            by_district = violence_df.groupby('district')[
                ['total_homicides', 'non_fatal_shootings']
            ].sum().reset_index()
            by_district['total'] = (by_district['total_homicides'] +
                                    by_district['non_fatal_shootings'])
            by_district = by_district.sort_values('total', ascending=False).head(10)
            fig, ax = plt.subplots()
            ax.bar(by_district['district'], by_district['total'], color='darkred')
            ax.set_xlabel("District")
            ax.set_ylabel("Total Incidents")
            ax.set_title("Top 10 Districts by Violence")
            st.pyplot(fig)
    else:
        st.info("No violence data yet. Run the Spark batch job first.")

# =====================
# PAGE: Correlations
# =====================
elif page == "Correlations":
    st.header("📊 Cross-Dataset Correlations")

    corr_df = query_pg(pg_conn, """
        SELECT correlation_type, district, community_area,
               violence_rate, arrest_rate, sex_offender_density, crime_rate
        FROM correlations
    """)

    if not corr_df.empty:
        corr_types = corr_df['correlation_type'].unique()

        for ctype in corr_types:
            st.subheader(f"Correlation: {ctype.replace('_', ' ').title()}")
            subset = corr_df[corr_df['correlation_type'] == ctype].dropna(
                subset=['district']
            ).head(20)

            if not subset.empty:
                fig, ax = plt.subplots(figsize=(10, 4))
                plot_df = subset.dropna(subset=['arrest_rate', 'violence_rate'])
                if not plot_df.empty and 'violence_rate' in plot_df.columns:
                    ax.scatter(plot_df['arrest_rate'], plot_df['violence_rate'],
                               color='navy', alpha=0.7)
                    ax.set_xlabel("Arrest Rate")
                    ax.set_ylabel("Violence Rate")
                    ax.set_title(f"{ctype} by District")
                    for _, row in plot_df.iterrows():
                        if pd.notna(row.get('district')):
                            ax.annotate(str(row['district']),
                                       (row['arrest_rate'], row['violence_rate']),
                                       fontsize=7)
                else:
                    ax.text(0.5, 0.5, 'Insufficient data for plot',
                            ha='center', va='center', transform=ax.transAxes)
                st.pyplot(fig)

        st.subheader("Raw Correlation Data")
        st.dataframe(corr_df, use_container_width=True)
    else:
        st.info("No correlation data yet. Run the Spark batch job first.")

# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_resource.clear()
    st.rerun()
