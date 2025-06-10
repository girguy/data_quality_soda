import streamlit as st
import plotly.express as px

import polars as pl
import psycopg2

from config import (
    DB_NAME, USERNAME, PASSWORD, TABLE_NAME,
    SCHEMA_OPTIONS, OUTCOME_OPTIONS, TIMESTAMP_OPTIONS, TABLE_OPTIONS)


# Page config
st.set_page_config(page_title="Data Quality Dashboard", layout="wide")
st.markdown("<h1 style='text-align: center;'>📊 Data Quality Dashboard</h1>", unsafe_allow_html=True)

st.markdown("---")


@st.cache_resource
def fetch_data_from_postgres(
    host="localhost",
    port=5438,
    dbname=DB_NAME,
    user=USERNAME,
    password=PASSWORD,
    table=None
    
):
    query=f"SELECT * FROM {table}"
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    try:
        df = pl.read_database(query, connection=conn)
        df = df.with_columns(
            pl.col("timestamp").cast(pl.Utf8)
        )
    finally:
        conn.close()
    return df


def get_filtered_data(
    df: pl.DataFrame,
    data_source: str,
    table_name: str,
    outcome: str,
    timestamp: str,
) -> dict:
    # Step 1: Apply filters
    filters = []

    if data_source.lower() != "all":
        filters.append(df["data_source"] == data_source)

    if table_name.lower() != "all":
        filters.append(df["table_name"] == table_name)

    if outcome.lower() != "all":
        filters.append(df["outcome"] == outcome)
    
    if timestamp.lower() != "all":
        filters.append(df["timestamp"] == timestamp)
        

    # Apply combined filters
    if filters:
        # Combine all filter expressions with logical AND
        mask = filters[0]
        for f in filters[1:]:
            mask = mask & f
        df = df.filter(mask)
    return df


def get_summary_data(df: pl.DataFrame) -> dict:
    # Step 2: Compute summary
    total_checks = df.height
    total_failed = df.filter(pl.col("outcome") == "fail").height

    failure_rate = f"{(total_failed / total_checks * 100):.1f}%" if total_checks > 0 else "0.0%"

    return {
        "Total Checks": total_checks,
        "Total Failed": total_failed,
        "Failure Rate": failure_rate
    }


def get_failures_by_table(table):
    return (
        table
        .filter(pl.col("outcome") == "fail")
        .group_by("table_name")
        .agg(pl.len().alias("Failures"))
        .sort("Failures", descending=True)
        .rename({"table_name": "Table"})
    )


def get_failures_by_check_type(table):
    return (
        table
        .filter(pl.col("outcome") == "fail")
        .group_by("check_name")
        .agg(pl.len().alias("Failures"))
        .sort("Failures", descending=True)
        .rename({"check_name": "Check Type"})
    )

def get_trend_failures_by_date(table):
    return (
        table
        .filter(pl.col("outcome") == "fail")
        .group_by("timestamp")
        .agg(pl.len().alias("Failed Checks"))
        .sort("timestamp")
        .rename({"timestamp": "Date"})
    )


# Connect to PostgreSQL and fetch data
df = fetch_data_from_postgres(table=f"soda_checks.{TABLE_NAME}")

schema, table, timestamp, outcome = st.columns(4)
with schema:
    schema_choice = st.selectbox("Schema", SCHEMA_OPTIONS)

with table:
    table_choice = st.selectbox("Table", TABLE_OPTIONS)

with timestamp:
    timestamp_choice = st.selectbox("Timestamp", TIMESTAMP_OPTIONS)

with outcome:
    outcome_choice = st.selectbox("Outcome", OUTCOME_OPTIONS)

st.markdown("---")

st.subheader("Overall Quality Summary")

# Summary cards
# Get summary with filters
filtered_table = get_filtered_data(
    df,
    data_source = schema_choice,
    table_name = table_choice,
    outcome = outcome_choice,
    timestamp = timestamp_choice
)

summary_data = get_summary_data(filtered_table)

col1, col2, col3 = st.columns(3)
col1.metric("Total Checks", summary_data["Total Checks"], border=True)
col2.metric("Total Failed", summary_data["Total Failed"], border=True)
col3.metric("Failure Rate", summary_data["Failure Rate"], border=True)

st.markdown("\n")


# Charts
col4, col5 = st.columns(2)

with col4:
    failures_by_table = get_failures_by_table(filtered_table)
    st.subheader("Failures by Table")
    fig_table = px.bar(failures_by_table, x="Failures", y="Table", orientation="h", color="Table", height=350)
    st.plotly_chart(fig_table, use_container_width=True, showlegend=True)

with col5:
    failures_by_check = get_failures_by_check_type(filtered_table)
    st.subheader("Failures by Check Type")
    fig_check = px.pie(failures_by_check, values="Failures", names="Check Type", height=350)
    st.plotly_chart(fig_check, use_container_width=True)

st.subheader("Failure Trend Over Time")
trend_data = get_trend_failures_by_date(filtered_table)
fig_trend = px.line(trend_data, x="Date", y="Failed Checks", markers=True)
st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("Latest Check Results")
filtered_table = filtered_table \
    .select(["data_source", "table_name", "check_name", "column_name", "outcome", "timestamp"]) \
    .rename({"data_source": "Data Source"}) \
    .rename({"check_name": "Check Name"}) \
    .rename({"column_name": "Column Name"}) \
    .rename({"outcome": "Outcome"}) \
    .rename({"timestamp": "Timestamp"}) \
    .rename({"table_name": "Table Name"})

st.dataframe(filtered_table, use_container_width=True)
