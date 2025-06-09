import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

import plotly.graph_objects as go
import polars as pl
import psycopg2

# Page config
st.set_page_config(page_title="Data Quality Dashboard", layout="wide")
st.markdown("<h1 style='text-align: center;'>📊 Data Quality Dashboard</h1>", unsafe_allow_html=True)

st.markdown("---")


@st.cache_data
def fetch_data_from_postgres(
    host="localhost",
    port=5432,
    dbname="mydb",
    user="myuser",
    password="mypassword",
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


def extract_unique_parameter(df, column):
    parameter = df[column].unique().to_list()
    if "all" not in parameter:
        parameter.insert(0, "all")
    return parameter


def get_filtered_data(
    df: pl.DataFrame,
    data_source: str = "all",
    table_name: str = "all",
    outcome: str = "all",
    timestamp: str = "all",
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

# Connect to PostgreSQL and fetch data
df = fetch_data_from_postgres(table="soda_checks.data_quality_checks")

schema_choice = extract_unique_parameter(df, column="data_source")
table_choice = extract_unique_parameter(df, column="table_name")
timestamp_choice = extract_unique_parameter(df, column="timestamp")
outcome_choice = extract_unique_parameter(df, column="outcome")





failures_by_table = pd.DataFrame({
    "Table": ["orders", "users", "products", "transactions", "customers"],
    "Failures": [40, 30, 22, 15, 10]
})

failures_by_check = pd.DataFrame({
    "Check Type": ["Null Check", "Range Check", "Format Check"],
    "Failures": [50, 40, 33]
})

trend_data = pd.DataFrame({
    "Date": pd.date_range(start="2024-04-01", periods=21, freq="D"),
    "Failed Checks": [25, 27, 30, 32, 35, 28, 20, 22, 25, 33, 36, 40, 38, 29, 22, 24, 28, 35, 30, 33, 29]
})

latest_checks = pd.DataFrame({
    "Data Source": ["db1", "db1", "db2", "db1", "db2", "db1", "db2"],
    "Table Name": ["orders", "orders", "products", "products", "products", "products", "products"],
    "Check Name": ["Null Check", "Range Check", "Null Check", "Format Check", "Format Check", "Range Check", "Format Check"],
    "Column Name": ["orisomer.id", "name", "price", "name", "Widget", "name", "N/A"],
    "Outcome": ["FAIL", "PASS", "FAIL", "PASS", "FAIL", "PASS", "FAIL"],
    "Timestamp": [
        "2024-04-21 12:30", "2024-04-21 12:20", "2024-04-21 10:50",
        "2024-04-21 10:10", "2024-04-21 10:45", "2024-04-21 12:50", "2024-04-21 08:45"
    ]
})


schema, table, timestamp, outcome = st.columns(4)
with schema:
    schema_choice = st.selectbox("Schema", schema_choice, placeholder='all')

with table:
    table_choice = st.selectbox("Table", table_choice)

with timestamp:
    timestamp_choice = st.selectbox("Timestamp", timestamp_choice)

with outcome:
    outcome_choice = st.selectbox("Outcome", outcome_choice)

st.markdown("---")

st.subheader("Overall Quality Summary")

# Summary cards
# Get summary with filters
filtered_table = get_filtered_data(
    df,
    data_source=schema_choice,
    table_name=table_choice,
    outcome=outcome_choice,
    timestamp=timestamp_choice
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
    st.subheader("Failures by Check Type")
    fig_check = px.pie(failures_by_check, values="Failures", names="Check Type", height=350)
    st.plotly_chart(fig_check, use_container_width=True)

st.subheader("Failure Trend Over Time")
fig_trend = px.line(trend_data, x="Date", y="Failed Checks", markers=True)
st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("Latest Check Results")
st.dataframe(latest_checks, use_container_width=True)
