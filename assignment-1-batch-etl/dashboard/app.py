"""Single-page Streamlit dashboard for the NYC taxi star schema.

    streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # allow `streamlit run dashboard/app.py`

from dashboard import queries  # noqa: E402
from etl.config import get_settings  # noqa: E402

PRIMARY = "#2a78d6"   # single-series colour
MUTED = "#b4b2a9"     # de-emphasised marks
HEAT_RANGE = ["#cde2fb", "#6da7ec", "#256abf", "#0d366b"]  # one-hue sequential ramp

st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")


@st.cache_resource
def get_engine() -> Engine:
    return create_engine(get_settings().sqlalchemy_url, pool_pre_ping=True)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def to_key(d: date) -> int:
    return d.year * 10_000 + d.month * 100 + d.day


def from_key(key: int) -> date:
    return date(key // 10_000, key // 100 % 100, key % 100)


def sidebar_filters() -> dict:
    bounds = run_query(queries.DATE_BOUNDS).iloc[0]
    if pd.isna(bounds["min_key"]):
        st.info("No trips loaded yet. Run `python -m flows.taxi_etl_flow run` first.")
        st.stop()

    min_date, max_date = from_key(int(bounds["min_key"])), from_key(int(bounds["max_key"]))
    st.sidebar.header("Filters")
    picked = st.sidebar.date_input(
        "Pickup date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
    )
    start, end = picked if isinstance(picked, tuple) and len(picked) == 2 else (min_date, max_date)
    boroughs = ["All", *run_query(queries.BOROUGHS)["borough"].tolist()]
    borough = st.sidebar.selectbox("Pickup borough", boroughs)
    st.sidebar.caption("Fare per mile is distance-weighted: SUM(fare) / SUM(miles).")
    return {"start_key": to_key(start), "end_key": to_key(end), "borough": borough}


def kpi_row(params: dict) -> None:
    k = run_query(queries.KPIS, **params).iloc[0]
    cols = st.columns(5)
    cols[0].metric("Trips", f"{int(k['trips']):,}")
    cols[1].metric("Revenue", f"${float(k['revenue']):,.0f}")
    cols[2].metric("Avg fare / mile", f"${float(k['fare_per_mile'] or 0):.2f}")
    cols[3].metric("Avg trip", f"{float(k['avg_distance'] or 0):.1f} mi · {float(k['avg_duration'] or 0):.0f} min")
    cols[4].metric("Card tip rate", f"{float(k['card_tip_rate'] or 0):.1%}")


def fare_per_mile_chart(params: dict) -> None:
    st.subheader("Average fare per mile by pickup borough")
    df = run_query(queries.FARE_PER_MILE_BY_BOROUGH, **params)
    if df.empty:
        st.caption("No data for this selection.")
        return
    df["fare_per_mile"] = df["fare_per_mile"].astype(float)
    chart = (
        alt.Chart(df)
        .mark_bar(color=PRIMARY, cornerRadiusEnd=4, height={"band": 0.6})
        .encode(
            x=alt.X("fare_per_mile:Q", title="Fare per mile (USD)", axis=alt.Axis(format="$.2f", grid=True)),
            y=alt.Y("borough:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("borough:N", title="Borough"),
                alt.Tooltip("fare_per_mile:Q", title="Fare / mile", format="$.2f"),
                alt.Tooltip("trips:Q", title="Trips", format=","),
            ],
        )
    )
    labels = chart.mark_text(align="left", dx=4).encode(text=alt.Text("fare_per_mile:Q", format="$.2f"))
    st.altair_chart((chart + labels).properties(height=260), width="stretch")


def peak_hours_chart(params: dict) -> None:
    st.subheader("Peak hours (trips by pickup hour)")
    df = run_query(queries.TRIPS_BY_HOUR, **params)
    if df.empty:
        st.caption("No data for this selection.")
        return
    top3 = set(df.nlargest(3, "trips")["hour_of_day"])
    df["hour"] = df["hour_of_day"].map(lambda h: f"{h:02d}:00")
    df["peak"] = df["hour_of_day"].isin(top3).map({True: "Top 3 hours", False: "Other hours"})
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, width={"band": 0.8})
        .encode(
            x=alt.X("hour:O", title="Pickup hour", axis=alt.Axis(labelAngle=0, labelExpr="datum.index % 3 === 0 ? datum.label : ''")),
            y=alt.Y("trips:Q", title="Trips", axis=alt.Axis(format="~s")),
            color=alt.Color(
                "peak:N",
                scale=alt.Scale(domain=["Top 3 hours", "Other hours"], range=[PRIMARY, MUTED]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[alt.Tooltip("hour:O", title="Hour"), alt.Tooltip("trips:Q", title="Trips", format=",")],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, width="stretch")
    peaks = ", ".join(f"{h:02d}:00" for h in sorted(top3))
    st.caption(f"Busiest pickup hours: **{peaks}**")


def weekday_heatmap(params: dict) -> None:
    st.subheader("Average trips per day, by weekday and hour")
    df = run_query(queries.TRIPS_BY_WEEKDAY_HOUR, **params)
    if df.empty:
        st.caption("No data for this selection.")
        return
    df["avg_trips_per_day"] = df["avg_trips_per_day"].astype(float)
    day_order = df.sort_values("day_of_week")["day_name"].unique().tolist()
    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X("hour_of_day:O", title="Pickup hour", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("day_name:N", sort=day_order, title=None),
            color=alt.Color(
                "avg_trips_per_day:Q",
                title="Trips / day",
                scale=alt.Scale(range=HEAT_RANGE),
                legend=alt.Legend(orient="right", format="~s"),
            ),
            tooltip=[
                alt.Tooltip("day_name:N", title="Day"),
                alt.Tooltip("hour_of_day:O", title="Hour"),
                alt.Tooltip("avg_trips_per_day:Q", title="Avg trips / day", format=",.0f"),
            ],
        )
        .properties(height=240)
    )
    st.altair_chart(chart, width="stretch")


def revenue_by_payment_chart(params: dict) -> None:
    st.subheader("Total revenue by payment type")
    df = run_query(queries.REVENUE_BY_PAYMENT_TYPE, **params)
    if df.empty:
        st.caption("No data for this selection.")
        return
    df["revenue"] = df["revenue"].astype(float)
    df["share"] = df["revenue"] / df["revenue"].sum()
    df["label"] = df.apply(lambda r: f"${r['revenue'] / 1e6:,.2f}M · {r['share']:.1%}", axis=1)
    chart = (
        alt.Chart(df)
        .mark_bar(color=PRIMARY, cornerRadiusEnd=4, height={"band": 0.6})
        .encode(
            x=alt.X("revenue:Q", title="Revenue (USD)", axis=alt.Axis(format="$~s")),
            y=alt.Y("payment_type_desc:N", sort="-x", title=None),
            tooltip=[
                alt.Tooltip("payment_type_desc:N", title="Payment type"),
                alt.Tooltip("revenue:Q", title="Revenue", format="$,.0f"),
                alt.Tooltip("share:Q", title="Share", format=".1%"),
                alt.Tooltip("trips:Q", title="Trips", format=","),
            ],
        )
    )
    labels = chart.mark_text(align="left", dx=4).encode(text="label:N")
    st.altair_chart((chart + labels).properties(height=260), width="stretch")
    with st.expander("Table view"):
        st.dataframe(
            df[["payment_type_desc", "trips", "revenue", "share"]],
            hide_index=True,
            column_config={
                "payment_type_desc": "Payment type",
                "trips": st.column_config.NumberColumn("Trips", format="%d"),
                "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                "share": st.column_config.ProgressColumn("Share", min_value=0.0, max_value=1.0, format="%.3f"),
            },
        )


def pipeline_health() -> None:
    st.subheader("Pipeline runs")
    runs = run_query(queries.PIPELINE_RUNS)
    if runs.empty:
        st.caption("No pipeline runs recorded yet.")
        return
    latest = runs.iloc[0]
    icon = {"success": "✅", "failed": "❌", "running": "⏳"}.get(latest["status"], "•")
    st.markdown(f"Latest run: {icon} **{latest['status']}** · `{latest['source_file']}` · started {latest['started_at']:%Y-%m-%d %H:%M}")
    st.dataframe(runs, hide_index=True, width="stretch")


def main() -> None:
    st.title("🚕 NYC Yellow Taxi — Trip Analytics")
    try:
        params = sidebar_filters()
        st.caption(
            f"Pickups {from_key(params['start_key'])} → {from_key(params['end_key'])}"
            f" · borough: {params['borough']}"
        )
        kpi_row(params)
        st.divider()

        left, right = st.columns(2)
        with left:
            fare_per_mile_chart(params)
        with right:
            revenue_by_payment_chart(params)

        peak_hours_chart(params)
        weekday_heatmap(params)
        st.divider()
        pipeline_health()
    except SQLAlchemyError as exc:
        st.error(f"Database error: {exc.__class__.__name__}. Is PostgreSQL running (`docker compose up -d`)?")
        st.exception(exc)


main()
