"""Phase 3: Streamlit dashboard, reading from the FastAPI service (not the
database directly) -- keeps the dashboard as just another API client, the
same way a real frontend would be, rather than coupling it to the schema.

Run with: streamlit run dashboard/app.py
(requires `uvicorn api.main:app --port 8000` running in another terminal)

Color/chart choices follow a sequential-magnitude palette (single blue hue,
#2a78d6) since every chart here answers "how much/how many", not "which
distinct category" -- see the dataviz method this was built against. One
honest scope note: the full light/dark CSS-token system that method
describes is meant for hand-authored HTML pages; Streamlit owns its own
theming layer, so colors here are fixed rather than theme-reactive. Fine
for an internal analytics tool; would need revisiting for a
customer-facing/branded version.
"""
import os

import altair as alt
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

BLUE = "#2a78d6"
GRIDLINE = "#e1e0d9"
INK_SECONDARY = "#52514e"

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud/Anomaly Detection Dashboard")


def api_get(path: str, **params):
    try:
        resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Couldn't reach the API at {API_BASE_URL}{path} -- is `uvicorn api.main:app --port 8000` running? ({e})")
        st.stop()


if st.button("Refresh"):
    st.rerun()

# --- KPI row -----------------------------------------------------------
summary = api_get("/stats/summary")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total transactions", f"{summary['total_transactions']:,}")
col2.metric("Flagged", f"{summary['total_flagged']:,}")
col3.metric("Flag rate", f"{summary['flag_rate']:.1%}")
col4.metric("Avg. severity score (flagged)", f"{summary['avg_flagged_score']:.2f}")

st.divider()

# --- Flag volume over time ---------------------------------------------
st.subheader("Flagged transactions over time")
window_minutes = st.select_slider(
    "Window", options=[30, 60, 120, 240, 480], value=120, format_func=lambda m: f"last {m} min"
)
timeseries = api_get("/stats/timeseries", window_minutes=window_minutes, bucket_minutes=max(1, window_minutes // 24))

if timeseries:
    df = pd.DataFrame(timeseries)
    df["bucket_start"] = pd.to_datetime(df["bucket_start"])

    line = (
        alt.Chart(df)
        .mark_line(color=BLUE, strokeWidth=2, point=alt.OverlayMarkDef(color=BLUE, size=40))
        .encode(
            x=alt.X("bucket_start:T", title=None, axis=alt.Axis(gridColor=GRIDLINE)),
            y=alt.Y("flagged_count:Q", title="Flagged transactions", axis=alt.Axis(gridColor=GRIDLINE)),
            tooltip=[
                alt.Tooltip("bucket_start:T", title="Time"),
                alt.Tooltip("flagged_count:Q", title="Flagged"),
                alt.Tooltip("total_count:Q", title="Total"),
            ],
        )
        .properties(height=280)
        .configure_axis(labelColor=INK_SECONDARY, titleColor=INK_SECONDARY)
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(line, use_container_width=True)
else:
    st.info("No transactions in this window yet.")

st.divider()

# --- Top triggered rules -------------------------------------------------
st.subheader("Most frequently triggered rules")
top_reasons = api_get("/stats/top-reasons", limit=5)

if top_reasons:
    df_reasons = pd.DataFrame(top_reasons)
    bar = (
        alt.Chart(df_reasons)
        .mark_bar(color=BLUE, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y("rule_type:N", sort="-x", title=None),
            x=alt.X("count:Q", title="Times triggered", axis=alt.Axis(gridColor=GRIDLINE)),
            tooltip=[alt.Tooltip("rule_type:N", title="Rule"), alt.Tooltip("count:Q", title="Count")],
        )
        .properties(height=140)
    )
    labels = bar.mark_text(align="left", dx=4, color="#0b0b0b").encode(text="count:Q")
    st.altair_chart(
        (bar + labels).configure_axis(labelColor=INK_SECONDARY, titleColor=INK_SECONDARY).configure_view(strokeWidth=0),
        use_container_width=True,
    )
else:
    st.info("No flagged events yet.")

st.divider()

# --- Recent flagged events table -----------------------------------------
st.subheader("Recent flagged events")
events = api_get("/flagged-events", limit=50)

if events:
    df_events = pd.DataFrame(events)
    df_events["flagged_at"] = pd.to_datetime(df_events["flagged_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_events["amount"] = df_events["amount"].map(lambda a: f"${a:,.2f}")
    df_events["rule_types"] = df_events["rule_types"].map(lambda rs: ", ".join(rs))
    st.dataframe(
        df_events[["flagged_at", "user_id", "amount", "merchant_category", "score", "rule_types", "reason"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No flagged events yet -- run the generator and consumer for a few minutes.")
