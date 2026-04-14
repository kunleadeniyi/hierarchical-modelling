"""
01_Overview.py — KPI cards, CL trend, issue type distribution.
"""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))
import db

st.set_page_config(page_title="Overview", layout="wide")
st.title("Overview")

# ── Project selector ──────────────────────────────────────────────────────────
project_id = st.sidebar.selectbox("Project", db.projects())

# ── KPI cards ─────────────────────────────────────────────────────────────────
kpis = db.kpis(project_id)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Issues",   f"{kpis['total_issues']:,}")
c2.metric("Changelists",    f"{kpis['total_cls']:,}")
c3.metric("Open Intervals", f"{kpis['open_issues']:,}")
c4.metric("Closed Intervals", f"{kpis['closed_issues']:,}")

st.divider()

# ── CL trend ──────────────────────────────────────────────────────────────────
st.subheader("Issue trend per changelist")

trend = db.cl_trend(project_id)

col_left, col_right = st.columns(2)

with col_left:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend["cl_number"], y=trend["total_issues"],
        name="Total", mode="lines", line=dict(color="#636EFA"),
    ))
    fig.add_trace(go.Scatter(
        x=trend["cl_number"], y=trend["new_issues"],
        name="New", mode="lines", line=dict(color="#00CC96"),
    ))
    fig.add_trace(go.Scatter(
        x=trend["cl_number"], y=trend["resolved_issues"],
        name="Resolved", mode="lines", line=dict(color="#EF553B"),
    ))
    fig.update_layout(
        title="New / Resolved / Total per CL",
        xaxis_title="CL number",
        yaxis_title="Issues",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=380,
        xaxis=dict(
            type="category",
            tickmode="auto",
            nticks=20,
            tickangle=-45,
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=trend["cl_number"], y=trend["running_open"],
        name="Running open", mode="lines",
        fill="tozeroy",
        line=dict(color="#AB63FA"),
    ))
    fig2.update_layout(
        title="Cumulative open issues",
        xaxis_title="CL number",
        yaxis_title="Open issues",
        height=380,
        xaxis=dict(
            type="category",
            tickmode="auto",
            nticks=20,
            tickangle=-45,
        ),
        yaxis=dict(rangemode="tozero"),
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Issue type distribution ───────────────────────────────────────────────────
st.subheader("Issue type distribution")

dist = db.issue_type_dist(project_id)

col_a, col_b = st.columns(2)

with col_a:
    fig3 = px.pie(
        dist,
        names="issue_type",
        values="issue_count",
        title="Share by issue type",
        hole=0.4,
    )
    fig3.update_layout(height=380)
    st.plotly_chart(fig3, use_container_width=True)

with col_b:
    fig4 = px.bar(
        dist,
        x="issue_type",
        y="issue_count",
        title="Count by issue type",
        color="issue_type",
        text_auto=True,
    )
    fig4.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig4, use_container_width=True)
