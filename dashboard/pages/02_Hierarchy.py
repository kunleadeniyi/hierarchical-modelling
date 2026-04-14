"""
02_Hierarchy.py — Hierarchy drill-down over CL numbers.

px.treemap / icicle / sunburst do not support animation_frame.
Step-through is handled by a Streamlit slider — each CL selection
re-renders the chart for that snapshot.
"""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))
import db

st.set_page_config(page_title="Hierarchy", layout="wide")
st.title("Hierarchy — drill-down over time")

# ── Project selector ──────────────────────────────────────────────────────────
project_id = st.sidebar.selectbox("Project", db.projects())

df = db.treemap_data(project_id)

if df.empty:
    st.warning("No hierarchy data found. Ensure the pipeline and 04_views.sql have been run.")
    st.stop()

cl_options = sorted(df["cl_number"].unique().tolist())

# ── Hierarchy chart at a chosen CL ───────────────────────────────────────────
st.subheader("Hierarchy at a chosen CL")
st.caption(
    "Drag the slider to step through changelists. "
    "Tile sizes reflect the issue count at each CL. Click tiles to drill down."
)

col_ctrl1, col_ctrl2 = st.columns([3, 1])
with col_ctrl1:
    selected_cl = st.select_slider("CL number", options=cl_options)
with col_ctrl2:
    chart_type = st.radio("Chart type", ["Treemap", "Icicle", "Sunburst"], horizontal=False)

snapshot = df[df["cl_number"] == selected_cl]

chart_args = dict(
    data_frame=snapshot,
    path=[px.Constant("All"), "level_1", "level_2", "level_3"],
    values="issue_count",
    color="issue_count",
    color_continuous_scale="Reds",
    title=f"{project_id} — CL {selected_cl}",
)

if chart_type == "Treemap":
    fig = px.treemap(**chart_args)
elif chart_type == "Icicle":
    fig = px.icicle(**chart_args)
else:
    fig = px.sunburst(**chart_args)

fig.update_layout(height=560, margin=dict(t=50, l=10, r=10, b=10))
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── CL comparison — side by side ─────────────────────────────────────────────
st.subheader("Compare two CLs")

c1, c2 = st.columns(2)
with c1:
    cl_a = st.selectbox("CL (left)", cl_options, index=0)
with c2:
    cl_b = st.selectbox("CL (right)", cl_options, index=min(len(cl_options) - 1, 10))

snap_a = df[df["cl_number"] == cl_a]
snap_b = df[df["cl_number"] == cl_b]

fig_a = px.treemap(
    snap_a,
    path=[px.Constant("All"), "level_1", "level_2", "level_3"],
    values="issue_count",
    color="issue_count",
    color_continuous_scale="Blues",
    title=f"CL {cl_a}",
)
fig_b = px.treemap(
    snap_b,
    path=[px.Constant("All"), "level_1", "level_2", "level_3"],
    values="issue_count",
    color="issue_count",
    color_continuous_scale="Blues",
    title=f"CL {cl_b}",
)
fig_a.update_layout(height=420, margin=dict(t=40, l=5, r=5, b=5))
fig_b.update_layout(height=420, margin=dict(t=40, l=5, r=5, b=5))

c1.plotly_chart(fig_a, use_container_width=True)
c2.plotly_chart(fig_b, use_container_width=True)

st.divider()

# ── Team × CL heatmap ─────────────────────────────────────────────────────────
st.subheader("Team × CL heatmap")
st.caption(
    "Colour intensity = distinct issues at that (team, CL) intersection. "
    "Useful for spotting which teams were hot at which points in the timeline."
)

heat = db.team_heatmap_data(project_id)

if not heat.empty:
    pivoted = heat.pivot_table(
        index="team",
        columns="cl_number",
        values="issue_count",
        fill_value=0,
    )

    fig_heat = go.Figure(
        go.Heatmap(
            z=pivoted.values,
            x=pivoted.columns.tolist(),
            y=pivoted.index.tolist(),
            colorscale="Blues",
            hoverongaps=False,
            hovertemplate="CL: %{x}<br>Team: %{y}<br>Issues: %{z}<extra></extra>",
        )
    )
    fig_heat.update_layout(
        title=f"{project_id} — issue density by team per CL",
        xaxis_title="CL number",
        yaxis_title="Team",
        height=max(300, len(pivoted) * 60),
        xaxis=dict(type="category"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.warning("No heatmap data found. Check that path_closure is populated (pipeline step 00).")
