"""
03_Lifecycle.py — Presence interval Gantt and reappearing issues.

The Gantt shows one bar per interval per issue. Open intervals extend to
the latest CL in the project. Colour distinguishes open vs closed.
"""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))
import db

st.set_page_config(page_title="Lifecycle", layout="wide")
st.title("Issue Lifecycle")

# ── Project selector ──────────────────────────────────────────────────────────
project_id = st.sidebar.selectbox("Project", db.projects())

# ── Presence interval Gantt ───────────────────────────────────────────────────
st.subheader("Presence intervals")
st.caption(
    "Each bar represents one continuous run of CLs in which an issue was present. "
    "Green = still open. Red = closed. Gaps between bars for the same issue = the issue disappeared and reappeared."
)

intervals = db.presence_intervals(project_id)

if not intervals.empty:
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        issue_types = ["All"] + sorted(intervals["issue_type"].unique().tolist())
        selected_type = st.selectbox("Filter by issue type", issue_types)
    with col2:
        status_filter = st.radio("Status", ["All", "open", "closed"], horizontal=True)

    filtered = intervals.copy()
    if selected_type != "All":
        filtered = filtered[filtered["issue_type"] == selected_type]
    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]

    # Limit rows for readability
    max_issues = st.slider("Max issues to display", 20, 200, 60, step=20)
    top_issues = filtered["issue_instance_id"].value_counts().head(max_issues).index
    filtered = filtered[filtered["issue_instance_id"].isin(top_issues)]

    if not filtered.empty:
        filtered = filtered.copy()
        filtered["duration"] = filtered["end_cl"] - filtered["start_cl"]

        fig = go.Figure()

        color_map = {"open": "#00CC96", "closed": "#EF553B"}

        for status, grp in filtered.groupby("status"):
            fig.add_trace(go.Bar(
                name=status,
                x=grp["duration"],
                y=grp["label"],
                base=grp["start_cl"],
                orientation="h",
                marker_color=color_map[status],
                opacity=0.75,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Start CL: %{base}<br>"
                    "End CL: %{x}<br>"
                    "<extra>" + status + "</extra>"
                ),
            ))

        fig.update_layout(
            barmode="overlay",
            title=f"{project_id} — issue presence intervals",
            xaxis_title="CL number",
            yaxis_title="Issue",
            height=max(400, len(filtered["label"].unique()) * 20),
            legend=dict(orientation="h", yanchor="bottom", y=1.01),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No intervals match the current filters.")
else:
    st.warning("No presence interval data found. Ensure pipeline step 04 has been run.")

st.divider()

# ── Reappearing issues ────────────────────────────────────────────────────────
st.subheader("Reappearing issues")
st.caption(
    "Issues with more than one presence interval — they resolved and came back. "
    "Higher interval count = more cycles."
)

recurring = db.recurring_issues(project_id)

if not recurring.empty:
    col_a, col_b = st.columns([3, 2])

    with col_a:
        fig2 = px.bar(
            recurring,
            x="interval_count",
            y="issue_pattern",
            color="issue_type",
            orientation="h",
            title="Open/close cycles per issue",
            text_auto=True,
        )
        fig2.update_layout(
            height=max(400, len(recurring) * 28),
            yaxis=dict(autorange="reversed"),
            showlegend=True,
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_b:
        st.markdown("**Summary**")
        summary = (
            recurring.groupby("issue_type")["interval_count"]
            .agg(issues="count", total_cycles="sum", avg_cycles="mean")
            .reset_index()
            .round({"avg_cycles": 1})
        )
        st.dataframe(summary, use_container_width=True, hide_index=True)

        st.markdown("**Top recurring issues**")
        st.dataframe(
            recurring[["issue_type", "issue_pattern", "interval_count", "first_seen_cl"]]
            .head(10),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("No recurring issues found (no issue has appeared more than once).")
