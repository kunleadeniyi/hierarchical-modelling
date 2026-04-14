"""
app.py — Hierarchical Issue Tracker dashboard entry point.

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    streamlit run dashboard/app.py
"""
import streamlit as st

st.set_page_config(
    page_title="Issue Tracker",
    page_icon="📊",
    layout="wide",
)

st.title("Hierarchical Issue Tracker")
st.markdown(
    """
A data modelling showcase built on PostgreSQL.
Use the sidebar to navigate between views.

| Page | What it shows |
|---|---|
| **Overview** | KPI cards, new/resolved trend, issue type distribution |
| **Hierarchy** | Animated treemap and heatmap — hierarchy drill-down over CL numbers |
| **Lifecycle** | Presence interval Gantt and reappearing issues |
"""
)

st.info(
    "**Prerequisite:** ensure `PG_DSN` is set and the pipeline has been run "
    "(`python3 pipeline/run_all.py`) before navigating to any page."
)
