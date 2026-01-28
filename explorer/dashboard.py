import streamlit as st
import sqlite3
import pandas as pd
import json
from pathlib import Path

# Page Config
st.set_page_config(
    page_title="Wow Agent - Explorer Dashboard",
    page_icon="🚀",
    layout="wide"
)

# --- Constants & Setup ---
DB_NAME = "product_memory.db"
DB_PATH = Path(__file__).resolve().parent / "store" / DB_NAME

# --- Helpers ---
@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

def load_runs():
    conn = get_connection()
    df = pd.read_sql("SELECT run_id, timestamp, unique_advertisers, raw_count FROM runs ORDER BY timestamp DESC", conn)
    return df

def load_winners(run_id, limit=50):
    conn = get_connection()
    query = f"""
    SELECT
      p.product_id,
      p.canonical_name,
      p.category,
      p.subcategory,
      p.candidate_score,
      o.ads_count,
      o.advertisers_count,
      o.avg_confidence,
      p.signals_json,
      p.rationale_json,
      p.first_seen_at
    FROM product_concepts p
    JOIN product_observations o ON o.product_id = p.product_id
    WHERE o.run_id = '{run_id}'
      AND p.product_id <> 'unknown_cluster'
    ORDER BY p.candidate_score DESC, o.advertisers_count DESC, o.ads_count DESC
    LIMIT {limit};
    """
    df = pd.read_sql(query, conn)
    return df

# --- UI Layout ---

st.title("🚀 Wow Explorer Results")

# Sidebar: Run Selection
st.sidebar.header("Configuration")
runs_df = load_runs()

if runs_df.empty:
    st.error("No Data found in DB.")
    st.stop()

selected_run_id = st.sidebar.selectbox(
    "Select Run ID", 
    options=runs_df["run_id"],
    index=0
)

# Metrics for selected Run
run_meta = runs_df[runs_df["run_id"] == selected_run_id].iloc[0]
c1, c2, c3 = st.columns(3)
c1.metric("Run ID", run_meta["run_id"])
c2.metric("Total Ads Scraped", run_meta["raw_count"])
c3.metric("Unique Advertisers", run_meta["unique_advertisers"])

st.markdown("---")

# Load Data
df = load_winners(selected_run_id, limit=100)

if df.empty:
    st.info("No winners found for this run.")
    st.stop()

# --- Main Table ---
st.subheader("🏆 Top Winning Candidates")

# Filter by category
categories = ["All"] + sorted(df["category"].unique().tolist())
cat_filter = st.sidebar.selectbox("Filter Category", categories)

if cat_filter != "All":
    df_show = df[df["category"] == cat_filter]
else:
    df_show = df

# Prepare display dataframe
display_cols = ["canonical_name", "category", "subcategory", "candidate_score", "advertisers_count", "ads_count", "avg_confidence"]
st.dataframe(
    df_show[display_cols].style.background_gradient(subset=["candidate_score", "advertisers_count"], cmap="Greens"),
    use_container_width=True,
    height=400
)

# --- Drill Down ---
st.markdown("---")
st.subheader("🔍 Deep Dive: Product Details")

selected_product_name = st.selectbox("Choose a product to analyze:", df_show["canonical_name"])

if selected_product_name:
    row = df_show[df_show["canonical_name"] == selected_product_name].iloc[0]
    
    c1, c2 = st.columns([1, 2])
    
    with c1:
        st.info(f"**Score**: {row['candidate_score']}")
        st.write(f"**Category**: {row['category']} > {row['subcategory']}")
        st.write(f"**Advertisers**: {row['advertisers_count']}")
        st.write(f"**Total Ads**: {row['ads_count']}")
        
        # Signals
        signals = json.loads(row["signals_json"] or "{}")
        st.write("**Signals Detected:**")
        for k, v in signals.items():
            if v:
                st.write(f"- ✅ {k.replace('_', ' ').title()}")
    
    with c2:
        st.write("**Why is this a winner? (Rationale)**")
        rationale = json.loads(row["rationale_json"] or "{}")
        
        # Reasons
        reasons = rationale.get("reasons", [])
        if reasons:
            st.success("Matched Criteria: " + ", ".join(reasons))
            
        # Evidence Spans
        evidence = rationale.get("evidence", {})
        if evidence:
            st.write("**Evidence found in ads:**")
            for k, texts in evidence.items():
                st.caption(f"_{k}_")
                for t in texts:
                    st.code(t, language="text")

