import streamlit as st
import sqlite3
import pandas as pd
import json
import altair as alt
from pathlib import Path

# --- Page Config ---
st.set_page_config(
    page_title="Wow Explorer - PhD Dashboard",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
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

def get_market_overview(run_id):
    conn = get_connection()
    # Categories
    df_cat = pd.read_sql(f"""
        SELECT category, COUNT(*) as count 
        FROM product_concepts p
        JOIN product_observations o ON o.product_id = p.product_id
        WHERE o.run_id = '{run_id}'
        GROUP BY category
        ORDER BY count DESC
    """, conn)
    
    # Signals (Aggregated)
    # This is a bit heavier, usually pre-calculated is better, but let's do simple aggregation
    # We'll assume the 'signals' JSON structure is flat
    
    return df_cat

def load_winners(run_id, limit=200):
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
      p.first_seen_at,
      -- Aggregate search tags for filtering
      (
        SELECT GROUP_CONCAT(DISTINCT s._query_matched)
        FROM ad_to_product map
        JOIN ad_snapshots s ON s.run_id = map.run_id AND s.ad_id = map.ad_id
        WHERE map.product_id = p.product_id AND map.run_id = o.run_id
      ) as search_tags
    FROM product_concepts p
    JOIN product_observations o ON o.product_id = p.product_id
    WHERE o.run_id = '{run_id}'
      AND p.product_id <> 'unknown_cluster'
    ORDER BY p.candidate_score DESC, o.advertisers_count DESC, o.ads_count DESC
    LIMIT {limit};
    """
    df = pd.read_sql(query, conn)
    return df

def get_product_drilldown(run_id, product_id):
    """
    Fetches all ads related to this product to show:
    - Thumbnails
    - Landing Page Links
    - Advertiser Profiles
    """
    conn = get_connection()
    query = f"""
    SELECT 
        s.ad_id,
        s.title,
        s.body_text,
        s.link_url,
        s.cta_type,
        a.advertiser_id,
        a.current_page_name,
        a.current_profile_uri,
        (SELECT image_url FROM ad_media m WHERE m.ad_id = s.ad_id LIMIT 1) as image_url,
        s.observed_at
    FROM ad_to_product map
    JOIN ad_snapshots s ON s.run_id = map.run_id AND s.ad_id = map.ad_id
    JOIN advertisers a ON a.advertiser_id = map.advertiser_id
    WHERE map.run_id = '{run_id}'
      AND map.product_id = '{product_id}'
    ORDER BY s.observed_at DESC
    LIMIT 20
    """
    return pd.read_sql(query, conn)

def get_advertiser_directory(run_id):
    conn = get_connection()
    # We can use advertiser_run_stats if populated, or calculate on the fly
    # Let's calculate on the fly for freshness
    query = f"""
    SELECT 
        a.advertiser_id,
        a.current_page_name,
        a.current_profile_uri,
        a.status,
        COUNT(DISTINCT s.ad_id) as ads_in_run,
        GROUP_CONCAT(DISTINCT e.category) as categories_seen
    FROM advertisers a
    JOIN ads ad ON ad.advertiser_id = a.advertiser_id
    JOIN ad_snapshots s ON s.ad_id = ad.ad_id
    LEFT JOIN ad_extractions e ON e.run_id = s.run_id AND e.ad_id = s.ad_id
    WHERE s.run_id = '{run_id}'
    GROUP BY a.advertiser_id
    ORDER BY ads_in_run DESC
    """
    return pd.read_sql(query, conn)

def is_dropship_compliant(tags_str):
    """Checks if search tags contain dropshipping keywords and exclude services."""
    if not tags_str or not isinstance(tags_str, str): return False
    tags = tags_str.lower()
    
    dropship_keywords = ["envio gratis", "free shipping", "gratis", "entrega", "contraentrega", "pago contra", "cash on", "discount", "oferta", "rebaja", "50%", "off", "promo", "shop"]
    service_keywords = ["service", "consult", "asesor", "inmobiliari", "curso", "taller", "b2b", "marketing", "agencia", "credit", "prestamo", "abogado"]
    
    # Must have at least one dropship keyword
    has_pos = any(k in tags for k in dropship_keywords)
    # Must NOT have service keywords (strong exclude)
    has_neg = any(k in tags for k in service_keywords)
    
    return has_pos and not has_neg

# --- UI Styling ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .ad-card {
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 10px;
        background-color: white;
    }
    .small-btn {
        padding: 2px 10px;
        font-size: 12px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar ---
st.sidebar.title("🧠 Explorer PhD")
runs_df = load_runs()

if runs_df.empty:
    st.error("No Database Found.")
    st.stop()

selected_run_id = st.sidebar.selectbox("Run Config", runs_df["run_id"])
run_meta = runs_df[runs_df["run_id"] == selected_run_id].iloc[0]

st.sidebar.info(f"""
**Run Details**
- 📅 {run_meta['timestamp'][:10]}
- 📦 {run_meta['raw_count']} Ads
- 🏢 {run_meta['unique_advertisers']} Advertisers
""")

# --- Main Logic ---


# OpenAI Integration
from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

# Helpers
@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_strategy_brief(product_name, ads_df):
    """
    Generates a strategic brief using GPT-4o based on the aggregated ads data.
    """
    client = get_openai_client()
    
    # Aggregate context
    titles = "\n- ".join(ads_df["title"].dropna().unique()[:10])
    bodies = "\n- ".join(ads_df["body_text"].dropna().unique()[:5])
    ctas = ", ".join(ads_df["cta_type"].dropna().unique())
    
    prompt = f"""
    Act as a World-Class eCommerce Strategist (PhD level). Analyze the following data for a winning product candidate.
    
    PRODUCT: {product_name}
    
    TOP ADS TITLES:
    - {titles}
    
    AD COPY SNIPPETS:
    - {bodies}
    
    CTAs USED: {ctas}
    
    Generate a formatted Strategy Brief (Markdown) covering:
    1. 🎯 **Ideal Avatar Profile**: psychographics, pain points, age range.
    2. 🪝 **Winning Hooks**: The top 3 marketing angles found (or that should be tested).
    3. ⚔️ **Competitive Gap**: What are these advertisers missing? (Deep critique).
    4. 📹 **Creative Recommendation**: Specifically describing the visual style/format to produce (e.g., "UGC video showing before/after").
    
    Keep it concise, high-impact, and actionable.
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert e-commerce strategist."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error generating strategy: {str(e)}"

# --- Reusable UI Component ---
def render_product_details(run_id, product_row):
    """
    Renders the deep-dive view for a single product row.
    Expected row columns: product_id, canonical_name, category, candidate_score, advertisers_count, ads_count, signals_json.
    """
    st.markdown(f"### 💎 {product_row['canonical_name']}")
    m1, m2, m3, m4 = st.columns(4)
    m1.caption("Category"); m1.write(f"**{product_row['category']}**")
    m2.caption("Score"); m2.write(f"**{product_row['candidate_score']:.2f}**")
    m3.caption("Advertisers"); m3.write(f"**{product_row['advertisers_count']}**")
    m4.caption("Total Ads"); m4.write(f"**{product_row['ads_count']}**")
    
    st.divider()
    
    # Get Ads Data first for context
    ads_df = get_product_drilldown(run_id, product_row['product_id'])
    
    # AI Strategist Section
    st.info("🤖 **AI Chief Strategist**")
    if st.button(f"✨ Generar Estrategia para '{product_row['canonical_name']}'", key=f"btn_strat_{product_row['product_id']}"):
        with st.spinner("Analizando competencia y redactando brief estratégico..."):
            strategy = generate_strategy_brief(product_row['canonical_name'], ads_df)
            st.markdown(strategy)
            
    st.divider()
    
    # Signals Check
    sigs = json.loads(product_row["signals_json"] or "{}")
    if sigs:
        st.write("🔥 **Signals Detected:**")
        st.write(" ".join([f"`{k}`" for k, v in sigs.items() if v]))
    
    st.subheader(f"📡 Active Advertisers & Funnels ({product_row['advertisers_count']})")
    
    if ads_df.empty:
        st.warning("No ad details found for this product.")
    else:
        # Display Layout: Card Grid
        for i, row in ads_df.iterrows():
            title_safe = row['title'] or "No Title"
            body_safe = row['body_text'] or "No Body Text"
            
            with st.expander(f"📢 {row['current_page_name']} - {title_safe[:50]}...", expanded=(i<3)):
                c_img, c_meta, c_actions = st.columns([1, 2, 1])
                
                with c_img:
                    if row['image_url']:
                        st.image(row['image_url'], width=None, use_column_width=True) # use_column_width for stability
                    else:
                        st.text("No Image")
                        
                with c_meta:
                    st.caption("Ad Copy Snippet")
                    st.markdown(f"> *{body_safe[:200]}...*")
                    st.caption(f"CTA: {row['cta_type']}")
                
                with c_actions:
                    st.write("**Intelligence Links:**")
                    if row["link_url"]:
                        st.markdown(f"[🔗 **Ver Landing Page**]({row['link_url']})", unsafe_allow_html=True)
                    else:
                        st.markdown("🚫 No Landing Link")
                        
                    if row["current_profile_uri"]:
                        st.markdown(f"[🏢 Ver Perfil FB]({row['current_profile_uri']})", unsafe_allow_html=True)
                    
                    # Direct Ad Link
                    ad_lib_url = f"https://www.facebook.com/ads/library/?id={row['ad_id']}"
                    st.markdown(f"[📄 **Ver Anuncio**]({ad_lib_url})", unsafe_allow_html=True)


# --- Main Logic ---

tab1, tab2, tab3, tab4 = st.tabs(["📊 Market Overview", "🏆 Product Deep Dive", "🕵️ Advertiser Intell.", "🧪 Validation Lab"])

# TAB 1: OVERVIEW
with tab1:
    st.title("Market Intelligence")
    
    # 1. KPIs
    c1, c2, c3, c4 = st.columns(4)
    winners_df = load_winners(selected_run_id, limit=5000)
    
    total_products = len(winners_df)
    high_confidence = len(winners_df[winners_df["candidate_score"] >= 0.8])
    top_cat = winners_df["category"].mode()[0] if not winners_df.empty else "N/A"
    
    c1.metric("Unique Products Analysis", total_products)
    c2.metric("High Score Candidates", high_confidence)
    c3.metric("Dominant Category", top_cat)
    c4.metric("Avg. Competition per Product", f"{winners_df['advertisers_count'].mean():.1f}")
    
    st.markdown("---")
    
    # 2. Charts
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Category Distribution")
        cat_counts = winners_df["category"].value_counts().reset_index()
        cat_counts.columns = ["category", "count"]
        
        base = alt.Chart(cat_counts).encode(theta=alt.Theta("count", stack=True))
        pie = base.mark_arc(outerRadius=120).encode(
            color=alt.Color("category"),
            order=alt.Order("count", sort="descending"),
            tooltip=["category", "count"]
        )
        st.altair_chart(pie, use_container_width=True)
        
    with col_chart2:
        st.subheader("Competition vs Confidence")
        points = alt.Chart(winners_df).mark_circle().encode(
            x='candidate_score',
            y='advertisers_count',
            color='category',
            tooltip=['canonical_name', 'advertisers_count', 'candidate_score']
        ).interactive()
        st.altair_chart(points, use_container_width=True)

# TAB 2: PRODUCT DEEP DIVE
with tab2:
    st.title("Winning Candidates Drill-Down")
    
    # Selection
    col_sel, col_filter = st.columns([3, 1])
    cat_filter = col_filter.selectbox("Filter Category", ["All"] + sorted(winners_df["category"].unique().tolist()))
    
    view_df = winners_df
    if cat_filter != "All":
        view_df = view_df[view_df["category"] == cat_filter]

    # Master Table
    st.markdown("### 📋 Top Candidates Overview")
    st.dataframe(
        view_df,
        column_order=["canonical_name", "category", "candidate_score", "advertisers_count", "ads_count", "avg_confidence"],
        column_config={
            "canonical_name": st.column_config.TextColumn("Product Name", width="large"),
            "candidate_score": st.column_config.ProgressColumn("Score", format="%.2f", min_value=0, max_value=1),
            "advertisers_count": st.column_config.NumberColumn("Advertisers", format="%d"),
            "ads_count": st.column_config.NumberColumn("Total Ads", format="%d"),
            "category": "Category",
            "avg_confidence": "Confidence"
        },
        use_container_width=True,
        hide_index=True,
        height=300
    )
    st.divider()

    selected_prod_name = col_sel.selectbox("Select Product to Analyze", view_df["canonical_name"])
    
    if selected_prod_name:
        prod_row = view_df[view_df["canonical_name"] == selected_prod_name].iloc[0]
        render_product_details(selected_run_id, prod_row)

# TAB 3: ADVERTISER INTELLIGENCE
with tab3:
    st.title("Competitor Directory")
    
    # Update query to include search tags
    def get_advertiser_directory_extended(run_id):
        conn = get_connection()
        query = f"""
        SELECT 
            a.advertiser_id,
            a.current_page_name,
            a.current_profile_uri,
            a.status,
            COUNT(DISTINCT s.ad_id) as ads_in_run,
            GROUP_CONCAT(DISTINCT e.category) as categories_seen,
            GROUP_CONCAT(DISTINCT s._query_matched) as search_tags
        FROM advertisers a
        JOIN ads ad ON ad.advertiser_id = a.advertiser_id
        JOIN ad_snapshots s ON s.ad_id = ad.ad_id
        LEFT JOIN ad_extractions e ON e.run_id = s.run_id AND e.ad_id = s.ad_id
        WHERE s.run_id = '{run_id}'
        GROUP BY a.advertiser_id
        ORDER BY ads_in_run DESC
        """
        return pd.read_sql(query, conn)

    adv_df = get_advertiser_directory_extended(selected_run_id)
    
    # Filters
    c_search, c_filter_cat, c_filter_tag, c_filter_drop = st.columns(4)
    
    # 1. Text Search
    search_term = c_search.text_input("Search Advertiser Name", "")
    
    # 2. Category Filter
    all_cats = set()
    for x in adv_df["categories_seen"].dropna():
        all_cats.update(x.split(","))
    selected_cats = c_filter_cat.multiselect("Filter by Category", sorted(list(all_cats)))
    
    # 3. Search Tag Filter
    all_tags = set()
    for x in adv_df["search_tags"].dropna():
        all_tags.update(x.split(","))
    selected_tags = c_filter_tag.multiselect("Filter by Search Tag", sorted(list(all_tags)))

    # 4. Dropship Filter
    filter_dropship_adv = c_filter_drop.checkbox("📦 Dropship Only", value=False)

    # Apply Filters
    if search_term:
        adv_df = adv_df[adv_df["current_page_name"].str.contains(search_term, case=False, na=False)]
    
    if selected_cats:
        adv_df = adv_df[adv_df["categories_seen"].apply(lambda x: any(c in str(x).split(",") for c in selected_cats) if x else False)]

    if selected_tags:
        adv_df = adv_df[adv_df["search_tags"].apply(lambda x: any(t in str(x).split(",") for t in selected_tags) if x else False)]
        
    if filter_dropship_adv:
        adv_df = adv_df[adv_df["search_tags"].apply(is_dropship_compliant)]
    
    st.metric("Total Active Advertisers", len(adv_df))
    
    st.dataframe(
        adv_df,
        column_order=["current_page_name", "ads_in_run", "categories_seen", "search_tags", "current_profile_uri"],
        column_config={
            "current_page_name": "Advertiser",
            "current_profile_uri": st.column_config.LinkColumn("FB Profile"),
            "ads_in_run": st.column_config.NumberColumn("Ads Volume", format="%d"),
            "categories_seen": "Categories",
            "search_tags": "Search Tags"
        },
        height=600,
        use_container_width=True
    )

# TAB 4: VALIDATION LAB
with tab4:
    st.title("🧪 Validation Lab")
    st.markdown("Filter candidates based on **Validation** (multiple sellers) & **Scaling** (high ad volume) signals.")

    # Controls
    with st.expander("⚙️ Lab Settings", expanded=True):
        col_c1, col_c2, col_c3 = st.columns(3)
        
        with col_c1:
            st.subheader("Strategy A: Market Validation")
            st.caption("Products sold by multiple distinct advertisers (Proof of Concept).")
            min_adv = st.number_input("Min Advertisers", 3)
            max_adv = st.number_input("Max Advertisers", 7)
        
        with col_c2:
            st.subheader("Strategy B: Scaling Hunter")
            st.caption("Products with intense activity from a single player (Hidden Scale).")
            min_ads_scaling = st.number_input("Min Ads (Single Advertiser)", 5)
            
        with col_c3:
            st.subheader("Niche Filters")
            st.caption("Exclude generic/services. Focus on physical goods signals.")
            filter_dropship = st.checkbox("📦 Dropshipping Only", value=True, help="Must match tags: envio gratis, contraentrega, oferta, discount... Excludes 'service' terms.")

    # Logic
    # 0. Pre-filter by Dropshipping Tags if enabled
    df_lab = winners_df.copy()
    
    if filter_dropship:
        df_lab = df_lab[df_lab["search_tags"].apply(is_dropship_compliant)]

    # 1. Validation Set
    mask_validation = (df_lab['advertisers_count'] >= min_adv) & (df_lab['advertisers_count'] <= max_adv)
    
    # 2. Scaling Set (Single Advertiser dominance)
    mask_scaling = (df_lab['advertisers_count'] == 1) & (df_lab['ads_count'] > min_ads_scaling)
    
    # Apply logic
    df_lab['Validation_Type'] = None
    
    df_lab.loc[mask_validation, 'Validation_Type'] = "✅ Validation Ready"
    df_lab.loc[mask_scaling, 'Validation_Type'] = "🚀 Scaling Signal"
    df_lab.loc[mask_validation & (df_lab['ads_count'] > 10), 'Validation_Type'] = "🔥 Super Winner (Valid + Scale)"
    
    # Filter only those that match
    df_lab = df_lab[df_lab['Validation_Type'].notnull()]
    
    st.divider()
    
    if df_lab.empty:
        st.info("No candidates match the current Validation Lab criteria (Check Dropshipping filter).")
    else:
        st.metric("Qualified Candidates Found", len(df_lab))
        
        st.dataframe(
            df_lab,
            column_order=["canonical_name", "Validation_Type", "advertisers_count", "ads_count", "category", "search_tags", "candidate_score"],
            column_config={
                "canonical_name": st.column_config.TextColumn("Product Name", width="large"),
                "Validation_Type": st.column_config.TextColumn("Signal Type"),
                "advertisers_count": st.column_config.NumberColumn("Advertisers", format="%d"),
                "ads_count": st.column_config.NumberColumn("Total Ads", format="%d"),
                "search_tags": "Search Tags",
                "candidate_score": st.column_config.ProgressColumn("Score", format="%.2f", min_value=0, max_value=1),
            },
            use_container_width=True,
            hide_index=True,
            height=400
        )
        
        # Deep Dive Integration
        st.subheader("Lab Deep Dive")
        selected_lab_prod = st.selectbox("Analyze Lab Candidate", df_lab["canonical_name"])
        
        if selected_lab_prod:
            lab_prod_row = df_lab[df_lab["canonical_name"] == selected_lab_prod].iloc[0]
            render_product_details(selected_run_id, lab_prod_row)

