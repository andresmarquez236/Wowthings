import streamlit as st
import sqlite3
import pandas as pd
import json
import altair as alt
import os
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

# --- Page Config ---
st.set_page_config(
    page_title="Wow Explorer - PhD Dashboard",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Constants & Setup ---
# Fixing path to point to explorer/store/product_memory.db from control_center/pages/
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DB_NAME = "product_memory.db"
DB_PATH = ROOT_DIR / "explorer" / "store" / DB_NAME
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)

# --- Helpers ---
@st.cache_resource
def get_connection():
    if not os.path.exists(DB_PATH):
        st.error(f"Database not found at {DB_PATH}")
        return None
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)

def get_openai_client():
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def load_runs():
    conn = get_connection()
    if not conn: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT run_id, timestamp, unique_advertisers, raw_count FROM runs ORDER BY timestamp DESC", conn)
        return df
    except Exception as e:
        st.error(f"Error loading runs: {e}")
        return pd.DataFrame()

def load_winners(run_id, limit=200):
    conn = get_connection()
    if not conn: return pd.DataFrame()
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
    return pd.read_sql(query, conn)

def get_product_drilldown(run_id, product_id):
    conn = get_connection()
    if not conn: return pd.DataFrame()
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

def is_dropship_compliant(tags_str):
    if not tags_str or not isinstance(tags_str, str): return False
    tags = tags_str.lower()
    dropship_keywords = ["envio gratis", "free shipping", "gratis", "entrega", "contraentrega", "pago contra", "cash on", "discount", "oferta", "rebaja", "50%", "off", "promo", "shop"]
    service_keywords = ["service", "consult", "asesor", "inmobiliari", "curso", "taller", "b2b", "marketing", "agencia", "credit", "prestamo", "abogado"]
    return any(k in tags for k in dropship_keywords) and not any(k in tags for k in service_keywords)

def generate_strategy_brief(product_name, ads_df):
    client = get_openai_client()
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
    2. 🪝 **Winning Hooks**: The top 3 marketing angles found.
    3. ⚔️ **Competitive Gap**: What are these advertisers missing?
    4. 📹 **Creative Recommendation**: Specifically describing the visual style/format.
    Keep it concise.
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

def render_product_details(run_id, product_row):
    st.markdown(f"### 💎 {product_row['canonical_name']}")
    m1, m2, m3, m4 = st.columns(4)
    m1.caption("Category"); m1.write(f"**{product_row['category']}**")
    m2.caption("Score"); m2.write(f"**{product_row['candidate_score']:.2f}**")
    m3.caption("Advertisers"); m3.write(f"**{product_row['advertisers_count']}**")
    m4.caption("Total Ads"); m4.write(f"**{product_row['ads_count']}**")
    
    st.divider()
    
    ads_df = get_product_drilldown(run_id, product_row['product_id'])
    
    st.info("🤖 **AI Chief Strategist**")
    if st.button(f"✨ Generate Strategy for '{product_row['canonical_name']}'", key=f"btn_strat_{product_row['product_id']}"):
        with st.spinner("Analyzing competition..."):
            strategy = generate_strategy_brief(product_row['canonical_name'], ads_df)
            st.markdown(strategy)
            
    st.divider()
    
    sigs = json.loads(product_row["signals_json"] or "{}")
    if sigs:
        st.write("🔥 **Signals Detected:**")
        st.write(" ".join([f"`{k}`" for k, v in sigs.items() if v]))
    
    st.subheader(f"📡 Active Advertisers & Funnels ({product_row['advertisers_count']})")
    
    if ads_df.empty:
        st.warning("No ad details found for this product.")
    else:
        for i, row in ads_df.iterrows():
            title_safe = row['title'] or "No Title"
            body_safe = row['body_text'] or "No Body Text"
            
            with st.expander(f"📢 {row['current_page_name']} - {title_safe[:50]}...", expanded=(i<3)):
                c_img, c_meta, c_actions = st.columns([1, 2, 1])
                with c_img:
                    if row['image_url']:
                        st.image(row['image_url'], use_container_width=True)
                    else:
                        st.text("No Image")
                with c_meta:
                    st.caption("Ad Copy Snippet")
                    st.markdown(f"> *{body_safe[:200]}...*")
                    st.caption(f"CTA: {row['cta_type']}")
                with c_actions:
                    if row["link_url"]:
                        st.markdown(f"[🔗 **Ver Landing Page**]({row['link_url']})", unsafe_allow_html=True)
                    if row["current_profile_uri"]:
                        st.markdown(f"[🏢 Ver Perfil FB]({row['current_profile_uri']})", unsafe_allow_html=True)
                    ad_lib_url = f"https://www.facebook.com/ads/library/?id={row['ad_id']}"
                    st.markdown(f"[📄 **Ver Anuncio**]({ad_lib_url})", unsafe_allow_html=True)

# --- Main App ---
st.title("🧠 Explorer PhD - Tablero de Inteligencia")

runs_df = load_runs()

if runs_df.empty:
    st.warning("No se encontraron datos de ejecución en la base de datos.")
else:
    selected_run_id = st.sidebar.selectbox("Configuración de Ejecución", runs_df["run_id"])
    run_meta = runs_df[runs_df["run_id"] == selected_run_id].iloc[0]
    st.sidebar.info(f"**Detalles de Ejecución**\n\n📅 {run_meta['timestamp'][:10]}\n📦 {run_meta['raw_count']} Anuncios\n🏢 {run_meta['unique_advertisers']} Anunciantes")

    tab1, tab2, tab3 = st.tabs(["📊 Panorama de Mercado", "🏆 Análisis Profundo de Producto", "🧪 Laboratorio de Validación"])

    # TAB 1: OVERVIEW
    with tab1:
        st.subheader("Inteligencia de Mercado")
        winners_df = load_winners(selected_run_id, limit=5000)
        
        if not winners_df.empty:
             c1, c2, c3, c4 = st.columns(4)
             total_products = len(winners_df)
             high_confidence = len(winners_df[winners_df["candidate_score"] >= 0.8])
             top_cat = winners_df["category"].mode()[0]
             c1.metric("Productos Únicos", total_products)
             c2.metric("Candidatos Alta Confianza", high_confidence)
             c3.metric("Categoría Dominante", top_cat)
             c4.metric("Competencia Promedio", f"{winners_df['advertisers_count'].mean():.1f}")
             
             col_chart1, col_chart2 = st.columns(2)
             with col_chart1:
                 st.caption("Distribución por Categoría")
                 cat_counts = winners_df["category"].value_counts().reset_index()
                 cat_counts.columns = ["category", "count"]
                 pie = alt.Chart(cat_counts).mark_arc(outerRadius=120).encode(
                    theta=alt.Theta("count", stack=True),
                    color=alt.Color("category"),
                    order=alt.Order("count", sort="descending"),
                    tooltip=["category", "count"]
                 )
                 st.altair_chart(pie, use_container_width=True)
             with col_chart2:
                 st.caption("Competencia vs Confianza")
                 points = alt.Chart(winners_df).mark_circle().encode(
                    x='candidate_score',
                    y='advertisers_count',
                    color='category',
                    tooltip=['canonical_name', 'advertisers_count', 'candidate_score']
                 ).interactive()
                 st.altair_chart(points, use_container_width=True)

    # TAB 2: PRODUCT DEEP DIVE
    with tab2:
         st.subheader("Análisis de Candidatos Ganadores")
         if not winners_df.empty:
             col_sel, col_filter = st.columns([3, 1])
             cat_filter = col_filter.selectbox("Filtrar Categoría", ["Todas"] + sorted(winners_df["category"].unique().tolist()))
             
             view_df = winners_df
             if cat_filter != "Todas":
                 view_df = view_df[view_df["category"] == cat_filter]
                 
             st.dataframe(
                view_df,
                column_order=["canonical_name", "category", "candidate_score", "advertisers_count", "ads_count", "avg_confidence"],
                column_config={
                    "canonical_name": st.column_config.TextColumn("Nombre Producto", width="large"),
                    "candidate_score": st.column_config.ProgressColumn("Puntaje", format="%.2f", min_value=0, max_value=1),
                },
                use_container_width=True,
                hide_index=True,
                height=300
             )
             
             selected_prod_name = col_sel.selectbox("Seleccionar Producto para Analizar", view_df["canonical_name"])
             if selected_prod_name:
                 prod_row = view_df[view_df["canonical_name"] == selected_prod_name].iloc[0]
                 render_product_details(selected_run_id, prod_row)

    # TAB 3: VALIDATION LAB
    with tab3:
        st.subheader("🧪 Laboratorio de Validación")
        if not winners_df.empty:
            with st.expander("⚙️ Configuración del Lab", expanded=True):
                 col_c1, col_c2, col_c3 = st.columns(3)
                 with col_c1:
                     min_adv = st.number_input("Min Anunciantes", 3)
                     max_adv = st.number_input("Max Anunciantes", 7)
                 with col_c2:
                     min_ads_scaling = st.number_input("Min Ads (Un solo Anunciante)", 5)
                 with col_c3:
                     filter_dropship = st.checkbox("📦 Solo Dropshipping", value=True)
            
            df_lab = winners_df.copy()
            if filter_dropship:
                 df_lab = df_lab[df_lab["search_tags"].apply(is_dropship_compliant)]
                 
            mask_validation = (df_lab['advertisers_count'] >= min_adv) & (df_lab['advertisers_count'] <= max_adv)
            mask_scaling = (df_lab['advertisers_count'] == 1) & (df_lab['ads_count'] > min_ads_scaling)
            
            df_lab['Validation_Type'] = None
            df_lab.loc[mask_validation, 'Validation_Type'] = "✅ Listo para Validar"
            df_lab.loc[mask_scaling, 'Validation_Type'] = "🚀 Señal de Escalado"
            df_lab.loc[mask_validation & (df_lab['ads_count'] > 10), 'Validation_Type'] = "🔥 Super Ganador"
            
            df_lab = df_lab[df_lab['Validation_Type'].notnull()]
            
            if df_lab.empty:
                st.info("No hay candidatos que coincidan con los filtros.")
            else:
                st.metric("Candidatos Calificados", len(df_lab))
                st.dataframe(df_lab[["canonical_name", "Validation_Type", "advertisers_count", "ads_count", "category"]], use_container_width=True)
                
                selected_lab_prod = st.selectbox("Analizar Candidato del Lab", df_lab["canonical_name"])
                if selected_lab_prod:
                    lab_prod_row = df_lab[df_lab["canonical_name"] == selected_lab_prod].iloc[0]
                    render_product_details(selected_run_id, lab_prod_row)
