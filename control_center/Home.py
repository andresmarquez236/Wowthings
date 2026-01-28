import streamlit as st
import os
import sys

# Page Config
st.set_page_config(
    page_title="InfinityLab | Centro de Control Agentes",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E1E1E;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #555;
        margin-bottom: 2rem;
    }
    .card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 20px;
        border-left: 5px solid #007bff;
    }
    .step-number {
        font-size: 1.5rem;
        font-weight: bold;
        color: #007bff;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🚀 Centro de Control de Agentes</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Plataforma Unificada de Orquestación para E-Commerce Automatizado</div>', unsafe_allow_html=True)

st.divider()

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("### 🛠 Flujo de Trabajo")
    
    st.markdown("""
    <div class="card">
        <span class="step-number">1. Investigación de Mercado</span><br>
        Análisis automatizado de oportunidades de productos usando <b>CheckList Generator</b>.
        <br><i>Valida productos contra 15 criterios clave y guarda datos de investigación de mercado.</i>
    </div>
    
    <div class="card">
        <span class="step-number">2. Generación de Ads</span><br>
        Creación de creativos de alta conversión usando <b>Ads Generator Auto</b>.
        <br><i>Genera imágenes, carruseles, miniaturas y guiones de video.</i>
    </div>
    
    <div class="card">
        <span class="step-number">3. Landing Page</span><br>
        Despliegue de páginas de venta elite usando <b>Landing Generator Auto</b>.
        <br><i>Escribe copy, genera assets visuales (Antes/Después, Beneficios), construye JSON de Shopify y despliega.</i>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("### 📊 Inteligencia")
    st.info("Accede al **Explorer Dashboard** para analizar tendencias de mercado, anuncios de competidores y señales de productos en tiempo real.")
    
    st.markdown("### ⚙️ Estado del Sistema")
    
    st.write(f"**Directorio:** `{os.getcwd()}`")
    st.write(f"**Python:** `{sys.version.split(' ')[0]}`")
    
    if os.path.exists("output"):
        output_count = len([d for d in os.listdir("output") if os.path.isdir(os.path.join("output", d))])
        st.write(f"**Productos Procesados:** `{output_count}`")
    else:
        st.warning("Carpeta 'output' no encontrada.")

st.success("Selecciona un módulo en la barra lateral para comenzar.")
