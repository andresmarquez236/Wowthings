import streamlit as st
import sys
import os

# Add parent path to find utils_ui
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils_ui import run_script_with_log_viewer

st.set_page_config(page_title="Generador de Landing", page_icon="🛬", layout="wide")

st.title("🛬 Automatización de Landing Pages")

st.markdown("""
Genera páginas de venta de alta conversión para Shopify.
""")

tab_auto, tab_manual = st.tabs(["🤖 Modo Automático (Lote)", "🛠 Modo Manual (Un Producto)"])

# --- AUTO MODE ---
with tab_auto:
    st.subheader("Procesamiento por Lotes")
    st.markdown("Procesa productos en `Resultados_Estudio` listos para landing (Ads generados).")
    
    st.divider()

    col1, col2 = st.columns([1, 2])

    with col1:
        st.info("Input: Productos con 'Camping Ads Gen' completado y 'Landing Auto Gen' pendiente.")

    with col2:
        st.subheader("🚀 Ejecución")
        run_script_with_log_viewer(
            "main_landing_generator_auto.py", 
            key_prefix="landing_auto",
            btn_label="🚀 Iniciar Generación de Landings"
        )

# --- MANUAL MODE ---
with tab_manual:
    st.subheader("Ejecución Unitaria")
    st.info("Genera una Landing Page para un producto específico ingresando la data manualmente.")
    
    col_input, col_action = st.columns(2)
    
    with col_input:
        m_p_name = st.text_input("Nombre del Producto", key="m_p_name")
        m_raw_info = st.text_area("Información / Características (Raw)", height=200, key="m_raw_info")
        m_avatar = st.text_area("Target Avatar JSON/Texto", 
                                value='"buyer_persona": "...", "promesa": "..."',
                                help="Copia el avatar o descríbelo.",
                                key="m_avatar")
        
    with col_action:
        st.write("### Confirmar Ejecución")
        
        if m_p_name and m_raw_info and m_avatar:
            manual_args = [
                "--product_name", m_p_name,
                "--raw_info", m_raw_info,
                "--target_avatar", m_avatar
            ]
            
            run_script_with_log_viewer(
                "main_landing_gen.py", 
                args=manual_args, 
                key_prefix="landing_manual",
                btn_label=f"🚀 Crear Landing para '{m_p_name}'"
            )
        else:
            st.warning("Completa todos los campos requeridos.")
