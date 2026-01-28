import streamlit as st
import sys
import os

# Add parent path to find utils_ui
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils_ui import run_script_with_log_viewer

st.set_page_config(page_title="Generador de Ads", page_icon="📢", layout="wide")

st.title("📢 Generación de Activos Publicitarios (Ads)")

st.markdown("""
Este módulo automatiza la creación de creativos de marketing.
""")

tab_auto, tab_manual = st.tabs(["🤖 Modo Automático (Lote)", "🛠 Modo Manual (Un Producto)"])

# --- AUTOMATIC MODE ---
with tab_auto:
    st.subheader("Procesamiento por Lotes")
    st.markdown("Procesa productos pendientes en `Resultados_Estudio` (Columna 'Agentes Ads Gen' = 'SI').")
    
    st.divider()

    col_conf, col_run = st.columns([1, 2])

    with col_conf:
        st.subheader("⚙️ Configuración")
        use_v1 = st.checkbox("Ejecutar Pipeline V1", value=True, help="Incluye imagenes simples, miniaturas y guiones.")
        use_v2 = st.checkbox("Ejecutar Pipeline V2", value=True, help="Incluye generación de imágenes mejorada.")
        cleanup = st.checkbox("Limpieza de Archivos", value=False, help="Borrar archivos locales tras subida exitosa a Drive.")

    with col_run:
        st.subheader("🚀 Ejecución")
        
        args = []
        args.append(f"--use_v1={str(use_v1).lower()}")
        args.append(f"--use_v2={str(use_v2).lower()}")
        args.append(f"--cleanup={str(cleanup).lower()}")
        
        run_script_with_log_viewer(
            "main_ads_generator_auto.py", 
            args=args, 
            key_prefix="ads_auto",
            btn_label="🚀 Iniciar Generación Automática"
        )

# --- MANUAL MODE ---
with tab_manual:
    st.subheader("Ejecución Unitaria")
    st.info("Ejecuta la generación de ads para un solo producto específico. Ideal para pruebas o re-intentos.")
    
    col_input, col_action = st.columns(2)
    
    with col_input:
        p_name = st.text_input("Nombre del Producto", placeholder="Ej: Tenis Barbara")
        p_desc = st.text_area("Descripción del Producto", placeholder="Características principales...", height=150)
        p_warranty = st.text_input("Garantía", value="30 dias")
        p_price = st.text_input("Precio", value="COP 150000")
        
    with col_action:
        st.write("### Confirmar Ejecución")
        st.caption("Esto ejecutará `main_ads_generator.py` con los valores ingresados.")
        
        if p_name and p_desc:
            clean_args = [
                "--product_name", p_name,
                "--product_desc", p_desc,
                "--warranty", p_warranty,
                "--price", p_price
            ]
            run_script_with_log_viewer(
                "main_ads_generator.py",
                args=clean_args,
                key_prefix="ads_manual",
                btn_label=f"🚀 Generar Ads para '{p_name}'"
            )
        else:
            st.warning("Ingresa el Nombre y Descripción para habilitar.")
