import streamlit as st
import sys
import os

# Add parent path to find utils_ui
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils_ui import run_script_with_log_viewer

st.set_page_config(page_title="Investigación de Mercado", page_icon="🔬", layout="wide")

st.title("🔬 Automatización de Investigación de Mercado")
st.markdown("""
Este módulo ejecuta el agente **Checklist Generator**.

**Funcionalidad:**
1. Lee productos de Google Sheets (`Info_Productos`) donde `Agentes Market Res` = 'SI'.
2. Descarga imágenes del producto.
3. Ejecuta el **Spy Agent** para revisar competidores en Facebook Ads Library.
4. Ejecuta el **Market Research Agent** para validar el producto contra 15 criterios.
5. Actualiza la hoja con estado `APROBADO` y puntuaciones detalladas.
""")

st.divider()

col1, col2 = st.columns([1, 2])

with col1:
    st.info("**Pre-requisitos:**\n- Asegúrate que la hoja 'Info_Productos' esté actualizada.\n- Revisa que las API keys de APIFY estén en `.env`.")

with col2:
    st.subheader("🚀 Ejecución")
    run_script_with_log_viewer("research/check_list_generator_auto.py", key_prefix="mr", btn_label="🚀 Ejecutar Investigación de Mercado")
