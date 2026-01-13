# check_list_generator.py
# ------------------------------------------------------------
# Script to generate ONLY the market research (checklist) for a product.
#
# INPUT:
#   Product parameters defined in main()
#
# OUTPUT:
#   output/{product_name}/market_research_min.json
# ------------------------------------------------------------

import os
import json
import subprocess
import sys
from typing import Dict, Any

from utils.logger import setup_logger
logger = setup_logger("CheckListGen")

# Configuration
BASE_OUTPUT_DIR = "output"

def safe_filename(name: str) -> str:
    return "".join([c if c.isalnum() else "_" for c in name]).lower()

def main():
    # 1. Define Product Parameters
    # ---------------------------------------------------------
    PRODUCT_NAME = "TAspersor De Riego I360"
    PRODUCT_DESC =""" Aspersor de Jardín 360° – Potente, Estable y Divertido 🌱🚿
¡Haz que tu jardín luzca siempre verde y saludable con este aspersor automático oscilante de 360°! Ideal para un riego uniforme y eficiente, ¡y también perfecto para que niños y mascotas se diviertan en días calurosos! 🧒🐶☀️

🔧 Características Principales:
💦 Potente riego automático: Brazos ajustables con boquillas de pulverización integradas para una cobertura uniforme y eficiente.

🔄 Rotación 360°: Riega en todas las direcciones, alcanzando cada rincón de tu jardín sin esfuerzo.

🌍 Cobertura de área extra grande: Ideal para jardines, céspedes y huertos. ¡Cubre más espacio con menos esfuerzo!

🧱 Estabilidad garantizada: Fabricado con plástico ABS de alta calidad y polímero resistente al desgaste. No se vuelca ni se mueve durante el uso.

🌧️ Simula lluvia natural: El patrón de riego es suave y uniforme, evitando encharcamientos y protegiendo el suelo.

👨‍👩‍👧‍👦 ¡Diversión bajo el sol!
Además de regar tus plantas, el aspersor puede convertirse en una divertida fuente de juegos acuáticos para los más pequeños y tus mascotas durante los días calurosos. 🏃‍♂️🐾💦

✅ Fácil de usar, resistente, multifuncional y perfecto para cualquier temporada.
¡Convierte tu jardín en un oasis de vida y alegría! 🌳🌞

"""
    WARRANTY = "10 dias"
    PRICE = "COP 95000"
    # ---------------------------------------------------------

    # 2. Setup Paths
    clean_name = PRODUCT_NAME
    if clean_name.lower().startswith("ejemplo:"):
        clean_name = clean_name[8:].strip()
    
    product_safe = safe_filename(clean_name)
    product_output_dir = os.path.join(BASE_OUTPUT_DIR, product_safe)
    os.makedirs(product_output_dir, exist_ok=True)
    
    logger.info(f"Output directory: {product_output_dir}")
    
    market_research_file = os.path.join(product_output_dir, "market_research_min.json")

    # 3. Run Market Research Agent
    logger.info("Running market_research_agent.py...")
    research_script = os.path.join("research", "market_research_agent.py")
    
    if not os.path.exists(research_script):
        logger.error(f"Script not found: {research_script}")
        return

    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    cmd = [
        sys.executable, research_script,
        "--product", PRODUCT_NAME,
        "--desc", PRODUCT_DESC,
        "--warranty", WARRANTY,
        "--price", PRICE,
        "--output", market_research_file
    ]

    try:
        subprocess.run(cmd, env=env, check=True, text=True)
        logger.info(f"Market research generated: {market_research_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Market research failed with exit code {e.returncode}.")
        return

    logger.info(f"Checklist generation finished. Check {market_research_file}")

if __name__ == "__main__":
    main()
