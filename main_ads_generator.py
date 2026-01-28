# main_ads_generator.py
# ------------------------------------------------------------
# Master script to generate all ad assets for a product.
#
# INPUT:
#   output/market_research_min.json
#
# OUTPUT:
#   output/{product_name}/... (all generated files)
#
# AGENTS:
#   1. nanobanana_carrusel_agent.py
#   2. nanobanana_image_agent.py
#   3. nanobanana_thumbnail_agent.py
#   4. video_script_agent.py
#   5. fix_format.py (optional cleanup)
# ------------------------------------------------------------

import os
import json
import subprocess
import sys
from typing import Dict, Any

# Configuration
MARKET_RESEARCH_PATH = "output/market_research_min.json"
BASE_OUTPUT_DIR = "output"

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_product_name(market: Dict[str, Any]) -> str:
    # Try to find product info in 'input' (preferred) or 'meta.producto_input' (fallback)
    producto_input = market.get("input", {}) or ((market.get("meta", {}) or {}).get("producto_input", {}) or {})
    nombre = str(producto_input.get("nombre_producto", market.get("product_name", "producto"))).strip() or "producto"
    if nombre.lower().startswith("ejemplo:"):
        nombre = nombre[8:].strip()
    return nombre

def safe_filename(name: str) -> str:
    return "".join([c if c.isalnum() else "_" for c in name]).lower()

def run_script(script_name: str, output_dir: str):
    print(f"\n🚀 Running {script_name}...")
    script_path = os.path.join("ads_generator", script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ Script not found: {script_path}")
        return

    env = os.environ.copy()
    env["OUTPUT_DIR"] = output_dir
    env["MARKET_RESEARCH_PATH"] = os.path.abspath(MARKET_RESEARCH_PATH)
    env["MARKET_RESEARCH_MIN_PATH"] = os.path.abspath(MARKET_RESEARCH_PATH) # For thumbnail agent
    # Ensure PYTHONPATH includes current dir so imports work
    env["PYTHONPATH"] = os.getcwd()

    # Determine arguments based on script
    args = [sys.executable, script_path]
    
    # For agents that support --all or auto-batch, we might need flags.
    # nanobanana_carrusel_agent.py: auto-batches 3 angles (no flag needed based on recent update)
    # nanobanana_image_agent.py: auto-batches 3 angles (no flag needed)
    # nanobanana_thumbnail_agent.py: auto-batches 3 angles (no flag needed)
    # video_script_agent.py: needs --all to run all 3 angles
    if script_name == "video_script_agent.py":
        args.append("--all")

    try:
        result = subprocess.run(args, env=env, check=True, text=True)
        print(f"✅ {script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} failed with exit code {e.returncode}.")
        # We don't exit here, we try to continue with other agents

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Manual Ads Generator")
    parser.add_argument("--product_name", type=str, required=True, help="Name of the product")
    parser.add_argument("--product_desc", type=str, required=True, help="Description of the product")
    parser.add_argument("--warranty", type=str, default="30 dias", help="Warranty info")
    parser.add_argument("--price", type=str, default="0", help="Price info")
    
    args = parser.parse_args()

    # 1. Define Product Parameters
    # ---------------------------------------------------------
    PRODUCT_NAME = args.product_name
    PRODUCT_DESC = args.product_desc
    WARRANTY = args.warranty
    PRICE = args.price
    # ---------------------------------------------------------

    # 2. Setup Paths
    clean_name = PRODUCT_NAME
    if clean_name.lower().startswith("ejemplo:"):
        clean_name = clean_name[8:].strip()
    
    product_safe = safe_filename(clean_name)
    product_output_dir = os.path.join(BASE_OUTPUT_DIR, product_safe)
    os.makedirs(product_output_dir, exist_ok=True)
    
    print(f"📂 Output directory: {product_output_dir}")
    
    market_research_file = os.path.join(product_output_dir, "market_research_min.json")

    # 3. Run Market Research Agent
    print("\n🚀 Running market_research_agent.py...")
    research_script = os.path.join("research", "market_research_agent.py")
    
    if not os.path.exists(research_script):
        print(f"❌ Script not found: {research_script}")
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
        print(f"✅ Market research generated: {market_research_file}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Market research failed with exit code {e.returncode}.")
        return

    # 4. Run Creative Agents
    # Update global path for run_script to use
    global MARKET_RESEARCH_PATH
    MARKET_RESEARCH_PATH = market_research_file

    # List of agents to run
    agents = [
        "nanobanana_carrusel_agent.py",
        "nanobanana_image_agent.py",
        "nanobanana_thumbnail_agent.py",
        "video_script_agent.py",
    ]

    for agent in agents:
        run_script(agent, product_output_dir)

    print(f"\n🎉 All agents execution attempt finished. Check {product_output_dir} for results.")

    # 5. Upload to Drive
    print(f"\n☁️ Uploading to Google Drive...")
    try:
        from tools.drive_uploader import upload_product_to_drive
        upload_product_to_drive(product_output_dir)
    except Exception as e:
        print(f"❌ Drive Upload Failed: {e}")

if __name__ == "__main__":
    main()
