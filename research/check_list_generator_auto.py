# check_list_generator_auto.py
# ------------------------------------------------------------
# Script to generate market research (checklist) for multiple products.
# INPUT: Fetches data via info_products.py (Column E="SI")
# OUTPUT: Updates Google Sheets (Info_Productos & Resultados_Estudio)
# ------------------------------------------------------------

import os
import json
import subprocess
import sys
import pandas as pd
from typing import Dict, Any, List
import os
import sys

from utils.logger import setup_logger
logger = setup_logger("CheckListGen_Auto")

# Import our new data source & update functions
from info_products import get_filtered_products, update_product_status, log_study_result, download_product_images

# Add parent dir to sys.path to allow imports from spy_agent
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

    try:
        from spy_agent.apy_fb_library_agent import run_spy_flow
        from spy_agent.process_info import slugify
    except ImportError as e:
        logger.warning(f"Could not import Spy Agent modules: {e}")
        run_spy_flow = None
        slugify = None

    # Configuration
    BASE_OUTPUT_DIR = "output"

    def safe_filename(name: str) -> str:
        return "".join([c if c.isalnum() else "_" for c in name]).lower()

    def run_agent_for_product(product_name: str, description: str, warranty: str, price: str, margin_ok: bool = None, competitors_ok: bool = None) -> Dict[str, Any]:
        """Runs the market_research_agent.py via subprocess and returns the parsed JSON."""
        
        clean_name = product_name
        if clean_name.lower().startswith("ejemplo:"):
            clean_name = clean_name[8:].strip()
        
        product_safe = safe_filename(clean_name)
        product_output_dir = os.path.join(BASE_OUTPUT_DIR, product_safe)
        os.makedirs(product_output_dir, exist_ok=True)
        
        market_research_file = os.path.join(product_output_dir, "market_research_min.json")
        
        research_script = os.path.join("research", "market_research_agent.py")
        if not os.path.exists(research_script):
            logger.error(f"Script not found: {research_script}")
            return {}

        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()

        cmd = [
            sys.executable, research_script,
        "--product", product_name,
        "--desc", description,
        "--warranty", warranty,
        "--price", price,
        "--output", market_research_file
    ]
    
    if margin_ok is not None:
        cmd.extend(["--margin_ok", str(margin_ok).lower()])
        
    if competitors_ok is not None:
        cmd.extend(["--competitors_ok", str(competitors_ok).lower()])

    try:
        logger.info(f"Running agent for: {product_name}...")
        subprocess.run(cmd, env=env, check=True, text=True, capture_output=True) 
        
        if os.path.exists(market_research_file):
            with open(market_research_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            logger.error("Output file not created.")
            return {}
    except subprocess.CalledProcessError as e:
        logger.error(f"Execution failed: {e}")
        logger.error(f"Stderr: {e.stderr}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {}

def parse_currency(val: str) -> float:
    # Colombian format: 1.000.000,00 implies . is thousands, , is decimal
    # Remove $ and spaces
    clean = val.replace("$", "").replace(" ", "").strip()
    if not clean:
        return 0.0
    try:
        # Remove thousands separator (.)
        clean = clean.replace(".", "")
        # Replace decimal separator (,) with (.)
        clean = clean.replace(",", ".")
        return float(clean)
    except ValueError:
        return 0.0

def parse_percentage(val: str) -> float:
    # Handles "20%" -> 20.0 and "0.2" -> 20.0
    clean = val.replace("%", "").strip()
    if not clean:
        return 0.0
    try:
        val_float = float(clean.replace(",", "."))
        if val_float <= 1.0: 
            val_float *= 100
        return val_float
    except ValueError:
        return 0.0

def run_spy_and_get_competitor_status(product_name: str, product_desc: str) -> bool:
    """
    Runs the Spy Agent and returns True if 'producto_test' is true (few competitors), 
    False otherwise (or None if failed).
    """
    if not run_spy_flow or not slugify:
        logger.warning("Spy Agent not available. Assuming competitor check failed (None).")
        return None

    logger.info(f"Running Spy Agent for: {product_name}...")
    try:
        # Run the full flow (Research -> Apify -> Process)
        run_spy_flow(
            product_name=product_name,
            product_description=product_desc,
            country="CO", # Defaulting to CO
            limit_per_source=80,
            scrape_ad_details=False
        )

        # Construct path to rank report
        product_slug = slugify(product_name)
        # Assumes BASE_OUTPUT_DIR is "output" relative to CWD
        report_path = os.path.join(BASE_OUTPUT_DIR, product_slug, "apify_results", f"fblibrary_advertisers_rank_{product_slug}.json")
        
        if not os.path.exists(report_path):
            logger.warning(f"Spy report not found at: {report_path}")
            return None

        with open(report_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        metrics = report_data.get("metrics", {})
        producto_test = metrics.get("producto_test", False)
        
        # Additional logging
        scaling_list = metrics.get("scaling", [])
        scaling_names = [s.get('page_name', 'Unknown') for s in scaling_list]
        logger.info(f"Spy Agent Result: producto_test={producto_test}. Scaling: {len(scaling_list)} ({', '.join(scaling_names)})")
        
        return producto_test

    except Exception as e:
        logger.error(f"Error running Spy Agent: {e}")
        return None

def main():
    logger.info("Fetching products from info_products.py...")
    try:
        # Returns list of tuples: (row_index, row_data)
        products_data = get_filtered_products()
    except Exception as e:
        logger.error(f"Error fetching products: {e}")
        return

    total_products = len(products_data)
    logger.info(f"Found {total_products} products to process (Filtered 'SI').")

    # Mapping Updated for New Columns (A-L)
    for i, (row_idx, row) in enumerate(products_data):
        # Safety check for length (Need at least up to Warranty/Index 8)
        if len(row) < 9:
            logger.warning(f"Row {row_idx} has insufficient data. Skipping: {row}")
            continue

        p_name = str(row[2]).strip()
        p_price = str(row[3]).strip()
        p_margin_raw = str(row[5]).strip()
        p_profit_raw = str(row[6]).strip()
        p_desc = str(row[7]).strip()
        p_warranty = str(row[8]).strip()

        # Calculate Margin Goodness
        # Rule: Margin > 30000 AND Profit > 17(%)
        margin_val = parse_currency(p_margin_raw)
        profit_val = parse_percentage(p_profit_raw)
        
        is_good_margin = (margin_val > 30000) and (profit_val > 17)
        logger.info(f"Margin Analysis: Val={margin_val}, Profit={profit_val}% -> Good? {is_good_margin}")

        if not p_name: 
            continue

        logger.info(f"[{i+1}/{total_products}] Processing Row {row_idx}: {p_name}")
        
        # 0. DOWNLOAD IMAGES FROM DRIVE
        # Replicate naming logic from research_product_querys.py to ensure it finds them
        # folder_name = name.lower().strip().replace(" ", "_")
        spy_folder_name = p_name.lower().strip().replace(" ", "_")
        images_dir = os.path.join(BASE_OUTPUT_DIR, spy_folder_name, "product_images")
        
        logger.info(f"Ensuring images exist in: {images_dir}")
        download_product_images(p_name, images_dir)
        
        # 1. RUN SPY AGENT FIRST
        competitors_ok = run_spy_and_get_competitor_status(p_name, p_desc)
        
        if competitors_ok is None:
             logger.warning("Spy Agent run failed or inconclusive. Proceeding without competitor check.")

        # 2. RUN MARKET RESEARCH AGENT (Passing competitors_ok)
        data = run_agent_for_product(
            p_name, 
            p_desc, 
            p_warranty, 
            p_price, 
            margin_ok=is_good_margin,
            competitors_ok=competitors_ok
        )
        
        if not data:
            logger.warning("No data returned. Skipping update.")
            continue

        # Extract info
        checklist = data.get("checklist", [])
        score_info = data.get("score_total", {})
        approved_bool = score_info.get("cumple_9_de_15", False)
        approved_str = "SI" if approved_bool else "NO"
        
        # 3. Update Sheet Status (Col I, J)
        try:
            update_product_status(row_idx, study_done="SI", approved_status=approved_str)
        except Exception as e:
            logger.error(f"Failed to update product status: {e}")

        # 4. Log Result to 'Resultados_Estudio'
        res_row = {
            "Nombre Producto": p_name,
            "APROBADO (>9/12)": approved_str,
            "Precio": p_price,
            "Total SI": score_info.get("total_si", 0)
        }
        for item in checklist:
            crit = item.get("criterio", "")
            ans = item.get("cumple", "N/A")
            res_row[crit] = ans
            
        try:
            log_study_result(res_row)
        except Exception as e:
            logger.error(f"Failed to log study result: {e}")

    logger.info("All processing completed.")

if __name__ == "__main__":
    main()
