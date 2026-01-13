# main_ads_generator_auto.py
# ------------------------------------------------------------
# Automated script to generate ad assets for approved products.
# Reads from Google Sheets (Resultados_Estudio) via research/info_products.py
# ------------------------------------------------------------

import os
import sys
import shutil
import subprocess
import json
from typing import Dict, Any

# Add current dir to sys.path to allow imports
sys.path.append(os.getcwd())

from research.info_products import get_approved_products_for_ads, mark_ads_gen_completed, upload_folder_to_drive
from image_generation.carrusel_generator import run_carrusel_generation
from image_generation.miniatura_generator import run_product_generation
from image_generation.simple_image_generator import run_simple_image_generation
from tools.drive_uploader import upload_product_to_drive
# from tools.organize_assets import organize_product_assets
from utils.logger import setup_logger, update_context, log_section
from dotenv import load_dotenv

# Configuration
BASE_OUTPUT_DIR = "output"
logger = setup_logger("AdsGenAuto")

def safe_filename(name: str) -> str:
    return "".join([c if c.isalnum() else "_" for c in name]).lower()

def run_script(script_name: str, output_dir: str, env_vars: Dict[str, str]):
    logger.info(f"Running script: {script_name}")
    script_path = os.path.join("ads_generator", script_name)
    
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        return

    # Update env with our specific vars
    env = os.environ.copy()
    env.update(env_vars)
    env["PYTHONPATH"] = os.getcwd()

    args = [sys.executable, script_path]
    
    # Specific flags
    if script_name == "video_script_agent.py":
        args.append("--all")

    try:
        subprocess.run(args, env=env, check=True, text=True)
        logger.info(f"{script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"{script_name} failed with exit code {e.returncode}.")

import argparse

def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Automated Ads Generation")
    # Both default to True as requested
    parser.add_argument("--use_v1", type=str, default="true", help="Run V1 pipeline (true/false)")
    parser.add_argument("--use_v2", type=str, default="true", help="Run V2 pipeline (true/false)")
    parser.add_argument("--cleanup", type=str, default="false", help="Delete local folder after upload (true/false)")
    args = parser.parse_args()
    
    use_v1 = args.use_v1.lower() == "true"
    use_v2 = args.use_v2.lower() == "true"
    
    print(f"🚀 Starting Automated Ads Generation (V1: {use_v1}, V2: {use_v2})...")
    
    # 1. Get Candidates
    update_context(step="Candidate Selection")
    log_section(logger, "Candidate Selection")
    candidates = get_approved_products_for_ads()
    
    if not candidates:
        logger.info("No pending approved products found.")
        return
        
    logger.info(f"Found {len(candidates)} products to process: {[c['nombre_producto'] for c in candidates]}")
    
    for product in candidates:
        p_name = product.get("nombre_producto", "").strip()
        p_desc = product.get("descripcion", "").strip()
        p_price = product.get("precio", "").strip()
        p_warranty = product.get("garantia", "").strip()
        row_idx = product.get("results_row_idx")
        
        log_section(logger, f"Processing Product: {p_name}")
        update_context(step="Initialization", module_name=p_name)
        
        # Setup paths
        clean_name = p_name
        if clean_name.lower().startswith("ejemplo:"):
            clean_name = clean_name[8:].strip()
            
        product_safe = safe_filename(clean_name)
        product_output_dir = os.path.join(BASE_OUTPUT_DIR, product_safe)
        os.makedirs(product_output_dir, exist_ok=True)
        
        market_research_file = os.path.join(product_output_dir, "market_research_min.json")
        
        # 1. Verify Market Research Exists (Expectation is that it was created by Checklist Generator)
        if not os.path.exists(market_research_file):
             logger.error(f"Market Research JSON missing at {market_research_file}")
             logger.warning("Skipping this product as precursors are missing.")
             continue
             
        logger.info(f"Found Market Research: {market_research_file}")
            
        # --- V1 LEGACY PIPELINE ---
        if use_v1:
            update_context(step="V1 Pipeline")
            log_section(logger, "V1 Pipeline Execution")
            logger.info(f"Launching V1 Pipeline for {p_name}...")
            # 2. Run Creative Agents
            agents = [
                "nanobanana_carrusel_agent.py",
                "nanobanana_image_agent.py",
                "nanobanana_thumbnail_agent.py",
                "video_script_agent.py",
            ]
            
            # Common Envs for agents
            agent_envs = {
                "OUTPUT_DIR": product_output_dir,
                "MARKET_RESEARCH_PATH": os.path.abspath(market_research_file),
                "MARKET_RESEARCH_MIN_PATH": os.path.abspath(market_research_file)
            }
            
            for agent in agents:
                try:
                    update_context(step=f"V1 Agent: {agent}")
                    run_script(agent, product_output_dir, agent_envs)
                except Exception as e:
                    logger.error(f"Error running {agent}: {e}")
                    
            # 3. Run Image Generators (Gemini)
            update_context(step="V1 Image Generation")
            logger.info("Starting Image Generation (Gemini)...")
            api_key = os.getenv("GEMINI_API_KEY")
            
            if not api_key:
                logger.error("Skipping Image Gen: GEMINI_API_KEY not found in env.")
            else:
                # try:
                #     # 3.1 Carousel Images
                #     logger.info("Generating Carousel Images...")
                #     run_carrusel_generation(
                #         product_name=product_safe, 
                #         num_angulos=1,
                #         output_root=BASE_OUTPUT_DIR,
                #         api_key=api_key
                #     )
                # except Exception as e:
                #     logger.error(f"Carousel Image Gen Failed: {e}")

                try:
                    # 3.1b Single Images (Simple)
                    logger.info("Generating Single Images...")
                    try:
                        run_simple_image_generation(
                            product_name=product_safe,
                            output_root=BASE_OUTPUT_DIR,
                            api_key=api_key,
                            num_angulos=3  
                        )
                    except TypeError:
                         run_simple_image_generation(
                            product_name=product_safe,
                            output_root=BASE_OUTPUT_DIR,
                            api_key=api_key
                        )
                except Exception as e:
                    logger.error(f"Single Image Gen Failed: {e}")
                    
                # try:
                #     # 3.2 Thumbnail Images
                #     logger.info("Generating Thumbnail Images...")
                #     run_product_generation(
                #         product_name=product_safe,
                #         num_angulos=1,
                #         output_root=BASE_OUTPUT_DIR,
                #         api_key=api_key
                #     )
                # except Exception as e:
                #     logger.error(f"Thumbnail Image Gen Failed: {e}")

        # --- V2 PIPELINE EXECUTION ---
        if use_v2:
            update_context(step="V2 Pipeline")
            log_section(logger, "V2 Pipeline Execution")
            logger.info(f"Launching V2 Pipeline for {p_name}...")
            
            v2_script = "main_ads_generator_v2.py"
            if not os.path.exists(v2_script):
                 logger.error(f"V2 Script not found: {v2_script}")
            else:
                cmd_v2 = [
                    sys.executable, v2_script,
                    "--input_path", market_research_file,
                    "--num_angles", "1" 
                ]
                
                try:
                    subprocess.run(cmd_v2, check=True)
                    logger.info(f"V2 Pipeline completed for {p_name}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"V2 Pipeline failed for {p_name} with code {e.returncode}")

    # 4. Skip Organization (Feature Removed)
        # try:
        #    organize_product_assets(product_output_dir)
        # except Exception as e:
        #    print(f"⚠️ Organization Failed: {e}")

        # 5. Upload to Drive (OAuth)
        update_context(step="Drive Upload")
        upload_success = False
        try:
            logger.info("Uploading output to Google Drive (OAuth)...")
            upload_success = upload_product_to_drive(product_output_dir)
        except Exception as e:
            logger.error(f"Drive Upload Failed: {e}")

        # 6. Mark as Completed & Cleanup
        update_context(step="Completion")
        mark_ads_gen_completed(row_idx)
        logger.info(f"Completed flow for {p_name}")
        
        # Cleanup Logic: Only if --cleanup True (default False) AND Upload Success
        should_cleanup = args.cleanup and (str(args.cleanup).lower() == "true")
        
        if upload_success:
            if should_cleanup:
                try:
                    logger.info(f"Cleaning up local folder: {product_output_dir}...")
                    shutil.rmtree(product_output_dir)
                    logger.info("Local folder deleted.")
                except Exception as e:
                    logger.error(f"Cleanup Failed: {e}")
            else:
                logger.info("Local folder preserved (Cleanup disabled).")
        else:
             logger.warning("Local folder preserved (Upload failed).")

if __name__ == "__main__":
    main()
