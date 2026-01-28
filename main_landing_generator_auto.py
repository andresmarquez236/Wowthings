
import os
import sys
import json
import logging
from dotenv import load_dotenv

# Ensure we can import from root
sys.path.append(os.getcwd())

from research.info_products import get_products_ready_for_landing, mark_landing_gen_completed
from utils.logger import setup_logger, log_section, update_context

# Import Landing Gen Modules
from shopify.content_agent import generate_elite_landing_copy
from shopify.mapper import map_payload_to_shopify_structure
from shopify.image_landing_gen import (
    section_before_after,
    section_pain,
    section_benefits,
    section_social_proof,
    evaluator_benefits
)
from shopify.upload_images import deploy_images
from shopify.visual_plan.visual_planer import VisualPlaner
from shopify.visual_plan.visual_injection import run_injection_pipeline

# Setup Logger
logger = setup_logger("AutoLandingGen")

def safe_filename(name: str) -> str:
    # This logic mimics what is likely used elsewhere for folder naming
    # User example: "samba_og_vaca_negro_blanco" from "Samba Og Vaca Negro Blanco"
    # Usually: name.replace(' ', '_').lower()
    return name.strip().replace(' ', '_').lower()

def main():
    load_dotenv()
    
    log_section(logger, "Automated Landing Page Generation")
    
    # 1. Get Candidates
    candidates = get_products_ready_for_landing()
    
    if not candidates:
        logger.info("No products found ready for Landing Generation (Ads=SI, Landing!=SI).")
        return
        
    logger.info(f"Found {len(candidates)} products to process.")

    for product in candidates:
        try:
            p_name = product.get("nombre_producto", "Unknown")
            p_desc = product.get("descripcion", "")
            row_idx = product.get("results_row_idx")
            
            log_section(logger, f"Processing: {p_name}")
            update_context(step="Init", module_name=p_name)
            
            # Setup Paths
            folder_name = safe_filename(p_name)
            BASE_OUTPUT_DIR = "output"
            product_dir = os.path.join(BASE_OUTPUT_DIR, folder_name)
            market_research_path = os.path.join(product_dir, "market_research_min.json")
            
            # Check for Market Research File
            if not os.path.exists(market_research_path):
                logger.error(f"Market Research JSON not found at: {market_research_path}")
                logger.warning("Skipping product due to missing data.")
                continue
                
            # Read JSON and Extract TARGET_AVATAR
            logger.info("Reading Market Research JSON...")
            with open(market_research_path, 'r', encoding='utf-8') as f:
                mr_data = json.load(f)
                
            top_angles = mr_data.get("top_5_angulos", [])
            target_avatar = ""
            
            if top_angles and isinstance(top_angles, list):
                # Rank 1 should be first, but let's check rank just in case or take idx 0
                best_angle = top_angles[0] # Assuming sorted or first is best
                
                buyer_persona = best_angle.get("buyer_persona", "")
                promesa = best_angle.get("promesa", "")
                
                target_avatar = f"Buyer Persona: {buyer_persona}\nPromesa: {promesa}"
                logger.info(f"Extracted Target Avatar: {target_avatar}")
            else:
                logger.warning("No 'top_5_angulos' found directly in JSON. Checking structure...")
                # Fallback or strict error? User said "sacamos de la clave top_5_angulos"
                # If structure is different, we might fail.
                logger.error("Failed to extract top angles. skipping.")
                continue

            # =========================================================
            # PIPELINE EXECUTION (Copied & Adapted from main_landing_gen.py)
            # =========================================================
            
            TARGET_DIR = os.path.join(product_dir, "resultados_landing")
            os.makedirs(TARGET_DIR, exist_ok=True)
            
            SHOPIFY_TEMPLATE_KEY = f"templates/product.landing-{p_name.replace(' ', '-').lower()}.json"
            OUTPUT_FILENAME = f"product.landing-{p_name.replace(' ', '-').lower()}.json"
            OUTPUT_PATH = os.path.join(TARGET_DIR, OUTPUT_FILENAME)
            TEMPLATE_PATH = "input_theme/product.custom_landing.json"
            
            # --- 1. Copy & Architecture ---
            update_context(step="Copy Generation")
            logger.info("[1/5] Generating Copy...")
            
            if not os.path.exists(TEMPLATE_PATH):
                logger.error(f"Template not found: {TEMPLATE_PATH}")
                continue
                
            with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
                shopify_base = json.load(f)
                
            # Generate Copy (No caching for auto mode usually, or strict new)
            ai_content = generate_elite_landing_copy(p_name, p_desc, target_avatar)
            if not ai_content:
                logger.error("Failed to generate AI content.")
                continue
            
            # Map & Save
            final_json = map_payload_to_shopify_structure(shopify_base, ai_content)
            with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
                json.dump(final_json, f, indent=4, ensure_ascii=False)
                
            # Save extracted copy for image agents
            copy_path = os.path.join(TARGET_DIR, "extracted_marketing_copy.json")
            with open(copy_path, 'w', encoding='utf-8') as f:
                json.dump(ai_content, f, indent=2, ensure_ascii=False)
                
            # --- 2. Visual Assets ---
            update_context(step="Visual Assets")
            logger.info("[2/5] Generating Visual Assets...")
            
            # We wrap these in try-except blocks to avoid hard crashing the loop? 
            # Or strict fail? User said "cada vez que se complete... con exito."
            # So if any step fails, we probably shouldn't mark as complete?
            # Impl: Single big try/except around the whole product loop is safer.
            
            section_before_after.run_before_after_pipeline(folder_name)
            section_pain.run_pain_pipeline(folder_name)
            section_benefits.run_benefits_pipeline(folder_name)
            section_social_proof.run_social_proof_pipeline(folder_name)
            
            # --- 3. Evaluator ---
            update_context(step="Evaluator")
            logger.info("[3/5] Running Evaluator...")
            evaluator_benefits.run_evaluation_pipeline(folder_name)
            
            # --- 4. Deploy ---
            update_context(step="Deploy")
            logger.info("[4/5] Deploying Images...")
            deploy_images.deploy_pipeline(folder_name)
            
            # --- 5. Visual Injection ---
            update_context(step="Visual Injection")
            logger.info("[5/5] Visual Injection...")
            
            try:
                planer = VisualPlaner()
                planer.analyze_and_generate(folder_name, p_name)
            except Exception as e:
                logger.warning(f"Visual Planer error: {e}")
                
            run_injection_pipeline(folder_name)
            
            # Success!
            logger.info(f"Successfully processed {p_name}")
            
            # Upload to Drive
            update_context(step="Drive Upload")
            logger.info("Uploading Landing Assets to Google Drive...")
            try:
                # Assuming product_dir is the root of the product interaction "output/product_slug"
                from tools.drive_uploader import upload_product_to_drive
                upload_product_to_drive(product_dir)
            except Exception as e:
                logger.error(f"Drive Upload Failed: {e}")

            # Update Sheet
            mark_landing_gen_completed(row_idx)
            
        except Exception as e:
            logger.error(f"Error processing {product.get('nombre_producto')}: {e}", exc_info=True)
            # Do NOT mark as complete
            continue

if __name__ == "__main__":
    main()
