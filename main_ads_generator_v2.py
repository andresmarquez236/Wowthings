
import os
import sys
import argparse
import json
import subprocess
import shutil
from datetime import datetime
from typing import List, Dict, Any

# Add current dir to sys.path to allow imports
sys.path.append(os.getcwd())

from utils.logger import setup_logger, update_context, log_section

# --- Configuration ---
SCRIPTS_DIR = "ads_generator_v2"
PYTHON_CMD = sys.executable
logger = setup_logger("AdsGenV2")

def run_agent(script_name: str, args: List[str]):
    """Runs a python agent script with arguments."""
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    if not os.path.exists(script_path):
        # Fallback: try raw path (e.g. image_generation_v2/...)
        if os.path.exists(script_name):
            script_path = script_name
        else:
             logger.error(f"Script not found: {script_name}")
             return False

    cmd = [PYTHON_CMD, script_path] + args
    
    logger.info(f"Running {script_name}")
    # logger.debug(f"Command: {' '.join(cmd)}")
    
    # Ensure subprocess can find 'utils' module by setting PYTHONPATH
    env = os.environ.copy()
    current_cwd = os.getcwd()
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{current_cwd}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = current_cwd

    # Stream output using Popen so we see sub-script logs (which will include their own timestamps)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, 
        text=True,
        bufsize=1,
        env=env
    )

    for line in process.stdout:
        print(line, end='') # Direct stream of sub-logger output
    
    process.wait()
    
    if process.returncode != 0:
        logger.error(f"Error running {script_name} (Exit Code: {process.returncode})")
        return False
    
    logger.info(f"{script_name} completed.")
    return True

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def main():
    parser = argparse.ArgumentParser(description="Ads Generator V2 Orchestrator")
    parser.add_argument("--input_path", required=True, help="Path to initial product JSON (or raw text file)")
    parser.add_argument("--num_angles", type=int, default=1, help="Number of angles to generate")
    parser.add_argument("--output_base", default="_results_2", help="Base directory for organized results")
    parser.add_argument("--run_id", default=None, help="Optional run identifier")
    
    args = parser.parse_args()

    # 1. Setup Directories
    # Logic: Output should be inside the product directory derived from input_path
    
    # Determine Product Directory
    input_abs_path = os.path.abspath(args.input_path)
    if os.path.isfile(input_abs_path):
        product_dir = os.path.dirname(input_abs_path)
        product_name = os.path.basename(product_dir) # heuristic
    else:
        product_dir = input_abs_path
        product_name = os.path.basename(product_dir)

    # Determine Output Base
    if args.output_base == "_results_2":
        # Use product-relative path FLATTENED (no timestamp)
        run_dir = os.path.join(product_dir, "_results_2")
    else:
        run_dir = args.output_base

    ensure_dir(run_dir)
    
    # Subfolders for organized output
    dirs = {
        "briefs": os.path.join(run_dir, "0_briefs"),
        "strategy": os.path.join(run_dir, "1_strategy"),
        "compliance": os.path.join(run_dir, "2_compliance"),
        "images": os.path.join(run_dir, "simple_images"),
        "carousels": os.path.join(run_dir, "carousels"),
        "video_prompts": os.path.join(run_dir, "video_prompts"),
        "thumbnails": os.path.join(run_dir, "thumbnails"),
        "qa_reports": os.path.join(run_dir, "qa_reports")
    }
    
    for d in dirs.values():
        ensure_dir(d)

    print(f"📂 Output Directory: {run_dir}")
    logger.info(f"Output Directory: {run_dir}")

    # 2. Agent 0: Product Extractor
    # Check if input is ALREADY a valid Agent 0 output (schema check) or raw
    # For simplicity, we ALWAYS run Agent 0 to normalize.
    brief_file_path = os.path.join(dirs["briefs"], "product_brief.json")
    
    update_context(step="Agent 0: Extraction")
    if not run_agent("agent_0_product_extractor.py", [
        "--input_path", args.input_path,
        "--output_file", brief_file_path
    ]):
        logger.error("Pipeline stopped at Agent 0.")
        sys.exit(1)

    # 3. Agent 1: Strategist (Dynamic Angles)
    angles_file_path = os.path.join(dirs["strategy"], "angles.json")
    
    update_context(step="Agent 1: Strategy")
    if not run_agent("agent_1_strategist.py", [
        "--brief_path", brief_file_path,
        "--output_file", angles_file_path,
        "--num_angles", str(args.num_angles)
    ]):
        logger.error("Pipeline stopped at Agent 1.")
        sys.exit(1)

    # 4. Agent 2: Compliance Pre-Check
    compliance_file_path = os.path.join(dirs["compliance"], "compliance_review.json")
    
    update_context(step="Agent 2: Compliance")
    if not run_agent("agent_2_compliance.py", [
        "--brief_path", brief_file_path,
        "--angles_path", angles_file_path,
        "--output_file", compliance_file_path
    ]):
        logger.error("Pipeline stopped at Agent 2.")
        sys.exit(1)

    # 5. Iteration over Angles
    log_section(logger, "Creative Generation Phase")
    
    # Load angles to iterate
    angles_data = load_json(angles_file_path)
    angles_list = angles_data.get("angles", [])
    
    if not angles_list:
        logger.error("No angles found!")
        sys.exit(1)

    for angle in angles_list:
        angle_id = angle.get("angle_id")
        log_section(logger, f"Processing {angle_id}")
        update_context(step=f"Angle Loop: {angle_id}")
        
        # Paths for this angle's assets
        image_out = os.path.join(dirs["images"], f"{angle_id}_image.json")
        carousel_out = os.path.join(dirs["carousels"], f"{angle_id}_carousel.json")
        video_out = os.path.join(dirs["video_prompts"], f"{angle_id}_video.json")
        thumb_out = os.path.join(dirs["thumbnails"], f"{angle_id}_thumbnails.json")
        qa_out = os.path.join(dirs["qa_reports"], f"{angle_id}_qa.json")
        
        # RUN AGENT 3 (Single Image)
        if not run_agent("agent_3_single_image.py", [
            "--brief_path", brief_file_path,
            "--angles_path", angles_file_path,
            "--compliance_path", compliance_file_path,
            "--angle_id", angle_id,
            "--output_file", image_out
        ]):
            logger.warning(f"Failed Agent 3 for {angle_id}")
            continue
        
        # RUN AGENT 4 (Carousel)
        if not run_agent("agent_4_carousel.py", [
            "--brief_path", brief_file_path,
            "--angles_path", angles_file_path,
            "--compliance_path", compliance_file_path,
            "--angle_id", angle_id,
            "--output_file", carousel_out
        ]):
            logger.warning(f"Failed Agent 4 for {angle_id}")
        
        # RUN AGENT 5 (Video)
        if not run_agent("agent_5_video.py", [
            "--brief_path", brief_file_path,
            "--angles_path", angles_file_path,
            "--compliance_path", compliance_file_path,
            "--angle_id", angle_id,
            "--output_file", video_out
        ]):
            logger.warning(f"Failed Agent 5 for {angle_id}")

        # RUN AGENT 5b (Thumbnails)
        if not run_agent("agent_5b_thumbnail.py", [
            "--brief_path", brief_file_path,
            "--angles_path", angles_file_path,
            "--compliance_path", compliance_file_path,
            "--angle_id", angle_id,
            "--output_file", thumb_out
        ]):
            logger.warning(f"Failed Agent 5b for {angle_id}")


        # RUN AGENT 6 (QA)
        assets_args = []
        if os.path.exists(image_out): assets_args.append(image_out)
        if os.path.exists(carousel_out): assets_args.append(carousel_out)
        if os.path.exists(video_out): assets_args.append(video_out)
        if os.path.exists(thumb_out): assets_args.append(thumb_out) 
        
        if assets_args:
             run_agent("agent_6_qa.py", [
                "--brief_path", brief_file_path,
                "--angles_path", angles_file_path,
                "--compliance_path", compliance_file_path,
                "--angle_id", angle_id,
                "--output_file", qa_out,
                "--assets"
            ] + assets_args)
        else:
            logger.warning(f"No assets generated for {angle_id}, skipping QA.")

    logger.info("Content generation validation passed.")

    # 6. VISUAL GENERATION (Gemini)
    log_section(logger, "Visual Generation Phase (Gemini)")
    update_context(step="Visual Generation")
    
    # Calculate root for generation scripts
    gen_root = os.path.dirname(product_dir)
    gen_product = os.path.basename(product_dir)
    
    gen_scripts = [
        "image_generation_v2/gen_simple_images.py",
        "image_generation_v2/gen_carousels.py",
        "image_generation_v2/gen_thumbnails.py"
    ]

    for script in gen_scripts:
        if not run_agent(script, [
            "--product_name", gen_product,
        ]):
             logger.warning(f"{script} failed or skipped.")

    logger.info("Pipeline Finished Successfully.")
    logger.info(f"Find results in: {run_dir}")

if __name__ == "__main__":
    main()
