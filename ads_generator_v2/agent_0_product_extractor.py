
import os
import sys
import argparse
import json
import glob
import base64
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Add root to sys.path to allow imports if needed
sys.path.append(os.getcwd())

from utils.logger import setup_logger, update_context, log_section
logger = setup_logger("Agent0_Extractor")

SYSTEM_PROMPT = """You are Agent 0: Product Extractor (Multimodal).

Your ONLY job: read images/metadata and output a STRICT JSON object matching "creative_product_extractor_v2".

Output Structure (MUST BE NESTED):
{
  "product_brief": {
    "product_name": "...",
    "price": "...",
    "description": "...",
    "category_hypotheses": [{"category": "...", "confidence": 0.9}],
    "key_features_benefits": [{"feature": "...", "benefit": "...", "evidence": [], "risk_level": "low"}],
    "compliance_risks": [{"risk": "...", "risk_level": "high"}],
    "downstream_constraints": "...",
    "what_is_unclear_or_missing": []
  },
  "visual_brief": {
    "recommended_shots": [{"shot_type": "vertical", "description": "...", "must_have": true}],
    "creative_style_hints_from_class": []
  }
}

Do NOT:
- do market research
- propose angles
- write ad copy
- invent specs

You MUST:
- separate “verifiable” vs “hypotheses”
- attach evidence pointers
- flag compliance risks
"""

DEVELOPER_PROMPT = """Follow creative class constraints:
1) Meta placements: vertical 9:16 priority.
2) Copy: AIDA, short (<280 chars).
3) Avoid unlikely promises.
4) Beauty: magazine aesthetic.
5) Fashion: product hero.
"""

USER_PROMPT_TEMPLATE = """INPUT:
product_name: {product_name}
price_raw: {price_raw}
description_raw: {description_raw}

images:
{images_list}

TASK:
Return the STRICT JSON object following "creative_product_extractor_v2" (nested `product_brief` and `visual_brief`).
"""

def get_images_from_dir(directory: str) -> List[str]:
    if not directory or not os.path.exists(directory):
        return []
    valid_exts = ["*.png", "*.jpg", "*.jpeg", "*.webp"]
    image_paths = []
    for ext in valid_exts:
        image_paths.extend(glob.glob(os.path.join(directory, ext)))
    
    # Sort for consistency (e.g. 1.png, 2.png...)
    image_paths.sort()
    return image_paths

def encode_image(image_path: str) -> str:
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def load_json_file(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON {path}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Agent 0: Product Extractor (OpenAI)")
    parser.add_argument("--input_path", help="Path to input JSON file (can be raw specs OR already processed brief)")
    parser.add_argument("--product_name", help="Name of the product (override)")
    parser.add_argument("--price", help="Price of the product (override)")
    parser.add_argument("--description", help="Description/metadata of the product (override)")
    parser.add_argument("--images_dir", help="Directory containing product images (override)")
    parser.add_argument("--output_file", required=True, help="Path to save the output JSON")
    
    args = parser.parse_args()

    # 1. Input Resolution
    product_name = args.product_name
    price = args.price
    description = args.description
    images_dir = args.images_dir

    if args.input_path:
        input_data = load_json_file(args.input_path)
        if input_data:
            # CASE A: Already Nested (Perfect)
            if "product_brief" in input_data and "visual_brief" in input_data:
                logger.info("Input file appears to be a processed Brief. Skipping extraction.")
                with open(args.output_file, "w", encoding="utf-8") as f:
                    json.dump(input_data, f, indent=2, ensure_ascii=False)
                logger.info(f"Success! Copied input to {args.output_file}")
                return

            # CASE B: Flat Structure (Legacy/Previous Run) - MIGRATION
            if "key_features_benefits" in input_data and "recommended_shots" in input_data:
                logger.warning("Input file is Flat Structure (v1). Migrating to Nested Structure (v2)...")
                # ... existing migration logic if needed ...
                pass 

            # CASE C: Market Research Min Structure (Standard Automation Input)
            if "input" in input_data and isinstance(input_data["input"], dict):
                logger.info("Input file appears to be Market Research data.")
                inp = input_data["input"]
                product_name = product_name or inp.get("nombre_producto") or inp.get("product_name")
                price = price or inp.get("precio") or inp.get("price")
                description = description or inp.get("descripcion") or inp.get("description")
                
                # Auto-detect images_dir relative to input file
                if not images_dir and args.input_path:
                    base_dir = os.path.dirname(os.path.abspath(args.input_path))
                    # Try 'product_images' sibling
                    candidate = os.path.join(base_dir, "product_images")
                    if os.path.exists(candidate) and os.path.isdir(candidate):
                        images_dir = candidate
                    else:
                        # Try base dir itself
                        images_dir = base_dir

            # CASE D: Raw Data (Legacy Fallback)
            if not product_name: product_name = input_data.get("product_name")
            if not price: price = input_data.get("price")
            if not description: description = input_data.get("description")
            if not images_dir: images_dir = input_data.get("images_dir")
    
    # Validation
    if not all([product_name, price, description, images_dir]):
        logger.error("Missing required inputs. Provide via --input_path JSON or CLI args.")
        logger.error(f"Got: name={bool(product_name)}, price={bool(price)}, desc={bool(description)}, img_dir={bool(images_dir)}")
        sys.exit(1)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not found in environment variables.")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    # 2. Load and Encode Images
    image_paths = get_images_from_dir(images_dir)
    if not image_paths:
        logger.error(f"No images found in {images_dir}")
        sys.exit(1)

    logger.info(f"Found {len(image_paths)} images in {images_dir}.")
    
    images_content = []
    images_list_str_parts = []
    
    for idx, p in enumerate(image_paths):
        try:
            base64_image = encode_image(p)
            images_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
            })
            images_list_str_parts.append(f"img:{idx+1} = {os.path.basename(p)}")
        except Exception as e:
            logger.warning(f"Could not load/encode image {p}: {e}")

    images_list_str = "\n".join(images_list_str_parts)

    # 3. Build Prompt
    user_prompt_text = USER_PROMPT_TEMPLATE.format(
        product_name=product_name,
        price_raw=price,
        description_raw=description,
        images_list=images_list_str
    )

    full_system_prompt = SYSTEM_PROMPT + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_prompt_text},
                *images_content
            ]
        }
    ]

    # 4. Call API
    logger.info("Extracting product details...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"},
            max_tokens=4000,
            temperature=0.0, 
        )
        
        content = response.choices[0].message.content
        
        if content:
            try:
                data = json.loads(content)
                with open(args.output_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Success! Output saved to {args.output_file}")
            except json.JSONDecodeError as e:
                logger.error("Model output resulted in invalid JSON.")
                logger.debug(f"Raw output: {content}")
                sys.exit(1)
        else:
            logger.error("Empty response from model.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
