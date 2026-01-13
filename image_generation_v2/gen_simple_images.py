import os
import json
import time
import random
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Sequence
from dataclasses import dataclass

from dotenv import load_dotenv
from PIL import Image, ImageOps
from google import genai
from google.genai import types

from utils.logger import setup_logger, update_context, log_section
logger = setup_logger("Gen_SimpleImages")

# ---------------------------
# UTILS
# ---------------------------

SUPPORTED_IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def find_product_dir(output_root: str, product_name: str) -> Path:
    root = Path(output_root)
    # Check direct match first
    candidate = root / product_name
    if candidate.exists() and candidate.is_dir():
        return candidate
    
    # Check subdirectories
    for p in root.iterdir():
        if p.is_dir() and p.name == product_name:
            return p
            
    # Try slugified
    def slugify(s): return re.sub(r'[^a-z0-9]', '_', s.lower())
    slug = slugify(product_name)
    candidate = root / slug
    if candidate.exists() and candidate.is_dir():
        return candidate

    raise FileNotFoundError(f"Product directory for '{product_name}' not found in '{output_root}'.")

def load_reference_images(product_dir: Path, max_ref_images: int = 5) -> List[Image.Image]:
    img_dir = product_dir / "product_images"
    if not img_dir.exists():
        logger.warning(f"Reference images directory not found: {img_dir}")
        return []

    paths = [p for p in sorted(img_dir.iterdir()) if p.is_file() and p.suffix.lower() in SUPPORTED_IMG_EXTS]
    paths = paths[:max_ref_images]
    
    images = []
    for p in paths:
        try:
            img = Image.open(p)
            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGB")
            images.append(img)
        except Exception as e:
            logger.warning(f"Failed to load image {p.name}: {e}")
            
    return images

# ---------------------------
# GEMINI GENERATION
# ---------------------------

@dataclass
class RateLimiter:
    rpm: float
    _next_time: float = 0.0

    def wait_turn(self):
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = time.monotonic() + (60.0 / self.rpm)

def _build_image_config() -> types.GenerateContentConfig:
    # 4K, 1:1 Aspect Ratio
    return types.GenerateContentConfig(
        response_modalities=["IMAGE"],
        image_config=types.ImageConfig(aspect_ratio="1:1", image_size="4K"),
        candidate_count=1,
    )

def generate_image(
    client: genai.Client,
    model: str,
    prompt_str: str,
    ref_images: List[Image.Image],
) -> bytes:
    response = client.models.generate_content(
        model=model,
        contents=[prompt_str, *ref_images],
        config=_build_image_config(),
    )
    
    if not response.candidates:
        raise RuntimeError("No candidates returned.")
        
    # Extract image data
    for part in response.candidates[0].content.parts:
        if hasattr(part, "inline_data") and part.inline_data:
            return part.inline_data.data
        if hasattr(part, "as_image"):
            # Setup for PIL conversion if needed, but returning bytes is standard
            from io import BytesIO
            buf = BytesIO()
            part.as_image().save(buf, format="PNG")
            return buf.getvalue()

    raise RuntimeError("No image data found in response.")

def generate_with_retry(
    client: genai.Client,
    models: List[str],
    prompt_str: str,
    ref_images: List[Image.Image],
    limiter: RateLimiter,
) -> Tuple[bytes, str]:
    last_err = None
    
    for model in models:
        for attempt in range(1, 4): # 3 attempts per model
            try:
                limiter.wait_turn()
                logger.debug(f"Generating with {model} (Attempt {attempt})...")
                img_bytes = generate_image(client, model, prompt_str, ref_images)
                return img_bytes, model
            except Exception as e:
                last_err = e
                logger.warning(f"Error: {e}")
                if "429" in str(e) or "quota" in str(e).lower():
                    time.sleep(5 * attempt) # Backoff
                else:
                    break # Try next model if non-quota error
                    
    raise RuntimeError(f"Failed to generate image after retries. Last error: {last_err}")

# ---------------------------
# MAIN LOGIC
# ---------------------------

def run_simple_image_generation(
    product_name: str,
    output_root: str = "output",
    results_dir_name: str = "_results_2",
    api_key: Optional[str] = None,
):
    load_dotenv()
    if not api_key:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logger.error("GEMINI_API_KEY not found.")
            return

    client = genai.Client(api_key=api_key)
    limiter = RateLimiter(rpm=5.0) # Conservative RPM
    models = ["gemini-3-pro-image-preview"]

    try:
        product_dir = find_product_dir(output_root, product_name)
    except FileNotFoundError as e:
        logger.error(f"{e}")
        return

    # Paths
    simple_images_dir = product_dir / results_dir_name / "simple_images"
    output_gen_dir = product_dir / results_dir_name / "generated_images" / "simple"
    output_gen_dir.mkdir(parents=True, exist_ok=True)
    
    ref_images = load_reference_images(product_dir)
    logger.info(f"Loaded {len(ref_images)} reference images.")

    if not simple_images_dir.exists():
        logger.warning(f"No simple_images directory found at {simple_images_dir}")
        return

    # Find JSONs
    json_files = list(simple_images_dir.glob("*_image.json"))
    if not json_files:
        logger.warning("No *_image.json files found.")
        return

    logger.info(f"Found {len(json_files)} angle files.")

    for json_file in json_files:
        logger.info(f"Processing {json_file.name}...")
        try:
            data = load_json(json_file)
        except Exception as e:
            logger.error(f"Failed to load JSON {json_file}: {e}")
            continue
        
        # Structure: schema_version, render_variants: [ {variant_id, nanobanana_prompt: {...}}, ... ]
        render_variants = data.get("render_variants", [])
        if not render_variants:
            logger.warning("No render_variants found.")
            continue
            
        for variant in render_variants:
            variant_id = variant.get("variant_id", "unknown")
            nb_prompt = variant.get("nanobanana_prompt", {})
            
            # Enforce 1:1 in prompt just in case
            if "format" not in nb_prompt: nb_prompt["format"] = {}
            nb_prompt["format"]["aspect_ratio"] = "1:1"
            nb_prompt["format"]["resolution"] = "1080x1080"
            
            # Inject Product Lock Rule
            if "input_assets" not in nb_prompt: nb_prompt["input_assets"] = {}
            nb_prompt["input_assets"]["product_lock_rule"] = (
                "CRITICAL: The provided images are the EXACT product reference. "
                "You MUST generate the product exactly as shown in these reference images. "
                + str(nb_prompt["input_assets"].get("product_lock_rule", ""))
            )

            prompt_str = json.dumps(nb_prompt, indent=2)
            
            # Filename: {angle_id}_{variant_id}.png
            # Derive angle_id from filename or data? Filename is safer: {angle_id}_image.json
            angle_id = json_file.stem.replace("_image", "")
            out_filename = f"{angle_id}_{variant_id}.png"
            out_path = output_gen_dir / out_filename
            
            if out_path.exists():
                logger.info(f"Skipping {out_filename} (Exists)")
                continue
                
            logger.info(f"Generating {out_filename}...")
            
            try:
                img_bytes, used_model = generate_with_retry(
                    client, models, prompt_str, ref_images, limiter
                )
                
                with open(out_path, "wb") as f:
                    f.write(img_bytes)
                logger.info(f"Saved to {out_path} ({used_model})")
                
                # Save prompt used
                with open(output_gen_dir / f"{angle_id}_{variant_id}_prompt.json", "w") as f:
                    f.write(prompt_str)
                    
            except Exception as e:
                logger.error(f"Failed to generate {out_filename}: {e}")

if __name__ == "__main__":
    # Test block
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--product_name", required=True)
    args = parser.parse_args()
    
    run_simple_image_generation(args.product_name)
