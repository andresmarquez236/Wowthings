import os
import json
import time
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from PIL import Image
from openai import OpenAI
from google import genai
from google.genai import types

# Load environment variables
load_dotenv()

# Initialize Clients
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# ---------------------------------------------------------
# CONSTANTS & CONFIG
# ---------------------------------------------------------
SYSTEM_PROMPT = """
You are SectionPromptAgent_01_BeforeAfter, an expert prompt engineer for conversion-focused e-commerce visuals.

TASK
Given (1) section copy for a BEFORE/AFTER transformation, (2) product fingerprint, (3) reference images, and (4) style/output constraints, generate TWO NanoBananaPro prompt bundles:
- bundle_before: depicts the “before” state
- bundle_after: depicts the “after” state

HARD RULES (non-negotiable)
1) Scene lock: BEFORE and AFTER must match: same camera angle, lens feel, distance, lighting, background, model/props, framing.
2) Single-variable change: ONLY change what the product fingerprint allows in “what_can_change_in_after”.
3) Product fidelity: the product must match reference images (shape/material/color/details). Do not invent logos, patterns, accessories, claims, or features not in verifiable_attributes.
4) No text inside image: DO NOT render any typography, captions, watermarks, UI, logos, or symbols. The landing already has text.
5) Photorealism: realistic, high-quality. Avoid “AI-looking” artifacts.
6) Safety: no prohibited content, no medical claims, no unrealistic transformations.

COMPOSITION GOAL
- BEFORE should visually communicate the problem baseline (plain, boring, not standout) WITHOUT degrading aesthetics.
- AFTER should visually communicate the desirable transformation (standout, premium, “camera-ready”) while staying plausible and consistent.

OUTPUT FORMAT
Return valid JSON ONLY with the following schema:
{
  "section_key": "transformation_highlight",
  "title": "...",
  "description": "...",
  "labels": {"before": "...", "after": "..."},
  "bundle_before": {
    "prompt_en": "...",
    "negative_prompt_en": "...",
    "aspect_ratio": "1:1|4:5|16:9",
    "must_match_reference_images": true,
    "text_in_image": "none",
    "composition_rules": ["...","...","..."]
  },
  "bundle_after": {
    "prompt_en": "...",
    "negative_prompt_en": "...",
    "aspect_ratio": "1:1|4:5|16:9",
    "must_match_reference_images": true,
    "text_in_image": "none",
    "composition_rules": ["...","...","..."]
  }
}
"""

SUPPORTED_IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

# ---------------------------------------------------------
# UTILS
# ---------------------------------------------------------
def clean_text(html_text):
    if not html_text: return ""
    text = re.sub(r'^<p>', '', html_text)
    text = re.sub(r'</p>$', '', text)
    return text

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def find_file(directory: Path, pattern: str) -> Optional[Path]:
    for f in directory.iterdir():
        if f.is_file() and pattern in f.name:
            return f
    return None

def load_reference_images(product_dir: Path, max_ref_images: int = 4) -> List[Image.Image]:
    img_dir = product_dir / "product_images"
    if not img_dir.exists():
        print(f"⚠️ No product_images folder found at {img_dir}")
        return []
    
    paths = [p for p in sorted(img_dir.iterdir()) if p.is_file() and p.suffix.lower() in SUPPORTED_IMG_EXTS]
    # Limit references
    paths = paths[:max_ref_images]
    
    images = []
    for p in paths:
        try:
            img = Image.open(p)
            if img.mode != "RGB": img = img.convert("RGB")
            images.append(img)
        except Exception as e:
            print(f"⚠️ Failed to load image {p.name}: {e}")
    return images

# ---------------------------------------------------------
# STEP 1: PROMPT GENERATION (OpenAI)
# ---------------------------------------------------------
def generate_prompts_payload(product_name, landing_data, market_data):
    # Extract Copy
    sections = landing_data.get('sections', {})
    # Look for compare-image section
    compare_section = None
    for key, val in sections.items():
        if val.get('type') == 'compare-image':
            compare_section = val
            break
            
    if not compare_section:
        print("❌ 'compare-image' section not found in landing JSON.")
        return None

    settings = compare_section.get('settings', {})
    
    section_copy = {
        "title": settings.get('title', ''),
        "description": clean_text(settings.get('text', '')),
        "label_before": settings.get('before', 'ANTES'),
        "label_after": settings.get('after', 'DESPUÉS')
    }
    
    # Construct Fingerprint from Market Data
    # Assume market_data has basic info. If strictly needed fields are missing, provide intelligent defaults.
    fingerprint = {
        "product_name": product_name,
        "category": market_data.get('category', 'General Product'),
        "verifiable_attributes": market_data.get('features', []), # Mapping 'features' to attributes
        "forbidden_claims": ["no impossible results", "no fake badges"],
        "what_can_change_in_after": ["product usage", "visual appeal", "lighting"]
    }
    
    input_data = {
        "section_key": "transformation_highlight",
        "section_copy": section_copy,
        "product_fingerprint": fingerprint,
        "reference_images_pack": {"note": "Images provided at generation time"},
        "style_guide_global": {
            "visual_mode": "ugc_clean",
            "lighting": "soft natural daylight",
            "camera_feel": "iphone_ugc",
            "text_in_image_policy": "none"
        },
        "output_spec": {
            "aspect_ratio": "1:1",
            "resolution": 2048,
            "safe_space_policy": "leave clean negative space"
        }
    }
    return input_data

def call_prompt_agent(input_data):
    user_prompt = f"Please generate the BEFORE and AFTER prompt bundles based on this input data:\n{json.dumps(input_data, indent=2)}"
    print(f"🧠 Generating Prompts...")
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5.1",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"❌ Error generating prompts: {e}")
        return None

# ---------------------------------------------------------
# STEP 2: IMAGE GENERATION (Gemini)
# ---------------------------------------------------------
def generate_image_gemini(prompt: str, ref_images: List[Image.Image] = [], context_image: Image.Image = None):
    """
    Generates an image using Gemini.
    """
    # Build contents
    contents = [prompt]
    if context_image:
        contents.append(context_image) # Add contextual image (e.g. Before image)
    if ref_images:
        contents.extend(ref_images)    # Add product references
        
    config = types.GenerateContentConfig(
        response_modalities=["IMAGE"],
        image_config=types.ImageConfig(aspect_ratio="1:1", image_size="4K"),
        candidate_count=1,
    )
    
    print(f"🎨 Generating Image with Gemini... (Refs: {len(ref_images)}, Context: {bool(context_image)})")
    try:
        response = gemini_client.models.generate_content(
            model="gemini-3-pro-image-preview",
            contents=contents,
            config=config,
        )
        if response.candidates:
             for part in response.candidates[0].content.parts:
                # Prioritize raw bytes if available
                if hasattr(part, "inline_data") and part.inline_data:
                    from io import BytesIO
                    return Image.open(BytesIO(part.inline_data.data))
                
                # Fallback to as_image()
                if hasattr(part, "as_image"):
                    try:
                        img = part.as_image()
                        # If it's a PIL Image, verify save method
                        from PIL import Image as PILImage
                        if isinstance(img, PILImage.Image):
                            return img
                        else:
                            # It might be an IPython image or similar wrapper
                            # Try getting data directly 
                            if hasattr(img, "data"):
                                from io import BytesIO
                                return Image.open(BytesIO(img.data))
                            return img # Hope it acts like a PIL image
                    except Exception as e:
                        print(f"⚠️ Error processing as_image: {e}")
                        pass
        return None
    except Exception as e:
        print(f"❌ Image Generation Failed: {e}")
        return None

# ---------------------------------------------------------
# ORCHESTRATOR
# ---------------------------------------------------------
def run_before_after_pipeline(product_folder_name: str):
    base_dir = Path("output") / product_folder_name
    
    if not base_dir.exists():
        print(f"❌ Directory not found: {base_dir}")
        return

    # 1. Locate Data Files
    results_dir = base_dir / "resultados_landing"
    landing_json_path = find_file(results_dir, "product.landing") or find_file(base_dir, "product.landing") or find_file(Path("output"), f"product.landing-{product_folder_name.replace('_', '-')}")
    market_json_path = find_file(base_dir, "market_research") # e.g., market_research_min.json
    
    if not landing_json_path:
        print("❌ Landing JSON not found.")
        return
        
    print(f"📂 Found Landing JSON: {landing_json_path}")
    
    landing_data = load_json(landing_json_path)
    market_data = load_json(market_json_path) if market_json_path else {}
    
    product_name = market_data.get('product_name', product_folder_name.replace('_', ' ').title())

    # 2. Extract & Generate Prompts
    input_payload = generate_prompts_payload(product_name, landing_data, market_data)
    if not input_payload: return

    prompts_result = call_prompt_agent(input_payload)
    if not prompts_result: return
    
    # Save Prompts
    results_dir = base_dir / "resultados_landing"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    with open(results_dir / "before_after_prompts.json", "w", encoding="utf-8") as f:
        json.dump(prompts_result, f, indent=2, ensure_ascii=False)
    print(f"✅ Prompts saved to {results_dir / 'before_after_prompts.json'}")

    # 3. Load References
    ref_images = load_reference_images(base_dir)
    print(f"📸 Loaded {len(ref_images)} Product Reference Images")

    # 4. Generate 'BEFORE' Image
    # 'BEFORE' usually implies the *absence* of the product or the *problem*, so we might strictly rely on the prompt 
    # and maybe NOT pass product references if the prompt says "generic old shoes". 
    # However, user said: "leemos las imagenes del producto y primero creamos la imagen before".
    # I'll pass them but rely on prompt to say "don't show these specific details" if needed, 
    # OR if the prompt asks for "boring sneakers", Gemini might struggle if we pass "cool sneakers" as ref.
    # STRATEGY: For BEFORE, if the prompt describes "generic", passing specific refs might bleed features.
    # But for "After" logic consistency, I will follow user instruction. TO be safe, I will pass refs.
    
    bundle_before = prompts_result.get("bundle_before", {})
    prompt_before = bundle_before.get("prompt_en", "")
    
    print("\n--- GENERATING BEFORE IMAGE ---")
    before_img = generate_image_gemini(prompt_before, ref_images) # Passing refs as requested
    
    if before_img:
        before_path = results_dir / "before_image.png"
        before_img.save(before_path)
        print(f"✅ 'Before' image saved: {before_path}")
    else:
        print("❌ Failed 'Before' image.")
        return

    # 5. Generate 'AFTER' Image
    # Input: Prompt + Product Refs + Before Image
    bundle_after = prompts_result.get("bundle_after", {})
    prompt_after = bundle_after.get("prompt_en", "") + " Make it the efficient, high-quality after state of the provided before image."
    
    print("\n--- GENERATING AFTER IMAGE ---")
    after_img = generate_image_gemini(prompt_after, ref_images, context_image=before_img)
    
    if after_img:
        after_path = results_dir / "after_image.png"
        after_img.save(after_path)
        print(f"✅ 'After' image saved: {after_path}")
    else:
        print("❌ Failed 'After' image.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        product_folder = sys.argv[1]
        run_before_after_pipeline(product_folder)
    else:
        # Default test
        run_before_after_pipeline("samba_og_vaca_negro_blanco")
