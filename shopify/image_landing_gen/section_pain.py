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
# CONSTANTS & PROMPTS
# ---------------------------------------------------------
SYSTEM_PROMPT = """
You are SectionPromptAgent_02_Pain, an elite prompt engineer for conversion-focused e-commerce landings.

TASK
Create ONE NanoBananaPro prompt bundle for the “Pain reinforcement” image-with-text section, using:
- section copy (pain heading + narrative),
- product fingerprint (verifiable attributes + forbidden claims),
- reference images (optional subtle inclusion),
- global style guide and output constraints.

HARD RULES (non-negotiable)
1) No text in image: DO NOT generate typography, captions, stickers, UI, watermarks, logos, or symbols.
2) Truthfulness: Do not imply medical/guaranteed outcomes or unverified claims. No “certified”, “doctor approved”, etc unless explicitly allowed.
3) Product fidelity: If the product is shown, it must match the reference images exactly (shape/material/color/details). No invented features.
4) Emotional realism: The scene must feel authentic and relatable (UGC or clean lifestyle), not staged or “AI glossy”.
5) Composition: Keep the image simple, with clear focal point and minimal clutter.

DECISION: PRODUCT VISIBILITY
Choose ONE:
- "none": no product visible (pure pain context).
- "subtle": product appears in the scene but NOT as a hero (e.g., on a table, partially out-of-focus).
Default to "subtle" unless showing the product would contradict the pain story.

SCENE SELECTION (choose best fit)
Select one archetype that matches the narrative:
A) Mirror/closet frustration (fashion/appearance pain)
B) Desk/room overwhelm (productivity/organization pain)
C) Bathroom/self-care dissatisfaction (beauty/grooming pain, non-medical)
D) Everyday usage friction (gadgets/home products)
Keep it culturally neutral and broadly applicable.

OUTPUT FORMAT
Return valid JSON ONLY:
{
  "section_key": "pain_point_resolution",
  "heading": "...",
  "narrative": "...",
  "product_visibility": "none|subtle|hero",
  "bundle": {
    "prompt_en": "...",
    "negative_prompt_en": "...",
    "aspect_ratio": "1:1|4:5|16:9",
    "must_match_reference_images": true|false,
    "text_in_image": "none",
    "composition_rules": ["...","...","..."],
    "safety_constraints": ["...","..."]
  }
}

SELF-AUDIT (must pass)
- No text/logos/UI?
- Pain is clear without “before/after” transformation?
- If product shown: matches refs exactly and appears subtly?
If any check fails, rewrite and re-output JSON only.
"""

SUPPORTED_IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

# ---------------------------------------------------------
# UTILS
# ---------------------------------------------------------
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
def generate_pain_prompt_payload(product_name, extracted_copy, market_data):
    # Extract copy from extracted_marketing_copy.json
    pain_section = extracted_copy.get('pain_point_resolution', {})
    
    # Fingerprint construction
    fingerprint = {
        "product_name": product_name,
        "category": market_data.get('category', 'General Product'),
        "verifiable_attributes": market_data.get('features', []),
        "forbidden_claims": ["no impossible outcomes", "no fake medical advice"]
    }
    
    input_data = {
        "section_key": "pain_point_resolution",
        "section_copy": {
            "heading": pain_section.get('heading', ''),
            "narrative": pain_section.get('narrative', '')
        },
        "product_fingerprint": fingerprint,
        "reference_images_pack": {"note": "Images provided at generation time"},
        "style_guide_global": {
            "visual_mode": "ugc_clean",
            "lighting": "soft natural daylight",
            "color_grade": "neutral, high clarity",
            "camera_feel": "iphone_ugc",
            "text_in_image_policy": "none"
        },
        "output_spec": {
            "aspect_ratio": "4:5",
            "resolution": 2048,
            "background_policy": "realistic lifestyle, not busy"
        }
    }
    return input_data

def call_prompt_agent(input_data):
    user_prompt = f"Please generate the PAIN REINFORCEMENT prompt bundle based on this input data:\n{json.dumps(input_data, indent=2)}"
    print(f"🧠 Generating Pain Prompt...")
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
def generate_image_gemini(prompt: str, ref_images: List[Image.Image] = []):
    """
    Generates an image using Gemini.
    """
    contents = [prompt]
    if ref_images:
        contents.extend(ref_images)
        
    config = types.GenerateContentConfig(
        response_modalities=["IMAGE"],
        image_config=types.ImageConfig(aspect_ratio="4:5", image_size="4K"), # 4:5 as requested
        candidate_count=1,
    )
    
    print(f"🎨 Generating Pain Image with Gemini... (Refs: {len(ref_images)})")
    try:
        response = gemini_client.models.generate_content(
            model="gemini-3-pro-image-preview",
            contents=contents,
            config=config,
        )
        if response.candidates:
             for part in response.candidates[0].content.parts:
                # Prioritize raw bytes
                if hasattr(part, "inline_data") and part.inline_data:
                    from io import BytesIO
                    return Image.open(BytesIO(part.inline_data.data))
                
                # Fallback to as_image()
                if hasattr(part, "as_image"):
                    try:
                        img = part.as_image()
                        from PIL import Image as PILImage
                        if isinstance(img, PILImage.Image):
                            return img
                        if hasattr(img, "data"):
                            from io import BytesIO
                            return Image.open(BytesIO(img.data))
                        return img
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
def run_pain_pipeline(product_folder_name: str):
    base_dir = Path("output") / product_folder_name
    results_dir = base_dir / "resultados_landing"
    
    if not results_dir.exists():
        print(f"❌ Results directory not found: {results_dir}. Did you run section_landing.py?")
        return

    # 1. Locate Data Files
    extracted_json_path = results_dir / "extracted_marketing_copy.json"
    market_json_path = find_file(base_dir, "market_research")
    
    if not extracted_json_path.exists():
        print("❌ extracted_marketing_copy.json not found.")
        return
        
    print(f"📂 Found Extracted Copy: {extracted_json_path}")
    
    extracted_data = load_json(extracted_json_path)
    market_data = load_json(market_json_path) if market_json_path else {}
    
    product_name = market_data.get('product_name', product_folder_name.replace('_', ' ').title())

    # 2. Extract & Generate Prompts
    input_payload = generate_pain_prompt_payload(product_name, extracted_data, market_data)
    prompts_result = call_prompt_agent(input_payload)
    if not prompts_result: return
    
    # Save Prompts
    with open(results_dir / "pain_prompts.json", "w", encoding="utf-8") as f:
        json.dump(prompts_result, f, indent=2, ensure_ascii=False)
    print(f"✅ Pain Prompts saved to {results_dir / 'pain_prompts.json'}")

    # 3. Load References
    ref_images = load_reference_images(base_dir)
    print(f"📸 Loaded {len(ref_images)} Product Reference Images")

    # 4. Generate Image
    bundle = prompts_result.get("bundle", {})
    prompt = bundle.get("prompt_en", "")
    must_match_refs = bundle.get("must_match_reference_images", True)
    
    # If explicit "none" visibility, technically we shouldn't pass product refs, 
    # but Gemini handles context better with refs even if asked not to show them primarily.
    # However, to avoid "hallucinating" the product into a "none" scene, we could conditionally pass them.
    # But usually "subtle" is the default. I'll pass refs.
    
    print("\n--- GENERATING PAIN IMAGE ---")
    pain_img = generate_image_gemini(prompt, ref_images if must_match_refs else [])
    
    if pain_img:
        out_path = results_dir / "pain_image.png"
        pain_img.save(out_path)
        print(f"✅ Pain image saved: {out_path}")
    else:
        print("❌ Failed Pain image.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        product_folder = sys.argv[1]
        run_pain_pipeline(product_folder)
    else:
        run_pain_pipeline("samba_og_vaca_negro_blanco")
