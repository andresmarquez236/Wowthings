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
You are SectionPromptAgent_03_Benefits, a world-class Art Director and Prompt Engineer specializing in high-conversion e-commerce visuals.

OBJECTIVE
For each product benefit provided, generate a "Shot Pack" of 3 distinct, photorealistic image prompts (NanoBananaPro compliant).
These images will be displayed in small "multicolumn" cards on a landing page.
Therefore, visual clarity, high contrast, and "one idea per image" are paramount.

INPUT CONTEXT
You will receive:
1. Benefit Title & Description (The core value proposition).
2. Product Fingerprint (Visual attributes, category, forbidden claims).
3. Style Constraints (Aspect ratio, no text policy, lighting).

OUTPUT REQUIREMENT (The Shot Pack)
For EACH benefit, provide 3 variation prompts (Shot A, Shot B, Shot C) following this strategy:
- Shot A: MACRO / HERO (Visual Tangibility). focus on texture, material, or specific feature quality.
- Shot B: IN-USE / CONTEXT (Functional Proof). clearly showing the benefit in action (e.g. tying laces, waterproof test).
- Shot C: LIFESTYLE / ASPIRATIONAL (Social Identity). the product in a relevant environment that implies the benefit.

HARD RULES
1. **No Text/Logos**: Absolute prohibition on typography, UI elements, watermarks, or brand logos inside the image.
2. **Product Fidelity**: Must strictly match the reference images (colors, materials, shapes). No hallucinations.
3. **PhD-Level Aesthetics**: Use sophisticated lighting terms (e.g., "chiaroscuro", "rembrandt lighting", "softbox diffused"), precise camera angles ("low angle", "dutch tilt", "top-down flatlay"), and depth of field control ("bokeh", "f/1.8").
4. **Thumbnail Optimization**: The composition must be readable at small sizes. Center the hero element. Avoid clutter.

OUTPUT FORMAT (JSON Only)
{
  "section_key": "detailed_benefits_multicolumn",
  "benefits_visuals": [
    {
      "benefit_title": "...",
      "shot_pack": [
        {
          "shot_id": "A_macro_hero",
          "aspect_ratio": "1:1|4:5",
          "prompt_en": "...",
          "negative_prompt_en": "...",
          "composition_notes": "..."
        },
        {
          "shot_id": "B_in_use",
          "aspect_ratio": "1:1|4:5",
          "prompt_en": "...",
          "negative_prompt_en": "...",
          "composition_notes": "..."
        },
        {
          "shot_id": "C_lifestyle",
          "aspect_ratio": "1:1|4:5",
          "prompt_en": "...",
          "negative_prompt_en": "...",
          "composition_notes": "..."
        }
      ]
    },
    ... (repeat for all benefits)
  ]
}
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
def generate_benefits_payload(product_name, extracted_copy, market_data):
    # Extract copy from extracted_marketing_copy.json
    # Expecting 'detailed_benefits_multicolumn' or mapping from 'key_benefits.detailed_features'
    
    # Check where the benefits are stored. 
    # Usually section_landing.py extracts them into 'detailed_benefits' or similar.
    # The user mentioned "key_benefits.detailed_features" in extracted_marketing_copy.json
    
    benefits_list = []
    
    # Try different possible keys based on section_landing output structure
    if 'key_benefits' in extracted_copy and 'detailed_features' in extracted_copy['key_benefits']:
        benefits_list = extracted_copy['key_benefits']['detailed_features']
    elif 'detailed_benefits' in extracted_copy:
         # Might be a list directly or under 'columns'
         if isinstance(extracted_copy['detailed_benefits'], list):
             benefits_list = extracted_copy['detailed_benefits']
         elif 'columns' in extracted_copy['detailed_benefits']:
             benefits_list = extracted_copy['detailed_benefits']['columns']
    
    # Normalize list to always have title/description
    clean_benefits = []
    for item in benefits_list:
        clean_benefits.append({
            "benefit_title": item.get('title', item.get('benefit_title', 'Benefit')),
            "benefit_description": item.get('description', item.get('benefit_description', ''))
        })
    
    # Fingerprint construction
    fingerprint = {
        "product_name": product_name,
        "category": market_data.get('category', 'General Product'),
        "verifiable_attributes": market_data.get('features', []),
        "forbidden_claims": ["no text", "no fake logos", "no impossible claims"]
    }
    
    input_data = {
        "section_key": "detailed_benefits_multicolumn",
        "benefits_input": clean_benefits,
        "product_fingerprint": fingerprint,
        "style_guide_global": {
            "visual_mode": "ugc_clean",
            "lighting": "soft natural daylight + studio accents",
            "recommended_aspect_ratio": "1:1",
            "text_in_image_policy": "none"
        }
    }
    return input_data

def call_prompt_agent(input_data):
    user_prompt = f"Please generate the BENEFITS SHOT PACKS based on this input data:\n{json.dumps(input_data, indent=2)}"
    print(f"🧠 Generating Benefits Prompts (PhD Level)...")
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5.1",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.75, # Slightly higher for creative variation
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"❌ Error generating prompts: {e}")
        return None

# ---------------------------------------------------------
# STEP 2: IMAGE GENERATION (Gemini)
# ---------------------------------------------------------
def generate_image_gemini(prompt: str, ref_images: List[Image.Image] = [], aspect_ratio="1:1"):
    """
    Generates an image using Gemini.
    """
    contents = [prompt]
    if ref_images:
        contents.extend(ref_images)
        
    config = types.GenerateContentConfig(
        response_modalities=["IMAGE"],
        image_config=types.ImageConfig(aspect_ratio=aspect_ratio, image_size="4K"),
        candidate_count=1,
    )
    
    # print(f"  🎨 Generating... (AR: {aspect_ratio})") 
    try:
        response = gemini_client.models.generate_content(
            model="gemini-3-pro-image-preview",
            contents=contents,
            config=config,
        )
        if response.candidates:
             for part in response.candidates[0].content.parts:
                if hasattr(part, "inline_data") and part.inline_data:
                    from io import BytesIO
                    return Image.open(BytesIO(part.inline_data.data))
                
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
                        pass
        return None
    except Exception as e:
        print(f"  ❌ Generation Error: {e}")
        return None

# ---------------------------------------------------------
# ORCHESTRATOR
# ---------------------------------------------------------
def run_benefits_pipeline(product_folder_name: str):
    base_dir = Path("output") / product_folder_name
    results_dir = base_dir / "resultados_landing"
    
    if not results_dir.exists():
        print(f"❌ Results directory not found: {results_dir}")
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
    input_payload = generate_benefits_payload(product_name, extracted_data, market_data)
    prompts_result = call_prompt_agent(input_payload)
    if not prompts_result: return
    
    # Save Prompts
    with open(results_dir / "benefits_prompts.json", "w", encoding="utf-8") as f:
        json.dump(prompts_result, f, indent=2, ensure_ascii=False)
    print(f"✅ Benefits Prompts saved to {results_dir / 'benefits_prompts.json'}")

    # 3. Load References
    ref_images = load_reference_images(base_dir)
    print(f"📸 Loaded {len(ref_images)} Product Reference Images")

    # 4. Generate Images (Batch)
    benefits_output_dir = results_dir / "benefits_images"
    benefits_output_dir.mkdir(exist_ok=True)
    
    print("\n--- STARTING BATCH GENERATION (3 SHOTS PER BENEFIT) ---")
    
    benefits_visuals = prompts_result.get("benefits_visuals", [])
    
    for i, benefit in enumerate(benefits_visuals):
        title = benefit.get('benefit_title', f"Benefit_{i+1}")
        clean_title = re.sub(r'[^a-zA-Z0-9]', '_', title).lower()[:20]
        shot_pack = benefit.get('shot_pack', [])
        
        print(f"\n🔹 Processing Benefit {i+1}: {title}")
        
        for shot in shot_pack:
            shot_id = shot.get('shot_id', 'unknown')
            prompt = shot.get('prompt_en', '')
            ar = shot.get('aspect_ratio', '1:1')
            
            # Sanitize aspect ratio for Gemini (needs "1:1", "4:5")
            # If user provided something else or multiple options "1:1|4:5", pick first
            if '|' in ar:
                ar = ar.split('|')[0]
            
            # Ensure valid AR for Gemini
            if ar not in ["1:1", "3:4", "4:3", "16:9", "9:16"]:
                if ar == "4:5": ar = "3:4" # Closest approximation supported or valid if 4:5 supported? 4:5 IS supported by Gemini Imagine 3? 
                # Wait, GenAI SDK supports '1:1', '3:4', '4:3', '16:9', '9:16'. 4:5 is not strictly in that list usually, 
                # but '3:4' is close. Let's use '1:1' as safe default if unsure, or '3:4' for vertical.
                # However, user explicitly requested 1:1 or 4:5. 
                # If Gemini doesn't support 4:5, 3:4 is the way.
                # Let's map 4:5 -> 3:4.
                if ar == "4:5": ar = "3:4"
            
            print(f"   📸 Generating Shot {shot_id} (AR: {ar})...")
            
            img = generate_image_gemini(prompt, ref_images, aspect_ratio=ar)
            
            if img:
                filename = f"benefit_{i+1}_{clean_title}_{shot_id}.png"
                out_path = benefits_output_dir / filename
                img.save(out_path)
                print(f"      ✅ Saved: {filename}")
            else:
                print(f"      ❌ Failed Shot {shot_id}")
                
    print(f"\n✅ All benefits images processed. Check {benefits_output_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        product_folder = sys.argv[1]
        run_benefits_pipeline(product_folder)
    else:
        run_benefits_pipeline("samba_og_vaca_negro_blanco")
