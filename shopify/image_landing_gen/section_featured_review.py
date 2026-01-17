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
SYSTEM_PROMPT = r"""
You are SectionPromptAgent_05_FeaturedReview — a specialist in generating "Anti-AI" Hyper-Realistic Personas.

MISSION
Generate a prompt for a "Facebook Profile Picture" of the person who wrote the review.
The goal is MAXIMUM HUMAN REALISM. It must look like a random, low-effort selfie or a photo taken by a friend.
It must NOT look like a stock photo, a model, or an AI generation.

INPUTS
- featured_review:
    - text: The review content (use this to infer age, gender, occupation, vibe).
    - author: Name (infer gender/ethnicity).
- buyer_persona:
    - vibe, style.

ABSOLUTE REALISM RULES (NON-NEGOTIABLE)
1) The "Facebook Profile Pic" Aesthetic:
   - Bad framing is good. Slightly off-center is good.
   - Lighting should be natural but not perfect (shadows on face, harsh sun, or indoor tungsten).
   - "Shot on Android/iPhone 8" quality: slight noise, not 8k perfectly sharp.
   - Backgrounds: messy rooms, car interiors, street corners, backyards. NOT studio backgrounds.
2) NO PRODUCT:
   - Do NOT include the product in this image. This is a picture of the USER, not an ad.
   - Unless the review explicitly says "me holding the product" (rare), default to just the PERSON.
3) Inferred Persona:
   - If author is "Carlos (Uber Driver)" -> Latino male, 30s-50s, maybe in a car (seatbelt visible) or standing near a car.
   - If author is "Maria (Mom)" -> Female, 30s, casual clothes, maybe messy hair bun, home background.
   - Look at the review text to guess the "Vibe".

PROMPT CONSTRUCTION
Output a single image job.
- prompt_en:
  - Subject: "Low quality selfie of...", "Candid photo of..."
  - Description: Age, gender, ethnicity, clothing (casual).
  - Environment: specific, cluttered, real.
  - Tech specs: "flash on", "grainy", "slightly blurred background", "front camera".
- negative_prompt_en:
  - Exclude: "professional lighting, studio, bokeh, smooth skin, airbrushed, model, symmetrical face, perfect makeup, 3d render, octane render".

OUTPUT JSON
{
  "section_key": "featured_review",
  "image_jobs": [
    {
      "job_id": "featured_review_1",
      "type": "featured_review",
      "aspect_ratio": "1:1",
      "prompt_en": "...",
      "negative_prompt_en": "...",
      "notes": "Explains why this face fits the review text..."
    }
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
def generate_featured_review_payload(product_name, extracted_copy, market_data):
    # Extract featured review from hero_section
    hero_section = extracted_copy.get('hero_section', {})
    featured_review = hero_section.get('featured_review', {})
    
    # Fallback if empty
    if not featured_review or not featured_review.get('text'):
        print("⚠️ No featured review found in hero_section. Using generic fallback.")
        featured_review = {
            "text": "This product changed my life! Highly recommended.",
            "author": "Happy Customer"
        }

    fingerprint = {
        "product_name": product_name,
        "category": market_data.get('category', 'General Product'),
        "verifiable_attributes": market_data.get('features', []),
        "forbidden_claims": ["no text", "no fake logos", "no medical claims"]
    }
    
    input_data = {
        "product_json": fingerprint,
        "featured_review": featured_review,
        "buyer_persona": {
            "vibe": "Authentic / Relatable",
            "style": "Casual",
            "typical_locations": ["Home", "Car", "Office", "Outdoors"]
        },
        "specs": {
            "aspect_ratio": "1:1",
            "resolution": 2048
        }
    }
    return input_data

def call_prompt_agent(input_data):
    user_prompt = f"Please generate the FEATURED REVIEW IMAGE JOB based on this input data:\n{json.dumps(input_data, indent=2)}"
    print(f"🧠 Generating Featured Review Prompt...")
    try:
        response = openai_client.chat.completions.create(
            model="gpt-5.1",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.75,
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
def run_featured_review_pipeline(product_folder_name: str):
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
    input_payload = generate_featured_review_payload(product_name, extracted_data, market_data)
    prompts_result = call_prompt_agent(input_payload)
    if not prompts_result: return
    
    # Save Prompts
    with open(results_dir / "featured_review_prompts.json", "w", encoding="utf-8") as f:
        json.dump(prompts_result, f, indent=2, ensure_ascii=False)
    print(f"✅ Featured Review Prompts saved to {results_dir / 'featured_review_prompts.json'}")

    # 3. Load References
    ref_images = load_reference_images(base_dir)
    print(f"📸 Loaded {len(ref_images)} Product Reference Images")

    # 4. Generate Images (Job Queue)
    output_dir = results_dir / "featured_review_image"
    output_dir.mkdir(exist_ok=True)
    
    print("\n--- STARTING FEATURED REVIEW GENERATION ---")
    
    image_jobs = prompts_result.get("image_jobs", [])
    
    for job in image_jobs:
        job_id = job.get('job_id', 'unknown')
        prompt = job.get('prompt_en', '')
        ar = job.get('aspect_ratio', '1:1')
        
        # Mapping 4:5 to 3:4 if strict constraint exists, but let's try 4:5 first logic from benefits
        # If Gemini fails, we fallback.
        if ar == "4:5": ar = "3:4" 
        
        print(f"   📸 Generating Job: {job_id} (AR: {ar})...")
        print(f"      Prompt: {prompt[:100]}...")

        # IMPORTANT: We pass NO reference images for the profile picture to avoid product leakage.
        img = generate_image_gemini(prompt, ref_images=[], aspect_ratio=ar)
        
        if img:
            filename = f"{job_id}.png"
            out_path = output_dir / filename
            img.save(out_path)
            print(f"      ✅ Saved: {filename}")
        else:
            print(f"      ❌ Failed Job {job_id}")
                
    print(f"\n✅ All featured review images processed. Check {output_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        product_folder = sys.argv[1]
        run_featured_review_pipeline(product_folder)
    else:
        # Default test product
        run_featured_review_pipeline("aspiradora_recargable_para_carro_3_en_1")
