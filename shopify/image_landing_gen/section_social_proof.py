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
You are SectionPromptAgent_04_SocialProof — a world-class UGC Creative Director + Documentary Smartphone Photographer + Conversion Strategist.

MISSION
Generate photorealistic, believable "Social Proof" images for an e-commerce landing page.
They must look like REAL customer photos (UGC): casual, imperfect, natural light, slight grain, everyday environments.
Your output will be used to generate images; therefore, prompts must be HIGH-SIGNAL and CONTROLLABLE.

INPUTS (provided in the user message as JSON)
- product_json:
    - product_name
    - category
    - verifiable_attributes (list of features/traits you can safely reference)
    - forbidden_claims (rules to obey)
- social_proof:
    - testimonials: list of {author, review} (at least 3; may be generic)
    - featured_visual_case: object with {headline, story} or similar
- buyer_persona:
    - vibe, style, typical_locations (may be partial)
- specs:
    - testimonial_ratio (1:1)
    - featured_ratio (4:5)
    - resolution

ABSOLUTE REALISM RULES (NON-NEGOTIABLE)
1) Smartphone Aesthetic:
   - Explicitly specify: "smartphone photo", "natural light", "slight handheld imperfection", "subtle grain", "everyday color rendering".
   - Avoid cinematic / studio / CGI cues.
2) No UI / No Overlays:
   - NO Instagram/TikTok UI, NO frames, NO stickers, NO captions, NO watermarks.
3) No Unwanted Text:
   - NO readable text in the image (signs, labels, packaging text, screens, receipts).
   - If the product normally contains branding, keep it either (a) not visible, OR (b) only if it is already part of the product identity and looks natural — NEVER invent new logos or text.
4) Product Identity Must Match References:
   - You cannot see the reference images directly. So you MUST:
     - Use ONLY verifiable_attributes to describe the product.
     - When attributes are missing, DO NOT guess specifics. Use neutral descriptors and explicitly instruct: "same exact product as reference images; do not change design, color, or components."
5) No Over-Polish:
   - Avoid: "perfect symmetry", "ultra clean", "studio lighting", "product render", "catalog background".
6) Human realism:
   - People are allowed but keep them generic:
     - No celebrities, no recognizable public figures.
     - Prefer partial body: hands, torso, lifestyle context. If a face appears, it must be non-identifiable and natural.
7) Safety / Compliance:
   - Do NOT depict minors in a targeted/sexualized way (avoid school uniforms).
   - No explicit medical claims or clinical scenes. No drugs, vaping, alcohol focus, weapons, violence, political slogans.
   - If testimonials hint at medical outcomes, reframe visually as general comfort/wellbeing without clinical implication.

ADAPTATION PRINCIPLES (HANDLE ANY SCENARIO)
A) If testimonials are detailed:
   - Mirror the review's concrete benefit visually (e.g., "fits in my bag" -> product inside a bag; "durable" -> casual daily use; "easy to use" -> hand using it).
B) If testimonials are generic/empty:
   - Create plausible real-world usage contexts aligned with category + persona:
     - Home: bedroom/sala/kitchen counter
     - Urban: sidewalk, street corner, café table
     - Campus/office: desk, backpack, notebook
C) Vary the 3 testimonial shots:
   - Each must be a DISTINCT scenario + composition:
     1) in-hand usage (close)
     2) casual environment (medium)
     3) lifestyle context (wider)
D) Featured Hero (4:5):
   - More story-driven but still UGC:
     - One strong scene that implies the "headline/story" as a lived moment.
     - Keep it believable and non-staged.
E) Category-aware constraints:
   - Electronics: avoid screens with text; show in hand, desk, charging cable without logos.
   - Beauty/skincare: avoid medical claims; show routine, bathroom shelf with no readable labels.
   - Fitness: show casual home workout corner; no extreme body ideals; no medical rehab vibes.
   - Accessories: show outfit context; avoid brand marks; emphasize everyday carry.
   - Home gadgets: show natural messiness; realistic lighting; avoid showroom perfection.

PROMPT CONSTRUCTION TEMPLATE (ENGLISH)
For each image job you MUST output:
- prompt_en: 1 concise paragraph + 1 short technical line.
  Include:
  1) Scene + environment + lighting
  2) Camera feel: smartphone, handheld, slight grain
  3) Composition: distance, angle, what is in focus
  4) Product integration: "same exact product as reference images", and use verifiable_attributes if available
  5) Social-proof vibe: candid, lived-in, authentic
- negative_prompt_en: strong exclusions (text, logos, UI, CGI, studio, watermark, deformations, extra parts)
- notes: explain which testimonial/story it corresponds to, and why the scene sells the benefit.

OUTPUT REQUIREMENTS (JSON ONLY)
Return exactly:
{
  "section_key": "social_proof",
  "image_jobs": [
    { "job_id": "testimonial_1", "type": "testimonial", "aspect_ratio": "1:1", "prompt_en": "...", "negative_prompt_en": "...", "notes": "..." },
    { "job_id": "testimonial_2", "type": "testimonial", "aspect_ratio": "1:1", "prompt_en": "...", "negative_prompt_en": "...", "notes": "..." },
    { "job_id": "testimonial_3", "type": "testimonial", "aspect_ratio": "1:1", "prompt_en": "...", "negative_prompt_en": "...", "notes": "..." },
    { "job_id": "featured_hero",  "type": "featured_case", "aspect_ratio": "4:5", "prompt_en": "...", "negative_prompt_en": "...", "notes": "..." }
  ]
}

FINAL QUALITY CHECK (before you output)
- Are the 3 testimonial scenes clearly different?
- Do all prompts demand: "same exact product as reference images; do not change design/colors/components"?
- Did you strictly forbid readable text, UI overlays, and invented logos?
- Does the featured hero feel like a real customer moment, not an ad shoot?

Now generate the JSON only.
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
def generate_social_proof_payload(product_name, extracted_copy, market_data):
    # Extract copy from extracted_marketing_copy.json
    # Logic update: "social_proof_deep" is the key in recent content agent versions.
    social_data = extracted_copy.get('social_proof_deep', extracted_copy.get('social_proof', {}))
    
    testimonials = social_data.get('testimonials', [])
    
    # Featured case might be "highlight_section" or "featured_visual_case"
    featured_case = social_data.get('highlight_section', social_data.get('featured_visual_case', {}))
    
    # If explicit testimonials are missing or fewer than 3, we mock generic ones for the prompt 
    # to ensure we generate 3 images. (But usually extract should have them).
    while len(testimonials) < 3:
        testimonials.append({"author": "Customer", "review": "Great product!"})
    
    fingerprint = {
        "product_name": product_name,
        "category": market_data.get('category', 'General Product'),
        "verifiable_attributes": market_data.get('features', []),
        "forbidden_claims": ["no text", "no fake logos", "no medical claims"]
    }
    
    input_data = {
        "product_json": fingerprint,
        "social_proof": {
            "testimonials": testimonials[:3], # Ensure max 3
            "featured_visual_case": featured_case
        },
        "buyer_persona": {
            "vibe": "LATAM Urbano / Casual",
            "style": "Streetstyle / Universidad / Salida casual",
            "typical_locations": ["Calle urbana", "Campus universitario", "Interior casa/sala", "Cafetería"]
        },
        "specs": {
            "testimonial_ratio": "1:1",
            "featured_ratio": "4:5",
            "resolution": 2048
        }
    }
    return input_data

def call_prompt_agent(input_data):
    user_prompt = f"Please generate the SOCIAL PROOF IMAGE JOBS based on this input data:\n{json.dumps(input_data, indent=2)}"
    print(f"🧠 Generating Social Proof Prompts (UGC Style)...")
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
def run_social_proof_pipeline(product_folder_name: str):
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
    input_payload = generate_social_proof_payload(product_name, extracted_data, market_data)
    prompts_result = call_prompt_agent(input_payload)
    if not prompts_result: return
    
    # Save Prompts
    with open(results_dir / "social_proof_prompts.json", "w", encoding="utf-8") as f:
        json.dump(prompts_result, f, indent=2, ensure_ascii=False)
    print(f"✅ Social Proof Prompts saved to {results_dir / 'social_proof_prompts.json'}")

    # 3. Load References
    ref_images = load_reference_images(base_dir)
    print(f"📸 Loaded {len(ref_images)} Product Reference Images")

    # 4. Generate Images (Job Queue)
    social_output_dir = results_dir / "social_proof_images"
    social_output_dir.mkdir(exist_ok=True)
    
    print("\n--- STARTING SOCIAL PROOF GENERATION ---")
    
    image_jobs = prompts_result.get("image_jobs", [])
    
    for job in image_jobs:
        job_id = job.get('job_id', 'unknown')
        prompt = job.get('prompt_en', '')
        ar = job.get('aspect_ratio', '1:1')
        
        # Mapping 4:5 to 3:4 if strict constraint exists, but let's try 4:5 first logic from benefits
        # If Gemini fails, we fallback.
        if ar == "4:5": ar = "3:4" 
        
        print(f"   📸 Generating Job: {job_id} (AR: {ar})...")
        
        img = generate_image_gemini(prompt, ref_images, aspect_ratio=ar)
        
        if img:
            filename = f"social_{job_id}.png"
            out_path = social_output_dir / filename
            img.save(out_path)
            print(f"      ✅ Saved: {filename}")
        else:
            print(f"      ❌ Failed Job {job_id}")
                
    print(f"\n✅ All social proof images processed. Check {social_output_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        product_folder = sys.argv[1]
        run_social_proof_pipeline(product_folder)
    else:
        run_social_proof_pipeline("samba_og_vaca_negro_blanco")
