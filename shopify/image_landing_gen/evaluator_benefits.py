import os
import json
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from io import BytesIO

from dotenv import load_dotenv
from PIL import Image

from google import genai
from google.genai import types

# -----------------------------
# CONFIG
# -----------------------------
load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "Missing API key. Set GEMINI_API_KEY (recommended) or GOOGLE_API_KEY in your environment."
    )

EVAL_MODEL = os.getenv("EVAL_MODEL", "gemini-2.0-flash")  # you can override if needed
TEMPERATURE = float(os.getenv("EVAL_TEMPERATURE", "0.2"))
MAX_OUTPUT_TOKENS = int(os.getenv("EVAL_MAX_OUTPUT_TOKENS", "1200"))

# Image input controls (reduce request size + keep judge consistent)
MAX_SIDE_PX = int(os.getenv("EVAL_MAX_SIDE_PX", "1024"))      # resize to max 1024px side
JPEG_QUALITY = int(os.getenv("EVAL_JPEG_QUALITY", "92"))      # high quality to preserve fidelity
MAX_REF_IMGS = int(os.getenv("EVAL_MAX_REF_IMGS", "3"))
MAX_CANDIDATES = int(os.getenv("EVAL_MAX_CANDIDATES", "8"))   # cap candidates per benefit

SUPPORTED_IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp"}

gemini_client = genai.Client(api_key=API_KEY)


# -----------------------------
# STRONG SYSTEM INSTRUCTION (JUDGE)
# -----------------------------
SYSTEM_INSTRUCTION_EVALUATOR_V2 = r"""
You are an extremely strict e-commerce Creative QA Judge (PhD-level Art Director + Product Specialist).

You will receive:
(A) PRODUCT REFERENCE IMAGES ("TRUTH") showing the real product.
(B) BENEFIT COPY (title + description).
(C) CANDIDATE IMAGES, each preceded by its FILENAME label.

Your job:
1) DISQUALIFY any candidate that does NOT depict the SAME product as the references.
2) Among remaining candidates, pick the SINGLE best image that most clearly and professionally communicates the benefit.
3) If no candidate is acceptable, return pass=false.

NON-NEGOTIABLE GATES (hard fails):
- Fidelity gate: if product identity is wrong (shape / silhouette / key components / colorway / materials / branding / design lines) => DISQUALIFY.
- Text/branding gate: if candidate contains hallucinated logos, random unreadable text, watermarks, or misleading claims not supported by the product => heavy penalty or disqualify (depending on severity).
- Artifact gate: if image is obviously broken (deformed product, extra parts, melted geometry, severe AI artifacts) => DISQUALIFY.

SCORING RUBRIC (0-100 each):
- fidelity (0-100) [MOST IMPORTANT]: exact match to references. Any mismatch is severe.
- benefit_clarity (0-100): does the visual clearly show/evoke the claimed benefit?
- aesthetics (0-100): lighting, composition, realism, sharpness, commercial quality.
- text_integrity (0-100): if any text appears, it must be clean, readable, and not hallucinated/irrelevant.

WEIGHTED TOTAL:
total = round(0.55*fidelity + 0.30*benefit_clarity + 0.10*aesthetics + 0.05*text_integrity)

SELECTION RULE:
- Only candidates with fidelity >= 85 are eligible to win.
- If multiple eligible, choose highest total.
- If the best total is < 75, return pass=false (quality not good enough).

STRICT OUTPUT RULES:
- You MUST return valid JSON only.
- You MUST use EXACT filenames provided. Never invent filenames.
- If pass=false: best_candidate_filename must be null.

Output JSON schema (must follow):
{
  "pass": boolean,
  "best_candidate_filename": string|null,
  "overall_score": integer,
  "analysis": string,
  "reasoning": string,
  "per_candidate": [
    {
      "filename": string,
      "eligible": boolean,
      "scores": {
        "fidelity": integer,
        "benefit_clarity": integer,
        "aesthetics": integer,
        "text_integrity": integer,
        "total": integer
      },
      "disqualify_reasons": [string],
      "notes": string
    }
  ],
  "regeneration_prompt": string|null
}

If pass=false, also provide regeneration_prompt: a short, high-signal prompt (1-3 sentences) describing what the new image must show to satisfy the benefit while matching the product references exactly.
"""


# -----------------------------
# HELPERS
# -----------------------------
def load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_benefit_copy(extracted_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Tries to robustly find benefits in multiple possible JSON structures.
    Returns: [{"title": ..., "description": ...}, ...]
    """
    benefits_list = []
    if "key_benefits" in extracted_data and isinstance(extracted_data["key_benefits"], dict):
        if "detailed_features" in extracted_data["key_benefits"]:
            benefits_list = extracted_data["key_benefits"]["detailed_features"]
    elif "detailed_benefits" in extracted_data:
        if isinstance(extracted_data["detailed_benefits"], list):
            benefits_list = extracted_data["detailed_benefits"]
        elif isinstance(extracted_data["detailed_benefits"], dict) and "columns" in extracted_data["detailed_benefits"]:
            benefits_list = extracted_data["detailed_benefits"]["columns"]

    clean = []
    for item in benefits_list or []:
        if not isinstance(item, dict):
            continue
        clean.append(
            {
                "title": item.get("title", item.get("benefit_title", "Benefit")).strip() if item.get("title") or item.get("benefit_title") else "Benefit",
                "description": item.get("description", item.get("benefit_description", "")).strip() if item.get("description") or item.get("benefit_description") else "",
            }
        )
    return clean


def list_images(folder: Path, max_count: int) -> List[Path]:
    if not folder.exists():
        return []
    paths = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_IMG_EXTS]
    paths = sorted(paths, key=lambda p: p.name)
    return paths[:max_count]


def image_path_to_jpeg_part(path: Path, max_side_px: int = MAX_SIDE_PX, jpeg_quality: int = JPEG_QUALITY) -> types.Part:
    """
    Reads an image from disk, converts to RGB, resizes (thumbnail), encodes to JPEG bytes,
    and returns a types.Part.from_bytes suitable for Gemini vision input.
    """
    with Image.open(path) as img:
        img = img.convert("RGB")
        img.thumbnail((max_side_px, max_side_px), Image.LANCZOS)

        buf = BytesIO()
        img.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
        jpeg_bytes = buf.getvalue()

    return types.Part.from_bytes(data=jpeg_bytes, mime_type="image/jpeg")


def safe_parse_response(response) -> Dict[str, Any]:
    """
    Tries: response.parsed (SDK may parse JSON automatically) else json.loads(response.text)
    """
    if hasattr(response, "parsed") and response.parsed is not None:
        parsed = response.parsed
        # parsed can be dict or pydantic object
        if isinstance(parsed, dict):
            return parsed
        if hasattr(parsed, "model_dump"):
            return parsed.model_dump()
        if hasattr(parsed, "dict"):
            return parsed.dict()
    text = getattr(response, "text", None) or getattr(response, "output_text", None)
    if not text:
        # fallback: try candidates/parts
        try:
            text = response.candidates[0].content.parts[0].text
        except Exception:
            raise ValueError("Model returned empty response.")
    return json.loads(text)


# -----------------------------
# CORE EVALUATION
# -----------------------------
def evaluate_candidates(
    ref_img_paths: List[Path],
    candidate_img_paths: List[Path],
    benefit_info: Dict[str, str],
) -> Dict[str, Any]:
    if not candidate_img_paths:
        return {"pass": False, "best_candidate_filename": None, "overall_score": 0, "analysis": "No candidates found.", "reasoning": "No candidates.", "per_candidate": [], "regeneration_prompt": "Generate a new candidate image that matches the product references exactly and clearly shows the stated benefit."}

    # Build multimodal parts in a deterministic order:
    parts: List[types.Part] = []

    # Context text
    candidate_names = [p.name for p in candidate_img_paths]
    parts.append(
        types.Part.from_text(
            text="You will evaluate product-benefit images.\n"
            f"BENEFIT TITLE: {benefit_info.get('title','')}\n"
            f"BENEFIT DESCRIPTION: {benefit_info.get('description','')}\n"
            f"CANDIDATE FILENAMES (EXACT): {', '.join(candidate_names)}\n"
            "The first block are PRODUCT REFERENCE IMAGES (TRUTH), then CANDIDATE IMAGES."
        )
    )

    # Reference images
    parts.append(types.Part.from_text(text="\n=== PRODUCT REFERENCE IMAGES (TRUTH) ==="))
    for i, p in enumerate(ref_img_paths, start=1):
        parts.append(types.Part.from_text(text=f"REFERENCE_{i}: {p.name}"))
        parts.append(image_path_to_jpeg_part(p))

    # Candidates
    parts.append(types.Part.from_text(text="\n=== CANDIDATE IMAGES ==="))
    for p in candidate_img_paths:
        parts.append(types.Part.from_text(text=f"CANDIDATE_FILENAME: {p.name}"))
        parts.append(image_path_to_jpeg_part(p))

    # Response schema (dict schema is supported)
    response_schema = {
        "type": "object",
        "properties": {
            "pass": {"type": "boolean"},
            "best_candidate_filename": {"type": "string", "nullable": True},
            "overall_score": {"type": "integer"},
            "analysis": {"type": "string"},
            "reasoning": {"type": "string"},
            "per_candidate": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "filename": {"type": "string"},
                        "eligible": {"type": "boolean"},
                        "scores": {
                            "type": "object",
                            "properties": {
                                "fidelity": {"type": "integer"},
                                "benefit_clarity": {"type": "integer"},
                                "aesthetics": {"type": "integer"},
                                "text_integrity": {"type": "integer"},
                                "total": {"type": "integer"},
                            },
                            "required": ["fidelity", "benefit_clarity", "aesthetics", "text_integrity", "total"],
                        },
                        "disqualify_reasons": {"type": "array", "items": {"type": "string"}},
                        "notes": {"type": "string"},
                    },
                    "required": ["filename", "eligible", "scores", "disqualify_reasons", "notes"],
                },
            },
            "regeneration_prompt": {"type": "string", "nullable": True},
        },
        "required": ["pass", "best_candidate_filename", "overall_score", "analysis", "reasoning", "per_candidate", "regeneration_prompt"],
    }

    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION_EVALUATOR_V2,
        temperature=TEMPERATURE,
        max_output_tokens=MAX_OUTPUT_TOKENS,
        response_mime_type="application/json",
        response_schema=response_schema,
    )

    try:
        response = gemini_client.models.generate_content(
            model=EVAL_MODEL,
            contents=parts,
            config=config,
        )
        result = safe_parse_response(response)
    except Exception as e:
        return {
            "pass": False,
            "best_candidate_filename": None,
            "overall_score": 0,
            "analysis": "Evaluation failed due to an exception.",
            "reasoning": f"AI Error: {e}",
            "per_candidate": [],
            "regeneration_prompt": "Generate a new candidate image that matches the product references exactly and clearly shows the stated benefit.",
        }

    # Defensive checks: filename must be one of candidates if pass=true
    best = result.get("best_candidate_filename")
    if result.get("pass") is True:
        if not best or best not in candidate_names:
            # Fallback: pick highest total from per_candidate if available
            per = result.get("per_candidate") or []
            best_by_total = None
            best_total = -1
            for row in per:
                try:
                    fn = row["filename"]
                    total = int(row["scores"]["total"])
                    if fn in candidate_names and total > best_total:
                        best_total = total
                        best_by_total = fn
                except Exception:
                    continue

            if best_by_total:
                result["best_candidate_filename"] = best_by_total
            else:
                # mark fail if we cannot validate
                result["pass"] = False
                result["best_candidate_filename"] = None
                result["regeneration_prompt"] = result.get("regeneration_prompt") or (
                    "Regenerate: ensure the product matches the reference images exactly (shape/color/materials/branding) "
                    "and show the benefit clearly with realistic commercial lighting."
                )

    return result


# -----------------------------
# PIPELINE RUNNER
# -----------------------------
def run_evaluation_pipeline(product_folder_name: str) -> None:
    print(f"🕵️‍♂️ Starting Benefits Evaluation for: {product_folder_name}")

    base_dir = Path("output") / product_folder_name
    results_dir = base_dir / "resultados_landing"
    product_images_dir = base_dir / "product_images"
    benefits_images_dir = results_dir / "benefits_images"

    copy_path = results_dir / "extracted_marketing_copy.json"
    if not copy_path.exists():
        print("❌ extracted_marketing_copy.json not found.")
        return

    # 1) Load Data
    extracted_data = load_json(copy_path)
    benefits = get_benefit_copy(extracted_data)
    if not benefits:
        print("❌ No benefits found in extracted_marketing_copy.json")
        return

    # 2) Load Reference Images
    ref_paths = list_images(product_images_dir, max_count=MAX_REF_IMGS)
    if not ref_paths:
        print("❌ No product images found for reference in output/<product>/product_images/")
        return

    # 3) Finals folder
    finals_dir = benefits_images_dir / "finals_images"
    finals_dir.mkdir(parents=True, exist_ok=True)

    # 4) Iterate benefits
    for i, benefit in enumerate(benefits):
        idx = i + 1
        print(f"\n🔹 Evaluating Benefit {idx}: {benefit.get('title','Benefit')}")

        if not benefits_images_dir.exists():
            print(f"   ❌ benefits_images_dir not found: {benefits_images_dir}")
            return

        # Gather candidate images for this benefit
        # Expected filenames: benefit_{idx}_...
        candidate_paths_all = sorted(
            [p for p in benefits_images_dir.iterdir()
             if p.is_file() and p.suffix.lower() in SUPPORTED_IMG_EXTS and f"benefit_{idx}_" in p.name],
            key=lambda p: p.name
        )

        candidate_paths = candidate_paths_all[:MAX_CANDIDATES]

        print(f"   Found {len(candidate_paths_all)} candidates; using {len(candidate_paths)} (cap={MAX_CANDIDATES}).")
        for p in candidate_paths:
            print(f"   - {p.name}")

        if not candidate_paths:
            print(f"   ⚠️ No candidates found for Benefit {idx}. Skipping.")
            continue

        # Evaluate
        result = evaluate_candidates(ref_paths, candidate_paths, benefit)

        passed = bool(result.get("pass"))
        score = result.get("overall_score", result.get("score", 0))
        best_filename = result.get("best_candidate_filename")

        print(f"   🧐 AI Decision: pass={passed} (overall_score={score})")
        print(f"   📝 Reasoning: {result.get('reasoning','')}")

        if passed and best_filename:
            src = benefits_images_dir / best_filename
            if src.exists():
                dst = finals_dir / f"benefit_{idx}_final.png"  # standardized output
                shutil.copy2(src, dst)
                print(f"   🏆 WINNER: {best_filename}")
                print(f"      ✅ Saved to {dst}")
            else:
                print(f"      ⚠️ Winner file not found on disk: {best_filename}")
                print(f"      ❌ Marking as fail for safety. Regen prompt:\n      {result.get('regeneration_prompt')}")
        else:
            print("   ❌ NO PASS.")
            regen = result.get("regeneration_prompt")
            if regen:
                print(f"   🔁 Regen prompt suggestion:\n   {regen}")

    print(f"\n✅ Evaluation Complete. Check: {finals_dir}")


if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "samba_og_vaca_negro_blanco"
    run_evaluation_pipeline(folder)
