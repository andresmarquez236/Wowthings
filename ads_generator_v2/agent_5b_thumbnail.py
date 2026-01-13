
import os
import sys
import argparse
import json
from dotenv import load_dotenv
from openai import OpenAI
from typing import Dict, Any, Optional

# Load environment variables
load_dotenv()

# Add root to sys.path to allow imports if needed
sys.path.append(os.getcwd())

from utils.logger import setup_logger
logger = setup_logger("Agent5b_Thumbnail")

SYSTEM_PROMPT = """You are a senior Direct-Response Creative Strategist + Meta Ads expert + Prompt Engineer.
Your job: generate HIGH-CONVERTING creative prompts for NanoBananaPro for eCommerce ads.

OUTPUT REQUIREMENTS
1) Output ONLY valid JSON via the 'return_thumbnails' tool.
2) ALWAYS generate EXACTLY 3 DISTINCT thumbnails per angle (T1, T2, T3). No more, no less.
3) LANGUAGE RULES: 
   - 'prompt' (visual description) MUST be in ENGLISH. 
   - 'text_overlays' (on-image text) and 'badges' MUST BE IN SPANISH.
4) Always include keys:
   - task
   - thumbnail or card_number (mapped to thumb_id)
   - variant
   - format: aspect_ratio + resolution + safe_margin_percent
   - input_assets: include product_lock_rule (when product reference images exist)
   - prompt (scene generation prompt)
   - text_overlays (if text is required): exact text + placement coordinates
   - negative_prompt
   - composition_rules
"""

DEVELOPER_PROMPT = """CORE GOAL
Generate an image that:
- is scroll-stopping in Meta feeds (mobile-first)
- matches the landing message
- is AIDA-structured (Attention -> Interest -> Desire -> Action) across cards/thumbnails
- uses the EXACT product identity when reference images are provided
- looks NEW (not a copy of the reference scene)
- incorporates consumer psychology and attention mechanics

PRODUCT LOCK (CRITICAL)
If reference images are provided:
- Use the reference ONLY to replicate the exact product identity (shape, colors, texture, face, unique marks).
- Do NOT recreate the reference background or composition.
- If the model tends to “re-generate” a different product, instruct: “cut out/mask the product from reference and composite into a new scene.”
- Explicitly forbid: “Do NOT generate a different model/variant.”

META-SPEC COMPLIANCE
- Default square creatives: 1:1, 1080x1080, indicate safe_margin_percent=10.
- Mobile readability: headline must be readable at 1x feed view.
- Keep key elements away from edges: respect safe margins and avoid placing text in corners that can be covered by UI.

AIDA CREATIVE RULES (APPLY PER THUMBNAIL)
- T1 (Scroll Stop): High contrast, big readable hook, simple composition (1 hero object + 1 supporting cue). Shock or curiosity.
- T2 (Proof/Demo): Explain mechanism visually (callouts, arrows, wave lines, feature badges). Product in use, 'hands-on'.
- T3 (Benefit/Result): Show outcome proof (user benefit in scene), emotional relief, comfort, simplicity. Lifestyle/Premium.

DESIGN SYSTEM (CONSISTENCY)
- Style: premium ecommerce, clean typography, high clarity.
- Background: minimal, blurred, or gradient to make the product pop.
- Avoid clutter: max 1 headline + 1 subline + 1–2 badges.
- Use vector-like arrows/badges only if they help comprehension.

TEXT OVERLAY RULES
- Provide text_exact exactly (including accents, punctuation).
- Provide placement with x_percent, y_percent, max_width_percent, max_height_percent.
- Typography: bold sans-serif; include stroke/shadow if needed for contrast.
- Add subtle gradient behind text zone only if background is busy.

NEGATIVE PROMPT MUST INCLUDE
- no watermarks, no logos (unless requested), no extra text, no distorted product, no public figure likeness, no medical promises (when relevant), no copyrighted UI/logos/landmarks.

WORKFLOW (WHAT YOU MUST DO)
Step 1) Identify creative type: thumbnail (T1, T2, or T3).
Step 2) Identify AIDA stage and single core message for this creative.
Step 3) Choose best scene structure (product-only hero, product + user outcome, etc.).
Step 4) Define composition: left/right split, center hero, diagonal split, etc.
Step 5) Enforce product lock rule with compositing instruction.
Step 6) Write the scene prompt (photorealistic, premium, simple, high contrast).
Step 7) Add text overlays with exact placement and legibility safeguards.
Step 8) Add composition_rules and negative_prompt.
"""

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_angle_data(angles_path: str, angle_id: str) -> Optional[Dict[str, Any]]:
    angles_data = load_json(angles_path)
    angles_list = angles_data.get("angles", [])
    for angle in angles_list:
        if angle.get("angle_id") == angle_id:
            return angle
    return None

def main():
    parser = argparse.ArgumentParser(description="Agent 5b: Thumbnail Agent (Nanobanana)")
    parser.add_argument("--brief_path", required=True, help="Path to product_brief.json")
    parser.add_argument("--angles_path", required=True, help="Path to angles.json")
    parser.add_argument("--compliance_path", required=True, help="Path to compliance_review.json")
    parser.add_argument("--angle_id", required=True, help="ID of the angle to process")
    parser.add_argument("--output_file", required=True, help="Path to save the output JSON")
    
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not found in environment variables.")
        sys.exit(1)

    client = OpenAI(api_key=api_key)

    # 1. Load Inputs
    if not os.path.exists(args.brief_path):
        logger.error(f"Brief file not found at {args.brief_path}")
        sys.exit(1)
        
    brief_data = load_json(args.brief_path)
    product_brief = brief_data.get("product_brief", brief_data.get("product_brief", {}))
    # Some brief files might be nested or flat, handle graceful fallback if needed, but standard is nested.

    angle_card = get_angle_data(args.angles_path, args.angle_id)
    if not angle_card:
        logger.error(f"Angle ID '{args.angle_id}' not found in {args.angles_path}")
        sys.exit(1)

    # 2. Build Tool Schema
    tools = [
        {
            "type": "function",
            "function": {
                "name": "return_thumbnails",
                "description": "Return exactly 3 thumbnail prompt objects for Nanobanana Pro.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "thumbnails": {
                            "type": "array",
                            "minItems": 3,
                            "maxItems": 3,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "thumb_id": {"type": "string", "enum": ["T1_ScrollStop", "T2_ProofDemo", "T3_BenefitResult"]},
                                    "nanobanana_prompt": {
                                        "type": "object",
                                        "properties": {
                                            "task": {"type": "string"},
                                            "format": {"type": "object"},
                                            "input_assets": {"type": "object"},
                                            "prompt": {"type": "string", "description": "Visual description in English"},
                                            "text_overlays": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": {"type": "string"},
                                                        "text_exact": {"type": "string"},
                                                        "placement": {"type": "object"},
                                                        "typography": {"type": "object"},
                                                        "style": {"type": "object"}
                                                    },
                                                    "required": ["id", "text_exact", "placement"]
                                                }
                                            },
                                            "composition_rules": {"type": "object"},
                                            "negative_prompt": {"type": "string"}
                                        },
                                        "required": ["task", "format", "input_assets", "prompt", "text_overlays", "composition_rules", "negative_prompt"]
                                    }
                                },
                                "required": ["thumb_id", "nanobanana_prompt"]
                            }
                        }
                    },
                    "required": ["thumbnails"]
                }
            }
        }
    ]

    # 3. Build User Prompt
    user_payload = {
        "product_brief": product_brief,
        "angle_card": angle_card,
        "global_constraints": {
            "required_output_format": "Nanobanana Pro (3 Thumbnails)",
            "dimensions": "9:16"
        }
    }
    user_prompt_str = json.dumps(user_payload, ensure_ascii=False, indent=2)

    full_system_prompt = SYSTEM_PROMPT + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt_str}
    ]

    # 4. Call API with Tool
    logger.info(f"Generating Thumbnails for {args.angle_id}...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=tools,
            tool_choice={"type": "function", "function": {"name": "return_thumbnails"}},
            max_tokens=4000,
            temperature=0.7,
        )
        
        tool_calls = response.choices[0].message.tool_calls
        if tool_calls:
            # Parse arguments from the tool call
            args_json = tool_calls[0].function.arguments
            try:
                data = json.loads(args_json)
                
                # Wrap in standard output structure
                output_data = {
                    "schema_version": "creative_thumbnails_v1",
                    "thumbnails": data.get("thumbnails", [])
                }

                with open(args.output_file, "w", encoding="utf-8") as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                logger.info(f"Success! Output saved to {args.output_file}")
                
            except json.JSONDecodeError:
                logger.error("Tool arguments were not valid JSON.")
                sys.exit(1)
        else:
            logger.error("Model did not call the required tool.")
            # Fallback handling could go here, but for now strict fail is safer for pipeline
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
