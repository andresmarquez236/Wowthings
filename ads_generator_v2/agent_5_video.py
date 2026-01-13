
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
logger = setup_logger("Agent5_Video")

SYSTEM_PROMPT = """Eres el Agente 5: Video Creative Agent (Nanobanana Pro).

Tu tarea: por cada ángulo, generar un paquete completo de VIDEO que incluya:
1) Video prompt (para Luma/Runway) + negative_prompt.
2) Guion con hook 0–3s, beats, visual, text, VO.
3) Shot list.
4) Copy del anuncio (AIDA).

Restricciones obligatorias:
- Usa SOLO allowed_claims del angle_policy.
- Evita banned_or_risky_phrases, copy_no_gos y visual_no_gos.
- Primary_text <= 400 caracteres y <=3 emojis.
- Devuelve SOLO JSON válido con schema_version "creative_video_v2".
- Sin markdown.
"""

DEVELOPER_PROMPT = """REGLAS DE LA CLASE (OBLIGATORIAS):
1) Video Script:
   - Hook 0-3s debe ser visual y texto, sin depender auditivo.
   - Beats claros (3-6, 6-10, 10-15, 15-20).
2) Copy (AIDA):
   - Title, Primary Text (<=400 chars, <=3 emojis), Headline.

EJECUCIÓN:
- En "video.video_prompt", escribe un prompt descriptivo para generador de video (ej. "Cinematic product shot of...").
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

def get_compliance_data(compliance_path: str, angle_id: str) -> Optional[Dict[str, Any]]:
    compliance_data = load_json(compliance_path)
    reviews_list = compliance_data.get("angles_review", compliance_data.get("angles", []))
    for review in reviews_list:
        if review.get("angle_id") == angle_id:
            return review
    return None

def main():
    parser = argparse.ArgumentParser(description="Agent 5: Video Creative (Nanobanana Pro)")
    parser.add_argument("--brief_path", required=True, help="Path to product_brief.json (Agent 0 output)")
    parser.add_argument("--angles_path", required=True, help="Path to angles.json (Agent 1 output)")
    parser.add_argument("--compliance_path", required=True, help="Path to compliance_review.json (Agent 2 output)")
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
    visual_brief = brief_data.get("visual_brief", brief_data.get("visual_brief", {}))

    angle_card = get_angle_data(args.angles_path, args.angle_id)
    if not angle_card:
        logger.error(f"Angle ID '{args.angle_id}' not found in {args.angles_path}")
        sys.exit(1)

    angle_policy = get_compliance_data(args.compliance_path, args.angle_id)
    if not angle_policy:
        logger.error(f"Compliance review for Angle ID '{args.angle_id}' not found in {args.compliance_path}")
        sys.exit(1)

    # 2. Build User Prompt Payload
    user_payload = {
        "product_brief": product_brief,
        "visual_brief": visual_brief,
        "angle_card": angle_card,
        "angle_policy": angle_policy,
        "global_constraints": {
            "framework": "AIDA",
            "max_primary_text_chars": 400,
            "max_emojis": 3,
            "preferred_orientation": "9:16",
            "resolution": "1080x1920",
            "safe_margin_percent": 10,
            "required_output_format": "Nanobanana Pro JSON Schema"
        }
    }
    
    user_prompt_str = json.dumps(user_payload, ensure_ascii=False, indent=2)

    # 3. Construct System Message
    full_system_prompt = SYSTEM_PROMPT + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt_str}
    ]

    # 4. Call API
    logger.info(f"Generating Video Creative for {args.angle_id}...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"},
            max_tokens=4000,
            temperature=0.7,
        )
        
        content = response.choices[0].message.content
        
        if content:
            # Validate JSON
            try:
                data = json.loads(content)
                
                # Basic validation
                if "video" not in data:
                     logger.warning("Output missing 'video' key.")
                
                with open(args.output_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Success! Output saved to {args.output_file}")
            except json.JSONDecodeError:
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
