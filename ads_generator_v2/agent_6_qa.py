
import os
import sys
import argparse
import json
from dotenv import load_dotenv
from openai import OpenAI
from typing import Dict, Any, List, Optional

# Load environment variables
load_dotenv()

# Add root to sys.path to allow imports if needed
sys.path.append(os.getcwd())

from utils.logger import setup_logger
logger = setup_logger("Agent6_QA")

SYSTEM_PROMPT = """Eres el Agente 6: Post-Gen QA + Policy Validator.

Tu tarea: auditar assets ya generados (single image, carousel, video) y devolver:
- PASS / PASS_WITH_PATCHES / FAIL por asset
- lista de checks con evidencia_paths
- parches mínimos (RFC6902) si aplica

Restricciones obligatorias:
- Copy AIDA, no muy extenso: primary_text <= 400 caracteres.
- No más de 3 emojis.
- Evitar promesas poco probables/absolutas.
- Video debe tener hook 0–3s.
- Preferir vertical y assets “uno por uno” por placements.

Debes usar SOLO:
- product_brief, visual_brief
- angle_card, angle_policy
- generated_assets
- global_constraints

Prohibido:
- Generar creativos nuevos desde cero
- Hacer investigación de mercado
- Inventar evidencia o especificaciones

Salida:
- SOLO JSON válido con schema_version "creative_postgen_qa_v1".
- Sin markdown.
"""

DEVELOPER_PROMPT = """CHECKS OBLIGATORIOS (mínimo):
- COPY_LEN_400: primary_text <= 400 chars
- EMOJI_MAX_3: emojis <= 3
- AIDA_PRESENT: framework == "AIDA" y copy tiene A/I/D/A implícito
- ALLOWED_CLAIMS_ONLY: no aparecen claims fuera de allowed_claims
- BANNED_PHRASES_NONE: no aparece banned_or_risky_phrases
- PROMISE_PROBABILITY: no hay promesas absolutas o poco probables
- LANDING_CONGRUENCE: promise_statement y guion muestran algo demostrable/probable (proof element)
- SAFE_MARGIN_TEXT: textos en imagen/thumbnail dentro del safe margin
- VIDEO_HOOK_0_3: existe y funciona sin audio (si asset_type=video)
- THUMBNAILS_3: exactamente 3 (si asset_type=video)
- CAROUSEL_4_AIDA: 4 cards A/I/D/A (si asset_type=carousel)
- SINGLE_IMAGE_VARIANTS: contiene 9:16 y 1:1 (si asset_type=single_image)

SEVERIDAD:
- HIGH = incumple compliance/claims (FAIL salvo que parche mínimo lo arregle)
- MEDIUM = estructura incompleta / hook débil / sobre-largo
- LOW = mejoras de claridad/estilo

PATCHES:
- Usa RFC6902: replace/remove/add.
- Evita más de 6 patches por asset. Si requiere más, marca FAIL.

ESTILO:
- Mensajes cortos, accionables.
- evidence_paths deben apuntar a paths JSON reales (ej: /ad_copy/primary_text).
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
    parser = argparse.ArgumentParser(description="Agent 6: Post-Gen QA + Policy Validator")
    parser.add_argument("--brief_path", required=True, help="Path to product_brief.json (Agent 0 output)")
    parser.add_argument("--angles_path", required=True, help="Path to angles.json (Agent 1 output)")
    parser.add_argument("--compliance_path", required=True, help="Path to compliance_review.json (Agent 2 output)")
    parser.add_argument("--angle_id", required=True, help="ID of the angle to process")
    parser.add_argument("--assets", nargs='+', required=True, help="List of paths to generated asset JSON files")
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
        
    generated_assets = []
    for asset_path in args.assets:
        if not os.path.exists(asset_path):
             logger.warning(f"Asset file not found at {asset_path}, skipping.")
             continue
        generated_assets.append(load_json(asset_path))

    if not generated_assets:
        logger.error("No valid asset files provided.")
        sys.exit(1)

    # 2. Build User Prompt Payload
    user_payload = {
        "market": "CO",
        "platform": "META",
        "product_brief": product_brief,
        "visual_brief": visual_brief,
        "angle_card": angle_card,
        "angle_policy": angle_policy,
        "global_constraints": {
            "framework": "AIDA",
            "max_primary_text_chars": 400,
            "max_emojis": 3,
            "preferred_orientations": ["9:16", "1:1"],
            "safe_margin_percent": 10,
            "hook_window_seconds": 3
        },
        "generated_assets": generated_assets
    }
    
    user_prompt_str = json.dumps(user_payload, ensure_ascii=False, indent=2)

    # 3. Construct System Message
    full_system_prompt = SYSTEM_PROMPT + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt_str}
    ]

    # 4. Call API
    logger.info(f"Validating Assets for {args.angle_id}...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"},
            max_tokens=4000,
            temperature=0.0, # strict checking
        )
        
        content = response.choices[0].message.content
        
        if content:
            # Validate JSON
            try:
                data = json.loads(content)
                
                # Basic validation
                if "assets_report" not in data:
                     logger.warning("Output missing 'assets_report' key.")
                
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
