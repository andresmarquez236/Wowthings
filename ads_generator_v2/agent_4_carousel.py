
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
logger = setup_logger("Agent4_Carousel")

SYSTEM_PROMPT = """You are an expert Facebook Ads Creative Strategist specialized in Carousel Ads (AIDA Framework).
Your goal is to generate a visual plan for a carousel ad based on a specific marketing angle.

CRITICAL LANGUAGE RULE:
- The 'prompt' field (image description) MUST be in ENGLISH.
- ALL text inside 'text_overlays', 'badges', and 'copy' MUST be in SPANISH (Colombia/Latam).
- If the input is English, TRANSLATE the text content to Spanish.

OUTPUT SCHEMA:
Return a JSON object with this exact structure:
Cada card debe contener un objeto "nanobanana_prompt" completo y detallado para renderizado profesional.

Restricciones obligatorias:
- Usa SOLO allowed_claims del angle_policy.
- Evita banned_or_risky_phrases y copy_no_gos.
- No promesas absolutas o poco probables.
- Primary_text <= 400 caracteres y emojis <= 3.
- Devuelve SOLO JSON válido con schema_version "creative_carousel_aida_v2".
- Sin markdown.

STRUCTURE OF OUTPUT JSON:
{
  "schema_version": "creative_carousel_aida_v2",
  "carousel": {
    "cards": [
      {
        "card_index": 1,
        "stage": "Attention",
        "nanobanana_prompt": { ... },
        "copy": { ... }
      },
      ...
    ]
  },
  "ad_copy": { "title": "...", "primary_text": "...", "headline": "..." }
}
"""

DEVELOPER_PROMPT = """REGLAS DE LA CLASE (OBLIGATORIAS):
1) Nanobanana Pro Specs (por card):
   - "prompt": SIEMPRE EN INGLÉS. Describe la escena visualmente.
   - "text_overlays": Mínimo 1 headline corto por card (excepto quizás en card de solo producto).
   - Badges: Usa "Envío gratis" / "Pago contraentrega" en cards de Desire/Action.
   - Placements: Coordenadas reales. Mantén consistencia visual entre cards (mismo estilo de badges/texto).
2) Estructura AIDA:
   - Card 1 (Attention): Visual impactante + Hook text.
   - Card 2 (Interest): Muestra el problema/solución o mecanismo.
   - Card 3 (Desire): Beneficio clave + Prueba (badge/iconos).
   - Card 4 (Action): CTA claro ("Pide hoy").
3) Copy del anuncio:
   - Un solo copy para todo el carrusel (el campo "copy" dentro de cada card es redundante en Meta Ads real, pero úsalo para proponer textos si fuera un Story sequence; lo importante es el "ad_copy" global al final del JSON).
   - "ad_copy" global: Title, Primary Text (<=400 chars), Headline.

EJEMPLO DE PLACEMENT BADGE (Top Left):
"placement": {"anchor": "top_left", "x_percent": 12, "y_percent": 12, "max_width_percent": 25, "max_height_percent": 10}
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
    parser = argparse.ArgumentParser(description="Agent 4: Carousel AIDA (Nanobanana Pro)")
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
            "primary_orientation": "1:1",
            "primary_resolution": "1080x1080",
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
    logger.info(f"Generating Carousel for {args.angle_id}...")
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
                if "carousel" not in data:
                     logger.warning("Output missing 'carousel' key.")
                
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
