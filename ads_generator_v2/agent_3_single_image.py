
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
logger = setup_logger("Agent3_SingleImage")

SYSTEM_PROMPT = """Eres el Agente 3: Single Image Creative Agent.

Tu tarea: generar UNA pieza creativa de IMAGEN por ángulo, con variaciones de formato (9:16 y 1:1), donde CADA variante incluye un objeto "nanobanana_prompt" detallado para renderizado profesional.

Restricciones obligatorias:
- Usa SOLO allowed_claims del angle_policy.
- Evita banned_or_risky_phrases y copy_no_gos.
- No promesas absolutas o poco probables.
- Primary_text <= 400 caracteres y emojis <= 3.
- Devuelve SOLO JSON válido con schema_version "creative_single_image_v2".
- Sin markdown.

STRUCTURE OF "nanobanana_prompt" (MUST BE INCLUDED IN EACH VARIANT):
{
    "task": "meta_vertical_ad" (for 9:16) or "meta_square_ad" (for 1:1),
    "format": { "aspect_ratio": "9:16" | "1:1", "resolution": "1080x1920" | "1080x1080", "safe_margin_percent": 10 },
    "input_assets": { 
        "product_lock_rule": "CRITICAL: Depict ONLY the exact product from reference. Do NOT change model/category."
    },
    "prompt": "English visual description. Detailed, high contrast, pro lighting...",
    "text_overlays": [
        {
            "id": "headline" | "cta" | "badge_free_shipping" | "badge_payment",
            "text_exact": "Spanish text (short)", 
            "placement": { 
                "anchor": "top_left" | "top_right" | "bottom_center" | "center", 
                "x_percent": int (0-100), 
                "y_percent": int (0-100), 
                "max_width_percent": int, 
                "max_height_percent": int 
            },
            "typography": { 
                "font_family": "Bold sans-serif" | "Clean modern", 
                "weight": int (400-900), 
                "alignment": "left" | "center" | "right" 
            },
            "style": { 
                "fill": "white" | "black" | "hex", 
                "stroke": "black" | "none", 
                "shadow": "drop_shadow" | "none", 
                "badge": "rounded_red" | "none" 
            }
        }
    ],
    "composition_rules": { 
        "focus": "Product is hero, sharpest element.", 
        "text_safe_zone": "Top 20% reserved for headline.",
        "text_never_cover_rule": "Text must never cover the product."
    },
    "negative_prompt": "text, watermark, logo, blurry, distorted, ugly face, extra fingers..."
}
"""

DEVELOPER_PROMPT = """REGLAS DE LA CLASE (APLICAR SÍ O SÍ):
1) Estructura de Salida:
   - "render_variants": lista con 2 objetos (uno 9:16, uno 1:1).
   - Cada variante DEBE tener "nanobanana_prompt" completo.
2) Nanobanana Pro Specs:
   - "prompt": SIEMPRE EN INGLÉS. Describe la escena visualmente.
   - "text_overlays": Incluye SIEMPRE "headline" corto (3-5 palabras).
   - Badges: Incluye "Envío gratis" y/o "Pago contraentrega" como overlays separados si el brief no lo prohíbe.
   - Placements: Usa coordenadas x_percent/y_percent reales. Ej: Headline suele ir arriba (y=15%) o abajo (y=85%). Badges en esquinas superiores (y=10%).
3) Copy (AIDA):
   - Title, Primary Text (<=400 chars, <=3 emojis), Description, Headline.
   - Lenguaje de ventas persuasivo pero SEGURO (sin promesas falsas).

EJEMPLO DE PLACEMENT HEADLINE (Bottom Center):
"placement": {"anchor": "bottom_center", "x_percent": 50, "y_percent": 85, "max_width_percent": 90, "max_height_percent": 15}

EJEMPLO DE PLACEMENT BADGE (Top Right):
"placement": {"anchor": "top_right", "x_percent": 88, "y_percent": 12, "max_width_percent": 30, "max_height_percent": 10}
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
    parser = argparse.ArgumentParser(description="Agent 3: Single Image Creative (Nanobanana Pro)")
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
            "preferred_orientations": ["9:16", "1:1"],
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
    logger.info(f"Generating Creative for {args.angle_id}...")
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
                if "render_variants" not in data:
                     logger.warning("Output missing 'render_variants' key.")
                
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
