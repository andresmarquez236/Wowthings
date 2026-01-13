
import os
import sys
import argparse
import json
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from typing import Dict, Any

# Load environment variables
load_dotenv()

# Add root to sys.path to allow imports if needed
sys.path.append(os.getcwd())

from utils.logger import setup_logger, update_context, log_section
logger = setup_logger("Agent1_Strategist")

SYSTEM_PROMPT = """Eres el Agente 1: Strategist / Research & Angles para un sistema multiagente de creativos.

Tu objetivo: generar EXACTAMENTE {args.num_angles} ángulos ganadores para el producto, usando SOLO:
- product_brief
- visual_brief
- (opcional) market/platform/today_iso

Debes:
- Clasificar categoría principal y secundarias con evidencia.
- Definir contexto estacional.
- Hacer una investigación breve.
- Para cada ángulo: 
  - "angle_id": "angle_1", "angle_2"...
  - "angle_name": Título del ángulo
  - "promise_type": "aspirational" | "pain" | "benefit"
  - "hooks": [3 strings]
  - "proof": plan de prueba
  - "compliance_risks": "low"/"medium"/"high"
  - "recommended_format": "image"/"video"

Prohibido:
- Redactar copies finales
- Inventar especificaciones

Salida:
- Solo JSON "creative_strategist_angles_v1".
- angles: lista de objetos con "angle_id" obligatorio.
"""

DEVELOPER_PROMPT = """REGLAS DE CALIDAD (OBLIGATORIAS):
1) Ventana de contexto: mantén todo compacto.
2) Investigación (timeboxed): máximo 8 min.
3) No pegues URLs.
4) Decide recommended_format.
5) Hook window: <=3 segundos.
6) Compliance: sé conservadora.
7) Landing congruence: nota global.

OUTPUT:
- Rellena: market_context, product_classification, research_digest, angles.
- angles debe tener "angle_id".
"""

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description="Agent 1: Strategist / Research & Angles")
    parser.add_argument("--brief_path", required=True, help="Path to product_brief.json (Agent 0 output)")
    parser.add_argument("--output_file", required=True, help="Path to save the output JSON")
    parser.add_argument("--market", default="CO", help="Target Market (default: CO)")
    parser.add_argument("--platform", default="META", help="Target Platform (default: META)")
    parser.add_argument("--num_angles", type=int, default=3, help="Number of angles to generate (default: 3)")
    
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
    product_brief = brief_data.get("product_brief", {})
    visual_brief = brief_data.get("visual_brief", {})
    
    # 2. Build User Prompt Payload
    today_iso = datetime.now().strftime("%Y-%m-%d")
    
    user_payload = {
        "market": args.market,
        "platform": args.platform,
        "today_iso": today_iso,
        "num_angles": args.num_angles,
        "product_brief": product_brief,
        "visual_brief": visual_brief
    }
    
    user_prompt_str = json.dumps(user_payload, ensure_ascii=False, indent=2)

    # 3. Construct System Message - Dynamic Angles
    dynamic_system_prompt = SYSTEM_PROMPT.replace("3 ángulos", f"{args.num_angles} ángulos").replace("EXACTLY 3", f"EXACTLY {args.num_angles}")
    dynamic_system_prompt = dynamic_system_prompt.replace("angles[3]", f"angles[{args.num_angles}]") # Handle DEVELOPER_PROMPT part if needed, but simple replace covers main instruction
    
    full_system_prompt = dynamic_system_prompt + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt_str}
    ]

    # 4. Call API
    logger.info(f"Strategizing {args.num_angles} angles...")
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
                
                # Unwrap if wrapped in "creative_strategist_angles_v1" or similar
                if "angles" not in data and len(data.keys()) == 1:
                     key = list(data.keys())[0]
                     if isinstance(data[key], dict) and "angles" in data[key]:
                         logger.info(f"Refactoring: Unwrapping root key '{key}'")
                         data = data[key]

                # Check critical fields (Quick validation)
                if "angles" not in data:
                     logger.warning("Output missing 'angles' key.")
                else:
                    count = len(data["angles"])
                    if count != args.num_angles:
                        logger.warning(f"Output may not have exactly {args.num_angles} angles. Found {count}.")
                    
                    # Patch: Ensure angle_id exists
                    for idx, angle in enumerate(data["angles"]):
                        if "angle_id" not in angle:
                            norm_id = f"angle_{idx+1:03d}"
                            angle["angle_id"] = norm_id
                            logger.info(f"Patched missing angle_id -> {norm_id}")
                
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
