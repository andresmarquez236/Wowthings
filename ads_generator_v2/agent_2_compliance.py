
import os
import sys
import argparse
import json
from dotenv import load_dotenv
from openai import OpenAI
from typing import Dict, Any

# Load environment variables
load_dotenv()

# Add root to sys.path to allow imports if needed
sys.path.append(os.getcwd())

from utils.logger import setup_logger
logger = setup_logger("Agent2_Compliance")

SYSTEM_PROMPT = """Eres el Agente 2: Compliance Pre-Check / Angle Gatekeeper para un sistema multiagente de creativos.

Tu objetivo: revisar cada ángulo propuesto, detectar riesgos de compliance y reescribir (de forma segura) promesas, hooks y plan de prueba, sin cambiar la esencia del ángulo.

Debes:
- Ser conservadora: si hay duda, marca riesgo medium/high.
- Evitar promesas absolutas, poco probables o no soportadas.
- Proponer alternativas seguras (rewrites) listas para que los agentes creativos produzcan sin bloqueo.
- Devolver SOLO JSON válido con schema_version "creative_compliance_gatekeeper_v1".

Prohibido:
- Generar copys finales completos.
- Inventar evidencia o especificaciones.
- Devolver texto fuera del JSON.
"""

DEVELOPER_PROMPT = """REGLAS DURAS (DEBES CUMPLIR):
1) Alineado con la clase: evita promesas poco probables y absolutas. Si detectas “garantizado”, “en X días sí o sí”, “cura”, “elimina al 100%”, marca HIGH y reescribe a lenguaje “ayuda a / puede contribuir / diseñado para”.
2) Congruencia con landing: si el ángulo promete algo que NO se puede respaldar con (a) un hero visual y (b) una prueba simple (demo/UGC/testimonio), baja score y exige cambios.
3) Hooks: deben quedar en versión safe, concretos y cortos.
4) Before/After: por defecto, marca allowed=false. Solo permitir “before_after_safe” si el product_brief y compliance perfil lo soportan, y siempre sin sensacionalismo. Si lo permites, exige condiciones claras en before_after_rules.
5) Salida por ángulo:
   - allowed_claims: lista de claims "soft/moderate" con base de evidencia (product_brief o visual_brief)
   - banned_or_risky_phrases: lista con alternativa segura
   - safe_rewrites: promise_statement_safe + hooks_safe + proof_plan_adjusted
   - creative_do_nots: visual_no_gos y copy_no_gos
   - status: APPROVED / APPROVED_WITH_CHANGES / REJECTED

CALIDAD:
- Compacto: máximo 10–14 items totales por ángulo entre allowed_claims y banned_phrases.
- No uses lenguaje legal; usa instrucciones accionables para creativos.
- Si el ángulo es REJECTED, explica required_changes mínimos para volverlo APPROVED_WITH_CHANGES.
"""

def load_json(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description="Agent 2: Compliance Pre-Check")
    parser.add_argument("--brief_path", required=True, help="Path to product_brief.json (Agent 0 output)")
    parser.add_argument("--angles_path", required=True, help="Path to angles.json (Agent 1 output)")
    parser.add_argument("--output_file", required=True, help="Path to save the output JSON")
    parser.add_argument("--market", default="CO", help="Target Market (default: CO)")
    parser.add_argument("--platform", default="META", help="Target Platform (default: META)")
    
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
        
    if not os.path.exists(args.angles_path):
        logger.error(f"Angles file not found at {args.angles_path}")
        sys.exit(1)
        
    brief_data = load_json(args.brief_path)
    product_brief = brief_data.get("product_brief", brief_data.get("product_brief", {})) # Fallback if structure varies
    visual_brief = brief_data.get("visual_brief", brief_data.get("visual_brief", {}))
    
    angles_data = load_json(args.angles_path)
    # Angles usually in "angles" list relative to Agent 1 output root
    angles_list = angles_data.get("angles", [])

    # 2. Build User Prompt Payload
    user_payload = {
        "market": args.market,
        "platform": args.platform,
        "product_brief": product_brief,
        "visual_brief": visual_brief,
        "angles": angles_list
    }
    
    user_prompt_str = json.dumps(user_payload, ensure_ascii=False, indent=2)

    # 3. Construct System Message
    full_system_prompt = SYSTEM_PROMPT + "\n\n" + DEVELOPER_PROMPT

    messages = [
        {"role": "system", "content": full_system_prompt},
        {"role": "user", "content": user_prompt_str}
    ]

    # 4. Call API
    logger.info("Reviewing compliance...")
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            response_format={"type": "json_object"},
            max_tokens=4000,
            temperature=0.0, # Strict compliance check
        )
        
        content = response.choices[0].message.content
        
        if content:
            # Validate JSON
            try:
                data = json.loads(content)
                
                # Basic validation
                if "angles_review" not in data:
                     logger.warning("Output missing 'angles_review' key.")
                
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
