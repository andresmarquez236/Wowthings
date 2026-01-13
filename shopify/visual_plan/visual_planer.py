import os
import json
import base64
import time
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from io import BytesIO
from dotenv import load_dotenv
from openai import OpenAI
from PIL import Image

# Quick hack to allow importing from utils if running as script
sys.path.append(os.getcwd())

try:
    from utils.logger import setup_logger, log_section, update_context
except ImportError:
    # Fallback if running from a different directory context
    import logging
    def setup_logger(name):
        l = logging.getLogger(name)
        l.addHandler(logging.StreamHandler())
        l.setLevel(logging.INFO)
        return l
    def log_section(l, t): l.info(f"--- {t} ---")
    def update_context(**kwargs): pass

load_dotenv()

# ==============================================================================
# CONFIG & LOGGING
# ==============================================================================

logger = setup_logger("VisualPlaner")
MODEL = "gpt-4o" 

# ==============================================================================
# CONSTANTS & PROMPTS
# ==============================================================================

SYSTEM_PROMPT = """
Eres un Director de Arte + CRO Lead + Psicólogo del consumidor (ecommerce), nivel PhD.
Tu objetivo es proponer la mejor combinación visual (paleta + aplicación por secciones + CTAs)
para maximizar conversión sin sacrificar credibilidad.

Recibirás:
- (A) Lista de imágenes del producto y de secciones (rutas o URLs).
- (B) Un JSON de template de Shopify con sections, order, ids, settings actuales.
- (C) (Opcional) Un JSON de copy/ángulos de venta.

Tareas:
1) Analiza el producto y su “arquetipo de compra”: impulsivo vs racional, premium vs value, riesgo percibido.
2) Extrae señales visuales de las imágenes: estilo (premium/UGC), fondos, colores dominantes, contraste, saturación.
3) Diseña 3 opciones de paleta (Opción A, B, C) con psicología explícita:
   - A: “Brand-derived” (colores extraídos del producto, consistente)
   - B: “Conversion-first” (CTA dominante, máxima claridad)
   - C: “Premium editorial” (confianza + estética + prueba social)
4) Para cada opción, define:
   - Paleta completa en HEX (accent_1, accent_2, text, background_1, background_2, button label, etc.)
   - Mapa por sección: qué color_scheme usar por section_id
   - Reglas CTA (color, contraste mínimo, hover)
   - Riesgos + mitigaciones
   - Score numérico (0–10) con desglose
5) Elige la mejor opción y entrega un “patch_plan” aplicable por código para:
   - config/settings_data.json (reemplazos hex o claves)
   - template JSON (cambios de color_scheme por sección)

Restricciones:
- Mantén legibilidad: contraste texto/fondo y CTA claro.
- Evita combinaciones que parezcan “baratas” o tipo spam.
- No inventes assets inexistentes. Si falta información, asume lo mínimo y deja el supuesto.
- Salida OBLIGATORIA en JSON válido según el schema solicitado.
"""

USER_PROMPT_TEMPLATE = """
{{
  "product_name": "{product_name}",
  "product_positioning_hint": "{positioning_hint}",
  "inputs": {{
    "landing_template_json": "See attached JSON content",
    "section_images_manifest": {section_images_manifest},
    "marketing_copy_json": "See attached JSON content"
  }},
  "sections_in_order": {sections_list},
  "required_output_schema": "landing_visual_plan_v1"
}}
"""

# ==============================================================================
# UTILITIES (Adapted from nanobanana_image_agent)
# ==============================================================================

def _strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s

def _extract_json_object(s: str) -> str:
    s = _strip_code_fences(s)
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        return s[start:end+1].strip()
    return s

def parse_json_or_dump(raw: str, dump_path: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    raw2 = _extract_json_object(raw)
    try:
        return json.loads(raw2)
    except Exception:
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(raw)
        logger.error(f"JSON inválido. RAW guardado en: {dump_path}")
        raise RuntimeError(f"JSON inválido. RAW guardado en: {dump_path}")

def call_with_retries(create_fn: Callable[[], Any], raw_dump_prefix: str, output_dir: Path, retries: int = 2) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 2):
        try:
            resp = create_fn()
            
            # Simple extraction for now, assuming standard ChatCompletion
            raw = resp.choices[0].message.content
            
            dump_path = output_dir / f"{raw_dump_prefix}_raw_attempt{attempt}_{int(time.time())}.txt"
            return parse_json_or_dump(raw, str(dump_path))
        except Exception as e:
            last_err = e
            logger.warning(f"Attempt {attempt} failed: {e}")
            time.sleep(1 * attempt)
    raise last_err

# ==============================================================================
# AGENT CLASS
# ==============================================================================

class VisualPlaner:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("❌ OPENAI_API_KEY not found in environment.")
        self.client = OpenAI(api_key=self.api_key)
        self.output_dir = Path("output")
        self.supported_img_exts = {".png", ".jpg", ".jpeg", ".webp"}

    def _encode_image(self, image_path: Path) -> str:
        """Encodes an image to base64 string, resizing if necessary."""
        try:
            with Image.open(image_path) as img:
                # Convert to RGB if needed
                if img.mode not in ("RGB", "L"):
                    img = img.convert("RGB")
                    
                # Resize if larger than 1024x1024 to save tokens/bandwidth
                max_size = (1024, 1024)
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
                
                # Save to BytesIO
                buffered = BytesIO()
                # Optimize to reduce size further
                img.save(buffered, format="JPEG", quality=85, optimize=True)
                return base64.b64encode(buffered.getvalue()).decode('utf-8')
        except Exception as e:
            logger.error(f"⚠️ Error encoding image {image_path}: {e}")
            return ""

    def _find_files(self, directory: Path, pattern: str) -> List[Path]:
        """Finds files matching a pattern in a directory."""
        return list(directory.glob(pattern))

    def _load_json(self, path: Path) -> Dict[str, Any]:
        """Loads a JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def collect_assets(self, product_folder_name: str) -> Dict[str, Any]:
        """
        Collects all necessary assets: content JSON, template JSON, product images,
        and section images.
        """
        update_context(step="Collect Assets")
        base_path = self.output_dir / product_folder_name
        results_dir = base_path / "resultados_landing"
        images_dir = base_path / "product_images"

        if not results_dir.exists():
            raise FileNotFoundError(f"❌ Results directory not found: {results_dir}")

        # 1. Load Landing Template JSON
        landing_files = self._find_files(results_dir, "product.landing-*.json")
        if not landing_files:
            raise FileNotFoundError(f"❌ No landing JSON found in {results_dir}")
        landing_path = landing_files[0]
        landing_json = self._load_json(landing_path)

        # 2. Load Copy JSON
        copy_path = results_dir / "extracted_marketing_copy.json"
        copy_json = self._load_json(copy_path) if copy_path.exists() else {}

        # 3. Collect Product Images (Limit 5)
        product_images = []
        if images_dir.exists():
            all_imgs = sorted([p for p in images_dir.iterdir() if p.suffix.lower() in self.supported_img_exts])
            product_images = all_imgs[:5]

        # 4. Collect Section Images
        section_images = {
            "pain_image": self._find_files(results_dir, "pain_image.*"),
            "before_after": self._find_files(results_dir, "before_image.*") + self._find_files(results_dir, "after_image.*"),
            "benefits": self._find_files(results_dir, "benefit_*.png"), 
            "social_proof": self._find_files(results_dir, "social_proof_*.png")
        }
        
        return {
            "landing_path": landing_path,
            "landing_json": landing_json,
            "copy_json": copy_json,
            "product_images": product_images,
            "section_images": section_images,
            "base_path": base_path,
            "results_dir": results_dir
        }

    def _prepare_openai_messages(self, assets: Dict[str, Any], product_name: str) -> List[Dict[str, Any]]:
        """Constructs the message payload for GPT-4o with Vision."""
        update_context(step="Build Payload")
        
        landing_json = assets["landing_json"]
        
        # Extract simplistic section order for the prompt
        sections_in_order = []
        if "order" in landing_json:
            for sec_id in landing_json["order"]:
                sec_type = landing_json["sections"].get(sec_id, {}).get("type", "unknown")
                sections_in_order.append({"id": sec_id, "type": sec_type})
        
        manifest = {k: [str(p.name) for p in v] for k, v in assets["section_images"].items()}
        
        user_text = USER_PROMPT_TEMPLATE.format(
            product_name=product_name,
            positioning_hint="High conversion, visual clarity, trustworthy",
            section_images_manifest=json.dumps(manifest, indent=2),
            sections_list=json.dumps(sections_in_order, indent=2)
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": []}
        ]
        
        content_list = messages[1]["content"]

        # Add Text Prompt
        content_list.append({"type": "text", "text": user_text})

        # Add LANDING JSON content
        content_list.append({
            "type": "text", 
            "text": f"--- LANDING TEMPLATE JSON ---\n{json.dumps(landing_json, indent=2)}"
        })

        # Add COPY JSON content
        content_list.append({
            "type": "text", 
            "text": f"--- MARKETING COPY JSON ---\n{json.dumps(assets['copy_json'], indent=2)}"
        })

        # Add Product Images (Vision)
        for img_path in assets["product_images"]:
            b64_str = self._encode_image(img_path)
            content_list.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{b64_str}",
                    "detail": "low"
                }
            })

        # Add Section Images (Vision)
        for key, paths in assets["section_images"].items():
            for img_path in paths:
                b64_str = self._encode_image(img_path)
                content_list.append({
                    "type": "text",
                    "text": f"[Image Context: {key} - {img_path.name}]"
                })
                content_list.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{b64_str}",
                        "detail": "low"
                    }
                })

        return messages

    def analyze_and_generate(self, product_folder_name: str, product_name: str):
        """Main pipeline execution."""
        log_section(logger, "VISUAL PLAN GENERATION START")
        
        # 1. Collect
        try:
            assets = self.collect_assets(product_folder_name)
        except Exception as e:
            logger.error(f"❌ Error collecting assets: {e}")
            return

        logger.info(f"Found Landing Template: {assets['landing_path'].name}")
        logger.info(f"Found {len(assets['product_images'])} Product Images")
        
        # 2. Prepare Payload
        messages = self._prepare_openai_messages(assets, product_name)

        # 3. Call AI with Retry
        update_context(step="AI Inference")
        logger.info(f"Sending to {MODEL} (Vision)...")
        
        def _api_call():
             return self.client.chat.completions.create(
                model=MODEL,
                messages=messages,
                response_format={"type": "json_object"},
                max_tokens=4000,
                temperature=0.7
            )

        try:
            plan_json = call_with_retries(_api_call, "visual_plan", assets["results_dir"])
        except Exception as e:
            logger.critical(f"❌ AI Generation Failed after retries: {e}")
            return

        # 4. Save Output
        update_context(step="Save Results")
        output_path = assets["results_dir"] / "landing_visual_plan.json"
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(plan_json, f, indent=2, ensure_ascii=False)
            
        logger.info(f"✅ Visual Plan Saved: {output_path}")
        
        # 5. Summary
        best_option_id = plan_json.get("best_option_id")
        best_opt = next((opt for opt in plan_json.get("options", []) if opt.get("id") == best_option_id), None)
        
        if best_opt:
            logger.info(f"🏆 Winner: {best_opt.get('name', 'Unknown')}")
            logger.info(f"   Rationale: {plan_json.get('psychology_rationale')}")

# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    agent = VisualPlaner()
    
    # Default testing
    folder = "coco_rose_mantequilla_truly_grande"
    name = "Coco Rose Mantequilla"
    
    if len(sys.argv) > 1:
        folder = sys.argv[1]
    if len(sys.argv) > 2:
        name = sys.argv[2]
        
    # Check if folder provided exists
    if not (Path("output") / folder).exists():
        logger.warning(f"Folder {folder} not found in output, using default/test.")

    agent.analyze_and_generate(folder, name)
