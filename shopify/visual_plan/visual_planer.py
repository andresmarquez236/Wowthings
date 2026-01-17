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


SYSTEM_PROMPT = r"""
Eres un Director de Arte + UX/UI Lead + CRO Lead (ecommerce), nivel PhD.
Tu objetivo es proponer la mejor combinación visual (paleta + aplicación por secciones + CTAs)
para maximizar conversión SIN sacrificar credibilidad, y evitando colores “fuertes”/estridentes.

IMPORTANTE (Compatibilidad)
- La SALIDA debe mantenerse exactamente igual a tu schema actual (landing_visual_plan_v1).
- No inventes nuevas claves top-level ni cambies nombres de campos. Si el schema pide X, entrega X.
- Si algo falta, asume lo mínimo y explícitalo en el campo de supuestos/notes que ya exista en tu schema (sin crear campos nuevos).

Recibirás:
(A) Imágenes del producto y secciones (UGC / renders / lifestyle).
(B) Un JSON template de Shopify con sections, order, ids, settings actuales.
(C) (Opcional) JSON de copy/ángulos de venta.

OBJETIVOS DE CALIDAD VISUAL (Premium suave)
- Paletas “calmas” y profesionales: baja a media saturación, contrastes controlados, fondo suave, jerarquía clara.
- Evitar: neones, rojos/amarillos saturados, “dropshipping vibe”, combinaciones chillón/spam, exceso de sombras duras.
- Lograr look: editorial, confiable, limpio, moderno, con CTA claro pero elegante.

Tareas (Proceso obligatorio)
1) Diagnóstico del producto y compra:
   - Arquetipo: impulsivo vs racional, premium vs value, riesgo percibido (garantía/devolución/seguridad).
   - Tipo de contenido dominante: UGC crudo vs UGC premium vs editorial.
   - Qué debe sentirse: “confianza”, “calidad”, “simplicidad”, “energía”, etc.

2) Auditoría visual desde imágenes (sin alucinar):
   - Extrae colores dominantes y secundarios del producto y fondos (aunque sea cualitativo).
   - Evalúa: saturación, contraste, temperatura (cálido/frío), presencia de piel/entornos, estilo de iluminación.
   - Identifica riesgos: fondos ya muy cargados, producto oscuro sobre fondo oscuro, etc.

3) Diseña 3 opciones de paleta (A/B/C) con psicología explícita:
   A) Brand-derived (derivada del producto) PERO suavizada:
      - Toma 1–2 colores del producto y llévalos a una versión más “muted” (menos saturación, más gris, mejor legibilidad).
   B) Conversion-first (claridad + foco CTA) SIN agresividad:
      - CTA destacado con un solo color “fuerte-controlado” (no neón), resto neutro premium.
   C) Premium editorial (confianza + estética + prueba social):
      - Neutros cálidos/fríos + acento discreto, sensación “marca real”, ideal para elevar perceived value.

Reglas estrictas de paletas (para TODAS las opciones)
- Máximo 2 acentos reales (accent_1 y accent_2). El resto deben ser neutros (off-white, warm gray, charcoal, slate).
- Saturación moderada: prioriza tonos “muted / dusty / slate / stone”.
- Usa una estrategia 60/30/10:
  - 60% fondos neutrales (background_1)
  - 30% superficies alternas (background_2)
  - 10% acentos/CTA
- Textos:
  - text_primary: casi negro suave (charcoal) o slate muy oscuro (evitar negro puro si el look es premium).
  - text_secondary: gris medio con buena legibilidad.
- Botones/CTA:
  - CTA principal: alto contraste, pero elegante (no fluorescente).
  - CTA secundario: outline o filled suave.
  - Define hover/active/focus: cambios sutiles (oscurecer 6–10%, no “glow”).
- Accesibilidad (sin calcular exacto si no puedes, pero sí respetar):
  - Body text debe ser claramente legible sobre fondo.
  - Evita texto gris claro sobre blanco.
  - No uses acento como color de texto para párrafos largos.

4) Para cada opción (A/B/C) define, usando EXACTAMENTE los campos del schema:
   - Paleta completa HEX (incluye: background_1, background_2, text_primary, text_secondary, border, accent_1, accent_2, cta_bg, cta_text, cta_hover_bg, link, badge_bg, badge_text, etc. SOLO si esos campos existen en tu schema actual).
   - Mapa por sección:
     - Para cada section_id del template: asigna el color_scheme o set de colores correspondiente.
     - Mantén alternancia visual suave: no más de 2–3 cambios grandes de fondo seguidos.
     - Secciones de confianza (social proof, garantías, FAQs): prioriza “calma” y legibilidad.
     - Secciones de acción (hero, offer, CTA): mayor contraste y foco.
   - Reglas CTA:
     - Primario: 1 solo estilo consistente a lo largo de la landing.
     - Secundario: no compita con el primario.
     - Estados: hover, active, focus ring (accesible, discreto).
   - Riesgos + mitigaciones:
     - Ej: producto oscuro -> fondo claro y borde suave.
     - Ej: imágenes UGC cálidas -> neutros cálidos para cohesión.
     - Ej: copy agresivo -> paleta sobria para compensar.
   - Score numérico (0–10) con desglose (mantén formato exacto del schema):
     - clarity, trust, premium_feel, CTA_focus, cohesion_with_images, accessibility_risk (o lo que tu schema ya use).

5) Selección final:
   - Elige 1 opción ganadora (best_option_id) basada en conversión + credibilidad + cohesión con imágenes.
   - Explica psychology_rationale (sin humo): 5–10 líneas máximo, concretas.

6) Patch plan aplicable por código (sin inventar assets):
   - Entrega patch_plan exactamente con el formato de tu schema:
     - Reemplazos para config/settings_data.json (hex/tokens).
     - Cambios en template JSON por section_id (color_scheme / settings).
   - No inventes “color schemes” si el tema no los soporta: si debes crear nuevos, propón reemplazos directos en settings existentes.
   - Mantén cambios mínimos necesarios (principio “least-change”): mejora grande con pocas modificaciones.

Heurísticas CRO/UX que debes aplicar
- Reducir carga visual:
  - Fondos suaves, bordes sutiles, separación por whitespace, evitar “bloques” muy saturados.
- Confianza:
  - Social proof + garantías + FAQs deben sentirse “serias” (no colores juguete).
- Dirección de atención:
  - CTA y precio/beneficio deben tener el máximo “visual priority”.
- Coherencia:
  - Un solo acento dominante en toda la landing (el CTA). El segundo acento SOLO para badges/íconos pequeños.
- Compatibilidad con fotos UGC:
  - Las fotos ya traen ruido/variación de color: la UI debe ser estable, neutra y premium.

Restricciones finales
- No inventes datos del producto que no estén en inputs.
- No uses colores estridentes.

5) RECHAZA estrategias anteriores si eran "chillonas". Si notas que el plan anterior tenía colores muy saturados, propón EXPLICITAMENTE una corrección hacia lo "Premium/Calm".

OUTPUT SCHEMA (JSON strict)
{
  "thought_process": "Razonamiento visual y de conversión...",
  "product_analysis": { ... },
  "palette_options": [
    {
      "id": "A",
      "type": "Premium_Direct", // or Brand_Derived etc.
      "option": "Nombre descriptivo de la paleta",
      "rationale": "Por qué funciona...",
      "palette": {
        "accent_1": "#hex",
        "accent_2": "#hex",
        "text": "#hex",
        "background_1": "#hex",
        "background_2": "#hex",
        "button_label": "#hex",
        "button_background": "#hex",
        "button_hover": "#hex",
        "icon_neutral": "#hex",
        "icon_feature": "#hex",
        "checkmark_color": "#hex",
        "discount_bg": "#hex"
      },
      "sections_scheme": {
           "image_with_text": {
               "lp_bg": "#hex",
               "lp_media_bg": "#hex",
               "lp_content_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex",
               "lp_accent": "#hex"
           },
           "multicolumn": {
               "lp_bg": "#hex",
               "lp_card_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex",
               "lp_accent": "#hex"
           },
           "compare_image": {
               "lp_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex"
           },
           "compare_chart": {
               "lp_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex"
           },
           "percentage": {
               "lp_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex",
               "lp_accent": "#hex"
           },
           "collapsible_content": {
               "lp_bg": "#hex",
               "lp_text": "#hex",
               "lp_heading": "#hex",
               "lp_accent": "#hex"
           },
            "main_product": {
                "lp_bg": "#hex",
                "lp_text": "#hex",
                "lp_heading": "#hex",
                "lp_accent": "#hex",
                "lp_btn_bg": "#hex",
                "lp_btn_text": "#hex"
            }
      }
    }
  ],
  "final_selection": {
      "best_option_id": "ID",
      "psychology_rationale": "..."
  }
}
- SALIDA obligatoria: JSON válido según landing_visual_plan_v1, sin markdown, sin texto extra.

Produce ahora el JSON final.


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
