# nanobanana_image_agent.py
# ------------------------------------------------------------
# Single Image Ad Agent (Batch 3 Angles)
# Mirrors structure of nanobanana_carrusel_agent.py
#
# INPUT:
#   output/market_research_min.json
#
# OUTPUT:
#   output/nanobanana_image_{product_name}.json
#
# CACHES (reused):
#   output/trends_pack.json
#   output/hooks_pack.json
# ------------------------------------------------------------

import os
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Callable, Optional

from dotenv import load_dotenv
from openai import OpenAI

# --- error types (compat) ---
try:
    from openai import BadRequestError
except Exception:
    BadRequestError = Exception

load_dotenv()

MODEL = os.getenv("AIDA_AGENT_MODEL", "gpt-4o")  # Using gpt-4o as standard
MARKET_PATH = os.getenv("MARKET_RESEARCH_PATH", "output/market_research_min.json")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRENDS_CACHE_PATH = os.path.join(OUTPUT_DIR, "trends_pack.json")
HOOKS_CACHE_PATH = os.path.join(OUTPUT_DIR, "hooks_pack.json")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ============================================================
# Utilities
# ============================================================

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def get_output_text(resp: Any) -> str:
    if hasattr(resp, "output_text") and resp.output_text:
        return resp.output_text
    try:
        for item in getattr(resp, "output", []) or []:
            if getattr(item, "type", None) != "message":
                continue
            for c in getattr(item, "content", []) or []:
                t = getattr(c, "text", None)
                if t:
                    return t
                if getattr(c, "type", None) in ("output_text", "text") and getattr(c, "text", None):
                    return c.text
    except Exception:
        pass
    # Standard ChatCompletion response
    if hasattr(resp, "choices") and resp.choices:
        return resp.choices[0].message.content
    raise RuntimeError("No se pudo extraer output_text del response.")

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
        raise RuntimeError(f"❌ JSON inválido. RAW guardado en: {dump_path}")

def safe_responses_create(**kwargs):
    try:
        return client.chat.completions.create(**kwargs)
    except BadRequestError as e:
        if "temperature" in str(e):
            kwargs.pop("temperature", None)
            return client.chat.completions.create(**kwargs)
        raise

def call_with_retries(create_fn: Callable[[], Any], raw_dump_prefix: str, retries: int = 2) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 2):
        try:
            resp = create_fn()
            raw = get_output_text(resp)
            dump_path = os.path.join(OUTPUT_DIR, f"{raw_dump_prefix}_raw_attempt{attempt}_{int(time.time())}.txt")
            return parse_json_or_dump(raw, dump_path)
        except Exception as e:
            last_err = e
            time.sleep(0.35 * attempt)
    raise last_err

def load_or_create_cache(path: str, builder_fn: Callable[[], Dict[str, Any]], force_refresh: bool) -> Dict[str, Any]:
    if (not force_refresh) and os.path.exists(path):
        try:
            return load_json(path)
        except Exception:
            pass
    data = builder_fn()
    save_json(path, data)
    return data

# ============================================================
# Market Research Extractors
# ============================================================

def extract_product(market: Dict[str, Any]) -> Dict[str, str]:
    # Try to find product info in 'input' (preferred) or 'meta.producto_input' (fallback)
    producto_input = market.get("input", {}) or ((market.get("meta", {}) or {}).get("producto_input", {}) or {})

    nombre = str(producto_input.get("nombre_producto", market.get("product_name", "Producto"))).strip() or "Producto"
    if nombre.lower().startswith("ejemplo:"):
        nombre = nombre[8:].strip()

    desc = str(producto_input.get("descripcion", market.get("product_description", ""))).strip()
    gar = str(producto_input.get("garantia", market.get("warranty", ""))).strip()
    precio = str(producto_input.get("precio", market.get("price", ""))).strip()

    return {
        "nombre_producto": nombre,
        "descripcion": desc,
        "garantia": gar,
        "precio": precio,
        "pais_objetivo": "Colombia",
    }

def extract_angles(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    angulos = market.get("top_5_angulos")
    if not isinstance(angulos, list) or not angulos:
        raise ValueError("market_research_min.json no trae 'top_5_angulos' como lista no vacía.")
    return angulos

def normalize_angle(a: Dict[str, Any], fallback_rank: int) -> Dict[str, str]:
    rank = int(a.get("rank", fallback_rank))
    return {
        "rank": rank,
        "angle_id": f"ANGLE_{rank}",
        "angle_name": str(a.get("angulo", f"Ángulo {rank}")).strip(),
        "buyer_persona": str(a.get("buyer_persona", "No especificado")).strip(),
        "promesa_core": str(a.get("promesa", a.get("promesa_core", ""))).strip(),
        "objecion_core": str(a.get("objecion_principal", a.get("objecion_core", ""))).strip(),
    }

def extract_hooks_for_rank(market: Dict[str, Any], rank: int) -> List[str]:
    arr = market.get("hooks_por_angulo")
    if not isinstance(arr, list):
        return []
    for item in arr:
        try:
            if int(item.get("rank_angulo")) == rank and isinstance(item.get("hooks"), list):
                hooks = [str(h).strip() for h in item["hooks"] if str(h).strip()]
                return hooks[:10]
        except Exception:
            continue
    return []

def extract_estacionalidad_hint(market: Dict[str, Any]) -> Dict[str, Any]:
    est = market.get("estacionalidad")
    if not isinstance(est, dict):
        return {"tipo": "no_confirmado", "hechos": [], "hipotesis": []}
    return {
        "tipo": est.get("tipo", "no_confirmado"),
        "hechos": est.get("hechos", []) if isinstance(est.get("hechos"), list) else [],
        "hipotesis": est.get("hipotesis", []) if isinstance(est.get("hipotesis"), list) else [],
    }

# ============================================================
# Schemas
# ============================================================

SINGLE_IMAGE_AD_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "angle_id",
        "angle_name",
        "buyer_persona",
        "ad_copy",
        "single_image_prompt",
        "hooks_used",
        "compliance_notes"
    ],
    "properties": {
        "angle_id": {"type": "string"},
        "angle_name": {"type": "string"},
        "buyer_persona": {"type": "string"},
        "ad_copy": {
            "type": "object",
            "additionalProperties": False,
            "required": ["title", "primary_text", "description"],
            "properties": {
                "title": {"type": "string"},
                "primary_text": {"type": "string"},
                "description": {"type": "string"}
            }
        },
        "single_image_prompt": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "task", "thumbnail", "variant", "format", "input_assets",
                "prompt", "text_overlays", "composition_rules", "negative_prompt"
            ],
            "properties": {
                "task": {"type": "string"},
                "thumbnail": {"type": "string"},
                "variant": {"type": "string"},
                "format": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["aspect_ratio", "resolution", "safe_margin_percent"],
                    "properties": {
                        "aspect_ratio": {"type": "string"},
                        "resolution": {"type": "string"},
                        "safe_margin_percent": {"type": "integer"}
                    }
                },
                "input_assets": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["product_lock_rule"],
                    "properties": {
                        "product_lock_rule": {"type": "string"}
                    }
                },
                "prompt": {"type": "string"},
                "text_overlays": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["id", "text_exact", "placement", "typography", "style"],
                        "properties": {
                            "id": {"type": "string"},
                            "text_exact": {"type": "string"},
                            "placement": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["anchor", "x_percent", "y_percent", "max_width_percent", "max_height_percent"],
                                "properties": {
                                    "anchor": {"type": "string"},
                                    "x_percent": {"type": "integer"},
                                    "y_percent": {"type": "integer"},
                                    "max_width_percent": {"type": "integer"},
                                    "max_height_percent": {"type": "integer"}
                                }
                            },
                            "typography": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["font_family", "weight", "alignment"],
                                "properties": {
                                    "font_family": {"type": "string"},
                                    "weight": {"type": "integer"},
                                    "alignment": {"type": "string"}
                                }
                            },
                            "style": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["fill", "stroke", "shadow", "badge"],
                                "properties": {
                                    "fill": {"type": ["string", "null"]},
                                    "stroke": {"type": ["string", "null"]},
                                    "shadow": {"type": ["string", "null"]},
                                    "badge": {"type": ["string", "null"]}
                                }
                            }
                        }
                    }
                },
                "composition_rules": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["focus", "hierarchy", "text_safe_zone", "text_never_cover_rule"],
                    "properties": {
                        "focus": {"type": "string"},
                        "hierarchy": {"type": "string"},
                        "text_safe_zone": {"type": "string"},
                        "text_never_cover_rule": {"type": "string"}
                    }
                },
                "negative_prompt": {"type": "string"}
            }
        },
        "hooks_used": {
            "type": "array",
            "items": {"type": "string"}
        },
        "compliance_notes": {
            "type": "array",
            "items": {"type": "string"}
        }
    }
}

# ============================================================
# Prompts
# ============================================================

SINGLE_IMAGE_SYSTEM = r"""
ROLE
You are a Creative Strategy Expert for Meta Ads (Instagram/Facebook).
Your task is to generate a SINGLE high-performance image ad concept + ad copy for a specific marketing angle.

INPUT
- Product info
- Marketing Angle (Buyer Persona, Promise, Objection)
- Hooks & Trends research

OUTPUT
Return ONLY valid JSON matching the `SINGLE_IMAGE_AD_SCHEMA`.
1. **Ad Copy**: Title, Primary Text, Description (Spanish LATAM).
2. **Single Image Prompt**: A detailed NanoBanana Pro prompt for generating the image.
   - **Prompt**: English, detailed scene description, lighting, composition.
   - **Text Overlays**: Spanish text to be rendered on the image (Headline, CTA, Badges).
   - **Composition**: Safe zones, hierarchy.

GUIDELINES
- **Visuals**: Must be "Thumb-stopping". Use high contrast, clear focal point, and professional lighting.
- **Text on Image**: Keep it short (3-5 words max for headlines). Use "Envío gratis" and "Pago contraentrega" badges if appropriate.
- **Copy**: Direct, benefit-driven, addressing the objection.
- **Consistency**: Ensure the image matches the angle (e.g., if angle is "Portability", show the product in a backpack or travel context).

CRITICAL
- Do NOT invent specs or features.
- Do NOT use restricted medical claims.
- Use the exact product name.
"""

# ============================================================
# Agent Call
# ============================================================

def build_payload(product, angle, hooks_hints, est_hint, trends_pack, hooks_pack):
    return {
        "product": product,
        "angle": angle,
        "hooks_hints": hooks_hints,
        "est_hint": est_hint,
        "trends_pack": trends_pack,
        "hooks_pack": hooks_pack
    }

def call_image_agent(payload: Dict[str, Any], angle_id: str) -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SINGLE_IMAGE_SYSTEM},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": f"single_image_{angle_id}",
                    "schema": SINGLE_IMAGE_AD_SCHEMA,
                    "strict": True
                }
            },
            max_tokens=4000,
        )
    return call_with_retries(_create, f"debug_image_{angle_id}", retries=2)

# ============================================================
# Accumulator
# ============================================================

def load_or_init_accum(product: Dict[str, str], accum_path: str) -> Dict[str, Any]:
    if os.path.exists(accum_path):
        try:
            data = load_json(accum_path)
            if isinstance(data, dict) and "results_by_angle" in data:
                return data
        except Exception:
            pass
    return {
        "schema_version": "nanobanana_image_v1",
        "generated_at_utc": now_utc_iso(),
        "product": product,
        "results_by_angle": [],
    }

def upsert_angle(accum: Dict[str, Any], angle_result: Dict[str, Any]) -> Dict[str, Any]:
    angle_id = angle_result.get("angle_id")
    if not angle_id:
        raise RuntimeError("angle_result no trae angle_id.")

    arr = accum.get("results_by_angle", [])
    if not isinstance(arr, list):
        arr = []

    for i, item in enumerate(arr):
        if isinstance(item, dict) and item.get("angle_id") == angle_id:
            arr[i] = angle_result
            break
    else:
        arr.append(angle_result)

    accum["results_by_angle"] = arr
    accum["generated_at_utc"] = now_utc_iso()
    return accum

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-trends", action="store_true")
    parser.add_argument("--refresh-hooks", action="store_true")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("❌ Falta OPENAI_API_KEY en tu entorno.")

    market = load_json(MARKET_PATH)
    product = extract_product(market)
    angles = extract_angles(market)

    # Generate dynamic filename
    product_name = product.get("nombre_producto", "producto").strip()
    safe_name = "".join([c if c.isalnum() else "_" for c in product_name]).lower()
    accum_path = os.path.join(OUTPUT_DIR, f"nanobanana_image_{safe_name}.json")

    # Reuse caches (no fetch logic here, assumed to exist or empty fallback if not implemented)
    # For simplicity, we just load them if they exist, or pass empty dicts if not.
    # In a full implementation, we'd import the fetch functions or duplicate them.
    # Since the user wants "simple", we'll assume caches are populated by the carousel agent
    # OR we can just duplicate the fetch logic if needed. 
    # Let's try to load, if not exist, we warn.
    
    trends_pack = {}
    if os.path.exists(TRENDS_CACHE_PATH):
        trends_pack = load_json(TRENDS_CACHE_PATH)
    
    hooks_pack = {}
    if os.path.exists(HOOKS_CACHE_PATH):
        hooks_pack = load_json(HOOKS_CACHE_PATH)

    # Iterate first 3 angles
    for angle_idx in range(3):
        if angle_idx >= len(angles):
            print(f"⚠️ Solo hay {len(angles)} ángulos disponibles. Saltando índice {angle_idx}.")
            continue

        print(f"\n--- Procesando Imagen Ángulo {angle_idx + 1} ---")
        
        angle_norm = normalize_angle(angles[angle_idx], fallback_rank=angle_idx + 1)
        hooks_hints = extract_hooks_for_rank(market, rank=int(angle_norm["rank"]))
        est_hint = extract_estacionalidad_hint(market)

        payload = build_payload(
            product=product,
            angle=angle_norm,
            hooks_hints=hooks_hints,
            est_hint=est_hint,
            trends_pack=trends_pack,
            hooks_pack=hooks_pack,
        )

        try:
            angle_result = call_image_agent(payload, angle_id=angle_norm["angle_id"])

            accum = load_or_init_accum(product, accum_path)
            accum = upsert_angle(accum, angle_result)
            save_json(accum_path, accum)

            print(f"✅ OK: {angle_norm['angle_id']} guardado en {accum_path}")

        except Exception as e:
            print(f"❌ Error procesando ángulo {angle_idx + 1}: {e}")
            continue

    print(f"\n🎉 Proceso finalizado. Output final: {accum_path}")

if __name__ == "__main__":
    main()
