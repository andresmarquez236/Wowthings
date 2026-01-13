# nanobanana_carrusel_agent.py
# ------------------------------------------------------------
# AIDA + Carousel + NanoBanana Pro prompts (incremental per angle)
#
# INPUT:
#   output/market_research_min.json
#
# OUTPUT:
#   output/aida_carousel_accum.json  (upsert por angle_id)
#
# CACHES (web_search 1 vez):
#   output/hooks_pack.json
#   output/trends_pack.json
#
# REQUIREMENTS:
#   pip install openai python-dotenv
#
# ENV:
#   export OPENAI_API_KEY="..."
# Optional:
#   export AIDA_AGENT_MODEL="gpt-5-mini"   # or "gpt-5"
#   export MARKET_RESEARCH_PATH="output/market_research_min.json"
#   export OUTPUT_DIR="output"
#
# RUN:
#   python nanobanana_carrusel_agent.py --angle-index 0
#   python nanobanana_carrusel_agent.py --angle-index 1
#   python nanobanana_carrusel_agent.py --angle-index 2
# Refresh caches:
#   python nanobanana_carrusel_agent.py --angle-index 0 --refresh-hooks --refresh-trends
# ------------------------------------------------------------

import os
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Callable, Optional

from dotenv import load_dotenv
from openai import OpenAI
import sys
sys.path.append(os.getcwd())

from utils.logger import setup_logger
logger = setup_logger("AdsGen_Carrusel_V1")

# --- error types (compat) ---
try:
    from openai import BadRequestError
except Exception:
    BadRequestError = Exception  # fallback

load_dotenv()

MODEL = os.getenv("AIDA_AGENT_MODEL", "gpt-4o")
MARKET_PATH = os.getenv("MARKET_RESEARCH_PATH", "output/market_research_min.json")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRENDS_CACHE_PATH = os.path.join(OUTPUT_DIR, "trends_pack.json")
HOOKS_CACHE_PATH = os.path.join(OUTPUT_DIR, "hooks_pack.json")
ACCUM_PATH = os.path.join(OUTPUT_DIR, "aida_carousel_accum.json")

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
    # new SDK usually has output_text
    if hasattr(resp, "output_text") and resp.output_text:
        return resp.output_text

    # Standard ChatCompletion response
    if hasattr(resp, "choices") and resp.choices:
        content = resp.choices[0].message.content
        if not content:
            # Check for refusal (Structured Outputs/Safety)
            refusal = getattr(resp.choices[0].message, "refusal", None)
            if refusal:
                raise RuntimeError(f"El modelo rechazó la respuesta (Refusal): {refusal}")
            
            # Dump full message for debug
            raise RuntimeError(f"El modelo retornó contenido vacío (content is None/Empty). Msg: {resp.choices[0].message}")
        return content

    # fallback scan
    try:
        for item in getattr(resp, "output", []) or []:
            if getattr(item, "type", None) != "message":
                continue
            for c in getattr(item, "content", []) or []:
                # various SDK shapes
                t = getattr(c, "text", None)
                if t:
                    return t
                if getattr(c, "type", None) in ("output_text", "text") and getattr(c, "text", None):
                    return c.text
    except Exception:
        pass

    raise RuntimeError("No se pudo extraer output_text del response (SDK openai).")

def parse_json_or_dump(raw: str, dump_path: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write(raw)
        logger.error(f"JSON inválido. RAW guardado en: {dump_path}")
        raise RuntimeError(f"JSON inválido. RAW guardado en: {dump_path}")

def safe_responses_create(**kwargs):
    """
    gpt-5 / gpt-5-mini: NO soportan 'temperature' en responses.create.
    Si por accidente existe, lo quitamos y reintentamos.
    """
    try:
        return client.chat.completions.create(**kwargs)
    except BadRequestError as e:
        msg = str(e)
        if "temperature" in msg:
            kwargs.pop("temperature", None)
            return client.chat.completions.create(**kwargs)
        raise

def call_with_retries(create_fn: Callable[[], Any], raw_dump_prefix: str, retries: int = 2) -> Dict[str, Any]:
    """
    Ejecuta llamada a modelo y parsea JSON. Si falla JSON por truncamiento/ruido, reintenta.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 2):
        try:
            resp = create_fn()
            raw = get_output_text(resp)
            dump_path = os.path.join(OUTPUT_DIR, f"{raw_dump_prefix}_raw_attempt{attempt}_{int(time.time())}.txt")
            return parse_json_or_dump(raw, dump_path)
        except Exception as e:
            last_err = e
            # small backoff
            time.sleep(0.35 * attempt)
    raise last_err  # type: ignore

# ============================================================
# Market Research Extractors (from market_research_min.json)
# ============================================================

def extract_product(market: Dict[str, Any]) -> Dict[str, str]:
    # Try to find product info in 'input' (preferred) or 'meta.producto_input' (fallback)
    producto_input = market.get("input", {}) or ((market.get("meta", {}) or {}).get("producto_input", {}) or {})

    nombre = str(producto_input.get("nombre_producto", market.get("product_name", "Producto"))).strip() or "Producto"
    # Remove "Ejemplo: " prefix if present
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
            if int(item.get("rank")) == rank and isinstance(item.get("hooks"), list):
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
# JSON Schemas (STRICT)
# ============================================================

TRENDS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["trend_sources", "creative_patterns", "typography", "badge_system", "do_not_do"],
    "properties": {
        "trend_sources": {
            "type": "array",
            "minItems": 2,
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["url", "title", "published_date", "used_for"],
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "published_date": {"type": "string"},
                    "used_for": {"type": "string"},
                },
            },
        },
        "creative_patterns": {"type": "array", "minItems": 10, "maxItems": 18, "items": {"type": "string"}},
        "typography": {
            "type": "object",
            "additionalProperties": False,
            "required": ["headline_rules", "stroke_shadow_rules", "safe_margin_rules"],
            "properties": {
                "headline_rules": {"type": "array", "minItems": 6, "maxItems": 12, "items": {"type": "string"}},
                "stroke_shadow_rules": {"type": "array", "minItems": 4, "maxItems": 10, "items": {"type": "string"}},
                "safe_margin_rules": {"type": "array", "minItems": 3, "maxItems": 8, "items": {"type": "string"}},
            },
        },
        "badge_system": {"type": "array", "minItems": 6, "maxItems": 12, "items": {"type": "string"}},
        "do_not_do": {"type": "array", "minItems": 6, "maxItems": 14, "items": {"type": "string"}},
    },
}

HOOKS_PACK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sources", "hook_archetypes", "high_performing_patterns", "latin_es_templates"],
    "properties": {
        "sources": {
            "type": "array",
            "minItems": 2,
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["url", "title", "published_date", "used_for"],
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "published_date": {"type": "string"},
                    "used_for": {"type": "string"},
                },
            },
        },
        "hook_archetypes": {"type": "array", "minItems": 12, "maxItems": 24, "items": {"type": "string"}},
        "high_performing_patterns": {"type": "array", "minItems": 8, "maxItems": 18, "items": {"type": "string"}},
        "latin_es_templates": {"type": "array", "minItems": 15, "maxItems": 35, "items": {"type": "string"}},
    },
}

AIDA_CAROUSEL_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "angle_id",
        "angle_name",
        "buyer_persona",
        "aida_strategy",
        "ad_copy",
        "carousel",
        "hooks_used",
        "compliance_notes",
    ],
    "properties": {
        "angle_id": {"type": "string"},
        "angle_name": {"type": "string"},
        "buyer_persona": {"type": "string"},
        "aida_strategy": {
            "type": "object",
            "additionalProperties": False,
            "required": ["attention", "interest", "desire", "action"],
            "properties": {
                "attention": {"type": "string"},
                "interest": {"type": "string"},
                "desire": {"type": "string"},
                "action": {"type": "string"},
            },
        },
        "ad_copy": {
            "type": "object",
            "additionalProperties": False,
            "required": ["title", "primary_text", "description"],
            "properties": {
                "title": {"type": "string"},
                "primary_text": {"type": "string"},
                "description": {"type": "string"},
            },
        },
        "carousel": {
            "type": "object",
            "additionalProperties": False,
            "required": ["num_cards", "cards"],
            "properties": {
                "num_cards": {"type": "integer", "minimum": 2, "maximum": 3},
                "cards": {
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["card_index", "stage", "goal", "nanobanana_prompt"],
                        "properties": {
                            "card_index": {"type": "integer"},
                            "stage": {"type": "string"},  # Attention / Interest / Desire / Action
                            "goal": {"type": "string"},
                            "nanobanana_prompt": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "task",
                                    "thumbnail",
                                    "variant",
                                    "format",
                                    "input_assets",
                                    "prompt",
                                    "text_overlays",
                                    "composition_rules",
                                    "negative_prompt",
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
                                            "safe_margin_percent": {"type": "integer"},
                                        },
                                    },
                                    "input_assets": {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "required": ["product_lock_rule"],
                                        "properties": {"product_lock_rule": {"type": "string"}},
                                    },
                                    "prompt": {"type": "string"},  # ENGLISH
                                    "text_overlays": {
                                        "type": "array",
                                        "minItems": 3,
                                        "maxItems": 6,
                                        "items": {
                                            "type": "object",
                                            "additionalProperties": False,
                                            "required": ["id", "text_exact", "placement", "typography", "style"],
                                            "properties": {
                                                "id": {"type": "string"},
                                                "text_exact": {"type": "string"},  # SPANISH
                                                "placement": {
                                                    "type": "object",
                                                    "additionalProperties": False,
                                                    "required": ["anchor", "x_percent", "y_percent", "max_width_percent", "max_height_percent"],
                                                    "properties": {
                                                        "anchor": {"type": "string"},
                                                        "x_percent": {"type": "integer"},
                                                        "y_percent": {"type": "integer"},
                                                        "max_width_percent": {"type": "integer"},
                                                        "max_height_percent": {"type": "integer"},
                                                    },
                                                },
                                                "typography": {
                                                    "type": "object",
                                                    "additionalProperties": False,
                                                    "required": ["font_family", "weight", "alignment"],
                                                    "properties": {
                                                        "font_family": {"type": "string"},
                                                        "weight": {"type": "integer"},
                                                        "alignment": {"type": "string"},
                                                    },
                                                },
                                                "style": {
                                                    "type": "object",
                                                    "additionalProperties": False,
                                                    "required": ["fill", "stroke", "shadow", "badge"],
                                                    "properties": {
                                                        "fill": {"type": ["string", "null"]},
                                                        "stroke": {"type": ["string", "null"]},
                                                        "shadow": {"type": ["string", "null"]},
                                                        "badge": {"type": ["string", "null"]},
                                                    },
                                                },
                                            },
                                        },
                                    },
                                    "composition_rules": {
                                        "type": "object",
                                        "additionalProperties": False,
                                        "required": ["focus", "hierarchy", "text_safe_zone", "text_never_cover_rule"],
                                        "properties": {
                                            "focus": {"type": "string"},
                                            "hierarchy": {"type": "string"},
                                            "text_safe_zone": {"type": "string"},
                                            "text_never_cover_rule": {"type": "string"},
                                        },
                                    },
                                    "negative_prompt": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            },
        },
        "hooks_used": {"type": "array", "minItems": 5, "maxItems": 12, "items": {"type": "string"}},
        "compliance_notes": {"type": "array", "minItems": 3, "maxItems": 10, "items": {"type": "string"}},
    },
}

# ============================================================
# PROMPTS (super prompt)
# ============================================================

TRENDS_SYSTEM = (
    "You are a Creative Intelligence Researcher for Meta ecommerce creatives.\n"
    "Use web_search. Return ONLY JSON matching schema. No markdown.\n"
    "Rules: Do not fabricate facts. Prefer Colombia/LATAM when possible; otherwise global but mobile-feed applicable."
)

TRENDS_USER = (
    "Collect CURRENT, widely applicable patterns for ecommerce carousel cards and thumbnails (Meta-first).\n"
    "Return:\n"
    "- trend_sources (2-8)\n"
    "- creative_patterns (10-18)\n"
    "- typography rules: headline_rules, stroke_shadow_rules, safe_margin_rules\n"
    "- badge_system guidance\n"
    "- do_not_do mistakes\n"
)

HOOKS_SYSTEM = (
    "You are a performance copy researcher specialized in hooks for paid social (Meta/IG/FB).\n"
    "Use web_search. Return ONLY JSON matching schema. No markdown.\n"
    "Hard rules: do not claim 'best' without evidence; produce practical patterns and templates in Spanish LATAM."
)

HOOKS_USER = (
    "Research RECENT hook archetypes and patterns for ecommerce ads (Meta/IG/FB), ideally 2024-2025.\n"
    "Return:\n"
    "- sources (2-8)\n"
    "- hook_archetypes (12-24)\n"
    "- high_performing_patterns (8-18)\n"
    "- latin_es_templates (15-35) short Spanish LATAM templates, no product-specific claims.\n"
)

AIDA_SYSTEM_SUPER = r"""
ROLE
You are a PhD-level Attention Psychology + Creative Strategy expert for Meta Ads.
You must build a complete AIDA strategy and a carousel (2–4 cards) where each card includes:
- stage (Attention / Interest / Desire / Action)
- goal
- a production-grade NanoBanana Pro JSON prompt.

OUTPUT RULES (NON-NEGOTIABLE)
- Return ONLY valid JSON matching the provided schema. No markdown. No extra text.
- PROMPT FIELD MUST BE IN ENGLISH (image model friendly).
- text_overlays[].text_exact MUST BE IN SPANISH (LATAM), short and readable.
- Choose carousel size 2–3 based on the best AIDA flow:
  * 2 cards: Card1=Attention+Interest, Card2=Desire+Action
  * 3 cards: Card1=Attention, Card2=Interest+Desire, Card3=Action
- Consistency across cards:
  * same visual system (lighting mood, background style, typography style, badge style)
  * but varied compositions to keep attention (different scene archetype / framing / angle)
- Product must be the SHARPEST element on every card; background softly blurred/bokeh.
- Absolutely NO invented specs, certifications, medical claims, performance numbers, “más vendido”, or unconfirmed warranty terms.
- Must include two default badges unless user input explicitly forbids:
  * "Envío gratis"
  * "Pago contraentrega"
- Text legibility and NEVER covered:
  * Define a “text_safe_zone” string describing the reserved empty area.
  * Provide a “text_never_cover_rule” that forbids placing product over the text zone.
  * Place text within safe margins (10%) with explicit placements.

HOOKS REQUIREMENT
- Combine and select the strongest hooks by using:
  a) market hooks hints (angle-specific)
  b) hooks_pack templates and patterns (researched)
- Output hooks_used (5–12) that are applicable to this angle WITHOUT inventing claims.

VISUAL PSYCHOLOGY / ATTENTION TRIGGERS
Each card should use at least one trigger appropriate to the stage:
- Attention: pattern interrupt + curiosity gap
- Interest: single clear mechanism/value cue (visual metaphor allowed)
- Desire: vivid outcome framing + objection softening (without lies)
- Action: clear CTA + low-friction instruction (e.g., “Pide hoy”, “Escríbenos”, “Compra ahora”)

NANOBANANA PROMPT QUALITY
Each card nanobanana_prompt must include:
- scene archetype + composition + lighting + background blur
- exact overlay placements with anchors and x/y percents
- typography rules (bold sans serif, strong stroke/shadow)
- negative_prompt forbidding: wrong product model, logos, watermarks, faces, extra text beyond overlays
""".strip()

# ============================================================
# Cache Builders (web_search)
# ============================================================

def fetch_trends_pack() -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": TRENDS_SYSTEM},
                {"role": "user", "content": TRENDS_USER},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "trends_pack",
                    "schema": TRENDS_SCHEMA,
                    "strict": True
                }
            },
            max_completion_tokens=1400,
        )
    return call_with_retries(_create, "debug_trends", retries=2)

def fetch_hooks_pack() -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": HOOKS_SYSTEM},
                {"role": "user", "content": HOOKS_USER},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "hooks_pack",
                    "schema": HOOKS_PACK_SCHEMA,
                    "strict": True
                }
            },
            max_completion_tokens=1800,
        )
    return call_with_retries(_create, "debug_hooks", retries=2)

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
# AIDA payload + call (per angle)
# ============================================================

def build_aida_payload(
    product: Dict[str, str],
    angle: Dict[str, str],
    hooks_hints: List[str],
    est_hint: Dict[str, Any],
    trends_pack: Dict[str, Any],
    hooks_pack: Dict[str, Any],
) -> Dict[str, Any]:
    product_lock_rule = (
        f"CRITICAL: Use ONLY the exact {product['nombre_producto']} from the provided reference image. "
        f"Do NOT generate a different model/variant. Prefer cutout/masking and compositing into a NEW scene."
    )

    # compact to keep stable token usage
    trends_mini = {
        "creative_patterns": (trends_pack.get("creative_patterns", []) or [])[:12],
        "typography": trends_pack.get("typography", {}),
        "badge_system": (trends_pack.get("badge_system", []) or [])[:10],
        "do_not_do": (trends_pack.get("do_not_do", []) or [])[:10],
    }

    hooks_mini = {
        "hook_archetypes": (hooks_pack.get("hook_archetypes", []) or [])[:18],
        "high_performing_patterns": (hooks_pack.get("high_performing_patterns", []) or [])[:12],
        "latin_es_templates": (hooks_pack.get("latin_es_templates", []) or [])[:28],
    }

    return {
        "product": product,
        "angle": angle,
        "hooks_hints_from_market": (hooks_hints or [])[:10],
        "estacionalidad_hint": est_hint,
        "trends_pack": trends_mini,
        "hooks_pack": hooks_mini,
        "defaults": {
            "format": {"aspect_ratio": "1:1", "resolution": "1080x1080", "safe_margin_percent": 10},
            "badges": ["Envío gratis", "Pago contraentrega"],
            "task": "meta_square_carousel_card",
            "variant": "with_text",
        },
        "input_assets": {"product_lock_rule": product_lock_rule},
        "constraints": {
            "no_faces": True,
            "no_logos": True,
            "no_watermarks": True,
            "no_extra_text_outside_overlays": True,
        },
    }

def call_aida_agent(payload: Dict[str, Any], angle_id: str) -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": AIDA_SYSTEM_SUPER},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": f"carousel_{angle_id}",
                    "schema": AIDA_CAROUSEL_SCHEMA,
                    "strict": True,
                }
            },
            max_completion_tokens=10000, # suficiente para 2–3 cards
        )

    return call_with_retries(_create, f"debug_aida_{angle_id}", retries=2)

# ============================================================
# Accumulator (upsert)
# ============================================================

# ============================================================
# Accumulator (upsert)
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
        "schema_version": "aida_carousel_incremental_v1",
        "generated_at_utc": now_utc_iso(),
        "product": product,
        "trend_cache_path": TRENDS_CACHE_PATH,
        "hooks_cache_path": HOOKS_CACHE_PATH,
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
# Main (batch 3 angles)
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
    accum_path = os.path.join(OUTPUT_DIR, f"nanobanana_carrusel_{safe_name}.json")

    # caches (web_search only when missing/refresh)
    trends_pack = load_or_create_cache(TRENDS_CACHE_PATH, fetch_trends_pack, args.refresh_trends)
    hooks_pack = load_or_create_cache(HOOKS_CACHE_PATH, fetch_hooks_pack, args.refresh_hooks)

    # Iterate first 3 angles
    for angle_idx in range(3):
        if angle_idx >= len(angles):
            logger.warning(f"Solo hay {len(angles)} ángulos disponibles. Saltando índice {angle_idx}.")
            continue

        logger.info(f"--- Procesando Ángulo {angle_idx + 1} ---")
        
        angle_norm = normalize_angle(angles[angle_idx], fallback_rank=angle_idx + 1)
        hooks_hints = extract_hooks_for_rank(market, rank=int(angle_norm["rank"]))
        est_hint = extract_estacionalidad_hint(market)

        payload = build_aida_payload(
            product=product,
            angle=angle_norm,
            hooks_hints=hooks_hints,
            est_hint=est_hint,
            trends_pack=trends_pack,
            hooks_pack=hooks_pack,
        )

        try:
            angle_result = call_aida_agent(payload, angle_id=angle_norm["angle_id"])

            # sanity checks (hard)
            carousel = angle_result.get("carousel", {})
            num_cards = carousel.get("num_cards")
            cards = carousel.get("cards", [])
            if not isinstance(num_cards, int) or not (2 <= num_cards <= 3):
                raise RuntimeError("Salida inválida: carousel.num_cards debe estar entre 2 y 3.")
            if not isinstance(cards, list) or len(cards) != num_cards:
                raise RuntimeError("Salida inválida: cards no coincide con num_cards.")

            accum = load_or_init_accum(product, accum_path)
            accum = upsert_angle(accum, angle_result)
            save_json(accum_path, accum)

            logger.info(f"OK: {angle_norm['angle_id']} guardado en {accum_path}")

        except Exception as e:
            logger.error(f"Error procesando ángulo {angle_idx + 1}: {e}")
            # Continue to next angle even if one fails
            continue

    logger.info(f"Proceso finalizado. Output final: {accum_path}")

if __name__ == "__main__":
    main()
