# wow_agent/ads_generator/nanobanana_thumbnail_agent.py
# -*- coding: utf-8 -*-

import os
import sys
import json
import re
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
load_dotenv()

# Para que funcione el import de fix_format aunque ejecutes desde otro cwd
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from fix_format import load_market_research_min, extract_context_and_first_three_angles


# -----------------------------
# Config
# -----------------------------
# Modelo oficial recomendado: gpt-4o
MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")

# Bajo = más compliance; alto = más creatividad pero más riesgo de formato
TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.15"))
MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "2600"))

# Proyecto: wow_agent/
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(PROJECT_ROOT, "output"))

INPUT_JSON_PATH = os.getenv(
    "MARKET_RESEARCH_MIN_PATH",
    os.path.join(OUTPUT_DIR, "market_research_min.json")
)

ACCUM_PATH = os.getenv(
    "NANOBANANA_THUMBNAILS_ACCUM_PATH",
    os.path.join(OUTPUT_DIR, "nanobanana_thumbnails_accum.json")
)

# Si quieres forzar una regla CRITICAL (ej: usar SOLO producto de imagen referencia)
PRODUCT_LOCK_RULE_OVERRIDE = os.getenv("PRODUCT_LOCK_RULE", "").strip()


# -----------------------------
# OpenAI client
# -----------------------------
def get_openai_client():
    try:
        from openai import OpenAI
    except ImportError as e:
        raise RuntimeError("Instala openai: pip install --upgrade openai") from e
    return OpenAI()


# -----------------------------
# Helpers
# -----------------------------
def minify_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def slugify(text: str, max_len: int = 40) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_-]+", "_", text, flags=re.UNICODE)
    return text[:max_len].strip("_") or "product"

def safe_json_load(path: str) -> Any:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def safe_json_dump(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# -----------------------------
# Strict validation (estructura tipo tu ejemplo)
# -----------------------------
REQUIRED_THUMBNAIL_KEYS = [
    "task", "thumbnail", "variant", "format", "input_assets",
    "prompt", "text_overlays", "composition_rules", "negative_prompt"
]
REQUIRED_FORMAT_KEYS = ["aspect_ratio", "resolution", "safe_margin_percent"]

REQUIRED_OVERLAY_KEYS = ["id", "text_exact", "placement", "typography", "style"]
REQUIRED_PLACEMENT_KEYS = ["anchor", "x_percent", "y_percent", "max_width_percent", "max_height_percent"]
REQUIRED_TYPO_KEYS = ["font_family", "weight", "alignment"]


def validate_thumbnail_object(obj: Dict[str, Any]) -> None:
    for k in REQUIRED_THUMBNAIL_KEYS:
        if k not in obj:
            raise ValueError(f"Falta key requerida: {k}")

    fmt = obj["format"]
    for k in REQUIRED_FORMAT_KEYS:
        if k not in fmt:
            raise ValueError(f"format.{k} es requerido")

    overlays = obj["text_overlays"]
    if not isinstance(overlays, list):
        raise ValueError("text_overlays debe ser lista")
    if len(overlays) != 3:
        raise ValueError("text_overlays debe tener EXACTAMENTE 3 items (headline + 2 badges)")

    for i, ov in enumerate(overlays):
        if not isinstance(ov, dict):
            raise ValueError(f"text_overlays[{i}] debe ser objeto")
        for k in REQUIRED_OVERLAY_KEYS:
            if k not in ov:
                raise ValueError(f"text_overlays[{i}].{k} es requerido")

        placement = ov["placement"]
        if not isinstance(placement, dict):
            raise ValueError(f"text_overlays[{i}].placement debe ser objeto")
        for k in REQUIRED_PLACEMENT_KEYS:
            if k not in placement:
                raise ValueError(f"text_overlays[{i}].placement.{k} es requerido")

        typo = ov["typography"]
        if not isinstance(typo, dict):
            raise ValueError(f"text_overlays[{i}].typography debe ser objeto")
        for k in REQUIRED_TYPO_KEYS:
            if k not in typo:
                raise ValueError(f"text_overlays[{i}].typography.{k} es requerido")

        style = ov["style"]
        if not isinstance(style, dict):
            raise ValueError(f"text_overlays[{i}].style debe ser objeto")

    if not isinstance(obj["composition_rules"], dict):
        raise ValueError("composition_rules debe ser objeto")


def validate_response_exact_three(thumbnails: Any) -> List[Dict[str, Any]]:
    if not isinstance(thumbnails, list):
        raise ValueError("La respuesta NO es un JSON array (lista).")
    if len(thumbnails) != 3:
        raise ValueError(f"La lista NO tiene exactamente 3 elementos (tiene {len(thumbnails)}).")
    for t in thumbnails:
        validate_thumbnail_object(t)
    return thumbnails


# -----------------------------
# NORMALIZADOR (anti-crash)
# -----------------------------
def _ensure_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}

def _ensure_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []

def _default_overlay(overlay_id: str, text_exact: str) -> Dict[str, Any]:
    # Placements por defecto (con safe margin)
    if overlay_id == "headline":
        placement = {"anchor": "bottom_center", "x_percent": 50, "y_percent": 88, "max_width_percent": 92, "max_height_percent": 14}
        typography = {"font_family": "Bold clean sans-serif", "weight": 900, "alignment": "center"}
        style = {"fill": "white", "stroke": "dark outline (thick)", "shadow": "subtle"}
    elif overlay_id == "badge_free_shipping":
        placement = {"anchor": "top_left", "x_percent": 12, "y_percent": 12, "max_width_percent": 28, "max_height_percent": 10}
        typography = {"font_family": "Clean sans-serif", "weight": 800, "alignment": "center"}
        style = {"badge": "rounded sticker, solid color, high contrast", "fill": "white"}
    else:  # badge_cod
        placement = {"anchor": "top_right", "x_percent": 88, "y_percent": 14, "max_width_percent": 34, "max_height_percent": 10}
        typography = {"font_family": "Clean sans-serif", "weight": 800, "alignment": "center"}
        style = {"badge": "rounded sticker, slightly smaller than 'Envío gratis', high contrast", "fill": "white"}

    return {
        "id": overlay_id,
        "text_exact": text_exact,
        "placement": placement,
        "typography": typography,
        "style": style,
    }

def normalize_thumbnail_obj(
    obj: Dict[str, Any],
    *,
    product_slug: str,
    angle_rank: int,
    variant_num: int,
    product_lock_rule: str,
    headline_text: str
) -> Dict[str, Any]:
    obj = _ensure_dict(obj)

    # Top-level defaults
    obj.setdefault("task", "meta_square_thumbnail")
    obj.setdefault("variant", "with_text")

    fmt = _ensure_dict(obj.get("format"))
    fmt.setdefault("aspect_ratio", "1:1")
    fmt.setdefault("resolution", "1080x1080")
    fmt.setdefault("safe_margin_percent", 10)
    obj["format"] = fmt

    obj.setdefault("thumbnail", f"{product_slug}_A{angle_rank}_V{variant_num}")

    input_assets = _ensure_dict(obj.get("input_assets"))
    input_assets["product_lock_rule"] = str(product_lock_rule)
    obj["input_assets"] = input_assets

    # Prompt default eliminada para forzar error si el LLM no la genera
    # obj.setdefault(
    #     "prompt",
    #     "Create an original square (1:1) high-conversion ecommerce thumbnail. "
    #     "Make the product the largest and sharpest element. Clean modern layout, high contrast, "
    #     "soft bokeh background, no watermarks, no brand logos, no recognizable faces."
    # )

    # composition_rules default si faltara
    comp = _ensure_dict(obj.get("composition_rules"))
    comp.setdefault("focus", "Product must be the sharpest and largest element; background soft bokeh.")
    comp.setdefault("text_legibility", "If text is not readable, adjust ONLY contrast and brightness (do not change composition).")
    obj["composition_rules"] = comp

    obj.setdefault(
        "negative_prompt",
        "No watermarks, no brand logos, no copyrighted characters, no recognizable faces, no extra text beyond specified overlays."
    )

    # Overlays: siempre EXACTAMENTE 3
    overlays = _ensure_list(obj.get("text_overlays"))

    # intentamos mapear por id si ya vienen
    by_id = {}
    for ov in overlays:
        if isinstance(ov, dict) and "id" in ov:
            by_id[str(ov["id"])] = ov

    # headline
    h = _ensure_dict(by_id.get("headline"))
    h.setdefault("id", "headline")
    h.setdefault("text_exact", headline_text)
    h.setdefault("placement", {})
    h.setdefault("typography", {})
    h.setdefault("style", {})  # <- clave para tu error
    # rellena nested
    dh = _default_overlay("headline", h["text_exact"])
    h["placement"] = {**dh["placement"], **_ensure_dict(h.get("placement"))}
    h["typography"] = {**dh["typography"], **_ensure_dict(h.get("typography"))}
    h["style"] = {**dh["style"], **_ensure_dict(h.get("style"))}

    # badge_free_shipping
    b1 = _ensure_dict(by_id.get("badge_free_shipping"))
    b1.setdefault("id", "badge_free_shipping")
    b1.setdefault("text_exact", "Envío gratis")
    b1.setdefault("placement", {})
    b1.setdefault("typography", {})
    b1.setdefault("style", {})
    db1 = _default_overlay("badge_free_shipping", b1["text_exact"])
    b1["placement"] = {**db1["placement"], **_ensure_dict(b1.get("placement"))}
    b1["typography"] = {**db1["typography"], **_ensure_dict(b1.get("typography"))}
    b1["style"] = {**db1["style"], **_ensure_dict(b1.get("style"))}

    # badge_cod
    b2 = _ensure_dict(by_id.get("badge_cod"))
    b2.setdefault("id", "badge_cod")
    b2.setdefault("text_exact", "Pago contraentrega")
    b2.setdefault("placement", {})
    b2.setdefault("typography", {})
    b2.setdefault("style", {})
    db2 = _default_overlay("badge_cod", b2["text_exact"])
    b2["placement"] = {**db2["placement"], **_ensure_dict(b2.get("placement"))}
    b2["typography"] = {**db2["typography"], **_ensure_dict(b2.get("typography"))}
    b2["style"] = {**db2["style"], **_ensure_dict(b2.get("style"))}

    obj["text_overlays"] = [h, b1, b2]
    return obj

def normalize_thumbnails_list(
    thumbnails: Any,
    *,
    product_slug: str,
    angle_rank: int,
    product_lock_rule: str,
    headline_text: str
) -> List[Dict[str, Any]]:
    thumbs = _ensure_list(thumbnails)

    # Si vienen menos de 3, duplicamos el último o creamos vacíos
    while len(thumbs) < 3:
        thumbs.append({})

    # Si vienen más, recortamos
    thumbs = thumbs[:3]

    out = []
    for i, t in enumerate(thumbs, start=1):
        out.append(
            normalize_thumbnail_obj(
                _ensure_dict(t),
                product_slug=product_slug,
                angle_rank=angle_rank,
                variant_num=i,
                product_lock_rule=product_lock_rule,
                headline_text=headline_text,
            )
        )
    return out


# -----------------------------
# Tool schema (function calling) - estable
# -----------------------------
def build_tools_schema() -> List[Dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "return_thumbnails",
                "description": "Return exactly 3 thumbnail prompt objects.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "thumbnails": {
                            "type": "array",
                            "minItems": 3,
                            "maxItems": 3,
                            "items": {"type": "object"}  # dejamos amplio; normalizador corrige estructura
                        }
                    },
                    "required": ["thumbnails"]
                }
            }
        }
    ]


# -----------------------------
# Prompt Engineering
# -----------------------------
def build_system_prompt() -> str:
    return (
        "You are NanoBanana Pro A — elite ecommerce thumbnail prompt engineer.\n"
        "You MUST use the tool return_thumbnails.\n\n"
        "HARD RULES:\n"
        "- Do NOT invent product specs.\n"
        "- No watermarks, no brand logos, no copyrighted characters.\n"
        "- No recognizable faces.\n"
        "- Product must be the hero: largest + sharpest.\n"
        "- Clean layout, high contrast.\n"
        "- EXACTLY 3 thumbnails.\n\n"
        "Output fields per thumbnail MUST include:\n"
        "task, thumbnail, variant, format, input_assets, prompt, text_overlays, composition_rules, negative_prompt.\n"
        "text_overlays MUST be a LIST of EXACTLY 3 overlays: headline, badge_free_shipping, badge_cod.\n"
        "Each overlay MUST include: id, text_exact, placement, typography, style.\n"
        "composition_rules MUST include: focus, text_legibility.\n"
        "prompt MUST be in ENGLISH.\n"
    )


def build_user_prompt(payload_min: str, angle: Dict[str, Any], product_slug: str, product_lock_rule: str) -> str:
    r = angle.get("rank")
    promesa = (angle.get("promesa") or "").strip()

    return (
        "Generate 3 distinct high-conversion thumbnails for ONLY this angle.\n"
        "Return via tool return_thumbnails.\n\n"
        f"ANGLE_RANK: {r}\n"
        f"ANGLE_PROMISE: {promesa}\n"
        f"ANGLE_DATA: {json.dumps(angle, ensure_ascii=False)}\n\n"
        "REQUIRED:\n"
        "- task='meta_square_thumbnail'\n"
        "- variant='with_text'\n"
        "- format={aspect_ratio:'1:1', resolution:'1080x1080', safe_margin_percent:10}\n"
        f"- thumbnail ids must be: '{product_slug}_A{r}_V1', '{product_slug}_A{r}_V2', '{product_slug}_A{r}_V3'\n"
        f"- input_assets.product_lock_rule MUST be EXACTLY:\n{product_lock_rule}\n"
        "- text_overlays must be EXACTLY 3 overlays with ids: headline, badge_free_shipping, badge_cod\n"
        "- badge texts must be EXACTLY: 'Envío gratis' and 'Pago contraentrega'\n"
        "- headline should reflect the angle promise (3-5 words max, Spanish ok)\n"
        "- composition_rules must include focus and text_legibility\n"
        "- negative_prompt must be strict\n\n"
        "GLOBAL CONTEXT (minified JSON):\n"
        f"{payload_min}\n"
    )


# -----------------------------
# OpenAI call (tool calling)
# -----------------------------
def call_with_tool(client, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    tools = build_tools_schema()
    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=TEMPERATURE,
        max_completion_tokens=MAX_TOKENS,
        tools=tools,
        tool_choice={"type": "function", "function": {"name": "return_thumbnails"}},
    )

    msg = resp.choices[0].message
    if not getattr(msg, "tool_calls", None):
        raise RuntimeError("El modelo no hizo tool_call (return_thumbnails).")

    tool_call = msg.tool_calls[0]
    args_str = tool_call.function.arguments
    return json.loads(args_str)


def headline_from_angle(angle: Dict[str, Any]) -> str:
    # Headline: usa promesa si existe; si no, angulo; si no, genérico
    promesa = (angle.get("promesa") or "").strip()
    angulo = (angle.get("angulo") or "").strip()

    base = promesa or angulo or "Oferta limitada"
    # lo dejamos corto (3-5 palabras aprox). No nos ponemos muy strict para no cortar feo.
    words = base.split()
    if len(words) > 5:
        base = " ".join(words[:5])
    return base


def fallback_templates(
    *,
    product_slug: str,
    angle_rank: int,
    product_lock_rule: str,
    headline_text: str
) -> List[Dict[str, Any]]:
    # Tres variantes limpias y válidas (por si el modelo insiste en omitirte campos)
    base_prompts = [
        "Create a high-conversion square (1:1) ecommerce thumbnail. Clean studio scene with soft gradient background and subtle bokeh. Place the product very large and ultra sharp in the center. High contrast, premium look, no clutter.",
        "Create a lifestyle square (1:1) ecommerce thumbnail. Cozy indoor scene with soft warm lighting and blurred background. Product is the hero on a simple surface, ultra sharp; background softly blurred (bokeh). No recognizable faces.",
        "Create a benefit-focused square (1:1) ecommerce thumbnail. Minimal modern scene with a clear visual metaphor that supports the angle promise (no text beyond overlays). Product is largest and sharpest element. High contrast, clean composition."
    ]

    thumbs = []
    for i, p in enumerate(base_prompts, start=1):
        thumbs.append(
            normalize_thumbnail_obj(
                {
                    "prompt": p,
                    "negative_prompt": "No watermarks, no brand logos, no copyrighted characters, no recognizable faces, no extra text beyond specified overlays."
                },
                product_slug=product_slug,
                angle_rank=angle_rank,
                variant_num=i,
                product_lock_rule=product_lock_rule,
                headline_text=headline_text
            )
        )
    return thumbs


def generate_three_for_angle(
    client,
    payload_min: str,
    angle: Dict[str, Any],
    product_slug: str,
    product_lock_rule: str,
    max_retries: int = 4
) -> List[Dict[str, Any]]:
    system_prompt = build_system_prompt()
    user_prompt = build_user_prompt(payload_min, angle, product_slug, product_lock_rule)

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    last_error: Optional[str] = None
    angle_rank = int(angle.get("rank") or 1)
    headline_text = headline_from_angle(angle)

    for attempt in range(1, max_retries + 1):
        try:
            payload = call_with_tool(client, messages)
            raw_thumbs = payload.get("thumbnails")

            # Normalizamos SIEMPRE antes de validar (esto elimina tus errores)
            thumbs = normalize_thumbnails_list(
                raw_thumbs,
                product_slug=product_slug,
                angle_rank=angle_rank,
                product_lock_rule=product_lock_rule,
                headline_text=headline_text
            )

            # Validación final (ya debería pasar)
            return validate_response_exact_three(thumbs)

        except Exception as e:
            last_error = str(e)
            print(f"DEBUG: Attempt {attempt} failed. Error: {last_error}")

            messages.append({
                "role": "user",
                "content": (
                    "FORMAT FIX. You MUST comply.\n"
                    "Use tool return_thumbnails.\n"
                    "Return EXACTLY 3 thumbnails.\n"
                    "Each thumbnail must include composition_rules and each overlay must include style.\n"
                    f"Error: {last_error}\n"
                )
            })

    # Si aún así falla, NO CRASHEA: devuelve templates válidos
    print(f"DEBUG: Fallback engaged for angle {angle_rank}. Last error: {last_error}")
    return fallback_templates(
        product_slug=product_slug,
        angle_rank=angle_rank,
        product_lock_rule=product_lock_rule,
        headline_text=headline_text
    )


# -----------------------------
# Saving helpers
# -----------------------------
def append_to_accum(accum_path: str, run_item: Dict[str, Any]) -> None:
    existing = safe_json_load(accum_path)

    if existing is None:
        payload = {"runs": [run_item]}
        safe_json_dump(accum_path, payload)
        return

    if isinstance(existing, list):
        existing.append(run_item)
        safe_json_dump(accum_path, existing)
        return

    if isinstance(existing, dict):
        if "runs" in existing and isinstance(existing["runs"], list):
            existing["runs"].append(run_item)
        else:
            existing["runs"] = [run_item]
        safe_json_dump(accum_path, existing)
        return

    safe_json_dump(accum_path, {"runs": [run_item]})


# -----------------------------
# Main
# -----------------------------
def main():
    if not os.path.exists(INPUT_JSON_PATH):
        raise FileNotFoundError(f"No existe el archivo: {INPUT_JSON_PATH}")

    market = load_market_research_min(INPUT_JSON_PATH)

    # 1) Payload compacto desde fix_format
    payload = extract_context_and_first_three_angles(
        market,
        max_hooks_per_angle=12,
        keep_evidence=True
    )

    # 2) Product slug + lock rule
    product_name = payload["context"]["input"].get("nombre_producto", "producto")
    product_slug = slugify(product_name)

    fp = payload["context"].get("product_fingerprint", {}) or {}
    fp_lock = (fp.get("product_description_lock") or "").strip()

    if PRODUCT_LOCK_RULE_OVERRIDE:
        product_lock_rule = PRODUCT_LOCK_RULE_OVERRIDE
    elif fp_lock:
        product_lock_rule = (
            "CRITICAL: Depict ONLY the product described here (do NOT change category/model): "
            + fp_lock
        )
    else:
        product_lock_rule = (
            "CRITICAL: Depict ONLY the exact product described in the provided data. "
            "Do NOT change the model/category. Do NOT add brand logos. Keep the product as the hero element."
        )

    payload_min = minify_json(payload)

    client = get_openai_client()

    all_angles_results: List[Dict[str, Any]] = []

    # 3) Generación por ángulo 1..3
    for angle in payload["angles"]:
        thumbs = generate_three_for_angle(
            client=client,
            payload_min=payload_min,
            angle=angle,
            product_slug=product_slug,
            product_lock_rule=product_lock_rule,
            max_retries=4
        )

        all_angles_results.append({
            "angle_rank": angle.get("rank"),
            "angle_name": angle.get("angulo", ""),
            "prompts": thumbs
        })

        print(f"✅ Angle {angle.get('rank')} listo: 3 prompts generados.")

    run_item = {
        "input_path": INPUT_JSON_PATH,
        "product_name": product_name,
        "product_slug": product_slug,
        "model": MODEL,
        "results": all_angles_results
    }

    per_product_path = os.path.join(OUTPUT_DIR, f"nanobanana_thumbnails_{product_slug}.json")
    safe_json_dump(per_product_path, run_item)
    append_to_accum(ACCUM_PATH, run_item)

    print(f"\n📦 Guardado producto: {per_product_path}")
    print(f"📦 Actualizado acumulado: {ACCUM_PATH}")

    return all_angles_results


if __name__ == "__main__":
    main()
