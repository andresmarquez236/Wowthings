# nanobanana_video_script_agent.py
# ------------------------------------------------------------
# Video Script Agent (High Conversion) with mandatory structure:
# - Hook 0–2s explicit
# - 3–3–3 cadence: every ~3s change (visual/text/rhythm)
# - Iterates top 3 angles from output/market_research_min.json
#
# INPUT:
#   output/market_research_min.json
#
# OUTPUT (upsert per angle_id):
#   output/video_script_accum.json
#
# CACHES (web_search once, reused):
#   output/hooks_pack.json
#   output/trends_pack.json
#   output/video_rules_pack.json
#
# REQUIREMENTS:
#   pip install openai python-dotenv
#
# ENV:
#   export OPENAI_API_KEY="..."
# Optional:
#   export VIDEO_AGENT_MODEL="gpt-5-mini"   # or "gpt-5"
#   export MARKET_RESEARCH_PATH="output/market_research_min.json"
#   export OUTPUT_DIR="output"
#
# RUN:
#   python nanobanana_video_script_agent.py --all
#   python nanobanana_video_script_agent.py --angle-index 0
#   python nanobanana_video_script_agent.py --angle-index 1
#   python nanobanana_video_script_agent.py --angle-index 2
#
# Refresh caches:
#   python nanobanana_video_script_agent.py --all --refresh-hooks --refresh-trends --refresh-video-rules
# ------------------------------------------------------------

import os
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Callable, Optional

from dotenv import load_dotenv
from openai import OpenAI

# ---- compat error type
try:
    from openai import BadRequestError
except Exception:
    BadRequestError = Exception

load_dotenv()

MODEL = os.getenv("VIDEO_AGENT_MODEL", "gpt-4o")
MARKET_PATH = os.getenv("MARKET_RESEARCH_PATH", "output/market_research_min.json")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

HOOKS_CACHE_PATH = os.path.join(OUTPUT_DIR, "hooks_pack.json")
TRENDS_CACHE_PATH = os.path.join(OUTPUT_DIR, "trends_pack.json")
VIDEO_RULES_CACHE_PATH = os.path.join(OUTPUT_DIR, "video_rules_pack.json")
VIDEO_RULES_CACHE_PATH = os.path.join(OUTPUT_DIR, "video_rules_pack.json")
# ACCUM_PATH will be determined dynamically in main()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ============================================================
# Utils
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
    # Standard ChatCompletion response
    if hasattr(resp, "choices") and resp.choices:
        return resp.choices[0].message.content
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
        return s[start:end + 1].strip()
    return s

def parse_json_or_dump(raw: str, dump_path: str) -> Dict[str, Any]:
    raw2 = _extract_json_object((raw or "").strip())
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
        msg = str(e)
        if "temperature" in msg:
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
            time.sleep(0.45 * attempt)
    raise last_err  # type: ignore

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
# Market Research extractors
# ============================================================

def extract_product(market: Dict[str, Any]) -> Dict[str, str]:
    producto = market.get("input", {}) or ((market.get("meta", {}) or {}).get("producto_input", {}) or {})
    nombre = str(producto.get("nombre_producto", market.get("product_name", "Producto"))).strip() or "Producto"
    if nombre.lower().startswith("ejemplo:"):
        nombre = nombre[8:].strip()
    desc = str(producto.get("descripcion", market.get("product_description", ""))).strip()
    gar = str(producto.get("garantia", market.get("warranty", ""))).strip()
    precio = str(producto.get("precio", market.get("price", ""))).strip()
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
                return hooks[:12]
        except Exception:
            continue
    return []

# ============================================================
# JSON Schemas (STRICT) - additionalProperties:false everywhere
# ============================================================

PACK_SOURCES_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["url", "title", "published_date", "used_for"],
    "properties": {
        "url": {"type": "string"},
        "title": {"type": "string"},
        "published_date": {"type": "string"},
        "used_for": {"type": "string"},
    },
}

HOOKS_PACK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sources", "hook_archetypes", "high_performing_patterns", "latin_es_templates"],
    "properties": {
        "sources": {"type": "array", "minItems": 2, "maxItems": 8, "items": PACK_SOURCES_SCHEMA},
        "hook_archetypes": {"type": "array", "minItems": 12, "maxItems": 24, "items": {"type": "string"}},
        "high_performing_patterns": {"type": "array", "minItems": 8, "maxItems": 18, "items": {"type": "string"}},
        "latin_es_templates": {"type": "array", "minItems": 15, "maxItems": 40, "items": {"type": "string"}},
    },
}

TRENDS_PACK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["trend_sources", "creative_patterns", "cadence_rules", "do_not_do"],
    "properties": {
        "trend_sources": {"type": "array", "minItems": 2, "maxItems": 8, "items": PACK_SOURCES_SCHEMA},
        "creative_patterns": {"type": "array", "minItems": 10, "maxItems": 18, "items": {"type": "string"}},
        "cadence_rules": {"type": "array", "minItems": 6, "maxItems": 12, "items": {"type": "string"}},
        "do_not_do": {"type": "array", "minItems": 6, "maxItems": 14, "items": {"type": "string"}},
    },
}

VIDEO_RULES_PACK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sources", "rules_summary", "three_three_three_definition", "hook_0_2_rule", "pacing_editing_rules"],
    "properties": {
        "sources": {"type": "array", "minItems": 2, "maxItems": 8, "items": PACK_SOURCES_SCHEMA},
        "rules_summary": {"type": "array", "minItems": 6, "maxItems": 12, "items": {"type": "string"}},
        "three_three_three_definition": {"type": "array", "minItems": 5, "maxItems": 10, "items": {"type": "string"}},
        "hook_0_2_rule": {"type": "array", "minItems": 4, "maxItems": 8, "items": {"type": "string"}},
        "pacing_editing_rules": {"type": "array", "minItems": 8, "maxItems": 14, "items": {"type": "string"}},
    },
}

VIDEO_SCRIPT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["angle_id", "angle_name", "buyer_persona", "ad_copy", "video", "hooks_used", "compliance_notes"],
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
                "description": {"type": "string"},
            },
        },
        "video": {
            "type": "object",
            "additionalProperties": False,
            "required": ["duration_seconds", "format", "structure_rules", "beats", "production_notes"],
            "properties": {
                "duration_seconds": {"type": "integer"},
                "format": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["aspect_ratio", "resolution"],
                    "properties": {
                        "aspect_ratio": {"type": "string"},
                        "resolution": {"type": "string"},
                    },
                },
                "structure_rules": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["hook_window", "cadence_rule", "change_definition"],
                    "properties": {
                        "hook_window": {"type": "string"},
                        "cadence_rule": {"type": "string"},
                        "change_definition": {"type": "string"},
                    },
                },
                "beats": {
                    "type": "array",
                    "minItems": 7,
                    "maxItems": 7,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "beat_id", "t_start", "t_end",
                            "on_screen_text", "visual", "voiceover",
                            "rhythm_editing", "sfx_music", "transition", "cta"
                        ],
                        "properties": {
                            "beat_id": {"type": "string"},
                            "t_start": {"type": "integer"},
                            "t_end": {"type": "integer"},
                            "on_screen_text": {"type": "string"},
                            "visual": {"type": "string"},
                            "voiceover": {"type": "string"},
                            "rhythm_editing": {"type": "string"},
                            "sfx_music": {"type": "string"},
                            "transition": {"type": "string"},
                            "cta": {"type": "string"},
                        },
                    },
                },
                "production_notes": {"type": "array", "minItems": 6, "maxItems": 14, "items": {"type": "string"}},
            },
        },
        "hooks_used": {"type": "array", "minItems": 4, "maxItems": 10, "items": {"type": "string"}},
        "compliance_notes": {"type": "array", "minItems": 4, "maxItems": 12, "items": {"type": "string"}},
    },
}

# ============================================================
# Research pack prompts (web_search)
# ============================================================

HOOKS_SYSTEM = (
    "You are a performance copy researcher specialized in hooks for paid social (Meta/IG/FB/Reels).\n"
    "Use web_search. Return ONLY JSON matching schema. No markdown.\n"
    "Hard rules: do not fabricate facts; produce practical patterns and Spanish LATAM templates."
)

HOOKS_USER = (
    "Research RECENT hook archetypes and patterns for ecommerce paid social (Meta/IG/FB/Reels), ideally 2024-2025.\n"
    "Return:\n"
    "- sources (2-8)\n"
    "- hook_archetypes (12-24)\n"
    "- high_performing_patterns (8-18)\n"
    "- latin_es_templates (15-40) short Spanish LATAM templates, no product-specific unverified claims.\n"
)

TRENDS_SYSTEM = (
    "You are a Creative Strategy Researcher for short-form performance video (Meta/TikTok style).\n"
    "Use web_search. Return ONLY JSON matching schema. No markdown.\n"
    "Rules: do not fabricate. Provide mobile-first, feed-native patterns."
)

TRENDS_USER = (
    "Collect CURRENT patterns for high-converting short-form performance videos.\n"
    "Return:\n"
    "- trend_sources (2-8)\n"
    "- creative_patterns (10-18)\n"
    "- cadence_rules (6-12) about pacing, scene changes, overlays\n"
    "- do_not_do (6-14)\n"
)

VIDEO_RULES_SYSTEM = (
    "You are an expert in performance video editing & attention psychology.\n"
    "Use web_search. Return ONLY JSON matching schema. No markdown.\n"
    "Do NOT pretend the '3-3-3' rule is official unless a source explicitly calls it that.\n"
    "Your job is to: (a) cite what platforms say about the first seconds + pacing, (b) define a practical 3-3-3 heuristic."
)

VIDEO_RULES_USER = (
    "Research what '3-3-3' style rules mean in short-form ads and what platforms recommend about:\n"
    "- first 2-3 seconds (hook)\n"
    "- introducing proposition early\n"
    "- using text overlays/captions\n"
    "- fast pacing / quick cuts\n"
    "Return:\n"
    "- sources (2-8)\n"
    "- rules_summary (6-12) evidence-informed rules\n"
    "- three_three_three_definition (5-10) a practical definition: changes every ~3 seconds across visual/text/rhythm\n"
    "- hook_0_2_rule (4-8) explicit guidance for 0–2s\n"
    "- pacing_editing_rules (8-14)\n"
)

# ============================================================
# Pack builders
# ============================================================

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
            max_completion_tokens=1700,
        )
    return call_with_retries(_create, "debug_hooks", retries=2)

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
                    "schema": TRENDS_PACK_SCHEMA,
                    "strict": True
                }
            },
            max_completion_tokens=1500,
        )
    return call_with_retries(_create, "debug_trends", retries=2)

def fetch_video_rules_pack() -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": VIDEO_RULES_SYSTEM},
                {"role": "user", "content": VIDEO_RULES_USER},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "video_rules_pack",
                    "schema": VIDEO_RULES_PACK_SCHEMA,
                    "strict": True
                }
            },
            max_completion_tokens=1600,
        )
    return call_with_retries(_create, "debug_video_rules", retries=2)

# ============================================================
# SUPER PROMPT for script generation
# ============================================================

VIDEO_SCRIPT_SYSTEM_SUPER = r"""
ROLE
You are a PhD-level Direct Response Creative Director + Attention Psychology expert for short-form ads (Reels/FB/IG).

GOAL
Given product + ONE angle + hooks packs + video_rules pack:
Generate a HIGH-CONVERTING video script for that angle with mandatory structure:
- Total duration: EXACTLY 20 seconds.
- Beat 1: 0–2s (explicit HOOK).
- Beats 2–7: six blocks of 3 seconds each (2–5, 5–8, 8–11, 11–14, 14–17, 17–20).
- 3–3–3 cadence: every 3 seconds (each beat) MUST change at least 2 of:
  (a) Visual subject/composition,
  (b) On-screen text message (new info),
  (c) Rhythm/tempo/editing (e.g., jump cut, whip, punch-in, speed ramp, B-roll insert).
- Hook must be explicit: call out WHO or the pain/problem + promise, in 0–2s.

NON-NEGOTIABLE OUTPUT
Return ONLY valid JSON matching the provided schema. No markdown, no extra text.

COPY OUTPUTS
Provide:
- ad_copy.title (short, scroll-stopping)
- ad_copy.primary_text (feed text, short, conversion-oriented)
- ad_copy.description (supporting line)

SCRIPT QUALITY
- Use the angle promise + objection to drive the narrative.
- Use DR flow across beats: Hook → Problem → Mechanism/Demo → Proof (non-numeric unless provided) → Offer/Guarantee mention (only if provided) → CTA.
- NO invented specs, numbers, certifications, or claims not present in input.
- If warranty/price is unknown or unclear, do NOT state it as fact. Use safe language like "según disponibilidad".

HOOK INTEGRATION
- Select 4–10 hooks_used based on:
  (1) market hooks hints for that angle
  (2) hooks_pack templates & patterns
- hooks_used must be applicable and not overclaim.

PRODUCTION NOTES
Add 6–14 concise notes: framing, lighting, captions for sound-off, speed, prop usage, hands/UGC style, etc.
""".strip()

# ============================================================
# Payload builder & model call
# ============================================================

def build_video_payload(
    product: Dict[str, str],
    angle: Dict[str, str],
    hooks_hints: List[str],
    hooks_pack: Dict[str, Any],
    trends_pack: Dict[str, Any],
    video_rules_pack: Dict[str, Any],
) -> Dict[str, Any]:
    # Keep packs compact to avoid token blowups
    hooks_mini = {
        "hook_archetypes": (hooks_pack.get("hook_archetypes", []) or [])[:16],
        "high_performing_patterns": (hooks_pack.get("high_performing_patterns", []) or [])[:12],
        "latin_es_templates": (hooks_pack.get("latin_es_templates", []) or [])[:26],
    }
    trends_mini = {
        "creative_patterns": (trends_pack.get("creative_patterns", []) or [])[:12],
        "cadence_rules": (trends_pack.get("cadence_rules", []) or [])[:10],
        "do_not_do": (trends_pack.get("do_not_do", []) or [])[:10],
    }
    video_rules_mini = {
        "rules_summary": (video_rules_pack.get("rules_summary", []) or [])[:10],
        "three_three_three_definition": (video_rules_pack.get("three_three_three_definition", []) or [])[:8],
        "hook_0_2_rule": (video_rules_pack.get("hook_0_2_rule", []) or [])[:6],
        "pacing_editing_rules": (video_rules_pack.get("pacing_editing_rules", []) or [])[:10],
        "sources": (video_rules_pack.get("sources", []) or [])[:6],
    }

    return {
        "product": product,
        "angle": angle,
        "hooks_hints_from_market": (hooks_hints or [])[:12],
        "packs": {
            "hooks_pack": hooks_mini,
            "trends_pack": trends_mini,
            "video_rules_pack": video_rules_mini,
        },
        "constraints": {
            "duration_seconds": 20,
            "format": {"aspect_ratio": "9:16", "resolution": "1080x1920"},
            "beats": [
                {"beat_id": "B1", "t_start": 0, "t_end": 2},
                {"beat_id": "B2", "t_start": 2, "t_end": 5},
                {"beat_id": "B3", "t_start": 5, "t_end": 8},
                {"beat_id": "B4", "t_start": 8, "t_end": 11},
                {"beat_id": "B5", "t_start": 11, "t_end": 14},
                {"beat_id": "B6", "t_start": 14, "t_end": 17},
                {"beat_id": "B7", "t_start": 17, "t_end": 20},
            ],
        },
    }

def call_video_script_agent(payload: Dict[str, Any], angle_id: str) -> Dict[str, Any]:
    def _create():
        return safe_responses_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": VIDEO_SCRIPT_SYSTEM_SUPER},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": f"video_script_{angle_id}",
                    "schema": VIDEO_SCRIPT_SCHEMA,
                    "strict": True
                }
            },
            max_completion_tokens=3200,
        )
    return call_with_retries(_create, f"debug_video_{angle_id}", retries=2)

# ============================================================
# Accumulator (upsert by angle_id)
# ============================================================

def upsert_accum(accum_path: str, angle_result: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    accum: Dict[str, Any] = {"meta": meta, "results": []}
    if os.path.exists(accum_path):
        try:
            accum = load_json(accum_path)
            if not isinstance(accum.get("results"), list):
                accum["results"] = []
        except Exception:
            pass

    accum["meta"] = meta

    angle_id = str(angle_result.get("angle_id", "")).strip()
    if not angle_id:
        raise ValueError("angle_result no trae angle_id.")

    replaced = False
    new_results = []
    for r in accum.get("results", []):
        if isinstance(r, dict) and str(r.get("angle_id", "")).strip() == angle_id:
            new_results.append(angle_result)
            replaced = True
        else:
            new_results.append(r)
    if not replaced:
        new_results.append(angle_result)

    accum["results"] = new_results
    save_json(accum_path, accum)
    return accum

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--angle-index", type=int, default=0, help="0-based index en top_5_angulos")
    parser.add_argument("--all", action="store_true", help="Genera los primeros 3 ángulos en una sola ejecución")
    parser.add_argument("--refresh-hooks", action="store_true")
    parser.add_argument("--refresh-trends", action="store_true")
    parser.add_argument("--refresh-video-rules", action="store_true")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en env.")

    market = load_json(MARKET_PATH)
    product = extract_product(market)
    angles_raw = extract_angles(market)

    # Dynamic filename
    product_name = product.get("nombre_producto", "producto").strip()
    safe_name = "".join([c if c.isalnum() else "_" for c in product_name]).lower()
    accum_path = os.path.join(OUTPUT_DIR, f"video_script_{safe_name}.json")

    hooks_pack = load_or_create_cache(HOOKS_CACHE_PATH, fetch_hooks_pack, args.refresh_hooks)
    trends_pack = load_or_create_cache(TRENDS_CACHE_PATH, fetch_trends_pack, args.refresh_trends)
    video_rules_pack = load_or_create_cache(VIDEO_RULES_CACHE_PATH, fetch_video_rules_pack, args.refresh_video_rules)

    indices: List[int]
    if args.all:
        indices = [0, 1, 2]
    else:
        indices = [args.angle_index]

    for idx in indices:
        if idx < 0 or idx >= len(angles_raw):
            raise ValueError(f"--angle-index fuera de rango. Recibido {idx}, pero top_5_angulos tiene {len(angles_raw)} elementos.")

        angle_norm = normalize_angle(angles_raw[idx], fallback_rank=idx + 1)
        hooks_hints = extract_hooks_for_rank(market, rank=int(angle_norm["rank"]))

        payload = build_video_payload(
            product=product,
            angle=angle_norm,
            hooks_hints=hooks_hints,
            hooks_pack=hooks_pack,
            trends_pack=trends_pack,
            video_rules_pack=video_rules_pack,
        )

        angle_result = call_video_script_agent(payload, angle_id=angle_norm["angle_id"])

        meta = {
            "generated_at_utc": now_utc_iso(),
            "model": MODEL,
            "market_path": MARKET_PATH,
            "caches": {
                "hooks_pack": HOOKS_CACHE_PATH,
                "trends_pack": TRENDS_CACHE_PATH,
                "video_rules_pack": VIDEO_RULES_CACHE_PATH,
            },
            "product": product,
        }

        upsert_accum(accum_path, angle_result, meta)

        print(f"✅ OK: {angle_norm['angle_id']} actualizado.")
        print(f"📌 Caches: {HOOKS_CACHE_PATH} | {TRENDS_CACHE_PATH} | {VIDEO_RULES_CACHE_PATH}")
        print(f"📄 Output acumulado: {accum_path}")

if __name__ == "__main__":
    main()
