# market_research_pipeline.py
# Requiere: pip install openai
# Exporta: OPENAI_API_KEY=...

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


# =========================
# Config & Utils
# =========================

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HOOK_TRENDS_CACHE_PATH = OUTPUT_DIR / "hook_trends_cache.json"
HOOK_TRENDS_CACHE_TTL_DAYS = 7  # cambia a 1 si quieres "tendencias ultra frescas"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def safe_json_loads(text: str) -> Any:
    """
    Intenta parsear JSON directo. Si el modelo mete basura, intenta recortar al primer {...}.
    (Con Structured Outputs bien configurado casi nunca se necesita.)
    """
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def extract_url_citations(response_obj: Any) -> List[Dict[str, str]]:
    """
    Extrae URLs citadas desde annotations del mensaje cuando se usa web_search.
    (Útil como respaldo / auditoría.)
    """
    citations: List[Dict[str, str]] = []
    try:
        for item in getattr(response_obj, "output", []) or []:
            if getattr(item, "type", None) != "message":
                continue
            for part in getattr(item, "content", []) or []:
                ann = getattr(part, "annotations", None) or []
                for a in ann:
                    if getattr(a, "type", None) == "url_citation":
                        citations.append(
                            {
                                "url": getattr(a, "url", ""),
                                "title": getattr(a, "title", ""),
                            }
                        )
    except Exception:
        pass
    # dedupe
    seen = set()
    uniq = []
    for c in citations:
        key = (c.get("url", ""), c.get("title", ""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)
    return uniq


# =========================
# JSON Schemas (Structured Outputs)
# =========================

HOOK_TRENDS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["meta", "hook_archetypes"],
    "properties": {
        "meta": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_date_utc", "focus_markets"],
            "properties": {
                "run_date_utc": {"type": "string"},
                "focus_markets": {"type": "array", "items": {"type": "string"}},
            },
        },
        "hook_archetypes": {
            "type": "array",
            "minItems": 6,
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "pattern", "cuando_usar", "riesgos", "fuentes", "confianza"],
                "properties": {
                    "id": {"type": "string"},
                    "pattern": {"type": "string"},
                    "cuando_usar": {"type": "string"},
                    "riesgos": {"type": "string"},
                    "fuentes": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 3,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["url", "nota"],
                            "properties": {
                                "url": {"type": "string"},
                                "nota": {"type": "string"},
                            },
                        },
                    },
                    "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                },
            },
        },
    },
}


MARKET_AGENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["meta", "resumen", "product_fingerprint", "checklist", "top_5_angulos", "hooks_por_angulo", "estacionalidad"],
    "properties": {
        "meta": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_date_utc", "pais_prioritario", "producto_input"],
            "properties": {
                "run_date_utc": {"type": "string"},
                "pais_prioritario": {"type": "string"},
                "producto_input": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["nombre_producto", "descripcion", "garantia", "precio"],
                    "properties": {
                        "nombre_producto": {"type": "string"},
                        "descripcion": {"type": "string"},
                        "garantia": {"type": "string"},
                        "precio": {"type": "string"},
                    },
                },
            },
        },
        "resumen": {"type": "string"},
        "product_fingerprint": {
            "type": "object",
            "additionalProperties": False,
            "required": ["marca_modelo", "categoria", "specs_clave", "claims", "rango_precio", "garantia_tipo"],
            "properties": {
                "marca_modelo": {"type": "string"},
                "categoria": {"type": "string"},
                "specs_clave": {"type": "array", "minItems": 3, "maxItems": 10, "items": {"type": "string"}},
                "claims": {"type": "array", "minItems": 2, "maxItems": 10, "items": {"type": "string"}},
                "rango_precio": {"type": "string"},
                "garantia_tipo": {"type": "string"},
            },
        },
        "checklist": {
            "type": "object",
            "additionalProperties": False,
            "required": ["criterios", "resumen_scoring"],
            "properties": {
                "criterios": {
                    "type": "array",
                    "minItems": 15,
                    "maxItems": 15,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["id", "criterio", "cumple", "evidencia_corta", "confianza"],
                        "properties": {
                            "id": {"type": "string"},
                            "criterio": {"type": "string"},
                            "cumple": {"type": "string", "enum": ["SI", "NO", "NO_CONFIRMADO", "N/A"]},
                            "evidencia_corta": {"type": "string"},
                            "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                        },
                    },
                },
                "resumen_scoring": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["si", "no", "no_confirmado", "criterios_contabilizados", "umbral_si", "pasa_umbral"],
                    "properties": {
                        "si": {"type": "integer"},
                        "no": {"type": "integer"},
                        "no_confirmado": {"type": "integer"},
                        "criterios_contabilizados": {"type": "integer"},
                        "umbral_si": {"type": "integer"},
                        "pasa_umbral": {"type": "boolean"},
                    },
                },
            },
        },
        "top_5_angulos": {
            "type": "array",
            "minItems": 5,
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["rank", "angulo", "score_0_10", "buyer_persona", "justificacion_evidencia", "confianza"],
                "properties": {
                    "rank": {"type": "integer", "minimum": 1, "maximum": 5},
                    "angulo": {"type": "string"},
                    "score_0_10": {"type": "number", "minimum": 0, "maximum": 10},
                    "buyer_persona": {"type": "string"},
                    "justificacion_evidencia": {"type": "string"},
                    "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                },
            },
        },
        "hooks_por_angulo": {
            "type": "array",
            "minItems": 5,
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["rank", "hooks"],
                "properties": {
                    "rank": {"type": "integer", "minimum": 1, "maximum": 5},
                    "hooks": {"type": "array", "minItems": 6, "maxItems": 8, "items": {"type": "string"}},
                },
            },
        },
        "estacionalidad": {
            "type": "object",
            "additionalProperties": False,
            "required": ["tipo", "hechos", "hipotesis", "correlacion_angulos"],
            "properties": {
                "tipo": {"type": "string", "enum": ["evergreen", "estacional", "mixto", "no_confirmado"]},
                "hechos": {"type": "array", "maxItems": 6, "items": {"type": "string"}},
                "hipotesis": {"type": "array", "maxItems": 6, "items": {"type": "string"}},
                "correlacion_angulos": {
                    "type": "array",
                    "minItems": 5,
                    "maxItems": 5,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["rank", "correlacion", "nota"],
                        "properties": {
                            "rank": {"type": "integer", "minimum": 1, "maximum": 5},
                            "correlacion": {"type": "string", "enum": ["Alta", "Media", "Baja", "No aplica"]},
                            "nota": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
}


CHECKLIST_CRITERIA_15 = [
    ("C01", "Soluciona un problema?"),
    ("C02", "Se entiende su funcionalidad rapidamente?"),
    ("C03", "Se le puede vender a una audiencia masiva?"),
    ("C04", "Algun amigo o familiar estaria dispuesto a comprarlo?"),
    ("C05", "Tiene un buen margen?"),
    ("C06", "Es de dificil de encontar en tiendas convencionales (es de dificil acceso)?"),
    ("C07", "Es unico e impresionante (WOW factor)? Factor wow novedosos nuevo"),
    ("C08", "Esta en tendencia en otros paises o en otros mercados?"),
    ("C09", "Existen pocos competidores vendiendolo?"),
    ("C10", "Tiene antecedente de buenas ventas en otros paises otros mercados?"),
    ("C11", "Algun amigo o familiar estaria dispuesto a comprarlo? (segunda validacion)"),
    ("C12", "Tu comprarias el producto?"),
    ("C13", "Tiene multimedia en internet a la que puedas acceder facilmente?"),
    ("C14", "Es un producto evergreen?"),
    ("C15", "Total? (N/A: se calcula aparte)"),
]


# =========================
# Prompts (PhD-level, deliverables-first)
# =========================

HOOK_TRENDS_SYSTEM = """
Eres “Hook Trend Researcher (Colombia + LATAM) para anuncios de e-commerce”.
Tu objetivo NO es enseñar teoría: es entregar un set de “arquetipos de hook” que estén siendo usados / recomendados
en el mercado AHORA MISMO, basados en evidencia web.

REGLAS
- Usa web_search obligatoriamente (mínimo 6 consultas).
- Prioriza fuentes recientes (ideal últimos 6–12 meses). Si no hay fecha, baja confianza.
- No inventes estadísticas.
- Entrega SOLO JSON según el schema.
- Cada arquetipo debe incluir 1–3 fuentes (URL + nota de por qué soporta el patrón).
"""

def build_hook_trends_user() -> str:
    return f"""
Necesito 6–8 arquetipos de hooks en tendencia para anuncios (Meta/TikTok/UGC) aplicables a e-commerce en español.

FOCO GEOGRÁFICO:
- Colombia primero
- luego LATAM

ENTREGABLE:
- hook_archetypes: id, pattern (plantilla corta), cuando_usar, riesgos (compliance/claims), fuentes, confianza.

HOY (UTC): {now_iso()}
"""


MARKET_AGENT_SYSTEM = """
Eres “Analista de Mercado y Copy Strategist para Colombia y LATAM”, experto en investigación con fuentes web verificables.

OBJETIVO:
Producir un output OPERATIVO (para agentes), en JSON, con:
- Top 5 ángulos rankeados (0–10) + buyer persona
- Hooks por ángulo (usando arquetipos de tendencia provistos)
- Estacionalidad (hechos vs hipótesis)
- Checklist MUY DURO (si no hay evidencia => NO o NO_CONFIRMADO)

OBLIGATORIO:
- Usa web_search (CO primero, luego LATAM).
- “Mismo producto”: valida huella (fingerprint). Si una fuente NO coincide, no la uses para claims clave.
- No alucines: si no confirmas => “NO_CONFIRMADO”.
- Output: SOLO JSON, sin explicaciones.

CHECKLIST (DUREZA EXTREMA):
- Empieza sesgado a “NO”.
- Solo “SI” si hay evidencia clara o razonamiento directo desde datos confirmados (precio, disponibilidad, competencia, etc).
- Si falta data para decidir => “NO_CONFIRMADO” (y confianza Baja o Media).
"""

def build_market_user(
    nombre_producto: str,
    descripcion: str,
    garantia: str,
    precio: str,
    hook_archetypes: List[Dict[str, Any]],
) -> str:
    criteria_lines = "\n".join([f"- {cid}: {c}" for cid, c in CHECKLIST_CRITERIA_15])

    return f"""
ENTRADAS:
- nombre_producto: {nombre_producto}
- descripcion: {descripcion}
- garantia: {garantia}
- precio: {precio}

ARQUETIPOS DE HOOK EN TENDENCIA (para combinar con los ángulos; NO repitas genérico):
{json.dumps(hook_archetypes, ensure_ascii=False, indent=2)}

CHECKLIST A LLENAR (15 filas exactas):
{criteria_lines}

REGLAS DE SALIDA:
- top_5_angulos: rank 1–5, score 0–10 (duro), buyer_persona, justificacion_evidencia (corta, con referencia a evidencia).
- hooks_por_angulo: 6–8 hooks por ángulo, específicos al producto + al ángulo + inspirados en arquetipos.
- estacionalidad: tipo + hechos + hipótesis + correlación por ángulo.
- checklist: 15 criterios; C15 siempre N/A (porque el total se calcula aparte).
- NO uses texto fuera del JSON.
"""


# =========================
# Agent Calls
# =========================

@dataclass
class AgentConfig:
    model: str = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    reasoning_effort: str = os.getenv("OPENAI_REASONING_EFFORT", "medium")  # none|medium|high...
    max_output_tokens: int = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "4500"))
    store: bool = False


def call_structured(
    client: OpenAI,
    cfg: AgentConfig,
    system_prompt: str,
    user_prompt: str,
    schema_name: str,
    schema: Dict[str, Any],
    max_tool_calls: int = 12,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    resp = client.responses.create(
        model=cfg.model,
        tools=[{"type": "web_search"}],
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": schema,
            }
        },
        reasoning={"effort": cfg.reasoning_effort},
        max_output_tokens=cfg.max_output_tokens,
        max_tool_calls=max_tool_calls,
        store=cfg.store,
    )

    data = safe_json_loads(resp.output_text)
    citations = extract_url_citations(resp)
    return data, citations


def load_hook_trends_cache() -> Optional[Dict[str, Any]]:
    if not HOOK_TRENDS_CACHE_PATH.exists():
        return None
    try:
        cached = json.loads(HOOK_TRENDS_CACHE_PATH.read_text(encoding="utf-8"))
        run_date = cached.get("meta", {}).get("run_date_utc")
        if not run_date:
            return None
        dt = datetime.fromisoformat(run_date.replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - dt <= timedelta(days=HOOK_TRENDS_CACHE_TTL_DAYS):
            return cached
        return None
    except Exception:
        return None


def get_hook_trends(client: OpenAI, cfg: AgentConfig, force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        cached = load_hook_trends_cache()
        if cached:
            return cached

    data, citations = call_structured(
        client=client,
        cfg=cfg,
        system_prompt=HOOK_TRENDS_SYSTEM,
        user_prompt=build_hook_trends_user(),
        schema_name="HookTrendsCO_LATAM",
        schema=HOOK_TRENDS_SCHEMA,
        max_tool_calls=10,
    )
    # opcional: guarda citas técnicas extraídas
    data["_debug_citations"] = citations[:20]
    save_json(HOOK_TRENDS_CACHE_PATH, data)
    return data


def hard_score_checklist(out: Dict[str, Any], umbral_si: int = 9) -> Dict[str, Any]:
    """
    Recalcula el scoring para evitar inconsistencias.
    Cuenta SOLO criterios con cumple en SI/NO/NO_CONFIRMADO (C15 es N/A).
    """
    criterios = out["checklist"]["criterios"]

    contabilizados = 0
    si = 0
    no = 0
    no_conf = 0

    for row in criterios:
        if row["id"] == "C15":
            # Debe ser N/A
            row["cumple"] = "N/A"
            row["confianza"] = "Alta"
            row["evidencia_corta"] = "Auto: el total se calcula en resumen_scoring."
            continue

        contabilizados += 1
        if row["cumple"] == "SI":
            si += 1
        elif row["cumple"] == "NO":
            no += 1
        else:
            no_conf += 1

    out["checklist"]["resumen_scoring"] = {
        "si": si,
        "no": no,
        "no_confirmado": no_conf,
        "criterios_contabilizados": contabilizados,
        "umbral_si": umbral_si,
        "pasa_umbral": (si >= umbral_si),
    }
    return out


def run_market_research(
    nombre_producto: str,
    descripcion: str,
    garantia: str,
    precio: str,
    model_market: str = "gpt-5-mini",
    force_refresh_trends: bool = False,
) -> Dict[str, Any]:
    client = OpenAI()
    cfg_trends = AgentConfig(model="gpt-5-mini", reasoning_effort="medium", max_output_tokens=8000, store=False)
    cfg_market = AgentConfig(model=model_market, reasoning_effort="medium", max_output_tokens=4500, store=False)

    # A) Hook trends (cacheable)
    trends = get_hook_trends(client, cfg_trends, force_refresh=force_refresh_trends)
    hook_archetypes = trends.get("hook_archetypes", [])

    # B) Market/angles per product
    user_prompt = build_market_user(
        nombre_producto=nombre_producto,
        descripcion=descripcion,
        garantia=garantia,
        precio=precio,
        hook_archetypes=hook_archetypes,
    )

    data, citations = call_structured(
        client=client,
        cfg=cfg_market,
        system_prompt=MARKET_AGENT_SYSTEM,
        user_prompt=user_prompt,
        schema_name="MarketAnglesCO_LATAM_Min",
        schema=MARKET_AGENT_SCHEMA,
        max_tool_calls=12,
    )

    # Añade metadatos técnicos
    data["meta"]["run_date_utc"] = now_iso()
    data["_debug_citations"] = citations[:30]

    # Endurece / normaliza scoring
    data = hard_score_checklist(data, umbral_si=9)

    # Guarda output
    out_path = OUTPUT_DIR / "market_research_min.json"
    save_json(out_path, data)

    return data


# =========================
# Example CLI usage
# =========================
if __name__ == "__main__":
    result = run_market_research(
        nombre_producto="(pega aquí)",
        descripcion="(pega aquí)",
        garantia="(pega aquí)",
        precio="(pega aquí)",
        model_market=os.getenv("OPENAI_MODEL_MARKET", "gpt-5.2"),
        force_refresh_trends=False,
    )
    print("✅ Listo. Guardado en: output/market_research_min.json")
