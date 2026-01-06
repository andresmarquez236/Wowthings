import os
import json
from datetime import datetime
import argparse
import sys
from typing import Any, Dict, Literal
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# =========================
# SYSTEM PROMPT (entregables-first + calificación dura)
# =========================
SYSTEM_PROMPT = """
Eres “Analista de Mercado y Copy Strategist para Colombia y LATAM”, experto en investigación con fuentes web verificables.

OBJETIVO ÚNICO:
Entregar SOLO un JSON final (estricto) con:
1) top_5_angulos (rankeados, score 0-10, buyer_persona, justificación con evidencia)
2) hooks_por_angulo (hooks en tendencia y optimizados para el producto y ese ángulo)
3) estacionalidad (hechos vs hipótesis + correlación con ángulos)
4) checklist (15 criterios SI/NO) con calificación MUY DURA y total (apunta a >=9/15)

HERRAMIENTAS (OBLIGATORIO):
- Usa web_search para investigar.
- Si el modelo soporta open_page, úsalo para verificar datos críticos (precio, specs, garantía, claims).
- No inventes: si no hay evidencia -> marca NO y en 'nota' escribe “No confirmado” y qué faltó buscar.

REGLA DE CALIFICACIÓN DURA (CRÍTICA):
- Por defecto, califica “NO” salvo que exista evidencia clara y relevante.
- Si no puedes confirmar un criterio, DEBES:
  - cumple = "NO"
  - confianza = "Baja"
  - nota = "No confirmado (falta evidencia)" + next_search

GATING “MISMO PRODUCTO”:
Antes de usar una fuente, verifica que corresponde al MISMO producto/variante.
Crea Product Fingerprint (marca, modelo, categoría, 5-10 specs/claims, rango precio, garantía).
Solo aceptes una fuente si coincide con >=3 elementos del fingerprint
(o 2 + evidencia fuerte: SKU/modelo exacto).

GEOGRAFÍA:
Prioriza Colombia (COP, sitios .co, envío/garantía en Colombia).
Luego LATAM como proxy (MX/CL/PE/AR/EC). Marca proxies como proxy.

EVIDENCIA:
Cada ángulo (y criterios relevantes) debe incluir:
- urls (1-3)
- por qué aplica al mismo producto (match_note)
- confianza Alta/Media/Baja

IMPORTANTE:
Devuelve SOLO el JSON. Nada de texto adicional.
Idioma: español neutro (con opción “colombianización ligera” en hooks).
""".strip()


# =========================
# JSON SCHEMA REDUCIDO
# =========================
MARKET_MIN_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "meta": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "run_date_iso": {"type": "string"},
                "model": {"type": "string"},
                "locale_priority": {"type": "string", "enum": ["CO_first_then_LATAM"]},
            },
            "required": ["run_date_iso", "model", "locale_priority"],
        },
        "input": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "nombre_producto": {"type": "string"},
                "descripcion": {"type": "string"},
                "garantia": {"type": "string"},
                "precio": {"type": "string"},
            },
            "required": ["nombre_producto", "descripcion", "garantia", "precio"],
        },
        "resumen_corto": {"type": "string"},

        "product_fingerprint": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "marca": {"type": "string"},
                "modelo": {"type": "string"},
                "categoria": {"type": "string"},
                "specs_claims_clave": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
                "rango_precio_observado": {"type": "string"},
                "garantia_tipo": {"type": "string"},
                "variantes_detectadas": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
            },
            "required": [
                "marca",
                "modelo",
                "categoria",
                "specs_claims_clave",
                "rango_precio_observado",
                "garantia_tipo",
                "variantes_detectadas",
            ],
        },

        "top_5_angulos": {
            "type": "array",
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "rank": {"type": "integer", "minimum": 1, "maximum": 5},
                    "angulo": {"type": "string"},
                    "score_0a10": {"type": "number", "minimum": 0, "maximum": 10},
                    "buyer_persona": {"type": "string"},
                    "promesa": {"type": "string"},
                    "objecion_principal": {"type": "string"},
                    "justificacion": {"type": "string"},
                    "evidencia": {
                        "type": "array",
                        "maxItems": 3,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "url": {"type": "string"},
                                "pais": {"type": "string"},
                                "match_note": {"type": "string"},
                                "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                            },
                            "required": ["url", "pais", "match_note", "confianza"],
                        },
                    },
                    "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                },
                "required": [
                    "rank",
                    "angulo",
                    "score_0a10",
                    "buyer_persona",
                    "promesa",
                    "objecion_principal",
                    "justificacion",
                    "evidencia",
                    "confianza",
                ],
            },
        },

        "hooks_por_angulo": {
            "type": "array",
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "rank_angulo": {"type": "integer", "minimum": 1, "maximum": 5},
                    "hooks": {"type": "array", "items": {"type": "string"}, "maxItems": 15},
                },
                "required": ["rank_angulo", "hooks"],
            },
        },

        "estacionalidad": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "clasificacion": {"type": "string", "enum": ["Atemporal", "Estacional", "Mixto", "No confirmado"]},
                "hechos": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
                "hipotesis": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
                "correlacion_con_angulos": {"type": "string"},
            },
            "required": ["clasificacion", "hechos", "hipotesis", "correlacion_con_angulos"],
        },

        "checklist": {
            "type": "array",
            "minItems": 13,
            "maxItems": 13,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "criterio": {"type": "string"},
                    "cumple": {"type": "string", "enum": ["SI", "NO"]},
                    "confianza": {"type": "string", "enum": ["Alta", "Media", "Baja"]},
                    "nota": {"type": "string"},
                    "fuentes": {"type": "array", "items": {"type": "string"}, "maxItems": 2},
                },
                "required": ["criterio", "cumple", "confianza", "nota", "fuentes"],
            },
        },

        "score_total": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "total_si": {"type": "integer", "minimum": 0, "maximum": 13},
                "total_criterios": {"type": "integer", "enum": [13]},
                "cumple_9_de_15": {"type": "boolean"},
            },
            "required": ["total_si", "total_criterios", "cumple_9_de_15"],
        },

        "next_searches": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
        "missing_data_flags": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
    },
    "required": [
        "meta",
        "input",
        "resumen_corto",
        "product_fingerprint",
        "top_5_angulos",
        "hooks_por_angulo",
        "estacionalidad",
        "checklist",
        "score_total",
        "next_searches",
        "missing_data_flags",
    ],
}


# =========================
# RUNNER
# =========================
def run_market_research_agent_min(
    *,
    nombre_producto: str,
    descripcion: str,
    garantia: str,
    precio: str,
    model: str = "gpt-5",
    max_output_tokens: int = 16384,
    margin_goodness: bool = None,
    competitors_goodness: bool = None,
) -> Dict[str, Any]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Checklist EXACTO (13 filas) como lo pediste
    checklist_criterios = [
        "Soluciona un problema?",
        "Se entiende su funcionalidad rapidamente?",
        "Se le puede vender a una audiencia masiva?",
        "Algun amigo o familiar estaria dispuesto a comprarlo?",
        "Tiene un buen margen?",
        "Es de dificil de encontar en tiendas convencionales (es de dificil acceso)?",
        "Es unico e impresionante (WOW factor)? Factor wow novedosos nuevo",
        "Esta en tendencia en otros paises o en otros mercados?",
        "Existen pocos competidores vendiendolo?",
        "Tiene antecedente de buenas ventas en otros paises otros mercados?",
        "Tu comprarias el producto?",
        "Tiene multimedia en internet a la que puedas acceder facilmente?",
        "Es un producto evergreen?"
    ]

    margin_note = "No hay datos internos. Debes investigar y estimar."
    if margin_goodness is True:
        margin_note = "VERIFICADO INTERNAMENTE: El producto TIENE buen margen (>30k y >17%). Para el criterio 'Tiene un buen margen?', DEBES marcar SI y citar 'Validación interna de costos'."
    elif margin_goodness is False:
        margin_note = "VERIFICADO INTERNAMENTE: El producto NO TIENE buen margen. Para el criterio 'Tiene un buen margen?', DEBES marcar NO y citar 'Validación interna de costos'."

    competitors_note = "No hay datos de Spy Agent. Debes investigar y estimar."
    if competitors_goodness is True:
        competitors_note = "VERIFICADO EXTERNAMENTE por Spy Agent (<4 ads escalando): Existen pocos competidores. Para 'Existen pocos competidores vendiendolo?', DEBES marcar SI y citar 'Análisis de Ad Library'."
    elif competitors_goodness is False:
        competitors_note = "VERIFICADO EXTERNAMENTE por Spy Agent (>4 ads escalando): Mercado saturado/escalando. Para 'Existen pocos competidores vendiendolo?', DEBES marcar NO y citar 'Análisis de Ad Library'."

    user_payload = {
        "nombre_producto": nombre_producto.strip(),
        "descripcion": descripcion.strip(),
        "garantia": garantia.strip(),
        "precio": precio.strip(),
        "informacion_margen_verificada": margin_note,
        "informacion_competidores_verificada": competitors_note,
        "checklist_criterios_obligatorios": checklist_criterios,
        "regla_minima": "Debe cumplir >= 9 de 13. Calificar MUY DURO. Si no hay evidencia, marcar NO + nota No confirmado.",
    }
    resp = client.responses.create(
        model=model,
        tools=[{"type": "web_search"}],
        input=[
            {"role": "developer", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "ENTRADA (JSON):\n" + json.dumps(user_payload, ensure_ascii=False)},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "market_research_min",
                "schema": MARKET_MIN_SCHEMA,
                "strict": True,
            }
        },
        max_output_tokens=max_output_tokens,
    )

    if getattr(resp, "status", None) == "incomplete":
        raise RuntimeError(
            f"Respuesta incompleta (reason={resp.incomplete_details.reason}). "
            "Sube max_output_tokens o reduce aún más el schema."
        )

    data = json.loads(resp.output_text)

    # Completa meta defensivo
    data.setdefault("meta", {})
    data["meta"].setdefault("run_date_iso", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    data["meta"].setdefault("model", model)
    data["meta"].setdefault("locale_priority", "CO_first_then_LATAM")

    return data


def save_json(data: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Market Research Agent")
    parser.add_argument("--product", required=True, help="Product Name")
    parser.add_argument("--desc", required=True, help="Product Description")
    parser.add_argument("--warranty", required=True, help="Warranty info")
    parser.add_argument("--price", required=True, help="Price info")
    parser.add_argument("--output", required=True, help="Output JSON path")
    parser.add_argument("--margin_ok", help="Is margin confirmed good? (true/false)")
    parser.add_argument("--competitors_ok", help="Are competitors few/confirmed good? (true/false)")
    
    args = parser.parse_args()

    # Parse boolean flags
    margin_good = None
    if args.margin_ok:
        lower = args.margin_ok.lower().strip()
        if lower == "true":
            margin_good = True
        elif lower == "false":
            margin_good = False

    competitors_good = None
    if args.competitors_ok:
        lower = args.competitors_ok.lower().strip()
        if lower == "true":
            competitors_good = True
        elif lower == "false":
            competitors_good = False

    out = run_market_research_agent_min(
        nombre_producto=args.product,
        descripcion=args.desc,
        garantia=args.warranty,
        precio=args.price,
        model="gpt-5",
        max_output_tokens=16384,
        margin_goodness=margin_good,
        competitors_goodness=competitors_good,
    )
    save_json(out, args.output)
    print(f"✅ Guardado en {args.output}")
