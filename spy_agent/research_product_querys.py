#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
product_query_researcher.py

Objetivo:
- Identificar con alta confianza cuál es el producto EXACTO (desde nombre + descripción + imágenes)
- Generar hasta N (default 10) queries robustas para búsquedas en Facebook Ads Library,
  minimizando "drift" (que la búsqueda se vaya a otros productos parecidos).

Requisitos:
  pip install --upgrade openai pydantic

Uso:
  export OPENAI_API_KEY="..."
  python product_query_researcher.py \
    --name "bee venom bswell" \
    --description "Crema/serum con veneno de abeja, antiarrugas, etc..." \
    --images_dir "/ruta/a/imagenes" \
    --max_queries 10 \
    --model "gpt-5.2" \
    --reasoning_effort high
"""


import base64
import json
import mimetypes
import os
from pathlib import Path
from typing import List, Literal, Optional

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, confloat, conint, field_validator

from utils.logger import setup_logger
logger = setup_logger("SpyAgent_ResearchQuerys")


# Cargar entorno (API Key)
load_dotenv()


# -----------------------------
# Structured Output (Pydantic)
# -----------------------------

DriftRisk = Literal["low", "medium", "high"]




class ProductResearchOutput(BaseModel):
    canonical_product_name: str = Field(..., min_length=2, max_length=120)
    product_type: str = Field(..., min_length=2, max_length=80, description="Ej: crema facial, serum, maleta cápsula, etc.")
    short_description: str = Field(..., min_length=10, max_length=260)
    disambiguation_notes: str = Field(..., min_length=10, max_length=420, description="Cómo diferenciarlo de productos similares")
    max_queries: conint(ge=1, le=50) = Field(..., description="N solicitado")
    querys: List[str] = Field(..., min_items=1, max_items=50, description="Lista de queries optimizadas para FB Ads Library")

    @field_validator("querys")
    def enforce_max_queries(cls, v, info):
        max_q = info.data.get("max_queries", 30)
        return v[:max_q]


# -----------------------------
# Helpers
# -----------------------------

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".webp"}

def list_images(images_dir: Path, max_images: int) -> List[Path]:
    if not images_dir.exists() or not images_dir.is_dir():
        raise FileNotFoundError(f"images_dir no existe o no es carpeta: {images_dir}")

    files = []
    for p in sorted(images_dir.rglob("*")):
        if p.is_file() and p.suffix.lower() in ALLOWED_EXT:
            files.append(p)

    return files[:max_images]

def encode_image_to_data_url(image_path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(image_path))
    if mime is None:
        # fallback razonable
        mime = "image/jpeg"

    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    return f"data:{mime};base64,{b64}"

def build_prompt(name: str, description: str, max_queries: int) -> str:
    # Prompt “duro” en constraints, suave en estilo: deja que el modelo piense,
    # pero le cierras el espacio de salida con schema.
    return f"""
Actúa como un experto PhD en Análisis Semántico y Estrategias de Búsqueda para E-commerce.
Tu objetivo es diseccionar la identidad de un producto y generar patrones de búsqueda (queries) altamente efectivos para Facebook Ads Library.

INPUTS:
- product_name: {name!r}
- product_description: {description!r}
- product_images: (imágenes adjuntas).

OBJETIVO 1 — IDENTIDAD DEL PRODUCTO (Deep Analysis)
1) Realiza un análisis fenoménico del producto: ¿Qué es ontológicamente? (Tipo, categoría, sustancia).
2) Identifica los "Unique Selling Propositions" (USPs) visuales y textuales (marcas, claims científicos, concentraciones, formato).
3) Diferencia el producto de genéricos: ¿Qué lo hace único frente a la competencia?

OBJETIVO 2 — GENERACIÓN DE QUERIES (Semantic Search Patterns)
Genera una lista de {max_queries} strings de búsqueda optimizados.
Reglas CRÍTICAS de formato:
- NO uses comillas internas (ej: "Ashwagandha" KSM-66) a menos que sea IMPRESCINDIBLE para forzar una frase exacta rara.
- El formato deseado es texto plano limpio: 'Ashwagandha KSM-66 oferta 5500mg'.
- Maximiza la recuperación (recall) y la precisión: combina nombre del producto con intención de compra y atributos clave.

Estrategia de Queries:
1. Exact Match: Nombre comercial + especificación técnica.
2. Attribute Combinations: Ingrediente clave + concentración + formato.
3. Commercial Intent (OBLIGATORIO): Debes generar al menos 10 queries que incluyan términos como: "envío gratis", "pago contra entrega", "oferta", "descuento", "promo", "2x1", "disponible en colombia" (o el país que sugiera la imagen/texto).
4. Competitor Differentiation: Términos que filtren versiones baratas o diferentes (ej: "softgels").
5. PROHIBIDO: NO incluyas términos negativos o de exclusión en la query (ej: "no gummies", "no polvo", "sin azúcar"). Las búsquedas deben ser siempre afirmativas sobre lo que ES el producto.

Output Schema (Strict JSON):
{{
  "canonical_product_name": "Nombre estandarizado y técnico del producto",
  "product_type": "Categoría taxonómica del producto",
  "short_description": "Descripción sintetizada de alto nivel",
  "disambiguation_notes": "Notas críticas para distinguir este producto de variantes similares",
  "querys": ["query1 clean", "query2 clean", ...]
}}
""".strip()


def run_research(
    name: str,
    description: str,
    images_dir: Path,
    max_queries: int,
    model: str,
    reasoning_effort: str,
    max_images: int,
    store: bool,
) -> ProductResearchOutput:
    client = OpenAI()

    image_paths = list_images(images_dir, max_images=max_images)
    content_parts = [{"type": "input_text", "text": build_prompt(name, description, max_queries)}]

    # Adjuntamos imágenes al mismo mensaje (en orden)
    for p in image_paths:
        data_url = encode_image_to_data_url(p)
        content_parts.append({
            "type": "input_image",
            "image_url": data_url,
        })

    # Responses API + Structured Outputs (Pydantic)
    response = client.responses.parse(
        model=model,
        input=[{
            "role": "user",
            "content": content_parts
        }],
        reasoning={"effort": reasoning_effort},
        # baja verbosidad porque el schema ya lleva todo
        text={"verbosity": "low"},
        text_format=ProductResearchOutput,
        store=store,
    )

    out: ProductResearchOutput = response.output_parsed

    # Post-proceso mínimo: dedupe queries manteniendo orden
    seen = set()
    deduped = []
    for q in out.querys:
        k = q.lower().strip()
        if k not in seen:
            seen.add(k)
            deduped.append(q)
    out.querys = deduped[:max_queries]
    
    return out


def run_research_step(
    name: str,
    description: str,
    max_queries: int = 30,
    model: str = "gpt-5.2",
    reasoning_effort: str = "high",
    max_images: int = 6,
    store: bool = False,
) -> Path:
    """
    Orquesta el paso de investigación:
    1. Calcula rutas de imágenes y salida.
    2. Ejecuta run_research.
    3. Guarda el JSON.
    Retorna la ruta del archivo JSON generado.
    """
    root_dir = Path(__file__).resolve().parent.parent
    folder_name = name.lower().strip().replace(" ", "_")
    images_dir = root_dir / "output" / folder_name / "product_images"
    
    if not images_dir.exists():
        # Fallback o error: intentar buscar si la carpeta existe con slugify diferente
        # Por ahora, lanzamos error claro.
        raise FileNotFoundError(f"La carpeta de imágenes '{images_dir}' no existe. Asegúrate de que las imágenes estén ahí.")

    logger.info(f"[{name}] Iniciando investigación...")
    logger.info(f"  - Images: {images_dir}")
    logger.info(f"  - Model: {model}")

    result = run_research(
        name=name,
        description=description,
        images_dir=images_dir,
        max_queries=max_queries,
        model=model,
        reasoning_effort=reasoning_effort,
        max_images=max_images,
        store=store,
    )

    # Guardado en JSON dentro de la carpeta del producto
    output_dir = root_dir / "output" / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    json_filename = f"querys_fblibrary_{folder_name}.json"
    json_path = output_dir / json_filename
    
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))
    
    logger.info(f"Investigación completada. Resultado guardado en: {json_path}")
    return json_path


def main():
    # Configuración de argumentos directos
    # Ajusta estos valores según tu necesidad o déjalos como defaults al importar
    NAME = "Ashawanda Ksm 66 X 2 Unidades"
    DESCRIPTION = "Ashwagandha: Un Adaptógeno Natural\n\nAshwagandha, también conocida como ginseng indio, es una hierba adaptogénica que ha sido utilizada durante siglos en la medicina ayurvédica. En los últimos años, ha ganado popularidad en el mundo occidental debido a sus numerosos beneficios para la salud.\n\nBeneficios del Ashwagandha\n\nReducción del estrés: Ayuda a reducir los niveles de cortisol, la hormona del estrés, lo que puede mejorar el estado de ánimo y la calidad del sueño.\nMejora de la función cognitiva: Puede mejorar la memoria, la concentración y la función cerebral en general.\nAumento de la fuerza muscular: Puede ayudar a aumentar la masa muscular y la fuerza física.\nReducción de la inflamación: Tiene propiedades antiinflamatorias que pueden aliviar el dolor y la rigidez articular.\nApoyo al sistema inmunológico: Puede fortalecer el sistema inmunológico y ayudar a combatir las infecciones.\nEquilibrio hormonal: Puede ayudar a equilibrar las hormonas, especialmente en las mujeres."
    
    MAX_QUERIES = 30
    MODEL = "gpt-5.2" 
    REASONING_EFFORT = "high"
    MAX_IMAGES = 6
    STORE = False

    try:
        run_research_step(
            name=NAME,
            description=DESCRIPTION,
            max_queries=MAX_QUERIES,
            model=MODEL,
            reasoning_effort=REASONING_EFFORT,
            max_images=MAX_IMAGES,
            store=STORE,
        )

    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}")


if __name__ == "__main__":
    main()
