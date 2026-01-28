#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
seed_query_generator.py

Objetivo:
- Generar queries "semilla" basadas en intención de compra y señales comerciales.
- NO busca productos específicos por nombre, sino por ofertas: "envío gratis", "pago contraentrega", etc.
- Salida: explorer/seed_queries.json

Uso:
  python explorer/seed_query_generator.py
"""

import json
import os
from pathlib import Path
from typing import List, Literal, Optional

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, conint

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.logger import setup_logger

# Configurar logger
logger = setup_logger("Explorer_SeedGen")

# Cargar entorno
load_dotenv()

# -----------------------------
# Structured Output (Pydantic)
# -----------------------------

class SeedQueriesOutput(BaseModel):
    category_intent: str = Field(..., description="Categoría de intención principal (ej: Urgency, Offer, Trust)")
    reasoning_summary: str = Field(..., description="Breve explicación de por qué estas queries funcionan para encontrar productos ganadores")
    queries: List[str] = Field(..., min_items=1, max_items=100, description="Lista de queries de búsqueda")

class MultiIntentOutput(BaseModel):
    intents: List[SeedQueriesOutput]

# -----------------------------
# Logic
# -----------------------------

def build_prompt(country: str) -> str:
    return f"""
Actúa como un experto en E-commerce DropShipping y Media Buying en Meta Ads.
Tu objetivo es generar "Seed Queries" para encontrar anuncios de productos ganadores en la Facebook Ads Library, SIN conocer el producto a priori.

Buscamos "Señales de Compra" y "Ganchos Comerciales" que usan los dropshippers exitosos.

PAÍS OBJETIVO: {country} (Ajusta jerigonza/modismos si aplica, pero mantén un español neutro comercial fuerte).

CATEGORÍAS DE INTENCIÓN A CUBRIR:
1. Modalidad de Pago/Envío (CRÍTICO en Latam): "pago contra entrega", "pagas al recibir", "envío gratis", "envío a todo el país".
2. Urgencia/Escasez: "últimas unidades", "quedan pocos", "oferta por tiempo limitado", "solo por hoy".
3. Ofertas Irresistibles: "2x1", "3x2", "compra 1 lleva 2", "50% descuento", "mitad de precio".
4. Garantía/Confianza: "garantía de satisfacción", "devolución gratis", "compra segura".
5. Call to Action (CTA): "pedir ahora", "comprar aquí", "clic para ordenar".

SALIDA ESPERADA:
Genera al menos 5 queries DIVERSAS y ÚNICAS por cada categoría.
Las queries deben ser frases cortas que esperarías encontrar en el TEXTO (body copy) o TÍTULO del anuncio.

NO uses:
- Marcas específicas (Nike, Adidas).
- Productos específicos (Zapatillas, Reloj).
- Palabras clave negativas.

Formato JSON estricto con la lista de intents y sus queries.
"""

def generate_seed_queries(
    country: str = "Colombia",
    max_queries_per_intent: int = 10,
    model: str = "gpt-5.2",
    store: bool = False
) -> List[SeedQueriesOutput]:
    
    client = OpenAI()
    
    logger.info(f"Generando seed queries para: {country} usando {model}")

    try:
        completion = client.beta.chat.completions.parse(
            model=model,
            messages=[
                {"role": "system", "content": "Eres un asistente experto en minería de datos de e-commerce."},
                {"role": "user", "content": build_prompt(country)},
            ],
            response_format=MultiIntentOutput,
        )
        
        parsed_result = completion.choices[0].message.parsed
        
        # Aplanar resultados si se desea, o devolver estructura completa
        return parsed_result.intents

    except Exception as e:
        logger.error(f"Error generando queries: {e}")
        raise e

def save_queries(intents: List[SeedQueriesOutput], filename: str = "seed_queries.json"):
    root_dir = Path(__file__).resolve().parent
    output_path = root_dir / filename
    
    # Convertir a formato simple para uso fácil: lista plana de strings o dict estructurado
    # Para el "Explorador", tal vez queramos un pool único, pero guardemos estructurado por ahora
    # para tener trazabilidad del "por qué".
    
    data_to_save = [intent.model_dump() for intent in intents]
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        
    logger.info(f"Queries guardadas en: {output_path}")
    
    # Return path for logging
    return output_path

def main():
    # Configuración
    COUNTRY = "Colombia"
    MODEL = "gpt-5.2"
    
    try:
        intents = generate_seed_queries(country=COUNTRY, model=MODEL)
        
        total_queries = sum(len(i.queries) for i in intents)
        logger.info(f"Se generaron {len(intents)} categorías con un total de {total_queries} queries.")
        
        save_queries(intents)
        
    except Exception as e:
        logger.error(f"Fallo en execution main: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
