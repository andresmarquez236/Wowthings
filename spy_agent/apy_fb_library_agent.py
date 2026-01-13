#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
apy_fb_library_agent.py

Agente orquestador para el flujo de espionaje en Facebook Ads Library.
1. Ejecuta la investigación de queries (OpenAI via research_product_querys.py)
2. Ejecuta el scraping (Apify Actor via apify_actor.py)
3. Procesa los resultados y genera reportes (process_info.py)

Uso:
    python spy_agent/apy_fb_library_agent.py
    (Configura los parámetros en main())
"""

import sys
import logging
from typing import Optional, List

# Configurar logging
from utils.logger import setup_logger
logger = setup_logger("SpyAgent_Orchestrator")

# Importar funciones de los scripts hermanos
try:
    from spy_agent.research_product_querys import run_research_step
    from spy_agent.apify_actor import run_apify_actor, NAME as DEFAULT_NAME, SEARCH_COUNTRY as DEFAULT_COUNTRY
    from spy_agent.process_info import run_process_info, RANGE_PRODUCT_TEST, NUM_MAX_ESCALING
except ImportError:
    # Si se ejecuta como script desde dentro de la carpeta
    sys.path.append(".")
    from research_product_querys import run_research_step
    from apify_actor import run_apify_actor, NAME as DEFAULT_NAME, SEARCH_COUNTRY as DEFAULT_COUNTRY
    from process_info import run_process_info, RANGE_PRODUCT_TEST, NUM_MAX_ESCALING


def run_spy_flow(
    product_name: str,
    product_description: str,
    country: str = DEFAULT_COUNTRY,
    limit_per_source: Optional[int] = 80,
    scrape_ad_details: bool = False,
    range_product_test: Optional[List[int]] = None,
    num_max_escaling: Optional[int] = None,
    dry_run: bool = False,
):
    """
    Orquesta el flujo completo: Research -> Scraping -> Procesamiento.
    """
    logger.info(f"Iniciando flujo Spy Agent para: '{product_name}' ({country})")

    if range_product_test is None:
        range_product_test = RANGE_PRODUCT_TEST
    if num_max_escaling is None:
        num_max_escaling = NUM_MAX_ESCALING

    if dry_run:
        logger.info("DRY RUN: No se ejecutarán operaciones reales.")
        return

    # PASO 0: Research (OpenAI)
    try:
        logger.info(">>> PASO 0: Ejecutando Investigación de Queries...")
        # Nota: asume que las imágenes ya están en output/<snake_case>/product_images/
        run_research_step(
            name=product_name,
            description=product_description,
            max_queries=30,  # Configurable si se desea pasar como arg
            model="gpt-5.2",  # Updated model
            store=False
        )
        logger.info("Investigación completada exitosamente.")
    except Exception as e:
        logger.error(f"Fallo en PASO 0 (Research): {e}")
        raise e

    # PASO 1: Scraping (Apify)
    try:
        logger.info(">>> PASO 1: Ejecutando Apify Scraper...")
        scrape_summary = run_apify_actor(
            name=product_name,
            country_code=country,
            limit_per_source=limit_per_source,
            scrape_ad_details=scrape_ad_details
        )
        logger.info("Scraping completado exitosamente.")
        logger.info(f"Raw Items: {scrape_summary['counts']['raw_items']} | Dedup Items: {scrape_summary['counts']['dedup_items']}")
    except Exception as e:
        logger.error(f"Fallo en PASO 1 (Scraping): {e}")
        raise e

    # PASO 2: Procesamiento (Reportes)
    try:
        logger.info(">>> PASO 2: Procesando información y generando reportes...")
        run_process_info(
            name=product_name,
            country=country,
            range_product_test=range_product_test,
            num_max_escaling=num_max_escaling
        )
        logger.info("Procesamiento completado exitosamente.")
    except Exception as e:
        logger.error(f"Fallo en PASO 2 (Procesamiento): {e}")
        raise e

    logger.info("✅ Flujo Spy Agent finalizado correctamente.")


def main():
    # ==========================================
    # CONFIGURACIÓN DEL AGENTE
    # ==========================================
    
    NAME = "Bee Venom Bswell"
    
    DESCRIPTION = (
        "Ashwagandha: Un Adaptógeno Natural\n\n"
        "Ashwagandha, también conocida como ginseng indio, es una hierba adaptogénica que ha sido utilizada "
        "durante siglos en la medicina ayurvédica. En los últimos años, ha ganado popularidad en el mundo "
        "occidental debido a sus numerosos beneficios para la salud.\n\n"
        "Beneficios:\n"
        "- Reducción del estrés (cortisol)\n"
        "- Mejora de función cognitiva\n"
        "- Aumento de fuerza muscular\n"
        "- Propiedades antiinflamatorias\n"
        "- Apoyo inmunológico y hormonal"
    )
    
    COUNTRY = "CO"
    LIMIT_PER_SOURCE = 80
    SCRAPE_DETAILS = False
    
    # ==========================================
    
    run_spy_flow(
        product_name=NAME,
        product_description=DESCRIPTION,
        country=COUNTRY,
        limit_per_source=LIMIT_PER_SOURCE,
        scrape_ad_details=SCRAPE_DETAILS,
    )


if __name__ == "__main__":
    main()
