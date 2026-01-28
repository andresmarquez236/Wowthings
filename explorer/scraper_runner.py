#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
explorer/scraper_runner.py

Objetivo:
- Leer seed_queries.json
- Construir URLs de búsqueda para Facebook Ads Library
- Ejecutar Apify Actor (curious_coder/facebook-ads-library-scraper)
- Guardar resultados RAW y DEDUP en explorer/data/runs/<timestamp>/

Uso:
  python explorer/scraper_runner.py
"""

import hashlib
import json
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse, parse_qs

from dotenv import load_dotenv
from apify_client import ApifyClient

# Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import setup_logger

logger = setup_logger("Explorer_Scraper")
load_dotenv()

# =============================
# CONFIG
# =============================

APIFY_ACTOR = "curious_coder/facebook-ads-library-scraper"
LIMIT_PER_SOURCE = 60  # Ajustable
SCRAPE_DETAILS = False

# =============================
# Helpers
# =============================

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _timestamp_folder() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def first_present(item: Dict[str, Any], paths: List[str]) -> Any:
    for p in paths:
        val = item
        for part in p.split("."):
            if isinstance(val, dict) and part in val:
                val = val[part]
            elif isinstance(val, list) and part.isdigit() and int(part) < len(val):
                val = val[int(part)]
            else:
                val = None
                break
        if val:
            return val
    return None

def compute_ad_dedupe_key(item: Dict[str, Any]) -> Tuple[str, str]:
    page_id = first_present(item, ["pageId", "pageID", "page_id", "advertiser.id"])
    page_name = first_present(item, ["pageName", "page_name", "advertiser.name"])
    
    advertiser_key = normalize_text(page_id) if page_id else normalize_text(page_name)
    if not advertiser_key:
        advertiser_key = "unknown_advertiser"

    ad_archive_id = first_present(item, ["adArchiveId", "adArchiveID", "ad_archive_id"])
    ad_id = first_present(item, ["adId", "ad_id", "id"])

    ad_key = str(ad_archive_id or ad_id or "").strip()
    if not ad_key:
        # Fallback hash
        parts = [
            first_present(item, ["snapshot.body", "adCreativeBody"]),
            first_present(item, ["snapshot.title", "adCreativeTitle"]),
            first_present(item, ["snapshot.link_url", "linkUrl"]),
            first_present(item, ["snapshot.images.0.original_image_url"])
        ]
        payload = "|".join([normalize_text(p) for p in parts])
        ad_key = hashlib.sha1(payload.encode("utf-8")).hexdigest()

    return advertiser_key, ad_key

def build_ads_library_search_url(query: str, country: str = "CO") -> str:
    # Defaults fijos para exploración amplia
    active_status = "all"
    ad_type = "all"
    search_type = "keyword_unordered"
    media_type = "all"
    
    q = quote_plus(query.strip())
    # Normalizar país simple
    c_code = "CO" if country.lower() in ["colombia", "co"] else country.upper()
    
    return (
        "https://www.facebook.com/ads/library/"
        f"?active_status={active_status}"
        f"&ad_type={ad_type}"
        f"&country={c_code}"
        f"&q={q}"
        f"&search_type={search_type}"
        f"&media_type={media_type}"
    )

def extract_query_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        return qs.get("q", [None])[0]
    except:
        return None

# =============================
# Core Logic
# =============================

def load_seed_queries(path: Path) -> List[Dict]:
    """
    Carga y aplana las queries del JSON generado.
    Retorna lista de dicts: {"query": str, "intent": str}
    """
    if not path.exists():
        raise FileNotFoundError(f"No existe: {path}")
        
    data = json.loads(path.read_text(encoding="utf-8"))
    
    flat_queries = []
    # Data puede ser lista de intents (si viene de seed_query_generator)
    if isinstance(data, list):
        for intent_grp in data:
            cat = intent_grp.get("category_intent", "Unknown")
            for q in intent_grp.get("queries", []):
                flat_queries.append({"query": q, "intent": cat})
    
    return flat_queries

def run_scraper():
    # Setup Paths
    root_dir = Path(__file__).resolve().parent
    seed_path = root_dir / "seed_queries.json"
    
    # Run folder
    run_id = _timestamp_folder()
    data_dir = root_dir / "data" / "runs" / run_id
    data_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Iniciando Scraper Explorer. RunID: {run_id}")
    
    # Load Queries
    queries_data = load_seed_queries(seed_path)
    # Deduplicar queries por texto
    unique_queries_map = {item["query"]: item["intent"] for item in queries_data}
    queries = list(unique_queries_map.keys())
    
    logger.info(f"Cargadas {len(queries)} queries únicas de {len(queries_data)} entradas.")
    
    # Apify Client
    token = os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_APY_KEY")
    if not token:
        raise ValueError("Missing APIFY_API_TOKEN")
        
    client = ApifyClient(token)
    
    # Build Input
    urls = [build_ads_library_search_url(q) for q in queries]
    
    run_input = {
        "urls": [{"url": u} for u in urls],
        "scrapeAdDetails": SCRAPE_DETAILS,
        "limitPerSource": LIMIT_PER_SOURCE,
        "scrapePageAds.activeStatus": "all",
        "scrapePageAds.countryCode": "CO",
    }
    
    logger.info(f"Ejecutando Apify Actor ({len(urls)} URLs)...")
    
    try:
        run = client.actor(APIFY_ACTOR).call(run_input=run_input)
        dataset_id = run.get("defaultDatasetId")
        
        logger.info(f"Actor finalizado. Dataset: {dataset_id}")
        
        # Process Results
        raw_path = data_dir / "raw_ads.jsonl"
        dedup_path = data_dir / "dedup_ads.jsonl"
        
        seen_keys = set()
        raw_count = 0
        dedup_count = 0
        advertisers = set()
        
        with raw_path.open("w", encoding="utf-8") as f_raw, dedup_path.open("w", encoding="utf-8") as f_dedup:
            for item in client.dataset(dataset_id).iterate_items():
                raw_count += 1
                f_raw.write(json.dumps(item, ensure_ascii=False) + "\n")
                
                # Dedup logic
                adv_key, ad_key = compute_ad_dedupe_key(item)
                ukey = f"{adv_key}::{ad_key}"
                
                advertisers.add(adv_key)
                
                if ukey in seen_keys:
                    continue
                seen_keys.add(ukey)
                
                # Enrich
                enriched = dict(item)
                enriched["_explorer_run_id"] = run_id
                enriched["_dedup_key"] = ukey
                
                # Try to map back intent
                poss_url = first_present(enriched, ["sourceUrl", "url", "adLibraryUrl"])
                if poss_url:
                    q_str = extract_query_from_url(poss_url)
                    if q_str:
                        enriched["_query_matched"] = q_str
                        # Recuperar intent si match exacto (puede variar si URL tiene encoding raro)
                        # Normalizamos un poco para buscar en map
                        enriched["_intent_guess"] = unique_queries_map.get(q_str, "Unknown")
                
                dedup_count += 1
                f_dedup.write(json.dumps(enriched, ensure_ascii=False) + "\n")

        # Summary
        summary = {
            "run_id": run_id,
            "timestamp": _now_iso(),
            "queries_loaded": len(queries),
            "raw_count": raw_count,
            "dedup_count": dedup_count,
            "unique_advertisers": len(advertisers),
            "paths": {
                "raw": str(raw_path),
                "dedup": str(dedup_path)
            },
            "apify_run": run.get("id")
        }
        
        summary_path = data_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        
        logger.info(f"Scraping completado. Resumen: {summary}")
        
    except Exception as e:
        logger.error(f"Error en scraping: {e}")
        raise e

if __name__ == "__main__":
    run_scraper()
