#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fb_library_scrape_from_queries.py

Objetivo:
- Leer querys desde: output/{folder_name}/querys_fblibrary_{folder_name}.json
- Ejecutar Apify Actor: curious_coder/facebook-ads-library-scraper
- Buscar por TODAS las querys (URLs Ads Library construidas)
- Guardar RAW + DEDUP + SUMMARY en la carpeta del producto

Requisitos:
  pip install --upgrade apify-client python-dotenv

Env:
  export APIFY_API_TOKEN="..."

Estructura esperada:
  <root>/output/<nombre_producto_snake_case>/querys_fblibrary_<nombre_producto_snake_case>.json
  <root>/output/<nombre_producto_snake_case>/product_images/...
"""

import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urlparse, parse_qs

from dotenv import load_dotenv
from apify_client import ApifyClient

load_dotenv()


# =============================
# CONFIG (ENTRADAS)
# =============================

# Nombre del producto (igual al usado para crear la carpeta output)
NAME = "Ashawanda Ksm 66 X 2 Unidades"

# Actor (slug o actorId)
APIFY_ACTOR = "curious_coder/facebook-ads-library-scraper"

# Ubicación / país de búsqueda (DEFAULT: Colombia)
# Acepta: "CO", "colombia", "ALL"
SEARCH_COUNTRY = "colombia"

# URL params de Ads Library
ACTIVE_STATUS = "all"          # all|active|inactive
AD_TYPE = "all"               # all
SEARCH_TYPE = "keyword_unordered"
MEDIA_TYPE = "all"

# Controles del actor (performance/costo)
LIMIT_PER_SOURCE = 80          # ads por query (URL)
COUNT_TOTAL = None             # None recomendado si usas limit_per_source
PERIOD = ""                    # "" o "last7d"/"last14d"/"last30d" (según actor)
SCRAPE_AD_DETAILS = False      # True = más lento/pesado

# scrapePageAds.* (aunque uses búsquedas por keyword, lo dejamos consistente)
SCRAPE_PAGE_ADS_ACTIVE_STATUS = "all"
SCRAPE_PAGE_ADS_COUNTRY_CODE = None  # si None, se setea igual que SEARCH_COUNTRY normalizado

# Proxy opcional (dict). Ej: {"useApifyProxy": True}
PROXY = None


# =============================
# Helpers base
# =============================

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "product"


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_country(country: str) -> str:
    """
    Normaliza país a ISO-2 o ALL.
    Default: Colombia -> CO
    """
    c = (country or "").strip().upper()

    if c in {"", "ALL"}:
        return "ALL"
    if c in {"COLOMBIA", "COL"}:
        return "CO"
    if c == "CO":
        return "CO"

    # si llega ISO2 válido
    if len(c) == 2 and re.fullmatch(r"[A-Z]{2}", c):
        return c

    # fallback: si alguien pone "colombia" en minúscula u otra cosa
    c2 = (country or "").strip().lower()
    if c2 == "colombia":
        return "CO"

    # último recurso: ALL
    return "ALL"


def get_nested(d: Any, path: str) -> Any:
    cur = d
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, list):
            if part.isdigit():
                idx = int(part)
                if 0 <= idx < len(cur):
                    cur = cur[idx]
                else:
                    return None
            else:
                return None
        elif isinstance(cur, dict):
            if part in cur:
                cur = cur[part]
            else:
                return None
        else:
            return None
    return cur


def first_present(item: Dict[str, Any], paths: List[str]) -> Any:
    for p in paths:
        v = get_nested(item, p)
        if v is not None and v != "":
            return v
    return None


# =============================
# Construcción URLs Ads Library
# =============================

def build_ads_library_search_url(
    query: str,
    country: str,
    active_status: str,
    ad_type: str,
    search_type: str,
    media_type: str,
) -> str:
    q = quote_plus(query.strip())
    return (
        "https://www.facebook.com/ads/library/"
        f"?active_status={active_status}"
        f"&ad_type={ad_type}"
        f"&country={country}"
        f"&q={q}"
        f"&search_type={search_type}"
        f"&media_type={media_type}"
    )


def extract_query_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        q = qs.get("q", [None])[0]
        return q
    except Exception:
        return None


# =============================
# Lectura de queries (JSON estándar)
# =============================

def load_queries_json(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo de querys: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    qs = data.get("querys")
    if not isinstance(qs, list) or not qs:
        raise ValueError("El JSON no contiene la clave 'querys' como lista no vacía.")
    return qs


def dedupe_preserve_order(strings: List[str]) -> List[str]:
    seen = set()
    out = []
    for s in strings:
        s2 = " ".join(str(s).strip().split())
        k = normalize_text(s2)
        if k and k not in seen:
            seen.add(k)
            out.append(s2)
    return out


# =============================
# Dedupe (anunciante + anuncio)
# =============================

def compute_ad_dedupe_key(item: Dict[str, Any]) -> Tuple[str, str]:
    """
    advertiser_key:
      - pageId / advertiser id, fallback a pageName.
    ad_key:
      - adArchiveId preferido, fallback adId, fallback hash.
    """
    page_id = first_present(item, [
        "pageId", "pageID", "page_id",
        "advertiser.id",
        "advertiser.page_id",
        "advertiser.ad_library_page_info.page_id",
        "advertiser.ad_library_page_info.page_info.page_id",
    ])
    page_name = first_present(item, [
        "pageName", "page_name",
        "advertiser.name",
        "advertiser.ad_library_page_info.page_info.name",
        "advertiser.ad_library_page_info.page_name",
    ])

    advertiser_key = normalize_text(page_id) if page_id else normalize_text(page_name)
    if not advertiser_key:
        advertiser_key = "unknown_advertiser"

    ad_archive_id = first_present(item, [
        "adArchiveId", "adArchiveID", "ad_archive_id", "ad_archiveId",
    ])
    ad_id = first_present(item, ["adId", "adID", "ad_id", "id"])

    ad_key = str(ad_archive_id or ad_id or "").strip()
    if not ad_key:
        body = first_present(item, ["snapshot.body", "snapshot.body.text", "adCreativeBody", "ad_creative_body"])
        title = first_present(item, ["snapshot.title", "adCreativeTitle", "ad_creative_title"])
        link = first_present(item, ["snapshot.link_url", "snapshot.link", "linkUrl", "link_url"])
        img = first_present(item, [
            "snapshot.images.0.original_image_url",
            "snapshot.images.0.originalImageUrl",
            "snapshot.images.0.url",
        ])
        payload = "|".join([normalize_text(body), normalize_text(title), normalize_text(link), normalize_text(img)])
        ad_key = hashlib.sha1(payload.encode("utf-8")).hexdigest()

    return advertiser_key, ad_key


# =============================
# Runner principal
# =============================

def run_actor_and_save(
    client: ApifyClient,
    actor_id: str,
    run_input: Dict[str, Any],
    raw_out_path: Path,
    dedup_out_path: Path,
) -> Dict[str, Any]:

    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError(f"No se encontró defaultDatasetId en la respuesta del run: {run}")

    seen = set()
    advertisers = set()
    raw_count = 0
    dedup_count = 0

    raw_out_path.parent.mkdir(parents=True, exist_ok=True)

    with raw_out_path.open("w", encoding="utf-8") as f_raw, dedup_out_path.open("w", encoding="utf-8") as f_dedup:
        for item in client.dataset(dataset_id).iterate_items():
            raw_count += 1
            f_raw.write(json.dumps(item, ensure_ascii=False) + "\n")

            adv_key, ad_key = compute_ad_dedupe_key(item)
            advertisers.add(adv_key)
            ukey = f"{adv_key}::{ad_key}"

            if ukey in seen:
                continue
            seen.add(ukey)

            enriched = dict(item)
            enriched["_dedupe_key"] = ukey

            possible_url = first_present(enriched, ["sourceUrl", "source_url", "inputUrl", "input_url", "url", "adLibraryUrl"])
            if isinstance(possible_url, str):
                q = extract_query_from_url(possible_url)
                if q:
                    enriched["_source_query_guess"] = q

            dedup_count += 1
            f_dedup.write(json.dumps(enriched, ensure_ascii=False) + "\n")

    return {
        "run": run,
        "dataset_id": dataset_id,
        "raw_count": raw_count,
        "dedup_count": dedup_count,
        "unique_advertisers": len(advertisers),
    }


def run_apify_actor(
    name: str = NAME,
    country_code: str = SEARCH_COUNTRY,
    limit_per_source: int = LIMIT_PER_SOURCE,
    count_total: Optional[int] = COUNT_TOTAL,
    period: str = PERIOD,
    scrape_ad_details: bool = SCRAPE_AD_DETAILS,
    active_status: str = ACTIVE_STATUS,
    ad_type: str = AD_TYPE,
    search_type: str = SEARCH_TYPE,
    media_type: str = MEDIA_TYPE,
    proxy: Optional[Dict] = PROXY,
    apify_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Función orquestadora que prepara los inputs y corre el actor.
    Retorna el dict summary.
    """
    if not apify_token:
        apify_token = os.getenv("APIFY_APY_KEY") or os.getenv("APIFY_API_TOKEN") or os.getenv("APIFY_TOKEN")
    
    if not apify_token:
        raise RuntimeError("Falta APIFY_APY_KEY (o APIFY_API_TOKEN) en variables de entorno.")

    client = ApifyClient(apify_token)
    
    # Directorios
    root_dir = Path(__file__).resolve().parent.parent
    folder_name = slugify(name)
    product_dir = root_dir / "output" / folder_name
    
    apify_results_dir = product_dir / "apify_results"
    apify_results_dir.mkdir(parents=True, exist_ok=True)
    
    query_path = product_dir / f"querys_fblibrary_{folder_name}.json"
    if not query_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de queries: {query_path}")

    # País normalizado
    norm_country = normalize_country(country_code)
    # Si scrapePageAds.countryCode es None, usamos el mismo país
    scrape_country = normalize_country(SCRAPE_PAGE_ADS_COUNTRY_CODE) if SCRAPE_PAGE_ADS_COUNTRY_CODE else norm_country

    queries = dedupe_preserve_order(load_queries_json(query_path))

    urls = [
        build_ads_library_search_url(
            query=q,
            country=norm_country,
            active_status=active_status,
            ad_type=ad_type,
            search_type=search_type,
            media_type=media_type,
        )
        for q in queries
    ]

    run_input: Dict[str, Any] = {
        "urls": [{"url": u} for u in urls],
        "scrapeAdDetails": bool(scrape_ad_details),
        "limitPerSource": int(limit_per_source) if limit_per_source else None,
        "count": int(count_total) if count_total else None,
        "period": period or "",
        "scrapePageAds.activeStatus": SCRAPE_PAGE_ADS_ACTIVE_STATUS,
        "scrapePageAds.countryCode": scrape_country,
        "proxy": proxy,
    }

    # limpia None
    run_input = {k: v for k, v in run_input.items() if v is not None}

    # Paths de salida
    raw_out = apify_results_dir / f"fblibrary_ads_raw_{folder_name}.jsonl"
    dedup_out = apify_results_dir / f"fblibrary_ads_dedup_{folder_name}.jsonl"
    summary_out = apify_results_dir / f"fblibrary_scrape_summary_{folder_name}.json"

    print(f"[{_now_iso()}] Product: {name}")
    print(f"[{_now_iso()}] Folder: {product_dir}")
    print(f"[{_now_iso()}] Query file: {query_path}")
    print(f"[{_now_iso()}] Country: {norm_country} (scrapePageAds.countryCode={scrape_country})")
    print(f"[{_now_iso()}] Actor: {APIFY_ACTOR}")
    print(f"[{_now_iso()}] Queries: {len(queries)} | URLs: {len(urls)}")
    print(f"[{_now_iso()}] params: limitPerSource={limit_per_source}, details={scrape_ad_details}")

    try:
        result = run_actor_and_save(
            client=client,
            actor_id=APIFY_ACTOR,
            run_input=run_input,
            raw_out_path=raw_out,
            dedup_out_path=dedup_out,
        )

        summary = {
            "timestamp_utc": _now_iso(),
            "product_name": name,
            "product_folder": str(product_dir),
            "query_file": str(query_path),
            "country": norm_country,
            "actor": APIFY_ACTOR,
            "run_input_meta": {
                "urls_count": len(urls),
                "scrapeAdDetails": run_input.get("scrapeAdDetails", False),
                "limitPerSource": run_input.get("limitPerSource"),
                "count": run_input.get("count"),
                "period": run_input.get("period", ""),
                "scrapePageAds.activeStatus": run_input.get("scrapePageAds.activeStatus"),
                "scrapePageAds.countryCode": run_input.get("scrapePageAds.countryCode"),
            },
            "apify": {
                "dataset_id": result["dataset_id"],
                "run_id": result["run"].get("id"),
                "startedAt": result["run"].get("startedAt"),
                "finishedAt": result["run"].get("finishedAt"),
                "status": result["run"].get("status"),
            },
            "counts": {
                "raw_items": result["raw_count"],
                "dedup_items": result["dedup_count"],
                "unique_advertisers": result["unique_advertisers"],
            },
            "files": {
                "raw_jsonl": str(raw_out),
                "dedup_jsonl": str(dedup_out),
                "summary_json": str(summary_out),
            },
            "dedupe_strategy": "ukey = advertiser(pageId|pageName) + ad(adArchiveId|adId|hash(snapshot/body/title/link/img))",
        }
        
        # Serialización segura con DateTimeEncoder
        summary_out.write_text(json.dumps(summary, cls=DateTimeEncoder, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"[{_now_iso()}] DONE ✅")
        print(f"  - RAW items:   {summary['counts']['raw_items']}")
        print(f"  - DEDUP items: {summary['counts']['dedup_items']}")
        print(f"  - Advertisers: {summary['counts']['unique_advertisers']}")
        print(f"  - Summary: {summary_out}")
        
        return summary

    except Exception as e:
        print(f"Error executing Apify actor: {e}")
        raise e


def main():
    run_apify_actor()


if __name__ == "__main__":
    main()
