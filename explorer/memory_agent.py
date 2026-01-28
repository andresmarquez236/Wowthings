#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
explorer/memory_agent.py

Objetivo:
- Ingestar resultados de scraping (summary + jsonl) en SQLite.
- Mantener trazabilidad de Anunciantes y Anuncios.
- Generar snapshots para detectar cambios.

Uso:
  python explorer/memory_agent.py --run-id <TIMESTAMP_RUN_ID>
  Si no se pasa run-id, intenta tomar el último de explorer/data/runs/
"""

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import setup_logger

logger = setup_logger("Explorer_Memory")

# =============================
# CONFIG
# =============================

DB_NAME = "product_memory.db"

# =============================
# Schema Definition
# =============================

SCHEMA_SQL = """
-- 1) runs
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    timestamp TEXT,
    queries_loaded INTEGER,
    raw_count INTEGER,
    dedup_count INTEGER,
    unique_advertisers INTEGER,
    apify_run TEXT,
    config_json TEXT
);

-- 2) advertisers (Current State)
CREATE TABLE IF NOT EXISTS advertisers (
    advertiser_id TEXT PRIMARY KEY, -- page_id
    current_page_name TEXT,
    current_profile_uri TEXT,
    current_like_count INTEGER,
    current_categories_json TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    status TEXT -- 'active', 'dormant'
);

-- 3) advertiser_profile_history (SCD)
CREATE TABLE IF NOT EXISTS advertiser_profile_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    advertiser_id TEXT,
    observed_at TEXT,
    page_name TEXT,
    profile_uri TEXT,
    like_count INTEGER,
    categories_json TEXT,
    FOREIGN KEY(advertiser_id) REFERENCES advertisers(advertiser_id)
);

-- 4) ads (Current State)
CREATE TABLE IF NOT EXISTS ads (
    ad_id TEXT PRIMARY KEY, -- ad_archive_id
    advertiser_id TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    current_is_active BOOLEAN,
    current_link_url TEXT,
    current_domain TEXT,
    current_body_hash TEXT,
    current_media_hash TEXT,
    FOREIGN KEY(advertiser_id) REFERENCES advertisers(advertiser_id)
);

-- 5) ad_snapshots (One per run per ad)
CREATE TABLE IF NOT EXISTS ad_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    ad_id TEXT,
    observed_at TEXT,
    is_active BOOLEAN,
    start_date TEXT,
    end_date TEXT,
    link_url TEXT,
    domain TEXT,
    title TEXT,
    body_text TEXT,
    cta_type TEXT,
    publisher_platform_json TEXT,
    _query_matched TEXT,
    _intent_guess TEXT,
    snapshot_hash TEXT,
    FOREIGN KEY(run_id) REFERENCES runs(run_id),
    FOREIGN KEY(ad_id) REFERENCES ads(ad_id),
    UNIQUE(run_id, ad_id)
);

-- 6) ad_extractions (Placeholder for Agent 2 output)
CREATE TABLE IF NOT EXISTS ad_extractions (
    run_id TEXT,
    ad_id TEXT,
    product_name_guess TEXT,
    category TEXT,
    subcategory TEXT,
    signals_json TEXT,
    evidence_json TEXT,
    confidence REAL,
    UNIQUE(run_id, ad_id)
);

-- Indices
CREATE INDEX IF NOT EXISTS idx_adv_last_seen ON advertisers(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_ads_adv_last_seen ON ads(advertiser_id, last_seen_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_domain ON ad_snapshots(domain);

-- 7) image_cache (global cache by URL)
CREATE TABLE IF NOT EXISTS image_cache (
    image_url TEXT PRIMARY KEY,
    dhash64 TEXT,
    fetched_at TEXT
);

-- 8) ad_media (traceability per run/ad)
CREATE TABLE IF NOT EXISTS ad_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    ad_id TEXT,
    image_url TEXT,
    dhash64 TEXT,
    created_at TEXT,
    UNIQUE(run_id, ad_id, image_url)
);

CREATE INDEX IF NOT EXISTS idx_image_cache_hash ON image_cache(dhash64);
CREATE INDEX IF NOT EXISTS idx_ad_media_ad ON ad_media(ad_id);
CREATE INDEX IF NOT EXISTS idx_ad_media_hash ON ad_media(dhash64);
"""

# =============================
# Helpers
# =============================

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def init_db():
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript(SCHEMA_SQL)
    conn.close()
    return db_path

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
        if val is not None:
            return val
    return None

def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return str(s).strip()

def compute_snapshot_hash(item: Dict) -> str:
    parts = [
        first_present(item, ["snapshot.body.text", "snapshot.body"]),
        first_present(item, ["snapshot.title"]),
        first_present(item, ["snapshot.link_url"]),
        first_present(item, ["snapshot.images.0.original_image_url", "snapshot.videos.0.video_preview_image_url"])
    ]
    payload = "|".join([normalize_text(p) for p in parts])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()

def extract_domain(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except:
        return ""

# =============================
# Ingestion Logic
# =============================

def ingest_run(run_id: str, summary_path: Path, conn: sqlite3.Connection) -> Dict:
    with open(summary_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Check if exists
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,))
    if cur.fetchone():
        logger.warning(f"Run {run_id} ya existe en DB. Saltando inserción de metadata.")
        return data
    
    cur.execute("""
        INSERT INTO runs (run_id, timestamp, queries_loaded, raw_count, dedup_count, unique_advertisers, apify_run, config_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        run_id,
        data.get("timestamp"),
        data.get("queries_loaded"),
        data.get("raw_count"),
        data.get("dedup_count"),
        data.get("unique_advertisers"),
        data.get("apify_run"),
        json.dumps(data.get("params", {})) # params might not comprise full config in current summary logic
    ))
    conn.commit()
    logger.info(f"Run {run_id} registrado.")
    return data

def ingest_ads(run_id: str, jsonl_path: Path, conn: sqlite3.Connection):
    cur = conn.cursor()
    
    stats = {
        "new_advertisers": 0,
        "updated_advertisers": 0,
        "new_ads": 0,
        "updated_ads": 0,
        "snapshots": 0
    }
    
    timestamp_now = datetime.utcnow().isoformat() + "Z"
    
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            
            try:
                ad_data = json.loads(line)
            except:
                continue
                
            # --- 1. Advertiser ---
            page_id = first_present(ad_data, ["pageId", "pageID", "page_id"])
            if not page_id:
                # Fallback weird cases
                page_id = "unknown_" + first_present(ad_data, ["pageName", "page_name", "advertiser.name"], "")
            
            page_name = first_present(ad_data, ["snapshot.page_name", "page_name", "pageName", "advertiser.name"])
            profile_uri = first_present(ad_data, ["snapshot.page_profile_uri", "page_profile_uri"])
            like_count = first_present(ad_data, ["snapshot.page_like_count", "pageLikeCount", "page_like_count"])
            cats = first_present(ad_data, ["snapshot.page_categories", "pageCategories", "page_categories"])
            cats_json = json.dumps(cats) if cats else None
            
            # Check exist
            cur.execute("SELECT current_page_name, current_profile_uri FROM advertisers WHERE advertiser_id=?", (page_id,))
            row = cur.fetchone()
            
            if not row:
                # Insert New
                cur.execute("""
                    INSERT INTO advertisers (advertiser_id, current_page_name, current_profile_uri, current_like_count, current_categories_json, first_seen_at, last_seen_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'active')
                """, (page_id, page_name, profile_uri, like_count, cats_json, timestamp_now, timestamp_now))
                stats["new_advertisers"] += 1
                
                # History init
                cur.execute("""
                    INSERT INTO advertiser_profile_history (advertiser_id, observed_at, page_name, profile_uri, like_count, categories_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (page_id, timestamp_now, page_name, profile_uri, like_count, cats_json))
                
            else:
                # Update existing
                current_name, current_uri = row
                stats["updated_advertisers"] += 1
                cur.execute("UPDATE advertisers SET last_seen_at=?, status='active' WHERE advertiser_id=?", (timestamp_now, page_id))
                
                # Check changes for history
                if (page_name and page_name != current_name) or (profile_uri and profile_uri != current_uri):
                    cur.execute("""
                        UPDATE advertisers SET current_page_name=?, current_profile_uri=?, current_like_count=?, current_categories_json=?
                        WHERE advertiser_id=?
                    """, (page_name, profile_uri, like_count, cats_json, page_id))
                    
                    cur.execute("""
                        INSERT INTO advertiser_profile_history (advertiser_id, observed_at, page_name, profile_uri, like_count, categories_json)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (page_id, timestamp_now, page_name, profile_uri, like_count, cats_json))

            # --- 2. Ads ---
            ad_archive_id = str(first_present(ad_data, ["adArchiveId", "adArchiveID", "ad_archive_id"]))
            # Basic fields
            is_active = first_present(ad_data, ["isActive", "is_active"])
            start_date = first_present(ad_data, ["startDate", "start_date"])
            end_date = first_present(ad_data, ["endDate", "end_date"])
            link_url = first_present(ad_data, ["snapshot.link_url"])
            domain = extract_domain(link_url)
            body_text = first_present(ad_data, ["snapshot.body.text"])
            
            snapshot_hash = compute_snapshot_hash(ad_data) # Hash for visual/content changes
            
            cur.execute("SELECT current_body_hash FROM ads WHERE ad_id=?", (ad_archive_id,))
            ad_row = cur.fetchone()
            
            if not ad_row:
                cur.execute("""
                    INSERT INTO ads (ad_id, advertiser_id, first_seen_at, last_seen_at, current_is_active, current_link_url, current_domain, current_body_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (ad_archive_id, page_id, timestamp_now, timestamp_now, is_active, link_url, domain, snapshot_hash))
                stats["new_ads"] += 1
            else:
                cur.execute("""
                    UPDATE ads SET last_seen_at=?, current_is_active=?, current_link_url=?, current_domain=?, current_body_hash=?
                    WHERE ad_id=?
                """, (timestamp_now, is_active, link_url, domain, snapshot_hash, ad_archive_id))
                stats["updated_ads"] += 1

            # --- 3. Snapshot ---
            # Try/Except for UNIQUE constraint (run_id, ad_id) just in case dedup failed or rerun
            try:
                cur.execute("""
                    INSERT INTO ad_snapshots (
                        run_id, ad_id, observed_at, is_active, start_date, end_date, 
                        link_url, domain, title, body_text, cta_type, 
                        _query_matched, _intent_guess, snapshot_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    run_id, ad_archive_id, timestamp_now, is_active, start_date, end_date,
                    link_url, domain, 
                    first_present(ad_data, ["snapshot.title"]),
                    body_text,
                    first_present(ad_data, ["snapshot.cta_type"]),
                    ad_data.get("_query_matched"),
                    ad_data.get("_intent_guess"),
                    snapshot_hash
                ))
                stats["snapshots"] += 1
            except sqlite3.IntegrityError:
                pass # Already ingested for this run

            # Commit batching could be optimized, but row-by-row for safety is fine for <5k rows
            
    conn.commit()
    return stats

def update_advertiser_status(conn: sqlite3.Connection):
    """
    Mark dormant if not seen in 7 days
    """
    seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
    conn.execute("""
        UPDATE advertisers 
        SET status = 'dormant' 
        WHERE last_seen_at < ? AND status = 'active'
    """, (seven_days_ago,))
    conn.commit()

# =============================
# Main
# =============================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", help="ID del run a ingestar (nombre de carpeta timestamp)", default=None)
    args = parser.parse_args()
    
    root_dir = Path(__file__).resolve().parent
    runs_dir = root_dir / "data" / "runs"
    
    run_id = args.run_id
    if not run_id:
        # Get latest
        all_runs = sorted([d.name for d in runs_dir.iterdir() if d.is_dir()])
        if not all_runs:
            logger.error("No hay runs disponibles en data/runs/")
            return
        run_id = all_runs[-1]
    
    run_dir = runs_dir / run_id
    if not run_dir.exists():
        logger.error(f"Run dir no existe: {run_dir}")
        return
        
    summary_path = run_dir / "summary.json"
    dedup_path = run_dir / "dedup_ads.jsonl"
    
    if not summary_path.exists() or not dedup_path.exists():
        logger.error("Faltan archivos summary.json o dedup_ads.jsonl en el run dir")
        return

    logger.info(f"Iniciando ingesta para Run ID: {run_id}")
    
    db_path = init_db()
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Ingest
        ingest_run(run_id, summary_path, conn)
        stats = ingest_ads(run_id, dedup_path, conn)
        update_advertiser_status(conn)
        
        logger.info("Ingesta completada exitosamente.")
        logger.info(f"Estadísticas: {json.dumps(stats, indent=2)}")
        
        # Save ingest report
        report = {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "run_id": run_id,
            "stats": stats
        }
        with open(run_dir / "ingest_report.json", "w") as f:
            json.dump(report, f, indent=2)
            
    except Exception as e:
        logger.error(f"Error durante la ingesta: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
