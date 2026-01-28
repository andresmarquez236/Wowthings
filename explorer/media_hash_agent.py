#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
explorer/media_hash_agent.py

Objetivo:
- Calcular hash visual (dHash 64-bit) de las imágenes de los ads.
- Usa cache global (image_cache) y multi-threading para performance.
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import requests
from PIL import Image

DB_NAME = "product_memory.db"

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS image_cache (
        image_url TEXT PRIMARY KEY,
        dhash64 TEXT,
        fetched_at TEXT
    );

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
    """)
    conn.commit()

def extract_image_urls(ad: Dict[str, Any], max_images: int = 1) -> List[str]:
    snap = ad.get("snapshot") or {}
    urls: List[str] = []

    def add(u: Optional[str]):
        if u and u not in urls:
            urls.append(u)

    # 1) Imágenes (IMAGE)
    for it in (snap.get("images") or []):
        add(it.get("resized_image_url") or it.get("original_image_url"))
        if len(urls) >= max_images:
            return urls

    for it in (snap.get("extra_images") or []):
        add(it.get("resized_image_url") or it.get("original_image_url"))
        if len(urls) >= max_images:
            return urls

    # 2) Videos (VIDEO) → thumbnail
    for v in (snap.get("videos") or []):
        add(v.get("video_preview_image_url"))
        if len(urls) >= max_images:
            return urls

    for v in (snap.get("extra_videos") or []):
        add(v.get("video_preview_image_url"))
        if len(urls) >= max_images:
            return urls

    # 3) Carruseles/DCO (cards) → imagen o thumbnail
    for c in (snap.get("cards") or []):
        add(c.get("resized_image_url") or c.get("original_image_url") or c.get("video_preview_image_url"))
        if len(urls) >= max_images:
            return urls

    return urls

def dhash64(img: Image.Image) -> str:
    # dHash: grayscale, resize 9x8, compare adjacent pixels
    img = img.convert("L").resize((9, 8), Image.Resampling.LANCZOS)
    pixels = list(img.getdata())  # 72 values
    # reshape into rows
    rows = [pixels[i*9:(i+1)*9] for i in range(8)]
    bits = 0
    for r in rows:
        for c in range(8):
            bits = (bits << 1) | (1 if r[c+1] > r[c] else 0)
    return f"{bits:016x}"

def fetch_and_hash(url: str, timeout: int, retries: int) -> Optional[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    last_err = None
    for _ in range(max(1, retries)):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code != 200 or not r.content:
                last_err = f"HTTP {r.status_code}"
                continue
            img = Image.open(BytesIO(r.content))
            return dhash64(img)
        except Exception as e:
            last_err = str(e)
            continue
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dedup-path", default=None)
    parser.add_argument("--max-images", type=int, default=1)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()

    root_dir = Path(__file__).resolve().parent
    dedup_path = Path(args.dedup_path) if args.dedup_path else (root_dir / "data" / "runs" / args.run_id / "dedup_ads.jsonl")
    if not dedup_path.exists():
        raise FileNotFoundError(f"No existe: {dedup_path}")

    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)
    cur = conn.cursor()

    # Use timezone-aware now
    from datetime import timezone
    created_at = datetime.now(timezone.utc).isoformat()

    # Pre-carga cache de URLs ya conocidas para evitar hits a DB por cada fila
    cur.execute("SELECT image_url, dhash64 FROM image_cache")
    cache_map = {u: h for (u, h) in cur.fetchall()}

    tasks: List[Tuple[str, str]] = []  # (ad_id, image_url)
    skipped_no_img = 0
    skipped_no_adid = 0
    already_run = 0

    # Build task list
    with open(dedup_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                ad = json.loads(line)
            except:
                continue

            ad_id = str(ad.get("ad_archive_id") or ad.get("adArchiveId") or ad.get("adArchiveID") or "")
            if not ad_id:
                skipped_no_adid += 1
                continue

            urls = extract_image_urls(ad, max_images=args.max_images)
            if not urls:
                skipped_no_img += 1
                continue

            for url in urls:
                # ya existe para este run?
                cur.execute("SELECT 1 FROM ad_media WHERE run_id=? AND ad_id=? AND image_url=?", (args.run_id, ad_id, url))
                if cur.fetchone():
                    already_run += 1
                    continue
                tasks.append((ad_id, url))

    # Worker execution (I/O bound)
    inserted = 0
    cached_used = 0
    downloaded = 0
    failed = 0

    def worker(ad_id: str, url: str) -> Tuple[str, str, Optional[str]]:
        # retorna hash si pudo, None si falló
        if url in cache_map and cache_map[url]:
            return (ad_id, url, cache_map[url])
        h = fetch_and_hash(url, timeout=args.timeout, retries=args.retries)
        return (ad_id, url, h)
    
    # Batch commit param
    BATCH_SIZE = 100
    pending_commits = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = [ex.submit(worker, ad_id, url) for (ad_id, url) in tasks]
        for fut in as_completed(futures):
            ad_id, url, h = fut.result()
            if not h:
                failed += 1
                continue

            # actualizar cache si era nuevo
            if url in cache_map:
                cached_used += 1
            else:
                downloaded += 1
                cache_map[url] = h
                cur.execute("""
                    INSERT OR REPLACE INTO image_cache (image_url, dhash64, fetched_at)
                    VALUES (?, ?, ?)
                """, (url, h, created_at))

            # insertar trazabilidad run/ad
            cur.execute("""
                INSERT OR IGNORE INTO ad_media (run_id, ad_id, image_url, dhash64, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (args.run_id, ad_id, url, h, created_at))
            inserted += 1
            
            pending_commits += 1
            if pending_commits >= BATCH_SIZE:
                conn.commit()
                pending_commits = 0

    conn.commit()
    conn.close()

    print(json.dumps({
        "run_id": args.run_id,
        "tasks_total": len(tasks),
        "inserted": inserted,
        "cache_hits": cached_used,
        "downloaded": downloaded,
        "failed": failed,
        "skipped_no_img": skipped_no_img,
        "skipped_no_adid": skipped_no_adid,
        "already_run": already_run
    }, indent=2))

if __name__ == "__main__":
    main()
