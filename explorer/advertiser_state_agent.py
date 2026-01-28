#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explorer Agent 3 - Advertiser State Manager

Objetivo:
- Analizar el comportamiento de los anunciantes en cada run.
- Generar estadísticas (ads activos, señales usadas: COD, free shipping, video).
- Actualizar el estado del anunciante (New, Monitoring, Dormant, Winner?).
- Persistir historial diario/por run.
"""

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

DB_NAME = "product_memory.db"

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def now_iso() -> str:
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat()

def ensure_schema(conn: sqlite3.Connection):
    # Tabla para snapshots de estadisticas del anunciante por run
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS advertiser_run_stats (
        run_id TEXT,
        advertiser_id TEXT,
        total_ads INTEGER,
        ads_with_cod INTEGER,
        ads_with_free_shipping INTEGER,
        ads_with_video INTEGER,
        main_category TEXT,
        created_at TEXT,
        PRIMARY KEY (run_id, advertiser_id)
    );
    
    -- Estado actual y fechas clave (ya existe en memory_agent.py parcialmente, aqui expandimos)
    -- Si 'advertisers' ya existe, le agregamos columnas si faltan (manualmente o con alter, aqui asumimos tabla 'advertiser_state' separada para logica de agente)
    CREATE TABLE IF NOT EXISTS advertiser_state (
        advertiser_id TEXT PRIMARY KEY,
        current_status TEXT, -- 'new', 'monitoring', 'candidate_pool', 'dormant', 'winner'
        first_seen_at TEXT,
        last_seen_at TEXT,
        total_runs_seen INTEGER DEFAULT 0,
        last_run_id TEXT,
        notes TEXT,
        updated_at TEXT
    );
    
    CREATE INDEX IF NOT EXISTS idx_adv_run_stats_run ON advertiser_run_stats(run_id);
    CREATE INDEX IF NOT EXISTS idx_adv_state_status ON advertiser_state(current_status);
    """)
    conn.commit()

def compute_run_stats(conn: sqlite3.Connection, run_id: str):
    print(f"Calculando estadísticas para run {run_id}...")
    
    # 1. Obtener todos los ads del run (snapshots + extractions si existen)
    # Join con extractions para signals (si ya corrio Agent 2)
    # Join con ad_media para saber si es video (si media hash detecto algo video-like? Dificil. Mejor usar snapshots raw properties si se guardaron)
    # Por ahora usaremos signals de extractions si estan, sino snapshots raw del ingest.
    
    # Supongamos que memory_agent guardó snapshot.* en ad_snapshots. 
    # Pero ad_snapshots tiene campos limitados. 
    # Lo mejor es re-leer el ad_snapshots.signals_json de extractions si queremos precision de COD, etc.
    
    # Vamos a basarnos en ad_extractions que tiene signals_json (Agent 2 output).
    # Si no corrio Agent 2, stats seran 0.
    
    cur = conn.cursor()
    
    # Traer ads del run con sus signals
    cur.execute("""
        SELECT e.ad_id, e.signals_json, e.category, s.domain, a.advertiser_id
        FROM ad_extractions e
        JOIN ad_snapshots s ON s.ad_id = e.ad_id AND s.run_id = e.run_id
        JOIN ads a ON a.ad_id = e.ad_id
        WHERE e.run_id = ?
    """, (run_id,))
    
    rows = cur.fetchall()
    
    stats = {} # advertiser_id -> {metrics}

    for row in rows:
        ad_id, sig_json, cat, domain, adv_id = row
        signals = json.loads(sig_json) if sig_json else {}
        
        if adv_id not in stats:
            stats[adv_id] = {
                "total_ads": 0,
                "cod": 0,
                "free_shipping": 0,
                "video": 0, # Dificil sin analizar tipo, asumimos 0 por ahora o sacamos de otro lado
                "categories": {},
                # "page_name": page_name # Removed, managed by memory_agent
            }
        
        s = stats[adv_id]
        s["total_ads"] += 1
        if signals.get("cod"): s["cod"] += 1
        if signals.get("free_shipping") or signals.get("nationwide_shipping"): s["free_shipping"] += 1
        
        if cat:
            s["categories"][cat] = s["categories"].get(cat, 0) + 1

    # Persistir stats
    ts = now_iso()
    for adv_id, s in stats.items():
        # Main category
        main_cat = "Otros"
        if s["categories"]:
            main_cat = max(s["categories"], key=s["categories"].get)
            
        cur.execute("""
            INSERT OR REPLACE INTO advertiser_run_stats 
            (run_id, advertiser_id, total_ads, ads_with_cod, ads_with_free_shipping, ads_with_video, main_category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (run_id, adv_id, s["total_ads"], s["cod"], s["free_shipping"], s["video"], main_cat, ts))
        
        # Update State logic
        # Check current state
        cur.execute("SELECT current_status, total_runs_seen, first_seen_at FROM advertiser_state WHERE advertiser_id=?", (adv_id,))
        curr = cur.fetchone()
        
        new_status = "monitoring"
        total_runs = 1
        first_seen = ts
        
        if curr:
            old_status, runs_seen, fs = curr
            total_runs = (runs_seen or 0) + 1
            first_seen = fs or ts
            
            # Simple state machine
            if old_status == "new":
                new_status = "monitoring"
            elif old_status == "dormant":
                new_status = "monitoring" # Reactivated
            else:
                new_status = old_status # Keep existing (e.g. winner, blacklisted)
        else:
            new_status = "new"
            
        cur.execute("""
            INSERT INTO advertiser_state (advertiser_id, current_status, first_seen_at, last_seen_at, total_runs_seen, last_run_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(advertiser_id) DO UPDATE SET
                current_status=excluded.current_status,
                last_seen_at=excluded.last_seen_at,
                total_runs_seen=excluded.total_runs_seen,
                last_run_id=excluded.last_run_id,
                updated_at=excluded.updated_at
        """, (adv_id, new_status, first_seen, ts, total_runs, run_id, ts))
    
    conn.commit()
    print(f"Estadísticas calculadas para {len(stats)} anunciantes.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    
    conn = sqlite3.connect(str(get_db_path()))
    ensure_schema(conn)
    
    compute_run_stats(conn, args.run_id)
    
    conn.close()

if __name__ == "__main__":
    main()
