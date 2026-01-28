#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import json
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

DB_NAME = "product_memory.db"

WEIGHTS = {
    "cod": 0.12,
    "free_shipping": 0.06,
    "nationwide_shipping": 0.05,
    "whatsapp_cta": 0.06,
    "discount_offer": 0.07,
    "urgency": 0.05,
    "guarantee_trust": 0.03,
    "cash_price": 0.02,
}

def now_iso() -> str:
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat()

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS product_concepts (
      product_id TEXT PRIMARY KEY,
      canonical_name TEXT,
      category TEXT,
      subcategory TEXT,
      signals_json TEXT,
      rationale_json TEXT,
      candidate_score REAL,
      first_seen_at TEXT,
      last_seen_at TEXT
    );

    CREATE TABLE IF NOT EXISTS product_observations (
      run_id TEXT,
      product_id TEXT,
      ads_count INTEGER,
      advertisers_count INTEGER,
      avg_confidence REAL,
      created_at TEXT,
      PRIMARY KEY (run_id, product_id)
    );

    CREATE TABLE IF NOT EXISTS ad_to_product (
      run_id TEXT,
      ad_id TEXT,
      product_id TEXT,
      advertiser_id TEXT,
      match_basis TEXT,
      confidence REAL,
      created_at TEXT,
      PRIMARY KEY (run_id, ad_id)
    );

    CREATE TABLE IF NOT EXISTS advertiser_product_state (
      advertiser_id TEXT,
      product_id TEXT,
      first_seen_at TEXT,
      last_seen_at TEXT,
      last_run_id TEXT,
      status TEXT,
      PRIMARY KEY (advertiser_id, product_id)
    );

    CREATE INDEX IF NOT EXISTS idx_prod_obs_run ON product_observations(run_id);
    CREATE INDEX IF NOT EXISTS idx_ad_to_product_prod ON ad_to_product(product_id);
    CREATE INDEX IF NOT EXISTS idx_adv_prod_last ON advertiser_product_state(last_seen_at);
    """)
    conn.commit()

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

STOP = {"de","la","el","los","las","para","con","sin","y","o","a","en","por","un","una","unos","unas","del","al"}

def normalize_product_name(name: str) -> str:
    s = strip_accents((name or "").lower()).strip()
    s = s.replace("$", " ").replace("%", " ")
    # quitar números sueltos/promos
    s = "".join(ch if (ch.isalpha() or ch.isspace()) else " " for ch in s)
    toks = [t for t in s.split() if len(t) > 2 and t not in STOP]
    return " ".join(toks) if toks else "desconocido"

def stable_product_id(norm_name: str) -> str:
    # ID estable basado SOLO en nombre normalizado (para cruzar categorias mal clasificadas)
    # Si norm_name es "desconocido", deberia manejarse antes (por hash o ignorar)
    base = norm_name.strip()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def compute_candidate_score(avg_conf: float, signals: Dict[str, bool]) -> Tuple[float, list]:
    score = float(avg_conf or 0.0)
    reasons = []
    for k, w in WEIGHTS.items():
        if signals.get(k) is True:
            score += w
            reasons.append(k)
    if score > 1.0:
        score = 1.0
    return score, reasons

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    args = ap.parse_args()

    conn = sqlite3.connect(str(get_db_path()))
    ensure_schema(conn)
    cur = conn.cursor()

    run_id = args.run_id
    created_at = now_iso()

    # 1) Cargar extracciones
    cur.execute("""
      SELECT ad_id, product_name_guess, category, subcategory, signals_json, evidence_json, confidence
      FROM ad_extractions
      WHERE run_id=?
    """, (run_id,))
    
    rows = cur.fetchall()
    if not rows:
        print(f"No hay ad_extractions para run_id={run_id}. Primero ingesta Agent 2.")
        return

    extr = {row[0]: {
        "product_name_guess": row[1] or "desconocido",
        "category": row[2] or "Otros",
        "subcategory": row[3] or "Otros",
        "signals": json.loads(row[4]) if row[4] else {},
        "evidence": json.loads(row[5]) if row[5] else {},
        "confidence": float(row[6] or 0.0),
    } for row in rows}

    # 2) advertiser_id por ad_id (desde snapshots del run)
    cur.execute("""
      SELECT s.ad_id, a.advertiser_id, s.observed_at
      FROM ad_snapshots s
      JOIN ads a ON a.ad_id = s.ad_id
      WHERE s.run_id=?
    """, (run_id,))
    ad_meta = {row[0]: {"advertiser_id": row[1], "observed_at": row[2]} for row in cur.fetchall()}

    # 3) dhash por ad_id (si existe)
    cur.execute("SELECT ad_id, dhash64 FROM ad_media WHERE run_id=?", (run_id,))
    ad_hash = {}
    for ad_id, dh in cur.fetchall():
        # si hay varios, nos quedamos con el primero que llegue
        if ad_id not in ad_hash and dh:
            ad_hash[ad_id] = dh

    # 3.5) Load Semantic Map (if exists)
    sem_map = {} # name -> (cluster_id, canonical_name)
    try:
        cur.execute("SELECT original_name, cluster_id, canonical_name FROM semantic_map WHERE run_id=?", (run_id,))
        for row in cur.fetchall():
            sem_map[row[0]] = (row[1], row[2])
    except sqlite3.OperationalError:
        # Table might not exist if semantic_grouper wasn't run
        pass

    # 4) Agregación
    agg = {}
    for ad_id, e in extr.items():
        meta = ad_meta.get(ad_id)
        if not meta:
            continue

        norm_name = normalize_product_name(e["product_name_guess"])
        raw_name = e["product_name_guess"]
        
        # Lógica de agrupación HÍBRIDA v2.0
        # 1. Visual Hash (strongest)
        if ad_id in ad_hash:
            product_id = f"vhash_{ad_hash[ad_id]}"
        # 2. Semantic Cluster (new)
        elif raw_name in sem_map:
            cid, canon = sem_map[raw_name]
            product_id = f"sem_{cid}"
            # Upgrade the name for the counter to align with semantic canonical
            e["product_name_guess"] = canon 
        # 3. Fallback: Text Normalization
        elif norm_name != "desconocido":
            product_id = f"text_{stable_product_id(norm_name)}"
        # 4. Unknown
        else:
            product_id = "unknown_cluster"

        if product_id not in agg:
            agg[product_id] = {
                "name_counter": Counter(),
                "cat_counter": Counter(),
                "sub_counter": Counter(),
                "signals_or": defaultdict(bool),
                "conf_sum": 0.0,
                "conf_n": 0,
                "ads_count": 0,
                "advertisers": set(),
                "first_seen": meta["observed_at"],
                "last_seen": meta["observed_at"],
                "evidence_samples": defaultdict(list),
            }

        g = agg[product_id]
        g["name_counter"][e["product_name_guess"]] += 1
        g["cat_counter"][e["category"]] += 1
        g["sub_counter"][e["subcategory"]] += 1
        for k, v in (e["signals"] or {}).items():
            if v is True:
                g["signals_or"][k] = True
                # guardar 1-2 evidencias por señal
                spans = (e["evidence"] or {}).get(k) or []
                for sp in spans[:2]:
                    if sp and sp not in g["evidence_samples"][k]:
                        g["evidence_samples"][k].append(sp)

        g["conf_sum"] += float(e["confidence"] or 0.0)
        g["conf_n"] += 1
        g["ads_count"] += 1
        g["advertisers"].add(meta["advertiser_id"])
        
        # Guard against None dates
        obs_at = meta["observed_at"] or created_at
        if g["first_seen"] is None: g["first_seen"] = obs_at
        if g["last_seen"] is None: g["last_seen"] = obs_at
        
        if obs_at < g["first_seen"]:
            g["first_seen"] = obs_at
        if obs_at > g["last_seen"]:
            g["last_seen"] = obs_at

    # 5) Persistir: product_concepts + observations + mappings + advertiser_product_state
    concepts_upserted = 0
    obs_upserted = 0
    mappings_upserted = 0
    adv_states_upserted = 0

    for product_id, g in agg.items():
        canonical_name = g["name_counter"].most_common(1)[0][0]
        category = g["cat_counter"].most_common(1)[0][0]
        subcategory = g["sub_counter"].most_common(1)[0][0]
        signals = {k: bool(v) for k, v in g["signals_or"].items()}
        avg_conf = (g["conf_sum"] / g["conf_n"]) if g["conf_n"] else 0.0
        candidate_score, reasons = compute_candidate_score(avg_conf, signals)

        rationale = {
            "reasons": reasons,
            "evidence": {k: v for k, v in g["evidence_samples"].items()},
            "avg_confidence": avg_conf,
            "ads_count": g["ads_count"],
            "advertisers_count": len(g["advertisers"]),
        }

        # upsert product_concepts
        cur.execute("SELECT first_seen_at FROM product_concepts WHERE product_id=?", (product_id,))
        row = cur.fetchone()
        first_seen = g["first_seen"]
        if row and row[0]:
            first_seen = min(row[0], first_seen)

        cur.execute("""
          INSERT INTO product_concepts (
            product_id, canonical_name, category, subcategory,
            signals_json, rationale_json, candidate_score,
            first_seen_at, last_seen_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(product_id) DO UPDATE SET
            canonical_name=excluded.canonical_name,
            category=excluded.category,
            subcategory=excluded.subcategory,
            signals_json=excluded.signals_json,
            rationale_json=excluded.rationale_json,
            candidate_score=excluded.candidate_score,
            last_seen_at=excluded.last_seen_at
        """, (
            product_id, canonical_name, category, subcategory,
            json.dumps(signals, ensure_ascii=False),
            json.dumps(rationale, ensure_ascii=False),
            float(candidate_score),
            first_seen, g["last_seen"]
        ))
        concepts_upserted += 1

        # observation por run
        cur.execute("""
          INSERT INTO product_observations (
            run_id, product_id, ads_count, advertisers_count, avg_confidence, created_at
          ) VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(run_id, product_id) DO UPDATE SET
            ads_count=excluded.ads_count,
            advertisers_count=excluded.advertisers_count,
            avg_confidence=excluded.avg_confidence
        """, (
            run_id, product_id, g["ads_count"], len(g["advertisers"]), float(avg_conf), created_at
        ))
        obs_upserted += 1

        # advertiser_product_state
        for adv_id in g["advertisers"]:
            cur.execute("SELECT first_seen_at FROM advertiser_product_state WHERE advertiser_id=? AND product_id=?", (adv_id, product_id))
            r = cur.fetchone()
            adv_first = g["first_seen"]
            if r and r[0]:
                adv_first = min(r[0], adv_first)

            cur.execute("""
              INSERT INTO advertiser_product_state (
                advertiser_id, product_id, first_seen_at, last_seen_at, last_run_id, status
              ) VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(advertiser_id, product_id) DO UPDATE SET
                last_seen_at=excluded.last_seen_at,
                last_run_id=excluded.last_run_id,
                status=excluded.status
            """, (adv_id, product_id, adv_first, g["last_seen"], run_id, "active"))
            adv_states_upserted += 1

    # ad_to_product mapping (por ad)
    for ad_id, e in extr.items():
        meta = ad_meta.get(ad_id)
        if not meta:
            continue
        norm_name = normalize_product_name(e["product_name_guess"])
        
        # Misma logica hibrida
        if ad_id in ad_hash:
            product_id = f"vhash_{ad_hash[ad_id]}"
            basis = "video_hash"
        elif norm_name != "desconocido":
            product_id = f"text_{stable_product_id(norm_name)}"
            basis = "text_name"
        else:
            product_id = "unknown_cluster"
            basis = "unknown"

        cur.execute("""
          INSERT INTO ad_to_product (run_id, ad_id, product_id, advertiser_id, match_basis, confidence, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(run_id, ad_id) DO UPDATE SET
            product_id=excluded.product_id,
            advertiser_id=excluded.advertiser_id,
            match_basis=excluded.match_basis,
            confidence=excluded.confidence
        """, (run_id, ad_id, product_id, meta["advertiser_id"], basis, float(e["confidence"] or 0.0), created_at))
        mappings_upserted += 1

    conn.commit()
    conn.close()

    print(json.dumps({
        "run_id": run_id,
        "products_total": len(agg),
        "concepts_upserted": concepts_upserted,
        "observations_upserted": obs_upserted,
        "mappings_upserted": mappings_upserted,
        "adv_states_upserted": adv_states_upserted,
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
