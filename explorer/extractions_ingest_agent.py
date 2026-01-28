#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
explorer/extractions_ingest_agent.py

Objetivo:
- Ingestar el enrichments (ads_enriched.jsonl) producido por el Agente 2 (Extractor).
- Guardar en la tabla `ad_extractions` de SQLite.
"""

import argparse
import json
import sqlite3
import os
from pathlib import Path

DB_NAME = "product_memory.db"

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True, help="ads_enriched.jsonl (salida del Agente 2)")
    args = parser.parse_args()

    # Resolver input path
    input_path = Path(args.input)
    if not input_path.exists():
        # Intentar relativo al run dir si no es absoluto
        root_dir = Path(__file__).resolve().parent
        potential_path = root_dir / "data" / "runs" / args.run_id / args.input
        if potential_path.exists():
            input_path = potential_path
        else:
            raise FileNotFoundError(f"Input file not found: {args.input}")

    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except:
                skipped += 1
                continue

            ad_id = str(row.get("ad_archive_id") or row.get("adArchiveId") or row.get("ad_id") or "")
            if not ad_id:
                skipped += 1
                continue

            # Extraer campos, manejando posibles discrepancias de nombres
            payload = (
                args.run_id,
                ad_id,
                row.get("product_name_guess"),
                row.get("category"),
                row.get("subcategory"),
                json.dumps(row.get("signals") or row.get("signals_json") or {}),
                json.dumps(row.get("evidence") or row.get("evidence_json") or {}),
                float(row.get("confidence") or 0.0),
            )

            try:
                cur.execute("""
                    INSERT INTO ad_extractions (run_id, ad_id, product_name_guess, category, subcategory, signals_json, evidence_json, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, ad_id) DO UPDATE SET
                      product_name_guess=excluded.product_name_guess,
                      category=excluded.category,
                      subcategory=excluded.subcategory,
                      signals_json=excluded.signals_json,
                      evidence_json=excluded.evidence_json,
                      confidence=excluded.confidence
                """, payload)
                updated += 1
            except Exception as e:
                # Log error opcional
                skipped += 1
                continue

    conn.commit()
    conn.close()

    print(json.dumps({
        "run_id": args.run_id,
        "updated": updated,
        "skipped": skipped
    }, indent=2))

if __name__ == "__main__":
    main()
