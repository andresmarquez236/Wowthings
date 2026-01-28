#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explorer Agent - Export Winners
Generates a Top Candidates file for Deep Analysis.
"""

import sqlite3
import json
import argparse
from pathlib import Path

DB_NAME = "product_memory.db"

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    conn = sqlite3.connect(str(get_db_path()))
    cur = conn.cursor()

    # Query for winners
    cur.execute(f"""
    SELECT
      p.product_id,
      p.canonical_name,
      p.category,
      p.subcategory,
      p.candidate_score,
      o.ads_count,
      o.advertisers_count,
      o.avg_confidence,
      p.signals_json,
      p.rationale_json
    FROM product_concepts p
    JOIN product_observations o ON o.product_id = p.product_id
    WHERE o.run_id = ?
      AND p.product_id <> 'unknown_cluster'
    ORDER BY p.candidate_score DESC, o.advertisers_count DESC, o.ads_count DESC
    LIMIT ?;
    """, (args.run_id, args.limit))

    rows = cur.fetchall()
    
    output_dir = Path(__file__).resolve().parent / "data" / "runs" / args.run_id
    output_file = output_dir / "top_winners.jsonl"
    
    count = 0
    with open(output_file, "w", encoding="utf-8") as f:
        for row in rows:
            prod_id, name, cat, subcat, score, ads, advs, conf, sigs, rat = row
            
            # Get some example ad_ids for deep analysis
            cur.execute("""
                SELECT ad_id FROM ad_to_product 
                WHERE run_id=? AND product_id=? 
                LIMIT 5
            """, (args.run_id, prod_id))
            sample_ads = [r[0] for r in cur.fetchall()]

            item = {
                "product_group_id": prod_id,
                "normalized_name": name,
                "category": cat,
                "subcategory": subcat,
                "score_total": float(score),
                "metrics": {
                    "advertiser_count": advs,
                    "ad_count": ads,
                    "median_confidence": float(conf)
                },
                "signals": json.loads(sigs) if sigs else {},
                "rationale": json.loads(rat) if rat else {},
                "sample_ad_archive_ids": sample_ads
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
            
    print(f"Exported {count} winners to: {output_file}")
    conn.close()

if __name__ == "__main__":
    main()
