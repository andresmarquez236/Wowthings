#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explorer Agent 2.5 - Semantic Grouper
Uses OpenAI Embeddings + Agglomerative Clustering to unify product names.
"""

import argparse
import sqlite3
import json
import os
import numpy as np
import sys
from pathlib import Path
from typing import List, Dict, Tuple
from collections import Counter

# External libs (pip install scikit-learn numpy openai)
from sklearn.cluster import AgglomerativeClustering
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

DB_NAME = "product_memory.db"
EMBEDDING_MODEL = "text-embedding-3-small"
DISTANCE_THRESHOLD = 0.45  # Tunable: Lower = stricter, Higher = looser merging
BATCH_SIZE = 500

def get_db_path() -> Path:
    return Path(__file__).resolve().parent / "store" / DB_NAME

def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS semantic_map (
        run_id TEXT,
        original_name TEXT,
        cluster_id INTEGER,
        canonical_name TEXT,
        PRIMARY KEY (run_id, original_name)
    );
    """)
    conn.commit()

def get_embeddings(client: OpenAI, texts: List[str]) -> List[List[float]]:
    # OpenAI API limits batch size, let's chunk
    results = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        try:
            resp = client.embeddings.create(input=batch, model=EMBEDDING_MODEL)
            # Ensure order is preserved
            embeds = [d.embedding for d in resp.data]
            results.extend(embeds)
        except Exception as e:
            print(f"Error generating embeddings: {e}")
            # Fill with zeros or handle errors? For now, we skip or crash.
            # Ideally retry.
            raise e
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--threshold", type=float, default=DISTANCE_THRESHOLD)
    args = parser.parse_args()

    db_path = get_db_path()
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)
    cur = conn.cursor()

    print(f"--- Semantic Grouper for Run: {args.run_id} ---")

    # 1. Fetch unique product names
    cur.execute("""
        SELECT DISTINCT product_name_guess 
        FROM ad_extractions 
        WHERE run_id = ? 
          AND product_name_guess IS NOT NULL 
          AND product_name_guess != 'desconocido'
    """, (args.run_id,))
    
    unique_names = [r[0] for r in cur.fetchall()]
    
    if not unique_names:
        print("No product names found to group.")
        return

    print(f"Unique names to cluster: {len(unique_names)}")

    # 2. Generate Embeddings
    client = OpenAI()
    print("Generating embeddings...")
    embeddings = get_embeddings(client, unique_names)
    X = np.array(embeddings)
    
    # Normalize for Cosine Distance (Aglo uses Euclidean, but normalized vectors Euclidean ~ Cosine)
    # text-embedding-3-small is usually normalized, but let's ensure.
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    X = X / (norms + 1e-10)

    # 3. Clustering
    print(f"Clustering with threshold {args.threshold}...")
    # metric="euclidean", linkage="average" on normalized vectors is good
    model = AgglomerativeClustering(
        n_clusters=None,
        metric="euclidean",
        linkage="average",
        distance_threshold=args.threshold
    )
    labels = model.fit_predict(X)
    
    # 4. Determine Canonical Names per cluster
    # We want the shortest name that is decently descriptive, or the most frequent one?
    # Since we only have unique names here, we can't weight by frequency unless we query ad counts.
    # Let's simple heuristic: Shortest name that is > 3 chars (prefer "Zapato" over "Zapato super increible oferta").
    # Actually, we should probably fetch counts to pick the "Head" term.
    
    # Get counts for weighting
    cur.execute("""
        SELECT product_name_guess, COUNT(*) 
        FROM ad_extractions 
        WHERE run_id = ?
        GROUP BY product_name_guess
    """, (args.run_id,))
    counts = dict(cur.fetchall()) # name -> count

    clusters: Dict[int, List[str]] = {}
    for name, label in zip(unique_names, labels):
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(name)
        
    print(f"Clusters found: {len(clusters)}")

    # Prepare batch insert
    to_insert = []
    
    # Debug: Print some merges
    shown = 0

    for label, group in clusters.items():
        # Pick canonical: The one with highest frequency. Tie-break: shortest length.
        canonical = sorted(group, key=lambda n: (-counts.get(n, 0), len(n)))[0]
        
        if len(group) > 1 and shown < 10:
            print(f"Cluster {label} (Canonical: {canonical}): {group}")
            shown += 1
            
        for name in group:
            to_insert.append((args.run_id, name, int(label), canonical))

    # 5. Persist
    print("Persisting semantic map...")
    cur.executemany("""
        INSERT OR REPLACE INTO semantic_map (run_id, original_name, cluster_id, canonical_name)
        VALUES (?, ?, ?, ?)
    """, to_insert)
    
    conn.commit()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
