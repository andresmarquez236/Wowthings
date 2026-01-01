# wow_agent/ads_generator/fix_format.py
# -*- coding: utf-8 -*-

import json
from typing import Any, Dict, List


def load_market_research_min(path: str) -> Dict[str, Any]:
    """Carga el JSON de market_research_min.json"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_context_and_first_three_angles(
    market: Dict[str, Any],
    *,
    max_hooks_per_angle: int = 12,
    keep_evidence: bool = True
) -> Dict[str, Any]:
    """
    Extrae un payload compacto:
    {
      "context": {...},
      "angles": [
        { rank:1, ... hooks:[...] },
        { rank:2, ... hooks:[...] },
        { rank:3, ... hooks:[...] }
      ]
    }
    """

    required = ["input", "resumen_corto", "product_fingerprint", "top_5_angulos", "hooks_por_angulo"]
    missing = [k for k in required if k not in market]
    if missing:
        raise ValueError(f"market_research_min.json no tiene keys requeridas: {missing}")

    # Contexto compacto (sin inflar tokens)
    product_fp = market.get("product_fingerprint", {}) or {}
    context = {
        "meta": market.get("meta", {}),
        "input": market["input"],  # nombre_producto, descripcion, precio, garantia
        "resumen_corto": market.get("resumen_corto", {}),
        "product_fingerprint": {
            # keys típicas útiles para no alucinar
            "product_description_lock": product_fp.get("product_description_lock", ""),
            "nonnegotiables": product_fp.get("nonnegotiables", []),
            "forbidden_claims": product_fp.get("forbidden_claims", []),
        },
        "estacionalidad": market.get("estacionalidad", {}),
        "score_total": market.get("score_total", {}),
        "missing_data_flags": market.get("missing_data_flags", []),
    }

    # hooks_map rank -> hooks
    hooks_map: Dict[int, List[str]] = {}
    for h in (market.get("hooks_por_angulo", []) or []):
        r = h.get("rank_angulo")
        if isinstance(r, int):
            hooks_map[r] = (h.get("hooks") or [])

    angles_raw = market.get("top_5_angulos", []) or []

    def pick_angle(rank: int) -> Dict[str, Any]:
        for a in angles_raw:
            if a.get("rank") == rank:
                return a
        idx = rank - 1
        if 0 <= idx < len(angles_raw):
            return angles_raw[idx]
        raise ValueError(f"No se encontró ángulo para rank={rank}")

    angles: List[Dict[str, Any]] = []
    for r in (1, 2, 3):
        a = pick_angle(r)

        angle_item = {
            "rank": r,
            "angulo": a.get("angulo", ""),
            "score_0a10": a.get("score_0a10", None),
            "buyer_persona": a.get("buyer_persona", ""),
            "promesa": a.get("promesa", ""),
            "objecion_principal": a.get("objecion_principal", ""),
            "justificacion": a.get("justificacion", ""),
            "confianza": a.get("confianza", ""),
            "hooks": (hooks_map.get(r, [])[:max_hooks_per_angle]),
            "evidencia": (a.get("evidencia", []) if keep_evidence else []),
        }
        angles.append(angle_item)

    return {"context": context, "angles": angles}
