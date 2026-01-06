#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fblibrary_postprocess_machine_report.py

Objetivo (automatizable, sin dependencias externas):
- Leer:
  1) output/<product_folder>/apify_results/fblibrary_ads_dedup_<product_folder>.jsonl
  2) output/<product_folder>/querys_fblibrary_<product_folder>.json
- Aplicar filtros (NUEVOS):
  A) LANG_ALLOWED (default "es")  -> conservar anuncios en el idioma permitido
  B) REQUIRE_PRODUCT_NAME_CONTAINED -> conservar anuncios donde el "nombre núcleo" del producto esté contenido
- Generar DOS archivos:
  1) output/<product_folder>/fblibrary_competition_report_<product_folder>.json
     (reporte machine-friendly completo para responder reglas + alimentar agente)
  2) output/<product_folder>/fblibrary_advertisers_rank_<product_folder>.json
     (ranking simple de anunciantes, mayor a menor por #ads, con flag de escalamiento)

Notas:
- "EXACTAMENTE el mismo producto" no se puede garantizar con texto únicamente. Este script:
  (a) filtra / etiqueta candidatos con heurísticas (anchor_score + bundle)
  (b) deja un "agent_queue" listo para validar con un agente multimodal (landing + creativos)
- Solo usa stdlib. No requiere pandas, pydantic, etc.
"""

import hashlib
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlparse


# =============================
# CONFIG (ENTRADAS)
# =============================

# Nombre del producto (igual al que usas para crear la carpeta output)
NAME = "Ashawanda Ksm 66 X 2 Unidades"

# Root de tu repo: por default parent.parent (igual a tus scripts previos)
ROOT_DIR = Path(__file__).resolve().parent.parent

# País/ubicación que se usó para el scraping (metadato)
SEARCH_COUNTRY = "CO"  # Default Colombia

# Nuevo parámetro (según tu solicitud):
# - "len_allowed" en tu mensaje corresponde a idioma permitido.
LANG_ALLOWED = "es"  # default español. Opciones típicas: "es", "en"
ALLOW_UNDETERMINED_LANGUAGE = False  # si True, mantiene "und" (idioma no determinable)

# Nuevo filtro:
REQUIRE_PRODUCT_NAME_CONTAINED = True  # conservar solo anuncios donde el nombre núcleo del producto esté contenido

# Regla negocio (tus thresholds)
RULE_MIN_ADVERTISERS_FOR_TEST = 4    # >3 anunciantes => test (deprecated by RANGE_PRODUCT_TEST?)
RULE_MAX_EXACT_ADVERTISERS = 7       # >7 anunciantes EXACTOS => no apto
RULE_SCALING_ADS_PER_ADVERTISER = 5  # >=5 ads del mismo producto => escalando (deprecated by NUM_MAX_ESCALING?)

# Nuevos parámetros solicitados
RANGE_PRODUCT_TEST = [3, 7]  # Rango inclusivo para advertisers_total (o candidate_same)
NUM_MAX_ESCALING = 4         # > 4 ads del mismo producto => escalando

# Heurística de match (sin agente)
# anchor_score = hits / len(anchor_tokens)
CANDIDATE_SCORE_THRESHOLD = 0.40   # candidato "mismo producto" (heurístico)
BUNDLE_SCORE_THRESHOLD = 0.25      # bundle/kit candidato (heurístico)
DRIFT_SCORE_THRESHOLD = 0.20       # debajo de esto es drift (heurístico)

# Límites de payload
MAX_URLS_STORED_PER_ADVERTISER = 50
MAX_AGENT_SAMPLES_PER_ADVERTISER = 3
MAX_TOP_ADS_PER_ADVERTISER = 5

# Keywords para detectar "bundle/kit"
BUNDLE_KEYWORDS = [
    "combo", "kit", "bundle", "pack", "paquete", "x2", "2 unidades", "dos unidades",
    "3x", "4x", "set", "incluye", "regalo", "gratis +", "2 en 1", "2x1"
]

# Palabras muy frecuentes/no ancla (se excluyen al derivar anchor tokens)
STOPWORDS = {
    "comprar", "compra", "precio", "oferta", "promo", "promocion", "promoción",
    "descuento", "envio", "envío", "gratis", "pago", "contra", "entrega",
    "colombia", "bogota", "bogotá", "medellin", "medellín", "cali", "disponible", "original",
    "tienda", "store", "shop", "sale", "deal", "now", "new"
}

# Heurística simple de idioma
SPANISH_MARKERS = {
    "envio", "envío", "entrega", "contraentrega", "pago", "oferta", "descuento",
    "colombia", "bogota", "bogotá", "medellin", "medellín", "cali", "gratis",
    "comprar", "precio", "original", "disponible", "punto", "fisico", "físico",
    "respaldo", "verificable", "enviamos", "envios", "envíos", "garantia", "garantía"
}
ENGLISH_MARKERS = {
    "free", "shipping", "sale", "discount", "deal", "buy", "now", "official",
    "original", "limited", "offer", "promo", "delivery", "cash", "cod"
}


# =============================
# Utils
# =============================

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def slugify(text: str) -> str:
    text = strip_accents(text.strip().lower())
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "product"


def norm(s: str | None) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def extract_text(value: Any) -> str:
    """
    Extrae texto de forma robusta:
    - Si es str: lo devuelve limpio
    - Si es dict (ej: {'text': '...'}): intenta sacar 'text' o 'original'
    - Si es None u otro: devuelve ""
    """
    if not value:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("original") or "").strip()
    return str(value).strip()


def get_domain(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def tokenize(text: str) -> List[str]:
    """
    Tokeniza conservando números y guiones útiles (ej: ksm-66, 5500mg).
    """
    text = norm(strip_accents(text))
    tokens = re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)?", text)
    return [t for t in tokens if t]


def contains_any(text: str, keywords: List[str]) -> bool:
    t = norm(strip_accents(text or ""))
    for kw in keywords:
        kw2 = norm(strip_accents(kw))
        if kw2 and kw2 in t:
            return True
    return False


def build_searchable_text(ad: Dict[str, Any]) -> str:
    snap = ad.get("snapshot") or {}
    parts = []
    for k in ["title", "body", "caption", "cta_text"]:
        parts.append(extract_text(snap.get(k)))
    for k in ["page_name", "ad_library_url", "ad_archive_id"]:
        parts.append(extract_text(ad.get(k)))
    landing = snap.get("link_url")
    if isinstance(landing, str):
        parts.append(landing.strip())
    return " ".join([p for p in parts if p])


def compute_anchor_score(search_text: str, anchors: List[str]) -> Tuple[float, List[str]]:
    if not anchors:
        return 0.0, []
    t = norm(strip_accents(search_text))
    hits = [a for a in anchors if a and a in t]
    score = len(hits) / float(len(anchors))
    return score, hits


def compute_ad_dedupe_key(ad: Dict[str, Any]) -> Tuple[str, str]:
    """
    advertiser_key: page_id o page_name
    ad_key: ad_archive_id preferido; fallback _dedupe_key; fallback hash
    """
    snap = ad.get("snapshot") or {}

    page_id = extract_text(ad.get("page_id") or snap.get("page_id"))
    page_name = extract_text(ad.get("page_name") or snap.get("page_name"))

    advertiser_key = norm(page_id) if page_id else norm(page_name)
    if not advertiser_key:
        advertiser_key = "unknown_advertiser"

    ad_archive_id = extract_text(ad.get("ad_archive_id"))
    ad_key = ad_archive_id or extract_text(ad.get("_dedupe_key"))

    if not ad_key:
        payload = "|".join([
            norm(extract_text(snap.get("link_url"))),
            norm(extract_text(snap.get("title"))),
            norm(extract_text(snap.get("body"))),
        ])
        ad_key = hashlib.sha1(payload.encode("utf-8")).hexdigest()

    return advertiser_key, ad_key


def safe_list_unique(items: List[str], limit: int) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if not isinstance(x, str):
            continue
        x2 = x.strip()
        if not x2:
            continue
        if x2 in seen:
            continue
        seen.add(x2)
        out.append(x2)
        if len(out) >= limit:
            break
    return out


def detect_language(text: str) -> str:
    """
    Heurística rápida ES/EN/UND.
    - No es perfecta, pero funciona bien para Ads con copy real.
    """
    t = strip_accents(norm(text))
    if not t:
        return "und"

    tokens = re.findall(r"[a-z]+", t)
    if not tokens:
        return "und"

    es = sum(1 for w in tokens if w in SPANISH_MARKERS)
    en = sum(1 for w in tokens if w in ENGLISH_MARKERS)

    # Señales por caracteres típicos (ñ/acentos) en original
    has_spanish_chars = any(ch in (text or "") for ch in "ñÑáéíóúÁÉÍÓÚüÜ")

    if (es >= 2 and es > en) or (has_spanish_chars and es >= 1) or (es >= 1 and en == 0):
        return "es"
    if (en >= 2 and en > es) or (en >= 1 and es == 0):
        return "en"
    return "und"


def language_allowed(lang: str) -> bool:
    if LANG_ALLOWED == "all":
        return True
    if lang == LANG_ALLOWED:
        return True
    if lang == "und" and ALLOW_UNDETERMINED_LANGUAGE:
        return True
    return False


def derive_anchor_tokens(product_meta: Dict[str, Any], product_name: str) -> List[str]:
    """
    Deriva tokens ancla desde:
    - NAME
    - canonical_product_name
    - product_type
    - short_description
    - querys[]
    Selecciona tokens frecuentes + tokens con dígitos/guion.
    """
    sources = [product_name]
    for k in ["canonical_product_name", "product_type", "short_description"]:
        if isinstance(product_meta.get(k), str):
            sources.append(product_meta[k])

    queries = product_meta.get("querys") or []
    if isinstance(queries, list):
        sources.extend([q for q in queries if isinstance(q, str)])

    freq: Dict[str, int] = {}
    for s in sources:
        for t in tokenize(s):
            if t in STOPWORDS:
                continue
            if len(t) < 3 and not any(ch.isdigit() for ch in t):
                continue
            freq[t] = freq.get(t, 0) + 1

    # prioriza tokens con dígitos o guion
    digit_or_hyphen = [t for t in freq.keys() if any(ch.isdigit() for ch in t) or "-" in t]
    digit_or_hyphen.sort(key=lambda x: (-freq[x], -len(x), x))

    top = sorted(freq.keys(), key=lambda x: (-freq[x], -len(x), x))

    anchors: List[str] = []
    for t in digit_or_hyphen:
        if t not in anchors:
            anchors.append(t)
        if len(anchors) >= 6:
            break

    for t in top:
        if t not in anchors:
            anchors.append(t)
        if len(anchors) >= 10:
            break

    if not anchors:
        anchors = [t for t in tokenize(product_name) if t not in STOPWORDS][:6]

    return anchors


def build_product_name_matcher(product_meta: Dict[str, Any], anchors: List[str]) -> Dict[str, Any]:
    """
    Construye 'core_tokens' y 'core_phrases' para el filtro
    REQUIRE_PRODUCT_NAME_CONTAINED.

    Idea:
    - usar canonical_product_name (mejor que NAME, porque NAME puede tener typos)
    - crear frases cortas (2-3 tokens) que suelen aparecer en el copy
    - si no hay match por frase, exigir >=2 hits de core_tokens para evitar drift
    """
    canonical = product_meta.get("canonical_product_name") or ""
    canonical = strip_accents(norm(canonical))

    # tokens core (prioriza anchors que estén en canonical)
    canonical_tokens = [t for t in tokenize(canonical) if t not in STOPWORDS]
    anchor_set = set(anchors)

    core_tokens = [t for t in canonical_tokens if t in anchor_set]
    # fallback: primeros tokens relevantes
    if len(core_tokens) < 2:
        core_tokens = canonical_tokens[:6]

    core_tokens = [t for t in core_tokens if t and t not in STOPWORDS][:6]

    core_phrases = []
    if len(core_tokens) >= 2:
        core_phrases.append(" ".join(core_tokens[:2]))
    if len(core_tokens) >= 3:
        core_phrases.append(" ".join(core_tokens[:3]))
    if canonical and len(canonical) <= 40:
        core_phrases.append(canonical)

    # dedupe
    seen = set()
    phrases = []
    for p in core_phrases:
        p = p.strip()
        if p and p not in seen:
            seen.add(p)
            phrases.append(p)

    return {
        "canonical_norm": canonical,
        "core_tokens": core_tokens,
        "core_phrases": phrases,
        "min_token_hits": 2,
    }


def product_name_contained(search_text: str, matcher: Dict[str, Any]) -> bool:
    t = strip_accents(norm(search_text))
    if not t:
        return False

    # 1) frases
    for ph in matcher.get("core_phrases", []):
        if ph and ph in t:
            return True

    # 2) hits por tokens
    core_tokens = matcher.get("core_tokens", [])
    min_hits = int(matcher.get("min_token_hits", 2))

    hits = sum(1 for tok in core_tokens if tok and tok in t)
    return hits >= min_hits


# =============================
# Main
# =============================

def run_process_info(
    name: str = NAME,
    country: str = SEARCH_COUNTRY,
    lang_allowed: str = LANG_ALLOWED,
    allow_und: bool = ALLOW_UNDETERMINED_LANGUAGE,
    require_product_name_contained: bool = REQUIRE_PRODUCT_NAME_CONTAINED,
    range_product_test: List[int] = RANGE_PRODUCT_TEST,
    num_max_escaling: int = NUM_MAX_ESCALING,
    rule_min_advertisers: int = RULE_MIN_ADVERTISERS_FOR_TEST,
    rule_max_exact: int = RULE_MAX_EXACT_ADVERTISERS,
    rule_scaling_ads: int = RULE_SCALING_ADS_PER_ADVERTISER,
):
    """
    Función orquestadora que procesa los resultados de scraping y genera reportes.
    """
    product_folder = slugify(name)
    product_dir = ROOT_DIR / "output" / product_folder
    apify_results_dir = product_dir / "apify_results"

    dedup_path = apify_results_dir / f"fblibrary_ads_dedup_{product_folder}.jsonl"
    querys_path = product_dir / f"querys_fblibrary_{product_folder}.json"

    if not dedup_path.exists():
        raise FileNotFoundError(f"No existe dedup jsonl: {dedup_path}")
    if not querys_path.exists():
        raise FileNotFoundError(f"No existe querys json: {querys_path}")

    product_meta = json.loads(querys_path.read_text(encoding="utf-8"))
    anchors = derive_anchor_tokens(product_meta, name)
    name_matcher = build_product_name_matcher(product_meta, anchors)

    advertisers: Dict[str, Dict[str, Any]] = {}
    ad_records: List[Dict[str, Any]] = []

    stats = {
        "input_ads_lines": 0,
        "skipped_error_lines": 0,
        "skipped_duplicate_ads": 0,
        "skipped_lang": 0,
        "skipped_product_name": 0,
        "kept_ads": 0,
    }

    seen_ads_global = set()  # (advertiser_key, ad_id)

    for ad in iter_jsonl(dedup_path):
        stats["input_ads_lines"] += 1

        if "error" in ad and isinstance(ad["error"], str):
            stats["skipped_error_lines"] += 1
            continue

        snap = ad.get("snapshot") or {}

        advertiser_key, ad_key = compute_ad_dedupe_key(ad)
        ad_id = str(extract_text(ad.get("ad_archive_id")) or ad_key)

        if (advertiser_key, ad_id) in seen_ads_global:
            stats["skipped_duplicate_ads"] += 1
            continue
        seen_ads_global.add((advertiser_key, ad_id))

        page_name = extract_text(ad.get("page_name") or snap.get("page_name"))
        page_id = extract_text(ad.get("page_id") or snap.get("page_id"))
        page_profile_uri = extract_text(snap.get("page_profile_uri"))

        ad_library_url = extract_text(ad.get("ad_library_url"))
        landing_url = extract_text(snap.get("link_url"))
        landing_domain = get_domain(landing_url)

        searchable_text = build_searchable_text(ad)

        # FILTRO 1: IDIOMA
        # Nota: detect_language y language_allowed usan parámetros globales
        # Para ser purista deberían pasarse, pero por ahora se mantiene la lógica
        # asumiendo que el script de llamada configura o acepta el default.
        # Mejora: usaremos el arg lang_allowed aquí.
        
        lang = detect_language(searchable_text)
        
        # Lógica local de language_allowed para respetar el argumento
        is_allowed = False
        if lang_allowed == "all" or lang == lang_allowed:
            is_allowed = True
        elif lang == "und" and allow_und:
            is_allowed = True
        
        if not is_allowed:
            stats["skipped_lang"] += 1
            continue

        # FILTRO 2: NOMBRE PRODUCTO CONTENIDO
        if require_product_name_contained:
            if not product_name_contained(searchable_text, name_matcher):
                stats["skipped_product_name"] += 1
                continue

        score, hits = compute_anchor_score(searchable_text, anchors)
        is_bundle = contains_any(searchable_text, BUNDLE_KEYWORDS) or contains_any(landing_url or "", BUNDLE_KEYWORDS)

        candidate_same = (score >= CANDIDATE_SCORE_THRESHOLD) and (not is_bundle)
        candidate_bundle = is_bundle and (score >= BUNDLE_SCORE_THRESHOLD)
        candidate_drift = score < DRIFT_SCORE_THRESHOLD

        ad_row = {
            "ad_id": ad_id,
            "advertiser_key": advertiser_key,
            "lang": lang,
            "page_name": page_name,
            "page_id": page_id,
            "page_profile_uri": page_profile_uri,
            "ad_library_url": ad_library_url,
            "landing_url": landing_url,
            "landing_domain": landing_domain,
            "start_date": ad.get("start_date_formatted") or ad.get("start_date"),
            "end_date": ad.get("end_date_formatted") or ad.get("end_date"),
            "is_active": ad.get("is_active"),
            "text": {
                "title": extract_text(snap.get("title")),
                "body": extract_text(snap.get("body")),
                "caption": extract_text(snap.get("caption")),
                "cta_text": extract_text(snap.get("cta_text")),
            },
            "match": {
                "anchor_score": round(score, 4),
                "anchor_hits": hits,
                "is_bundle": bool(is_bundle),
                "candidate_same_product": bool(candidate_same),
                "candidate_bundle": bool(candidate_bundle),
                "candidate_drift": bool(candidate_drift),
            },
        }
        ad_records.append(ad_row)
        stats["kept_ads"] += 1

        if advertiser_key not in advertisers:
            advertisers[advertiser_key] = {
                "advertiser_key": advertiser_key,
                "page_name": page_name,
                "page_id": page_id,
                "page_profile_uri": page_profile_uri,
                "counts": {
                    "ads_total": 0,
                    "ads_candidate_same": 0,
                    "ads_candidate_bundle": 0,
                    "ads_candidate_drift": 0,
                    "ads_lang_es": 0,
                    "ads_lang_en": 0,
                    "ads_lang_und": 0,
                },
                "landing_domains": [],
                "landing_urls": [],
                "ad_library_urls": [],
                "top_ads": [],
                "flags": {"is_scaling_candidate_same": False},
            }

        adv = advertisers[advertiser_key]
        adv["counts"]["ads_total"] += 1
        adv["counts"]["ads_candidate_same"] += 1 if candidate_same else 0
        adv["counts"]["ads_candidate_bundle"] += 1 if candidate_bundle else 0
        adv["counts"]["ads_candidate_drift"] += 1 if candidate_drift else 0

        if lang == "es":
            adv["counts"]["ads_lang_es"] += 1
        elif lang == "en":
            adv["counts"]["ads_lang_en"] += 1
        else:
            adv["counts"]["ads_lang_und"] += 1

        if landing_domain:
            adv["landing_domains"].append(landing_domain)
        if landing_url:
            adv["landing_urls"].append(landing_url)
        if ad_library_url:
            adv["ad_library_urls"].append(ad_library_url)

    for adv_key, adv in advertisers.items():
        adv["landing_domains"] = sorted({d for d in adv["landing_domains"] if isinstance(d, str) and d})
        adv["landing_urls"] = safe_list_unique(adv["landing_urls"], MAX_URLS_STORED_PER_ADVERTISER)
        adv["ad_library_urls"] = safe_list_unique(adv["ad_library_urls"], MAX_URLS_STORED_PER_ADVERTISER)

        adv_ads = [a for a in ad_records if a["advertiser_key"] == adv_key]
        adv_ads_sorted = sorted(adv_ads, key=lambda x: x["match"]["anchor_score"], reverse=True)

        top_ads = []
        for a in adv_ads_sorted[:MAX_TOP_ADS_PER_ADVERTISER]:
            body = extract_text(a["text"].get("body"))
            snippet = body.replace("\n", " ").strip()
            if len(snippet) > 240:
                snippet = snippet[:240] + "..."
            top_ads.append({
                "ad_id": a["ad_id"],
                "ad_library_url": a["ad_library_url"],
                "landing_url": a["landing_url"],
                "anchor_score": a["match"]["anchor_score"],
                "flags": {
                    "candidate_same_product": a["match"]["candidate_same_product"],
                    "candidate_bundle": a["match"]["candidate_bundle"],
                    "candidate_drift": a["match"]["candidate_drift"],
                    "is_bundle": a["match"]["is_bundle"],
                },
                "snippet": snippet
            })

        adv["top_ads"] = top_ads
        # Flag escalamiento: > NUM_MAX_ESCALING
        adv["flags"]["is_scaling_candidate_same"] = adv["counts"]["ads_candidate_same"] > num_max_escaling

    advertisers_list = list(advertisers.values())
    advertisers_total = len(advertisers_list)
    advertisers_candidate_same = sum(1 for a in advertisers_list if a["counts"]["ads_candidate_same"] > 0)
    advertisers_candidate_bundle = sum(1 for a in advertisers_list if a["counts"]["ads_candidate_bundle"] > 0)

    scaling_advertisers = [
        a["advertiser_key"] for a in advertisers_list
        if a["flags"]["is_scaling_candidate_same"]
    ]
    
    # Producto Test: advertisers_candidate_same dentro del rango [min, max]
    in_test_range = (
        advertisers_candidate_same >= range_product_test[0] and 
        advertisers_candidate_same <= range_product_test[1]
    )

    rule_eval = {
        "producto_test": in_test_range,
        "test_candidate_by_advertisers": advertisers_candidate_same >= rule_min_advertisers,
        "too_competitive_if_exact": advertisers_candidate_same > rule_max_exact,
        "scaling_detected": len(scaling_advertisers) > 0,
        "notes": (
            "Heurístico: advertisers_candidate_same se basa en anchor_score + filtros. "
            "Para concluir 'exactamente el mismo producto' se recomienda validación multimodal del agente."
        )
    }

    def adv_priority(a: Dict[str, Any]):
        return (a["counts"]["ads_candidate_same"], a["counts"]["ads_total"], a["counts"]["ads_candidate_bundle"])

    advertisers_sorted = sorted(advertisers_list, key=adv_priority, reverse=True)

    agent_queue = []
    for adv in advertisers_sorted:
        if adv["counts"]["ads_candidate_same"] == 0 and adv["counts"]["ads_candidate_bundle"] == 0:
            continue

        adv_ads = [a for a in ad_records if a["advertiser_key"] == adv["advertiser_key"]]
        adv_ads_sorted = sorted(adv_ads, key=lambda x: x["match"]["anchor_score"], reverse=True)

        selected = []
        for a in adv_ads_sorted:
            if len(selected) >= MAX_AGENT_SAMPLES_PER_ADVERTISER:
                break
            selected.append(a["ad_id"])

        agent_queue.append({
            "task_type": "VALIDATE_PRODUCT_MATCH",
            "advertiser_key": adv["advertiser_key"],
            "page_name": adv.get("page_name"),
            "page_profile_uri": adv.get("page_profile_uri"),
            "selected_ad_ids": selected,
            "why_selected": {
                "ads_candidate_same": adv["counts"]["ads_candidate_same"],
                "ads_candidate_bundle": adv["counts"]["ads_candidate_bundle"],
                "top_anchor_scores": [x["anchor_score"] for x in adv["top_ads"][:3]],
                "landing_domains": adv["landing_domains"][:5],
            },
            "expected_output_labels": [
                "EXACT_MATCH", "SAME_FAMILY_DIFF_VARIANT", "BUNDLE_OR_KIT", "DRIFT_UNRELATED"
            ]
        })

    competition_report = {
        "schema_version": "1.1.0",
        "generated_at_utc": now_iso(),
        "product": {
            "input_name": name,
            "folder_name": product_folder,
            "search_country": country,
            "canonical_product_name": product_meta.get("canonical_product_name"),
            "product_type": product_meta.get("product_type"),
            "short_description": product_meta.get("short_description"),
            "disambiguation_notes": product_meta.get("disambiguation_notes"),
        },
        "filters": {
            "lang_allowed": lang_allowed,
            "allow_und": allow_und,
            "require_product_name_contained": require_product_name_contained,
            "product_name_matcher": name_matcher,
        },
        "input_files": {
            "dedup_jsonl": str(dedup_path),
            "queries_json": str(querys_path)
        },
        "rules": {
            "min_advertisers_for_test": rule_min_advertisers,
            "max_exact_advertisers": rule_max_exact,
            "scaling_ads_per_advertiser": rule_scaling_ads
        },
        "anchors": {
            "anchor_tokens": anchors,
            "score_thresholds": {
                "candidate_same_product": CANDIDATE_SCORE_THRESHOLD,
                "candidate_bundle": BUNDLE_SCORE_THRESHOLD,
                "drift": DRIFT_SCORE_THRESHOLD
            },
            "bundle_keywords": BUNDLE_KEYWORDS
        },
        "summary": {
            "stats": stats,
            "ads_total_after_filters": len(ad_records),
            "advertisers_total": advertisers_total,
            "advertisers_candidate_same": advertisers_candidate_same,
            "advertisers_candidate_bundle": advertisers_candidate_bundle,
            "scaling_advertisers_candidate_same": scaling_advertisers,
            "rule_evaluation_heuristic": rule_eval
        },
        "advertisers": advertisers_sorted,
        "ads": ad_records,
        "agent_queue": agent_queue
    }

    out_path = apify_results_dir / f"fblibrary_competition_report_{product_folder}.json"
    out_path.write_text(json.dumps(competition_report, ensure_ascii=False, indent=2), encoding="utf-8")

    # OUTPUT 2: Ranking simple
    advertisers_ranked = sorted(
        advertisers_sorted,
        key=lambda a: (a["counts"]["ads_total"], a["counts"]["ads_candidate_same"]),
        reverse=True
    )

    ranked = []
    for i, adv in enumerate(advertisers_ranked, start=1):
        ranked.append({
            "rank": i,
            "advertiser_key": adv["advertiser_key"],
            "page_name": adv.get("page_name"),
            "page_profile_uri": adv.get("page_profile_uri"),
            "ads_total": adv["counts"]["ads_total"],
            "ads_candidate_same": adv["counts"]["ads_candidate_same"],
            "is_scaling_candidate_same": bool(adv["flags"]["is_scaling_candidate_same"]),
            "landing_domains": adv.get("landing_domains", [])[:10],
            "sample_ad_library_urls": adv.get("ad_library_urls", [])[:10],
            "sample_landing_urls": adv.get("landing_urls", [])[:10],
        })

    # Scaling list: advertisers con >= NUM_MAX_ESCALING sample URLs
    scaling_list = []
    for r in ranked:
        if len(r["sample_ad_library_urls"]) >= num_max_escaling:
            scaling_list.append({
                "page_name": r.get("page_name"),
                "num_samples": len(r["sample_ad_library_urls"])
            })

    advertisers_rank_report = {
        "schema_version": "1.0.0",
        "generated_at_utc": now_iso(),
        "product": {
            "input_name": name,
            "folder_name": product_folder,
            "search_country": country,
            "canonical_product_name": product_meta.get("canonical_product_name"),
        },
        "filters": {
            "lang_allowed": lang_allowed,
            "allow_und": allow_und,
            "require_product_name_contained": require_product_name_contained,
        },
        "metrics": {
            "producto_test": in_test_range,
            "range_product_test": range_product_test,
            "scaling_threshold_ads": num_max_escaling,
            "scaling": scaling_list,
        },
        "advertisers_total": advertisers_total,
        "advertisers_ranked": ranked
    }

    rank_path = apify_results_dir / f"fblibrary_advertisers_rank_{product_folder}.json"
    rank_path.write_text(json.dumps(advertisers_rank_report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[{now_iso()}] OK ✅ Reports generados:")
    print(f"  1) {out_path}")
    print(f"  2) {rank_path}")
    print(
        f"  advertisers_total={advertisers_total} | candidate_same={advertisers_candidate_same} | "
        f"scaling_candidates={len(scaling_advertisers)} | kept_ads={len(ad_records)}"
    )


def main():
    run_process_info()


if __name__ == "__main__":
    main()
