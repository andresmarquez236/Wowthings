#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Explorer Agent 2 - Extractor

Enriquece ads (dedup_ads.jsonl) con:
- product_name_guess
- category / subcategory (taxonomía cerrada)
- signals (envío gratis, contraentrega, descuento, urgencia, garantía, whatsapp, etc.)
- evidence (spans del texto)
- confidence (0..1): qué tan candidato es (para tu pipeline)

Modo recomendado:
1) Pass 1 (texto en batch) -> rápido/barato
2) Pass 2 (visión opcional) -> solo ads con needs_vision=True o low_confidence
"""

import argparse
import base64
import json
import os
import re
import time
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from PIL import Image

# OpenAI SDK v1.x
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ----------------------------
# Config
# ----------------------------

DEFAULT_MODEL_TEXT = os.getenv("OPENAI_MODEL_TEXT", "gpt-4o")   # cambia a gpt-5.2 si lo tienes habilitado
DEFAULT_MODEL_VISION = os.getenv("OPENAI_MODEL_VISION", DEFAULT_MODEL_TEXT)
DEFAULT_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))

TAXONOMY = {
    "Moda": ["Jeans/Denim", "Calzado", "Ropa interior", "Deportiva", "Accesorios", "Otros"],
    "Belleza": ["Skincare", "Maquillaje", "Cabello", "Perfumes", "Uñas", "Otros"],
    "Hogar": ["Cocina", "Organización", "Decoración", "Limpieza", "Iluminación", "Otros"],
    "Tecnología": ["Audio", "Celulares", "Accesorios", "Computación", "Smartwatch", "Otros"],
    "Salud y Bienestar": ["Suplementos", "Fitness", "Ortopedia", "Bienestar íntimo", "Otros"],
    "Mascotas": ["Perros", "Gatos", "Accesorios", "Higiene", "Otros"],
    "Bebés y Niños": ["Juguetes", "Ropa", "Cuidado", "Otros"],
    "Deportes y Fitness": ["Ropa deportiva", "Equipos", "Accesorios", "Otros"],
    "Automotriz y Moto": ["Accesorios auto", "Accesorios moto", "Herramientas", "Otros"],
    "Educación": ["Cursos", "Tareas/Academia", "Idiomas", "Otros"],
    "Servicios": ["Servicios digitales", "Servicios profesionales", "Otros"],
    "Alimentos y Bebidas": ["Snacks", "Bebidas", "Suplementos alimenticios", "Otros"],
    "Otros": ["Otros"],
}

SIGNALS_SCHEMA = [
    "free_shipping",           # envío gratis
    "nationwide_shipping",     # envíos a nivel nacional / todo Colombia
    "cod",                     # pago contraentrega / paga al recibir
    "whatsapp_cta",            # CTA a WhatsApp
    "discount_offer",          # descuento, 2x1, 3x$..., 50% off
    "urgency",                 # solo hoy, últimas unidades
    "guarantee_trust",         # garantía, compra segura, satisfacción
    "cash_price",              # precio explícito
]

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

# ----------------------------
# Helpers: extract fields from ad JSON
# ----------------------------

def get_snapshot(ad: Dict[str, Any]) -> Dict[str, Any]:
    return ad.get("snapshot") or {}

def extract_text_blob(ad: Dict[str, Any]) -> Dict[str, Any]:
    snap = get_snapshot(ad)
    body = (snap.get("body") or {}).get("text") or ""
    title = snap.get("title") or ""
    link_desc = snap.get("link_description") or ""
    caption = snap.get("caption") or ""
    cta_text = snap.get("cta_text") or ""
    cta_type = snap.get("cta_type") or ""
    link_url = snap.get("link_url") or ""
    display_format = snap.get("display_format") or ""
    page_name = snap.get("page_name") or ad.get("page_name") or ""
    page_categories = snap.get("page_categories") or []
    like_count = snap.get("page_like_count")

    # extras
    query_matched = ad.get("_query_matched") or ""
    intent_guess = ad.get("_intent_guess") or ""

    text = "\n".join([t for t in [title, body, link_desc, caption, cta_text] if t]).strip()

    return {
        "ad_archive_id": str(ad.get("ad_archive_id") or ""),
        "page_id": str(ad.get("page_id") or snap.get("page_id") or ""),
        "page_name": page_name,
        "display_format": display_format,
        "cta_type": cta_type,
        "cta_text": cta_text,
        "link_url": link_url,
        "page_categories": page_categories,
        "page_like_count": like_count,
        "query_matched": query_matched,
        "intent_guess": intent_guess,
        "title": title,
        "body": body,
        "link_description": link_desc,
        "caption": caption,
        "text": text,
    }

def extract_preview_image_url(ad: Dict[str, Any]) -> Optional[str]:
    """No descarga video. Usa miniaturas de video o imagen resized/original."""
    snap = get_snapshot(ad)

    # 1) IMAGE
    for it in (snap.get("images") or []):
        u = it.get("resized_image_url") or it.get("original_image_url")
        if u:
            return u
    for it in (snap.get("extra_images") or []):
        u = it.get("resized_image_url") or it.get("original_image_url")
        if u:
            return u

    # 2) VIDEO -> thumbnail
    for v in (snap.get("videos") or []):
        u = v.get("video_preview_image_url")
        if u:
            return u
    for v in (snap.get("extra_videos") or []):
        u = v.get("video_preview_image_url")
        if u:
            return u

    # 3) CARDS / DCO
    for c in (snap.get("cards") or []):
        u = c.get("resized_image_url") or c.get("original_image_url") or c.get("video_preview_image_url")
        if u:
            return u

    return None

def download_image_as_base64(url: str, timeout: int = 20) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
        "Referer": "https://www.facebook.com/",
    }
    try:
        r = requests.get(url, headers=headers, timeout=(5, timeout))
        if r.status_code != 200 or not r.content:
            return None
        # re-encode to jpeg (reduce size, normalize)
        img = Image.open(BytesIO(r.content)).convert("RGB")
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return b64
    except Exception:
        return None

# ----------------------------
# LLM prompt
# ----------------------------

def build_taxonomy_text() -> str:
    lines = []
    for cat, subs in TAXONOMY.items():
        lines.append(f"- {cat}: {', '.join(subs)}")
    return "\n".join(lines)

SYSTEM_PROMPT = (
    "Eres una analista experta en ads de e-commerce (Colombia). "
    "Tu trabajo es extraer información estructurada de anuncios de Facebook Ads Library. "
    "Debes ser estricta, no inventar, y justificar con evidencia textual."
)

def user_prompt_for_batch(batch_ads: List[Dict[str, Any]]) -> str:
    taxonomy_text = build_taxonomy_text()
    return f"""
Analiza los siguientes anuncios (Colombia) y devuelve **JSONL**: 1 línea JSON por cada ad, sin texto adicional.

Taxonomía (elige SOLO de esta lista):
{taxonomy_text}

Campos obligatorios por ad (JSON):
- ad_archive_id (string)
- product_name_guess (string): nombre canónico del producto (sin promo; ej "jeans para mujer", "condimentero giratorio 360", "audífonos bluetooth")
- category (string): una de las categorías de la taxonomía
- subcategory (string): una de las subcategorías válidas para esa categoría
- is_bundle (boolean): true si es combo/pack (ej "3 jeans", "2x1", "kit")
- signals (object con booleanos): {", ".join(SIGNALS_SCHEMA)}
- evidence (object): para cada signal=true incluye 1-3 fragmentos exactos del texto (spans) que lo soportan; si signal=false puede omitirse
- confidence (number 0..1): qué tan buen candidato es el producto para tu explorador (orientado a compra por impulso/dropshipping). Considera señales como contraentrega, envío nacional, descuentos, urgencia, WhatsApp, etc.
- needs_vision (boolean): true si el texto NO es suficiente para identificar producto/categoría y probablemente esté en la imagen.

Reglas:
- NO inventes producto si no aparece en el texto: si no puedes, usa product_name_guess="desconocido" y needs_vision=true.
- category/subcategory deben ser válidas según la taxonomía.
- evidence debe copiar frases tal cual (cortas).
- Responde SOLO JSONL.

Ads:
{json.dumps(batch_ads, ensure_ascii=False)}
""".strip()

def user_prompt_for_vision(ad_payload: Dict[str, Any]) -> str:
    taxonomy_text = build_taxonomy_text()
    return f"""
Analiza este anuncio (texto + imagen) y devuelve un único JSON (sin texto adicional).

Taxonomía (elige SOLO de esta lista):
{taxonomy_text}

Campos obligatorios:
- ad_archive_id (string)
- product_name_guess (string)
- category (string)
- subcategory (string)
- is_bundle (boolean)
- signals (object booleanos): {", ".join(SIGNALS_SCHEMA)}
- evidence (object): spans textuales que soporten signals=true
- confidence (number 0..1)
- needs_vision (boolean): normalmente false si ya pudiste identificar con imagen+texto

Reglas:
- Si la imagen confirma el producto, úsala para corregir product_name_guess/categoría.
- NO inventes marcas/modelos si no se ven claros.
- Responde SOLO JSON.

Texto del ad:
{json.dumps(ad_payload, ensure_ascii=False)}
""".strip()

def extract_json_objects(text: str) -> List[Dict[str, Any]]:
    """
    Intenta recuperar JSONL o JSON incluso si el modelo devuelve algo extra.
    """
    text = text.strip()
    objs = []
    # Caso JSONL: múltiples líneas
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{") and line.endswith("}"):
            try:
                objs.append(json.loads(line))
            except Exception:
                pass
    if objs:
        return objs

    # Fallback: primer bloque JSON {...}
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        try:
            obj = json.loads(m.group(0))
            return [obj] if isinstance(obj, dict) else []
        except Exception:
            return []
    return []

# ----------------------------
# Main
# ----------------------------

@dataclass
class RunPaths:
    run_dir: Path
    dedup_path: Path
    out_path: Path
    err_path: Path

def get_run_paths(run_id: str) -> RunPaths:
    root = Path(__file__).resolve().parent
    run_dir = root / "data" / "runs" / run_id
    dedup_path = run_dir / "dedup_ads.jsonl"
    out_path = run_dir / "ads_enriched.jsonl"
    err_path = run_dir / "ads_enriched.errors.jsonl"
    return RunPaths(run_dir, dedup_path, out_path, err_path)

def load_processed_ids(out_path: Path) -> Set[str]:
    processed: Set[str] = set()
    if not out_path.exists():
        return processed
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                adid = str(obj.get("ad_archive_id") or "")
                if adid:
                    processed.add(adid)
            except Exception:
                continue
    return processed

def read_dedup_ads(dedup_path: Path) -> Iterable[Dict[str, Any]]:
    with open(dedup_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit", type=int, default=0, help="0 = sin límite")
    parser.add_argument("--batch-size", type=int, default=15)
    parser.add_argument("--model-text", default=DEFAULT_MODEL_TEXT)
    parser.add_argument("--model-vision", default=DEFAULT_MODEL_VISION)
    parser.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    parser.add_argument("--vision-pass", action="store_true", help="Ejecuta segundo pass con imagen para needs_vision o baja confianza")
    parser.add_argument("--vision-threshold", type=float, default=0.55, help="Si confidence < threshold, entra a visión (si hay imagen)")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep entre llamadas (segundos) para cuidar rate limits")
    args = parser.parse_args()

    rp = get_run_paths(args.run_id)
    rp.run_dir.mkdir(parents=True, exist_ok=True)
    if not rp.dedup_path.exists():
        raise FileNotFoundError(f"No existe: {rp.dedup_path}")

    client = OpenAI()

    processed_ids = load_processed_ids(rp.out_path)

    # ---- Pass 1 (texto batch) ----
    buffer: List[Dict[str, Any]] = []
    total_in = 0
    written = 0

    out_f = open(rp.out_path, "a", encoding="utf-8")
    err_f = open(rp.err_path, "a", encoding="utf-8")

    for ad in read_dedup_ads(rp.dedup_path):
        total_in += 1
        if args.limit and total_in > args.limit:
            break

        payload = extract_text_blob(ad)
        adid = payload["ad_archive_id"]
        if not adid or adid in processed_ids:
            continue

        buffer.append(payload)

    # ---- Helper for Circuit Breaker ----
    def process_batch_recursive(batch: List[Dict[str, Any]], depth=0):
        # Base case
        if not batch:
            return

        try:
            prompt = user_prompt_for_batch(batch)
            resp = client.chat.completions.create(
                model=args.model_text,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=args.temperature,
                max_completion_tokens=2000,
            )
            content = resp.choices[0].message.content or ""
            objs = extract_json_objects(content)
            
            # Write success
            out_map = {str(o.get("ad_archive_id") or ""): o for o in objs if isinstance(o, dict)}
            
            for item in batch:
                aid = item["ad_archive_id"]
                o = out_map.get(aid)
                if not o:
                    # If single item batch and no JSON -> it's a failure (model refused)
                    if len(batch) == 1:
                        err_f.write(json.dumps({
                            "run_id": args.run_id,
                            "stage": "text_batch_missing",
                            "ad_archive_id": aid,
                            "error": "Model did not return JSON for this ad",
                        }, ensure_ascii=False) + "\n")
                    # If larger batch, it might be just this item missing, but let's assume successful processing of the rest is fine
                    continue

                o["ad_archive_id"] = aid
                o["_explorer_run_id"] = args.run_id
                o["_ts"] = now_iso()
                out_f.write(json.dumps(o, ensure_ascii=False) + "\n")
                out_f.flush()
                processed_ids.add(aid)
                
            nonlocal written
            written += len(out_map)

        except Exception as e:
            # Circuit Breaker Logic
            err_msg = str(e)
            
            # If batch is 1, we can't split anymore -> Log Error
            if len(batch) == 1:
                err_f.write(json.dumps({
                    "run_id": args.run_id,
                    "stage": "text_batch_exception",
                    "ad_archive_id": batch[0].get("ad_archive_id"),
                    "error": err_msg,
                    "depth": depth
                }, ensure_ascii=False) + "\n")
                return

            # If it's a potentially recoverable error by splitting (Contex Length, or one bad apple)
            # Strategy: Split in half and recurse
            mid = len(batch) // 2
            left = batch[:mid]
            right = batch[mid:]
            
            print(f"⚠️ Batch Error (len={len(batch)}). Splitting -> {len(left)} + {len(right)}. Error: {err_msg[:100]}...")
            process_batch_recursive(left, depth + 1)
            process_batch_recursive(right, depth + 1)

    # ---- Main Loop ----
    for ad in read_dedup_ads(rp.dedup_path):
        total_in += 1
        if args.limit and total_in > args.limit:
            break

        payload = extract_text_blob(ad)
        adid = payload["ad_archive_id"]
        if not adid or adid in processed_ids:
            continue

        buffer.append(payload)

        if len(buffer) >= args.batch_size:
            process_batch_recursive(buffer)
            buffer = []
            if args.sleep:
                time.sleep(args.sleep)

    # flush remainder
    if buffer:
        process_batch_recursive(buffer)

    print(json.dumps({
        "run_id": args.run_id,
        "stage": "pass1_text_done",
        "written": written,
        "processed_total": len(processed_ids),
        "limit": args.limit,
    }, ensure_ascii=False, indent=2))

    out_f.close()
    err_f.close()

    # ---- Pass 2 (visión opcional) ----
    if not args.vision_pass:
        return

    # Cargar enriquecidos y seleccionar los que necesitan visión
    needs: List[Dict[str, Any]] = []
    with open(rp.out_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                o = json.loads(line)
                if not isinstance(o, dict):
                    continue
                if bool(o.get("needs_vision")) or float(o.get("confidence") or 0.0) < args.vision_threshold:
                    needs.append(o)
            except Exception:
                continue

    # Para poder usar imagen necesitamos volver a leer el dedup y mapear payloads (solo para esos IDs)
    need_ids = {str(o.get("ad_archive_id") or "") for o in needs}
    if not need_ids:
        print(json.dumps({"run_id": args.run_id, "stage": "pass2_vision_done", "updated": 0}, ensure_ascii=False))
        return

    id_to_ad: Dict[str, Dict[str, Any]] = {}
    for ad in read_dedup_ads(rp.dedup_path):
        aid = str(ad.get("ad_archive_id") or "")
        if aid in need_ids:
            id_to_ad[aid] = ad

    updated = 0
    err_path2 = rp.run_dir / "ads_enriched.vision.errors.jsonl"
    err_f2 = open(err_path2, "a", encoding="utf-8")

    # Escribimos un archivo nuevo “vision_overrides.jsonl” con los resultados; luego lo mergeas si quieres
    vision_out_path = rp.run_dir / "ads_enriched.vision_overrides.jsonl"
    vout = open(vision_out_path, "a", encoding="utf-8")

    for o in needs:
        aid = str(o.get("ad_archive_id") or "")
        ad = id_to_ad.get(aid)
        if not ad:
            continue

        payload = extract_text_blob(ad)
        img_url = extract_preview_image_url(ad)
        if not img_url:
            continue

        b64 = download_image_as_base64(img_url, timeout=25)
        if not b64:
            continue

        try:
            prompt = user_prompt_for_vision(payload)
            resp = client.chat.completions.create(
                model=args.model_vision,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                        ],
                    },
                ],
                temperature=args.temperature,
                max_completion_tokens=1200,
            )
            content = resp.choices[0].message.content or ""
            objs = extract_json_objects(content)
            if not objs:
                raise ValueError("No JSON returned")

            v = objs[0]
            v["ad_archive_id"] = aid
            v["_explorer_run_id"] = args.run_id
            v["_ts"] = now_iso()
            v["_vision_image_url"] = img_url

            vout.write(json.dumps(v, ensure_ascii=False) + "\n")
            vout.flush()
            updated += 1

        except Exception as e:
            err_f2.write(json.dumps({
                "run_id": args.run_id,
                "stage": "vision_exception",
                "ad_archive_id": aid,
                "error": str(e),
            }, ensure_ascii=False) + "\n")
        finally:
            if args.sleep:
                time.sleep(args.sleep)

    vout.close()
    err_f2.close()

    print(json.dumps({
        "run_id": args.run_id,
        "stage": "pass2_vision_done",
        "updated": updated,
        "vision_overrides_path": str(vision_out_path),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
