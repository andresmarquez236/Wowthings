# carrusel_generator.py
import os
import re
import json
import time
import random
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from dotenv import load_dotenv
from PIL import Image

from google import genai
from google.genai import types


# ----------------------------
# Utilidades de archivos
# ----------------------------
SUPPORTED_IMG_EXTS = {".png", ".jpg", ".jpeg", ".webp"}


def find_product_dir(output_root: str, product_name: str) -> Path:
    """
    Busca dentro de output_root una carpeta con nombre exacto product_name.
    1) Intenta output_root/product_name
    2) Si no existe, busca en subdirectorios de primer nivel.
    """
    root = Path(output_root)
    direct = root / product_name
    if direct.exists() and direct.is_dir():
        return direct

    # fallback: buscar en primer nivel (por si hay variaciones)
    for p in root.iterdir():
        if p.is_dir() and p.name == product_name:
            return p

    raise FileNotFoundError(
        f"No encontré la carpeta del producto '{product_name}' dentro de '{output_root}'. "
        f"Esperaba algo como: {direct}"
    )


def find_carrusel_json(product_dir: Path, product_name: str) -> Path:
    """
    Encuentra el json de carrusel dentro de product_dir.
    Prioridad:
      1) nanobanana_carrusel_<product_name>.json
      2) cualquier archivo que contenga 'carrusel' y product_name
      3) cualquier archivo que contenga 'carrusel'
    """
    preferred = product_dir / f"nanobanana_carrusel_{product_name}.json"
    if preferred.exists():
        return preferred

    candidates = list(product_dir.glob(f"*carrusel*{product_name}*.json"))
    if candidates:
        return sorted(candidates)[0]

    candidates = list(product_dir.glob("*carrusel*.json"))
    if candidates:
        return sorted(candidates)[0]

    raise FileNotFoundError(
        f"No encontré archivo de carrusel en {product_dir}. "
        f"Esperaba 'nanobanana_carrusel_{product_name}.json' o algún '*carrusel*.json'."
    )


def load_reference_images(product_dir: Path, max_ref_images: int) -> List[Image.Image]:
    """
    Carga imágenes desde product_dir/product_images.
    Ordena por nombre para estabilidad.
    """
    img_dir = product_dir / "product_images"
    if not img_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta de imágenes de referencia: {img_dir}")

    paths = [p for p in sorted(img_dir.iterdir()) if p.is_file() and p.suffix.lower() in SUPPORTED_IMG_EXTS]
    if not paths:
        raise FileNotFoundError(f"No encontré imágenes en: {img_dir} (exts: {SUPPORTED_IMG_EXTS})")

    paths = paths[: max_ref_images if max_ref_images and max_ref_images > 0 else len(paths)]

    images: List[Image.Image] = []
    for p in paths:
        try:
            images.append(Image.open(p))
        except Exception as e:
            print(f"⚠️ No pude abrir imagen {p.name}: {e}")

    if not images:
        raise RuntimeError(f"No se pudo cargar ninguna imagen válida desde: {img_dir}")

    return images


# ----------------------------
# Utilidades JSON / ordenación
# ----------------------------
def _angle_sort_key(angle_obj: Dict[str, Any]) -> Tuple[int, str]:
    """
    Ordena ángulos por número si angle_id es tipo 'ANGLE_1', 'ANGLE_2', etc.
    Si no, cae a orden lexicográfico.
    """
    angle_id = str(angle_obj.get("angle_id", "")).upper()
    m = re.search(r"(\d+)", angle_id)
    if m:
        return (int(m.group(1)), angle_id)
    return (10**9, angle_id)


def select_angles(results_by_angle: List[Dict[str, Any]], num_angulos: int) -> List[Dict[str, Any]]:
    if not results_by_angle:
        return []
    ordered = sorted(results_by_angle, key=_angle_sort_key)
    if num_angulos is None or num_angulos <= 0:
        return ordered
    return ordered[:num_angulos]


def stringify_prompt_object(prompt_obj: Dict[str, Any]) -> str:
    """
    Convierte el objeto completo del prompt (nanobanana_prompt) a string JSON
    preservando orden de claves (Python 3.7+ mantiene insertion order).
    IMPORTANT: NO extrae solo "prompt". Envía TODO el objeto.
    """
    return json.dumps(prompt_obj, ensure_ascii=False, indent=2)


# ----------------------------
# Gemini - generación robusta
# ----------------------------
def _is_rate_limit_error(err: Exception) -> bool:
    s = str(err).lower()
    return ("429" in s) or ("quota" in s) or ("rate" in s and "limit" in s) or ("resource exhausted" in s)


def _sleep_backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> None:
    # backoff exponencial con jitter
    t = min(cap, base * (2 ** max(0, attempt - 1)))
    t = t * (0.85 + random.random() * 0.3)  # jitter 0.85x - 1.15x
    time.sleep(t)


def generate_image_with_gemini(
    client: genai.Client,
    model: str,
    prompt_text: str,
    ref_images: List[Image.Image],
    aspect_ratio: str = "1:1",
    image_size: str = "4K",
) -> bytes:
    """
    Genera una imagen (bytes) a partir de prompt_text + ref_images.
    Devuelve bytes de la imagen generada.
    """
    response = client.models.generate_content(
        model=model,
        contents=[prompt_text, *ref_images],
        config=types.GenerateContentConfig(
            candidate_count=1,
            response_modalities=["IMAGE"],
            image_config=types.ImageConfig(
                aspect_ratio=aspect_ratio,
                image_size=image_size,  # "1K" | "2K" | "4K" (K MAYÚSCULA)
            ),
        ),
    )

    # Extraer el primer bloque de imagen que llegue
    if not response.candidates:
        raise RuntimeError("Respuesta sin candidates.")
    parts = response.candidates[0].content.parts if response.candidates[0].content else None
    if not parts:
        raise RuntimeError("Respuesta sin parts (vacía).")

    for part in parts:
        # SDK nuevo suele exponer inline_data y/o as_image()
        if hasattr(part, "inline_data") and part.inline_data and getattr(part.inline_data, "data", None):
            return part.inline_data.data
        if hasattr(part, "as_image"):
            img = part.as_image()
            # Convertimos a PNG bytes
            from io import BytesIO
            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()

    raise RuntimeError("No se encontró imagen en la respuesta del modelo.")


def generate_with_retries(
    client: genai.Client,
    model_candidates: List[str],
    prompt_text: str,
    ref_images: List[Image.Image],
    max_retries: int = 3,
    aspect_ratio: str = "1:1",
    image_size: str = "4K",
) -> Tuple[str, bytes]:
    """
    Prueba modelos en orden. Para cada modelo, reintenta en rate-limit.
    Devuelve (modelo_usado, image_bytes).
    """
    last_err: Optional[Exception] = None

    for model in model_candidates:
        for attempt in range(1, max_retries + 1):
            try:
                img_bytes = generate_image_with_gemini(
                    client=client,
                    model=model,
                    prompt_text=prompt_text,
                    ref_images=ref_images,
                    aspect_ratio=aspect_ratio,
                    image_size=image_size,
                )
                return model, img_bytes
            except Exception as e:
                last_err = e
                if _is_rate_limit_error(e) and attempt < max_retries:
                    print(f"   ⚠️ Quota/Rate limit con '{model}' (intento {attempt}/{max_retries}). Backoff...")
                    _sleep_backoff(attempt)
                    continue
                # error no rate-limit o ya sin retries
                break

        print(f"   ❌ Falló modelo '{model}'. Probando siguiente si existe...")

    raise RuntimeError(f"No se pudo generar imagen con ningún modelo. Último error: {last_err}")


# ----------------------------
# Pipeline carrusel
# ----------------------------
def run_carrusel_generation(
    product_name: str,
    num_angulos: int = 1,
    output_root: str = "output",
    api_key: Optional[str] = None,
    max_ref_images: int = 5,
    model_candidates: Optional[List[str]] = None,
    image_size: str = "4K",
    aspect_ratio: str = "1:1",
) -> None:
    """
    Genera todas las imágenes del carrusel para los primeros `num_angulos`.
    Guarda en output/<product>/generated_carrusel/angle_XX/...
    """
    if model_candidates is None or not model_candidates:
        model_candidates = ["gemini-3-pro-image-preview"]

    product_dir = find_product_dir(output_root=output_root, product_name=product_name)
    carrusel_json_path = find_carrusel_json(product_dir=product_dir, product_name=product_name)

    print(f"📦 Producto: {product_name}")
    print(f"📁 Product dir: {product_dir}")
    print(f"🧾 Carrusel JSON: {carrusel_json_path.name}")

    with open(carrusel_json_path, "r", encoding="utf-8") as f:
        carrusel_data = json.load(f)

    results_by_angle = carrusel_data.get("results_by_angle", [])
    selected = select_angles(results_by_angle, num_angulos=num_angulos)
    if not selected:
        raise RuntimeError("El JSON no trae 'results_by_angle' o está vacío.")

    # Cargar imágenes de referencia (una vez por producto)
    ref_images = load_reference_images(product_dir=product_dir, max_ref_images=max_ref_images)
    print(f"🖼️ Ref images cargadas: {len(ref_images)} (max={max_ref_images})")

    # Cliente
    if not api_key:
        raise ValueError("API key no provista. Usa os.environ['GEMINI_API_KEY'] o pásala por parámetro.")

    client = genai.Client(api_key=api_key)

    # Output dirs
    out_root = product_dir / "generated_carrusel"
    out_root.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Any] = {
        "product_name": product_name,
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "image_size": image_size,
        "aspect_ratio": aspect_ratio,
        "model_candidates": model_candidates,
        "max_ref_images": max_ref_images,
        "inputs": {
            "carrusel_json": str(carrusel_json_path),
            "product_images_dir": str(product_dir / "product_images"),
        },
        "outputs": [],
    }

    # Iterar ángulos
    for a_idx, angle_obj in enumerate(selected, start=1):
        angle_id = str(angle_obj.get("angle_id", f"ANGLE_{a_idx}"))
        angle_name = str(angle_obj.get("angle_name", ""))

        angle_out = out_root / f"angle_{a_idx:02d}"
        angle_out.mkdir(parents=True, exist_ok=True)

        carousel = angle_obj.get("carousel", {})
        cards = carousel.get("cards", [])
        if not cards:
            print(f"⚠️ Sin cards para {angle_id}. Saltando.")
            continue

        # Ordenar por card_index
        cards = sorted(cards, key=lambda c: int(c.get("card_index", 999999)))

        print(f"\n🎯 Ángulo {a_idx}/{len(selected)}: {angle_id}")
        if angle_name:
            print(f"   🧠 {angle_name}")
        print(f"   🧩 Cards: {len(cards)}")
        print(f"   📂 Output: {angle_out}")

        for card in cards:
            card_index = int(card.get("card_index", 0))
            nb_prompt_obj = card.get("nanobanana_prompt")
            if not isinstance(nb_prompt_obj, dict):
                print(f"   ⚠️ Card {card_index}: nanobanana_prompt inválido. Saltando.")
                continue

            # USAR EL OBJETO COMPLETO COMO STRING JSON
            prompt_text = stringify_prompt_object(nb_prompt_obj)

            # Nombre sugerido: usar "thumbnail" si existe, si no, Axx_Cyy
            thumb_name = str(nb_prompt_obj.get("thumbnail", "")).strip()
            if thumb_name:
                base = Path(thumb_name).stem
                out_name = f"{base}.png"
            else:
                out_name = f"A{a_idx:02d}_C{card_index:02d}.png"

            out_path = angle_out / out_name

            print(f"   🚀 Generando Card {card_index} -> {out_name}")

            used_model, img_bytes = generate_with_retries(
                client=client,
                model_candidates=model_candidates,
                prompt_text=prompt_text,
                ref_images=ref_images,
                max_retries=3,
                aspect_ratio=aspect_ratio,
                image_size=image_size,
            )

            with open(out_path, "wb") as f:
                f.write(img_bytes)

            manifest["outputs"].append(
                {
                    "angle_index": a_idx,
                    "angle_id": angle_id,
                    "angle_name": angle_name,
                    "card_index": card_index,
                    "stage": card.get("stage"),
                    "goal": card.get("goal"),
                    "thumbnail_field": thumb_name,
                    "output_path": str(out_path),
                    "model_used": used_model,
                }
            )

            print(f"   ✅ Guardado: {out_path}")

    # Guardar manifest
    manifest_path = out_root / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\n🧾 Manifest guardado: {manifest_path}")
    print("✅ Carrusel generado.")


# ----------------------------
# Main (config manual como pediste)
# ----------------------------
def main():
    load_dotenv()
    # ---------------------------------------------------------
    # CONFIGURACIÓN MANUAL
    # ---------------------------------------------------------
    PRODUCT_NAME = "tenis_barbara"   # Nombre o slug del producto
    NUM_ANGULOS = 1                    # Cantidad de ángulos (default 1)
    OUTPUT_ROOT = "output"             # Carpeta raíz de salida
    MAX_REF_IMAGES = 5                 # Máximo de imágenes de referencia (Gemini 3 soporta hasta 14)
    IMAGE_SIZE = "4K"                  # "1K" | "2K" | "4K"  (K MAYÚSCULA)
    ASPECT_RATIO = "1:1"               # Mantener 1:1 para carrusel

    # Modelos a probar en orden de prioridad
    MODEL_CANDIDATES = ["gemini-3-pro-image-preview"]

    # API KEY (usa env var GEMINI_API_KEY)
    API_KEY = os.getenv("GEMINI_API_KEY")
    if not API_KEY:
        raise ValueError("No se encontró GEMINI_API_KEY en las variables de entorno.")
    # ---------------------------------------------------------

    run_carrusel_generation(
        product_name=PRODUCT_NAME,
        num_angulos=NUM_ANGULOS,
        output_root=OUTPUT_ROOT,
        api_key=API_KEY,
        max_ref_images=MAX_REF_IMAGES,
        model_candidates=MODEL_CANDIDATES,
        image_size=IMAGE_SIZE,
        aspect_ratio=ASPECT_RATIO,
    )


if __name__ == "__main__":
    main()
