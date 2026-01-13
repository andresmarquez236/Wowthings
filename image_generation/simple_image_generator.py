import os
import re
import json
import time
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dotenv import load_dotenv
from PIL import Image, ImageOps
from google import genai
from google.genai import types

import sys
sys.path.append(os.getcwd())

from utils.logger import setup_logger
logger = setup_logger("SimpleImgGen_V1")

# ---------------------------
# Utilidades base
# ---------------------------

def slugify(name: str) -> str:
    s = name.strip().lower()
    s = s.replace("-", "_")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def resolve_product_dir(output_root: Path, product_name: str) -> Path:
    direct = output_root / product_name
    if direct.exists() and direct.is_dir():
        return direct

    slug_dir = output_root / slugify(product_name)
    if slug_dir.exists() and slug_dir.is_dir():
        return slug_dir

    # Fallback: buscar en primer nivel
    for p in output_root.iterdir():
        if p.is_dir() and p.name == product_name:
            return p

    raise FileNotFoundError(
        f"No encontré carpeta del producto. Intenté:\n"
        f" - {direct}\n"
        f" - {slug_dir}\n"
        f"Revisa product_name u output_root."
    )

def find_image_json(product_dir: Path, product_name: str) -> Path:
    """Busca nanobanana_image_<product>.json"""
    # 1. Exacto
    preferred = product_dir / f"nanobanana_image_{product_name}.json"
    if preferred.exists():
        return preferred
    
    # 2. Con slug
    slug = slugify(product_name)
    preferred_slug = product_dir / f"nanobanana_image_{slug}.json"
    if preferred_slug.exists():
        return preferred_slug

    # 3. Patrón *image*.json
    candidates = list(product_dir.glob("*image*.json"))
    # Filtrar los que no sean 'carrusel' si es posible, aunque 'image' suele ser específico
    candidates = [p for p in candidates if "carrusel" not in p.name.lower()]
    
    if candidates:
        # Preferir el que tenga el nombre del producto
        for c in candidates:
            if product_name in c.name or slug in c.name:
                return c
        return candidates[0]

    raise FileNotFoundError(
        f"No encontré JSON de imagen single (nanobanana_image_*) en {product_dir}."
    )

def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def list_product_images(product_dir: Path) -> List[Path]:
    img_dir = product_dir / "product_images"
    if not img_dir.exists() or not img_dir.is_dir():
        raise FileNotFoundError(f"No existe la carpeta: {img_dir}")

    exts = {".jpg", ".jpeg", ".png", ".webp"}
    imgs = [p for p in img_dir.iterdir() if p.is_file() and p.suffix.lower() in exts]
    imgs.sort(key=lambda p: p.name.lower())

    if not imgs:
        raise FileNotFoundError(f"No encontré imágenes en: {img_dir}")
    return imgs

def load_pil_images(image_paths: Sequence[Path], max_images: int = 14) -> List[Image.Image]:
    selected = list(image_paths)[:max_images]
    out: List[Image.Image] = []
    for p in selected:
        img = Image.open(p)
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        out.append(img)
    return out

# ---------------------------
# Rate limit / backoff (429)
# ---------------------------

@dataclass
class RateLimiter:
    rpm: float
    _next_time: float = 0.0

    def __post_init__(self):
        if self.rpm <= 0:
            raise ValueError("rpm debe ser > 0")

    @property
    def min_interval(self) -> float:
        return 60.0 / float(self.rpm)

    def wait_turn(self):
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = time.monotonic() + self.min_interval

def _safe_get_code_status(e: Exception) -> Tuple[Optional[int], str]:
    code = getattr(e, "code", None)
    status = str(getattr(e, "status", "") or "")
    msg = str(e)
    if code is None:
        m = re.search(r"\bcode\b[=: ]+(\d{3})\b", msg)
        if m:
            try:
                code = int(m.group(1))
            except Exception:
                pass
    return code, (status + " " + msg).upper()

def _parse_retry_delay_seconds(e: Exception) -> Optional[float]:
    s = str(e)
    m = re.search(r"retryDelay['\":= ]+\"?(\d+(?:\.\d+)?)s\"?", s, re.IGNORECASE)
    if m:
        return float(m.group(1))
    m = re.search(r"retry[- ]after[: ]+(\d+(?:\.\d+)?)", s, re.IGNORECASE)
    if m:
        return float(m.group(1))
    m = re.search(r"retry[_ ]delay['\":= ]+(\d+(?:\.\d+)?)", s, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return None

def _is_retriable(e: Exception) -> bool:
    code, up = _safe_get_code_status(e)
    if code in (429, 500, 503, 504):
        return True
    if "RESOURCE_EXHAUSTED" in up or "QUOTA" in up or "RATE LIMIT" in up:
        return True
    return False

# ---------------------------
# Gemini image generation
# ---------------------------

def create_client(api_key: Optional[str]) -> genai.Client:
    return genai.Client(api_key=api_key) if api_key else genai.Client()

def _build_image_config_4k() -> types.GenerateContentConfig:
    try:
        return types.GenerateContentConfig(
            response_modalities=["IMAGE"],
            image_config=types.ImageConfig(aspect_ratio="1:1", image_size="4K"),
            candidate_count=1,
        )
    except TypeError:
        # Fallback
        return types.GenerateContentConfig(
            response_modalities=["IMAGE"],
            candidate_count=1,
        )

def extract_images_from_response(response: Any) -> List[Image.Image]:
    images: List[Image.Image] = []
    parts = None
    if hasattr(response, "parts") and response.parts:
        parts = response.parts
    elif getattr(response, "candidates", None):
        try:
            parts = response.candidates[0].content.parts
        except Exception:
            parts = None

    if not parts:
        return images

    for part in parts:
        try:
            if hasattr(part, "as_image"):
                img = part.as_image()
                if isinstance(img, Image.Image):
                    images.append(img)
                    continue
        except Exception:
            pass

        inline = getattr(part, "inline_data", None)
        if inline and getattr(inline, "data", None):
            try:
                from io import BytesIO
                images.append(Image.open(BytesIO(inline.data)))
            except Exception:
                continue
    return images

def ensure_square_4k(img: Image.Image) -> Image.Image:
    w, h = img.size
    if w != h:
        side = min(w, h)
        img = ImageOps.fit(img, (side, side), method=Image.LANCZOS, centering=(0.5, 0.5))

    if img.size != (4096, 4096):
        img = img.resize((4096, 4096), resample=Image.LANCZOS)
    return img

def generate_one_image_4k(
    prompt_text: str,
    ref_images: List[Image.Image],
    client: genai.Client,
    model_candidates: Sequence[str],
    limiter: Optional[RateLimiter] = None,
    rpm_limit: float = 6.0,
    retries: int = 6,
    base_backoff: float = 2.0,
    max_backoff: float = 90.0,
) -> Tuple[Image.Image, str]:
    if limiter is None:
        limiter = RateLimiter(rpm=rpm_limit)

    cfg = _build_image_config_4k()
    last_err: Optional[Exception] = None

    for model_name in model_candidates:
        for attempt in range(1, retries + 1):
            try:
                limiter.wait_turn()

                response = client.models.generate_content(
                    model=model_name,
                    contents=[prompt_text, *ref_images],
                    config=cfg,
                )

                imgs = extract_images_from_response(response)
                if not imgs:
                    raise RuntimeError("La respuesta no trajo imagen.")
                return imgs[0], model_name

            except Exception as e:
                last_err = e
                if not _is_retriable(e):
                    pass # Try next model or fail if last
                
                # Check if it was non-retriable but we want to try next model?
                # logic from miniatura: if not retriable, raise.
                # But if we have multiple models, maybe we want to try next model on some errors?
                # For now, sticking to miniatura logic: non-retriable = fail fast unless we handle specific codes.
                if not _is_retriable(e):
                     # If it's a 400 or something fatal, maybe we should just fail?
                     # Let's print and break inner loop to try next model
                     logger.warning(f"Error NO retriable en '{model_name}': {e}. Probando siguiente modelo...")
                     break 

                retry_delay = _parse_retry_delay_seconds(e)
                if retry_delay is not None:
                    wait_s = min(max_backoff, retry_delay + random.uniform(1.0, 4.0))
                else:
                    exp = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
                    wait_s = exp + random.uniform(0.0, min(5.0, exp * 0.15))

                code, up = _safe_get_code_status(e)
                logger.warning(f"429/Error en '{model_name}'. Backoff {wait_s:.1f}s (attempt {attempt}/{retries})")
                time.sleep(wait_s)

        logger.warning(f"… agoté retries en '{model_name}', probando siguiente modelo")

    raise RuntimeError(f"No se pudo generar imagen con ningún modelo. Último error: {last_err}")

# ---------------------------
# Prompt builder
# ---------------------------

def build_full_prompt_json_string(prompt_obj: Dict[str, Any]) -> str:
    # Deep copy
    payload = json.loads(json.dumps(prompt_obj))

    # Inject strict reference image rule
    input_assets = payload.get("input_assets")
    if not isinstance(input_assets, dict):
        input_assets = {}
    
    strong_rule = "CRITICAL: The provided images are the EXACT product reference. You MUST generate the product exactly as shown in these reference images, maintaining its key visual features, logo placement (if any), and packaging details."
    
    current_rule = str(input_assets.get("product_lock_rule", "")).strip()
    input_assets["product_lock_rule"] = f"{strong_rule} {current_rule}"
    payload["input_assets"] = input_assets

    # Force 1:1 format in JSON just in case
    fmt = payload.get("format") or {}
    if not isinstance(fmt, dict):
        fmt = {}
    fmt["aspect_ratio"] = "1:1"
    # fmt["resolution"] = "4096x4096" # Optional
    payload["format"] = fmt

    return json.dumps(payload, ensure_ascii=False, indent=2)

# ---------------------------
# Orquestación principal
# ---------------------------

def run_simple_image_generation(
    product_name: str,
    output_root: str = "output",
    api_key: Optional[str] = None,
    max_ref_images: int = 5,
    model_candidates: Optional[List[str]] = None,
    rpm_limit: float = 6.0,
    overwrite: bool = False,
):
    product_dir = resolve_product_dir(Path(output_root), product_name)
    json_path = find_image_json(product_dir, product_name)
    data = load_json(json_path)

    results_by_angle = data.get("results_by_angle", [])
    results_by_angle = data.get("results_by_angle", [])
    if not results_by_angle:
        logger.warning("No results_by_angle found in JSON.")
        return

    # Load images
    img_paths = list_product_images(product_dir)
    ref_images = load_pil_images(img_paths, max_images=max_ref_images)
    logger.info(f"Loaded {len(ref_images)} reference images.")

    # Client
    client = create_client(api_key)

    if not model_candidates:
        model_candidates = ["gemini-3-pro-image-preview", "gemini-2.5-flash-image"]

    out_dir = product_dir / "generated_simple_images"
    out_dir.mkdir(parents=True, exist_ok=True)

    limiter = RateLimiter(rpm=rpm_limit)
    manifest_results = []

    for angle in results_by_angle:
        angle_id = angle.get("angle_id", "UNKNOWN")
        angle_name = angle.get("angle_name", "")
        
        single_prompt = angle.get("single_image_prompt")
        if not single_prompt:
            logger.warning(f"No single_image_prompt for {angle_id}. Skipping.")
            continue

        prompt_id = str(single_prompt.get("thumbnail") or f"{angle_id}_generated")
        # Limpiar nombre de archivo
        safe_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", Path(prompt_id).stem)
        out_filename = f"{angle_id}_{safe_name}.png"
        out_path = out_dir / out_filename

        if out_path.exists() and not overwrite:
            logger.info(f"[SKIP] Ya existe {out_filename}")
            continue

        logger.info(f"Generando: {angle_id} -> {out_filename}")

        # Build prompt
        prompt_str = build_full_prompt_json_string(single_prompt)

        # Save prompt sent
        prompt_sent_path = out_dir / f"{angle_id}_{safe_name}_prompt.json"
        with prompt_sent_path.open("w", encoding="utf-8") as f:
            f.write(prompt_str)

        # Generate
        try:
            img, used_model = generate_one_image_4k(
                prompt_text=prompt_str,
                ref_images=ref_images,
                client=client,
                model_candidates=model_candidates,
                limiter=limiter,
                rpm_limit=rpm_limit,
            )

            img4k = ensure_square_4k(img)
            img4k.save(out_path, format="PNG")
            logger.info(f"Guardado: {out_filename} ({used_model})")

            manifest_results.append({
                "angle_id": angle_id,
                "file": str(out_path),
                "model": used_model
            })

        except Exception as e:
            logger.error(f"Error generando {angle_id}: {e}")

    # Manifest
    with (out_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump({
            "product": product_name,
            "generated_at": time.time(),
            "results": manifest_results
        }, f, indent=2)

# ---------------------------
# MAIN
# ---------------------------

def main():
    load_dotenv()

    # CONFIG
    PRODUCT_NAME = "bee_venom_bswell"  # Cambiar según necesidad
    OUTPUT_ROOT = "output"
    MAX_REF_IMAGES = 5
    MODEL_CANDIDATES = ["gemini-3-pro-image-preview"]
    API_KEY = os.getenv("GEMINI_API_KEY")
    RPM_LIMIT = 6.0
    OVERWRITE = True

    run_simple_image_generation(
        product_name=PRODUCT_NAME,
        output_root=OUTPUT_ROOT,
        api_key=API_KEY,
        max_ref_images=MAX_REF_IMAGES,
        model_candidates=MODEL_CANDIDATES,
        rpm_limit=RPM_LIMIT,
        overwrite=OVERWRITE,
    )

if __name__ == "__main__":
    main()
