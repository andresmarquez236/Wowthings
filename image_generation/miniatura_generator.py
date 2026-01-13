from __future__ import annotations

import json
import os
import random
import re
import time
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
logger = setup_logger("MiniaturaGen_V1")


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

    raise FileNotFoundError(
        f"No encontré carpeta del producto. Intenté:\n"
        f" - {direct}\n"
        f" - {slug_dir}\n"
        f"Revisa product_name u output_root."
    )


def find_thumbnails_json(product_dir: Path) -> Path:
    slug = product_dir.name
    candidates: List[Path] = []

    for p in product_dir.glob("*.json"):
        n = p.name.lower()
        if "thumbnail" in n or "thumbnails" in n:
            candidates.append(p)

    if not candidates:
        for p in product_dir.rglob("*.json"):
            n = p.name.lower()
            if "thumbnail" in n or "thumbnails" in n:
                candidates.append(p)

    if not candidates:
        raise FileNotFoundError(
            f"No encontré JSON de thumbnails en {product_dir}. "
            f"Esperaba algo como 'nanobanana_thumbnails_{slug}.json'."
        )

    candidates.sort(key=lambda p: (slug not in p.name, len(p.name)))
    return candidates[0]


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
    """
    Carga imágenes con PIL. Cap por request (recomendado <= 14).
    """
    selected = list(image_paths)[:max_images]
    out: List[Image.Image] = []
    for p in selected:
        img = Image.open(p)
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        out.append(img)
    return out


def load_pil_images_by_names(product_dir: Path, filenames: Sequence[str]) -> List[Image.Image]:
    """
    Carga imágenes por nombre exacto dentro de product_images/.
    Útil para clonar tu test manual (abeja1/2/3).
    """
    img_dir = product_dir / "product_images"
    out: List[Image.Image] = []
    for fn in filenames:
        p = img_dir / fn
        if not p.exists():
            raise FileNotFoundError(f"No existe imagen requerida: {p}")
        img = Image.open(p)
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        out.append(img)
    return out


def extract_angles(thumbnails_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = thumbnails_data.get("results")
    if isinstance(results, list):
        return results
    raise ValueError("Estructura inesperada del JSON: no encontré 'results' como lista.")


def select_top_angles(angles: List[Dict[str, Any]], num_angulos: int) -> List[Dict[str, Any]]:
    filtered = []
    for a in angles:
        rank = a.get("angle_rank")
        if isinstance(rank, int) and 1 <= rank <= num_angulos:
            filtered.append(a)
    filtered.sort(key=lambda x: int(x.get("angle_rank", 10**9)))
    return filtered


# ---------------------------
# Rate limit / backoff (429)
# ---------------------------

@dataclass
class RateLimiter:
    """
    Throttle simple por RPM: asegura mínimo X segundos entre requests.
    """
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
    # Si api_key es None, el SDK usa GEMINI_API_KEY del entorno.
    return genai.Client(api_key=api_key) if api_key else genai.Client()


def _build_image_config_4k() -> types.GenerateContentConfig:
    """
    Config 1:1 + 4K, con fallbacks si cambia la firma del SDK.
    """
    try:
        return types.GenerateContentConfig(
            response_modalities=["IMAGE"],
            image_config=types.ImageConfig(aspect_ratio="1:1", image_size="4K"),
            candidate_count=1,
        )
    except TypeError:
        # fallback 1
        try:
            return types.GenerateContentConfig(
                response_modalities=["IMAGE"],
                aspect_ratio="1:1",
                resolution="4K",
                number_of_images=1,
            )
        except TypeError:
            # fallback 2 (mínimo)
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
        # SDK moderno
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
    """
    Garantiza 1:1 y 4096x4096.
    """
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
    """
    Genera 1 imagen (1:1, 4K) usando prompt TAL CUAL + imágenes.
    Retorna (PIL_image, model_usado).
    """
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
                    contents=[prompt_text, *ref_images],  # Prompt + imágenes
                    config=cfg,
                )

                imgs = extract_images_from_response(response)
                if not imgs:
                    raise RuntimeError("La respuesta no trajo imagen.")
                return imgs[0], model_name

            except Exception as e:
                last_err = e
                if not _is_retriable(e):
                    raise

                retry_delay = _parse_retry_delay_seconds(e)
                if retry_delay is not None:
                    wait_s = min(max_backoff, retry_delay + random.uniform(1.0, 4.0))
                else:
                    exp = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
                    wait_s = exp + random.uniform(0.0, min(5.0, exp * 0.15))

                code, up = _safe_get_code_status(e)
                if code == 429 or "RESOURCE_EXHAUSTED" in up:
                    logger.warning(f"429/Quota en '{model_name}'. Backoff {wait_s:.1f}s (attempt {attempt}/{retries})")
                else:
                    logger.warning(f"Error retriable en '{model_name}'. Backoff {wait_s:.1f}s (attempt {attempt}/{retries})")

                time.sleep(wait_s)

        logger.warning(f"… agoté retries en '{model_name}', probando siguiente modelo")

    raise RuntimeError(f"No se pudo generar imagen con ningún modelo. Último error: {last_err}")


# ---------------------------
# Prompt builder (OBJETO COMPLETO, 1x1, opcional 4K)
# ---------------------------

def build_full_prompt_json_string(
    prompt_obj: Dict[str, Any],
    force_format_1x1: bool = True,
    force_resolution_4k_in_prompt: bool = False,
) -> str:
    """
    Convierte el prompt_obj COMPLETO a string JSON para enviarlo al modelo.
    Esto clona tu prueba manual: envías task/format/negative_prompt/text_overlays, etc.

    - force_format_1x1: asegura aspect_ratio="1:1" en el payload.
    - force_resolution_4k_in_prompt: opcional, cambia format.resolution a "4096x4096" dentro del payload.
      (Ojo: NO es obligatorio si ya pides 4K via config del modelo.)
    """
    # deep copy sin dependencias
    payload = json.loads(json.dumps(prompt_obj))

    # Inject strict reference image rule (CRITICAL for Gemini to use the images)
    input_assets = payload.get("input_assets")
    if not isinstance(input_assets, dict):
        input_assets = {}
    
    # Strong rule to force reference image usage
    strong_rule = "CRITICAL: The provided images are the EXACT product reference. You MUST generate the product exactly as shown in these reference images, maintaining its key visual features, logo placement (if any), and packaging details. Do NOT hallucinate a different model."
    
    current_rule = str(input_assets.get("product_lock_rule", "")).strip()
    # Prepend our strong rule
    input_assets["product_lock_rule"] = f"{strong_rule} {current_rule}"
    payload["input_assets"] = input_assets

    if force_format_1x1:
        fmt = payload.get("format") or {}
        if not isinstance(fmt, dict):
            fmt = {}
        fmt["aspect_ratio"] = "1:1"
        if force_resolution_4k_in_prompt:
            fmt["resolution"] = "4096x4096"
        payload["format"] = fmt

    return json.dumps(payload, ensure_ascii=False, indent=2)


# ---------------------------
# Orquestación principal
# ---------------------------

def run_product_generation(
    product_name: str,
    num_angulos: int,
    output_root: str = "output",
    api_key: Optional[str] = None,
    max_ref_images: int = 14,
    model_candidates: Optional[List[str]] = None,
    rpm_limit: float = 6.0,
    overwrite: bool = False,
    # Si quieres clonar tu test manual, pon lista como ["abeja1.png","abeja2.jpeg","abeja3.jpeg"]
    ref_image_files: Optional[List[str]] = None,
    # Prompt object completo:
    force_prompt_format_1x1: bool = True,
    force_prompt_resolution_4k: bool = False,  # recomendado False: pides 4K por config, no por payload
) -> Dict[str, Any]:
    """
    - Busca output/<product_name>/
    - Lee JSON de thumbnails
    - Toma angle_rank 1..num_angulos
    - Por cada prompt (V1,V2,V3): ENVÍA EL OBJETO COMPLETO (dict) serializado a JSON + imágenes
    - Genera y guarda 1 imagen 1:1 4K por prompt
    - Guarda manifest y prompt enviado por cada output
    """
    if num_angulos < 1:
        raise ValueError("num_angulos debe ser >= 1")

    product_dir = resolve_product_dir(Path(output_root), product_name)
    thumbs_path = find_thumbnails_json(product_dir)
    thumbs_data = load_json(thumbs_path)

    angles = extract_angles(thumbs_data)
    selected_angles = select_top_angles(angles, num_angulos)
    if not selected_angles:
        raise ValueError(f"No encontré ángulos 1..{num_angulos} en {thumbs_path}")

    # Imágenes de referencia
    if ref_image_files:
        ref_images = load_pil_images_by_names(product_dir, ref_image_files)
    else:
        img_paths = list_product_images(product_dir)
        ref_images = load_pil_images(img_paths, max_images=max_ref_images)

    # Cliente Gemini
    client = create_client(api_key)

    # Modelos por prioridad
    if not model_candidates:
        model_candidates = ["gemini-3-pro-image-preview", "gemini-2.5-flash-image"]

    out_dir = product_dir / "generated_thumbnails_gemini_4k"
    out_dir.mkdir(parents=True, exist_ok=True)

    limiter = RateLimiter(rpm=rpm_limit)
    results: List[Dict[str, Any]] = []

    for angle in selected_angles:
        angle_rank = int(angle.get("angle_rank"))
        angle_name = str(angle.get("angle_name", "")).strip()
        prompts = angle.get("prompts") or []
        if not isinstance(prompts, list):
            continue

        for j, p_obj in enumerate(prompts, start=1):
            if not isinstance(p_obj, dict):
                continue

            prompt_id = str(p_obj.get("thumbnail") or f"A{angle_rank}_V{j}")
            out_path = out_dir / f"{prompt_id}__4k.png"

            if out_path.exists() and not overwrite:
                logger.info(f"[SKIP] Ya existe {out_path.name}")
                results.append({
                    "angle_rank": angle_rank,
                    "angle_name": angle_name,
                    "prompt_thumbnail_id": prompt_id,
                    "out_file": str(out_path),
                    "model": "SKIPPED_EXISTING",
                })
                continue

            # ✅ ENVIAR PROMPT COMO OBJETO COMPLETO, UNO A UNO
            prompt_text = build_full_prompt_json_string(
                prompt_obj=p_obj,
                force_format_1x1=force_prompt_format_1x1,
                force_resolution_4k_in_prompt=force_prompt_resolution_4k,
            )

            # Guardar el prompt exacto que se envía (para comparar con tu test manual)
            prompt_sent_path = out_dir / f"{prompt_id}__prompt_sent.json"
            with prompt_sent_path.open("w", encoding="utf-8") as f:
                f.write(prompt_text)

            img, used_model = generate_one_image_4k(
                prompt_text=prompt_text,
                ref_images=ref_images,
                client=client,
                model_candidates=model_candidates,
                limiter=limiter,
                rpm_limit=rpm_limit,
            )

            img4k = ensure_square_4k(img)
            img4k.save(out_path, format="PNG", optimize=False)

            logger.info(f"[OK] angle={angle_rank} prompt={prompt_id} -> {out_path.name} | model={used_model}")

            results.append({
                "angle_rank": angle_rank,
                "angle_name": angle_name,
                "prompt_thumbnail_id": prompt_id,
                "out_file": str(out_path),
                "model": used_model,
                "prompt_sent_file": str(prompt_sent_path),
                "used_ref_images": len(ref_images),
            })

    manifest: Dict[str, Any] = {
        "product_dir": str(product_dir),
        "thumbnails_json": str(thumbs_path),
        "num_angulos": num_angulos,
        "used_ref_images": len(ref_images),
        "max_ref_images": max_ref_images,
        "rpm_limit": rpm_limit,
        "models": model_candidates,
        "results": results,
    }

    with (out_dir / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    return manifest


# ---------------------------
# MAIN (CONFIGURACIÓN MANUAL)
# ---------------------------

def main():
    load_dotenv()

    # ---------------------------------------------------------
    # CONFIGURACIÓN MANUAL
    # ---------------------------------------------------------
    PRODUCT_NAME = "tenis_barbara"  # Nombre o slug del producto
    NUM_ANGULOS = 1                    # Cantidad de ángulos (1 -> angle_rank=1)
    OUTPUT_ROOT = "output"             # Carpeta raíz de salida

    # Para clonar tu test manual, recomiendo:
    # - MAX_REF_IMAGES = 3
    # - REF_IMAGE_FILES = ["abeja1.png", "abeja2.jpeg", "abeja3.jpeg"]
    MAX_REF_IMAGES = 5

    # (Opcional) Si defines esto, ignora MAX_REF_IMAGES y usa EXACTAMENTE esos archivos.
    REF_IMAGE_FILES = None
    # REF_IMAGE_FILES = ["abeja1.png", "abeja2.jpeg", "abeja3.jpeg"]

    # Modelos a probar en orden de prioridad
    MODEL_CANDIDATES = ["gemini-3-pro-image-preview"]

    # API KEY (Opcional, si es None usa os.environ["GEMINI_API_KEY"])
    API_KEY = os.getenv("GEMINI_API_KEY")

    # Rate limit preventivo (sube/baja según tu cuota)
    RPM_LIMIT = 6.0

    # Si True, regenera aunque ya existan PNGs
    OVERWRITE = True

    # Forzar 1:1 dentro del prompt JSON (recomendado True)
    FORCE_PROMPT_FORMAT_1X1 = True

    # Forzar "resolution": "4096x4096" dentro del prompt JSON (opcional; normalmente NO hace falta)
    # La resolución real la estás pidiendo por config del modelo (4K).
    FORCE_PROMPT_RESOLUTION_4K = False
    # ---------------------------------------------------------

    run_product_generation(
        product_name=PRODUCT_NAME,
        num_angulos=NUM_ANGULOS,
        output_root=OUTPUT_ROOT,
        api_key=API_KEY,
        max_ref_images=MAX_REF_IMAGES,
        model_candidates=MODEL_CANDIDATES,
        rpm_limit=RPM_LIMIT,
        overwrite=OVERWRITE,
        ref_image_files=REF_IMAGE_FILES,
        force_prompt_format_1x1=FORCE_PROMPT_FORMAT_1X1,
        force_prompt_resolution_4k=FORCE_PROMPT_RESOLUTION_4K,
    )


if __name__ == "__main__":
    main()
