import os
import json
import time
import mimetypes
from pathlib import Path
import requests
from dotenv import load_dotenv
from utils.logger import setup_logger

logger = setup_logger("Shopify.DeployImages")

load_dotenv()

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-01")

# === CONFIG ===
COMPARE_SECTION_ID     = os.getenv("COMPARE_SECTION_ID", "086a98ac-209d-4db2-9d62-5b0fe9663693").strip()
PAIN_SECTION_ID        = os.getenv("PAIN_SECTION_ID", "3bc3f381-5c12-4e1e-9f92-289fb87d308d").strip()
MULTICOLUMN_SECTION_ID = os.getenv("MULTICOLUMN_SECTION_ID", "50f8db15-9a00-4bfb-8176-786845498504").strip()

BEFORE_FILENAME = "before_image.png"
AFTER_FILENAME  = "after_image.png"
PAIN_FILENAME   = "pain_image.png"
FEATURED_REVIEW_FILENAME = "featured_review_1.png"

BENEFITS_DIRNAME = "benefits_images"      # Folder for benefits (first multicolumn)
SOCIAL_DIRNAME   = "social_proof_images"  # Folder for social proof (second multicolumn)
SECOND_IWT_FILENAME = "social_featured_hero.png" # The hero image from social proof

IMAGE_WITH_TEXT_TYPES = {"image-with-text", "image_with_text"}


def require_env(*keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"❌ Faltan variables en .env: {', '.join(missing)}")


def graphql(shop_url: str, access_token: str, query: str, variables: dict, retries: int = 5, backoff_factor: int = 2):
    url = f"https://{shop_url}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=60)
            
            if r.status_code == 429:
                # Rate limit
                sleep_time = backoff_factor ** attempt
                logger.warning(f"Rate limit generated (429). Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
                continue
                
            if r.status_code >= 500:
                # Server error
                sleep_time = backoff_factor ** attempt
                logger.warning(f"Server error ({r.status_code}). Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
                continue
                
            if r.status_code != 200:
                 raise RuntimeError(f"❌ GraphQL HTTP {r.status_code}: {r.text}")
                 
            payload = r.json()
            if "errors" in payload:
                # Business logic error, usually not transient, but check throttle
                if "Throttled" in str(payload['errors']):
                     sleep_time = backoff_factor ** attempt
                     logger.warning(f"Shopify Throttled. Retrying in {sleep_time}s...")
                     time.sleep(sleep_time)
                     continue
                raise RuntimeError(f"❌ GraphQL errors: {payload['errors']}")
            
            return payload["data"]
            
        except requests.exceptions.RequestException as e:
            # Network-level errors (DNS, Timeout, Connection Refused)
            sleep_time = backoff_factor ** attempt
            logger.warning(f"Network error: {e}. Retrying {attempt+1}/{retries} in {sleep_time}s...")
            time.sleep(sleep_time)
    
    raise RuntimeError(f"❌ Failed to execute GraphQL query after {retries} attempts.")


STAGED_UPLOADS_CREATE = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
    userErrors { field message }
  }
}
"""

FILE_CREATE = """
mutation fileCreate($files: [FileCreateInput!]!) {
  fileCreate(files: $files) {
    files {
      id
      fileStatus
      ... on MediaImage {
        image { url }
      }
    }
    userErrors { field message }
  }
}
"""

NODE_MEDIAIMAGE_URL = """
query node($id: ID!) {
  node(id: $id) {
    ... on MediaImage {
      id
      fileStatus
      image { url }
    }
  }
}
"""


def upload_to_staged_target(target: dict, file_path: Path):
    data = {p["name"]: p["value"] for p in target["parameters"]}
    with open(file_path, "rb") as f:
        files = {"file": f}
        r = requests.post(target["url"], data=data, files=files, timeout=120)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"❌ Error subiendo a staged target: {r.status_code} {r.text}")


def wait_for_mediaimage_url(shop_url: str, access_token: str, media_id: str, tries: int = 20, sleep_s: int = 2):
    for _ in range(tries):
        data = graphql(shop_url, access_token, NODE_MEDIAIMAGE_URL, {"id": media_id})
        node = data.get("node")
        if node and node.get("image") and node["image"].get("url"):
            return {"fileStatus": node.get("fileStatus"), "url": node["image"]["url"]}
        time.sleep(sleep_s)
    return {"fileStatus": None, "url": None}


def upload_image_to_shopify_files(image_path: Path, shop_url: str, access_token: str, unique_prefix: str = None):
    mime, _ = mimetypes.guess_type(str(image_path))
    if not mime or not mime.startswith("image/"):
        raise ValueError(f"❌ No parece imagen: {image_path} (mime={mime})")

    file_size_mb = image_path.stat().st_size / (1024 * 1024)
    if file_size_mb > 4:
        logger.warning(f"Imagen {image_path.name} pesa {file_size_mb:.2f}MB. Comprimiendo...")
        from PIL import Image
        with Image.open(image_path) as img:
            if img.mode in ('RGBA', 'P'): img = img.convert('RGB')
            # Resize if huge
            if max(img.size) > 4000:
                img.thumbnail((4000, 4000), Image.Resampling.LANCZOS)
            
            # Save compressed temp file
            temp_path = image_path.with_suffix(".compressed.jpg")
            img.save(temp_path, "JPEG", quality=80, optimize=True)
            logger.info(f"Comprimida a: {temp_path.stat().st_size / (1024*1024):.2f}MB")
            
            # Recursive call with smaller file, then clean up
            try:
                # Pass prefix recursively
                result = upload_image_to_shopify_files(temp_path, shop_url, access_token, unique_prefix)
                return result
            finally:
                if temp_path.exists(): temp_path.unlink()

    # Determine unique filename for Shopify
    final_name = image_path.name
    if unique_prefix:
        # Sanitize prefix just in case
        safe_prefix = unique_prefix.replace("_", "-").replace(" ", "-")
        # Ensure we don't double prefix if logic is re-run or temp file usage
        if not final_name.startswith(safe_prefix):
            final_name = f"{safe_prefix}_{final_name}"

    # 1) stagedUploadsCreate (FILE)
    staged = graphql(shop_url, access_token, STAGED_UPLOADS_CREATE, {
        "input": [{
            "filename": final_name,
            "mimeType": mime,
            "httpMethod": "POST",
            "resource": "FILE",
        }]
    })

    errs = staged["stagedUploadsCreate"]["userErrors"]
    if errs:
        raise RuntimeError(f"❌ stagedUploadsCreate userErrors: {errs}")

    target = staged["stagedUploadsCreate"]["stagedTargets"][0]

    # 2) subir binario al staging
    upload_to_staged_target(target, image_path)

    # 3) crear File en Shopify
    created = graphql(shop_url, access_token, FILE_CREATE, {
        "files": [{
            "contentType": "IMAGE",
            "originalSource": target["resourceUrl"],
            "alt": image_path.stem,
            "filename": final_name,
        }]
    })

    errs2 = created["fileCreate"]["userErrors"]
    if errs2:
        raise RuntimeError(f"❌ fileCreate userErrors: {errs2}")

    f = created["fileCreate"]["files"][0]
    media_id = f["id"]
    
    # CRITICAL FIX: Use the ACTUAL filename assigned by Shopify (handling duplicates like _1)
    # The API 'fileCreate' returns the file object, which should contain the final 'filename'.
    # If not present in fileCreate response, we rely on the `node` query in `wait_for_mediaimage_url`?
    # Actually, fileCreate V2 usually returns 'filename'.
    remote_filename = f.get("filename") or final_name
    logger.info(f"   ✅ Uploaded as: {remote_filename} (ID: {media_id})")

    # 4) esperar url final
    ready = wait_for_mediaimage_url(shop_url, access_token, media_id)

    return {
        "id": media_id,
        "fileStatus": ready["fileStatus"] or f.get("fileStatus"),
        "url": ready["url"] or (f.get("image") or {}).get("url"),
        # Referencia CORRECTA con el nombre real en servidor:
        "shopify_ref": f"shopify://shop_images/{remote_filename}"
    }


def upload_to_shopify_theme_asset(local_filepath: str, shopify_filename: str):
    require_env("SHOP_URL", "ACCESS_TOKEN", "THEME_ID")

    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    theme_id = os.getenv("THEME_ID")

    url = f"https://{shop_url}/admin/api/{API_VERSION}/themes/{theme_id}/assets.json"
    content_string = Path(local_filepath).read_text(encoding="utf-8")

    payload = {"asset": {"key": shopify_filename, "value": content_string}}
    headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

    logger.info(f"Subiendo template JSON al tema: {shopify_filename} ...")
    r = requests.put(url, headers=headers, json=payload, timeout=60)

    if r.status_code in (200, 201):
        logger.info("Template JSON subido correctamente.")
        return True
    logger.error(f"Error subiendo template JSON ({r.status_code}): {r.text}")
    return False


def upload_folder_images_to_files(folder: Path, shop_url: str, access_token: str, filter_keyword: str | None = None, unique_prefix: str = None):
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    files = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in exts])
    
    if filter_keyword:
        files = [p for p in files if filter_keyword in p.name]
        
    if not files:
        logger.warning(f"No hay imágenes en {folder} con filtro '{filter_keyword}' (o carpeta vacía).")
        return [], []

    logger.info(f"Subiendo {len(files)} imágenes de {folder.name}...")
    uploads = []
    for p in files:
        uploads.append(upload_image_to_shopify_files(p, shop_url, access_token, unique_prefix))

    refs = [u["shopify_ref"] for u in uploads]
    return uploads, refs


def find_compare_section_id(template: dict):
    # 1) Si viene por env, úsalo
    if COMPARE_SECTION_ID:
        return COMPARE_SECTION_ID
    # 2) Detectar por type
    for sid, sec in (template.get("sections") or {}).items():
        if sec.get("type") == "compare-image":
            return sid
    return None


def find_pain_image_with_text_section_id(template: dict, compare_sid: str | None):
    sections = template.get("sections") or {}
    # 1) Si viene por env, úsalo
    if PAIN_SECTION_ID and PAIN_SECTION_ID in sections:
        return PAIN_SECTION_ID

    candidates = [sid for sid, sec in sections.items() if sec.get("type") in IMAGE_WITH_TEXT_TYPES]
    if not candidates:
        return None

    # 2) Si hay 'order', toma el primero DESPUÉS del compare
    order = template.get("order")
    if isinstance(order, list) and compare_sid and compare_sid in order:
        idx = order.index(compare_sid)
        for sid in order[idx + 1:]:
            if sid in candidates:
                return sid

    # 3) fallback
    return candidates[0]


def find_next_section_of_type(template: dict, after_sid: str, types: set[str]):
    sections = template.get("sections") or {}
    order = template.get("order")

    if not isinstance(order, list) or after_sid not in order:
        # Fallback: just find any section of that type?
        # Or maybe order is required.
        for sid, sec in sections.items():
            if sec.get("type") in types and sid != after_sid: # Avoid same section if type matches
                return sid
        return None

    idx = order.index(after_sid)
    for sid in order[idx + 1:]:
        sec = sections.get(sid)
        if sec and sec.get("type") in types:
            return sid
    return None


def patch_multicolumn_section(template: dict, multicolumn_sid: str, image_refs: list[str]):
    sections = template.get("sections") or {}
    if multicolumn_sid not in sections:
        raise KeyError(f"❌ No existe multicolumn sid={multicolumn_sid} en sections.")

    sec = sections[multicolumn_sid]
    if sec.get("type") != "multicolumn":
        raise ValueError(f"❌ sid={multicolumn_sid} no es multicolumn (type={sec.get('type')}).")

    blocks = sec.get("blocks")
    if not isinstance(blocks, dict) or not blocks:
        raise ValueError(f"❌ multicolumn sid={multicolumn_sid} no tiene blocks dict.")

    block_order = sec.get("block_order")
    if not isinstance(block_order, list) or not block_order:
        block_order = list(blocks.keys())

    if len(image_refs) < len(block_order):
        logger.warning(f"OJO: Hay {len(block_order)} blocks pero solo {len(image_refs)} imágenes. Se asignarán las primeras {len(image_refs)}.")
    
    # Iterate based on image count to fill available blocks
    for i, bid in enumerate(block_order):
        if i >= len(image_refs):
            break
        b = blocks.get(bid)
        if not isinstance(b, dict):
            continue
        b_settings = b.setdefault("settings", {})
        b_settings["image"] = image_refs[i]

    return multicolumn_sid


def patch_sections(template: dict, out_json_path: Path,
                   before_ref: str, after_ref: str, pain_ref: str, 
                   benefits_refs: list[str], social_refs: list[str], second_iwt_ref: str,
                   featured_review_ref: str = None):
    
    data = template
    sections = data.get("sections", {})

    # 0) Patch Featured Review in Main Section
    if "main" in sections:
        main_sec = sections["main"]
        blocks = main_sec.get("blocks", {})
        # Find block of type 'featuredreview'
        for bid, b in blocks.items():
             if b.get("type") == "featuredreview":
                 b.setdefault("settings", {})["reviewimage"] = featured_review_ref
                 logger.info(f"Main Section: featuredreview patched: {bid}")
                 break

    compare_sid = find_compare_section_id(data)
    if not compare_sid or compare_sid not in sections:
        raise KeyError("❌ No encuentro la sección compare-image.")

    # 1) Compare
    sections[compare_sid].setdefault("settings", {})["image1"] = before_ref
    sections[compare_sid].setdefault("settings", {})["image2"] = after_ref

    # 2) Pain IWT
    pain_sid = find_pain_image_with_text_section_id(data, compare_sid)
    if not pain_sid: raise KeyError("❌ No encuentro sección pain image-with-text.")
    sections[pain_sid].setdefault("settings", {})["image"] = pain_ref
    
    # 3) Benefits Multicolumn (ANCHOR: Pain)
    # The first multicolumn after pain is Benefits
    mc_benefits_sid = find_next_section_of_type(data, after_sid=pain_sid, types={"multicolumn"})
    # Backup: check env var if finding fails?
    if not mc_benefits_sid and MULTICOLUMN_SECTION_ID in sections:
         mc_benefits_sid = MULTICOLUMN_SECTION_ID
         
    if not mc_benefits_sid: raise KeyError("❌ No encuentro multicolumn (benefits) después del pain.")
    patch_multicolumn_section(data, mc_benefits_sid, benefits_refs)
    logger.info(f"multicolumn benefits patched: {mc_benefits_sid}")

    # 4) Social Proof Multicolumn (ANCHOR: Benefits)
    # The next multicolumn after benefits is Social Proof
    mc_social_sid = find_next_section_of_type(data, after_sid=mc_benefits_sid, types={"multicolumn"})
    if not mc_social_sid: raise KeyError("❌ No encuentro multicolumn (social proof) después del benefits.")
    patch_multicolumn_section(data, mc_social_sid, social_refs)
    logger.info(f"multicolumn social proof patched: {mc_social_sid}")

    # 5) Second Image-With-Text (ANCHOR: Social Proof)
    # The next IWT after social proof is the Featured Case
    second_iwt_sid = find_next_section_of_type(data, after_sid=mc_social_sid, types=IMAGE_WITH_TEXT_TYPES)
    if not second_iwt_sid: raise KeyError("❌ No encontré la segunda image-with-text después del social proof.")
    sections[second_iwt_sid].setdefault("settings", {})["image"] = second_iwt_ref
    logger.info(f"second image-with-text patched: {second_iwt_sid}")

    out_json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"JSON parcheado guardado en: {out_json_path}")
    logger.info(f"compare-image patched: {compare_sid}")
    logger.info(f"image-with-text (pain) patched: {pain_sid}")
    return out_json_path


def deploy_pipeline(product_folder: str = None):
    import sys

    require_env("SHOP_URL", "ACCESS_TOKEN", "THEME_ID")

    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")

    if not product_folder:
        product_folder = sys.argv[1] if len(sys.argv) > 1 else "samba_og_vaca_negro_blanco"
    
    logger.info(f"Iniciando despliegue de imágenes para: {product_folder}")

    IMAGES_DIR = Path(f"output/{product_folder}/resultados_landing")
    slug = product_folder.replace("_", "-")
    json_filename = f"product.landing-{slug}.json"

    TEMPLATE_JSON = IMAGES_DIR / json_filename
    if not TEMPLATE_JSON.exists():
        TEMPLATE_JSON = Path(f"output/{json_filename}")

    OUT_DIR = IMAGES_DIR

    before_path = IMAGES_DIR / BEFORE_FILENAME
    after_path  = IMAGES_DIR / AFTER_FILENAME
    pain_path   = IMAGES_DIR / PAIN_FILENAME
    
    # BASIC CHECKS
    for p in [before_path, after_path, pain_path, TEMPLATE_JSON]:
        if not p.exists(): raise FileNotFoundError(f"❌ No existe: {p}")

    logger.info(f"Recursos encontrados en: {IMAGES_DIR}")

    # UPLOADS: CORE
    logger.info("Subiendo BEFORE / AFTER / PAIN...")
    before_up = upload_image_to_shopify_files(before_path, shop_url, access_token, unique_prefix=slug)
    after_up  = upload_image_to_shopify_files(after_path,  shop_url, access_token, unique_prefix=slug)
    pain_up   = upload_image_to_shopify_files(pain_path,   shop_url, access_token, unique_prefix=slug)
    
 


    # UPLOADS: BENEFITS (From finals_images, sorted by name)
    benefits_dir = IMAGES_DIR / BENEFITS_DIRNAME / "finals_images"
    if not benefits_dir.exists():
        # Fallback to main dir if finals doesn't exist (e.g. evaluator didn't run)
        logger.warning(f"finals_images no existe en {benefits_dir}, intentando carpeta padre...")
        benefits_dir = IMAGES_DIR / BENEFITS_DIRNAME
        benefits_uploads, benefits_refs = upload_folder_images_to_files(benefits_dir, shop_url, access_token, filter_keyword="A_macro_hero", unique_prefix=slug)
    else:
        # Upload exact files from finals_images (benefit_1_final.png, etc.)
        benefits_uploads, benefits_refs = upload_folder_images_to_files(benefits_dir, shop_url, access_token, unique_prefix=slug)

    # UPLOADS: SOCIAL PROOF (Filter 'testimonial')
    social_dir = IMAGES_DIR / SOCIAL_DIRNAME
    # We want the 3 testimonials.
    social_uploads, social_refs = upload_folder_images_to_files(social_dir, shop_url, access_token, filter_keyword="testimonial", unique_prefix=slug)
    
    # UPLOADS: SECOND IWT (The Hero)
    second_iwt_path = IMAGES_DIR / SOCIAL_DIRNAME / SECOND_IWT_FILENAME
    logger.info(f"Subiendo Second IWT: {SECOND_IWT_FILENAME}...")
    if not second_iwt_path.exists(): raise FileNotFoundError(f"❌ No existe Second IWT: {second_iwt_path}")
    second_iwt_up = upload_image_to_shopify_files(second_iwt_path, shop_url, access_token, unique_prefix=slug)
    second_iwt_ref = second_iwt_up["shopify_ref"]

    # UPLOADS: FEATURED REVIEW (Profile Pic)
    featured_review_path = IMAGES_DIR / "featured_review_image" / FEATURED_REVIEW_FILENAME
    featured_review_ref = ""
    if featured_review_path.exists():
        logger.info(f"Subiendo Featured Review: {FEATURED_REVIEW_FILENAME}...")
        fr_up = upload_image_to_shopify_files(featured_review_path, shop_url, access_token, unique_prefix=slug)
        featured_review_ref = fr_up["shopify_ref"]
    else:
        logger.warning(f"No existe Featured Review image: {featured_review_path}")

    files_map = {
        BEFORE_FILENAME: before_up,
        AFTER_FILENAME: after_up,
        PAIN_FILENAME: pain_up,
        "benefits": benefits_refs,
        "social_proof": social_refs,
        "second_iwt": second_iwt_up,
        "featured_review": featured_review_ref
    }

    map_path = OUT_DIR / "shopify_files_map.json"
    map_path.write_text(json.dumps(files_map, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Map generado: {map_path}")

    # 5) Patch JSON (compare-image + pain + multicolumn)
    logger.info("Parcheando el template JSON...")
    patched_filename = json_filename.replace(".json", ".patched.json")
    patched_path = OUT_DIR / patched_filename

    template_data = json.loads(TEMPLATE_JSON.read_text(encoding="utf-8"))

    patch_sections(
        template=template_data,
        out_json_path=patched_path,
        before_ref=before_up["shopify_ref"],
        after_ref=after_up["shopify_ref"],
        pain_ref=pain_up["shopify_ref"],
        benefits_refs=benefits_refs,
        social_refs=social_refs,
        second_iwt_ref=second_iwt_ref,
        featured_review_ref=featured_review_ref
    )

    # 6) Upload patched JSON to theme
    SHOPIFY_TEMPLATE_KEY = f"templates/{json_filename}"
    ok = upload_to_shopify_theme_asset(str(patched_path), SHOPIFY_TEMPLATE_KEY)

    if ok:
        logger.info(f"Listo. Template actualizado: {SHOPIFY_TEMPLATE_KEY}")
    else:
        logger.error("Error subiendo el template al theme.")


if __name__ == "__main__":
    deploy_pipeline()
