import os
import json
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

# Quick hack to allow importing from utils if running as script
sys.path.append(os.getcwd())

try:
    from utils.logger import setup_logger, log_section, update_context
except ImportError:
    # Fallback loggers
    import logging
    def setup_logger(name):
        l = logging.getLogger(name)
        l.addHandler(logging.StreamHandler())
        l.setLevel(logging.INFO)
        return l
    def log_section(l, t): l.info(f"--- {t} ---")
    def update_context(**kwargs): pass

load_dotenv()
logger = setup_logger("VisualInjection")

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-01")

def require_env(*keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"❌ Faltan variables en .env: {', '.join(missing)}")

def upload_to_shopify_theme_asset_str(content_string: str, shopify_filename: str) -> bool:
    """Sube un archivo (liquid/json/css) al theme como Asset."""
    require_env("SHOP_URL", "ACCESS_TOKEN", "THEME_ID")
    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    theme_id = os.getenv("THEME_ID")

    url = f"https://{shop_url}/admin/api/{API_VERSION}/themes/{theme_id}/assets.json"
    payload = {"asset": {"key": shopify_filename, "value": content_string}}
    headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

    try:
        r = requests.put(url, headers=headers, json=payload, timeout=60)
        if r.status_code in (200, 201):
            logger.info(f"✅ Subido asset: {shopify_filename}")
            return True
        logger.error(f"❌ Error subiendo asset {shopify_filename} ({r.status_code}): {r.text}")
        return False
    except Exception as e:
        logger.error(f"❌ Exception uploading {shopify_filename}: {e}")
        return False

# Path to the liquid section
LANDING_PALETTE_SECTION_KEY = "sections/landing-palette-overrides.liquid"
LOCAL_SECTION_PATH = Path("sections/landing-palette-overrides.liquid")

def ensure_landing_palette_section_in_theme():
    """
    Sube la sección landing-palette-overrides al theme.
    """
    update_context(step="Upload Section")
    if not LOCAL_SECTION_PATH.exists():
        raise FileNotFoundError(
            f"❌ No encuentro {LOCAL_SECTION_PATH}. Asegúrate de que existe."
        )
    
    content = LOCAL_SECTION_PATH.read_text(encoding="utf-8")
    if not upload_to_shopify_theme_asset_str(content, LANDING_PALETTE_SECTION_KEY):
        raise RuntimeError(f"Failed to upload {LANDING_PALETTE_SECTION_KEY}")

def patch_template_with_palette_and_schemes(
    template_json_path: Path,
    out_json_path: Path,
    palette: dict,
    section_scheme_map: dict,
    slug: str
) -> Path:
    update_context(step="Patch Template")
    
    try:
        data = json.loads(template_json_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read template {template_json_path}: {e}")
        raise

    sections = data.setdefault("sections", {})
    order = data.setdefault("order", [])

    # 1) Insertar sección de overrides al inicio (única por template)
    palette_section_id = f"landing_palette_{slug}".replace("-", "_")
    
    # Check if already exists to avoid duplication or overwrite with logic
    if palette_section_id not in sections:
        logger.info(f"Injecting palette override section: {palette_section_id}")
        sections[palette_section_id] = {
            "type": "landing-palette-overrides",
            "settings": {
                "accent_1": palette.get("accent_1", "#000"),
                "accent_2": palette.get("accent_2", "#fff"),
                "text": palette.get("text", "#333"),
                "background_1": palette.get("background_1", "#fff"),
                "background_2": palette.get("background_2", "#f5f5f5"),
                "button_label": palette.get("button_label", "#fff"),
                "button_background": palette.get("button_background", "#000"),
                "button_hover": palette.get("button_hover", palette.get("button_background", "#000"))
            }
        }
        # Insert at the very top of the order
        if palette_section_id not in order:
            order.insert(0, palette_section_id)
    else:
        logger.info(f"Palette section {palette_section_id} already exists. Updating settings.")
        sections[palette_section_id]["settings"].update({
             "accent_1": palette.get("accent_1"),
             "accent_2": palette.get("accent_2"),
             "text": palette.get("text"),
             "background_1": palette.get("background_1"),
             "background_2": palette.get("background_2"),
             "button_label": palette.get("button_label"),
             "button_background": palette.get("button_background"),
             "button_hover": palette.get("button_hover")
        })

    # 2) Aplicar color_scheme por section_id
    logger.info("Applying color schemes to sections...")
    for sec_id, scheme in section_scheme_map.items():
        if sec_id not in sections:
            continue
        
        sec = sections[sec_id]
        settings = sec.setdefault("settings", {})
        
        # Only set if the section *might* support it (we assume most do in Dawn-like)
        # OR force it. Since extra settings are usually ignored by Liquid if not in schema, 
        # it's relatively safe to inject. But typically we check relevant types.
        # For now, we trust the map provided by the agent.
        settings["color_scheme"] = scheme

    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    logger.info(f"✅ Template parcheado localment: {out_json_path}")
    return out_json_path

def run_injection_pipeline(product_folder_name: str):
    """
    Main entry point for pipeline integration.
    """
    log_section(logger, "VISUAL INJECTION START")
    update_context(step="Init", module_name=product_folder_name)

    base_path = Path("output") / product_folder_name
    results_dir = base_path / "resultados_landing"
    plan_path = results_dir / "landing_visual_plan.json"

    if not plan_path.exists():
        logger.error(f"❌ Visual Plan not found at {plan_path}. Run visual_planer.py first.")
        return

    # Load Plan
    try:
        plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
        # Unpack structure: usually plan_data has a root key or is direct
        # Inspecting previous file context: keys are "landing_visual_plan_v1" -> options etc
        if "landing_visual_plan_v1" in plan_data:
            root = plan_data["landing_visual_plan_v1"]
        else:
            root = plan_data # Fallback
            
        selected_id = root.get("selected_option")
        options = root.get("palette_options", []) # Schema said options is a list? Or palette_options?
        # Re-reading generated JSON: "palette_options": [...]
        
        best_opt = next((o for o in options if o["option"] == selected_id), None)
        if not best_opt:
            logger.error(f"❌ Selected option {selected_id} not found in plan.")
            return
        
        logger.info(f"Selected Visual Option: {best_opt['option']} ({best_opt['type']})")
        
        palette = best_opt["palette"]
        # merge cta rules into palette for the liquid section
        cta_rules = best_opt.get("cta_rules", {})
        palette["button_background"] = cta_rules.get("background", palette.get("button_background"))
        palette["button_label"] = cta_rules.get("color", palette.get("button_label"))
        palette["button_hover"] = cta_rules.get("hover")
        
        section_scheme_map = best_opt.get("section_color_scheme", {})
        
    except Exception as e:
        logger.error(f"❌ Error parsing Visual Plan: {e}")
        return

    # Identify Template
    slug = product_folder_name.replace("_", "-")
    template_json = results_dir / f"product.landing-{slug}.json"
    
    # Handle the case where we might be patching an already patched file?
    # Ideally we always start from the base valid json generated by content_agent
    if not template_json.exists():
        logger.error(f"❌ Template JSON not found: {template_json}")
        return

    patched_filename = f"product.landing-{slug}.patched.json"
    patched_path = results_dir / patched_filename

    # 1. Ensure Liquid Section exists in Theme
    try:
        ensure_landing_palette_section_in_theme()
    except Exception as e:
        logger.error(f"❌ Critical error ensuring liquid section: {e}")
        return

    # 2. Patch Template
    patch_template_with_palette_and_schemes(
        template_json_path=template_json,
        out_json_path=patched_path,
        palette=palette,
        section_scheme_map=section_scheme_map,
        slug=slug
    )

    # 3. Upload Template to Shopify
    update_context(step="Upload Template")
    template_key = f"templates/product.landing-{slug}.json"
    
    content = patched_path.read_text(encoding="utf-8")
    if upload_to_shopify_theme_asset_str(content, template_key):
        logger.info(f"🎉 SUCCESSS: Landing page deployed with visual injection!")
        logger.info(f"   Template Key: {template_key}")
    else:
        logger.error("❌ Failed to upload patched template.")

if __name__ == "__main__":
    import sys
    # Default testing
    folder = "coco_rose_mantequilla_truly_grande"
    if len(sys.argv) > 1:
        folder = sys.argv[1]
        
    run_injection_pipeline(folder)
