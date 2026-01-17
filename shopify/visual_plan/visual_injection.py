import os
import json
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv
import re
import datetime

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

IMAGE_WITH_TEXT_SECTION_KEY = "sections/image-with-text.liquid"
LOCAL_IMAGE_WITH_TEXT_PATH = Path("sections/image-with-text.liquid")

MULTICOLUMN_SECTION_KEY = "sections/multicolumn.liquid"
LOCAL_MULTICOLUMN_PATH = Path("sections/multicolumn.liquid")

COMPARE_IMAGE_SECTION_KEY = "sections/compare-image.liquid"
LOCAL_COMPARE_IMAGE_PATH = Path("sections/compare-image.liquid")

MAIN_PRODUCT_SECTION_KEY = "sections/main-product.liquid"
LOCAL_MAIN_PRODUCT_PATH = Path("sections/main-product.liquid")

# New Dependencies
KEY_BENEFITS_PATH = Path("sections/key-benefits.liquid")
ICON_TEXT_PATH = Path("sections/icon-with-text.liquid")
COLLAPSIBLE_PATH = Path("sections/collapsible-content.liquid")
COMPARE_CHART_PATH = Path("sections/compare-chart.liquid")
PERCENTAGE_PATH = Path("sections/percentage.liquid")

def create_and_upload_scoped_section(base_path: Path, original_type: str, slug: str) -> str:
    """
    Reads a local base section, renames it to be unique for this product (slug),
    updates its schema name, and uploads it.
    Returns the new section 'type' string (filename without .liquid or path).
    """
    if not base_path.exists():
        logger.warning(f"⚠️ Base section {base_path} not found. Skipping scoped creation.")
        return original_type

    import hashlib
    
    # Standardize slug for filenames (replace _ with - just in case)
    safe_slug = slug.replace("_", "-")
    
    # Shopify limit is 50 chars for the key 'sections/filename.liquid'.
    # So filename max length is ~41 chars (50 - 9 for 'sections/').
    # original_type is e.g. 'image-with-text' (15 chars).
    # prefix 'lp-' (3 chars).
    # Remaining for slug: 41 - 18 = 23 chars.
    
    # Strategy: "lp-{type[:10]}-{hash[:6]}" to be super safe and unique?
    # Or "lp-{type}-{slug_truncated}"
    
    # Let's generate a short hash of the full slug to ensure uniqueness even if we truncate
    slug_hash = hashlib.md5(safe_slug.encode("utf-8")).hexdigest()[:6]
    
    # We construct a short name: "lp-iwt-{slug_hash}" ? 
    # Or keep it readable. 
    # "lp-imgtxt-{slug[:10]}-{hash}"
    
    # Let's try to preserve as much as possible but strictly cut.
    # Max filename length = 35 (safe buffer)
    # Prefix: "lp-"
    
    # STRICT SHORTENING to ensure < 25 chars for Valid Block Types
    # limit slug hash to 6 chars
    short_hash = slug_hash[:6]
    
    if original_type == "image-with-text":
        scoped_prefix = "lp-img"
    elif original_type == "multicolumn":
        scoped_prefix = "lp-col"
    elif original_type == "compare-image":
        scoped_prefix = "lp-cmp" 
    elif original_type == "main-product":
        scoped_prefix = "lp-mai"
    elif original_type == "key-benefits":
        scoped_prefix = "lp-ben"
    elif original_type == "iconss": # schema name in json is often "iconss" for custom blocks
        scoped_prefix = "lp-ico"
    elif original_type == "collapsible-content":
        scoped_prefix = "lp-clp"
    elif original_type == "compare-chart":
        scoped_prefix = "lp-cch"
    elif original_type == "percentages" or original_type == "percentage":
        scoped_prefix = "lp-pct"
    else:
        scoped_prefix = "lp-unk"
        
    # Format: lp-ben-123456 (13 chars) -> ALWAYS SAFE
    new_type = f"{scoped_prefix}-{short_hash}"
    
    new_type = f"{scoped_prefix}-{short_hash}"
    
    new_filename = f"{new_type}.liquid"
    
    # 2. Read content FIRST (Moved up to fix UnboundLocalError)
    content = base_path.read_text(encoding="utf-8")
    
    # main-product uses {% render %} for benefits/icons, so they MUST be in snippets/
    if original_type in ["key-benefits", "iconss"]:
        shopify_key = f"snippets/{new_filename}"
        
        # KEY FIX: If we are converting a SECTION to a SNIPPET (e.g. icon-with-text.liquid -> lp-ico-XXX.liquid),
        # we must replace 'section.settings' with 'block.settings' so it renders correctly inside the {% render ..., block: block %} call.
        if original_type == "iconss":
            content = content.replace("section.settings", "block.settings")
            content = content.replace("section.blocks", "block.blocks") # unlikely but safe
            # Also handle section.id vs block.id
            content = content.replace("section.id", "block.id")
            
    else:
        shopify_key = f"sections/{new_filename}"
    
    # 3. Patch Schema Name to be identifiable in Editor
    # Look for "name": "..."
    # We want "name": "Image with text (Coco Rose)" or similar
    import re
    
    # Extract readable name from slug
    readable_slug = safe_slug.replace("-", " ").title()
    short_slug = (readable_slug[:20] + '..') if len(readable_slug) > 20 else readable_slug
    
    # Schema name replacer
    # Shopify Schema Name Limit: 25 characters.
    # Base: "Image with text" is 15 chars.
    # We have very little room. ~10 chars.
    
    # Strategy: "IWT {hash}" (4 + 6 = 10 chars) -> Total ~10 chars. Safe.
    # Or "LP {short_slug}"
    
    if original_type == "image-with-text":
        schema_name = f"IWT {slug_hash}" # "IWT a1b2c3" (10 chars)
    elif original_type == "multicolumn":
        schema_name = f"Cols {slug_hash}"
    elif original_type == "compare-image":
        schema_name = f"Cmp {slug_hash}"
    elif original_type == "main-product":
        schema_name = f"Main {slug_hash}"
    elif original_type == "key-benefits":
        schema_name = f"Ben {slug_hash}"
    elif original_type == "iconss":
        schema_name = f"Ico {slug_hash}"
    elif original_type == "collapsible-content":
        schema_name = f"Col {slug_hash}"
    elif original_type == "compare-chart":
        schema_name = f"Chart {slug_hash}"
    elif original_type == "percentages" or original_type == "percentage":
        schema_name = f"Pct {slug_hash}" 
    else:
        schema_name = f"LP {slug_hash}"

    def replacer(match):
        # match.group(0) is the whole line/match "name": "..."
        return f'"name": "{schema_name}"'

    # Regex to find the name property in schema. 
    # Handles: "name": "Something" or "name": "t:..."
    # Simple approach: Replace the first occurrence inside {% schema %} block? 
    # Or just replace the specific translation key if we know it.
    
    # Let's try to replace the known translation keys first, if not, generic regex.
    if "t:sections.image-with-text.name" in content:
        content = content.replace("t:sections.image-with-text.name", schema_name)
    elif "Image with text" in content:
        content = content.replace("Image with text", schema_name)
    
    # Also generic replacement for t:sections.multicolumn.name
    if "t:sections.multicolumn.name" in content:
        content = content.replace("t:sections.multicolumn.name", schema_name)
    elif "Multicolumn" in content:
        content = content.replace("Multicolumn", schema_name)
        
    # And compare-image
    if "Compare Image" in content:
        content = content.replace("Compare Image", schema_name)
    
    # And main-product
    if "t:sections.main-product.name" in content:
        content = content.replace("t:sections.main-product.name", schema_name)
    elif "Main Product" in content: # just in case
         content = content.replace("Main Product", schema_name)

    # And compare-chart (Check original file schema name)
    if "Compare chart" in content:
        content = content.replace("Compare chart", schema_name)
    elif "Compare Chart" in content:
        content = content.replace("Compare Chart", schema_name)

    # And percentages (Check original file schema name)
    if "Percentages" in content:
         content = content.replace("Percentages", schema_name)
    elif "percentages" in content: # schema fallback
         # Be careful not to replace random text, only "name": "percentages"
         # Using a slightly safer check if possible, or just trusting the schema block usually comes last.
         # Actually, the file provided has "name": "Percentages" on line 103.
         pass
    
    # Generic "name" replacement for these new custom sections if they use a simple string
    if original_type == "percentage" or original_type == "percentages":
         content = content.replace('"Percentages"', f'"{schema_name}"')
    if original_type == "compare-chart":
         content = content.replace('"Compare chart"', f'"{schema_name}"')

    # And collapsible-content
    if "t:sections.collapsible_content.name" in content:
        content = content.replace("t:sections.collapsible_content.name", schema_name)
    elif "Collapsible content" in content: # Default fallback text
        content = content.replace("Collapsible content", schema_name)


        
    # Generic robust patch: find "name":\s*".*?" inside schema
    # But schema is at the end. simple string replace is safer if we target the specific key we know base uses.
    # Base image-with-text.liquid uses "t:sections.image-with-text.name"
    
    # 4. Upload
    if upload_to_shopify_theme_asset_str(content, shopify_key):
        logger.info(f"✅ Created & Uploaded Scoped Section: {new_type}")
        return new_type
    else:
        logger.error(f"❌ Failed to upload scoped section {new_type}")
        return original_type


def ensure_landing_palette_section_in_theme():
    """
    Sube la sección landing-palette-overrides al theme.
    """
    update_context(step="Upload Section: Palette")
    if not LOCAL_SECTION_PATH.exists():
        raise FileNotFoundError(
            f"❌ No encuentro {LOCAL_SECTION_PATH}. Asegúrate de que existe."
        )
    
    content = LOCAL_SECTION_PATH.read_text(encoding="utf-8")
    if not upload_to_shopify_theme_asset_str(content, LANDING_PALETTE_SECTION_KEY):
        raise RuntimeError(f"Failed to upload {LANDING_PALETTE_SECTION_KEY}")

def ensure_image_with_text_section_in_theme():
    """
    Sube la sección image-with-text.liquid al theme.
    """
    update_context(step="Upload Section: ImageWithText")
    if not LOCAL_IMAGE_WITH_TEXT_PATH.exists():
        # It's optional if missing locally, but user requested insertion
        logger.warning(f"⚠️ {LOCAL_IMAGE_WITH_TEXT_PATH} not found locally. Skipping upload.")
        return

    content = LOCAL_IMAGE_WITH_TEXT_PATH.read_text(encoding="utf-8")
    if not upload_to_shopify_theme_asset_str(content, IMAGE_WITH_TEXT_SECTION_KEY):
        logger.error(f"Failed to upload {IMAGE_WITH_TEXT_SECTION_KEY}")
    else:
        logger.info(f"✅ Subido asset: {IMAGE_WITH_TEXT_SECTION_KEY}")

def ensure_multicolumn_section_in_theme():
    """
    Sube la sección multicolumn.liquid al theme.
    """
    update_context(step="Upload Section: Multicolumn")
    if not LOCAL_MULTICOLUMN_PATH.exists():
        logger.warning(f"⚠️ {LOCAL_MULTICOLUMN_PATH} not found locally. Skipping upload.")
        return

    content = LOCAL_MULTICOLUMN_PATH.read_text(encoding="utf-8")
    if not upload_to_shopify_theme_asset_str(content, MULTICOLUMN_SECTION_KEY):
        logger.error(f"Failed to upload {MULTICOLUMN_SECTION_KEY}")
    else:
        logger.info(f"✅ Subido asset: {MULTICOLUMN_SECTION_KEY}")

def patch_template_with_palette_and_schemes(
    template_json_path: Path,
    out_json_path: Path,
    palette: dict,
    section_scheme_map: dict,
    slug: str,
    scoped_types: dict = None,
    sections_scheme_data: dict = None
) -> Path:
    update_context(step="Patch Template")
    if scoped_types is None: scoped_types = {}
    if sections_scheme_data is None: sections_scheme_data = {}
    
    try:
        data = json.loads(template_json_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read template {template_json_path}: {e}")
        raise

    sections = data.setdefault("sections", {})
    # ... (rest of function body until logic)

    # ... [SKIP TO LOGIC PART IN NEXT REPLACEMENT OR HANDLE HERE IF CONTIGUOUS] ...
    # Wait, I need to preserve lines 303-424 which are largely unchanged except for using sections_scheme_data.
    # It is safer to just update signature here manually if I can match the block.
    # The tool requires a single contiguous block.
    # I will update signature first.

    # ... Code omitted for brevity ...
    
    # 2) Call Site Update
    # Located in run_injection_pipeline
    
    pass

# I will do this in TWO steps. 
# Step 1: Update Signature

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
                "button_hover": palette.get("button_hover", palette.get("button_background", "#000")),
                # Dynamic keys from Plan (Soft Fallback to existing accents if plan is old version)
                "icon_neutral": palette.get("icon_neutral") or palette.get("text"),
                "icon_feature": palette.get("icon_feature") or palette.get("accent_2"),
                "checkmark_color": palette.get("checkmark_color") or palette.get("accent_2"),
                "discount_bg": palette.get("discount_bg") or palette.get("accent_1")
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
    # 2) Aplicar color_scheme por section_id
    logger.info("Applying color schemes to sections...")
    # FIX: Iterate over ALL sections in the template, not just the ones in the map.
    for sec_id, sec in sections.items():
        
        # Get scheme from plan if available
        scheme = section_scheme_map.get(sec_id)
        
        settings = sec.setdefault("settings", {})
        
        # Apply base scheme from plan if present
        if scheme:
            settings["color_scheme"] = scheme

        # Smart Overrides for specific section types
        # Smart Overrides for specific section types
        original_type = sec.get("type")
        
        # 3. Apply Scoped Type Swap
        # If we have a scoped version for this type, swap it in the JSON
        # "image-with-text" -> "lp-image-with-text-coco-rose"
        if original_type == "image-with-text" and "image-with-text-type" in scoped_types:
             new_type = scoped_types["image-with-text-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type
        
        elif original_type == "multicolumn" and "multicolumn-type" in scoped_types:
             new_type = scoped_types["multicolumn-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

        elif original_type == "compare-image" and "compare-image-type" in scoped_types:
             new_type = scoped_types["compare-image-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

        elif original_type == "compare-chart" and "compare-chart-type" in scoped_types:
             new_type = scoped_types["compare-chart-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

        elif (original_type == "percentages" or original_type == "percentage") and "percentage-type" in scoped_types:
             new_type = scoped_types["percentage-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

        elif original_type == "collapsible-content" and "collapsible-content-type" in scoped_types:
             new_type = scoped_types["collapsible-content-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

        elif original_type == "main-product" and "main-product-type" in scoped_types:
             new_type = scoped_types["main-product-type"]
             if new_type != original_type:
                logger.info(f"🔄 Swapping section {sec_id} type: {original_type} -> {new_type}")
                sec["type"] = new_type

                # NOW SWAP INNER BLOCKS FOR MAIN PRODUCT
                if "blocks" in sec:
                    for block_id, block in sec["blocks"].items():
                        b_type = block.get("type")
                        if b_type == "keybenefit" and "key-benefits-type" in scoped_types:
                             # Note: in main-product blocks, type is often "keybenefit" pointing to snippets? 
                             # Wait, if they are BLOCKS defined in main-product schema, we can't swap their "type" unless main-product schema is updated.
                             # If they are SECTIONS included via "type": "shopify://apps...", that's different.
                             # The user said "sections/key-benefits.liquid".
                             # If the main-product uses {% render 'key-benefits' %} inside a block, we need to change the LIQUID of main-product to render the NEW snippet.
                             # BUT we created a SCOPED SECTION for main-product. So we can edit THAT scoped file.
                             pass
    
    # ... Wait, the user said "sections/key-benefits.liquid". 
    # If it is a SECTION, it appears in "sections" dict. 
    # If it is used as a BLOCK inside main-product, it might be a snippet. 
    # Let's assume they are stand-alone sections OR we need to patch the Liquid of the scoped main-product to render the new names.
    
    # IF main-product.liquid has {% section 'key-benefits' %} -> we replace it in the LIQUID.
    # IF main-product.liquid has {% render 'key-benefits' %} -> we replace in LIQUID.
    
    # For now, let's assuming we simply upload the new Liquid files (scoped) and update the main-product liquid to Point to them?
    # Actually, create_and_upload_scoped_section returns the NEW NAME.
    # We need to update LOCAL_MAIN_PRODUCT content to point to these new names BEFORE uploading it.
    
        # Re-check type since we changed it
        current_type = sec.get("type", "")
        # DEBUG: Log every section check
        # logger.debug(f"Checking section {sec_id} type={current_type}")

        # LOGIC FOR IMAGE-WITH-TEXT (Original OR Scoped)
        # Matches: "image-with-text", "lp-img-XXXXXX"
        if "image-with-text" in current_type or "lp-img" in current_type:
            settings["lp_use_custom_colors"] = True
            
            # Use granular scheme if available
            granular = sections_scheme_data.get("image_with_text", {})
            
            # Defaults if granular missing
            bg_color = granular.get("lp_bg") or palette.get("background_2") or palette.get("background_1") or "#ffffff"
            media_bg = granular.get("lp_media_bg") or bg_color
            content_bg = granular.get("lp_content_bg") or palette.get("background_1") or "#ffffff"
            text_color = granular.get("lp_text") or palette.get("text") or "#333333"
            heading_color = granular.get("lp_heading") or palette.get("text") or "#333333"
            accent_color = granular.get("lp_accent") or palette.get("accent_1") or "#ff4081"

            settings["lp_bg"] = bg_color
            settings["lp_media_bg"] = media_bg
            settings["lp_content_bg"] = content_bg
            settings["lp_text"] = text_color
            settings["lp_heading"] = heading_color
            settings["lp_accent"] = accent_color

            # Fallback standard schemes
            settings["color_scheme"] = "background-2" 
            settings["media_color_scheme"] = "background-1"
            settings["content_color_scheme"] = "background-1"

        # Matches: "multicolumn", "lp-col-XXXXXX"
        elif "multicolumn" in current_type or "lp-col" in current_type:
            settings["lp_use_custom_colors"] = True
            
            granular = sections_scheme_data.get("multicolumn", {})
            
            bg_color = granular.get("lp_bg") or palette.get("background_2") or palette.get("background_1") or "#fafafa"
            card_bg = granular.get("lp_card_bg") or palette.get("background_1") or "#ffffff"
            text_color = granular.get("lp_text") or palette.get("text") or "#111111"
            heading_color = granular.get("lp_heading") or text_color
            accent_color = granular.get("lp_accent") or palette.get("accent_1") or "#ff4081"
            
            settings["lp_bg"] = bg_color
            settings["lp_card_bg"] = card_bg
            settings["lp_text"] = text_color
            settings["lp_heading"] = heading_color
            settings["lp_accent"] = accent_color 

            # Fallback schemes
            settings["color_scheme"] = "background-2"
            settings["card_color_scheme"] = "background-1"

        # Matches: "compare-image", "lp-cmp-XXXXXX"
        elif "compare-image" in current_type or "lp-cmp" in current_type:
            settings["lp_use_custom_colors"] = True
            
            granular = sections_scheme_data.get("compare_image", {})
            
            bg_color = granular.get("lp_bg") or palette.get("background_1") or "#ffffff"
            text_color = granular.get("lp_text") or palette.get("text") or "#333333"
            heading_color = granular.get("lp_heading") or palette.get("text") or "#111111"

            settings["lp_bg"] = bg_color
            settings["lp_text"] = text_color 
            settings["lp_heading"] = heading_color

        # Matches: "compare-chart", "lp-cch-XXXXX"
        elif "compare-chart" in current_type or "lp-cch" in current_type:
            settings["lp_use_custom_colors"] = True
            
            granular = sections_scheme_data.get("compare_chart", {})
            bg_color = granular.get("lp_bg") or palette.get("background_1") or "#ffffff"
            text_color = granular.get("lp_text") or palette.get("text") or "#111111"
            heading_color = granular.get("lp_heading") or palette.get("text") or "#111111"

            settings["lp_bg"] = bg_color
            settings["lp_text"] = text_color 
            settings["lp_heading"] = heading_color

        # Matches: "percentage", "lp-pct-XXXXX"
        elif "percentages" in current_type or "percentage" in current_type or "lp-pct" in current_type:
            settings["lp_use_custom_colors"] = True
            
            granular = sections_scheme_data.get("percentage", {})
            bg_color = granular.get("lp_bg") or palette.get("background_1") or "#ffffff"
            text_color = granular.get("lp_text") or palette.get("text") or "#111111"
            heading_color = granular.get("lp_heading") or palette.get("text") or "#111111"
            accent_color = granular.get("lp_accent") or palette.get("accent_1") or "#ff4081"

            settings["lp_bg"] = bg_color
            settings["lp_text"] = text_color 
            settings["lp_heading"] = heading_color
            settings["lp_heading"] = heading_color
            settings["lp_accent"] = accent_color

        # Matches: "collapsible-content", "lp-clp-XXXXX"
        elif "collapsible-content" in current_type or "lp-clp" in current_type:
            settings["lp_use_custom_colors"] = True
            
            granular = sections_scheme_data.get("collapsible_content", {})
            bg_color = granular.get("lp_bg") or palette.get("background_2") or "#f8f8f8"
            text_color = granular.get("lp_text") or palette.get("text") or "#111111"
            heading_color = granular.get("lp_heading") or palette.get("text") or "#111111"
            accent_color = granular.get("lp_accent") or palette.get("accent_1") or "#000000"

            settings["lp_bg"] = bg_color
            settings["lp_text"] = text_color 
            settings["lp_heading"] = heading_color
            settings["lp_accent"] = accent_color

        # Matches: "main-product", "lp-mai-XXXXXX"
        elif "main-product" in current_type or "lp-mai" in current_type:
             settings["lp_use_custom_colors"] = True
             
             granular = sections_scheme_data.get("main_product", {})
             
             bg_color = granular.get("lp_bg") or palette.get("background_1") or "#ffffff"
             text_color = granular.get("lp_text") or palette.get("text") or "#333333"
             heading_color = granular.get("lp_heading") or palette.get("text") or "#111111"
             accent_color = granular.get("lp_accent") or palette.get("accent_1") or "#ff4081"
             btn_bg = granular.get("lp_btn_bg") or palette.get("button_background") or "#000000"
             btn_text = granular.get("lp_btn_text") or palette.get("button_label") or "#ffffff"

             settings["lp_bg"] = bg_color
             settings["lp_text"] = text_color
             settings["lp_accent"] = accent_color
             settings["lp_btn_bg"] = btn_bg
             settings["lp_btn_text"] = btn_text

             # FIX: Also iterate through BLOCKS to find "key-benefits" and update its background setting
             if "blocks" in sec:
                 for blk_id, blk in sec["blocks"].items():
                     b_type = blk.get("type", "")
                     
                     # 1. KEY BENEFITS (Type: "keybenefit")
                     if b_type == "keybenefit":
                         if "settings" not in blk: blk["settings"] = {}
                         # Checkmark background: maybe Background 2 for subtle contrast?
                         blk["settings"]["checkmarkcolorbackground"] = palette.get("background_2", "#f5f5f5")
                         # Use explicit key from plan, fallback to accent_2 if missing (old plan)
                         blk["settings"]["checkmarkcolor"] = palette.get("checkmark_color") or palette.get("accent_2", "#4caf50")

                     # 2. ICONS WITH TEXT (Type: "iconss")
                     elif b_type == "iconss":
                         if "settings" not in blk: blk["settings"] = {}
                         # Use explicit key from plan, fallback to feature icon color or accent_2
                         blk["settings"]["iconcolorreturns"] = palette.get("icon_feature") or palette.get("accent_2", "#4caf50")





    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    logger.info(f"✅ Template parcheado localment: {out_json_path}")
    return out_json_path

    logger.info(f"✅ Template parcheado localment: {out_json_path}")
    return out_json_path

def log_created_files(product_name: str, files: list):
    """
    Logs the list of created/uploaded files to a local JSON registry.
    """
    log_path = Path("output/landing_files_log.json")
    
    # Load existing logs
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    else:
        logs = []
        
    # Create new record
    record = {
        "timestamp": datetime.datetime.now().isoformat(),
        "product": product_name,
        "files": files
    }
    
    logs.append(record)
    
    # Write back
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
        logger.info(f"📝 Log file updated: {log_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write log file: {e}")

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
            
        # PIPELINE INTEGRATION ADAPTATION: Handle Schema Variations
        # Schema A: { "selected_option": "B", "palette_options": [ ... ] }
        # Schema B: { "best_option": "option_b", "palettes": { "option_a": ... } }
        # Schema C (Premium Prompt): Direct Output { "landing_palette_slug": {...}, "sections": {...} }

        selected_id = root.get("selected_option") or root.get("best_option") or root.get("best_option_id") or (root.get("final_selection") and root["final_selection"].get("best_option_id")) or "Direct"
        best_opt = None
        
        # Try List Format (Schema A)
        if "palette_options" in root:
             options = root.get("palette_options", [])
             # Match by 'option' name OR by 'id' (e.g. "C")
             best_opt_source = next((o for o in options if o.get("option") == selected_id or o.get("id") == selected_id), None)
             if best_opt_source:
                 best_opt = best_opt_source.copy()
                 # Ensure keys exist standardly
                 if "sections_scheme" not in best_opt: best_opt["sections_scheme"] = {}
                 if "section_color_scheme" not in best_opt: best_opt["section_color_scheme"] = {} # For compatibility
             
        # Try Dict Format (Schema B)
        elif "palettes" in root:
             palettes = root.get("palettes", {})
             if selected_id in palettes:
                 raw_opt = palettes[selected_id]
                 best_opt = {
                     "option": selected_id,
                     "type": raw_opt.get("description", "Start"),
                     "palette": raw_opt.get("colors", {}),
                     "cta_rules": raw_opt.get("cta_rules", {}),
                     "section_color_scheme": raw_opt.get("section_color_schemes", {})
                 }
        
        # Try Dirct Format (Schema C - Premium)
        # Look for a key starting with "landing_palette_"
        else:
            palette_key = next((k for k in root.keys() if k.startswith("landing_palette_")), None)
            if palette_key:
                 best_opt = {
                     "option": "Premium_Direct",
                     "type": "Premium Plan",
                     "palette": root[palette_key].get("settings", {}),
                     "cta_rules": root.get("rules_cta", {}).get("primary_style", {}),
                     "section_color_scheme": {} # section mapping is likely direct in 'sections' key now?
                 }
                 # Map flattened 'sections' dict to section_color_scheme map expected by injection
                 # The new schema puts settings directly in sections. 
                 # We might need a slightly different logic or just map what we can.
                 # The patch logic uses 'section_scheme_map' to set 'color_scheme'.
                 # New schema has "sections": { "id": { "settings": { ... } } }
                 # We can extract color_scheme if present.
                 if "sections" in root:
                     for sec_id, sec_data in root["sections"].items():
                         if "settings" in sec_data and "color_scheme" in sec_data["settings"]:
                             best_opt["section_color_scheme"][sec_id] = sec_data["settings"]["color_scheme"]

        if not best_opt:
            logger.error(f"❌ Selected option {selected_id} not found/parsed in plan.")
            return

        logger.info(f"Selected Visual Option: {best_opt['option']} ({best_opt['type']})")

        palette = best_opt["palette"]
        
        # merge cta rules into palette if needed (Schema C already points 'palette' to settings, which has button colors)
        
        # Schema C 'cta_rules' might have extra content not in 'palette' settings?
        cta_rules = best_opt.get("cta_rules", {})
        # Ensure fallback
        if "button_background" not in palette:
             palette["button_background"] = cta_rules.get("background")
        if "button_label" not in palette:
             palette["button_label"] = cta_rules.get("text")
        if "button_hover" not in palette:    
             palette["button_hover"] = cta_rules.get("hover_background")

        section_scheme_map = best_opt.get("section_color_scheme", {})
        sections_scheme_data = best_opt.get("sections_scheme", {})
        
        logger.info(f"📊 Granular Scheme Data Extracted: {json.dumps(sections_scheme_data, indent=2)}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing Visual Plan: {e}")
        return

    # Identify Template
    slug = product_folder_name.replace("_", "-")
    
    # PIPELINE INTEGRATION: Check for ALREADY PATCHED JSON (from deploy_images.py)
    # If it exists, we treat it as the source template to preserve image injections.
    patched_filename = f"product.landing-{slug}.patched.json"
    patched_path = results_dir / patched_filename
    
    original_template_json = results_dir / f"product.landing-{slug}.json"
    
    if patched_path.exists():
        logger.info(f"🔗 Pipeline: Found existing PATCHED template. Using it as base source: {patched_filename}")
        template_json = patched_path
    else:
        template_json = original_template_json
    
    if not template_json.exists():
        logger.error(f"❌ Template JSON not found: {template_json}")
        return

    # 1. Upload Scoped Sections
    # We create unique liquid files for this product to isolate styles.
    # Currently only enabled for 'image-with-text' as requested.
    
    params = {}
    
    # Create a list to track all files created during this run
    created_files_log = []
    
    try:
        ensure_landing_palette_section_in_theme()
        created_files_log.append("sections/landing-palette-overrides.liquid")
        
        # SCOPED: Image with text
        # Returns the new type string (e.g. 'lp-image-with-text-coco-rose')
        iwt_type = create_and_upload_scoped_section(LOCAL_IMAGE_WITH_TEXT_PATH, "image-with-text", slug)
        params["image-with-text-type"] = iwt_type
        created_files_log.append(f"sections/{iwt_type}.liquid")
        
        # SCOPED: Multicolumn
        mc_type = create_and_upload_scoped_section(LOCAL_MULTICOLUMN_PATH, "multicolumn", slug)
        params["multicolumn-type"] = mc_type
        created_files_log.append(f"sections/{mc_type}.liquid")
        
        # SCOPED: Compare Image
        ci_type = create_and_upload_scoped_section(LOCAL_COMPARE_IMAGE_PATH, "compare-image", slug)
        params["compare-image-type"] = ci_type
        created_files_log.append(f"sections/{ci_type}.liquid")
        
        # SCOPED: Compare Chart
        cch_type = create_and_upload_scoped_section(COMPARE_CHART_PATH, "compare-chart", slug)
        params["compare-chart-type"] = cch_type
        created_files_log.append(f"sections/{cch_type}.liquid")

        # SCOPED: Percentage
        pct_type = create_and_upload_scoped_section(PERCENTAGE_PATH, "percentage", slug)
        params["percentage-type"] = pct_type
        created_files_log.append(f"sections/{pct_type}.liquid")


        
        # SCOPED: Key Benefits & Others (Helper Sections potentially used as Snippets or Sections)
        # We create them so they exist.
        # Note: key-benefits now uses CUSTOM path logic below, so we log it there.
        
        it_type = create_and_upload_scoped_section(ICON_TEXT_PATH, "iconss", slug)
        created_files_log.append(f"snippets/{it_type}.liquid")
        
        col_type = create_and_upload_scoped_section(COLLAPSIBLE_PATH, "collapsible-content", slug)
        params["collapsible-content-type"] = col_type
        created_files_log.append(f"sections/{col_type}.liquid")
        
        # NOW: The Critical Step. The `main-product` needs to use these NEW names if it renders them.
        # But `visual_injection` loads `LOCAL_MAIN_PRODUCT_PATH` from disk.
        # We need to INTERCEPT that load or modify the file on disk temporarily? 
        # Better: create_and_upload_scoped_section for main-product should do the replacement.
        # BUT create_and_upload_scoped_section is generic.
        
        # Let's Modify `create_and_upload_scoped_section` behavior slightly or pass a map.
        # Actually, let's just do a specific "Patch Main Product" step here before uploading it.
        
        # READ Main Product
        with open(LOCAL_MAIN_PRODUCT_PATH, 'r', encoding='utf-8') as f:
            mp_content = f.read()
            
        # REPLACE references to old snippets/sections
        # "sections/key-benefits" -> "sections/lp-benefits-..." ? 
        # No, liquid uses {% section 'key-benefits' %}. We change to {% section 'lp-benefits-...' %}
        # CUSTOM Snippet Logic:
        # User requested a simplified custom file to guarantee fix.
        # We upload `snippets/lp-benefits-custom.liquid` instead of the original.
        # We re-enable Regex replacement to point to this new scoped file.
        
        CUSTOM_BENEFITS_PATH = Path("snippets/lp-benefits-custom.liquid")
        kb_type = create_and_upload_scoped_section(CUSTOM_BENEFITS_PATH, "key-benefits", slug)
        created_files_log.append(f"snippets/{kb_type}.liquid")
        
        # KEY FIX: Use CUSTOM Snippet for Icon-With-Text (Returns) to avoid section/block settings mismatch
        # We inject the "background_2" color from the palette to ensure harmony with Key Benefits oval style.
        oval_bg = palette.get("background_2", "#f0f0f0") 
        
        # We construct the content DYNAMICALLY here instead of reading a file, 
        # allowing us to bake in the palette color since the block schema doesn't have a background setting.
        # USER REQUEST: Both items inside a SINGLE oval.
        custom_icon_content = f'''
<div class="icon-with-text-custom" style="display: inline-flex; gap: 1.5rem; align-items: center; justify-content: center; flex-wrap: wrap; margin-top: 1rem; background-color: {oval_bg}; padding: 10px 24px; border-radius: 50px;">
  {{%- if block.settings.text1 != blank -%}}
    <div class="icon-item" style="display: flex; align-items: center; gap: 0.5rem;">
      <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{{{{ block.settings.iconcolorreturns | default: '#000000' }}}}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <rect x="1" y="3" width="15" height="13"></rect>
        <polygon points="16 8 20 8 23 11 23 16 16 16 16 8"></polygon>
        <circle cx="5.5" cy="18.5" r="2.5"></circle>
        <circle cx="18.5" cy="18.5" r="2.5"></circle>
      </svg>
      <span class="h5" style="margin: 0; font-size: 13px; font-weight: 600; letter-spacing: 0px;">{{{{ block.settings.text1 | default: "Envío Gratis" }}}}</span>
    </div>
  {{%- endif -%}}

  {{%- if block.settings.text2 != blank -%}}
    <!-- Vertical Divider line logic could be added here if desired, using a simple border-left or separate element -->
    <div class="icon-item" style="display: flex; align-items: center; gap: 0.5rem;">
      <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{{{{ block.settings.iconcolorreturns | default: '#000000' }}}}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path>
      </svg>
      <span class="h5" style="margin: 0; font-size: 13px; font-weight: 600; letter-spacing: 0px;">{{{{ block.settings.text2 | default: "Pago Contraentrega" }}}}</span>
    </div>
  {{%- endif -%}}
</div>
'''
        # Write this dynamic content to a temp file or pass directly (API requires file or string)
        # create_and_upload... expects a path. We will write to the custom path FIRST.
        CUSTOM_ICONS_PATH = Path("snippets/lp-icon-text-custom.liquid")
        CUSTOM_ICONS_PATH.write_text(custom_icon_content, encoding="utf-8")
        
        it_type = create_and_upload_scoped_section(CUSTOM_ICONS_PATH, "iconss", slug)
        
        # Regex replacement for render calls to handle quotes and whitespace - RE-ENABLED
        if kb_type:
            mp_content = re.sub(r"(render\s+['\"])key-benefits(['\"])", f"\\1{kb_type}\\2", mp_content)
        
        if it_type:
             mp_content = re.sub(r"(render\s+['\"])icon-with-text(['\"])", f"\\1{it_type}\\2", mp_content)
             # Also replace variations (icon-with-text2, 3) mapping them to the same scoped file
             mp_content = re.sub(r"(render\s+['\"])icon-with-text\d+(['\"])", f"\\1{it_type}\\2", mp_content)
             
        if col_type:
             mp_content = re.sub(r"(render\s+['\"])collapsible-content(['\"])", f"\\1{col_type}\\2", mp_content)
        
        # Write to a temp file to upload as the scoped main
        temp_main_path = LOCAL_MAIN_PRODUCT_PATH.parent / f"temp_{slug}_main.liquid"
        with open(temp_main_path, 'w', encoding='utf-8') as f:
            f.write(mp_content)
            
        # Upload using the temp file but acting as main-product
        mp_type = create_and_upload_scoped_section(temp_main_path, "main-product", slug)
        params["main-product-type"] = mp_type
        created_files_log.append(f"sections/{mp_type}.liquid")
        
        # Cleanup temp
        if temp_main_path.exists():
            temp_main_path.unlink()




        
        # ensure_multicolumn_section_in_theme() # No longer needed as we use scoped
 

    except Exception as e:
        logger.error(f"❌ Critical error ensuring liquid sections: {e}")
        return

    # 2. Patch Template
    patch_template_with_palette_and_schemes(
        template_json_path=template_json,
        out_json_path=patched_path,
        palette=palette,
        section_scheme_map=section_scheme_map,
        slug=slug,
        scoped_types=params,
        sections_scheme_data=sections_scheme_data
    )

    # 3. Upload Template to Shopify
    update_context(step="Upload Template")
    template_key = f"templates/product.landing-{slug}.json"
    
    content = patched_path.read_text(encoding="utf-8")
    if upload_to_shopify_theme_asset_str(content, template_key):
        logger.info(f"🎉 SUCCESSS: Landing page deployed with visual injection!")
        logger.info(f"   Template Key: {template_key}")
        created_files_log.append(template_key)
    else:
        logger.error("❌ Failed to upload patched template.")
        
    # FINAL STEP: Log created files (Always log whatever we generated/attempted)
    log_created_files(product_folder_name, created_files_log)


if __name__ == "__main__":
    import sys
    # Default testing
    folder = "coco_rose_mantequilla_truly_grande"
    if len(sys.argv) > 1:
        folder = sys.argv[1]
        
    run_injection_pipeline(folder)
