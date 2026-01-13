
import os
import shutil
import json
import re
from pathlib import Path

def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def safe_name(name):
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", name).strip("_")

def extract_copy_to_md(json_path, out_path, type_asset):
    data = load_json(json_path)
    content = ""
    
    if type_asset == "single_image":
        # Usually inside render_variants -> 0 -> nanobanana_prompt...
        # Or look for high level ad_copy if Agent 3 provides it (Agent 3 is mostly visual, but let's check)
        # Agent 3 doesn't produce "ad_copy" text usually, just visual prompts.
        # But we can dump the prompt description as context.
        pass

    elif type_asset == "carousel":
        # { "ad_copy": { "title":..., "primary_text":..., "headline":... } }
        copy = data.get("ad_copy", {})
        if copy:
            content += f"# {copy.get('title', 'Ad Copy')}\n\n"
            content += f"**Primary Text:**\n{copy.get('primary_text', '')}\n\n"
            content += f"**Headline:** {copy.get('headline', '')}\n\n"
            content += "---\n"
        
        # Cards
        carousel = data.get("carousel", {})
        cards = carousel.get("cards", []) or data.get("cards", [])
        if cards:
            content += "## Cards Copy\n"
            for c in cards:
                idx = c.get("card_index", "?")
                txt = c.get("copy", {}).get("text", "")
                content += f"**Slide {idx}:** {txt}\n"

    elif type_asset == "video":
        # Video script
        script = data.get("video_script", {})
        if script:
            content += f"# Video Script: {script.get('hook_type', 'Hook')}\n\n"
            content += f"**Hook:** {script.get('visual_hook', '')} (Audio: {script.get('audio_hook', '')})\n\n"
            content += "## Script Body\n"
            for scene in script.get("script_body", []):
                content += f"- [{scene.get('seconds', '0')}s] **Visual:** {scene.get('visual', '')}\n"
                content += f"  **Audio:** {scene.get('audio', '')}\n\n"
            content += f"**Call to Action:** {script.get('cta_text', '')}\n"
            
            caption = data.get("ad_copy_caption", {})
            if caption:
                content += "\n---\n## Ad Caption\n"
                content += f"{caption.get('caption', '')}\n"

    if content:
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def organize_product_assets(product_dir_path):
    """
    Reorganizes _results_2 into a clean '00_Final_Assets_Pack' folder.
    """
    root = Path(product_dir_path)
    res2 = root / "_results_2"
    
    if not res2.exists():
        print(f"⚠️ No _results_2 found in {root}. Skipping organization.")
        return

    dest_root = root / "00_Final_Assets_Pack"
    if dest_root.exists():
        shutil.rmtree(dest_root)
    dest_root.mkdir()

    print(f"📦 Organizing assets into {dest_root.name}...")

    # 1. Strategy & Research
    strat_dir = dest_root / "01_Strategy_and_Research"
    strat_dir.mkdir()
    
    # Copy essential JSONs
    files_to_copy = {
        res2 / "0_briefs/product_brief.json": "Product_Brief.json",
        res2 / "1_strategy/angles.json": "Marketing_Angles.json",
        res2 / "2_compliance/compliance_review.json": "Compliance_Report.json",
        root / "market_research_min.json": "Market_Research.json" # usually in root
    }
    
    for src, dest_name in files_to_copy.items():
        if src.exists():
            shutil.copy2(src, strat_dir / dest_name)

    # 2. Angles Assets
    assets_root = dest_root / "02_Creative_Assets"
    assets_root.mkdir()

    # Load angles to know what to expect
    angles_path = res2 / "1_strategy/angles.json"
    angles_map = {}
    if angles_path.exists():
        adata = load_json(angles_path)
        for a in adata.get("angles", []):
            angles_map[a["angle_id"]] = a.get("angle_name", "Angle")

    # Iterate known angles or discover folders
    # We'll trust the generated_images folder structure primarily
    gen_images = res2 / "generated_images"
    
    # Helper to get angle dest folder
    def get_angle_dir(aid):
        aname = angles_map.get(aid, "Unknown")
        name = f"{aid}_{safe_name(aname)}"
        d = assets_root / name
        d.mkdir(exist_ok=True)
        return d

    # A. Carousels (generated_images/carousels/{angle_id}/*.png)
    src_car = gen_images / "carousels"
    if src_car.exists():
        for angle_dir in src_car.iterdir():
            if angle_dir.is_dir():
                aid = angle_dir.name
                dest = get_angle_dir(aid) / "Carousels"
                dest.mkdir(exist_ok=True)
                # Copy images
                count = 0
                for img in angle_dir.glob("*.png"):
                    shutil.copy2(img, dest / img.name)
                    count += 1
                
                # Extract Copy
                json_src = res2 / "carousels" / f"{aid}_carousel.json"
                if json_src.exists():
                    extract_copy_to_md(json_src, dest / "Ad_Copy_Carousel.md", "carousel")
                    # Also copy original JSON for ref
                    # shutil.copy2(json_src, dest / "source_carousel.json")

    # B. Thumbnails (generated_images/thumbnails/{angle_id}_*.png)
    # The current gen_thumbnails.py saves in generated_images/thumbnails/angle_1/*.png? 
    # Let's verify structure. The previous list_dir said: generated_images/thumbnails -> numChildren=6 (subdirs? or files?)
    # Wait, Step 806 said: generated_images/thumbnails has 6 children.
    # Step 806 summary: "carousels" (8 children), "simple" (2 children), "thumbnails" (6 children). 
    # Usually generated_images/thumbnails has flat files OR folders. 
    # Current gen_thumbnails.py: out_dir = product_dir / "generated_images" / "thumbnails" / angle_id
    # So it should be folders.
    
    src_thumb = gen_images / "thumbnails"
    if src_thumb.exists():
        for item in src_thumb.iterdir():
            # item could be directory "angle_1"
            if item.is_dir():
                aid = item.name
                dest = get_angle_dir(aid) / "Video_Thumbnails"
                dest.mkdir(exist_ok=True)
                for img in item.glob("*.png"):
                    shutil.copy2(img, dest / img.name)
            # Or if flattened (older version?), check name
    
    # Extract Video Script Copy (even if no video generated, script exists)
    # Iterate angles from map
    for aid in angles_map.keys():
        json_vid = res2 / "video_prompts" / f"{aid}_video.json"
        if json_vid.exists():
            dest = get_angle_dir(aid) / "Video_Scripts"
            dest.mkdir(exist_ok=True)
            extract_copy_to_md(json_vid, dest / "Video_Script.md", "video")

    # C. Simple Images (generated_images/simple/{angle_id}_*.png)
    # Current gen_simple_images.py: out_dir = product_dir / "generated_images" / "simple" / angle_id 
    # (Actually I should check gen_simple_images code. 
    #  Line 350: out_dir = product_dir / "_results_2" / "generated_images" / "simple"
    #  Line 368: out_filename = f"{angle_id}_{safe_name}.png"
    # So it is flat inside 'simple', prefixed range angle_id.)
    
    src_simple = gen_images / "simple"
    if src_simple.exists():
        for img in src_simple.glob("*.png"):
            # Filename: angle_1_somename.png
            # Extract angle_id
            parts = img.name.split("_")
            if len(parts) >= 2 and parts[0] == "angle":
                aid = f"{parts[0]}_{parts[1]}" # angle_1
                dest = get_angle_dir(aid) / "Single_Images"
                dest.mkdir(exist_ok=True)
                shutil.copy2(img, dest / img.name)
            else:
                # unknown angle, put in Misc
                pass
                
    print(f"✅ Organization complete! Folder: 00_Final_Assets_Pack")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        organize_product_assets(sys.argv[1])
