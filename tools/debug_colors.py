import json
from pathlib import Path

def debug_colors(product_folder):
    base_dir = Path(f"output/{product_folder}/resultados_landing")
    plan_path = base_dir / "landing_visual_plan.json"
    
    slug = product_folder.replace("_", "-")
    patched_path = base_dir / f"product.landing-{slug}.patched.json"
    
    print(f"--- Debugging {product_folder} ---")
    
    # 1. Check Plan
    if not plan_path.exists():
        print("❌ Plan not found")
        return
        
    with open(plan_path) as f:
        plan = json.load(f)
        
    # Handle structure variations
    root = plan.get("landing_visual_plan_v1", plan)
    selected = root.get("selected_option")
    options = root.get("palette_options", [])
    best = next((o for o in options if o["option"] == selected), None)
    
    if best:
        print(f"✅ Plan Selected: {selected}")
        print(f"   Background 1: {best['palette'].get('background_1')}")
        print(f"   Background 2: {best['palette'].get('background_2')}")
    else:
        print("❌ Selected option not found in options")

    # 2. Check Patched JSON
    if not patched_path.exists():
        print("❌ Patched JSON not found")
        return

    with open(patched_path) as f:
        template = json.load(f)
        
    palette_sec_id = f"landing_palette_{slug}".replace("-", "_")
    section = template.get("sections", {}).get(palette_sec_id)
    
    if section:
        print(f"✅ Section {palette_sec_id} found")
        settings = section.get("settings", {})
        print(f"   Settings Bg 1: {settings.get('background_1')}")
        print(f"   Settings Bg 2: {settings.get('background_2')}")
        
        # Check consistency
        if settings.get('background_1') == best['palette'].get('background_1'):
             print("✅ Colors MATCH Plan -> Template")
        else:
             print("❌ Colors MISMATCH")
    else:
        print(f"❌ Section {palette_sec_id} NOT found in template sections")
        print("Available sections:", list(template.get("sections", {}).keys()))

if __name__ == "__main__":
    debug_colors("coco_rose_mantequilla_truly_grande")
