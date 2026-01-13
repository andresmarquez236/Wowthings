
import os
import shutil
import json
from pathlib import Path

def restore_product_structure(output_root="output"):
    root = Path(output_root)
    if not root.exists():
        print(f"❌ Root not found: {output_root}")
        return

    for product_dir in root.iterdir():
        if not product_dir.is_dir():
            continue
        
        print(f"\n🔍 Checking {product_dir.name}...")
        
        # 1. Locate Market Research
        target_mr = product_dir / "market_research_min.json"
        
        if not target_mr.exists():
            found_mr = None
            # Search recursively
            for path in product_dir.rglob("*.json"):
                if path.name == "market_research_min.json":
                    found_mr = path
                    break
                if path.name == "Market_Research.json": # From Final Assets Pack
                    found_mr = path
                    break
            
            if found_mr:
                print(f"   found MR at {found_mr.relative_to(product_dir)}")
                try:
                    shutil.copy2(found_mr, target_mr)
                    print(f"   ✅ Restored market_research_min.json to root")
                except Exception as e:
                    print(f"   ❌ Failed to restore MR: {e}")
            else:
                print(f"   ⚠️ market_research_min.json NOT found anywhere.")
        else:
             print(f"   ✅ market_research_min.json already present.")

        # 2. Locate Product Images
        target_imgs = product_dir / "product_images"
        if not target_imgs.exists():
            # Search for 'product_images' folder
            found_imgs = None
            for path in product_dir.rglob("product_images"):
                if path.is_dir() and path != target_imgs:
                    found_imgs = path
                    break
            
            if found_imgs:
                print(f"   found images at {found_imgs.relative_to(product_dir)}")
                # Move or Copy? Copy safer if nested.
                try:
                    shutil.copytree(found_imgs, target_imgs)
                    print(f"   ✅ Restored product_images/ to root")
                except Exception as e:
                    print(f"   ❌ Failed to restore images: {e}")
            else:
                print("   ⚠️ product_images folder NOT found.")
        else:
            print("   ✅ product_images/ already present.")

if __name__ == "__main__":
    restore_product_structure()
