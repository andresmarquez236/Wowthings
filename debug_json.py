import json
from pathlib import Path

path = Path("output/aspiradora_recargable_para_carro_3_en_1/resultados_landing/product.landing-aspiradora-recargable-para-carro-3-en-1.patched.json")
print(f"Reading {path}")
try:
    data = json.loads(path.read_text(encoding="utf-8"))
    sections = data.get("sections", {})
    print(f"Found {len(sections)} sections.")
    for k in sections.keys():
        print(f" - {k} (Type: {sections[k].get('type')})")
except Exception as e:
    print(f"Error: {e}")
