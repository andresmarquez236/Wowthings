import os
from dotenv import load_dotenv
import PIL.Image
from google import genai
from google.genai import types

# 0. Cargar variables de entorno
load_dotenv()

# 1. Configuración del Cliente
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("No se encontró GEMINI_API_KEY en las variables de entorno.")

client = genai.Client(api_key=api_key)

# 2. Cargar imágenes de referencia
# Ajusta las rutas a tus imágenes reales
base_path = "output/bee_venom_bswell/product_images"
try:
    img1 = PIL.Image.open(f"{base_path}/abeja1.png")
    img2 = PIL.Image.open(f"{base_path}/abeja2.jpeg")
    img3 = PIL.Image.open(f"{base_path}/abeja3.jpeg")
    print("✅ Imágenes cargadas correctamente.")
except FileNotFoundError as e:
    print(f"❌ Error cargando imágenes: {e}")
    print("Asegúrate de que las rutas de las imágenes sean correctas.")
    exit(1)

# 3. Prompt de Ingeniería
prompt = """
{
          "task": "meta_square_thumbnail",
          "variant": "with_text",
          "format": {
            "aspect_ratio": "1:1",
            "resolution": "1080x1080",
            "safe_margin_percent": 10
          },
          "thumbnail": "bee_venom_bswell_A1_V1",
          "input_assets": {
            "product_lock_rule": "CRITICAL: Depict ONLY the exact product described in the provided data. Do NOT change the model/category. Do NOT add brand logos. Keep the product as the hero element."
          },
          "prompt": "Create an original square (1:1) high-conversion ecommerce thumbnail. Make the product the largest and sharpest element. Clean modern layout, high contrast, soft bokeh background, no watermarks, no brand logos, no recognizable faces.",
          "composition_rules": {
            "focus": "Product must be the sharpest and largest element; background soft bokeh.",
            "text_legibility": "If text is not readable, adjust ONLY contrast and brightness (do not change composition)."
          },
          "negative_prompt": "No watermarks, no brand logos, no copyrighted characters, no recognizable faces, no extra text beyond specified overlays.",
          "text_overlays": [
            {
              "id": "headline",
              "text_exact": "Piel más firme y con",
              "placement": {
                "anchor": "bottom_center",
                "x_percent": 50,
                "y_percent": 88,
                "max_width_percent": 92,
                "max_height_percent": 14
              },
              "typography": {
                "font_family": "Bold clean sans-serif",
                "weight": 900,
                "alignment": "center"
              },
              "style": {
                "fill": "white",
                "stroke": "dark outline (thick)",
                "shadow": "subtle"
              }
            },
            {
              "id": "badge_free_shipping",
              "text_exact": "Envío gratis",
              "placement": {
                "anchor": "top_left",
                "x_percent": 12,
                "y_percent": 12,
                "max_width_percent": 28,
                "max_height_percent": 10
              },
              "typography": {
                "font_family": "Clean sans-serif",
                "weight": 800,
                "alignment": "center"
              },
              "style": {
                "badge": "rounded sticker, solid color, high contrast",
                "fill": "white"
              }
            },
            {
              "id": "badge_cod",
              "text_exact": "Pago contraentrega",
              "placement": {
                "anchor": "top_right",
                "x_percent": 88,
                "y_percent": 14,
                "max_width_percent": 34,
                "max_height_percent": 10
              },
              "typography": {
                "font_family": "Clean sans-serif",
                "weight": 800,
                "alignment": "center"
              },
              "style": {
                "badge": "rounded sticker, slightly smaller than 'Envío gratis', high contrast",
                "fill": "white"
              }
            }
          ]
        }
"""

print("🚀 Generando imagen...")

# 4. Generación Multimodal
try:
    response = client.models.generate_content(
        model="gemini-3-pro-image-preview",
        contents=[prompt, img1, img2, img3],
        config=types.GenerateContentConfig(
            candidate_count=1,
            response_modalities=["IMAGE"],
            # resolution="2K", # Opciones dependen del modelo
            # aspect_ratio="1:1" 
        )
    )

    # 5. Guardar el resultado
    if response.candidates and response.candidates[0].content.parts:
        for i, part in enumerate(response.candidates[0].content.parts):
            # Manejo robusto de la imagen recibida
            if hasattr(part, "as_image"):
                img_result = part.as_image()
                img_result.save(f"ad_generado_v2_{i}.png")
                print(f"✅ Imagen guardada: ad_generado_v2_{i}.png")
            elif part.inline_data:
                with open(f"ad_generado_v2_{i}.png", "wb") as f:
                    f.write(part.inline_data.data)
                print(f"✅ Imagen guardada: ad_generado_v2_{i}.png")
    else:
        print("⚠️ No se generó ninguna imagen (respuesta vacía).")

except Exception as e:
    print(f"❌ Error durante la generación: {e}")