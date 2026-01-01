import json
import os
from dotenv import load_dotenv

# --- IMPORTACIONES DE TUS MÓDULOS ---
from content_agent import generate_elite_landing_copy
from mapper import map_payload_to_shopify_structure
from uploader import upload_to_shopify  # <--- NUEVA IMPORTACIÓN

load_dotenv()

def main():
    # =========================================================
    # CONFIGURACIÓN DEL LANZAMIENTO
    # =========================================================
    PRODUCT_NAME = "Truly Aceite Soft Serve 50 Ml"
    RAW_INFO = """Contenido 50 ML

TRULY ACEITE UNICORNIO (SOFT SERVE) CON SELLO DE ORIGINALIDAD 1.1

Evita el dolor de la irritación por afeitado y los vellos encarnados con nuestro nuevo aceite para después del afeitado Soft Serve. Formulado con una valiosa mezcla de péptidos, ácido hialurónico y fresa para exfoliar suavemente, hidratar y acelerar el proceso de curación de la piel.
Tu piel se mantendrá suave e hidratada durante días después del afeitado. Es la solución calmante con aroma a fresa que tu piel anhela.
Rasguños y quemaduras por afeitado: calma la inflamación y promueve la curación para un acabado más elegante.
Piel seca y con picazón: hidrata profundamente y alivia para una piel nutrida y sedosa al tacto.
Pelos encarnados: elimina las células muertas y regula el sebo para prevenir protuberancias dolorosas. """
    TARGET_AVATAR = "Mujer 18–34, piel sensible, se depila/afeita piernas, axilas y bikini; busca solución rápida al ardor y protuberancias"
    
    # Nombre del archivo que se creará en Shopify
    # IMPORTANTE: Debe empezar con 'templates/' y 'product.'
    SHOPIFY_TEMPLATE_KEY = f"templates/product.landing-{PRODUCT_NAME.replace(' ', '-').lower()}.json"
    
    # Rutas Locales
    TEMPLATE_PATH = "input_theme/product.custom_landing.json" 
    OUTPUT_FILENAME = f"product.landing-{PRODUCT_NAME.replace(' ', '-').lower()}.json"
    OUTPUT_PATH = os.path.join("output", OUTPUT_FILENAME)
    
    os.makedirs("output", exist_ok=True)

    # =========================================================
    # PASO 1: CARGAR ARQUITECTURA (Plantilla Base)
    # =========================================================
    print(f"⚙️ [1/4] Cargando arquitectura base...")
    try:
        with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            shopify_base_json = json.load(f)
    except FileNotFoundError:
        print("❌ Error: Falta la plantilla base. Ejecuta 'get_shopify_theme.py' primero.")
        return

    # =========================================================
    # PASO 2: INTELIGENCIA ARTIFICIAL (Generar Contenido)
    # =========================================================
    print(f"🧠 [2/4] Creando estrategia de ventas con IA...")
    
    # Comenta esta línea si quieres usar datos cacheados para pruebas rápidas
    ai_content = generate_elite_landing_copy(PRODUCT_NAME, RAW_INFO, TARGET_AVATAR)
    
    # -- MODO DEBUG (Descomentar para usar datos guardados y no gastar tokens) --
    # try:
    #     with open('output/ai_content_raw.json', 'r', encoding='utf-8') as f:
    #         ai_content = json.load(f)
    # except: pass
    # -------------------------------------------------------------------------

    if not ai_content:
        return

    # Guardar backup de la IA
    with open("output/ai_content_raw.json", "w", encoding='utf-8') as f:
        json.dump(ai_content, f, indent=4, ensure_ascii=False)

    # =========================================================
    # PASO 3: INGENIERÍA DE DATOS (Mapeo y Fusión)
    # =========================================================
    print(f"🔧 [3/4] Inyectando copy y estructura...")
    final_landing_json = map_payload_to_shopify_structure(shopify_base_json, ai_content)

    # Guardar archivo final en disco
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(final_landing_json, f, indent=4, ensure_ascii=False)

    # =========================================================
    # PASO 4: DEPLOYMENT (Subida a Shopify)
    # =========================================================
    print(f"🚀 [4/4] Iniciando despliegue a Shopify...")
    
    success = upload_to_shopify(OUTPUT_PATH, SHOPIFY_TEMPLATE_KEY)
    
    if success:
        print("\n" + "="*50)
        print(f"🎉 ¡SISTEMA COMPLETADO! LANDING PAGE ONLINE")
        print("="*50)
        print(f"Producto: {PRODUCT_NAME}")
        print(f"Plantilla creada: {SHOPIFY_TEMPLATE_KEY}")
        print("\n👉 CÓMO VERLA:")
        print("1. Ve a tu Admin de Shopify > Productos.")
        print("2. Entra al producto 'Nutria Zen' (o créalo si no existe).")
        print(f"3. En la caja 'Theme template' (abajo a la derecha o al centro), selecciona:")
        print(f"   '{SHOPIFY_TEMPLATE_KEY.replace('templates/product.', '').replace('.json', '')}'")
        print("4. Guarda y dale a 'Preview'.")

if __name__ == "__main__":
    main()