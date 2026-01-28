import json
import os
from dotenv import load_dotenv

# --- IMPORTACIONES DE MODULOS ---
from shopify.content_agent import generate_elite_landing_copy
from shopify.mapper import map_payload_to_shopify_structure
from shopify.image_landing_gen import (
    section_before_after,
    section_pain,
    section_benefits,
    section_social_proof,
    section_featured_review,
    evaluator_benefits
)
from shopify.upload_images import deploy_images
from shopify.visual_plan.visual_planer import VisualPlaner
from shopify.visual_plan.visual_injection import run_injection_pipeline

load_dotenv()

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Manual Landing Page Generator")
    parser.add_argument("--product_name", type=str, required=True, help="Name of the product")
    parser.add_argument("--raw_info", type=str, required=True, help="Product raw info/features")
    parser.add_argument("--target_avatar", type=str, required=True, help="Target Avatar (Buyer Persona & Promise)")
    
    args = parser.parse_args()

    # =========================================================
    # CONFIGURACIÓN DEL LANZAMIENTO
    # =========================================================
    PRODUCT_NAME = args.product_name
    RAW_INFO = args.raw_info
    TARGET_AVATAR = args.target_avatar
    
    # Nombre de carpeta (slug)
    product_folder_name = PRODUCT_NAME.replace(' ', '_').lower()
    
    # Nombre del archivo plantilla en Shopify
    SHOPIFY_TEMPLATE_KEY = f"templates/product.landing-{PRODUCT_NAME.replace(' ', '-').lower()}.json"
    
    # Rutas Locales
    TARGET_DIR = os.path.join("output", product_folder_name, "resultados_landing")
    os.makedirs(TARGET_DIR, exist_ok=True)
    
    TEMPLATE_PATH = "input_theme/product.custom_landing.json" 
    OUTPUT_FILENAME = f"product.landing-{PRODUCT_NAME.replace(' ', '-').lower()}.json"
    OUTPUT_PATH = os.path.join(TARGET_DIR, OUTPUT_FILENAME)

    print("\n" + "="*60)
    print(f"🚀 INICIANDO SUPER-PIPELINE PARA: {PRODUCT_NAME}")
    print("="*60)

    # =========================================================
    # PASO 1: ARQUITECTURA & COPY (CONTENT AGENT)
    # =========================================================
    print(f"\n📝 [1/5] Generando Copy & Arquitectura Liquid...")
    
    # 1.1 Cargar Base
    try:
        with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
            shopify_base_json = json.load(f)
    except FileNotFoundError:
        print("❌ Error: Falta input_theme/product.custom_landing.json. Ejecuta get_shopify_theme.py.")
        return

    # 1.2 Generar Contenido (IA)
    # Check cache first for speed during dev
    cache_path = "output/ai_content_raw.json"
    if os.path.exists(cache_path) and os.getenv("USE_CACHE", "False").lower() == "true":
        print("   ⚠️ Usando caché de copy (output/ai_content_raw.json)")
        with open(cache_path, 'r', encoding='utf-8') as f:
            ai_content = json.load(f)
    else:
        ai_content = generate_elite_landing_copy(PRODUCT_NAME, RAW_INFO, TARGET_AVATAR)
        if not ai_content: return
        with open(cache_path, "w", encoding='utf-8') as f:
            json.dump(ai_content, f, indent=4, ensure_ascii=False)

    # 1.3 Map & Save
    final_landing_json = map_payload_to_shopify_structure(shopify_base_json, ai_content)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(final_landing_json, f, indent=4, ensure_ascii=False)
    
    # Important: Also save extracted copy for image agents!
    copy_path = os.path.join(TARGET_DIR, "extracted_marketing_copy.json")
    with open(copy_path, 'w', encoding='utf-8') as f:
        # We save the ai_content structure which usually has keys like 'key_benefits'
        json.dump(ai_content, f, indent=2, ensure_ascii=False)
    print(f"   ✅ Copy guardado en: {copy_path}")


    # =========================================================
    # PASO 2: GENERACIÓN DE IMÁGENES (VISUAL AGENTS)
    # =========================================================
    print(f"\n🎨 [2/5] Generando Recursos Visuales (Gemini)...")
    
    print("\n   🔹 2.1 Before vs After...")
    section_before_after.run_before_after_pipeline(product_folder_name)
    
    print("\n   🔹 2.2 Pain Visualization...")
    section_pain.run_pain_pipeline(product_folder_name)
    
    print("\n   🔹 2.3 Benefits (Batch Generation)...")
    section_benefits.run_benefits_pipeline(product_folder_name)
    
    print("\n   🔹 2.4 Social Proof (UGC)...")
    section_social_proof.run_social_proof_pipeline(product_folder_name)

    print("\n   🔹 2.5 Featured Review (Profile Pic)...")
    section_featured_review.run_featured_review_pipeline(product_folder_name)


    # =========================================================
    # PASO 3: CONTROL DE CALIDAD (EVALUATOR AGENT)
    # =========================================================
    print(f"\n⚖️ [3/5] Evaluación de Beneficios (PhD Judge)...")
    evaluator_benefits.run_evaluation_pipeline(product_folder_name)


    # =========================================================
    # PASO 4: DESPLIEGUE A SHOPIFY (DEPLOY AGENT)
    # =========================================================
    print(f"\n☁️ [4/5] Desplegando assets y parcheando Theme...")
    try:
        deploy_images.deploy_pipeline(product_folder_name)
    except Exception as e:
        print(f"❌ Error crítico en despliegue: {e}")
        return

    # =========================================================
    # PASO 5: CAPA VISUAL (COLOR & STYLE INJECTION)
    # =========================================================
    print(f"\n🎨 [5/5] Generando e Inyectando Plan Visual (Colores + Estilos Scoped)...")
    
    # 5.1 Plan Visual (Analiza copys e imagenes)
    try:
        planer = VisualPlaner()
        planer.analyze_and_generate(product_folder_name, PRODUCT_NAME)
    except Exception as e:
        print(f"⚠️ Error generando Visual Plan: {e}")

    # 5.2 Inyeccion Visual (Sobrescribe template con estilos)
    try:
        run_injection_pipeline(product_folder_name)
    except Exception as e:
        print(f"❌ Error inyectando estilos visuales: {e}")


    print("\n" + "="*60)
    print(f"🎉 ¡ÉXITO TOTAL! LANDING PAGE COMPLETADA + VISUAL STYLE")
    print("="*60)
    print(f"👉 Preview URL: (Check Shopify Admin -> Online Store -> Themes -> Customize)")
    print(f"👉 Template: {SHOPIFY_TEMPLATE_KEY}")

    # =========================================================
    # PASO 6: SUBIDA A DRIVE
    # =========================================================
    print(f"\n☁️ [6/5] Subiendo a Google Drive...")
    try:
        from tools.drive_uploader import upload_product_to_drive
        # product_folder_name is just the slug, e.g. "tenis_barbara"
        # We need the full path: output/tenis_barbara
        full_product_path = os.path.join("output", product_folder_name)
        upload_product_to_drive(full_product_path)
    except Exception as e:
        print(f"❌ Error en subida a Drive: {e}")


if __name__ == "__main__":
    main()