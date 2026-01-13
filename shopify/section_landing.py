import json
import os
import re

def extract_content(landing_page_path):
    """
    Extracts specific content sections from a Shopify landing page JSON and creates a structured dictionary.
    
    Args:
        landing_page_path (str): Path to the input JSON file.
        
    Returns:
        dict: Extracted content with professional keys.
    """
    with open(landing_page_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    sections = data.get('sections', {})
    content_map = {}

    # Helper to strip HTML tags for cleaner output, if desired. 
    # For now, we will keep HTML tags or minimal cleaning as the user asked for "copies" 
    # which might need to be reused with formatting. 
    # If "clean text" is needed, we can add a regex stripper.
    def clean_text(html_text):
        if not html_text: return ""
        # Remove simple paragraph tags if they are the only thing
        text = re.sub(r'^<p>', '', html_text)
        text = re.sub(r'</p>$', '', text)
        return text

    # 1. Beneficios del producto antes y despues (Before/After)
    # Section ID: 086a98ac-209d-4db2-9d62-5b0fe9663693 (Type: compare-image)
    before_after_section = sections.get('086a98ac-209d-4db2-9d62-5b0fe9663693', {})
    if before_after_section:
        settings = before_after_section.get('settings', {})
        content_map['transformation_highlight'] = {
            "title": settings.get('title', ''),
            "description": clean_text(settings.get('text', '')),
            "label_before": settings.get('before', ''),
            "label_after": settings.get('after', '')
        }

    # 2. Refuerzo del punto de dolor (Pain Point Reinforcement)
    # Section ID: 3bc3f381-5c12-4e1e-9f92-289fb87d308d (Type: image-with-text)
    pain_point_section = sections.get('3bc3f381-5c12-4e1e-9f92-289fb87d308d', {})
    if pain_point_section:
        blocks = pain_point_section.get('blocks', {})
        heading = ""
        text = ""
        for block in blocks.values():
            if block['type'] == 'heading':
                heading = block['settings'].get('heading', '')
            if block['type'] == 'text':
                text = clean_text(block['settings'].get('text', ''))
        
        content_map['pain_point_resolution'] = {
            "heading": heading,
            "narrative": text
        }

    # 3. Beneficios que haya (Key Benefits & Detailed Benefits)
    # We have "quick_benefits" in main section (keybenefit block) and "detailed_benefits" in multicolumn
    
    # Quick Benefits (Main Section)
    main_section = sections.get('main', {})
    main_blocks = main_section.get('blocks', {})
    # Block ID: daa0e452-23d5-4bd1-afb3-35d7babab88c
    quick_benefits_data = []
    if 'daa0e452-23d5-4bd1-afb3-35d7babab88c' in main_blocks:
        settings = main_blocks['daa0e452-23d5-4bd1-afb3-35d7babab88c'].get('settings', {})
        quick_benefits_data = [
            settings.get('key1', ''),
            settings.get('key2', ''),
            settings.get('key3', '')
        ]
        # Filter empty
        quick_benefits_data = [b for b in quick_benefits_data if b]

    # Detailed Benefits (Multicolumn)
    # Section ID: 50f8db15-9a00-4bfb-8176-786845498504
    detailed_benefits_data = []
    det_benefits_section = sections.get('50f8db15-9a00-4bfb-8176-786845498504', {})
    if det_benefits_section:
        for block in det_benefits_section.get('blocks', {}).values():
            if block['type'] == 'column':
                detailed_benefits_data.append({
                    "title": block['settings'].get('title', ''),
                    "description": clean_text(block['settings'].get('text', ''))
                })

    content_map['key_benefits'] = {
        "quick_list": quick_benefits_data,
        "detailed_features": detailed_benefits_data
    }

    # 4. Prueba Social (Social Proof)
    # Includes Text Testimonials, Image w/ Text Testimonial, and Statistics
    social_proof_data = {}

    # A. Text Testimonials (Multicolumn)
    # Section ID: b6fc2703-d30b-40d4-b2cb-fcc2eade4e34
    testimonials = []
    testimonials_section = sections.get('b6fc2703-d30b-40d4-b2cb-fcc2eade4e34', {})
    if testimonials_section:
        for block in testimonials_section.get('blocks', {}).values():
            if block['type'] == 'column':
                testimonials.append({
                    "author": block['settings'].get('title', ''),
                    "review": clean_text(block['settings'].get('text', ''))
                })
    social_proof_data['testimonials'] = testimonials

    # B. Image with Text Testimonial (Visual Proof)
    # Section ID: 78161370-bfb0-428e-adf1-f106aca5123b
    visual_proof_section = sections.get('78161370-bfb0-428e-adf1-f106aca5123b', {})
    if visual_proof_section:
        blocks = visual_proof_section.get('blocks', {})
        heading = ""
        text = ""
        for block in blocks.values():
            if block['type'] == 'heading':
                heading = block['settings'].get('heading', '')
            if block['type'] == 'text':
                text = clean_text(block['settings'].get('text', ''))
        
        social_proof_data['featured_visual_case'] = {
            "headline": heading,
            "story": text
        }

    content_map['social_proof'] = social_proof_data

    # Return structured dict
    return content_map

def save_extracted_content(landing_page_path):
    """
    Orchestrates extraction and saving to the correct location.
    """
    
    print(f"📂 Extrayendo contenido de: {landing_page_path}")
    
    if not os.path.exists(landing_page_path):
        print(f"❌ Error: El archivo {landing_page_path} no existe.")
        return

    extracted_data = extract_content(landing_page_path)
    
    # Determinar ruta de salida
    # Input example: 'output/samba_og_vaca_negro_blanco/resultados_landing/product.landing-samba-og-vaca-negro-blanco.json' (The new structure)
    # OR 'output/product.landing-samba-og-vaca-negro-blanco.json' (If user points to old file)
    
    # La instrucción dice: "dentro de la carpeta de cada producto... crear una carpeta llamada resultados_landing"
    # Si el input ya está ahí, guardamos el resultado al lado.
    
    input_dir = os.path.dirname(landing_page_path)
    output_filename = "extracted_marketing_copy.json"
    
    # Case: The file is in the root output folder, we need to create the product folder
    # But usually we expect the landing_page_path to be dynamic. 
    # Let's assume we save it in the same directory as the input file as a safe default, 
    # OR try to deduce the product folder if it's in the root.
    
    target_dir = input_dir
    
    # Check if we are in 'output' root and need to move deeper?
    # The user asked to create `resultados_landing` inside the product folder.
    # If the input file is `output/product.landing...json`, we need to derive the folder name.
    
    # Let's verify where the user said the file IS.
    # "a partir del archivo ejem output/product.landing-samba-og-vaca-negro-blanco.json"
    # This implies the file is currently in `output/`.
    
    # Logic to find product folder:
    # Filename format: product.landing-[slug].json
    basename = os.path.basename(landing_page_path)
    if basename.startswith("product.landing-") and basename.endswith(".json"):
        slug = basename.replace("product.landing-", "").replace(".json", "")
        # Assuming product folder matches slug or has underscores instead of dashes
        # The user said: "los nombres de las carpetas de los productos son el nombre pero con _"
        product_folder_name = slug.replace("-", "_")
        
        # Construct target path
        # If we run this script from root: `output/{product_folder_name}/resultados_landing`
        potential_target_dir = os.path.join("output", product_folder_name, "resultados_landing")
        
        # If that folder exists or we can create it, prefer it.
        # But if the input file was ALREADY in that folder (from previous step), then input_dir is already correct.
        if "resultados_landing" not in input_dir:
             target_dir = potential_target_dir
             os.makedirs(target_dir, exist_ok=True)
    
    output_path = os.path.join(target_dir, output_filename)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)
        
    print(f"✅ Contenido extraído y guardado en: {output_path}")

if __name__ == "__main__":
    # Default behavior for testing or direct execution
    # Try to find the file mentioned in the prompt if argument not provided
    import sys
    
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # Default fallback for testing
        file_path = "output/product.landing-samba-og-vaca-negro-blanco.json"
        
    save_extracted_content(file_path)
