def map_payload_to_shopify_structure(shopify_json, ai_content):
    """
    Esta función es el CIRUJANO. Toma el contenido de la IA y lo injerta 
    quirúrgicamente en los IDs específicos de tu template.
    """
    
    sections = shopify_json['sections']
    
    # --- 1. HERO SECTION (Main) ---
    # Reviews
    sections['main']['blocks']['custom_liquid_9XDG7p']['settings']['custom_liquid'] = \
        sections['main']['blocks']['custom_liquid_9XDG7p']['settings']['custom_liquid'].replace("+560 REVIEWS", ai_content['hero_section']['reviews_count'])
    
    # Propuesta de Valor
    sections['main']['blocks']['text_gazJkx']['settings']['text'] = \
        f"<strong>{ai_content['hero_section']['value_proposition']}</strong>"
        
    # Beneficios Rápidos
    sections['main']['blocks']['daa0e452-23d5-4bd1-afb3-35d7babab88c']['settings']['key1'] = ai_content['hero_section']['quick_benefits'][0]
    sections['main']['blocks']['daa0e452-23d5-4bd1-afb3-35d7babab88c']['settings']['key2'] = ai_content['hero_section']['quick_benefits'][1]
    sections['main']['blocks']['daa0e452-23d5-4bd1-afb3-35d7babab88c']['settings']['key3'] = ai_content['hero_section']['quick_benefits'][2]
    
    # Envío
    sections['main']['blocks']['eed7bb10-67b9-4655-8c71-ca64ed54fb48']['settings']['shippingarrow'] = ai_content['hero_section']['shipping_text']['days']
    
    # --- 2. PAIN POINTS (Image with Text) ---
    pain_section_id = "3bc3f381-5c12-4e1e-9f92-289fb87d308d"
    # Título (Heading) - Iterar bloques para encontrar el heading
    for block_id, block in sections[pain_section_id]['blocks'].items():
        if block['type'] == 'heading':
            block['settings']['heading'] = ai_content['pain_agitation_solution']['pain_heading']
        if block['type'] == 'text':
            block['settings']['text'] = f"<p>{ai_content['pain_agitation_solution']['pain_text']}</p>"

    # --- 3. COMPARISON TABLE (Compare Chart) ---
    comp_section_id = "435217f9-0491-4e04-93de-8fa3a5a996db"
    sections[comp_section_id]['settings']['title'] = ai_content['competitor_comparison']['title']
    sections[comp_section_id]['settings']['text'] = f"<p>{ai_content['competitor_comparison']['subtitle']}</p>"
    
    # Filas de comparación
    # Nota: Aquí deberíamos iterar por los bloques 'row' en el orden correcto
    # Simplificación para el ejemplo:
    row_blocks = [b for b in sections[comp_section_id]['blocks'].values() if b['type'] == 'row']
    for i, row in enumerate(row_blocks):
        if i < len(ai_content['competitor_comparison']['rows']):
            data = ai_content['competitor_comparison']['rows'][i]
            row['settings']['benefit'] = data['feature']
            row['settings']['us'] = data['us']
            row['settings']['others'] = data['them']

    # --- 4. DETAILED BENEFITS (Multicolumn) ---
    benefits_id = "50f8db15-9a00-4bfb-8176-786845498504"
    col_blocks = [b for b in sections[benefits_id]['blocks'].values() if b['type'] == 'column']
    for i, col in enumerate(col_blocks):
        if i < len(ai_content['detailed_benefits']['columns']):
            data = ai_content['detailed_benefits']['columns'][i]
            col['settings']['title'] = data['title']
            col['settings']['text'] = f"<p>{data['description']}</p>"

    # --- 5. FAQ (Collapsible) ---
    faq_id = "ffa79f40-3ca5-4c0f-842b-5fa404007924"
    sections[faq_id]['settings']['heading'] = ai_content['faq_section']['heading']
    faq_rows = [b for b in sections[faq_id]['blocks'].values() if b['type'] == 'collapsible_row']
    for i, row in enumerate(faq_rows):
        if i < len(ai_content['faq_section']['questions']):
            data = ai_content['faq_section']['questions'][i]
            row['settings']['heading'] = data['q']
            row['settings']['row_content'] = f"<p>{data['a']}</p>"

    return shopify_json