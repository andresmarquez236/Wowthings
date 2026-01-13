import json

def map_payload_to_shopify_structure(shopify_json, ai_content):
    """
    Recibe:
    1. shopify_json: El diccionario del archivo product.custom_landing.json (La Plantilla)
    2. ai_content: El diccionario generado por la IA (El Contenido)
    
    Retorna:
    - Un nuevo diccionario JSON listo para subir a Shopify.
    """
    # Hacemos una copia profunda para no modificar el original en memoria
    final_json = json.loads(json.dumps(shopify_json))
    sections = final_json['sections']

    def _ensure_html(text):
        if not text: return ""
        text = text.strip()
        # Si ya empieza con tags permitidos, lo dejamos
        if text.startswith("<p") or text.startswith("<ul") or text.startswith("<ol") or text.startswith("<h"):
            return text
        # Si no, lo envolvemos en <p>
        return f"<p>{text}</p>"

    # ==========================================
    # 1. HERO SECTION (Sección 'main')
    # ==========================================
    main_blocks = sections['main']['blocks']
    hero_data = ai_content.get('hero_section', {})

    # A. Reviews Count (+560 REVIEWS)
    # ID: custom_liquid_9XDG7p
    if 'custom_liquid_9XDG7p' in main_blocks:
        current_html = main_blocks['custom_liquid_9XDG7p']['settings']['custom_liquid']
        # Reemplazamos genéricamente buscando el patrón de número o inyectando de nuevo
        # Para seguridad, reemplazamos el texto visible en el HTML
        if "+560 REVIEWS" in current_html:
            new_html = current_html.replace("+560 REVIEWS", hero_data.get('reviews_count', '+500 Happy Customers'))
            main_blocks['custom_liquid_9XDG7p']['settings']['custom_liquid'] = new_html

    # B. Propuesta de Valor (Título debajo del precio)
    # ID: text_gazJkx
    if 'text_gazJkx' in main_blocks:
        main_blocks['text_gazJkx']['settings']['text'] = f"<strong>{hero_data.get('value_proposition', '')}</strong>"

    # C. Beneficios Rápidos (Checkmarks)
    # ID: daa0e452-23d5-4bd1-afb3-35d7babab88c
    if 'daa0e452-23d5-4bd1-afb3-35d7babab88c' in main_blocks:
        qb = hero_data.get('quick_benefits', [])
        settings = main_blocks['daa0e452-23d5-4bd1-afb3-35d7babab88c']['settings']
        if len(qb) > 0: settings['key1'] = qb[0]
        if len(qb) > 1: settings['key2'] = qb[1]
        if len(qb) > 2: settings['key3'] = qb[2]

    # D. Envío (Shipping Arrow)
    # ID: eed7bb10-67b9-4655-8c71-ca64ed54fb48
    if 'eed7bb10-67b9-4655-8c71-ca64ed54fb48' in main_blocks:
        ship_data = hero_data.get('shipping_text', {})
        settings = main_blocks['eed7bb10-67b9-4655-8c71-ca64ed54fb48']['settings']
        settings['preshipsby'] = ship_data.get('pre_text', 'Se envía en')
        settings['shippingarrow'] = ship_data.get('days', '24h')
        settings['postshipsby'] = ship_data.get('post_text', 'días')

    # E. Review Destacada (Featured Review)
    # ID: 1c393517-7c5c-4915-ba22-5dbd8ede91e5
    if '1c393517-7c5c-4915-ba22-5dbd8ede91e5' in main_blocks:
        feat_rev = hero_data.get('featured_review', {})
        settings = main_blocks['1c393517-7c5c-4915-ba22-5dbd8ede91e5']['settings']
        settings['reviewtext'] = feat_rev.get('text', '')
        settings['reviewname'] = feat_rev.get('author', '')

    # F. Iconos de Confianza
    # ID: 2c100155-1d2d-4504-a37e-f2abd070099f
    if '2c100155-1d2d-4504-a37e-f2abd070099f' in main_blocks:
        icons = hero_data.get('trust_icons', [])
        settings = main_blocks['2c100155-1d2d-4504-a37e-f2abd070099f']['settings']
        if len(icons) > 0: settings['text1'] = icons[0]
        if len(icons) > 1: settings['text2'] = icons[1]

    # ==========================================
    # 2. PAIN POINTS & DESIRED OUTCOME
    # ==========================================
    pain_data = ai_content.get('pain_agitation_solution', {})
    
    # A. Desired Outcome (Rich Text)
    # ID: 7401e664-d026-4a46-bd1f-bc32c9f0558a
    section_outcome = sections.get('7401e664-d026-4a46-bd1f-bc32c9f0558a')
    if section_outcome:
        for block in section_outcome['blocks'].values():
            if block['type'] == 'heading':
                block['settings']['heading'] = pain_data.get('desired_outcome_title', '')
            if block['type'] == 'text':
                block['settings']['text'] = f"<p>{pain_data.get('desired_outcome_text', '')}</p>"

    # B. Pain Agitation (Image with Text)
    # ID: 3bc3f381-5c12-4e1e-9f92-289fb87d308d
    section_pain = sections.get('3bc3f381-5c12-4e1e-9f92-289fb87d308d')
    if section_pain:
        for block in section_pain['blocks'].values():
            if block['type'] == 'heading':
                block['settings']['heading'] = pain_data.get('pain_heading', '')
            if block['type'] == 'text':
                block['settings']['text'] = f"<p>{pain_data.get('pain_text', '')}</p>"

    # ==========================================
    # 3. VISUAL EVIDENCE (Antes / Después)
    # ID: 086a98ac-209d-4db2-9d62-5b0fe9663693
    # ==========================================
    visual_data = ai_content.get('visual_evidence', {}).get('before_after', {})
    section_ba = sections.get('086a98ac-209d-4db2-9d62-5b0fe9663693')
    if section_ba:
        section_ba['settings']['title'] = visual_data.get('title', 'Antes vs Después')
        section_ba['settings']['text'] = f"<p>{visual_data.get('description', '')}</p>"
        section_ba['settings']['before'] = visual_data.get('label_before', 'ANTES')
        section_ba['settings']['after'] = visual_data.get('label_after', 'DESPUÉS')

    # ==========================================
    # 4. DETAILED BENEFITS (Multicolumn 1)
    # ID: 50f8db15-9a00-4bfb-8176-786845498504
    # ==========================================
    benefits_data = ai_content.get('detailed_benefits', {}).get('columns', [])
    section_benefits = sections.get('50f8db15-9a00-4bfb-8176-786845498504')
    if section_benefits:
        # Encontramos los bloques de tipo columna y los llenamos en orden
        col_blocks = [b for b in section_benefits['blocks'].values() if b['type'] == 'column']
        for i, col in enumerate(col_blocks):
            if i < len(benefits_data):
                col['settings']['title'] = benefits_data[i]['title']
                col['settings']['text'] = f"<p>{benefits_data[i]['description']}</p>"

    # ==========================================
    # 5. SOCIAL PROOF (Testimonios Multicolumn)
    # ID: b6fc2703-d30b-40d4-b2cb-fcc2eade4e34
    # ==========================================
    social_data = ai_content.get('social_proof_deep', {}).get('testimonials', [])
    section_social = sections.get('b6fc2703-d30b-40d4-b2cb-fcc2eade4e34')
    if section_social:
        col_blocks = [b for b in section_social['blocks'].values() if b['type'] == 'column']
        for i, col in enumerate(col_blocks):
            if i < len(social_data):
                col['settings']['title'] = f"{social_data[i]['name']} ⭐️⭐️⭐️⭐️⭐️"
                col['settings']['text'] = f"<p>{social_data[i]['review']}</p>"

    # ==========================================
    # 5.1. SOCIAL PROOF (Image With Text)
    # ID: 78161370-bfb0-428e-adf1-f106aca5123b
    # ==========================================
    social_img_data = ai_content.get('social_proof_image_with_text', {})
    section_social_img = sections.get('78161370-bfb0-428e-adf1-f106aca5123b')
    if section_social_img:
        for block in section_social_img['blocks'].values():
            if block['type'] == 'heading':
                block['settings']['heading'] = social_img_data.get('heading', 'Resultados Reales')
            if block['type'] == 'text':
                block['settings']['text'] = _ensure_html(social_img_data.get('text', ''))

    # ==========================================
    # 6. COMPETITOR COMPARISON (Tabla)
    # ID: 435217f9-0491-4e04-93de-8fa3a5a996db
    # ==========================================
    comp_data = ai_content.get('competitor_comparison', {})
    section_comp = sections.get('435217f9-0491-4e04-93de-8fa3a5a996db')
    if section_comp:
        section_comp['settings']['title'] = comp_data.get('title', '')
        section_comp['settings']['text'] = f"<p>{comp_data.get('subtitle', '')}</p>"
        
        rows_data = comp_data.get('rows', [])
        row_blocks = [b for b in section_comp['blocks'].values() if b['type'] == 'row']
        
        for i, row in enumerate(row_blocks):
            if i < len(rows_data):
                row['settings']['benefit'] = rows_data[i]['feature']
                row['settings']['us'] = rows_data[i]['us']
                row['settings']['others'] = rows_data[i]['them']

    # ==========================================
    # 7. STATISTICS (Porcentajes)
    # ID: 3d28e77d-b7ce-4326-99e5-6a739a02727b
    # ==========================================
    stats_data = ai_content.get('statistics_section', {})
    section_stats = sections.get('3d28e77d-b7ce-4326-99e5-6a739a02727b')
    if section_stats:
        section_stats['settings']['title'] = stats_data.get('title', 'Resultados')
        stats_list = stats_data.get('stats', [])
        row_blocks = [b for b in section_stats['blocks'].values() if b['type'] == 'row']
        
        for i, row in enumerate(row_blocks):
            if i < len(stats_list):
                row['settings']['percentage'] = stats_list[i]['percentage']
                row['settings']['row_text'] = f"<p>{stats_list[i]['text']}</p>"

    # ==========================================
    # 8. FAQ (Preguntas Frecuentes)
    # ID: ffa79f40-3ca5-4c0f-842b-5fa404007924
    # ==========================================
    faq_data = ai_content.get('faq_section', {})
    section_faq = sections.get('ffa79f40-3ca5-4c0f-842b-5fa404007924')
    if section_faq:
        section_faq['settings']['heading'] = faq_data.get('heading', 'Preguntas Frecuentes')
        questions_list = faq_data.get('questions', [])
        # Buscamos bloques que sean 'collapsible_row'
        faq_blocks = [b for b in section_faq['blocks'].values() if b['type'] == 'collapsible_row']
        
        for i, row in enumerate(faq_blocks):
            if i < len(questions_list):
                row['settings']['heading'] = questions_list[i]['q']
                row['settings']['row_content'] = f"<p>{questions_list[i]['a']}</p>"

    # ==========================================
    # 9. EXTRA INFO TABS (Que Incluye, Como se usa, etc)
    #Keys: collapsible_tab_AUafHX, collapsible_tab_NKgDKr, etc.
    # ==========================================
    extra_data = ai_content.get('extra_info_tabs', {})
    
    # A. Que Incluye
    if 'collapsible_tab_AUafHX' in sections.get('main', {}).get('blocks', {}):
        sections['main']['blocks']['collapsible_tab_AUafHX']['settings']['content'] = _ensure_html(extra_data.get('whats_included', ''))

    # B. Como se usa
    if 'collapsible_tab_NKgDKr' in sections.get('main', {}).get('blocks', {}):
        sections['main']['blocks']['collapsible_tab_NKgDKr']['settings']['content'] = _ensure_html(extra_data.get('how_to_use', ''))

    # C. Información de Envío
    if 'c11bfb2f-901d-4b03-90cf-b6b766353d13' in sections.get('main', {}).get('blocks', {}):
        sections['main']['blocks']['c11bfb2f-901d-4b03-90cf-b6b766353d13']['settings']['content'] = _ensure_html(extra_data.get('shipping_info', ''))

    # D. Devoluciones (Warranty)
    if 'e6778313-45ad-4d6d-8260-2498242a6df0' in sections.get('main', {}).get('blocks', {}):
         sections['main']['blocks']['e6778313-45ad-4d6d-8260-2498242a6df0']['settings']['content'] = _ensure_html(extra_data.get('warranty_info', ''))

    return final_json