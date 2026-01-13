import os
import json
from openai import OpenAI
from dotenv import load_dotenv

# Cargar entorno
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_elite_landing_copy(product_name, raw_info, target_avatar):
    """
    Genera el contenido COMPLETO para la landing page mapeada al JSON de Shopify.
    """
    
    # 1. Definición del System Prompt (El Cerebro)
    system_prompt = """
    [ROLE]
    You are the "Conversion Architect AI". You combine the skills of:
    - Eugene Schwartz (Copywriting Legend)
    - Jakob Nielsen (UX Research)
    - Robert Cialdini (Psychology of Persuasion)

    [OBJECTIVE]
    Generate a COMPLETE, HIGH-CONVERSION content package for a Shopify Landing Page. 
    The output must strictly follow the JSON structure provided.
    
    [FRAMEWORKS TO APPLY]
    1. **The 'Switch' Method:** Address the user's current pain (Before) and the dream outcome (After).
    2. **Objection Killing:** Every FAQ and Benefit must proactively address a doubt (Price, Trust, Speed, Difficulty).
    3. **Specificity:** Do not say "High Quality". Say "Made with 100% Organic Cotton".
    4. **Constraints:**
        - **FAQs:** Generate SINGLE, DISTINCT questions. Do not combine multiple questions in one.
        - **Benefits:** Titles max 40 chars. Descriptions max 140 chars (approx 20 words). Concise and punchy.
    
    [LANGUAGE]
    Spanish (Neutral/Latinoamérica) - Persuasive and Direct.
    
    [IMAGE PROMPTS]
    For the 'image_prompts' section, provide detailed English prompts for DALL-E 3 (Photorealistic, Studio Lighting, Commercial style).
    """

    # 2. Definición del User Prompt con la Estructura (El Cuerpo)
    # Incluimos el schema como string para que el modelo lo rellene
    user_prompt = f"""
    PRODUCT: {product_name}
    CONTEXT: {raw_info}
    TARGET AUDIENCE: {target_avatar}

    Please generate the content filling exactly this JSON structure:
    
    {{
        "hero_section": {{
            "value_proposition": "String (Short & Punchy)",
            "reviews_count": "String (e.g. '+1200 MUJERES FELICES')",
            "shipping_text": {{ "pre_text": "String", "days": "String", "post_text": "String" }},
            "trust_icons": ["String", "String"],
            "featured_review": {{ "text": "String", "author": "String" }},
            "quick_benefits": ["String", "String", "String"]
        }},
        "pain_agitation_solution": {{
            "desired_outcome_title": "String (H2)",
            "desired_outcome_text": "String (Paragraph)",
            "pain_heading": "String (Max 5 words)",
            "pain_text": "String (Agitate the problem)"
        }},
        "visual_evidence": {{
            "before_after": {{ 
                "title": "String", 
                "description": "String", 
                "label_before": "ANTES", 
                "label_after": "DESPUÉS" 
            }}
        }},
        "detailed_benefits": {{
            "columns": [
                {{ "title": "String (Max 40 chars)", "description": "String (Max 140 chars)" }},
                {{ "title": "String (Max 40 chars)", "description": "String (Max 140 chars)" }},
                {{ "title": "String (Max 40 chars)", "description": "String (Max 140 chars)" }},
                {{ "title": "String (Max 40 chars)", "description": "String (Max 140 chars)" }}
            ]
        }},
        "social_proof_deep": {{
            "testimonials": [
                {{ "name": "String", "review": "String" }},
                {{ "name": "String", "review": "String" }},
                {{ "name": "String", "review": "String" }}
            ],
            "highlight_section": {{ "heading": "String", "text": "String" }}
        }},
        "social_proof_image_with_text": {{
            "heading": "String (Short powerful testimony title)",
            "text": "String (the testimonial text explaining the photo result)",
            "image_prompt": "String (Description of the user photo)"
        }},
        "competitor_comparison": {{
            "title": "String",
            "subtitle": "String",
            "rows": [
                {{ "feature": "String", "us": true, "them": false }},
                {{ "feature": "String", "us": true, "them": false }},
                {{ "feature": "String", "us": true, "them": false }},
                {{ "feature": "String", "us": true, "them": false }},
                {{ "feature": "String", "us": true, "them": false }}
            ]
        }},
        "statistics_section": {{
            "title": "String",
            "stats": [
                {{ "percentage": number, "text": "String" }},
                {{ "percentage": number, "text": "String" }},
                {{ "percentage": number, "text": "String" }}
            ]
        }},
        "faq_section": {{
            "heading": "String",
            "questions": [
                {{ "q": "String (Unique question)", "a": "String" }},
                {{ "q": "String (Unique question)", "a": "String" }},
                {{ "q": "String (Unique question)", "a": "String" }},
                {{ "q": "String (Unique question)", "a": "String" }}
            ]
        }},
        "extra_info_tabs": {{
            "whats_included": "String (List content in HTML <ul><li> format)",
            "how_to_use": "String (Step-by-step instructions in HTML <p> or <ol> format)",
            "shipping_info": "String (Shipping policy, times, carriers)",
            "warranty_info": "String (Warranty text and return policy)"
        }},
        "image_prompts": {{
            "hero_background": "String (English)",
            "pain_visual": "String (English)",
            "social_proof_photo": "String (English)",
            "product_in_use": "String (English)"
        }}
    }}
    """
    
    print(f"🚀 Iniciando generación Neural para: {product_name}...")

    try:
        response = client.chat.completions.create(
            model="gpt-5.1", 
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.75, # Un poco más alto para creatividad en marketing
            response_format={"type": "json_object"}
        )
        
        return json.loads(response.choices[0].message.content)

    except Exception as e:
        print(f"❌ Error Critical: {e}")
        return None

# --- TESTING ---
if __name__ == "__main__":
    # Test Data
    prod = "LuminoSleep - Antifaz Inteligente"
    raw = "Antifaz con bluetooth, auriculares planos, bloqueo de luz 100%, memory foam."
    target = "Viajeros frecuentes y personas con insomnio ligero."

    content = generate_elite_landing_copy(prod, raw, target)
    
    if content:
        # Guardamos el JSON de contenido puro
        with open("final_content_payload.json", "w", encoding='utf-8') as f:
            json.dump(content, f, indent=4, ensure_ascii=False)
        print("✅ Payload de Contenido generado. Listo para inyectar en Shopify.")