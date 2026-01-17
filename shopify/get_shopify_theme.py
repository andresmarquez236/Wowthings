# import os
# import requests
# import json
# from dotenv import load_dotenv

# def list_themes():
#     # Endpoint para VER TODOS los temas
#     url = f"https://{SHOP_URL}/admin/api/2024-01/themes.json"
    
#     headers = {
#         "X-Shopify-Access-Token": ACCESS_TOKEN,
#         "Content-Type": "application/json"
#     }
    
#     response = requests.get(url, headers=headers)
    
#     if response.status_code == 200:
#         themes = response.json().get('themes', [])
#         print(f"✅ Conexión Exitosa. Se encontraron {len(themes)} temas:\n")
#         for theme in themes:
#             print(f"ID: {theme['id']} | Nombre: {theme['name']} | Rol: {theme['role']}")
#             if theme['role'] == 'main':
#                 print("   ⭐️ (Este es tu tema activo actual)")
#         print("\nCopia el ID del tema que quieres usar y ponlo en tu .env")
#     else:
#         print(f"❌ Error {response.status_code}: {response.text}")


# def get_shopify_theme():
#     # Load environment variables
#     load_dotenv()

#     shop_url = os.getenv("SHOP_URL")
#     access_token = os.getenv("ACCESS_TOKEN")
#     theme_id = os.getenv("THEME_ID")
    
#     if not all([shop_url, access_token, theme_id]):
#         print("Error: Missing environment variables. Please check your .env file.")
#         return

#     # Construct the API URL
#     # Note: Using the 2024-01 API version as per plan, but it's good practice to verify.
#     # The endpoint for a specific theme is /admin/api/{version}/themes/{theme_id}.json
#     url = f"https://{shop_url}/admin/api/2024-01/themes/{theme_id}.json"

#     headers = {
#         "X-Shopify-Access-Token": access_token,
#         "Content-Type": "application/json"
#     }

#     try:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
        
#         theme_data = response.json()
#         print("Theme Data Retrieved Successfully:")
#         print(theme_data)
        
#         # Save to file
#         output_dir = "input_theme"
#         os.makedirs(output_dir, exist_ok=True)
#         output_file = os.path.join(output_dir, "theme.json")
        
#         with open(output_file, "w") as f:
#             json.dump(theme_data, f, indent=4)
            
#         print(f"Theme data saved to {output_file}")
        
#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching theme: {e}")
#         if 'response' in locals() and response is not None:
#              print(f"Response status: {response.status_code}")
#              print(f"Response text: {response.text}")

# if __name__ == "__main__":
#     get_shopify_theme()

import os
import requests
import json
from dotenv import load_dotenv
from utils.logger import setup_logger

logger = setup_logger("Shopify.GetTheme")

def get_shopify_template_content():
    # Load environment variables
    load_dotenv()

    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    theme_id = os.getenv("THEME_ID")
    
    # IMPORTANTE: El nombre exacto del archivo que creaste en el editor de Shopify
    # Si no estás seguro, revisa el script de "listar archivos" más abajo.
    # Ejemplo: "templates/product.landing-master-aida.json"
    asset_key = os.getenv("TEMPLATE_FILENAME", "templates/product.custom_landing.json") 
    
    if not all([shop_url, access_token, theme_id]):
        logger.error("Faltan variables en el .env")
        return

    # --- CAMBIO CLAVE AQUÍ ---
    # Usamos el endpoint de 'assets.json' y pasamos el parámetro asset[key]
    url = f"https://{shop_url}/admin/api/2024-01/themes/{theme_id}/assets.json"
    
    params = {
        "asset[key]": asset_key
    }

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"Buscando el archivo: {asset_key} ...")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # La API de Assets devuelve el contenido dentro de ['asset']['value']
        if 'asset' in data and 'value' in data['asset']:
            content_string = data['asset']['value']
            
            # Como es un archivo .json, convertimos el string a objeto Python
            json_content = json.loads(content_string)

            # Guardar archivo
            output_dir = "input_theme"
            os.makedirs(output_dir, exist_ok=True)
            # Usamos el nombre del archivo original para guardar
            filename = asset_key.split('/')[-1] 
            output_file = os.path.join(output_dir, filename)
            
            with open(output_file, "w", encoding='utf-8') as f:
                json.dump(json_content, f, indent=4)
                
            logger.info(f"¡ÉXITO! Plantilla descargada en: {output_file}")
            logger.info("Ahora tu IA puede leer este archivo para entender la estructura.")
        else:
            logger.warning("El archivo existe, pero no tiene contenido 'value' (podría ser una imagen binaria o estar vacío).")
            logger.debug(str(data))

    except requests.exceptions.HTTPError as err:
        if response.status_code == 404:
            logger.error(f"Error 404: El archivo '{asset_key}' no existe en este tema.")
            logger.error("Verifica que creaste la plantilla y que el nombre es exacto.")
        else:
            logger.error(f"Error HTTP: {err}")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")

if __name__ == "__main__":
    get_shopify_template_content()