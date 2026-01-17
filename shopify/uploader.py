import os
import requests
from dotenv import load_dotenv
from utils.logger import setup_logger

logger = setup_logger("Shopify.Uploader")

load_dotenv()

def upload_to_shopify(local_filepath, shopify_filename):
    """
    Sube un archivo JSON local a Shopify como un Asset del tema.
    
    Args:
        local_filepath (str): Ruta del archivo en tu PC (ej: output/mi-landing.json)
        shopify_filename (str): Nombre final en Shopify (ej: templates/product.landing-v1.json)
    """
    
    # 1. Credenciales
    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    theme_id = os.getenv("THEME_ID")
    
    if not all([shop_url, access_token, theme_id]):
        logger.error("Faltan credenciales en .env para la subida.")
        return False

    # 2. Preparar el Endpoint
    url = f"https://{shop_url}/admin/api/2024-01/themes/{theme_id}/assets.json"
    
    # 3. Leer el contenido del archivo local
    try:
        with open(local_filepath, 'r', encoding='utf-8') as f:
            # Leemos como texto puro porque Shopify espera un string en el campo 'value'
            content_string = f.read()
    except FileNotFoundError:
        logger.error(f"No encuentro el archivo local: {local_filepath}")
        return False

    # 4. Crear el Payload (La carga útil)
    # IMPORTANTE: La 'key' debe incluir la carpeta, ej: 'templates/product.nombre.json'
    payload = {
        "asset": {
            "key": shopify_filename,
            "value": content_string
        }
    }

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    # 5. Enviar Request (PUT)
    logger.info(f"Subiendo a Shopify: {shopify_filename} ...")
    response = requests.put(url, headers=headers, json=payload)

    # 6. Validar Resultado
    if response.status_code in [200, 201]:
        logger.info(f"¡DEPLOY EXITOSO! Landing disponible en el tema {theme_id}")
        return True
    else:
        logger.error(f"Error en la subida ({response.status_code}):")
        logger.error(response.text)
        return False