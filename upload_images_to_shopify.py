import os
import requests

def upload_image_to_shopify_files(image_url, filename):
    """
    Descarga una imagen de una URL pública (ej. DALL-E) y la sube a Shopify Files.
    Devuelve la referencia 'shopify://shop_images/filename' para usar en el JSON.
    """
    shop_url = os.getenv("SHOP_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    
    # 1. Descargar imagen a memoria
    print(f"⬇️ Descargando imagen: {filename}...")
    img_data = requests.get(image_url).content
    
    # 2. Preparar Mutation para Staged Upload (GraphQL)
    graphql_url = f"https://{shop_url}/admin/api/2024-01/graphql.json"
    headers = {"X-Shopify-Access-Token": access_token}
    
    # Paso A: Solicitar permiso de subida
    mutation_staged = """
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets {
          url
          resourceUrl
          parameters {
            name
            value
          }
        }
      }
    }
    """
    
    variables = {
        "input": [{
            "resource": "FILE",
            "filename": filename,
            "mimeType": "image/jpeg",
            "httpMethod": "POST",
            "acl": "private"
        }]
    }
    
    response = requests.post(graphql_url, json={"query": mutation_staged, "variables": variables}, headers=headers)
    target = response.json()['data']['stagedUploadsCreate']['stagedTargets'][0]
    
    # Paso B: Subir el archivo real al bucket de Google Cloud que Shopify nos dio
    upload_url = target['url']
    params = {p['name']: p['value'] for p in target['parameters']}
    files = {'file': (filename, img_data, 'image/jpeg')}
    
    print(f"⬆️ Subiendo a Shopify Staging...")
    requests.post(upload_url, data=params, files=files)
    
    # Paso C: Registrar el archivo en Shopify (FileCreate)
    mutation_create = """
    mutation fileCreate($files: [FileCreateInput!]!) {
      fileCreate(files: $files) {
        files {
          alt
          createdAt
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    # IMPORTANTE: El resourceUrl es lo que conecta el paso B con el C
    resource_url = target['resourceUrl']
    
    variables_create = {
        "files": [{
            "originalSource": resource_url,
            "alt": f"Landing Image for {filename}"
        }]
    }
    
    final_res = requests.post(graphql_url, json={"query": mutation_create, "variables": variables_create}, headers=headers)
    
    if 'userErrors' in final_res.json()['data']['fileCreate'] and final_res.json()['data']['fileCreate']['userErrors']:
        print("❌ Error creando archivo:", final_res.json()['data']['fileCreate']['userErrors'])
        return None
        
    print(f"✅ Imagen registrada en Shopify.")
    
    # Devolvemos el string mágico que necesita el JSON template
    return f"shopify://shop_images/{filename}"

# Ejemplo de uso (Simulado)
# final_ref = upload_image_to_shopify_files("https://dalle-url...", "hero-section-v1.jpg")
# print(final_ref) # -> "shopify://shop_images/hero-section-v1.jpg"