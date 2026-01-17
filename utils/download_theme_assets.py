import os
import requests
from dotenv import load_dotenv
import json

load_dotenv()

API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-01")
SHOP_URL = os.getenv("SHOP_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
THEME_ID = os.getenv("THEME_ID")

def download_asset(key):
    url = f"https://{SHOP_URL}/admin/api/{API_VERSION}/themes/{THEME_ID}/assets.json?asset[key]={key}"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        content = data.get("asset", {}).get("value", "")
        if content:
            local_filename = key.split("/")[-1]
            os.makedirs("downloaded_assets", exist_ok=True)
            with open(f"downloaded_assets/{local_filename}", "w") as f:
                f.write(content)
            print(f"✅ Downloaded {key}")
        else:
            print(f"⚠️ No content for {key}")
    else:
        print(f"❌ Failed to download {key}: {response.text}")

if __name__ == "__main__":
    assets_to_download = [
        "assets/custom.css",
        "assets/custom2.css",
        "assets/base.css", 
        "assets/global.css" # Just in case
    ]
    
    print("Downloading theme stylesheets...")
    for asset in assets_to_download:
        download_asset(asset)
