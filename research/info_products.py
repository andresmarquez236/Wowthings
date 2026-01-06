import gspread
from google.oauth2.service_account import Credentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from typing import List, Any, Tuple
import io
import os

# Constants
SERVICE_ACCOUNT_FILE = "gen-lang-client-0178743357-82f8fdac6954.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEET_URL = "https://docs.google.com/spreadsheets/d/13rXJoDZyFd_zdUatI0rFIyG5TNoPxngROYa9ts4cSho/edit?gid=299724498#gid=299724498"
WORKSHEET_INFO = "Info_Productos"
WORKSHEET_RESULTS = "Resultados_Estudio"
START_ROW = 5  # Data starts at row 5

def get_google_sheet_client():
    """Authenticates and returns the gspread client."""
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    return gspread.authorize(creds)

def get_drive_service():
    """Authenticates and returns the Drive API service."""
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    return build('drive', 'v3', credentials=creds)

def download_product_images(product_name: str, local_output_dir: str) -> bool:
    """
    Searches for a folder named 'product_name' in the specific parent folder.
    Downloads all images found to 'local_output_dir'.
    Returns True if successful and images were found/downloaded.
    """
    PARENT_FOLDER_ID = "1QquAjl4BJsr0mR2s19ZXKIY0PTjl62CO"
    
    print(f"   ☁️ Drive: Searching for folder '{product_name}'...")
    service = get_drive_service()
    
    # 1. Search for folder
    # Escape single quotes in product name if present
    query_name = product_name.replace("'", "\\'")
    q = f"'{PARENT_FOLDER_ID}' in parents and name = '{query_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    
    results = service.files().list(q=q, fields="files(id, name)").execute()
    folders = results.get('files', [])
    
    if not folders:
        # Try case-insensitive? Drive search is case-insensitive for 'name contains', but '=' is specific.
        # Let's try 'contains' if exact match fails, or assume exact match is required as per user instructions.
        # Check user prompt: "hay que buscar la carpeta con el nombre del producto".
        # Let's try a broader search just in case: name contains product_name
        print(f"   ⚠️ Exact match not found. Trying contains '{product_name}'...")
        q_broad = f"'{PARENT_FOLDER_ID}' in parents and name contains '{query_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results_broad = service.files().list(q=q_broad, fields="files(id, name)").execute()
        folders = results_broad.get('files', [])
        
        if not folders:
            print(f"   ❌ Drive folder for '{product_name}' not found.")
            return False
            
    # Use the first found folder
    folder = folders[0]
    folder_id = folder['id']
    folder_real_name = folder['name']
    print(f"   ✅ Found folder: {folder_real_name} (ID: {folder_id})")
    
    # 2. List images in that folder
    q_imgs = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed = false"
    results_imgs = service.files().list(q=q_imgs, fields="files(id, name, mimeType)").execute()
    files = results_imgs.get('files', [])
    
    if not files:
        print("   ⚠️ No images found in the Drive folder.")
        return False
        
    # 3. Download images
    os.makedirs(local_output_dir, exist_ok=True)
    count = 0
    for file in files:
        file_id = file['id']
        file_name = file['name']
        
        # Avoid duplicates or just overwrite? Overwrite is safer to ensure latest.
        file_path = os.path.join(local_output_dir, file_name)
        
        # print(f"      ⬇️ Downloading {file_name}...")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            # print(f"Download {int(status.progress() * 100)}%.")
            
        with open(file_path, "wb") as f:
            f.write(fh.getbuffer())
        count += 1
        
    print(f"   ✅ Downloaded {count} images to {local_output_dir}")
    return True

def get_filtered_products() -> List[Tuple[int, List[Any]]]:
    """
    Returns products that have 'SI' in Column E (Index 4).
    Returns a list of tuples: (row_index_1_based, row_data_list)
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    ws = spreadsheet.worksheet(WORKSHEET_INFO)
    
    # Using .get for specific range A5:L to include new columns up to Status
    # Columns:
    # A: Campana, B: ID, C: Nombre, D: Precio, E: Test (SI), F: Margen, G: Rent, H: Desc, I: Garantia, J: Stock, K: Estudio, L: Estado
    
    rows = ws.get(f"A{START_ROW}:L")
    
    filtered = []
    
    for i, row in enumerate(rows):
        # Calculate actual sheet row index
        # i=0 is START_ROW (5)
        current_row_idx = START_ROW + i
        
        # Ensure row has enough columns
        if len(row) > 4:
            # Column E is index 4 (Must be "SI")
            val_e = str(row[4]).strip().upper()
            
            # Column K is index 10 (Estudio Hecho - Should NOT be "SI")
            val_k = ""
            if len(row) > 10:
                val_k = str(row[10]).strip().upper()
                
            if val_e == "SI" and val_k != "SI":
                filtered.append((current_row_idx, row))
    
    return filtered

def update_product_status(row_idx: int, study_done: str = "SI", approved_status: str = "NO"):
    """
    Updates Column K (Estudio Hecho) and L (Estado) for the given row.
    Col K is 11th letter, Col L is 12th.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    ws = spreadsheet.worksheet(WORKSHEET_INFO)

    # Update cells
    # K -> Col 11, L -> Col 12
    ws.update_cell(row_idx, 11, study_done)
    ws.update_cell(row_idx, 12, approved_status)
    print(f"✅ Updated Row {row_idx}: Estudio={study_done}, Estado={approved_status}")

def log_study_result(result_data: dict):
    """
    Appends a row to 'Resultados_Estudio' sheet.
    Expected dict structure matches checklist Output + Product Name + Approved.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    
    try:
        ws_res = spreadsheet.worksheet(WORKSHEET_RESULTS)
    except gspread.exceptions.WorksheetNotFound:
        # Create if not exists (Optional, usually we expect it to exist)
        ws_res = spreadsheet.add_worksheet(title=WORKSHEET_RESULTS, rows=1000, cols=20)
        # Add Header
        header = ["Nombre Producto", "Aprobado (9/13)"] + [k for k in result_data.keys() if k not in ["Nombre Producto", "APROBADO (>9/13)", "Total SI", "Score"]]
        ws_res.append_row(header)

    # Prepare values row based on header if possible, or just dump dict values in specific order?
    # Better to enforce order.
    # User said: "colocar columna nombre_prodcuto, todas las columnas de estudio, cumple_9_de_15"
    
    # Let's dynamically determine headers if sheet is empty, or append matches.
    # For simplicity, we just append the values we have ensuring Name and Approval are first/last or clearly present.
    
    # Flatten dict to list
    # Fixed Order Preference:
    # 1. Nombre Producto
    # 2. Aprobado
    # 3. ... Checklist items ...
    
    row_values = [
        result_data.get("Nombre Producto", ""),
        result_data.get("APROBADO (>9/13)", "")
    ]
    
    # Add other keys sorted or constant
    # We filter out the ones we already added
    ignored = {"Nombre Producto", "APROBADO (>9/13)", "Status", "Precio", "Garantia", "Total SI"}
    
    others = [str(v) for k, v in result_data.items() if k not in ignored]
    
    row_values.extend(others)
    
    ws_res.append_row(row_values)
    print(f"📝 Logged result for: {result_data.get('Nombre Producto', 'Unknown')}")

def main():
    print("🚀 Fetching and filtering products...")
    
    try:
        filtered_items = get_filtered_products()
        
        print(f"\n✅ Found {len(filtered_items)} products with 'SI' in Column E:\n")
        for idx, row in filtered_items:
            print(f"Row {idx}: {row}")
            
            # Test update on only the first one if run directly?
            # update_product_status(idx, "SI", "TEST_PENDING")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
