import gspread
from google.oauth2.service_account import Credentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from typing import List, Any, Tuple, Dict, Optional
from typing import List, Any, Tuple, Dict
import io
import os

from utils.logger import setup_logger
logger = setup_logger("InfoProducts")

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
    
    logger.info(f"Drive: Searching for folder '{product_name}'...")
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
        logger.warning(f"Exact match not found. Trying contains '{product_name}'...")
        q_broad = f"'{PARENT_FOLDER_ID}' in parents and name contains '{query_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results_broad = service.files().list(q=q_broad, fields="files(id, name)").execute()
        folders = results_broad.get('files', [])
        
        if not folders:
            logger.error(f"Drive folder for '{product_name}' not found.")
            return False
            
    # Use the first found folder
    folder = folders[0]
    folder_id = folder['id']
    folder_real_name = folder['name']
    logger.info(f"Found folder: {folder_real_name} (ID: {folder_id})")
    
    # 2. List images in that folder
    q_imgs = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed = false"
    results_imgs = service.files().list(q=q_imgs, fields="files(id, name, mimeType)").execute()
    files = results_imgs.get('files', [])
    
    if not files:
        logger.warning("No images found in the Drive folder.")
        return False
        
    # 3. Download images
    os.makedirs(local_output_dir, exist_ok=True)
    count = 0
    for file in files:
        file_id = file['id']
        file_name = file['name']
        
        # Avoid duplicates or just overwrite? Overwrite is safer to ensure latest.
        file_path = os.path.join(local_output_dir, file_name)
        
        # logger.debug(f"Downloading {file_name}...")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
        with open(file_path, "wb") as f:
            f.write(fh.getbuffer())
        count += 1
        
    logger.info(f"Downloaded {count} images to {local_output_dir}")
    return True

def upload_folder_to_drive(local_folder_path: str, parent_folder_id: str = "1U4lWIeqyKojgG-KDFwataZtiNkhxqNTe"):
    """
    Recursively uploads a local folder to Google Drive.
    """
    service = get_drive_service()
    
    folder_name = os.path.basename(local_folder_path)
    logger.info(f"(Drive) Uploading '{folder_name}' to Drive ID: {parent_folder_id}")

    # 1. Check if folder already exists in parent
    q = f"'{parent_folder_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    existing = service.files().list(q=q, fields="files(id)").execute().get('files', [])
    
    if existing:
        drive_folder_id = existing[0]['id']
        logger.info(f"Folder '{folder_name}' exists (ID: {drive_folder_id}). Merging contents.")
    else:
        # Create folder
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        folder = service.files().create(body=file_metadata, fields='id').execute()
        drive_folder_id = folder.get('id')
        logger.info(f"Created Drive Folder: {folder_name} (ID: {drive_folder_id})")
        
    # 2. Upload files and recurse folders
    for item in os.listdir(local_folder_path):
        item_path = os.path.join(local_folder_path, item)
        
        if os.path.isfile(item_path):
            # Check if file exists to avoid duplicates? Or overwrite? 
            # Simple check: name matches
            q_file = f"'{drive_folder_id}' in parents and name = '{item}' and trashed = false"
            params = {'q': q_file, 'fields': 'files(id)'}
            exist_files = service.files().list(**params).execute().get('files', [])
            
            if exist_files:
                # Skip or Update? Let's skip to save time/bandwidth unless needed.
                logger.info(f"File '{item}' already exists. Skipping.")
                continue
                
            logger.info(f"Uploading file: {item}")
            media = MediaFileUpload(item_path, resumable=True)
            file_metadata = {'name': item, 'parents': [drive_folder_id]}
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            
        elif os.path.isdir(item_path):
            # Recurse
            upload_folder_to_drive(item_path, parent_folder_id=drive_folder_id)
            
    logger.info(f"Upload of '{folder_name}' completed.")

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
    logger.info(f"Updated Row {row_idx}: Estudio={study_done}, Estado={approved_status}")

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
        header = ["Nombre Producto", "Aprobado (9/12)"] + [k for k in result_data.keys() if k not in ["Nombre Producto", "APROBADO (>9/12)", "Total SI", "Score"]]
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
        result_data.get("APROBADO (>9/12)", "")
    ]
    
    # Add other keys sorted or constant
    # We filter out the ones we already added
    ignored = {"Nombre Producto", "APROBADO (>9/12)", "Status", "Precio", "Garantia", "Total SI"}
    
    others = [str(v) for k, v in result_data.items() if k not in ignored]
    
    row_values.extend(others)
    
    ws_res.append_row(row_values)
    logger.info(f"Logged result for: {result_data.get('Nombre Producto', 'Unknown')}")

def main():
    logger.info("Fetching and filtering products...")
    
    try:
        filtered_items = get_filtered_products()
        
        logger.info(f"Found {len(filtered_items)} products with 'SI' in Column E:")
        for idx, row in filtered_items:
            logger.info(f"Row {idx}: {row}")
            
            # Test update on only the first one if run directly?
            # update_product_status(idx, "SI", "TEST_PENDING")
            
    except Exception as e:
        logger.error(f"Error: {e}")


def get_approved_products_for_ads() -> List[Dict[str, Any]]:
    """
    Scans 'Resultados_Estudio' for products that:
    1. Have 'Agentes Ads Gen' != 'SI' (not yet processed).
    2. Have 'Aprobado Calculado' == 'SI' OR 'Aprobado Manual' == 'SI'.
    
    Returns a list of dicts with full product details fetched from 'Info_Productos'.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    
    # 1. Read Results Sheet
    try:
        ws_res = spreadsheet.worksheet(WORKSHEET_RESULTS)
    except gspread.exceptions.WorksheetNotFound:
        logger.error("'Resultados_Estudio' worksheet not found.")
        return []

    # Get all values to leverage headers
    rows = ws_res.get_all_values()
    if not rows:
        return []
        
    headers = [h.strip() for h in rows[0]]
    
    # Identify column indices (0-based)
    try:
        idx_name = headers.index("Nombre Producto")
        # 'Aprobado Calculado' usually matches 'Aprobado (>9/12)' or similar, let's be flexible or exact based on user prompt.
        # User said: "Aprobado Calculado" and "Aprobado Manual".
        # Let's inspect headers if possible, but assuming standard names from prompt.
        # Actually, my previous code writes "Aprobado (9/12)".
        # User prompt implies "Aprobado Calculado" might be that column or a new one?
        # "Aprobado Calculado, Aprobado Manual, Agentes Ads Gen" are the last 3 columns mentioned.
        # Let's look for them by name.
        
        # Check if 'Agentes Ads Gen' exists, if not, maybe we need to rely on position?
        # User said: "en la primera fila estan las columnas: ... Aprobado Calculado, Aprobado Manual, Agentes Ads Gen"
        # Since I am not generating those last specific columns in `log_study_result` yet (I generate "Aprobado (9/12)"), 
        # I assume they might be added manually or by another process, OR I should look for loosely matching names.
        # "Aprobado (9/12)" corresponds to "Aprobado Calculado" likely.
        
        idx_calc = -1
        for i, h in enumerate(headers):
            if "Aprobado" in h and "Manual" not in h and "Agentes" not in h:
                idx_calc = i
                break
        
        try:
            idx_manual = headers.index("Aprobado Manual")
        except ValueError:
            idx_manual = -1
            
        try:
            idx_agents = headers.index("Agentes Ads Gen")
        except ValueError:
            # If not found, maybe valid to assume NOT PROCESSED?
            # Or assume it is the last column?
            # Let's be safe: if not found, we can't filter by it, or we assume empty.
            idx_agents = -1

    except ValueError as e:
        logger.error(f"Key column missing in Resultados_Estudio: {e}")
        return []

    candidates = []
    
    # Iterate rows (skipping header)
    for i, row in enumerate(rows[1:]):
        row_idx = i + 2 # 1-based index (Header is 1, so first data row is 2)
        
        # Safe get value
        val_name = row[idx_name] if len(row) > idx_name else ""
        if not val_name: continue
        
        val_calc = (row[idx_calc] if idx_calc != -1 and len(row) > idx_calc else "").strip().upper()
        val_manual = (row[idx_manual] if idx_manual != -1 and len(row) > idx_manual else "").strip().upper()
        val_agents = (row[idx_agents] if idx_agents != -1 and len(row) > idx_agents else "").strip().upper()
        
        # LOGIC: 
        # IF Agentes Ads Gen == "SI" -> SKIP
        if val_agents == "SI":
            continue
            
        # ELSE IF (Calc == SI OR Manual == SI) -> PROCESS
        is_approved = (val_calc == "SI") or (val_manual == "SI")
        
        if is_approved:
            logger.info(f"Found Candidate: {val_name} (Row {row_idx})")
            
            # Now fetch details from Info_Productos
            details = get_product_details_by_name(val_name, spreadsheet)
            if details:
                details['results_row_idx'] = row_idx
                # We need column index for 'Agentes Ads Gen' to update it later. 
                # If it doesn't exist in sheet, we might need to append it? 
                # For now, assumes it exists or we append to row info?
                # User instructions imply columns verify existence.
                candidates.append(details)
            else:
                logger.warning(f"Details not found in Info_Productos for '{val_name}'")

    return candidates

def get_product_details_by_name(product_name: str, spreadsheet: gspread.Spreadsheet) -> Dict[str, Any]:
    """
    Searches for 'product_name' in 'Info_Productos' (Column C) and returns dict with:
    Nombre, Precio, Descripcion, Garantia.
    """
    ws_info = spreadsheet.worksheet(WORKSHEET_INFO)
    
    # Search in Column C (Index 3). Get all values of Col C?
    # Getting all is faster than find() if many rows? Or find is okay.
    try:
        cell = ws_info.find(product_name, in_column=3) # Column C is 3
    except gspread.exceptions.CellNotFound:
        return {}
        
    if not cell:
        return {}
        
    row_values = ws_info.row_values(cell.row)
    
    # Columns map (0-based from row_values):
    # A(0): Campana, B(1): ID, C(2): Nombre, D(3): Precio, E(4): Test ...
    # H(7): Desc, I(8): Garantia ...
    
    # Make sure list is long enough
    def get_col(idx):
        return row_values[idx] if len(row_values) > idx else ""
        
    return {
        "nombre_producto": get_col(2),
        "precio": get_col(3),
        "descripcion": get_col(7), # H
        "garantia": get_col(8)     # I
    }

def mark_ads_gen_completed(results_row_idx: int):
    """
    Updates 'Agentes Ads Gen' column to 'SI' for the specified row in 'Resultados_Estudio'.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    ws_res = spreadsheet.worksheet(WORKSHEET_RESULTS)
    
    # Find column index for "Agentes Ads Gen"
    headers = ws_res.row_values(1)
    try:
        col_idx = headers.index("Agentes Ads Gen") + 1 # 1-based for gspread update_cell
    except ValueError:
        # If not found, assume it is the column after "Aprobado Manual" or simply append header?
        # Let's try to find "Aprobado Manual" and go +1? Or just append to row?
        # Safer: Find it or fail gracefully (or add it).
        # User implies it exists. If not, let's append it to header if we are the first run?
        # Doing dynamic header update is risky.
        logger.error("Could not find column 'Agentes Ads Gen' to update.")
        return

    ws_res.update_cell(results_row_idx, col_idx, "SI")
    logger.info(f"Marked row {results_row_idx} as Completed (Agentes Ads Gen = SI)")

def get_products_ready_for_landing() -> List[Dict[str, Any]]:
    """
    Scans 'Resultados_Estudio' for products that:
    1. Have 'Agentes Ads Gen' == 'SI' (Ads generation completed).
    2. Have 'Landing Auto Gen' != 'SI' (Landing not yet generated).
    
    Returns a list of dicts with full product details fetched from 'Info_Productos'.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    
    try:
        ws_res = spreadsheet.worksheet(WORKSHEET_RESULTS)
    except gspread.exceptions.WorksheetNotFound:
        logger.error("'Resultados_Estudio' worksheet not found.")
        return []

    rows = ws_res.get_all_values()
    if not rows:
        return []
        
    headers = [h.strip() for h in rows[0]]
    
    try:
        idx_name = headers.index("Nombre Producto")
        
        # Check for 'Agentes Ads Gen'
        try:
            idx_ads_gen = headers.index("Agentes Ads Gen")
        except ValueError:
            # If not found, we can't process
            logger.warning("'Agentes Ads Gen' column not found. Cannot filter ready products.")
            return []

        # Check for 'Landing Auto Gen' - This might be new, so index lookup is key
        # User said "Columna R" (index 17 if 0-based), but safer to look for header first
        # If header doesn't exist, we might check Column R directly if we trust the user?
        # Let's try to find header "Landing Auto Gen" or "Landing Gen".
        # If not, we will assume it is the column AFTER Agentes Ads Gen or check Col R (Index 17).
        
        idx_landing_gen = -1
        possible_headers = ["Landing Auto Gen", "Landing Gen", "Estado Landing"]
        for ph in possible_headers:
            if ph in headers:
                idx_landing_gen = headers.index(ph)
                break
        
        if idx_landing_gen == -1:
            # Fallback: User said Columna R. A=0... R=17.
            # Let's verify if we have enough columns.
            if len(headers) > 17:
                # Just a heuristic warning
                logger.warning("Header 'Landing Auto Gen' not found. Checking Column R (Index 17).")
                idx_landing_gen = 17
            else:
                 # If sheet is smaller, maybe we assume 18th column?
                 idx_landing_gen = 17
    
    except ValueError as e:
        logger.error(f"Key column missing: {e}")
        return []

    candidates = []
    
    for i, row in enumerate(rows[1:]):
        row_idx = i + 2
        
        val_name = row[idx_name] if len(row) > idx_name else ""
        if not val_name: continue
        
        val_ads = (row[idx_ads_gen] if len(row) > idx_ads_gen else "").strip().upper()
        val_landing = (row[idx_landing_gen] if len(row) > idx_landing_gen else "").strip().upper()
        
        # CONDITION: Ads Gen == SI  AND Landing Gen != SI
        if val_ads == "SI" and val_landing != "SI":
            logger.info(f"Found Landing Candidate: {val_name} (Row {row_idx})")
            
            details = get_product_details_by_name(val_name, spreadsheet)
            if details:
                details['results_row_idx'] = row_idx
                # Pass the column index so we know where to write later if needed (though we recap it in mark func)
                candidates.append(details)
            else:
                logger.warning(f"Details not found in Info_Productos for '{val_name}'")

    return candidates

def mark_landing_gen_completed(results_row_idx: int):
    """
    Updates 'Landing Auto Gen' column (Col R) to 'SI'.
    """
    client = get_google_sheet_client()
    spreadsheet = client.open_by_url(SHEET_URL)
    ws_res = spreadsheet.worksheet(WORKSHEET_RESULTS)
    
    # Try to find header "Landing Auto Gen"
    headers = ws_res.row_values(1)
    col_idx = -1
    
    possible_headers = ["Landing Auto Gen", "Landing Gen"]
    for ph in possible_headers:
        if ph in headers:
            col_idx = headers.index(ph) + 1
            break
            
    if col_idx == -1:
        # Fallback to Column R (18)
        col_idx = 18
        # Validate if header exists at 18? If not, maybe we should write it?
        if len(headers) < 18:
            # Write header if missing?
            # ws_res.update_cell(1, 18, "Landing Auto Gen")
            pass

    ws_res.update_cell(results_row_idx, col_idx, "SI")
    logger.info(f"Marked row {results_row_idx} as Landing Gen Completed (Col {col_idx} = SI)")

if __name__ == "__main__":
    main()
