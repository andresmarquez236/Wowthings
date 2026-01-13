import os
import re
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from utils.logger import setup_logger
logger = setup_logger("DriveUploader")

# ===== OAuth constants =====
OAUTH_CLIENT_FILE = "client_secret_798183657592-6k8hnbfhvjid0mqiugekdptaturvs488.apps.googleusercontent.com.json"   # <-- Descargado del OAuth Client ID (Desktop)
TOKEN_FILE = "token.json"               # <-- Se crea automáticamente
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
# drive.file: permite crear/editar archivos que la app sube (suficiente y más seguro)
# Si necesitas ver/editar TODO tu Drive, usa: ["https://www.googleapis.com/auth/drive"]

def get_drive_service_oauth():
    if not os.path.exists(OAUTH_CLIENT_FILE):
        raise FileNotFoundError(
            f"❌ No encuentro {OAUTH_CLIENT_FILE}. Descarga el OAuth client JSON (Desktop) y nómbralo 'credentials.json'."
        )

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Abre navegador para login
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            # Si estás en servidor sin navegador, usa:
            # creds = flow.run_console()

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)

def sanitize_drive_query_value(value: str) -> str:
    # Drive query usa comillas simples. Debemos escapar comillas simples en nombres.
    return value.replace("'", r"\'")

def ensure_drive_folder(service, folder_name: str, parent_id: str) -> str:
    folder_name_q = sanitize_drive_query_value(folder_name)
    q = (
        f"'{parent_id}' in parents and "
        f"name = '{folder_name_q}' and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )

    results = service.files().list(
        q=q,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    files = results.get("files", [])
    if files:
        return files[0]["id"]

    meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = service.files().create(
        body=meta,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return folder["id"]

def file_exists(service, parent_id: str, file_name: str) -> bool:
    file_name_q = sanitize_drive_query_value(file_name)
    q = f"'{parent_id}' in parents and name = '{file_name_q}' and trashed = false"

    res = service.files().list(
        q=q,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    return bool(res.get("files"))

def upload_file(service, local_path: str, parent_id: str):
    file_name = os.path.basename(local_path)

    if file_exists(service, parent_id, file_name):
        logger.info(f"Skipping {file_name} (Already exists)")
        return

    logger.info(f"Uploading: {file_name} ...")

    meta = {"name": file_name, "parents": [parent_id]}
    media = MediaFileUpload(local_path, resumable=True)

    try:
        service.files().create(
            body=meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()
        logger.info(f"Successfully uploaded: {file_name}")
    except Exception as e:
        logger.error(f"Failed to upload {file_name}: {e}")

def upload_folder_recursive(service, local_folder: str, parent_id: str):
    folder_name = os.path.basename(local_folder.rstrip("/\\"))
    logger.info(f"Processing folder: {folder_name} (Parent ID: {parent_id})")

    drive_folder_id = ensure_drive_folder(service, folder_name, parent_id)
    logger.info(f"Drive folder ID: {drive_folder_id}")

    for item in os.listdir(local_folder):
        item_path = os.path.join(local_folder, item)

        if os.path.isfile(item_path):
            upload_file(service, item_path, drive_folder_id)

        elif os.path.isdir(item_path):
            upload_folder_recursive(service, item_path, drive_folder_id)

def main():
    # ===== Config =====
    FOLDER_TO_UPLOAD = "output/estufa_camping"

    # PARENT_DRIVE_ID puede ser:
    # - ID de una carpeta en "Mi unidad"
    # - ID de carpeta dentro de Shared Drive (si quieres)
    # Si lo pones en None, sube creando carpeta en raíz de "Mi unidad"
    PARENT_DRIVE_ID = "1U4lWIeqyKojgG-KDFwataZtiNkhxqNTe"
    # Si quieres subir a "Mi unidad" raíz, usa: PARENT_DRIVE_ID = "root"

    if not os.path.exists(FOLDER_TO_UPLOAD):
        logger.error(f"Local folder does not exist: {FOLDER_TO_UPLOAD}")
        return

    logger.info("Authenticating with OAuth (your user)...")
    service = get_drive_service_oauth()

    logger.info(f"Starting Recursive Upload of '{FOLDER_TO_UPLOAD}' to Drive parent: {PARENT_DRIVE_ID}...")
    try:
        upload_folder_recursive(service, FOLDER_TO_UPLOAD, PARENT_DRIVE_ID)
        logger.info("Upload Completed Successfully!")
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")

def upload_product_to_drive(local_folder: str, parent_id: str = "1U4lWIeqyKojgG-KDFwataZtiNkhxqNTe") -> bool:
    """
    Wrapper to upload a folder to Drive using OAuth.
    Default parent_id must be valid (Shared Drive or folder ID).
    """
    if not os.path.exists(local_folder):
        logger.error(f"Upload failed: Local folder not found: {local_folder}")
        return

    logger.info(f"Authenticating with OAuth (uploading {os.path.basename(local_folder)})...")
    try:
        service = get_drive_service_oauth()
        logger.info(f"Starting Upload to Drive Parent: {parent_id}...")
        upload_folder_recursive(service, local_folder, parent_id)
        logger.info("Upload Completed Successfully!")
        return True
    except Exception as e:
        logger.error(f"Upload Failed: {e}")
        return False

if __name__ == "__main__":
    main()
