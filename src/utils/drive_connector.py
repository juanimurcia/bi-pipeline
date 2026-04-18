import json
import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

def get_drive_connection():
    """Establece conexión con Google Drive usando la Service Account."""
    scope = ['https://www.googleapis.com/auth/drive']
    
    # Obtener el secreto desde la variable de entorno de GitHub
    service_account_info = json.loads(os.environ.get('GDRIVE_SERVICE_ACCOUNT'))
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
    
    gauth = GoogleAuth()
    gauth.credentials = creds
    return GoogleDrive(gauth)

def upload_to_drive(local_file_path, drive_file_name, folder_id):
    """Sube un archivo a una carpeta específica de Google Drive."""
    drive = get_drive_connection()
    
    file_metadata = {
        'title': drive_file_name,
        'parents': [{'id': folder_id}]
    }
    
    file_drive = drive.CreateFile(file_metadata)
    file_drive.SetContentFile(local_file_path)
    file_drive.Upload()
    print(f"✅ Archivo '{drive_file_name}' subido correctamente a Drive.")
