import os
from supabase import create_client

def get_storage_client():
    """Establece conexión con Supabase Storage."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("❌ Error: SUPABASE_URL o SUPABASE_KEY no están configuradas.")
        
    return create_client(url, key)

def upload_to_lakehouse(local_path, remote_path, bucket="Lakehouse"):
    """Sube un archivo al bucket de Supabase (Capa Bronze)."""
    supabase = get_storage_client()
    
    try:
        with open(local_path, 'rb') as f:
            # upsert=true permite sobreescribir el archivo si ya existe
            supabase.storage.from_(bucket).upload(
                path=remote_path,
                file=f,
                file_options={"upsert": "true"}
            )
        print(f"✅ Archivo '{remote_path}' subido con éxito al Lakehouse.")
    except Exception as e:
        print(f"❌ Error al subir a Supabase: {e}")
        raise e
