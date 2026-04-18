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
    supabase = get_storage_client()
    
    with open(local_path, 'rb') as f:
        try:
            # Intentamos subir (INSERT)
            supabase.storage.from_(bucket).upload(
                path=remote_path,
                file=f,
                file_options={"upsert": "true"}
            )
            print(f"✅ Archivo subido: {remote_path}")
        except Exception:
            # Si el INSERT falla porque ya existe o por política, intentamos UPDATE
            with open(local_path, 'rb') as f:
                supabase.storage.from_(bucket).update(
                    path=remote_path,
                    file=f
                )
            print(f"✅ Archivo actualizado: {remote_path}")
