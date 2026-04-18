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
    """Sube archivos al bucket de Supabase manejando sobreescritura."""
    supabase = get_storage_client()
    
    try:
        with open(local_path, 'rb') as f:
            # Intentamos subir con upsert=True
            # Si el archivo ya existe, lo reemplaza. Si no, lo crea.
            supabase.storage.from_(bucket).upload(
                path=remote_path,
                file=f,
                file_options={"upsert": "true"}
            )
        print(f"✅ Archivo '{remote_path}' procesado con éxito.")
    except Exception as e:
        # Si el error es que ya existe o falta permiso, intentamos un 'update' explícito
        if "already exists" in str(e).lower():
            with open(local_path, 'rb') as f:
                supabase.storage.from_(bucket).update(
                    path=remote_path,
                    file=f
                )
            print(f"✅ Archivo '{remote_path}' actualizado con éxito.")
        else:
            print(f"❌ Error real de Storage: {e}")
            raise e
