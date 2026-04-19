import os
from supabase import create_client

def get_storage_client():
    """Establece conexión con Supabase."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("❌ Error: SUPABASE_URL o SUPABASE_KEY no están configuradas.")
        
    return create_client(url, key)

def upload_to_lakehouse(local_path, remote_path, bucket="Lakehouse"):
    """Sube un archivo al Storage de Supabase usando la lógica de sobrescritura."""
    supabase = get_storage_client()
    
    with open(local_path, 'rb') as f:
        try:
            # Usar upsert=true es la forma limpia de manejar archivos existentes
            response = supabase.storage.from_(bucket).upload(
                path=remote_path,
                file=f,
                file_options={"x-upsert": "true"} # Nota: En algunas versiones de la librería es "x-upsert"
            )
            print(f"✅ Archivo procesado correctamente en: {remote_path}")
        except Exception as e:
            # Si el error es simplemente que ya existe, pero queremos asegurar que se suba:
            print(f"⚠️ Nota: Si el archivo ya existía, se intentará actualizar. Error original: {e}")
            with open(local_path, 'rb') as f_retry:
                supabase.storage.from_(bucket).update(
                    path=remote_path,
                    file=f_retry
                )
            print(f"✅ Archivo actualizado: {remote_path}")
