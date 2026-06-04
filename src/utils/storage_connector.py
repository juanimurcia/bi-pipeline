import os
from supabase import create_client
from src.config import SUPABASE_BUCKET_NAME

class StorageConnector:
    def __init__(self, bucket_name: str = SUPABASE_BUCKET_NAME):
        """
        Conector encargado de la comunicación con la capa de almacenamiento (Lakehouse Storage).
        Mantiene el cliente y el bucket asignado como estado de la instancia.
        """
        self.bucket_name = bucket_name
        # Inicializamos la conexión una sola vez al instanciar la clase
        self.client = self._get_storage_client()

    def _get_storage_client(self):
        """Establece conexión con Supabase (Método Interno Privado)."""
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("❌ Error: SUPABASE_URL o SUPABASE_KEY no están configuradas.")
            
        return create_client(url, key)

    def upload_to_lakehouse(self, local_path: str, remote_path: str):
        """Sube un archivo al Storage de Supabase usando la lógica de sobrescritura."""
        with open(local_path, 'rb') as f:
            try:
                # Usamos self.client y self.bucket_name de la instancia
                self.client.storage.from_(self.bucket_name).upload(
                    path=remote_path,
                    file=f,
                    file_options={"x-upsert": "true"}
                )
                print(f"✅ Archivo procesado correctamente en: {remote_path}")
            except Exception as e:
                print(f"⚠️ Nota: Si el archivo ya existía, se intentará actualizar. Error original: {e}")
                with open(local_path, 'rb') as f_retry:
                    self.client.storage.from_(self.bucket_name).update(
                        path=remote_path,
                        file=f_retry
                    )
                print(f"✅ Archivo actualizado: {remote_path}")
