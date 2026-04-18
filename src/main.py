import os
# Importamos el ID desde el archivo de configuración
from src.config import DRIVE_FOLDER_ID
from src.utils.drive_connector import upload_to_drive

def main():
    print("--- Probando Conector de Lakehouse (Bronce) ---")
    
    # 1. Creamos un archivo de texto simple para probar la subida
    test_file_name = "prueba_conexion.txt"
    try:
        with open(test_file_name, "w", encoding="utf-8") as f:
            f.write("Hola! Esta es una prueba utilizando el archivo de configuracion.")
        
        # 2. Usamos DRIVE_FOLDER_ID que viene de src/config.py
        # IMPORTANTE: upload_to_drive ya tiene su propio try/except y raise
        upload_to_drive(test_file_name, "conexion_con_config.txt", DRIVE_FOLDER_ID)
        
        print("🚀 Prueba completada con éxito. Revisa tu carpeta de Drive!")

    except Exception as e:
        # Imprimimos el error pero NO lo silenciamos
        print(f"❌ El Pipeline falló en la etapa de prueba: {e}")
        # Al relanzar el error (raise), GitHub Actions marcará la ejecución como FALLIDA
        raise e
        
    finally:
        # El bloque finally se asegura de borrar el archivo temporal 
        # sin importar si la subida funcionó o no.
        if os.path.exists(test_file_name):
            os.remove(test_file_name)
            print(f"--- Limpieza: Archivo temporal {test_file_name} eliminado ---")

if __name__ == "__main__":
    main()
