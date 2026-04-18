import os
# Importamos el ID desde el nuevo archivo de configuración
from src.config import DRIVE_FOLDER_ID
from src.utils.drive_connector import upload_to_drive

def main():
    # Ya no necesitas declarar FOLDER_ID aquí manualmente
    
    print("--- Probando Conector de Lakehouse (Bronce) ---")
    
    # 1. Creamos un archivo de texto simple para probar la subida
    test_file_name = "prueba_conexion.txt"
    with open(test_file_name, "w") as f:
        f.write("Hola! Esta es una prueba utilizando el archivo de configuracion.")
    
    try:
        # 2. Usamos DRIVE_FOLDER_ID que viene de src/config.py
        upload_to_drive(test_file_name, "conexion_con_config.txt", DRIVE_FOLDER_ID)
        print("🚀 Prueba completada con éxito. Revisa tu carpeta de Drive!")
    except Exception as e:
        print(f"❌ Error en la conexión: {e}")
    finally:
        # Limpiamos el archivo temporal
        if os.path.exists(test_file_name):
            os.remove(test_file_name)

if __name__ == "__main__":
    main()
