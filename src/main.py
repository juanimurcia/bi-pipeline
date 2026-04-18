import os
from src.utils.drive_connector import upload_to_drive

def main():
    # Este es el ID que encontraste en la URL
    FOLDER_ID = '1UaEweoUHsv8H3ILXCFqde-9Rt2kyGufs'
    
    print("--- Probando Conector de Lakehouse (Bronce) ---")
    
    # 1. Creamos un archivo de texto simple para probar la subida
    test_file_name = "prueba_conexion.txt"
    with open(test_file_name, "w") as f:
        f.write("Hola! Esta es una prueba desde GitHub Actions hacia el Lakehouse.")
    
    try:
        # 2. Intentamos subirlo usando tu función modularizada
        upload_to_drive(test_file_name, "conexion_exitosa.txt", FOLDER_ID)
        print("🚀 Prueba completada con éxito. Revisa tu carpeta de Drive!")
    except Exception as e:
        print(f"❌ Error en la conexión: {e}")
    finally:
        # Limpiamos el archivo temporal del servidor de GitHub
        if os.path.exists(test_file_name):
            os.remove(test_file_name)

if __name__ == "__main__":
    main()
