import os
from src.utils.storage_connector import upload_to_lakehouse

def main():
    print("--- Iniciando Ingesta: Lakehouse Medallion (Capa Bronze) ---")
    
    test_file = "test_ingesta.txt"
    try:
        # 1. Crear dato de prueba
        with open(test_file, "w", encoding="utf-8") as f:
            f.write("Prueba de pipeline: Conexión Supabase exitosa.")
        
        # 2. Subir al Storage (Bucket: Lakehouse, Carpeta: bronze)
        upload_to_lakehouse(test_file, "bronze/test_conexion.txt")
        
        print("🚀 ¡Pipeline completado! Revisa el bucket 'Lakehouse' en Supabase.")

    except Exception as e:
        print(f"❌ El proceso falló: {e}")
        raise e
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)

if __name__ == "__main__":
    main()
