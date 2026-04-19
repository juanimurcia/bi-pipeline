import os
from src.bronze.ingestion import descargar_datos, aplicar_logica_carga, obtener_ruta_supabase
from src.utils.storage_connector import upload_to_lakehouse

def main():
    try:
        # 1. Obtener tipo de carga desde GitHub
        tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
        
        # 2. Descargar y Transformar
        df_raw = descargar_datos()
        df_final = aplicar_logica_carga(df_raw, tipo_carga)
        
        # 3. Definir ruta y guardar temporalmente
        remote_path = obtener_ruta_supabase(df_final, tipo_carga)
        temp_file = "temp_bronze.parquet"
        df_final.to_parquet(temp_file, index=False)
        
        # 4. Subir a Supabase
        upload_to_lakehouse(temp_file, remote_path)
        
        print(f"✅ Proceso finalizado exitosamente en: {remote_path}")

    except Exception as e:
        print(f"❌ Error crítico en el pipeline: {e}")
        raise e

if __name__ == "__main__":
    main()
