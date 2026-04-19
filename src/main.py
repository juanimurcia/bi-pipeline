import os
from src.bronze.ingestion import descargar_datos, aplicar_logica_carga, obtener_ruta_supabase
from src.utils.storage_connector import upload_to_lakehouse

def main():
    try:
        # 1. Obtener tipo de carga (GLOBAL o INCREMENTAL)
        tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
        
        # 2. Descargar datos de Kaggle
        df_raw = descargar_datos()
        
        # 3. Aplicar lógica (Mover fechas si es Incremental, o dejar todo si es Global)
        df_final = aplicar_logica_carga(df_raw, tipo_carga)
        
        if tipo_carga == "GLOBAL":
            print("--- Iniciando Particionamiento de Datos Históricos ---")
            # Agrupamos por Año y Mes para crear las carpetas correspondientes
            # Usamos dt.year y dt.month sobre la columna de fecha de orden
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            
            for (year, month), df_grupo in grupos:
                # Generamos la ruta dinámica para cada grupo
                # month:02d asegura que enero sea '01' y no '1'
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                
                temp_file = f"temp_{year}_{month}.parquet"
                df_grupo.to_parquet(temp_file, index=False)
                
                # Subimos cada "rebanada" a su propia carpeta
                upload_to_lakehouse(temp_file, remote_path)
                
                # Borramos el temporal para no llenar el disco del servidor
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            print("✅ Carga Global particionada con éxito.")

        else:
            # Lógica para Carga INCREMENTAL (un solo archivo en la carpeta de hoy)
            remote_path = obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            df_final.to_parquet(temp_file, index=False)
            
            upload_to_lakehouse(temp_file, remote_path)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            print(f"✅ Carga Incremental finalizada en: {remote_path}")

    except Exception as e:
        print(f"❌ Error crítico en el pipeline: {e}")
        raise e

if __name__ == "__main__":
    main()

