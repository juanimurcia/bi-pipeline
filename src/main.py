import os
import sys
from datetime import datetime

# Importaciones de clases orientadas a objetos
from src.bronze.ingestion import DataIngestor
from src.utils.storage_connector import StorageConnector  # <- CAMBIO: Traemos la Clase
from src.silver.transformation import transformar_a_silver
from src.utils.db_connector import DbConnector  # <- CAMBIO: Traemos la Clase

def main():
    """
    Orquestador principal del Pipeline de Datos (Arquitectura Medallion).
    
    Este script coordina el flujo de datos a través de dos capas:
    1. Capa Bronze: Ingesta de datos crudos desde Kaggle a Supabase Storage (Parquet).
    2. Capa Silver: Transformación, limpieza y carga en Supabase SQL (PostgreSQL).
    
    El flujo se adapta según la variable de entorno 'TIPO_CARGA' (GLOBAL o INCREMENTAL).
    """
    start_time = datetime.now()
    tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
    
    # Inicializamos todos nuestros componentes Orientados a Objetos
    db_connector = DbConnector()           # <- CAMBIO: Inicializa engine e infraestructura SQL
    storage_connector = StorageConnector() # <- CAMBIO: Inicializa cliente de Supabase Storage
    ingestor = DataIngestor()              # Mantiene la inicialización previa
    
    try:
        # ---------------------------------------------------------
        # 1. CONFIGURACIÓN DE ENTORNO
        # ---------------------------------------------------------
        print(f"{'='*60}")
        print(f"🚀 EJECUCIÓN DEL PIPELINE - MODO: {tipo_carga}")
        print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        # ---------------------------------------------------------
        # 2. INGESTA Y PREPARACIÓN (BRONZE)
        # ---------------------------------------------------------
        print("Step 1: Descargando datos desde Kaggle API...")
        df_raw = ingestor.descargar_datos()
        
        print("Step 2: Aplicando lógica de carga y simulación de fechas...")
        df_final = ingestor.aplicar_logica_carga(df_raw, tipo_carga)
        
        # ---------------------------------------------------------
        # 3. PERSISTENCIA EN STORAGE (CAPA BRONZE)
        # ---------------------------------------------------------
        if tipo_carga == "GLOBAL":
            print("Step 3 [GLOBAL]: Iniciando particionamiento histórico...")
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            
            for (year, month), df_grupo in grupos:
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                temp_file = f"temp_{year}_{month}.parquet"
                
                df_grupo.to_parquet(temp_file, index=False)
                storage_connector.upload_to_lakehouse(temp_file, remote_path) # <- CAMBIO: Uso del objeto conector
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            print("Step 3 [INCREMENTAL]: Generando archivo diario...")
            remote_path = ingestor.obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            
            df_final.to_parquet(temp_file, index=False)
            storage_connector.upload_to_lakehouse(temp_file, remote_path) # <- CAMBIO: Uso del objeto conector
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        print("✅ CAPA BRONZE: Datos persistidos en Storage exitosamente.")

        # ---------------------------------------------------------
        # 4. PROCESAMIENTO Y CARGA SQL (CAPA SILVER)
        # ---------------------------------------------------------
        print("\nStep 4: Iniciando Transformación (Esquema Snowflake)...")
        tablas_silver = transformar_a_silver(df_final)

        # Lógica de IDs Únicos con Tiempo Congelado (yyMMdd + Segundos + ID)
        if tipo_carga == 'INCREMENTAL':
            print("⚠️ Ajustando IDs para carga incremental (Timestamp Congelado)...")
            
            now_incremental = datetime.now()
            fecha_prefijo = int(now_incremental.strftime("%y%m%d"))
            segundos_dia = (now_incremental.hour * 3600) + (now_incremental.minute * 60) + now_incremental.second
            factor = 10000000 
            
            prefijo_ejecucion = (fecha_prefijo * factor) + segundos_dia
            
            tablas_silver = {k: v for k, v in tablas_silver.items() if k.startswith('fact_')}
            
            if 'fact_order' in tablas_silver:
                tablas_silver['fact_order']['order_id'] = prefijo_ejecucion + tablas_silver['fact_order']['order_id']
            
            if 'fact_item_order' in tablas_silver:
                tablas_silver['fact_item_order']['order_id'] = prefijo_ejecucion + tablas_silver['fact_item_order']['order_id']
                tablas_silver['fact_item_order']['order_item_id'] = prefijo_ejecucion + tablas_silver['fact_item_order']['order_item_id']

        print("Step 5: Persistiendo en Base de Datos PostgreSQL (Supabase)...")
        db_connector.cargar_a_sql(tablas_silver, tipo_carga) # <- CAMBIO: Uso del objeto conector
        
        # ---------------------------------------------------------
        # 5. FINALIZACIÓN Y AUDITORÍA EXITOSA
        # ---------------------------------------------------------
        filas_cargadas = len(tablas_silver.get('fact_order', []))
        db_connector.registrar_auditoria(tipo_carga, 'SUCCESS', filas=filas_cargadas) # <- CAMBIO: Ya no requiere pasar 'engine'
        
        end_time = datetime.now()
        print(f"\n{'='*60}\n🏁 PIPELINE FINALIZADO CON ÉXITO\n{'='*60}")

    except Exception as e:
        # AUDITORÍA DE FALLO
        try:
            db_connector.registrar_auditoria(tipo_carga, 'FAILED', error=str(e)) # <- CAMBIO: Ya no requiere pasar 'engine'
        except:
            print("⚠️ No se pudo registrar el fallo en la tabla de auditoría.")
            
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()

