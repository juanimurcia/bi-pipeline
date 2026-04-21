import os
import sys
from datetime import datetime

# Importaciones de módulos internos del proyecto
from src.bronze.ingestion import descargar_datos, aplicar_logica_carga, obtener_ruta_supabase
from src.utils.storage_connector import upload_to_lakehouse
from src.silver.transformation import transformar_a_silver
from src.utils.db_connector import cargar_a_sql

def main():
    """
    Orquestador principal del Pipeline de Datos (Arquitectura Medallion).
    
    Este script coordina el flujo de datos a través de dos capas:
    1. Capa Bronze: Ingesta de datos crudos desde Kaggle a Supabase Storage (Parquet).
    2. Capa Silver: Transformación, limpieza y carga en Supabase SQL (PostgreSQL).
    
    El flujo se adapta según la variable de entorno 'TIPO_CARGA' (GLOBAL o INCREMENTAL).
    """
    start_time = datetime.now()
    
    try:
        # ---------------------------------------------------------
        # 1. CONFIGURACIÓN DE ENTORNO
        # ---------------------------------------------------------
        # Captura la variable definida en GitHub Actions o usa INCREMENTAL por defecto
        tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
        print(f"{'='*60}")
        print(f"🚀 EJECUCIÓN DEL PIPELINE - MODO: {tipo_carga}")
        print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        # ---------------------------------------------------------
        # 2. INGESTA Y PREPARACIÓN (BRONZE)
        # ---------------------------------------------------------
        print("Step 1: Descargando datos desde Kaggle API...")
        df_raw = descargar_datos()
        
        print("Step 2: Aplicando lógica de carga y simulación de fechas...")
        # En GLOBAL usa datos históricos; en INCREMENTAL simula datos del día actual
        df_final = aplicar_logica_carga(df_raw, tipo_carga)
        
        # ---------------------------------------------------------
        # 3. PERSISTENCIA EN STORAGE (CAPA BRONZE)
        # ---------------------------------------------------------
        if tipo_carga == "GLOBAL":
            print("Step 3 [GLOBAL]: Iniciando particionamiento histórico...")
            # Particionamos por Año/Mes para optimizar futuras lecturas (Hive Partitioning)
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            
            for (year, month), df_grupo in grupos:
                # Definición de ruta siguiendo el estándar del Lakehouse
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                temp_file = f"temp_{year}_{month}.parquet"
                
                # Persistencia temporal y subida al Storage
                df_grupo.to_parquet(temp_file, index=False)
                upload_to_lakehouse(temp_file, remote_path)
                
                # Limpieza de archivos temporales en el runner de GitHub
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            print("Step 3 [INCREMENTAL]: Generando archivo diario...")
            remote_path = obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            
            df_final.to_parquet(temp_file, index=False)
            upload_to_lakehouse(temp_file, remote_path)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        print("✅ CAPA BRONZE: Datos persistidos en Storage exitosamente.")

        # ---------------------------------------------------------
        # 4. PROCESAMIENTO Y CARGA SQL (CAPA SILVER)
        # ---------------------------------------------------------
        print("\nStep 4: Iniciando Transformación (Esquema Snowflake)...")
        # El módulo de transformación limpia nulos, outliers y normaliza textos
        tablas_silver = transformar_a_silver(df_final)
        
        print("Step 5: Persistiendo en Base de Datos PostgreSQL (Supabase)...")
        # cargar_a_sql decide automáticamente entre 'replace' (Global) o 'append' (Incremental)
        cargar_a_sql(tablas_silver, tipo_carga)
        
        # ---------------------------------------------------------
        # 5. FINALIZACIÓN Y MÉTRICAS
        # ---------------------------------------------------------
        end_time = datetime.now()
        duracion = end_time - start_time
        print(f"\n{'='*60}")
        print(f"🏁 PIPELINE FINALIZADO CON ÉXITO")
        print(f"⏱️ Tiempo total de ejecución: {duracion}")
        print(f"{'='*60}")

    except Exception as e:
        # Registro detallado de errores para facilitar el debugging en los logs de GitHub Actions
        print(f"\n❌ ERROR CRÍTICO DETECTADO:")
        print(f"Detalle: {str(e)}")
        print(f"{'='*60}")
        sys.exit(1) # Finaliza con error para alertar a GitHub Actions

if __name__ == "__main__":
    main()

