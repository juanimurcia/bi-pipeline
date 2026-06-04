import os
import sys
from datetime import datetime

# Importaciones de módulos internos del proyecto
from src.bronze.ingestion import DataIngestor  # <- CAMBIO: Importamos la clase en vez de las funciones
from src.utils.storage_connector import upload_to_lakehouse
from src.silver.transformation import transformar_a_silver
from src.utils.db_connector import cargar_a_sql, registrar_auditoria, get_db_engine

def main():
    """
    Orquestador principal del Pipeline de Datos (Arquitectura Medallion).
    
    Este script coordina el flujo de datos a través de dos capas:
    1. Capa Bronze: Ingesta de datos crudos desde Kaggle a Supabase Storage (Parquet).
    2. Capa Silver: Transformación, limpieza y carga en Supabase SQL (PostgreSQL).
    
    El flujo se adapta según la variable de entorno 'TIPO_CARGA' (GLOBAL o INCREMENTAL).
    """
    start_time = datetime.now()

    # Inicializamos el engine fuera del try para que esté disponible en el except
    engine = get_db_engine()
    tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
    
    try:
        # ---------------------------------------------------------
        # 1. CONFIGURACIÓN DE ENTORNO
        # ---------------------------------------------------------
        print(f"{'='*60}")
        print(f"🚀 EJECUCIÓN DEL PIPELINE - MODO: {tipo_carga}")
        print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        # Inicializamos nuestro componente de Ingesta Orientado a Objetos
        ingestor = DataIngestor()  # <- CAMBIO: Instanciación de la clase

        # ---------------------------------------------------------
        # 2. INGESTA Y PREPARACIÓN (BRONZE)
        # ---------------------------------------------------------
        print("Step 1: Descargando datos desde Kaggle API...")
        df_raw = ingestor.descargar_datos()  # <- CAMBIO: Llamada al método del objeto
        
        print("Step 2: Aplicando lógica de carga y simulación de fechas...")
        # En GLOBAL usa datos históricos; en INCREMENTAL simula datos del día actual
        df_final = ingestor.aplicar_logica_carga(df_raw, tipo_carga)  # <- CAMBIO: Llamada al método del objeto
        
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
            remote_path = ingestor.obtener_ruta_supabase(df_final, tipo_carga)  # <- CAMBIO: Llamada al método del objeto
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
        tablas_silver = transformar_a_silver(df_final)

        # Lógica de IDs Únicos con Tiempo Congelado (yyMMdd + Segundos + ID)
        if tipo_carga == 'INCREMENTAL':
            print("⚠️ Ajustando IDs para carga incremental (Timestamp Congelado)...")
            
            # 1. Congelamos el sello de tiempo para todo el lote actual
            now_incremental = datetime.now()
            fecha_prefijo = int(now_incremental.strftime("%y%m%d"))
            segundos_dia = (now_incremental.hour * 3600) + (now_incremental.minute * 60) + now_incremental.second
            factor = 10000000 
            
            # Prefijo único para esta ejecución (ej: 2604210086400)
            prefijo_ejecucion = (fecha_prefijo * factor) + segundos_dia
            
            # Filtramos para procesar solo tablas de hechos en incremental
            tablas_silver = {k: v for k, v in tablas_silver.items() if k.startswith('fact_')}
            
            if 'fact_order' in tablas_silver:
                # Sumamos el prefijo idéntico a todas las filas de la tabla de órdenes
                tablas_silver['fact_order']['order_id'] = prefijo_ejecucion + tablas_silver['fact_order']['order_id']
            
            if 'fact_item_order' in tablas_silver:
                # IMPORTANTE: Usamos el MISMO prefijo_ejecucion para que el order_id coincida
                tablas_silver['fact_item_order']['order_id'] = prefijo_ejecucion + tablas_silver['fact_item_order']['order_id']
                # También lo aplicamos al ID del ítem para asegurar su unicidad
                tablas_silver['fact_item_order']['order_item_id'] = prefijo_ejecucion + tablas_silver['fact_item_order']['order_item_id']

        print("Step 5: Persistiendo en Base de Datos PostgreSQL (Supabase)...")
        cargar_a_sql(tablas_silver, tipo_carga)
        
        # ---------------------------------------------------------
        # 5. FINALIZACIÓN Y AUDITORÍA EXITOSA
        # ---------------------------------------------------------
        filas_cargadas = len(tablas_silver.get('fact_order', []))
        registrar_auditoria(engine, tipo_carga, 'SUCCESS', filas=filas_cargadas)
        
        end_time = datetime.now()
        duracion = end_time - start_time
        print(f"\n{'='*60}\n🏁 PIPELINE FINALIZADO CON ÉXITO\n{'='*60}")

    except Exception as e:
        # AUDITORÍA DE FALLO
        try:
            registrar_auditoria(engine, tipo_carga, 'FAILED', error=str(e))
        except:
            print("⚠️ No se pudo registrar el fallo en la tabla de auditoría.")
            
        print(f"\n❌ ERROR CRÍTICO: {str(e)}")
        sys.exit(1) # Esto garantiza el ROJO en GitHub

if __name__ == "__main__":
    main()

