import os
import sys
from datetime import datetime

# Importaciones de clases orientadas a objetos de nuestro proyecto
from src.bronze.ingestion import DataIngestor
from src.utils.storage_connector import StorageConnector
from src.silver.transformation import SilverTransformer  # <- CAMBIO: Traemos la nueva Clase
from src.utils.db_connector import DbConnector

class PipelineOrchestrator:
    def __init__(self):
        """
        Orquestador principal del Data Pipeline (Arquitectura Medallion).
        Sigue un enfoque Orientado a Objetos encapsulando las variables de entorno
        y los subsistemas del lakehouse como estado interno.
        """
        self.tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
        
        # Composición de componentes: Instanciamos todas nuestras clases de infraestructura
        self.db_connector = DbConnector()
        self.storage_connector = StorageConnector()
        self.ingestor = DataIngestor()
        self.transformer = SilverTransformer()  # <- Agregado al flujo OO

    def run(self):
        """Ejecuta el ciclo de vida completo del pipeline (Extract, Transform, Load)."""
        start_time = datetime.now()
        
        try:
            # ---------------------------------------------------------
            # 1. CONFIGURACIÓN DE ENTORNO
            # ---------------------------------------------------------
            print(f"{'='*60}")
            print(f"🚀 EJECUCIÓN DEL PIPELINE - MODO: {self.tipo_carga}")
            print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}\n")

            # ---------------------------------------------------------
            # 2. INGESTA Y PREPARACIÓN (BRONZE)
            # ---------------------------------------------------------
            print("Step 1: Descargando datos desde Kaggle API...")
            df_raw = self.ingestor.descargar_datos()
            
            print("Step 2: Aplicando lógica de carga y simulación de fechas...")
            df_final = self.ingestor.aplicar_logica_carga(df_raw, self.tipo_carga)
            
            # ---------------------------------------------------------
            # 3. PERSISTENCIA EN STORAGE (CAPA BRONZE)
            # ---------------------------------------------------------
            if self.tipo_carga == "GLOBAL":
                print("Step 3 [GLOBAL]: Iniciando particionamiento histórico...")
                grupos = df_final.groupby([
                    df_final['order date (DateOrders)'].dt.year, 
                    df_final['order date (DateOrders)'].dt.month
                ])
                
                for (year, month), df_grupo in grupos:
                    remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                    temp_file = f"temp_{year}_{month}.parquet"
                    
                    df_grupo.to_parquet(temp_file, index=False)
                    self.storage_connector.upload_to_lakehouse(temp_file, remote_path)
                    
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
            else:
                print("Step 3 [INCREMENTAL]: Generando archivo diario...")
                remote_path = self.ingestor.obtener_ruta_supabase(df_final, self.tipo_carga)
                temp_file = "temp_incremental.parquet"
                
                df_final.to_parquet(temp_file, index=False)
                self.storage_connector.upload_to_lakehouse(temp_file, remote_path)
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            print("✅ CAPA BRONZE: Datos persistidos en Storage exitosamente.")

            # ---------------------------------------------------------
            # 4. PROCESAMIENTO Y CARGA SQL (CAPA SILVER)
            # ---------------------------------------------------------
            print("\nStep 4: Iniciando Transformación (Esquema Snowflake)...")
            # Invocamos el método del objeto transformador
            tablas_silver = self.transformer.transformar_a_silver(df_final)

            # Lógica de IDs Únicos con Tiempo Congelado (yyMMdd + Segundos + ID)
            if self.tipo_carga == 'INCREMENTAL':
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
            self.db_connector.cargar_a_sql(tablas_silver, self.tipo_carga)
            
            # ---------------------------------------------------------
            # 5. FINALIZACIÓN Y AUDITORÍA EXITOSA
            # ---------------------------------------------------------
            filas_cargadas = len(tablas_silver.get('fact_order', []))
            self.db_connector.registrar_auditoria(self.tipo_carga, 'SUCCESS', filas=filas_cargadas)
            
            end_time = datetime.now()
            print(f"\n{'='*60}\n🏁 PIPELINE FINALIZADO CON ÉXITO\n{'='*60}")

        except Exception as e:
            # AUDITORÍA DE FALLO
            try:
                self.db_connector.registrar_auditoria(self.tipo_carga, 'FAILED', error=str(e))
            except:
                print("⚠️ No se pudo registrar el fallo en la tabla de auditoría.")
                
            print(f"\n❌ ERROR CRÍTICO: {str(e)}")
            sys.exit(1)

if __name__ == "__main__":
    # Instanciamos el objeto orquestador y ejecutamos el pipeline
    orchestrator = PipelineOrchestrator()
    orchestrator.run()

