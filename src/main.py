"""
Módulo Principal: Orquestador del Pipeline de Datos (Medallion Architecture)
----------------------------------------------------------------------------
Este script representa el punto de entrada principal del sistema. Implementa
GRASP 'Controlador / Orquestador' coordinando el flujo secuencial de los datos a través
de las capas Bronze, Silver y Database.

Principios de Diseño Aplicados:
    - Bajo Acoplamiento (Low Coupling): El orquestador no manipula ni lee la lógica interna 
      de negocio de los componentes; interactúa puramente de forma declarativa.
    - Alta Cohesión (High Cohesion): Delega las tareas operativas a sus respectivos subsistemas.
    - Principio de Única Responsabilidad (SRP): Controlar la ejecución global, auditoría y captura de excepciones.
"""

import os
import sys
from datetime import datetime

# Importación de subsistemas y conectores de infraestructura técnica
from src.bronze.ingestion import DataIngestor
from src.utils.storage_connector import StorageConnector
from src.silver.transformation import SilverTransformer 
from src.utils.db_connector import DbConnector

class PipelineOrchestrator:
    """
    Clase Orquestadora del Sistema.
    Mantiene el estado global de la ejecución y gestiona la composición de componentes.
    """

    def __init__(self):
        """
        Inicializa las variables del entorno y compone los objetos de los subsistemas del pipeline.
        Sigue el principio de inyección y composición de dependencias.
        """
        # Lectura de la configuración del entorno técnico externo
        self.tipo_carga = os.getenv("TIPO_CARGA", "INCREMENTAL")
        
        # Composición de infraestructura persistente y conectores
        self.db_connector = DbConnector()
        self.storage_connector = StorageConnector()
        
        # Composición de módulos lógicos especialistas
        self.ingestor = DataIngestor()
        self.transformer = SilverTransformer()

    def registrar_auditoria_pipeline(self, estado, filas=0, error=None):
        """
        Centraliza los registros logs técnicos e históricos de auditoría en la BD.
        Implementa un bloque defensivo aislado para evitar que un fallo registrando
        la auditoría tire abajo el estado final del flujo operativo.

        Args:
            estado (str): Estado de finalización ('SUCCESS', 'FAILED').
            filas (int, opcional): Volumen de datos procesados (Medido en fact_orders).
            error (str, opcional): Mensaje de excepción técnica si ocurrió un fallo.
        """
        try:
            self.db_connector.registrar_auditoria(
                self.tipo_carga, 
                estado, 
                filas=filas, 
                error=error
            )
        except Exception as aud_err:
            # Falla silenciosa controlada en consola estándar para resguardar el flujo principal
            print(f"⚠️ No se pudo registrar el estado '{estado}' en la tabla de auditoría. Detalle: {str(aud_err)}")

    def ejecutarIngesta(self):
        """
        Operación del Sistema que resuelve el Caso de Uso: Ingestar Datos (Capa Bronze).
        Coordina de forma abstracta la descarga externa y manda a guardar el Parquet final.

        Returns:
            pd.DataFrame: Conjunto de datos en crudo retenido en memoria para optimizar el paso a Silver.
        """
        print("Step 1: Descargando datos desde Kaggle API...")
        df_raw = self.ingestor.descargar_datos()
        
        print("Step 2: Aplicando lógica de carga y simulación de fechas...")
        df_final = self.ingestor.aplicar_logica_carga(df_raw, self.tipo_carga)
        
        print("Step 3: Persistiendo en Storage (Capa Bronze)...")
        # Delegación Pura: El orquestador no manipula el file system ni conoce os.remove()
        self.ingestor.salvar_en_storage(df_final, self.tipo_carga, self.storage_connector)
        
        print("✅ CAPA BRONZE: Datos persistidos en Storage exitosamente.")
        return df_final

    def ejecutarLimpieza(self, df_bronze):
        """
        Operación del Sistema que resuelve el Caso de Uso: Limpiar y Estructurar (Capa Silver).
        Mapea el DataFrame de Bronze delegando su conversión analítica.

        Args:
            df_bronze (pd.DataFrame): Datos brutos sin tratamiento de la capa previa.

        Returns:
            dict: Colección indexada de DataFrames mapeados al modelo de datos.
        """
        print("\nStep 4: Iniciando Transformación (Esquema Snowflake)...")
        # Delegación de dominio: Toda lógica de IDs de claves se delega pasándole 'self.tipo_carga'
        tablas_silver = self.transformer.transformar_a_silver(df_bronze, self.tipo_carga)

        print("✅ CAPA SILVER: Estructuras relacionales generadas.")
        return tablas_silver

    def ejecutarCarga(self, tablas_silver):
        """
        Operación del Sistema que resuelve el Caso de Uso: Cargar Datos (Base de Datos).
        Escribe la colección relacional de manera física e incremental en PostgreSQL.

        Args:
            tablas_silver (dict): Estructuras de datos relacionales validadas.
        """
        print("Step 5: Persistiendo en Base de Datos PostgreSQL (Supabase)...")
        self.db_connector.cargar_a_sql(tablas_silver, self.tipo_carga)
        print("✅ BASE DE DATOS: Tablas persistidas exitosamente.")

    def ejecutarPipeline(self):
        """
        Método Principal del Ciclo de Vida: Ejecutar Pipeline Completo.
        Gobierna secuencialmente las fases lógicas del proceso bajo un marco robusto
        de control de excepciones globales (Try/Catch de Arquitectura).
        """
        start_time = datetime.now()
        
        try:
            print(f"{'='*60}")
            print(f"🚀 EJECUCIÓN DEL PIPELINE - MODO: {self.tipo_carga}")
            print(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}\n")

            # Ejecución secuencial controlada (Trazabilidad estricta con Diagramas de Secuencia y Casos de Uso)
            df_bronze = self.ejecutarIngesta()               # Ejecuta: [«include» Ingestar]
            tablas_silver = self.ejecutarLimpieza(df_bronze)     # Ejecuta: [«include» Limpiar]
            self.ejecutarCarga(tablas_silver)                 # Ejecuta: [«include» Cargar]

            # Registro de éxito en métricas operacionales
            filas_cargadas = len(tablas_silver.get('fact_order', []))
            self.registrar_auditoria_pipeline('SUCCESS', filas=filas_cargadas)
            
            end_time = datetime.now()
            duracion = end_time - start_time
            print(f"\n{'='*60}\n🏁 PIPELINE FINALIZADO CON ÉXITO - Duración: {duracion}\n{'='*60}")

        except Exception as e:
            # Captura global de excepciones: Evita la caída del script sin dejar traza explícita en auditoría
            self.registrar_auditoria_pipeline('FAILED', error=str(e))
            print(f"\n❌ ERROR CRÍTICO EN PIPELINE: {str(e)}")
            sys.exit(1)

if __name__ == "__main__":
    # Instanciación limpia y ejecución formal de la aplicación dirigida por eventos u orquestación de consola
    orchestrator = PipelineOrchestrator()
    orchestrator.ejecutarPipeline()
