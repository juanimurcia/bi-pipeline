"""
Módulo: Capa Bronze - Ingestión de Datos
-----------------------------------------
Este módulo implementa el componente experto en la extracción y persistencia inicial
dentro de la Arquitectura Medallion. Sigue el principio de Única Responsabilidad (SRP),
encapsulando el acceso a APIs externas (Kaggle API) y abstrayendo la estrategia física
de almacenamiento local y particionamiento en el Lakehouse.

Diseño Arquitectónico: High Cohesion & Information Hiding.
"""

import os
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter 
from datetime import datetime
from src.config import SUPABASE_BUCKET_NAME, KAGGLE_DATASET_HANDLE, KAGGLE_FILE_NAME

class DataIngestor:
    """
    Componente encargado del Subsistema de Ingestión (Capa Bronze).
    
    Responsabilidades:
        1. Conectarse y descargar datos crudos desde la API de Kaggle.
        2. Controlar las mutaciones temporales de fechas según el tipo de carga.
        3. Gestionar el ciclo de vida de los archivos físicos locales (.parquet).
        4. Orquestar el particionamiento histórico (Year/Month) para cargas globales.
    """

    def __init__(self, dataset_handle: str = KAGGLE_DATASET_HANDLE, file_name: str = KAGGLE_FILE_NAME):
        """
        Inicializa el ingestor inyectando las dependencias de configuración.

        Args:
            dataset_handle (str): Identificador único del dataset en Kaggle (ej: 'usuario/dataset').
            file_name (str): Nombre del archivo físico dentro del repositorio de Kaggle.
        """
        self.dataset_handle = dataset_handle
        self.file_name = file_name
        self.bucket_name = SUPABASE_BUCKET_NAME  # Conservado para trazabilidad con la infraestructura

    def descargar_datos(self) -> pd.DataFrame:
        """
        Descarga el dataset de Kaggle de forma nativa optimizada para Pandas.
        Garantiza la tipificación inicial forzada de las columnas cronológicas críticas.

        Returns:
            pd.DataFrame: Conjunto de datos crudos (Raw Data) con tipos de fecha corregidos.
        """
        print(f"--- Descargando dataset: {self.dataset_handle} ---")
        
        # Consumo de la API externa encapsulado en el adaptador oficial
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            self.dataset_handle,
            self.file_name,
            pandas_kwargs={"encoding": "ISO-8859-1"}
        )
        
        # Precondición técnica: Asegurar el tipo datetime para evitar fallos de ordenamiento posteriores
        df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
        df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')
        return df

    def aplicar_logica_carga(self, df: pd.DataFrame, tipo_carga: str) -> pd.DataFrame:
        """
        Aplica la política o estrategia de carga de datos requerida por el negocio.

        Estrategias:
            - GLOBAL: Preserva la línea histórica original del dataset (2017-2018).
            - INCREMENTAL: Simula un delta de ingesta diaria extrayendo un lote aleatorio
                           y desplazando las fechas al día de hoy, manteniendo el desfasaje de envío.

        Args:
            df (pd.DataFrame): DataFrame crudo original.
            tipo_carga (str): Modo de ejecución del pipeline ('GLOBAL' o 'INCREMENTAL').

        Returns:
            pd.DataFrame: Dataset procesado listo para ser persistido en la capa Bronze.
        """
        if tipo_carga == "GLOBAL":
            print("--- Procesando Carga Histórica Original ---")
            return df
        else:
            print("--- Procesando Carga Incremental Simulada (Hoy) ---")
            # Muestreo determinístico acotado a 100 registros para simular transacciones diarias
            df_inc = df.sample(n=min(100, len(df))).copy()
            
            # Regla de integridad temporal: Calcular el gap original para no corromper la métrica de retrasos
            diff = df_inc['shipping date (DateOrders)'] - df_inc['order date (DateOrders)']
            
            # Normalización del tiempo a las 00:00:00 del día en curso
            hoy = pd.Timestamp.now().normalize()
            df_inc['order date (DateOrders)'] = hoy
            df_inc['shipping date (DateOrders)'] = hoy + diff
            
            return df_inc

    def obtener_ruta_supabase(self, df_lote: pd.DataFrame, tipo_carga: str) -> str:
        """
        Estrategia de Naming y Ruteo: Genera la URI semántica del objeto dentro del Lakehouse
        siguiendo el patrón estándar de particionamiento hive (year=YYYY/month=MM/).

        Args:
            df_lote (pd.DataFrame): Lote de datos del cual se extraerá la fecha de referencia.
            tipo_carga (str): Modo de ejecución actual.

        Returns:
            str: Ruta remota relativa para el almacenamiento del archivo Parquet.
        """
        fecha_ref = df_lote['order date (DateOrders)'].iloc[0]
        
        year = fecha_ref.strftime("%Y")
        month = fecha_ref.strftime("%m")
        day = fecha_ref.strftime("%d")
        
        if tipo_carga == "GLOBAL":
            return f"bronze/supply_chain/year={year}/month={month}/global_load.parquet"
        else:
            # En modo incremental, agrega un timestamp técnico para evitar colisiones entre ejecuciones del mismo día
            timestamp = datetime.now().strftime("%H%M")
            return f"bronze/supply_chain/year={year}/month={month}/batch_{day}_{timestamp}.parquet"

    def generar_archivo_temporal(self, df: pd.DataFrame, nombre_archivo: str) -> str:
        """
        Serializa un objeto DataFrame a formato binario físico Parquet de forma local.
        Abstrae los detalles de bajo nivel de las librerías de persistencia (I/O).

        Args:
            df (pd.DataFrame): Datos a escribir.
            nombre_archivo (str): Nombre o ruta del archivo temporal en disco.

        Returns:
            str: Confirmación de la ruta del archivo generado.
        """
        df.to_parquet(nombre_archivo, index=False)
        return nombre_archivo

    def salvar_en_storage(self, df_final: pd.DataFrame, tipo_carga: str, storage_connector) -> None:
        """
        Orquesta de extremo a extremo la estrategia física de almacenamiento en Bronze.
        Libera completamente al orquestador principal del manejo del sistema de archivos local y remoto.

        Args:
            df_final (pd.DataFrame): Dataset completo a guardar.
            tipo_carga (str): Estrategia de carga ('GLOBAL' / 'INCREMENTAL').
            storage_connector (StorageConnector): Componente de infraestructura para la subida a la nube.
        """
        if tipo_carga == "GLOBAL":
            print("Step 3 [GLOBAL]: Iniciando particionamiento histórico...")
            # Particionamiento físico en base al año y mes de la transacción
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            
            for (year, month), df_grupo in grupos:
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                temp_file = f"temp_{year}_{month}.parquet"
                
                # Ciclo de vida del archivo: Crear -> Subir -> Destruir (Evita fugas de almacenamiento local)
                self.generar_archivo_temporal(df_grupo, temp_file)
                storage_connector.upload_to_lakehouse(temp_file, remote_path)
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            print("Step 3 [INCREMENTAL]: Generando archivo diario Parquet...")
            remote_path = self.obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            
            # Persistencia incremental directa
            self.generar_archivo_temporal(df_final, temp_file)
            storage_connector.upload_to_lakehouse(temp_file, remote_path)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
