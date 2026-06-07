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
        self.dataset_handle = dataset_handle
        self.file_name = file_name
        self.bucket_name = SUPABASE_BUCKET_NAME

    def ejecutar_ingestion_bronze(self, tipo_carga: str, storage_connector) -> pd.DataFrame:
        """
        MÉTODO MAESTRO (Fachada): Orquesta de extremo a extremo el ciclo de vida 
        de la capa Bronze, abstrayendo la secuencia del orquestador global.
        """
        print("Step 1: Descargando datos desde Kaggle API...")
        df_raw = self._descargar_datos()
        
        print("Step 2: Aplicando lógica de carga y simulación de fechas...")
        df_final = self._aplicar_logica_carga(df_raw, tipo_carga)
        
        print("Step 3: Persistiendo en Storage (Capa Bronze)...")
        self._salvar_en_storage(df_final, tipo_carga, storage_connector)
        
        return df_final

    def _descargar_datos(self) -> pd.DataFrame:
        """Descarga el dataset de Kaggle de forma nativa optimizada para Pandas."""
        print(f"--- Descargando dataset: {self.dataset_handle} ---")
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            self.dataset_handle,
            self.file_name,
            pandas_kwargs={"encoding": "ISO-8859-1"}
        )
        df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
        df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')
        return df

    def _aplicar_logica_carga(self, df: pd.DataFrame, tipo_carga: str) -> pd.DataFrame:
        """Aplica la política o estrategia de carga de datos requerida por el negocio."""
        if tipo_carga == "GLOBAL":
            print("--- Procesando Carga Histórica Original ---")
            return df
        else:
            print("--- Procesando Carga Incremental Simulada (Hoy) ---")
            df_inc = df.sample(n=min(100, len(df))).copy()
            diff = df_inc['shipping date (DateOrders)'] - df_inc['order date (DateOrders)']
            hoy = pd.Timestamp.now().normalize()
            df_inc['order date (DateOrders)'] = hoy
            df_inc['shipping date (DateOrders)'] = hoy + diff
            return df_inc

    def _obtener_ruta_supabase(self, df_lote: pd.DataFrame, tipo_carga: str) -> str:
        """Genera la URI semántica del objeto dentro del Lakehouse."""
        fecha_ref = df_lote['order date (DateOrders)'].iloc[0]
        year = fecha_ref.strftime("%Y")
        month = fecha_ref.strftime("%m")
        day = fecha_ref.strftime("%d")
        
        if tipo_carga == "GLOBAL":
            return f"bronze/supply_chain/year={year}/month={month}/global_load.parquet"
        else:
            timestamp = datetime.now().strftime("%H%M")
            return f"bronze/supply_chain/year={year}/month={month}/batch_{day}_{timestamp}.parquet"

    def _generar_archivo_temporal(self, df: pd.DataFrame, nombre_archivo: str) -> str:
        """Serializa un objeto DataFrame a formato binario físico Parquet local."""
        df.to_parquet(nombre_archivo, index=False)
        return nombre_archivo

    def _salvar_en_storage(self, df_final: pd.DataFrame, tipo_carga: str, storage_connector) -> None:
        """Orquesta la estrategia física de almacenamiento remoto en Bronze."""
        if tipo_carga == "GLOBAL":
            print("--- [GLOBAL]: Iniciando particionamiento histórico... ---")
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            for (year, month), df_grupo in grupos:
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                temp_file = f"temp_{year}_{month}.parquet"
                
                self._generar_archivo_temporal(df_grupo, temp_file)
                storage_connector.upload_to_lakehouse(temp_file, remote_path)
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            print("--- [INCREMENTAL]: Generando archivo diario Parquet... ---")
            remote_path = self._obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            
            self._generar_archivo_temporal(df_final, temp_file)
            storage_connector.upload_to_lakehouse(temp_file, remote_path)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
