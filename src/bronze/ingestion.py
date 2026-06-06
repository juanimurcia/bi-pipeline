import os
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter 
from datetime import datetime
# Importamos las constantes desde nuestro archivo de configuración
from src.config import SUPABASE_BUCKET_NAME, KAGGLE_DATASET_HANDLE, KAGGLE_FILE_NAME

class DataIngestor:
    def __init__(self, dataset_handle: str = KAGGLE_DATASET_HANDLE, file_name: str = KAGGLE_FILE_NAME):
        """
        Componente de la Capa Bronze encargado de la extracción y preparación inicial.
        Encapsula las credenciales y el handle del dataset como estado interno.
        """
        self.dataset_handle = dataset_handle
        self.file_name = file_name
        self.bucket_name = SUPABASE_BUCKET_NAME  # Se conserva por consistencia de configuración

    def descargar_datos(self) -> pd.DataFrame:
        """Descarga el dataset de Kaggle usando el adaptador de Pandas."""
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

    def aplicar_logica_carga(self, df: pd.DataFrame, tipo_carga: str) -> pd.DataFrame:
        """
        GLOBAL: Mantiene fechas originales (2017-2018).
        INCREMENTAL: Mueve 100 registros al día de hoy (2026).
        """
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

    def obtener_ruta_supabase(self, df_lote: pd.DataFrame, tipo_carga: str) -> str:
        """Genera la ruta del archivo basada en la fecha de los registros."""
        fecha_ref = df_lote['order date (DateOrders)'].iloc[0]
        
        year = fecha_ref.strftime("%Y")
        month = fecha_ref.strftime("%m")
        day = fecha_ref.strftime("%d")
        
        if tipo_carga == "GLOBAL":
            return f"bronze/supply_chain/year={year}/month={month}/global_load.parquet"
        else:
            timestamp = datetime.now().strftime("%H%M")
            return f"bronze/supply_chain/year={year}/month={month}/batch_{day}_{timestamp}.parquet"

    def generar_archivo_temporal(self, df: pd.DataFrame, nombre_archivo: str) -> str:
        """Convierte un DataFrame en un archivo físico Parquet local."""
        df.to_parquet(nombre_archivo, index=False)
        return nombre_archivo

    def salvar_en_storage(self, df_final: pd.DataFrame, tipo_carga: str, storage_connector) -> None:
        """
        Maneja la estrategia física de particionamiento, guardado y limpieza local.
        Abstrae al orquestador de los detalles de infraestructura de archivos.
        """
        if tipo_carga == "GLOBAL":
            print("Step 3 [GLOBAL]: Iniciando particionamiento histórico...")
            grupos = df_final.groupby([
                df_final['order date (DateOrders)'].dt.year, 
                df_final['order date (DateOrders)'].dt.month
            ])
            
            for (year, month), df_grupo in grupos:
                remote_path = f"bronze/supply_chain/year={year}/month={month:02d}/global_load.parquet"
                temp_file = f"temp_{year}_{month}.parquet"
                
                self.generar_archivo_temporal(df_grupo, temp_file)
                storage_connector.upload_to_lakehouse(temp_file, remote_path)
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        else:
            print("Step 3 [INCREMENTAL]: Generando archivo diario Parquet...")
            remote_path = self.obtener_ruta_supabase(df_final, tipo_carga)
            temp_file = "temp_incremental.parquet"
            
            self.generar_archivo_temporal(df_final, temp_file)
            storage_connector.upload_to_lakehouse(temp_file, remote_path)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
