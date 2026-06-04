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
        
        # Usamos los atributos de la instancia (self) en lugar de las constantes sueltas
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            self.dataset_handle,
            self.file_name,
            pandas_kwargs={"encoding": "ISO-8859-1"}
        )
        
        # Aseguramos el formato de fecha para las columnas críticas
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
            # Tomamos 100 filas al azar
            df_inc = df.sample(n=min(100, len(df))).copy()
            
            # Calculamos la diferencia de días para no romper la lógica de envío
            diff = df_inc['shipping date (DateOrders)'] - df_inc['order date (DateOrders)']
            
            # Seteamos la fecha de orden a 'Hoy'
            hoy = pd.Timestamp.now().normalize()
            df_inc['order date (DateOrders)'] = hoy
            
            # Ajustamos la fecha de envío manteniendo el retraso original
            df_inc['shipping date (DateOrders)'] = hoy + diff
            
            return df_inc

    def obtener_ruta_supabase(self, df_lote: pd.DataFrame, tipo_carga: str) -> str:
        """Genera la ruta del archivo basada en la fecha de los registros."""
        # Tomamos la fecha del primer registro para definir la carpeta
        fecha_ref = df_lote['order date (DateOrders)'].iloc[0]
        
        year = fecha_ref.strftime("%Y")
        month = fecha_ref.strftime("%m")
        day = fecha_ref.strftime("%d")
        
        if tipo_carga == "GLOBAL":
            # Ejemplo: bronze/supply_chain/year=2017/month=09/global_load.parquet
            return f"bronze/supply_chain/year={year}/month={month}/global_load.parquet"
        else:
            # Ejemplo: bronze/supply_chain/year=2026/month=04/batch_19_2030.parquet
            timestamp = datetime.now().strftime("%H%M")
            return f"bronze/supply_chain/year={year}/month={month}/batch_{day}_{timestamp}.parquet"
