import os
import pandas as pd
import kagglehub
from datetime import datetime

def descargar_datos():
    """Descarga el dataset de Kaggle con el encoding correcto."""
    dataset_handle = "shashwatwork/dataco-smart-supply-chain-for-big-data-analysis"
    file_path = "DataCoSupplyChainDataset.csv"
    
    print("--- Descargando dataset desde Kaggle ---")
    df = kagglehub.load_dataset(
        "pandas",
        dataset_handle,
        file_path,
        pandas_kwargs={"encoding": "ISO-8859-1"}
    )
    
    df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
    df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')
    return df

def aplicar_logica_carga(df, tipo_carga):
    """
    GLOBAL: Todo el histórico (2017).
    INCREMENTAL: 100 filas movidas a la fecha actual (2026).
    """
    if tipo_carga == "GLOBAL":
        print("--- Procesando Carga Histórica Original ---")
        return df
    else:
        print("--- Procesando Carga Incremental Simulada (Hoy) ---")
        df_inc = df.sample(n=min(100, len(df))).copy()
        
        # Calculamos diferencia de días original
        diff = df_inc['shipping date (DateOrders)'] - df_inc['order date (DateOrders)']
        
        # Seteamos a hoy (Abril 2026)
        hoy = pd.Timestamp.now().normalize()
        df_inc['order date (DateOrders)'] = hoy
        df_inc['shipping date (DateOrders)'] = hoy + diff
        
        return df_inc

def obtener_ruta_supabase(df_lote, tipo_carga):
    """Define la carpeta según la fecha de los datos."""
    fecha_ref = df_lote['order date (DateOrders)'].iloc[0]
    
    year = fecha_ref.strftime("%Y")
    month = fecha_ref.strftime("%m")
    day = fecha_ref.strftime("%d")
    
    if tipo_carga == "GLOBAL":
        return f"bronze/supply_chain/year={year}/month={month}/global_load.parquet"
    else:
        timestamp = datetime.now().strftime("%H%M")
        return f"bronze/supply_chain/year={year}/month={month}/batch_{day}_{timestamp}.parquet"
