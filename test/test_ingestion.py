import pandas as pd
from unittest.mock import MagicMock
from src.bronze.ingestion import DataIngestor

def test_salvar_en_storage_llama_al_conector():
    ingestor = DataIngestor()
    mock_storage = MagicMock()
    
    # Creamos un DataFrame que tenga exactamente las columnas que el código espera
    # Incluimos datos de fecha para que el strftime no falle
    df_prueba = pd.DataFrame({
        'order date (DateOrders)': pd.to_datetime(['2025-01-01']),
        'shipping date (DateOrders)': pd.to_datetime(['2025-01-02']),
        'otra_columna': [1]
    })
    
    # Ejecutamos el método que salva
    ingestor._salvar_en_storage(df_prueba, "INCREMENTAL", mock_storage)
    
    # Verificamos que se haya llamado al conector
    mock_storage.upload_to_lakehouse.assert_called_once()
