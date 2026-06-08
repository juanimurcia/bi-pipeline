import pytest
import pandas as pd
from src.bronze.ingestion import DataIngestor
from unittest.mock import MagicMock

# 1. Test de Lógica Pura: Validar la carga incremental
def test_aplicar_logica_carga_incremental():
    ingestor = DataIngestor()
    # Creamos un DF pequeño de prueba
    data = {
        'order date (DateOrders)': [pd.Timestamp('2025-01-01')],
        'shipping date (DateOrders)': [pd.Timestamp('2025-01-03')]
    }
    df = pd.DataFrame(data)
    
    # Ejecutamos la lógica incremental
    df_resultado = ingestor._aplicar_logica_carga(df, "INCREMENTAL")
    
    # Verificamos que las fechas se hayan actualizado a "hoy"
    hoy = pd.Timestamp.now().normalize()
    assert df_resultado['order date (DateOrders)'].iloc[0] == hoy
    assert df_resultado['shipping date (DateOrders)'].iloc[0] > hoy

# 2. Test con Mock: Validar que se intente subir al Storage
def test_salvar_en_storage_llama_al_conector():
    ingestor = DataIngestor()
    
    # Creamos un MOCK del conector de Storage
    mock_storage = MagicMock()
    
    df_prueba = pd.DataFrame({'a': [1]})
    
    # Ejecutamos el método que salva
    ingestor._salvar_en_storage(df_prueba, "INCREMENTAL", mock_storage)
    
    # ASSERT: Verificamos que el MOCK recibió una llamada a upload_to_lakehouse
    mock_storage.upload_to_lakehouse.assert_called_once()
