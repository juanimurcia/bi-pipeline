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
    mock_storage = MagicMock()
    
    # Crea un DataFrame que tenga la columna que el código busca
    df_prueba = pd.DataFrame({
        'order date (DateOrders)': pd.to_datetime(['2025-01-01']),
        'columna_extra': [1]
    })
    
    # Ahora sí, al ejecutar esto, el código encontrará la columna
    ingestor._salvar_en_storage(df_prueba, "INCREMENTAL", mock_storage)
    
    # Verificamos que se haya intentado subir al storage
    mock_storage.upload_to_lakehouse.assert_called_once()
