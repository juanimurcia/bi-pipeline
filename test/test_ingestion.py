import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.bronze.ingestion import DataIngestor

# 1. Test de Lógica Global: Asegura que el dataframe no sea alterado
def test_logica_carga_global():
    ingestor = DataIngestor()
    df_falso = pd.DataFrame({'a': [1, 2, 3]})
    df_resultado = ingestor._aplicar_logica_carga(df_falso, "GLOBAL")
    assert len(df_resultado) == 3

# 2. Test de Lógica Incremental: Asegura el límite de 100 registros
def test_logica_carga_incremental_limita_registros():
    ingestor = DataIngestor()
    # Creamos un DF con 500 registros para probar el corte
    df_falso = pd.DataFrame({
        'order date (DateOrders)': pd.to_datetime(['2025-01-01'] * 500),
        'shipping date (DateOrders)': pd.to_datetime(['2025-01-05'] * 500)
    })
    df_resultado = ingestor._aplicar_logica_carga(df_falso, "INCREMENTAL")
    assert len(df_resultado) == 100

# 3. Test de Fechas: Asegura que el modo incremental "mueva" las fechas al presente
def test_fechas_incremental_son_actuales():
    ingestor = DataIngestor()
    df_falso = pd.DataFrame({
        'order date (DateOrders)': pd.to_datetime(['2020-01-01']),
        'shipping date (DateOrders)': pd.to_datetime(['2020-01-05'])
    })
    df_resultado = ingestor._aplicar_logica_carga(df_falso, "INCREMENTAL")
    # Comprobamos que la fecha resultante sea el día de hoy
    assert df_resultado['order date (DateOrders)'].iloc[0].date() == pd.Timestamp.now().date()

# 4. Test de Persistencia: Asegura que el sistema intente subir el archivo 
# PERO usando un MOCK para que NO haya conexión real a Supabase
def test_salvar_en_storage_llama_al_conector():
    ingestor = DataIngestor()
    # Creamos un objeto falso (Mock) en lugar del conector real
    mock_storage = MagicMock()
    
    # Datos necesarios para que la función no falle (debe coincidir con tus columnas)
    df_prueba = pd.DataFrame({
        'order date (DateOrders)': pd.to_datetime(['2025-01-01']),
        'shipping date (DateOrders)': pd.to_datetime(['2025-01-02']),
        'columna_extra': [1]
    })
    
    # Ejecutamos la función de guardado usando el mock
    ingestor._salvar_en_storage(df_prueba, "INCREMENTAL", mock_storage)
    
    # VERIFICACIÓN: Comprobamos que el código llamó a la función de subida
    # Esto confirma que la lógica de "decidir subir" funciona, sin usar internet
    mock_storage.upload_to_lakehouse.assert_called_once()
