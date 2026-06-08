import pytest
import pandas as pd
import numpy as np
from src.silver.transformation import SilverTransformer

@pytest.fixture
def sample_df():
    """Crea un DataFrame con todas las columnas esperadas por SilverTransformer."""
    return pd.DataFrame({
        'Order ID': [1, 1],
        'Order Item ID': [101, 102],
        'Customer ID': [50, 50],
        'Order Item Total': [100.0, 200.0],
        'Benefit per order': [10.0, 20.0],
        'Customer City': ['BUENOS AIRES', 'BUENOS AIRES'],
        'order_date (DateOrders)': ['2025-01-01', '2025-01-01'],
        'shipping_date (DateOrders)': ['2025-01-03', '2025-01-03'],
        'Market': ['LATAM', 'LATAM'],
        'order_region': ['South America', 'South America'],
        'order_country': ['Argentina', 'Argentina'],
        'order_state': ['CABA', 'CABA'],
        'order_city': ['CABA', 'CABA'],
        'type': ['DEBIT', 'DEBIT'],
        'order_status': ['COMPLETE', 'COMPLETE'],
        'delivery_status': ['SHIPPED', 'SHIPPED'],
        'shipping_mode': ['STANDARD', 'STANDARD'],
        'customer_segment': ['Consumer', 'Consumer'],
        'department_id': [1, 1],
        'department_name': ['Sports', 'Sports'],
        'category_id': [10, 10],
        'category_name': ['Soccer', 'Soccer'],
        'product_card_id': [999, 999],
        'product_name': ['Ball', 'Ball'],
        'product_price': [50.0, 50.0],
        'late_delivery_risk': [0, 0],
        'customer_fname': ['Juan', 'Juan'],
        'customer_lname': ['Perez', 'Perez'],
        'customer_email': ['juan@mail.com', 'juan@mail.com'],
        'customer_street': ['Calle 123', 'Calle 123'],
        'customer_zipcode': ['1000', '1000'],
        'latitude': [-34.6, -34.6],
        'longitude': [-58.4, -58.4],
        # --- COLUMNAS AGREGADAS PARA CORREGIR EL ERROR ---
        'order_item_quantity': [1, 2],
        'sales': [100.0, 200.0],
        'order_item_discount_rate': [0.1, 0.1],
        'order_item_discount': [10.0, 20.0]
    })

def test_estandarizar_nombres_columnas():
    transformer = SilverTransformer()
    df = pd.DataFrame({'Order Date (DateOrders)': [1]})
    df_clean = transformer._estandarizar_nombres_columnas(df)
    assert 'order_date_dateorders' in df_clean.columns

def test_filtrar_outliers_financieros():
    transformer = SilverTransformer()
    # Profit ratio de -2.0 debería ser filtrado (es menor a -1.0)
    df = pd.DataFrame({
        'order_item_total': [100], 
        'benefit_per_order': [-200]
    })
    df_clean = transformer._filtrar_outliers_financieros(df)
    assert len(df_clean) == 0

def test_transformar_a_silver_global_genera_tablas(sample_df):
    transformer = SilverTransformer()
    resultado = transformer.transformar_a_silver(sample_df, "GLOBAL")
    
    # Verificamos que contenga las tablas clave del modelo
    assert 'fact_order' in resultado
    assert 'dim_customer' in resultado
    assert 'dim_city' in resultado
    assert not resultado['fact_order'].empty

def test_resolver_unicidad_incremental_modifica_ids(sample_df):
    transformer = SilverTransformer()
    # Primero transformamos a tablas de hechos
    resultado_global = transformer.transformar_a_silver(sample_df, "GLOBAL")
    f_order = resultado_global['fact_order']
    f_item = resultado_global['fact_item_order']
    
    # Aplicamos la lógica incremental
    resultado_inc = transformer._resolver_unicidad_incremental(f_order, f_item)
    
    # El ID original era 1, al ser incremental debería ser mucho mayor (por el prefijo de fecha)
    assert resultado_inc['fact_order']['order_id'].iloc[0] > 1000000
