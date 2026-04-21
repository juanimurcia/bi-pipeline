import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool # Importante para el pooler
from sqlalchemy import BigInteger, Float, String, DateTime, Integer, Boolean

def get_db_engine():
    """
    Construye la conexión a Supabase usando las variables de entorno 
    configuradas en GitHub Secrets.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST") # El que termina en .pooler.supabase.com
    port = os.getenv("DB_PORT", "6543")
    dbname = os.getenv("DB_NAME", "postgres")
    
    # 1. Quitamos "&pgbouncer=true" de la URL. 
    # El puerto 6543 ya le indica a Supabase que pase por el pooler.
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    
    # 2. Mantenemos NullPool. Esto es VITAL para el Transaction Mode del Pooler.
    return create_engine(
        db_url, 
        poolclass=NullPool,
        connect_args={"connect_timeout": 30} # Aumentamos un poco el timeout por seguridad
    )

def cargar_a_sql(tablas_dict, tipo_carga):
    engine = get_db_engine()
    modo = 'replace' if tipo_carga == 'GLOBAL' else 'append'
    
    # --- DICCIONARIO DE ESQUEMAS COMPLETO ---
    esquemas = {
        # Tablas de Hechos (Facts)
        'fact_item_order': {
            'order_item_id': BigInteger, 'order_id': BigInteger, 'product_id': BigInteger,
            'order_item_quantity': Integer, 'sales': Float, 'order_item_discount': Float,
            'order_item_discount_rate': Float, 'order_item_total': Float,
            'benefit_per_order': Float, 'profit_ratio_calc': Float
        },
        'fact_order': {
            'order_id': BigInteger, 'customer_id': BigInteger, 'city_id': Integer,
            'order_date_dateorders': DateTime, 'shipping_date_dateorders': DateTime,
            'type_id': Integer, 'order_status_id': Integer, 'delivery_status_id': Integer,
            'shipping_mode_id': Integer, 'late_delivery_risk': Boolean
        },
        # Dimensiones Principales
        'dim_customer': {
            'customer_id': BigInteger, 'customer_fname': String(100), 'customer_lname': String(100),
            'customer_email': String(150), 'customer_street': String(255), 
            'customer_zipcode': String(20), 'latitude': Float, 'longitude': Float
        },
        'dim_product': {
            'product_id': BigInteger, 'category_id': Integer, 
            'product_name': String(255), 'product_price': Float
        },
        'dim_category': {
            'category_id': Integer, 'category_name': String(100), 'department_id': Integer
        },
        'dim_department': {
            'department_id': Integer, 'department_name': String(100)
        },
        # Snowflake Geográfico
        'dim_city': {'city_id': Integer, 'city_name': String(100), 'state_id': Integer},
        'dim_state': {'state_id': Integer, 'state_name': String(100), 'country_id': Integer},
        'dim_country': {'country_id': Integer, 'country_name': String(100), 'region_id': Integer},
        'dim_region': {'region_id': Integer, 'region_name': String(100), 'market_id': Integer},
        'dim_market': {'market_id': Integer, 'market_name': String(100)},
        # Tablas de Referencia (Lookups)
        'dim_type': {'type_id': Integer, 'type_name': String(50)},
        'dim_order_status': {'order_status_id': Integer, 'order_status_name': String(50)},
        'dim_delivery_status': {'delivery_status_id': Integer, 'delivery_status_name': String(50)},
        'dim_shipping_mode': {'shipping_mode_id': Integer, 'shipping_mode_name': String(50)},
        'dim_customer_segment': {'customer_segment_id': Integer, 'customer_segment_name': String(50)}
    }

    print(f"\n--- Iniciando persistencia en Silver (Modo: {modo}) ---")
    
    for nombre_tabla, df in tablas_dict.items():
        try:
            # Limpieza técnica: eliminamos columnas auxiliares que no pertenecen al modelo físico
            if nombre_tabla == 'dim_city' and 'state_name' in df.columns:
                df = df.drop(columns=['state_name'])
            
            dtype_map = esquemas.get(nombre_tabla, None)
            
            df.to_sql(
                nombre_tabla, 
                engine, 
                if_exists=modo, 
                index=False, 
                chunksize=1000, # Aumentamos el chunksize para mayor velocidad en carga global
                dtype=dtype_map
            )
            print(f"✅ Tabla '{nombre_tabla}' sincronizada con tipos definidos.")
            
        except Exception as e:
            print(f"❌ Error al cargar la tabla {nombre_tabla}: {e}")
