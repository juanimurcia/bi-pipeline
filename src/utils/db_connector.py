import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy import BigInteger, Float, String, DateTime, Integer, Boolean

class DbConnector:
    def __init__(self):
        """
        Conector encargado de la persistencia y auditoría en la Base de Datos Relacional.
        Construye el motor de SQLAlchemy y encapsula los esquemas técnicos de la capa Silver.
        """
        # El motor se genera una sola vez y queda disponible en toda la instancia
        self.engine = self._build_engine()
        
        # Mapeo de tipos SQL encapsulado como atributo de la clase
        self.esquemas = {
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
            'dim_customer': {
                'customer_id': BigInteger, 'customer_fname': String(100), 'customer_lname': String(100),
                'customer_email': String(150), 'customer_street': String(255), 
                'customer_zipcode': String(20), 'latitude': Float, 'longitude': Float, 'customer_segment_id': Integer
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
            'dim_city': {'city_id': Integer, 'city_name': String(100), 'state_id': Integer},
            'dim_state': {'state_id': Integer, 'state_name': String(100), 'country_id': Integer},
            'dim_country': {'country_id': Integer, 'country_name': String(100), 'region_id': Integer},
            'dim_region': {'region_id': Integer, 'region_name': String(100), 'market_id': Integer},
            'dim_market': {'market_id': Integer, 'market_name': String(100)},
            'dim_type': {'type_id': Integer, 'type_name': String(50)},
            'dim_order_status': {'order_status_id': Integer, 'order_status_name': String(50)},
            'dim_delivery_status': {'delivery_status_id': Integer, 'delivery_status_name': String(50)},
            'dim_shipping_mode': {'shipping_mode_id': Integer, 'shipping_mode_name': String(50)},
            'dim_customer_segment': {'customer_segment_id': Integer, 'customer_segment_name': String(50)}
        }

    def _build_engine(self):
        """Construye la conexión a Supabase usando variables de entorno (Método Privado)."""
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST") 
        port = os.getenv("DB_PORT", "6543")
        dbname = os.getenv("DB_NAME", "postgres")
        
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
        
        return create_engine(
            db_url, 
            poolclass=NullPool,
            connect_args={"connect_timeout": 30}
        )

    def registrar_auditoria(self, tipo: str, estado: str, filas: int = 0, error: str = None):
        """Inserta un registro en la tabla audit_logs para seguimiento del pipeline."""
        query = text("""
            INSERT INTO audit_logs (tipo_carga, estado, filas_insertadas, mensaje_error)
            VALUES (:tipo, :estado, :filas, :error)
        """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(query, {
                    "tipo": tipo, 
                    "estado": estado, 
                    "filas": filas, 
                    "error": error
                })
                conn.commit()
        except Exception as e:
            print(f"⚠️ No se pudo escribir en audit_logs: {e}")

    def cargar_a_sql(self, tablas_dict: dict, tipo_carga: str):
        """Sincroniza los DataFrames transformados con las tablas de PostgreSQL."""
        modo = 'replace' if tipo_carga == 'GLOBAL' else 'append'
        print(f"\n--- Iniciando persistencia en Silver (Modo: {modo}) ---")
        
        with self.engine.connect() as connection:
            for nombre_tabla, df in tablas_dict.items():
                # 1. SI ES GLOBAL, ELIMINAMOS CON CASCADE
                if modo == 'replace':
                    connection.execute(text(f'DROP TABLE IF EXISTS {nombre_tabla} CASCADE'))
                    connection.commit()
                
                # 2. LIMPIEZA TÉCNICA
                if nombre_tabla == 'dim_city' and 'state_name' in df.columns:
                    df = df.drop(columns=['state_name'])
                
                # 3. PERSISTENCIA (Buscamos el tipo en nuestro atributo encapsulado)
                dtype_map = self.esquemas.get(nombre_tabla, None)
                
                df.to_sql(
                    nombre_tabla, 
                    self.engine, 
                    if_exists=modo, 
                    index=False, 
                    chunksize=1000, 
                    dtype=dtype_map
                )
                print(f"✅ Tabla '{nombre_tabla}' sincronizada.")
