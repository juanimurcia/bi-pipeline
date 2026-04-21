import os
import pandas as pd
from sqlalchemy import create_engine

def get_db_engine():
    """
    Construye la conexión a Supabase usando las variables de entorno 
    configuradas en GitHub Secrets.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "postgres")
    
    # Construcción de la URL para PostgreSQL
    # Usamos psycopg2 como driver (está en tu requirements.txt)
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    return create_engine(db_url)

def cargar_a_sql(tablas_dict, tipo_carga):
    """
    Recibe un diccionario de DataFrames y los persiste en la DB.
    - Si es GLOBAL: Usa 'replace' para fundar la base de datos.
    - Si es INCREMENTAL: Usa 'append' para sumar registros nuevos.
    """
    engine = get_db_engine()
    
    # Definimos el modo de escritura según el tipo de carga
    # GLOBAL borra y crea; INCREMENTAL solo agrega al final
    modo = 'replace' if tipo_carga == 'GLOBAL' else 'append'
    
    print(f"\n--- Iniciando persistencia en Silver (Modo: {modo}) ---")
    
    for nombre_tabla, df in tablas_dict.items():
        try:
            # Limpieza técnica: eliminar columnas de sistema antes de inyectar si existieran
            if 'state_name' in df.columns and nombre_tabla == 'dim_city':
                df = df.drop(columns=['state_name'])
            
            # Persistencia efectiva
            df.to_sql(nombre_tabla, engine, if_exists=modo, index=False)
            print(f"✅ Tabla '{nombre_tabla}' sincronizada con éxito.")
            
        except Exception as e:
            print(f"❌ Error al cargar la tabla {nombre_tabla}: {e}")
