"""
Módulo: Capa Silver - Transformación y Modelado Relacional
-----------------------------------------------------------
Este módulo procesa los datos de la capa Bronze para transformarlos en estructuras limpias,
normalizadas y enriquecidas bajo un modelo relacional. Se encarga de aplicar reglas de negocio
estrictas, tipificación, limpieza de outliers y estructuración dimensional.

Diseño Arquitectónico: Alta Cohesión de Dominio. Toda la lógica de transformación estructural 
y resolución analítica de IDs para la base de datos se encapsula estrictamente aquí.
"""

import pandas as pd
import numpy as np
from datetime import datetime

class SilverTransformer:
    """
    Componente experto encargado de la Capa de Transformación (Capa Silver).
    
    Aplica patrones de Information Hiding mediante métodos privados (_) para las subtareas
    operativas, exponiendo una interfaz pública limpia para el orquestador.
    """

    def __init__(self):
        """Inicializa el transformador de la capa Silver."""
        pass

    # =========================================================================
    # MÉTODOS PRIVADOS: OPERACIONES ATÓMICAS DE LIMPIEZA Y CALIDAD DE DATOS
    # =========================================================================

    def _estandarizar_nombres_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza los headers del DataFrame al estándar técnico snake_case de bases de datos.
        Remueve espacios y caracteres especiales disruptivos como paréntesis.

        Args:
            df (pd.DataFrame): DataFrame con nombres originales.

        Returns:
            pd.DataFrame: Copia del DataFrame con nombres formateados uniformemente.
        """
        df_new = df.copy()
        df_new.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df_new.columns]
        return df_new

    def _filtrar_outliers_financieros(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Regla de Calidad de Datos (Regla de Negocio): Asegura la consistencia financiera
        recalculando el profit ratio y eliminando registros con inconsistencias extremas.

        Args:
            df (pd.DataFrame): DataFrame con nombres estandarizados.

        Returns:
            pd.DataFrame: Dataset sanitizado sin anomalías de división por cero o ratios imposibles.
        """
        df_clean = df.copy()
        # Tratamiento preventivo contra la división por cero en transacciones nulas
        df_clean['profit_ratio_calc'] = np.where(
            df_clean['order_item_total'] != 0, 
            df_clean['benefit_per_order'] / df_clean['order_item_total'], 
            0
        )
        # Filtro estricto: Elimina pérdidas superiores al 100% que representen anomalías del sistema origen
        return df_clean[df_clean['profit_ratio_calc'] >= -1.0].copy()

    def _normalizar_tipos_y_textos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza formatos estéticos de cadenas (Title Case) y fuerza el casting
        de tipos primitivos (booleanos y timestamps) para la correcta inserción en BD.

        Args:
            df (pd.DataFrame): DataFrame limpio de anomalías.

        Returns:
            pd.DataFrame: DataFrame listo para el proceso de normalización relacional.
        """
        df_norm = df.copy()
        text_cols = ['customer_city', 'customer_state', 'order_city', 'order_state', 'order_country', 
                     'order_region', 'product_name', 'category_name', 'market',
                     'customer_segment', 'type', 'order_status', 'delivery_status', 'shipping_mode']
        
        # Elimina espacios espurios en los extremos y unifica en formato de títulos profesionales
        for col in text_cols:
            if col in df_norm.columns:
                df_norm[col] = df_norm[col].astype(str).str.strip().str.title()
        
        # Seteo explícito del tipo de dato cronológico
        df_norm['order_date_dateorders'] = pd.to_datetime(df_norm['order_date_dateorders'])
        df_norm['shipping_date_dateorders'] = pd.to_datetime(df_norm['shipping_date_dateorders'])
        
        if 'product_status' in df_norm.columns:
            df_norm['product_status'] = df_norm['product_status'].astype(bool)
            
        return df_norm

    # =========================================================================
    # MÉTODOS PRIVADOS: PROCESO DE SEPARACIÓN EN DIMENSIONES (ESQUEMA RELACIONAL)
    # =========================================================================

    def _crear_lookup_table(self, df: pd.DataFrame, col_source: str, id_name: str, val_name: str) -> pd.DataFrame:
        """
        Abstracción Factoría: Genera tablas dimensionales maestras (Lookup tables)
        garantizando unicidad y asignando claves surrogadas numéricas auto-incrementales.

        Args:
            df (pd.DataFrame): Dataset origen.
            col_source (str): Columna de texto original a extraer.
            id_name (str): Nombre de la clave primaria surrogada a generar (PK).
            val_name (str): Nombre que adoptará la columna de valor de negocio.

        Returns:
            pd.DataFrame: Tabla dimensional limpia con estructura (ID, Nombre).
        """
        lookup = pd.DataFrame(sorted(df[col_source].unique()), columns=[val_name])
        lookup.insert(0, id_name, lookup.index + 1)
        return lookup

    def _generar_jerarquia_geografica(self, df: pd.DataFrame):
        """
        Desnormaliza y estructura la jerarquía geográfica (Snowflake Pattern).
        Resuelve dependencias transitivas: Market -> Region -> Country -> State -> City.

        Returns:
            tuple: Diccionario u objetos DataFrame correspondientes a cada nivel de la jerarquía.
        """
        dim_market = self._crear_lookup_table(df, 'market', 'market_id', 'market_name')
        
        dim_region = df[['order_region', 'market']].drop_duplicates().merge(dim_market, left_on='market', right_on='market_name')
        dim_region = dim_region[['order_region', 'market_id']].reset_index(drop=True)
        dim_region.insert(0, 'region_id', dim_region.index + 1)
        dim_region.rename(columns={'order_region': 'region_name'}, inplace=True)
        
        dim_country = df[['order_country', 'order_region']].drop_duplicates().merge(dim_region, left_on='order_region', right_on='region_name')
        dim_country = dim_country[['order_country', 'region_id']].reset_index(drop=True)
        dim_country.insert(0, 'country_id', dim_country.index + 1)
        dim_country.rename(columns={'order_country': 'country_name'}, inplace=True)
        
        dim_state = df[['order_state', 'order_country']].drop_duplicates().merge(dim_country, left_on='order_country', right_on='country_name')
        dim_state = dim_state[['order_state', 'country_id']].reset_index(drop=True)
        dim_state.insert(0, 'state_id', dim_state.index + 1)
        dim_state.rename(columns={'order_state': 'state_name'}, inplace=True)
        
        dim_city = df[['order_city', 'order_state']].drop_duplicates().merge(dim_state, left_on='order_state', right_on='state_name')
        dim_city = dim_city[['order_city', 'state_id']].reset_index(drop=True)
        dim_city.insert(0, 'city_id', dim_city.index + 1)
        dim_city.rename(columns={'order_city': 'city_name'}, inplace=True)
        
        return dim_market, dim_region, dim_country, dim_state, dim_city

    def _construir_tablas_hechos(self, df_master: pd.DataFrame, dims: dict):
        """
        Ensambla y blinda las tablas de hechos inyectando las claves foráneas (FK)
        del modelo mediante cruces estructurados (Joins) y eliminando duplicidades.

        Args:
            df_master (pd.DataFrame): Dataset central completamente normalizado.
            dims (dict): Colección de dimensiones generadas previamente para mapear IDs.

        Returns:
            tuple: DataFrames correspondientes a (fact_order, fact_item_order).
        """
        # Creación del puente geográfico para resolver la FK hacia dim_city
        geo_bridge = dims['dim_city'].merge(dims['dim_state'], on='state_id')
        
        df_f = df_master.merge(
            geo_bridge, 
            left_on=['order_city', 'order_state'], 
            right_on=['city_name', 'state_name'],
            how='left'
        )

        # Mapeo masivo de claves foráneas
        df_f = df_f.merge(dims['dim_type'], left_on='type', right_on='type_name') \
                   .merge(dims['dim_order_status'], left_on='order_status', right_on='order_status_name') \
                   .merge(dims['dim_delivery_status'], left_on='delivery_status', right_on='delivery_status_name') \
                   .merge(dims['dim_shipping_mode'], left_on='shipping_mode', right_on='shipping_mode_name')
        
        # Tabla de Hechos Principal: Cabecera del Pedido (Grano por Pedido Único)
        f_order = df_f[['order_id', 'customer_id', 'city_id', 'order_date_dateorders', 'shipping_date_dateorders', 
                        'type_id', 'order_status_id', 'delivery_status_id', 'shipping_mode_id', 'late_delivery_risk']].drop_duplicates('order_id')
        
        # Tabla de Hechos Detalle: Ítems del Pedido (Grano por Transacción/Línea)
        f_item = df_f[['order_item_id', 'order_id', 'product_card_id', 'order_item_quantity', 'sales', 'order_item_discount_rate', 'order_item_discount',
                       'order_item_total', 'benefit_per_order', 'profit_ratio_calc']].rename(columns={'product_card_id': 'product_id'})
        f_item = f_item.drop_duplicates(subset=['order_item_id'])
        
        return f_order, f_item

    # =========================================================================
    # INTERFAZ PÚBLICA: ORQUESTADOR COMPONENTE DE CAPA
    # =========================================================================

    def transformar_a_silver(self, df_bronze: pd.DataFrame, tipo_carga: str) -> dict:
        """
        Punto de Entrada del Caso de Uso: Convierte el DataFrame de la capa Bronze
        en una colección estructurada de tablas de negocio (Dimensiones y Hechos).
        
        Garantiza el desacoplamiento al procesar de forma interna las claves primarias
        en ejecuciones incrementales mediante transformaciones determinísticas.

        Args:
            df_bronze (pd.DataFrame): Datos de entrada provenientes del Data Lakehouse.
            tipo_carga (str): Modo de pipeline que determina la política de salida.

        Returns:
            dict: Estructuras relacionales indexadas por su nombre de destino físico.
        """
        print("--- Transformando datos a Capa Silver ---")
        
        # Fase 1: Calidad de datos secuencial
        df = self._estandarizar_nombres_columnas(df_bronze)
        df = df.drop_duplicates(subset=['order_item_id'])
        df = self._filtrar_outliers_financieros(df)
        df = self._normalizar_tipos_y_textos(df)
        
        # Fase 2: Modelado de Dimensiones
        d_mkt, d_reg, d_cnt, d_st, d_ct = self._generar_jerarquia_geografica(df)
        d_segment = self._crear_lookup_table(df, 'customer_segment', 'customer_segment_id', 'customer_segment_name')
        
        # Modelado avanzado de la dimensión Clientes
        d_customer = df[['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 
                         'customer_street', 'customer_zipcode', 'customer_segment', 
                         'latitude', 'longitude']].drop_duplicates('customer_id')
        
        d_customer = d_customer.merge(
            d_segment, 
            left_on='customer_segment', 
            right_on='customer_segment_name', 
            how='left'
        ).drop(columns=['customer_segment', 'customer_segment_name'])
        
        # Estructuración inicial del mapa de tablas del modelo analítico
        tablas_silver = {
            'dim_market': d_mkt, 
            'dim_region': d_reg, 
            'dim_country': d_cnt, 
            'dim_state': d_st, 
            'dim_city': d_ct,
            'dim_type': self._crear_lookup_table(df, 'type', 'type_id', 'type_name'),
            'dim_order_status': self._crear_lookup_table(df, 'order_status', 'order_status_id', 'order_status_name'),
            'dim_delivery_status': self._crear_lookup_table(df, 'delivery_status', 'delivery_status_id', 'delivery_status_name'),
            'dim_shipping_mode': self._crear_lookup_table(df, 'shipping_mode', 'shipping_mode_id', 'shipping_mode_name'),
            'dim_customer_segment': d_segment,
            'dim_department': df[['department_id', 'department_name']].drop_duplicates(),
            'dim_category': df[['category_id', 'category_name', 'department_id']].drop_duplicates(),
            'dim_product': df[['product_card_id', 'category_id', 'product_name', 'product_price']].drop_duplicates('product_card_id').rename(columns={'product_card_id': 'product_id'}),
            'dim_customer': d_customer
        }
        
        # Fase 3: Modelado de Hechos
        f_order, f_item = self._construir_tablas_hechos(df, tablas_silver)
        tablas_silver['fact_order'] = f_order
        tablas_silver['fact_item_order'] = f_item
        
        # Fase 4: Resolución de Unicidad de Claves (Encapsulación de Carga Incremental)
        if tipo_carga == 'INCREMENTAL':
            print("⚠️ Ajustando IDs para carga incremental (Concatenación Determinística)...")
            
            # Generación de una semilla temporal única basada en la fecha y el segundo exacto de ejecución
            now_incremental = datetime.now()
            fecha_prefijo = now_incremental.strftime("%y%m%d") 
            segundos_dia = (now_incremental.hour * 3600) + (now_incremental.minute * 60) + now_incremental.second
            segundos_prefijo = f"{segundos_dia:05d}" 
            prefijo_ejecucion = f"{fecha_prefijo}{segundos_prefijo}" 
            
            # Regla de Negocio: En modo incremental solo se actualizan las transacciones (Hechos)
            # Se filtran y descartan las tablas de dimensiones estáticas para optimizar la carga a BD
            tablas_silver = {k: v for k, v in tablas_silver.items() if k.startswith('fact_')}
            
            # Mutación controlada de IDs (BIGINT seguro) para blindar la PK frente a duplicaciones en base de datos
            if 'fact_order' in tablas_silver:
                tablas_silver['fact_order']['order_id'] = (prefijo_ejecucion + tablas_silver['fact_order']['order_id'].astype(str)).astype(int)
            
            if 'fact_item_order' in tablas_silver:
                tablas_silver['fact_item_order']['order_id'] = (prefijo_ejecucion + tablas_silver['fact_item_order']['order_id'].astype(str)).astype(int)
                tablas_silver['fact_item_order']['order_item_id'] = (prefijo_ejecucion + tablas_silver['fact_item_order']['order_item_id'].astype(str)).astype(int)

        return tablas_silver
