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
        """Normaliza los headers del DataFrame al estándar técnico snake_case."""
        df_new = df.copy()
        df_new.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df_new.columns]
        return df_new

    def _filtrar_outliers_financieros(self, df: pd.DataFrame) -> pd.DataFrame:
        """Regla de Calidad de Datos: Remueve registros con inconsistencias extremas."""
        df_clean = df.copy()
        df_clean['profit_ratio_calc'] = np.where(
            df_clean['order_item_total'] != 0, 
            df_clean['benefit_per_order'] / df_clean['order_item_total'], 
            0
        )
        return df_clean[df_clean['profit_ratio_calc'] >= -1.0].copy()

    def _normalizar_tipos_y_textos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza formatos estéticos de cadenas y fuerza el casting de tipos primitivos."""
        df_norm = df.copy()
        text_cols = ['customer_city', 'customer_state', 'order_city', 'order_state', 'order_country', 
                     'order_region', 'product_name', 'category_name', 'market',
                     'customer_segment', 'type', 'order_status', 'delivery_status', 'shipping_mode']
        
        for col in text_cols:
            if col in df_norm.columns:
                df_norm[col] = df_norm[col].astype(str).str.strip().str.title()
        
        df_norm['order_date_dateorders'] = pd.to_datetime(df_norm['order_date_dateorders'])
        df_norm['shipping_date_dateorders'] = pd.to_datetime(df_norm['shipping_date_dateorders'])
        
        if 'product_status' in df_norm.columns:
            df_norm['product_status'] = df_norm['product_status'].astype(bool)
            
        return df_norm

    # =========================================================================
    # MÉTODOS PRIVADOS: PROCESO DE SEPARACIÓN EN DIMENSIONES (ESQUEMA RELACIONAL)
    # =========================================================================

    def _crear_lookup_table(self, df: pd.DataFrame, col_source: str, id_name: str, val_name: str) -> pd.DataFrame:
        """Abstracción Factoría: Genera tablas dimensionales maestras con claves surrogadas."""
        lookup = pd.DataFrame(sorted(df[col_source].unique()), columns=[val_name])
        lookup.insert(0, id_name, lookup.index + 1)
        return lookup

    def _generar_jerarquia_geografica(self, df: pd.DataFrame) -> tuple:
        """Desnormaliza y estructura la jerarquía geográfica (Snowflake Pattern)."""
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

    def _construir_dimension_clientes(self, df: pd.DataFrame, d_segment: pd.DataFrame) -> pd.DataFrame:
        """Modelado cohesivo de la dimensión Clientes resolviendo su cruce relacional."""
        d_customer = df[['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 
                         'customer_street', 'customer_zipcode', 'customer_segment', 
                         'latitude', 'longitude']].drop_duplicates('customer_id')
        
        d_customer = d_customer.merge(
            d_segment, 
            left_on='customer_segment', 
            right_on='customer_segment_name', 
            how='left'
        ).drop(columns=['customer_segment', 'customer_segment_name'])
        return d_customer

    def _inicializar_mapa_dimensiones(self, df: pd.DataFrame, geo_dims: tuple, d_segment: pd.DataFrame, d_customer: pd.DataFrame) -> dict:
        """Estructura el catálogo inicial del modelo relacional de dimensiones."""
        d_mkt, d_reg, d_cnt, d_st, d_ct = geo_dims
        return {
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

    # =========================================================================
    # MÉTODOS PRIVADOS: PROCESO DE HECHOS Y AJUSTES INCREMENTALES
    # =========================================================================

    def _construir_tablas_hechos(self, df_master: pd.DataFrame, dims: dict) -> tuple:
        """Ensambla las tablas de hechos inyectando las claves foráneas (FK)."""
        geo_bridge = dims['dim_city'].merge(dims['dim_state'], on='state_id')
        
        df_f = df_master.merge(
            geo_bridge, 
            left_on=['order_city', 'order_state'], 
            right_on=['city_name', 'state_name'],
            how='left'
        )

        df_f = df_f.merge(dims['dim_type'], left_on='type', right_on='type_name') \
                   .merge(dims['dim_order_status'], left_on='order_status', right_on='order_status_name') \
                   .merge(dims['dim_delivery_status'], left_on='delivery_status', right_on='delivery_status_name') \
                   .merge(dims['dim_shipping_mode'], left_on='shipping_mode', right_on='shipping_mode_name')
        
        f_order = df_f[['order_id', 'customer_id', 'city_id', 'order_date_dateorders', 'shipping_date_dateorders', 
                        'type_id', 'order_status_id', 'delivery_status_id', 'shipping_mode_id', 'late_delivery_risk']].drop_duplicates('order_id')
        
        f_item = df_f[['order_item_id', 'order_id', 'product_card_id', 'order_item_quantity', 'sales', 'order_item_discount_rate', 'order_item_discount',
                       'order_item_total', 'benefit_per_order', 'profit_ratio_calc']].rename(columns={'product_card_id': 'product_id'})
        f_item = f_item.drop_duplicates(subset=['order_item_id'])
        
        return f_order, f_item

    def _resolver_unicidad_incremental(self, f_order: pd.DataFrame, f_item: pd.DataFrame) -> dict:
        """
        [MODIFICADO PARA EL DIAGRAMA]
        Encapsula la mutación determinística de IDs para cargas incrementales.
        Recibe las estructuras transaccionales directamente y devuelve el catálogo filtrado.
        """
        print("⚠️ Ajustando IDs para carga incremental (Concatenación Determinística)...")
        
        now_incremental = datetime.now()
        fecha_prefijo = now_incremental.strftime("%y%m%d") 
        segundos_dia = (now_incremental.hour * 3600) + (now_incremental.minute * 60) + now_incremental.second
        segundos_prefijo = f"{segundos_dia:05d}" 
        prefijo_ejecucion = f"{fecha_prefijo}{segundos_prefijo}" 
        
        # Copias explícitas para mitigar el SettingWithCopyWarning de Pandas
        f_order_inc = f_order.copy()
        f_item_inc = f_item.copy()
        
        # Mutación determinística para BIGINT seguro en base de datos
        f_order_inc['order_id'] = (prefijo_ejecucion + f_order_inc['order_id'].astype(str)).astype(int)
        f_item_inc['order_id'] = (prefijo_ejecucion + f_item_inc['order_id'].astype(str)).astype(int)
        f_item_inc['order_item_id'] = (prefijo_ejecucion + f_item_inc['order_item_id'].astype(str)).astype(int)

        # Regala cohesión: En incremental solo se devuelven tablas de hechos (fact_)
        return {
            'fact_order': f_order_inc,
            'fact_item_order': f_item_inc
        }

    # =========================================================================
    # INTERFAZ PÚBLICA: ORQUESTADOR COMPONENTE DE CAPA
    # =========================================================================

    def transformar_a_silver(self, df_bronze: pd.DataFrame, tipo_carga: str) -> dict:
        """
        [MODIFICADO - SOPORTE HÍBRIDO GLOBAL/INCREMENTAL]
        Punto de Entrada del Caso de Uso: Convierte el DataFrame de la capa Bronze.
        Mapea el DataFrame delegando su conversión analítica según la estrategia configurada.
        """
        print(f"--- Transformando datos a Capa Silver ({tipo_carga}) ---")
        
        # Fase 1: Calidad de datos secuencial
        df = self._estandarizar_nombres_columnas(df_bronze)
        df = df.drop_duplicates(subset=['order_item_id'])
        df = self._filtrar_outliers_financieros(df)
        df = self._normalizar_tipos_y_textos(df)
        
        # Fase 2: Modelado de Dimensiones
        geo_dims = self._generar_jerarquia_geografica(df)
        d_segment = self._crear_lookup_table(df, 'customer_segment', 'customer_segment_id', 'customer_segment_name')
        d_customer = self._construir_dimension_clientes(df, d_segment)
        tablas_silver = self._inicializar_mapa_dimensionses(df, geo_dims, d_segment, d_customer)
        
        # Fase 3: Modelado de Hechos
        f_order, f_item = self._construir_tablas_hechos(df, tablas_silver)
        
        # Fase 4: Desvío de estrategia de arquitectura
        if tipo_carga == 'INCREMENTAL':
            # Ejecuta el flujo exacto que modela el Diagrama de Secuencia Lineal
            tablas_silver_final = self._resolver_unicidad_incremental(f_order, f_item)
            return tablas_silver_final
        
        # Flujo alternativo para carga GLOBAL: Mantiene dimensiones + agrega hechos intactos
        tablas_silver['fact_order'] = f_order
        tablas_silver['fact_item_order'] = f_item
        
        return tablas_silver
