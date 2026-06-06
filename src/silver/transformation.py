import pandas as pd
import numpy as np
from datetime import datetime

class SilverTransformer:
    def __init__(self):
        """
        Componente de la Capa Silver encargado de la limpieza, normalización
        und modelado dimensional de los datos bajo el estándar Snowflake.
        """
        pass

    # ==========================================
    # MÉTODOS PRIVADOS DE LIMPIEZA Y OPERACIONES
    # ==========================================

    def _estandarizar_nombres_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza headers a snake_case."""
        df_new = df.copy()
        df_new.columns = [col.lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df_new.columns]
        return df_new

    def _filtrar_outliers_financieros(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica reglas de negocio para asegurar integridad financiera."""
        df_clean = df.copy()
        df_clean['profit_ratio_calc'] = np.where(
            df_clean['order_item_total'] != 0, 
            df_clean['benefit_per_order'] / df_clean['order_item_total'], 
            0
        )
        return df_clean[df_clean['profit_ratio_calc'] >= -1.0].copy()

    def _normalizar_tipos_y_textos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Casting de tipos y normalización estética de strings."""
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

    # ==========================================
    # MÉTODOS PRIVADOS DE MODELADO (DIMENSIONES)
    # ==========================================

    def _crear_lookup_table(self, df: pd.DataFrame, col_source: str, id_name: str, val_name: str) -> pd.DataFrame:
        """Crea tablas de referencia genéricas."""
        lookup = pd.DataFrame(sorted(df[col_source].unique()), columns=[val_name])
        lookup.insert(0, id_name, lookup.index + 1)
        return lookup

    def _generar_jerarquia_geografica(self, df: pd.DataFrame):
        """Construye el Snowflake Geográfico (Market -> City)."""
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

    # ==========================================
    # MÉTODOS PRIVADOS DE ENSAMBLAJE (HECHOS)
    # ==========================================

    def _construir_tablas_hechos(self, df_master: pd.DataFrame, dims: dict):
        """Versión Blindada para evitar duplicados en el DER final."""
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

    # ==========================================
    # INTERFAZ PÚBLICA (ORQUESTADOR DE CAPA)
    # ==========================================

    def transformar_a_silver(self, df_bronze: pd.DataFrame, tipo_carga: str) -> dict:
        """Punto de entrada principal que orquesta toda la transformación de la capa."""
        print("--- Transformando datos a Capa Silver ---")
        
        # 1. Limpieza 
        df = self._estandarizar_nombres_columnas(df_bronze)
        df = df.drop_duplicates(subset=['order_item_id'])
        df = self._filtrar_outliers_financieros(df)
        df = self._normalizar_tipos_y_textos(df)
        
        # 2. Dimensiones Geográficas
        d_mkt, d_reg, d_cnt, d_st, d_ct = self._generar_jerarquia_geografica(df)
        
        # Dimensiones de Entidad
        d_segment = self._crear_lookup_table(df, 'customer_segment', 'customer_segment_id', 'customer_segment_name')
        
        d_customer = df[['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 
                         'customer_street', 'customer_zipcode', 'customer_segment', 
                         'latitude', 'longitude']].drop_duplicates('customer_id')
        
        d_customer = d_customer.merge(
            d_segment, 
            left_on='customer_segment', 
            right_on='customer_segment_name', 
            how='left'
        ).drop(columns=['customer_segment', 'customer_segment_name'])
        
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
        
        # 3. Hechos
        f_order, f_item = self._construir_tablas_hechos(df, tablas_silver)
        tablas_silver['fact_order'] = f_order
        tablas_silver['fact_item_order'] = f_item
        
        # 4. Lógica de Involución/Ajuste Estructural por Tipo de Carga (Encapsulado en Silver)
        if tipo_carga == 'INCREMENTAL':
            print("⚠️ Ajustando IDs para carga incremental (Concatenación Determinística)...")
            
            now_incremental = datetime.now()
            fecha_prefijo = now_incremental.strftime("%y%m%d") 
            segundos_dia = (now_incremental.hour * 3600) + (now_incremental.minute * 60) + now_incremental.second
            segundos_prefijo = f"{segundos_dia:05d}" 
            prefijo_ejecucion = f"{fecha_prefijo}{segundos_prefijo}" 
            
            # Filtramos el diccionario para mantener solo las tablas de hechos según lógica del negocio
            tablas_silver = {k: v for k, v in tablas_silver.items() if k.startswith('fact_')}
            
            if 'fact_order' in tablas_silver:
                tablas_silver['fact_order']['order_id'] = (prefijo_ejecucion + tablas_silver['fact_order']['order_id'].astype(str)).astype(int)
            
            if 'fact_item_order' in tablas_silver:
                tablas_silver['fact_item_order']['order_id'] = (prefijo_ejecucion + tablas_silver['fact_item_order']['order_id'].astype(str)).astype(int)
                tablas_silver['fact_item_order']['order_item_id'] = (prefijo_ejecucion + tablas_silver['fact_item_order']['order_item_id'].astype(str)).astype(int)

        return tablas_silver
