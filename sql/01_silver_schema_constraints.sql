-- ============================================================
-- 1. DEFINICIÓN DE PRIMARY KEYS (PK)
-- ============================================================

-- Geografía
ALTER TABLE dim_market ADD PRIMARY KEY (market_id);
ALTER TABLE dim_region ADD PRIMARY KEY (region_id);
ALTER TABLE dim_country ADD PRIMARY KEY (country_id);
ALTER TABLE dim_state ADD PRIMARY KEY (state_id);
ALTER TABLE dim_city ADD PRIMARY KEY (city_id);

-- Producto y Categoría
ALTER TABLE dim_department ADD PRIMARY KEY (department_id);
ALTER TABLE dim_category ADD PRIMARY KEY (category_id);
ALTER TABLE dim_product ADD PRIMARY KEY (product_id);

-- Cliente y Segmento (Modificado)
ALTER TABLE dim_customer_segment ADD PRIMARY KEY (customer_segment_id);
ALTER TABLE dim_customer ADD PRIMARY KEY (customer_id);

-- Otros Lookups
ALTER TABLE dim_type ADD PRIMARY KEY (type_id);
ALTER TABLE dim_order_status ADD PRIMARY KEY (order_status_id);
ALTER TABLE dim_delivery_status ADD PRIMARY KEY (delivery_status_id);
ALTER TABLE dim_shipping_mode ADD PRIMARY KEY (shipping_mode_id);

-- Hechos
ALTER TABLE fact_order ADD PRIMARY KEY (order_id);
ALTER TABLE fact_item_order ADD PRIMARY KEY (order_item_id);

-- ============================================================
-- 2. DEFINICIÓN DE FOREIGN KEYS (FK)
-- ============================================================

-- Snowflake Geográfico
ALTER TABLE dim_region ADD CONSTRAINT fk_region_market FOREIGN KEY (market_id) REFERENCES dim_market(market_id);
ALTER TABLE dim_country ADD CONSTRAINT fk_country_region FOREIGN KEY (region_id) REFERENCES dim_region(region_id);
ALTER TABLE dim_state ADD CONSTRAINT fk_state_country FOREIGN KEY (country_id) REFERENCES dim_country(country_id);
ALTER TABLE dim_city ADD CONSTRAINT fk_city_state FOREIGN KEY (state_id) REFERENCES dim_state(state_id);

-- Snowflake de Producto
ALTER TABLE dim_category ADD CONSTRAINT fk_category_dept FOREIGN KEY (department_id) REFERENCES dim_department(department_id);
ALTER TABLE dim_product ADD CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES dim_category(category_id);

-- Snowflake de Cliente
ALTER TABLE dim_customer 
ADD CONSTRAINT fk_customer_segment 
FOREIGN KEY (customer_segment_id) REFERENCES dim_customer_segment(customer_segment_id);

-- Conexiones de fact_order (La Estrella)
ALTER TABLE fact_order ADD CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id);
ALTER TABLE fact_order ADD CONSTRAINT fk_order_city FOREIGN KEY (city_id) REFERENCES dim_city(city_id);
ALTER TABLE fact_order ADD CONSTRAINT fk_order_type FOREIGN KEY (type_id) REFERENCES dim_type(type_id);
ALTER TABLE fact_order ADD CONSTRAINT fk_order_status FOREIGN KEY (order_status_id) REFERENCES dim_order_status(order_status_id);
ALTER TABLE fact_order ADD CONSTRAINT fk_order_delivery FOREIGN KEY (delivery_status_id) REFERENCES dim_delivery_status(delivery_status_id);
ALTER TABLE fact_order ADD CONSTRAINT fk_order_shipping FOREIGN KEY (shipping_mode_id) REFERENCES dim_shipping_mode(shipping_mode_id);

-- Conexiones de fact_item_order
ALTER TABLE fact_item_order ADD CONSTRAINT fk_item_order_parent FOREIGN KEY (order_id) REFERENCES fact_order(order_id);
ALTER TABLE fact_item_order ADD CONSTRAINT fk_item_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id);
