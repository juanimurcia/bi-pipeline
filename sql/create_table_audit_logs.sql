CREATE TABLE IF NOT EXISTS audit_logs (

    log_id SERIAL PRIMARY KEY,

    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    tipo_carga VARCHAR(20), -- 'GLOBAL' o 'INCREMENTAL'

    estado VARCHAR(20),      -- 'SUCCESS' o 'FAILED'

    filas_insertadas INT8,   -- Total de registros en fact_order

    mensaje_error TEXT       -- Para guardar el traceback si algo falla

);
