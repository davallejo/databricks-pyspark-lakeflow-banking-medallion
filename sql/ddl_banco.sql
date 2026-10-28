-- DDL del proyecto Banco Andino (Unity Catalog)
-- Generado: 2026-10-28 por Diego Vallejo
-- Los datos se regeneran ejecutando los notebooks notebooks/00-05 en orden.

-- ============ ESQUEMA banco_bronze (5 tablas) ============

-- banco_bronze.clientes: 2,040 filas
CREATE SCHEMA IF NOT EXISTS banco_bronze;
CREATE TABLE workspace.banco_bronze.clientes (
  cliente_id STRING COLLATE UTF8_BINARY,
  cedula STRING COLLATE UTF8_BINARY,
  nombres STRING COLLATE UTF8_BINARY,
  apellidos STRING COLLATE UTF8_BINARY,
  fecha_nacimiento STRING COLLATE UTF8_BINARY,
  genero STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  segmento STRING COLLATE UTF8_BINARY,
  ingreso_mensual STRING COLLATE UTF8_BINARY,
  fecha_alta STRING COLLATE UTF8_BINARY,
  email STRING COLLATE UTF8_BINARY,
  estado STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Bronze - clientes crudo desde CSV | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_bronze.cuentas: 3,342 filas
CREATE SCHEMA IF NOT EXISTS banco_bronze;
CREATE TABLE workspace.banco_bronze.cuentas (
  cuenta_id STRING COLLATE UTF8_BINARY,
  cliente_id STRING COLLATE UTF8_BINARY,
  sucursal_id STRING COLLATE UTF8_BINARY,
  tipo_cuenta STRING COLLATE UTF8_BINARY,
  fecha_apertura STRING COLLATE UTF8_BINARY,
  saldo_inicial STRING COLLATE UTF8_BINARY,
  estado STRING COLLATE UTF8_BINARY,
  moneda STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Bronze - cuentas crudo desde CSV | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_bronze.prestamos: 710 filas
CREATE SCHEMA IF NOT EXISTS banco_bronze;
CREATE TABLE workspace.banco_bronze.prestamos (
  prestamo_id STRING COLLATE UTF8_BINARY,
  cliente_id STRING COLLATE UTF8_BINARY,
  producto STRING COLLATE UTF8_BINARY,
  monto_aprobado STRING COLLATE UTF8_BINARY,
  plazo_meses STRING COLLATE UTF8_BINARY,
  tasa_anual STRING COLLATE UTF8_BINARY,
  fecha_desembolso STRING COLLATE UTF8_BINARY,
  estado STRING COLLATE UTF8_BINARY,
  dias_mora STRING COLLATE UTF8_BINARY,
  saldo_capital STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Bronze - prestamos crudo desde CSV | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_bronze.sucursales: 12 filas
CREATE SCHEMA IF NOT EXISTS banco_bronze;
CREATE TABLE workspace.banco_bronze.sucursales (
  sucursal_id STRING COLLATE UTF8_BINARY,
  nombre_sucursal STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  provincia STRING COLLATE UTF8_BINARY,
  region STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Bronze - sucursales crudo desde CSV | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_bronze.transacciones: 60,150 filas
CREATE SCHEMA IF NOT EXISTS banco_bronze;
CREATE TABLE workspace.banco_bronze.transacciones (
  tx_id STRING COLLATE UTF8_BINARY,
  cuenta_id STRING COLLATE UTF8_BINARY,
  fecha_hora STRING COLLATE UTF8_BINARY,
  tipo STRING COLLATE UTF8_BINARY,
  canal STRING COLLATE UTF8_BINARY,
  monto STRING COLLATE UTF8_BINARY,
  categoria_comercio STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Bronze - transacciones crudo desde CSV | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- ============ ESQUEMA banco_silver (6 tablas) ============

-- banco_silver.clientes: 1,994 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.clientes (
  cliente_id STRING COLLATE UTF8_BINARY,
  cedula STRING COLLATE UTF8_BINARY,
  nombres STRING COLLATE UTF8_BINARY,
  apellidos STRING COLLATE UTF8_BINARY,
  fecha_nacimiento DATE,
  genero STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  segmento STRING COLLATE UTF8_BINARY,
  ingreso_mensual DECIMAL(12,2),
  fecha_alta DATE,
  email STRING COLLATE UTF8_BINARY,
  estado STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Silver - clientes limpios | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_silver.cuentas: 3,321 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.cuentas (
  cliente_id STRING COLLATE UTF8_BINARY,
  cuenta_id STRING COLLATE UTF8_BINARY,
  sucursal_id STRING COLLATE UTF8_BINARY,
  tipo_cuenta STRING COLLATE UTF8_BINARY,
  fecha_apertura DATE,
  saldo_inicial DECIMAL(14,2),
  estado STRING COLLATE UTF8_BINARY,
  moneda STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Silver - cuentas validadas | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_silver.prestamos: 680 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.prestamos (
  cliente_id STRING COLLATE UTF8_BINARY,
  prestamo_id STRING COLLATE UTF8_BINARY,
  producto STRING COLLATE UTF8_BINARY,
  monto_aprobado DECIMAL(14,2),
  plazo_meses INT,
  tasa_anual DECIMAL(5,2),
  fecha_desembolso DATE,
  estado STRING COLLATE UTF8_BINARY,
  dias_mora INT,
  saldo_capital DECIMAL(14,2),
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Silver - préstamos validados | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_silver.rechazados: 677 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.rechazados (
  tabla_origen STRING COLLATE UTF8_BINARY,
  llave STRING COLLATE UTF8_BINARY,
  motivo STRING COLLATE UTF8_BINARY,
  registro_json STRING COLLATE UTF8_BINARY,
  fecha_proceso TIMESTAMP)
USING delta
COMMENT 'Silver - registros rechazados con motivo (cuarentena) | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_silver.sucursales: 12 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.sucursales (
  sucursal_id STRING COLLATE UTF8_BINARY,
  nombre_sucursal STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  provincia STRING COLLATE UTF8_BINARY,
  region STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Silver - sucursales | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_silver.transacciones: 59,570 filas
CREATE SCHEMA IF NOT EXISTS banco_silver;
CREATE TABLE workspace.banco_silver.transacciones (
  cuenta_id STRING COLLATE UTF8_BINARY,
  tx_id STRING COLLATE UTF8_BINARY,
  fecha_hora TIMESTAMP,
  tipo STRING COLLATE UTF8_BINARY,
  canal STRING COLLATE UTF8_BINARY,
  monto DECIMAL(14,2),
  categoria_comercio STRING COLLATE UTF8_BINARY,
  _archivo_origen STRING COLLATE UTF8_BINARY,
  _ingesta_ts TIMESTAMP)
USING delta
COMMENT 'Silver - transacciones validadas | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- ============ ESQUEMA banco_gold (8 tablas) ============

-- banco_gold.calidad_datos: 4 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.calidad_datos (
  tabla STRING COLLATE UTF8_BINARY,
  filas_bronze BIGINT,
  filas_silver BIGINT,
  filas_rechazadas BIGINT,
  pct_valido DOUBLE)
USING delta
COMMENT 'Gold - calidad de datos por tabla | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.cartera_prestamos: 246 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.cartera_prestamos (
  producto STRING COLLATE UTF8_BINARY,
  estado STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  segmento STRING COLLATE UTF8_BINARY,
  n_prestamos BIGINT,
  monto_aprobado DECIMAL(25,2),
  saldo_capital DECIMAL(25,2),
  tasa_promedio DECIMAL(6,2),
  dias_mora_promedio DOUBLE)
USING delta
COMMENT 'Gold - cartera de préstamos por producto/estado/ciudad/segmento | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.fact_transacciones: 59,570 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.fact_transacciones (
  tx_id STRING COLLATE UTF8_BINARY,
  cuenta_id STRING COLLATE UTF8_BINARY,
  cliente_id STRING COLLATE UTF8_BINARY,
  fecha_hora TIMESTAMP,
  fecha DATE,
  anio_mes STRING COLLATE UTF8_BINARY,
  hora INT,
  dia_semana_num INT,
  dia_semana STRING COLLATE UTF8_BINARY,
  tipo_transaccion STRING COLLATE UTF8_BINARY,
  flujo STRING COLLATE UTF8_BINARY,
  canal STRING COLLATE UTF8_BINARY,
  categoria_comercio STRING COLLATE UTF8_BINARY,
  monto DECIMAL(14,2),
  es_alerta_monto_alto BOOLEAN,
  tipo_cuenta STRING COLLATE UTF8_BINARY,
  segmento STRING COLLATE UTF8_BINARY,
  genero STRING COLLATE UTF8_BINARY,
  rango_edad STRING COLLATE UTF8_BINARY,
  sucursal_id STRING COLLATE UTF8_BINARY,
  nombre_sucursal STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  provincia STRING COLLATE UTF8_BINARY,
  region STRING COLLATE UTF8_BINARY)
USING delta
COMMENT 'Gold - hecho de transacciones enriquecido | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.info_proyecto: 1 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.info_proyecto (
  proyecto STRING COLLATE UTF8_BINARY,
  autor STRING COLLATE UTF8_BINARY,
  version STRING COLLATE UTF8_BINARY,
  nota STRING COLLATE UTF8_BINARY,
  ultima_ejecucion TIMESTAMP)
USING delta
COMMENT 'Gold - autoría y metadatos del proyecto | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.kpi_mensual: 9 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.kpi_mensual (
  anio_mes STRING COLLATE UTF8_BINARY,
  n_transacciones BIGINT,
  clientes_activos BIGINT,
  monto_total DECIMAL(25,2),
  ingresos DECIMAL(25,2),
  egresos DECIMAL(25,2),
  ticket_promedio DECIMAL(15,2),
  n_alertas_monto_alto BIGINT,
  flujo_neto DECIMAL(27,2))
USING delta
COMMENT 'Gold - KPIs mensuales de transaccionalidad | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.rechazos_por_motivo: 10 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.rechazos_por_motivo (
  tabla_origen STRING COLLATE UTF8_BINARY,
  motivo STRING COLLATE UTF8_BINARY,
  n_registros BIGINT)
USING delta
COMMENT 'Gold - rechazos por tabla y motivo | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.resumen_segmento: 4 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.resumen_segmento (
  segmento STRING COLLATE UTF8_BINARY,
  n_clientes BIGINT,
  ingreso_promedio DECIMAL(13,2),
  n_transacciones BIGINT,
  monto_transaccionado DECIMAL(25,2),
  ticket_promedio DECIMAL(15,2))
USING delta
COMMENT 'Gold - comportamiento por segmento de cliente | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- banco_gold.resumen_sucursal: 12 filas
CREATE SCHEMA IF NOT EXISTS banco_gold;
CREATE TABLE workspace.banco_gold.resumen_sucursal (
  sucursal_id STRING COLLATE UTF8_BINARY,
  nombre_sucursal STRING COLLATE UTF8_BINARY,
  ciudad STRING COLLATE UTF8_BINARY,
  provincia STRING COLLATE UTF8_BINARY,
  region STRING COLLATE UTF8_BINARY,
  n_cuentas BIGINT,
  n_clientes BIGINT,
  saldo_inicial_total DECIMAL(25,2),
  n_transacciones BIGINT,
  monto_transaccionado DECIMAL(25,2))
USING delta
COMMENT 'Gold - desempeño por sucursal | Desarrollado por Diego Vallejo'
TBLPROPERTIES (
  'autor' = 'Diego Vallejo',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;
