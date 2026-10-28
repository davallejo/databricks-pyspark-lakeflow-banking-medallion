-- =====================================================================================
-- Banco Andino · Datasets para Databricks Dashboards
-- Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo
--
-- Cómo usar: en el dashboard, pestaña "Data" → "Create from SQL" → pega UNA consulta
-- por dataset y ponle el nombre indicado en el comentario "DATASET:".
--
-- IMPORTANTE: estas consultas usan el catálogo `workspace`. Si el notebook 00_setup te
-- mostró otro nombre de catálogo (por ejemplo `main`), reemplaza `workspace.` por `main.`
-- =====================================================================================

-- DATASET: ds_info   (para mostrar la autoría en el dashboard)
SELECT autor, proyecto, version, ultima_ejecucion
FROM workspace.banco_gold.info_proyecto;

-- DATASET: ds_totales   (contadores/tarjetas KPI)
SELECT COUNT(*)                                        AS n_transacciones,
       ROUND(SUM(monto), 2)                            AS monto_total,
       COUNT(DISTINCT cliente_id)                      AS clientes_activos,
       SUM(CAST(es_alerta_monto_alto AS INT))          AS alertas_monto_alto
FROM workspace.banco_gold.fact_transacciones;

-- DATASET: ds_mora   (índice de mora de la cartera)
SELECT ROUND(100 * SUM(CASE WHEN estado IN ('En Mora', 'Castigado') THEN saldo_capital ELSE 0 END)
             / SUM(saldo_capital), 2)                  AS indice_mora_pct,
       ROUND(SUM(saldo_capital), 2)                    AS saldo_cartera_total
FROM workspace.banco_gold.cartera_prestamos;

-- DATASET: ds_kpi_mensual   (línea: ingresos vs egresos por mes)
SELECT anio_mes, n_transacciones, clientes_activos, monto_total, ingresos, egresos,
       flujo_neto, ticket_promedio, n_alertas_monto_alto
FROM workspace.banco_gold.kpi_mensual
ORDER BY anio_mes;

-- DATASET: ds_transacciones   (tabla ancha: base para los filtros y la mayoría de gráficos)
SELECT anio_mes, fecha, hora, dia_semana, tipo_transaccion, flujo, canal, categoria_comercio,
       monto, es_alerta_monto_alto, tipo_cuenta, segmento, genero, rango_edad,
       nombre_sucursal, ciudad, provincia, region, tx_id, cliente_id, cuenta_id
FROM workspace.banco_gold.fact_transacciones;

-- DATASET: ds_alertas   (tabla: transacciones de monto alto)
SELECT tx_id, fecha_hora, tipo_transaccion, canal, monto, segmento, ciudad, nombre_sucursal
FROM workspace.banco_gold.fact_transacciones
WHERE es_alerta_monto_alto
ORDER BY monto DESC
LIMIT 50;

-- DATASET: ds_cartera   (barras: cartera por producto y estado)
SELECT producto, estado, ciudad, segmento, n_prestamos, monto_aprobado, saldo_capital,
       tasa_promedio, dias_mora_promedio
FROM workspace.banco_gold.cartera_prestamos;

-- DATASET: ds_sucursal   (barras: desempeño por sucursal)
SELECT nombre_sucursal, ciudad, region, n_cuentas, n_clientes, n_transacciones, monto_transaccionado
FROM workspace.banco_gold.resumen_sucursal
ORDER BY monto_transaccionado DESC;

-- DATASET: ds_segmento   (tabla/donut: clientes y monto por segmento)
SELECT segmento, n_clientes, ingreso_promedio, n_transacciones, monto_transaccionado, ticket_promedio
FROM workspace.banco_gold.resumen_segmento;

-- DATASET: ds_calidad   (tabla: calidad de datos por tabla)
SELECT tabla, filas_bronze, filas_silver, filas_rechazadas, pct_valido
FROM workspace.banco_gold.calidad_datos
ORDER BY tabla;

-- DATASET: ds_rechazos   (barras: por qué se rechazaron registros)
SELECT tabla_origen, motivo, n_registros
FROM workspace.banco_gold.rechazos_por_motivo
ORDER BY n_registros DESC;
