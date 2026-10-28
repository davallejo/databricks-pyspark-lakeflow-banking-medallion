# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Capa GOLD — Modelo analítico para el dashboard
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC Gold = tablas **listas para consumir** en el dashboard (ya unidas y agregadas).
# MAGIC
# MAGIC | Tabla | Qué contiene |
# MAGIC |---|---|
# MAGIC | `fact_transacciones` | Cada transacción enriquecida con cliente, cuenta y sucursal |
# MAGIC | `kpi_mensual` | Indicadores por mes |
# MAGIC | `cartera_prestamos` | Cartera por producto, estado y ciudad |
# MAGIC | `resumen_sucursal` | Desempeño por sucursal |
# MAGIC | `resumen_segmento` | Comportamiento por segmento de cliente |
# MAGIC | `calidad_datos` | Filas recibidas / válidas / rechazadas por tabla |
# MAGIC | `info_proyecto` | Autoría y fecha de ejecución (se muestra en el dashboard) |

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
BRONZE, SILVER, GOLD = (f"{CATALOGO}.banco_{x}" for x in ("bronze", "silver", "gold"))
AUTOR = "Diego Vallejo"
UMBRAL_MONTO_ALTO = 5000      # transacciones >= 5.000 USD se marcan como alerta

def guardar_tabla(df, nombre_completo, descripcion):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre_completo))
    spark.sql(f"COMMENT ON TABLE {nombre_completo} IS '{descripcion} | Desarrollado por {AUTOR}'")
    spark.sql(f"ALTER TABLE {nombre_completo} SET TBLPROPERTIES ('autor' = '{AUTOR}')")

ETIQ_TIPO = {"DEPOSITO": "Depósito", "RETIRO": "Retiro", "TRANSFERENCIA_ENVIADA": "Transferencia enviada",
             "TRANSFERENCIA_RECIBIDA": "Transferencia recibida", "PAGO_SERVICIO": "Pago de servicio",
             "COMPRA_TARJETA": "Compra con tarjeta", "PAGO_PRESTAMO": "Pago de préstamo"}
ETIQ_CANAL = {"APP_MOVIL": "App móvil", "WEB": "Web", "CAJERO": "Cajero", "VENTANILLA": "Ventanilla", "POS": "POS"}

def etiqueta(col, diccionario):
    """Convierte códigos técnicos (APP_MOVIL) en etiquetas legibles (App móvil) para el dashboard."""
    expr = None
    for codigo, texto in diccionario.items():
        expr = F.when(col == codigo, F.lit(texto)) if expr is None else expr.when(col == codigo, F.lit(texto))
    return expr.otherwise(col)

tx  = spark.table(f"{SILVER}.transacciones")
cta = spark.table(f"{SILVER}.cuentas")
cli = spark.table(f"{SILVER}.clientes")
suc = spark.table(f"{SILVER}.sucursales")
pre = spark.table(f"{SILVER}.prestamos")

# COMMAND ----------

# MAGIC %md ## 4.1 fact_transacciones (tabla ancha para filtrar libremente en el dashboard)

# COMMAND ----------

fact = (tx.alias("t")
  .join(cta.alias("a").select("cuenta_id", "cliente_id", "sucursal_id", "tipo_cuenta"), "cuenta_id")
  .join(cli.alias("c").select("cliente_id", "segmento", "genero", "fecha_nacimiento"), "cliente_id")
  .join(suc.alias("s").select("sucursal_id", "nombre_sucursal", "ciudad", "provincia", "region"), "sucursal_id")
  .select(
      "tx_id", "cuenta_id", "cliente_id",
      F.col("fecha_hora"),
      F.to_date("fecha_hora").alias("fecha"),
      F.date_format("fecha_hora", "yyyy-MM").alias("anio_mes"),
      F.hour("fecha_hora").alias("hora"),
      F.dayofweek("fecha_hora").alias("dia_semana_num"),
      F.expr("elt(dayofweek(fecha_hora), '7-Dom','1-Lun','2-Mar','3-Mié','4-Jue','5-Vie','6-Sáb')").alias("dia_semana"),
      etiqueta(F.col("tipo"), ETIQ_TIPO).alias("tipo_transaccion"),
      F.when(F.col("tipo").isin("DEPOSITO", "TRANSFERENCIA_RECIBIDA"), "Ingreso").otherwise("Egreso").alias("flujo"),
      etiqueta(F.col("canal"), ETIQ_CANAL).alias("canal"),
      F.col("categoria_comercio"),
      F.col("monto"),
      (F.col("monto") >= UMBRAL_MONTO_ALTO).alias("es_alerta_monto_alto"),
      "tipo_cuenta", "segmento", "genero",
      F.expr("""CASE WHEN floor(months_between(fecha_hora, fecha_nacimiento)/12) < 26 THEN '18-25'
                     WHEN floor(months_between(fecha_hora, fecha_nacimiento)/12) < 36 THEN '26-35'
                     WHEN floor(months_between(fecha_hora, fecha_nacimiento)/12) < 51 THEN '36-50'
                     WHEN floor(months_between(fecha_hora, fecha_nacimiento)/12) < 66 THEN '51-65'
                     ELSE '66+' END""").alias("rango_edad"),
      "sucursal_id", "nombre_sucursal", "ciudad", "provincia", "region"))
guardar_tabla(fact, f"{GOLD}.fact_transacciones", "Gold - hecho de transacciones enriquecido")
print("✅ fact_transacciones:", spark.table(f"{GOLD}.fact_transacciones").count())

# COMMAND ----------

# MAGIC %md ## 4.2 kpi_mensual

# COMMAND ----------

f = spark.table(f"{GOLD}.fact_transacciones")
kpi = (f.groupBy("anio_mes").agg(
        F.count("*").alias("n_transacciones"),
        F.countDistinct("cliente_id").alias("clientes_activos"),
        F.round(F.sum("monto"), 2).alias("monto_total"),
        F.round(F.sum(F.when(F.col("flujo") == "Ingreso", F.col("monto")).otherwise(0)), 2).alias("ingresos"),
        F.round(F.sum(F.when(F.col("flujo") == "Egreso", F.col("monto")).otherwise(0)), 2).alias("egresos"),
        F.round(F.avg("monto"), 2).alias("ticket_promedio"),
        F.sum(F.col("es_alerta_monto_alto").cast("int")).alias("n_alertas_monto_alto"))
       .withColumn("flujo_neto", F.round(F.col("ingresos") - F.col("egresos"), 2))
       .orderBy("anio_mes"))
guardar_tabla(kpi, f"{GOLD}.kpi_mensual", "Gold - KPIs mensuales de transaccionalidad")
print("✅ kpi_mensual")

# COMMAND ----------

# MAGIC %md ## 4.3 cartera_prestamos

# COMMAND ----------

cartera = (pre.join(cli.select("cliente_id", "ciudad", "segmento"), "cliente_id")
  .groupBy("producto", "estado", "ciudad", "segmento").agg(
      F.count("*").alias("n_prestamos"),
      F.round(F.sum("monto_aprobado"), 2).alias("monto_aprobado"),
      F.round(F.sum("saldo_capital"), 2).alias("saldo_capital"),
      F.round(F.avg("tasa_anual"), 2).alias("tasa_promedio"),
      F.round(F.avg("dias_mora"), 1).alias("dias_mora_promedio")))
guardar_tabla(cartera, f"{GOLD}.cartera_prestamos", "Gold - cartera de préstamos por producto/estado/ciudad/segmento")
print("✅ cartera_prestamos")

# COMMAND ----------

# MAGIC %md ## 4.4 resumen_sucursal y resumen_segmento

# COMMAND ----------

por_suc_cta = (cta.groupBy("sucursal_id").agg(
    F.count("*").alias("n_cuentas"), F.countDistinct("cliente_id").alias("n_clientes"),
    F.round(F.sum("saldo_inicial"), 2).alias("saldo_inicial_total")))
por_suc_tx = (f.groupBy("sucursal_id").agg(
    F.count("*").alias("n_transacciones"), F.round(F.sum("monto"), 2).alias("monto_transaccionado")))
res_suc = (suc.select("sucursal_id", "nombre_sucursal", "ciudad", "provincia", "region")
              .join(por_suc_cta, "sucursal_id", "left").join(por_suc_tx, "sucursal_id", "left")
              .na.fill(0))
guardar_tabla(res_suc, f"{GOLD}.resumen_sucursal", "Gold - desempeño por sucursal")

res_seg = (cli.groupBy("segmento").agg(
              F.count("*").alias("n_clientes"),
              F.round(F.avg("ingreso_mensual"), 2).alias("ingreso_promedio"))
           .join(f.groupBy("segmento").agg(
              F.count("*").alias("n_transacciones"),
              F.round(F.sum("monto"), 2).alias("monto_transaccionado"),
              F.round(F.avg("monto"), 2).alias("ticket_promedio")), "segmento", "left"))
guardar_tabla(res_seg, f"{GOLD}.resumen_segmento", "Gold - comportamiento por segmento de cliente")
print("✅ resumen_sucursal, resumen_segmento")

# COMMAND ----------

# MAGIC %md ## 4.5 calidad_datos e info_proyecto

# COMMAND ----------

bronce = None
for nombre in ["clientes", "cuentas", "transacciones", "prestamos"]:
    r = spark.table(f"{BRONZE}.{nombre}").agg(F.count("*").alias("filas_bronze")) \
             .withColumn("tabla", F.lit(nombre))
    bronce = r if bronce is None else bronce.unionByName(r)
silver_cnt = None
for nombre in ["clientes", "cuentas", "transacciones", "prestamos"]:
    r = spark.table(f"{SILVER}.{nombre}").agg(F.count("*").alias("filas_silver")) \
             .withColumn("tabla", F.lit(nombre))
    silver_cnt = r if silver_cnt is None else silver_cnt.unionByName(r)
rech = (spark.table(f"{SILVER}.rechazados").groupBy(F.col("tabla_origen").alias("tabla"))
        .agg(F.count("*").alias("filas_rechazadas")))
calidad = (bronce.join(silver_cnt, "tabla").join(rech, "tabla", "left").na.fill(0)
           .withColumn("pct_valido", F.round(F.col("filas_silver") / F.col("filas_bronze") * 100, 2))
           .select("tabla", "filas_bronze", "filas_silver", "filas_rechazadas", "pct_valido"))
guardar_tabla(calidad, f"{GOLD}.calidad_datos", "Gold - calidad de datos por tabla")

# Detalle de rechazos por motivo (para el gráfico de calidad)
motivos = (spark.table(f"{SILVER}.rechazados").groupBy("tabla_origen", "motivo")
           .agg(F.count("*").alias("n_registros")))
guardar_tabla(motivos, f"{GOLD}.rechazos_por_motivo", "Gold - rechazos por tabla y motivo")

info = spark.createDataFrame(
    [("Banco Andino - Analítica de Datos (proyecto demo)", AUTOR, "1.0", "Datos 100% ficticios")],
    ["proyecto", "autor", "version", "nota"]).withColumn("ultima_ejecucion", F.current_timestamp())
guardar_tabla(info, f"{GOLD}.info_proyecto", "Gold - autoría y metadatos del proyecto")
print("✅ calidad_datos, rechazos_por_motivo, info_proyecto")
