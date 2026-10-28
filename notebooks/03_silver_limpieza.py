# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa SILVER — Limpieza y validación
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC Aquí los datos se **convierten a su tipo correcto, se estandarizan y se validan**.
# MAGIC Cada fila que no cumple una regla NO se pierde: va a la tabla de cuarentena
# MAGIC `banco_silver.rechazados` con el motivo del rechazo (auditoría).
# MAGIC
# MAGIC | Tabla | Reglas |
# MAGIC |---|---|
# MAGIC | clientes | sin duplicados · cédula obligatoria · ingreso negativo → nulo · segmento normalizado |
# MAGIC | cuentas | sin duplicados · el cliente debe existir |
# MAGIC | transacciones | sin duplicados · fecha válida · monto > 0 · la cuenta debe existir · tipo válido |
# MAGIC | prestamos | sin duplicados · el cliente debe existir · tasa válida |

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
BRONZE, SILVER = f"{CATALOGO}.banco_bronze", f"{CATALOGO}.banco_silver"
AUTOR = "Diego Vallejo"

def guardar_tabla(df, nombre_completo, descripcion):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre_completo))
    spark.sql(f"COMMENT ON TABLE {nombre_completo} IS '{descripcion} | Desarrollado por {AUTOR}'")
    spark.sql(f"ALTER TABLE {nombre_completo} SET TBLPROPERTIES ('autor' = '{AUTOR}')")

def separar(df, tabla, llave, reglas):
    """
    Evalúa las reglas EN ORDEN; la primera que falla define el motivo de rechazo.
    reglas = [("MOTIVO", "condicion_SQL_que_indica_que_el_registro_es_INVALIDO"), ...]
    Devuelve (validos, rechazados).
    """
    motivo = F.lit(None).cast("string")
    for nombre, condicion in reversed(reglas):
        motivo = F.when(F.expr(condicion), F.lit(nombre)).otherwise(motivo)
    marcado = df.withColumn("_motivo", motivo)
    cols = [c for c in df.columns if not c.startswith("_")]
    validos = marcado.filter(F.col("_motivo").isNull()).drop("_motivo")
    rechazados = (marcado.filter(F.col("_motivo").isNotNull())
                  .select(F.lit(tabla).alias("tabla_origen"),
                          F.col(llave).cast("string").alias("llave"),
                          F.col("_motivo").alias("motivo"),
                          F.to_json(F.struct(*cols)).alias("registro_json"),
                          F.current_timestamp().alias("fecha_proceso")))
    return validos, rechazados

def dup_rn(df, llave, orden="_ingesta_ts"):
    """Agrega rn: 1 = primer registro de la llave, >1 = duplicado."""
    w = Window.partitionBy(llave).orderBy(F.col(orden))
    return df.withColumn("_rn", F.row_number().over(w))

lista_rechazados = []

# COMMAND ----------

# MAGIC %md ## 3.1 Sucursales (catálogo pequeño, solo se estandariza)

# COMMAND ----------

suc = (spark.table(f"{BRONZE}.sucursales")
       .select(*[F.trim(c).alias(c) for c in ["sucursal_id", "nombre_sucursal", "ciudad", "provincia", "region"]],
               "_ingesta_ts"))
guardar_tabla(suc, f"{SILVER}.sucursales", "Silver - sucursales")
print("✅ sucursales:", suc.count())

# COMMAND ----------

# MAGIC %md ## 3.2 Clientes

# COMMAND ----------

c = spark.table(f"{BRONZE}.clientes")
c = (c.withColumn("cliente_id", F.upper(F.trim("cliente_id")))
      .withColumn("cedula", F.trim("cedula"))
      .withColumn("nombres", F.initcap(F.regexp_replace(F.trim("nombres"), r"\s+", " ")))
      .withColumn("apellidos", F.initcap(F.regexp_replace(F.trim("apellidos"), r"\s+", " ")))
      .withColumn("fecha_nacimiento", F.expr("to_date(try_to_timestamp(fecha_nacimiento, 'yyyy-MM-dd'))"))
      .withColumn("genero", F.upper(F.trim("genero")))
      .withColumn("ciudad", F.trim("ciudad"))
      .withColumn("segmento", F.initcap(F.trim("segmento")))                       # PREMIUM/premium -> Premium
      .withColumn("ingreso_mensual", F.expr("try_cast(ingreso_mensual AS DECIMAL(12,2))"))
      .withColumn("ingreso_mensual", F.when(F.col("ingreso_mensual") > 0, F.col("ingreso_mensual")))  # negativo -> nulo
      .withColumn("fecha_alta", F.expr("to_date(try_to_timestamp(fecha_alta, 'yyyy-MM-dd'))"))
      .withColumn("email", F.lower(F.trim("email")))
      .withColumn("estado", F.initcap(F.trim("estado"))))
c = dup_rn(c, "cliente_id")
c_ok, c_rej = separar(c, "clientes", "cliente_id", [
    ("DUPLICADO", "_rn > 1"),
    ("CEDULA_NULA", "cedula IS NULL OR cedula = ''"),
    ("SEGMENTO_INVALIDO", "segmento NOT IN ('Masivo','Preferente','Premium','Empresarial')"),
])
c_ok = c_ok.drop("_rn")
guardar_tabla(c_ok, f"{SILVER}.clientes", "Silver - clientes limpios")
lista_rechazados.append(c_rej)
print("✅ clientes válidos:", spark.table(f"{SILVER}.clientes").count())

# COMMAND ----------

# MAGIC %md ## 3.3 Cuentas

# COMMAND ----------

ids_clientes = spark.table(f"{SILVER}.clientes").select("cliente_id").withColumn("_cli_ok", F.lit(1))
a = spark.table(f"{BRONZE}.cuentas")
a = (a.withColumn("cuenta_id", F.upper(F.trim("cuenta_id")))
      .withColumn("cliente_id", F.upper(F.trim("cliente_id")))
      .withColumn("sucursal_id", F.trim("sucursal_id"))
      .withColumn("tipo_cuenta", F.initcap(F.trim("tipo_cuenta")))
      .withColumn("fecha_apertura", F.expr("to_date(try_to_timestamp(fecha_apertura, 'yyyy-MM-dd'))"))
      .withColumn("saldo_inicial", F.expr("try_cast(saldo_inicial AS DECIMAL(14,2))"))
      .withColumn("estado", F.initcap(F.trim("estado")))
      .withColumn("moneda", F.upper(F.trim("moneda"))))
a = dup_rn(a, "cuenta_id").join(ids_clientes, "cliente_id", "left")
a_ok, a_rej = separar(a, "cuentas", "cuenta_id", [
    ("DUPLICADO", "_rn > 1"),
    ("CLIENTE_INEXISTENTE", "_cli_ok IS NULL"),
])
a_ok = a_ok.drop("_rn", "_cli_ok")
guardar_tabla(a_ok, f"{SILVER}.cuentas", "Silver - cuentas validadas")
lista_rechazados.append(a_rej)
print("✅ cuentas válidas:", spark.table(f"{SILVER}.cuentas").count())

# COMMAND ----------

# MAGIC %md ## 3.4 Transacciones

# COMMAND ----------

ids_cuentas = spark.table(f"{SILVER}.cuentas").select("cuenta_id").withColumn("_cta_ok", F.lit(1))
t = spark.table(f"{BRONZE}.transacciones")
t = (t.withColumn("tx_id", F.upper(F.trim("tx_id")))
      .withColumn("cuenta_id", F.upper(F.trim("cuenta_id")))
      .withColumn("fecha_hora", F.expr("try_to_timestamp(fecha_hora, 'yyyy-MM-dd HH:mm:ss')"))
      .withColumn("tipo", F.upper(F.trim("tipo")))
      .withColumn("canal", F.upper(F.trim("canal")))
      .withColumn("monto", F.expr("try_cast(monto AS DECIMAL(14,2))"))
      .withColumn("categoria_comercio", F.trim("categoria_comercio")))
t = dup_rn(t, "tx_id").join(ids_cuentas, "cuenta_id", "left")
t_ok, t_rej = separar(t, "transacciones", "tx_id", [
    ("DUPLICADO", "_rn > 1"),
    ("FECHA_INVALIDA", "fecha_hora IS NULL"),
    ("MONTO_INVALIDO", "monto IS NULL OR monto <= 0"),
    ("CUENTA_INEXISTENTE", "_cta_ok IS NULL"),
    ("TIPO_INVALIDO", "tipo NOT IN ('DEPOSITO','RETIRO','TRANSFERENCIA_ENVIADA','TRANSFERENCIA_RECIBIDA','PAGO_SERVICIO','COMPRA_TARJETA','PAGO_PRESTAMO')"),
])
t_ok = t_ok.drop("_rn", "_cta_ok")
guardar_tabla(t_ok, f"{SILVER}.transacciones", "Silver - transacciones validadas")
lista_rechazados.append(t_rej)
print("✅ transacciones válidas:", spark.table(f"{SILVER}.transacciones").count())

# COMMAND ----------

# MAGIC %md ## 3.5 Préstamos

# COMMAND ----------

p = spark.table(f"{BRONZE}.prestamos")
p = (p.withColumn("prestamo_id", F.upper(F.trim("prestamo_id")))
      .withColumn("cliente_id", F.upper(F.trim("cliente_id")))
      .withColumn("producto", F.initcap(F.trim("producto")))
      .withColumn("monto_aprobado", F.expr("try_cast(monto_aprobado AS DECIMAL(14,2))"))
      .withColumn("plazo_meses", F.expr("try_cast(plazo_meses AS INT)"))
      .withColumn("tasa_anual", F.expr("try_cast(tasa_anual AS DECIMAL(5,2))"))
      .withColumn("fecha_desembolso", F.expr("to_date(try_to_timestamp(fecha_desembolso, 'yyyy-MM-dd'))"))
      .withColumn("estado", F.initcap(F.regexp_replace(F.lower(F.trim("estado")), "_", " ")))   # en_mora -> En Mora
      .withColumn("dias_mora", F.expr("try_cast(dias_mora AS INT)"))
      .withColumn("saldo_capital", F.expr("try_cast(saldo_capital AS DECIMAL(14,2))")))
p = dup_rn(p, "prestamo_id").join(ids_clientes, "cliente_id", "left")
p_ok, p_rej = separar(p, "prestamos", "prestamo_id", [
    ("DUPLICADO", "_rn > 1"),
    ("CLIENTE_INEXISTENTE", "_cli_ok IS NULL"),
    ("TASA_INVALIDA", "tasa_anual IS NULL OR tasa_anual <= 0 OR tasa_anual > 100"),
    ("MONTO_INVALIDO", "monto_aprobado IS NULL OR monto_aprobado <= 0"),
    ("ESTADO_INVALIDO", "estado NOT IN ('Vigente','En Mora','Cancelado','Castigado')"),
])
p_ok = p_ok.drop("_rn", "_cli_ok")
guardar_tabla(p_ok, f"{SILVER}.prestamos", "Silver - préstamos validados")
lista_rechazados.append(p_rej)
print("✅ préstamos válidos:", spark.table(f"{SILVER}.prestamos").count())

# COMMAND ----------

# MAGIC %md ## 3.6 Cuarentena: todo lo rechazado, con su motivo

# COMMAND ----------

from functools import reduce
rechazados = reduce(lambda x, y: x.unionByName(y), lista_rechazados)
guardar_tabla(rechazados, f"{SILVER}.rechazados", "Silver - registros rechazados con motivo (cuarentena)")
spark.table(f"{SILVER}.rechazados").groupBy("tabla_origen", "motivo").count().orderBy("tabla_origen", "motivo").show(50, truncate=False)
