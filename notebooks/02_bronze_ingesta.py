# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa BRONZE — Ingesta de datos crudos
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC **Regla de Bronze:** guardar los datos *tal como llegan*, sin limpiar nada.
# MAGIC Leemos todo como texto (así no se pierde ningún dato "sucio") y solo agregamos
# MAGIC columnas de auditoría: archivo de origen y fecha/hora de ingesta.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
BRONZE = f"{CATALOGO}.banco_bronze"
RUTA_LANDING = f"/Volumes/{CATALOGO}/banco_bronze/landing"
AUTOR = "Diego Vallejo"

def guardar_tabla(df, nombre_completo, descripcion):
    """Guarda un DataFrame como tabla Delta y le pone descripción + autor."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(nombre_completo))
    spark.sql(f"COMMENT ON TABLE {nombre_completo} IS '{descripcion} | Desarrollado por {AUTOR}'")
    spark.sql(f"ALTER TABLE {nombre_completo} SET TBLPROPERTIES ('autor' = '{AUTOR}')")

# COMMAND ----------

TABLAS = ["sucursales", "clientes", "cuentas", "transacciones", "prestamos"]

for t in TABLAS:
    crudo = (spark.read.format("csv")
             .option("header", "true")
             .option("inferSchema", "false")      # todo como texto: Bronze no interpreta
             .option("encoding", "UTF-8")
             .load(f"{RUTA_LANDING}/{t}.csv"))
    bronze = (crudo
              .withColumn("_archivo_origen", F.col("_metadata.file_path"))
              .withColumn("_ingesta_ts", F.current_timestamp()))
    guardar_tabla(bronze, f"{BRONZE}.{t}", f"Bronze - {t} crudo desde CSV")
    print(f"✅ {BRONZE}.{t}: {spark.table(f'{BRONZE}.{t}').count():,} filas")
