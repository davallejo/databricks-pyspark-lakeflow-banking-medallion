# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup del proyecto Banco Andino
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC Este notebook crea la "infraestructura" de datos en Unity Catalog:
# MAGIC * 3 esquemas (carpetas de tablas): `banco_bronze`, `banco_silver`, `banco_gold`
# MAGIC * 1 volumen (carpeta de archivos) llamado `landing`, donde "aterrizan" los CSV crudos

# COMMAND ----------

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
print(f"✅ Tu catálogo de trabajo es: {CATALOGO}")

for esquema, descripcion in [
    ("banco_bronze", "Capa Bronze: datos crudos tal como llegan. Desarrollado por Diego Vallejo"),
    ("banco_silver", "Capa Silver: datos limpios y validados. Desarrollado por Diego Vallejo"),
    ("banco_gold",   "Capa Gold: modelo analítico para el dashboard. Desarrollado por Diego Vallejo"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema} COMMENT '{descripcion}'")
    print(f"✅ Esquema listo: {CATALOGO}.{esquema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.banco_bronze.landing COMMENT 'Archivos CSV crudos - Banco Andino (Diego Vallejo)'")
print(f"✅ Volumen listo: /Volumes/{CATALOGO}/banco_bronze/landing")
