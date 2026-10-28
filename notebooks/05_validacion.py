# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Validación final del pipeline
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC Comprueba que todo cuadra antes de construir el dashboard. Si algo falla, el notebook se detiene con un error claro.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
BRONZE, SILVER, GOLD = (f"{CATALOGO}.banco_{x}" for x in ("bronze", "silver", "gold"))
n = lambda tabla: spark.table(tabla).count()

# 1) Conciliación: lo que entra a Bronze = lo que queda en Silver + lo rechazado
for t in ["clientes", "cuentas", "transacciones", "prestamos"]:
    b, s = n(f"{BRONZE}.{t}"), n(f"{SILVER}.{t}")
    r = spark.table(f"{SILVER}.rechazados").filter(F.col("tabla_origen") == t).count()
    assert b == s + r, f"❌ {t}: bronze={b} != silver={s} + rechazados={r}"
    print(f"✅ Conciliación {t:14s} bronze={b:>7,} = silver={s:>7,} + rechazados={r:>5,}")

# 2) Sin duplicados en Silver
for t, k in [("clientes", "cliente_id"), ("cuentas", "cuenta_id"), ("transacciones", "tx_id"), ("prestamos", "prestamo_id")]:
    d = spark.table(f"{SILVER}.{t}").groupBy(k).count().filter("count > 1").count()
    assert d == 0, f"❌ {t} tiene {d} llaves duplicadas"
print("✅ Sin duplicados en Silver")

# 3) Reglas de negocio en Silver
assert spark.table(f"{SILVER}.transacciones").filter("monto <= 0 OR monto IS NULL OR fecha_hora IS NULL").count() == 0
assert spark.table(f"{SILVER}.clientes").filter("segmento NOT IN ('Masivo','Preferente','Premium','Empresarial')").count() == 0
print("✅ Reglas de negocio cumplidas")

# 4) Gold coherente con Silver
assert n(f"{GOLD}.fact_transacciones") == n(f"{SILVER}.transacciones"), "❌ fact_transacciones no coincide con Silver"
tot_fact = spark.table(f"{GOLD}.fact_transacciones").agg(F.sum("monto")).first()[0]
tot_kpi = spark.table(f"{GOLD}.kpi_mensual").agg(F.sum("monto_total")).first()[0]
assert abs(float(tot_fact) - float(tot_kpi)) < 1, "❌ kpi_mensual no suma igual que fact_transacciones"
print(f"✅ Gold coherente (monto total = {float(tot_fact):,.2f} USD)")

# 5) Autoría
autor = spark.table(f"{GOLD}.info_proyecto").first()["autor"]
assert autor == "Diego Vallejo"
print(f"✅ Autoría registrada: {autor}")

print(f"\n🎉 VALIDACIÓN COMPLETA: el pipeline está listo para el dashboard.")
