"""
Prueba local del pipeline completo (notebooks 00 -> 05 + consultas del dashboard).
Proyecto Banco Andino · Desarrollado por Diego Vallejo

Ejecuta los notebooks .py tal cual, con un SparkSession local (PySpark 4.0, ANSI activo,
igual que el cómputo serverless de Databricks). Diferencias locales respecto a Databricks:
  * No hay Unity Catalog ni Volumes: el catálogo es `spark_catalog` y /Volumes/... es una carpeta normal.
  * Las tablas se guardan como Parquet en vez de Delta si los JAR de Delta no están disponibles.

Uso:  python tests/run_local.py
"""
import os, re, shutil, sys, time
from pyspark.sql import SparkSession, DataFrameWriter

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WH = "/tmp/wh_banco"
shutil.rmtree(WH, ignore_errors=True)
shutil.rmtree("/Volumes/spark_catalog", ignore_errors=True)
os.makedirs("/Volumes/spark_catalog/banco_bronze/landing", exist_ok=True)

spark_real = (SparkSession.builder.master("local[4]").appName("banco-andino-test")
              .config("spark.sql.warehouse.dir", WH).config("spark.sql.shuffle.partitions", "4")
              .config("spark.ui.enabled", "false").getOrCreate())
spark_real.sparkContext.setLogLevel("ERROR")

# Parquet en lugar de Delta (solo para la prueba local)
_orig_format = DataFrameWriter.format
DataFrameWriter.format = lambda self, f: _orig_format(self, "parquet" if f == "delta" else f)

class SparkProxy:
    def __init__(self, s): self._s = s
    def sql(self, q, *a, **k):
        if q.strip().upper().startswith("CREATE VOLUME"):
            print("   (local) se omite CREATE VOLUME:", q[:60], "..."); return None
        if q.strip().upper().startswith("ALTER TABLE") and "TBLPROPERTIES" in q.upper():
            return None   # TBLPROPERTIES sobre Parquet no aplica localmente
        return self._s.sql(q, *a, **k)
    def __getattr__(self, n): return getattr(self._s, n)

spark = SparkProxy(spark_real)
t0 = time.time()
for nb in ["00_setup", "01_generar_datos", "02_bronze_ingesta", "03_silver_limpieza", "04_gold_modelo", "05_validacion"]:
    print(f"\n{'='*20} {nb} {'='*20}")
    codigo = open(os.path.join(RAIZ, "notebooks", nb + ".py"), encoding="utf-8").read()
    exec(compile(codigo, nb, "exec"), {"spark": spark, "__name__": "__main__"})

print(f"\n{'='*20} CONSULTAS DEL DASHBOARD {'='*20}")
sql = open(os.path.join(RAIZ, "sql", "dashboard_datasets.sql"), encoding="utf-8").read()
for bloque in [b for b in sql.split(";") if "DATASET:" in b]:
    nombre = re.search(r"DATASET:\s*(\w+)", bloque).group(1)
    consulta = re.sub(r"--[^\n]*", "", bloque.replace("workspace.", "spark_catalog.")).strip()
    df = spark_real.sql(consulta)
    print(f"✅ {nombre:18s} filas={df.count():>6,}  columnas={len(df.columns)}")
print(f"\nPrueba completa en {time.time()-t0:.0f} s")
