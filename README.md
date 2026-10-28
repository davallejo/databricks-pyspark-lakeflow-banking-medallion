# Banco Andino · Pipeline Medallion en Databricks

**Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**

Pipeline de datos bancarios de extremo a extremo con Arquitectura Medallion (Bronze → Silver → Gold), PySpark, orquestación con Lakeflow Jobs y gobernanza con Unity Catalog, más un dashboard ejecutivo en Databricks AI/BI.

> Datos 100% ficticios generados con semilla fija (reproducibles).

## Estructura del repositorio

| Carpeta | Contenido |
|---|---|
| `notebooks/` | Pipeline completo: setup, generación de datos, ingesta Bronze, limpieza Silver, modelo Gold y validación |
| `data_generation/` | Generación local de los CSV sin Databricks |
| `sql/` | `dashboard_datasets.sql` (datasets del dashboard) y `ddl_banco.sql` (DDL de las tablas de Unity Catalog) |
| `tests/` | Pruebas locales del pipeline |
| `docs/img/` | Capturas del dashboard y del job |
| `dashboards/` | Dashboard ejecutivo (Lakeview, `.lvdash.json`) |

## Cómo reproducir

Ejecutar los notebooks en orden sobre un clúster/serverless de Databricks:

1. `00_setup.py` — crea esquemas `banco_bronze`, `banco_silver`, `banco_gold` y el volumen `landing`
2. `01_generar_datos.py` — genera los CSV ficticios con errores intencionales de calidad
3. `02_bronze_ingesta.py` — ingesta cruda (todo como texto + columnas de auditoría)
4. `03_silver_limpieza.py` — limpieza, tipado y validación con cuarentena `banco_silver.rechazados`
5. `04_gold_modelo.py` — modelo analítico para el dashboard (`fact_transacciones`, `kpi_mensual`, etc.)
6. `05_validacion.py` — conciliación y reglas de negocio

El DDL completo de las tablas está en `sql/ddl_banco.sql`; los datos se regeneran con los notebooks.
