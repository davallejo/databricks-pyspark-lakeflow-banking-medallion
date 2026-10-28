# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generación de datos ficticios
# MAGIC **Proyecto demo de Ingeniería de Datos · Desarrollado por Diego Vallejo**
# MAGIC
# MAGIC Este notebook **crea los archivos CSV** del banco ficticio y los guarda en el volumen
# MAGIC `landing` (la "zona de aterrizaje" de la arquitectura). Los datos incluyen errores
# MAGIC intencionales (duplicados, nulos, montos negativos, fechas inválidas, claves huérfanas)
# MAGIC para que las capas Silver y Gold tengan trabajo real.

# COMMAND ----------

import os
import random

import numpy as np
import pandas as pd

SEMILLA = 42
N_CLIENTES = 2000
N_TRANSACCIONES = 60000
N_PRESTAMOS = 700
FECHA_INI_TX = "2026-01-01"
FECHA_FIN_TX = "2026-09-30"


def generar(ruta_salida):
    os.makedirs(ruta_salida, exist_ok=True)
    rng = np.random.default_rng(SEMILLA)
    random.seed(SEMILLA)

    # ------------------------------------------------------------------ SUCURSALES
    sucursales = pd.DataFrame(
        [
            ("SUC-01", "Guayaquil Centro", "Guayaquil", "Guayas", "Costa"),
            ("SUC-02", "Guayaquil Norte", "Guayaquil", "Guayas", "Costa"),
            ("SUC-03", "Quito Centro", "Quito", "Pichincha", "Sierra"),
            ("SUC-04", "Quito Norte", "Quito", "Pichincha", "Sierra"),
            ("SUC-05", "Cuenca Centro", "Cuenca", "Azuay", "Sierra"),
            ("SUC-06", "Manta Malecón", "Manta", "Manabí", "Costa"),
            ("SUC-07", "Portoviejo Centro", "Portoviejo", "Manabí", "Costa"),
            ("SUC-08", "Ambato Centro", "Ambato", "Tungurahua", "Sierra"),
            ("SUC-09", "Loja Centro", "Loja", "Loja", "Sierra"),
            ("SUC-10", "Machala Centro", "Machala", "El Oro", "Costa"),
            ("SUC-11", "Esmeraldas Centro", "Esmeraldas", "Esmeraldas", "Costa"),
            ("SUC-12", "Ibarra Centro", "Ibarra", "Imbabura", "Sierra"),
        ],
        columns=["sucursal_id", "nombre_sucursal", "ciudad", "provincia", "region"],
    )
    ciudades = sucursales["ciudad"].unique().tolist()  # 10 ciudades
    pesos_ciudad = np.array([0.30, 0.22, 0.10, 0.07, 0.06, 0.06, 0.05, 0.05, 0.05, 0.04])
    pesos_ciudad = pesos_ciudad / pesos_ciudad.sum()

    # ------------------------------------------------------------------ CLIENTES
    nombres = ["Juan", "María", "Carlos", "Ana", "Luis", "Carmen", "José", "Rosa", "Pedro", "Lucía",
               "Jorge", "Elena", "Andrés", "Patricia", "Diego", "Gabriela", "Fernando", "Sofía",
               "Ricardo", "Daniela", "Santiago", "Valeria", "Mateo", "Camila"]
    apellidos = ["Vallejo", "Mora", "Cedeño", "Zambrano", "Pérez", "Gómez", "Torres", "Andrade", "Salazar",
                 "Vera", "Castro", "Rivera", "Mendoza", "Cevallos", "Naranjo", "Guerrero", "Ortiz", "Paredes",
                 "Jaramillo", "Chávez", "Villacís", "Intriago", "Macías", "Proaño", "Lara"]
    segmentos = rng.choice(["Masivo", "Preferente", "Premium", "Empresarial"], N_CLIENTES, p=[0.60, 0.28, 0.10, 0.02])
    mediana_ingreso = {"Masivo": 650, "Preferente": 1800, "Premium": 4500, "Empresarial": 9000}
    ingreso = np.array([rng.lognormal(np.log(mediana_ingreso[s]), 0.35) for s in segmentos]).round(2)
    dias_alta = rng.integers(0, (pd.Timestamp("2026-06-30") - pd.Timestamp("2015-01-01")).days, N_CLIENTES)
    dias_nac = rng.integers(0, (pd.Timestamp("2008-12-31") - pd.Timestamp("1950-01-01")).days, N_CLIENTES)
    nom = rng.choice(nombres, N_CLIENTES)
    ape1 = rng.choice(apellidos, N_CLIENTES)
    ape2 = rng.choice(apellidos, N_CLIENTES)
    ids = [f"C{i:05d}" for i in range(1, N_CLIENTES + 1)]

    def sin_tildes(s):
        for a, b in zip("áéíóúñ", "aeioun"):
            s = s.replace(a, b)
        return s

    clientes = pd.DataFrame(
        {
            "cliente_id": ids,
            "cedula": [f"99{n:08d}" for n in rng.integers(0, 10**8, N_CLIENTES)],  # prefijo 99 = ficticia
            "nombres": nom,
            "apellidos": [f"{a} {b}" for a, b in zip(ape1, ape2)],
            "fecha_nacimiento": (pd.Timestamp("1950-01-01") + pd.to_timedelta(dias_nac, unit="D")).strftime("%Y-%m-%d"),
            "genero": rng.choice(["M", "F"], N_CLIENTES),
            "ciudad": rng.choice(ciudades, N_CLIENTES, p=pesos_ciudad),
            "segmento": segmentos,
            "ingreso_mensual": ingreso,
            "fecha_alta": (pd.Timestamp("2015-01-01") + pd.to_timedelta(dias_alta, unit="D")).strftime("%Y-%m-%d"),
            "email": [sin_tildes(f"{n.lower()}.{a.lower()}{i}@ejemplo.com")
                      for n, a, i in zip(nom, ape1, range(1, N_CLIENTES + 1))],
            "estado": rng.choice(["Activo", "Inactivo"], N_CLIENTES, p=[0.92, 0.08]),
        }
    )
    # --- errores de calidad intencionales ---
    idx = rng.choice(N_CLIENTES, 100, replace=False)
    clientes.loc[idx[:50], "segmento"] = clientes.loc[idx[:50], "segmento"].str.upper()        # PREMIUM
    clientes.loc[idx[50:100], "segmento"] = clientes.loc[idx[50:100], "segmento"].str.lower()  # premium
    idx = rng.choice(N_CLIENTES, 60, replace=False)
    clientes.loc[idx, "nombres"] = "  " + clientes.loc[idx, "nombres"].str.upper() + "  "       # espacios + mayusculas
    clientes.loc[rng.choice(N_CLIENTES, 80, replace=False), "email"] = None                    # correos nulos
    clientes.loc[rng.choice(N_CLIENTES, 8, replace=False), "ingreso_mensual"] = -100.0         # ingreso negativo
    clientes.loc[rng.choice(N_CLIENTES, 6, replace=False), "cedula"] = None                    # cedula nula
    clientes = pd.concat([clientes, clientes.sample(40, random_state=SEMILLA)], ignore_index=True)  # duplicados
    clientes = clientes.sample(frac=1, random_state=SEMILLA).reset_index(drop=True)

    # ------------------------------------------------------------------ CUENTAS
    base = clientes.drop_duplicates("cliente_id")
    filas = []
    n = 0
    for _, c in base.iterrows():
        for _ in range(int(rng.choice([1, 2, 3], p=[0.50, 0.35, 0.15]))):
            n += 1
            cand = sucursales[sucursales["ciudad"] == c["ciudad"]]["sucursal_id"].tolist()
            apertura = pd.Timestamp(c["fecha_alta"]) + pd.Timedelta(days=int(rng.integers(0, 60)))
            filas.append(
                (
                    f"A{n:06d}", c["cliente_id"], random.choice(cand),
                    str(rng.choice(["Ahorros", "Corriente"], p=[0.70, 0.30])),
                    apertura.strftime("%Y-%m-%d"),
                    round(float(rng.lognormal(np.log(900), 1.0)), 2),
                    str(rng.choice(["Activa", "Inactiva", "Cerrada"], p=[0.90, 0.07, 0.03])),
                    "USD",
                )
            )
    cuentas = pd.DataFrame(filas, columns=["cuenta_id", "cliente_id", "sucursal_id", "tipo_cuenta",
                                           "fecha_apertura", "saldo_inicial", "estado", "moneda"])
    huerfanas = cuentas.sample(12, random_state=SEMILLA).copy()  # cuentas de clientes que no existen
    huerfanas["cuenta_id"] = [f"A9{i:05d}" for i in range(12)]
    huerfanas["cliente_id"] = [f"C9{i:04d}" for i in range(12)]
    cuentas = pd.concat([cuentas, huerfanas], ignore_index=True)

    # ------------------------------------------------------------------ TRANSACCIONES
    activas = cuentas[(cuentas["estado"] != "Cerrada") & (~cuentas["cuenta_id"].str.startswith("A9"))]["cuenta_id"].to_numpy()
    peso_cta = rng.lognormal(0, 1.0, len(activas))
    peso_cta = peso_cta / peso_cta.sum()
    dias = pd.date_range(FECHA_INI_TX, FECHA_FIN_TX, freq="D")
    peso_dia = np.array([1.0 + 0.6 * (d.day in (1, 2, 15, 16, 30, 31)) - 0.3 * (d.dayofweek >= 5) for d in dias])
    peso_dia = peso_dia / peso_dia.sum()
    dia = rng.choice(len(dias), N_TRANSACCIONES, p=peso_dia)
    horas_p = np.array([1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 8, 8, 8, 7, 7, 7, 7, 8, 7, 5, 3, 2, 1], dtype=float)
    hora = rng.choice(24, N_TRANSACCIONES, p=horas_p / horas_p.sum())
    seg = rng.integers(0, 3600, N_TRANSACCIONES)
    fecha_hora = pd.to_datetime(dias[dia]) + pd.to_timedelta(hora, unit="h") + pd.to_timedelta(seg, unit="s")

    tipos = ["DEPOSITO", "RETIRO", "TRANSFERENCIA_ENVIADA", "TRANSFERENCIA_RECIBIDA",
             "PAGO_SERVICIO", "COMPRA_TARJETA", "PAGO_PRESTAMO"]
    tipo = rng.choice(tipos, N_TRANSACCIONES, p=[0.17, 0.17, 0.15, 0.15, 0.10, 0.21, 0.05])
    canales_por_tipo = {
        "DEPOSITO": (["VENTANILLA", "CAJERO", "APP_MOVIL"], [0.45, 0.35, 0.20]),
        "RETIRO": (["CAJERO", "VENTANILLA"], [0.80, 0.20]),
        "TRANSFERENCIA_ENVIADA": (["APP_MOVIL", "WEB"], [0.70, 0.30]),
        "TRANSFERENCIA_RECIBIDA": (["APP_MOVIL", "WEB"], [0.70, 0.30]),
        "PAGO_SERVICIO": (["APP_MOVIL", "WEB", "VENTANILLA"], [0.55, 0.30, 0.15]),
        "COMPRA_TARJETA": (["POS", "WEB"], [0.80, 0.20]),
        "PAGO_PRESTAMO": (["APP_MOVIL", "WEB", "VENTANILLA"], [0.50, 0.30, 0.20]),
    }
    mediana_monto = {"DEPOSITO": 300, "RETIRO": 80, "TRANSFERENCIA_ENVIADA": 150, "TRANSFERENCIA_RECIBIDA": 150,
                     "PAGO_SERVICIO": 40, "COMPRA_TARJETA": 35, "PAGO_PRESTAMO": 220}
    canal, monto, categoria = [], [], []
    cats = ["Supermercado", "Restaurantes", "Combustible", "Salud", "Tecnología", "Viajes", "Educación"]
    for t in tipo:
        opc, p = canales_por_tipo[t]
        canal.append(str(rng.choice(opc, p=p)))
        monto.append(round(float(rng.lognormal(np.log(mediana_monto[t]), 0.8)), 2))
        categoria.append(str(rng.choice(cats, p=[0.30, 0.20, 0.15, 0.10, 0.10, 0.05, 0.10])) if t == "COMPRA_TARJETA" else None)
    tx = pd.DataFrame(
        {
            "tx_id": [f"T{i:07d}" for i in range(1, N_TRANSACCIONES + 1)],
            "cuenta_id": rng.choice(activas, N_TRANSACCIONES, p=peso_cta),
            "fecha_hora": fecha_hora.strftime("%Y-%m-%d %H:%M:%S"),
            "tipo": tipo,
            "canal": canal,
            "monto": monto,
            "categoria_comercio": categoria,
        }
    )
    # montos atipicos (candidatos a alerta)
    tx.loc[rng.choice(N_TRANSACCIONES, 40, replace=False), "monto"] = rng.uniform(8000, 25000, 40).round(2)
    # --- errores de calidad intencionales ---
    malos = rng.choice(N_TRANSACCIONES, 230, replace=False)
    tx.loc[malos[:40], "monto"] = None
    tx.loc[malos[40:80], "monto"] = -tx.loc[malos[40:80], "monto"].abs()
    tx.loc[malos[80:100], "monto"] = 0.0
    tx.loc[malos[100:180], "cuenta_id"] = "A999999"                       # cuenta inexistente
    tx.loc[malos[180:230], "fecha_hora"] = rng.choice(["2026-13-45 99:00:00", "sin_fecha", ""], 50)
    tx = pd.concat([tx, tx.sample(150, random_state=SEMILLA)], ignore_index=True)  # tx duplicadas
    tx = tx.sample(frac=1, random_state=SEMILLA).reset_index(drop=True)

    # ------------------------------------------------------------------ PRESTAMOS
    clientes_ok = base[base["estado"] == "Activo"]
    elegidos = clientes_ok.sample(N_PRESTAMOS, random_state=SEMILLA, replace=False)["cliente_id"].to_numpy()
    producto = rng.choice(["Consumo", "Hipotecario", "Vehicular", "Microcrédito", "Educativo"], N_PRESTAMOS,
                          p=[0.38, 0.12, 0.18, 0.22, 0.10])
    rango_monto = {"Consumo": (1500, 15000), "Hipotecario": (40000, 150000), "Vehicular": (8000, 35000),
                   "Microcrédito": (500, 10000), "Educativo": (2000, 20000)}
    rango_tasa = {"Consumo": (15, 17), "Hipotecario": (8, 10), "Vehicular": (11, 13),
                  "Microcrédito": (20, 25), "Educativo": (9, 11)}
    plazos = {"Consumo": [12, 24, 36, 48], "Hipotecario": [120, 180, 240], "Vehicular": [36, 48, 60],
              "Microcrédito": [6, 12, 24], "Educativo": [24, 36, 48]}
    estado = rng.choice(["Vigente", "En_Mora", "Cancelado", "Castigado"], N_PRESTAMOS, p=[0.70, 0.12, 0.15, 0.03])
    monto_ap = np.array([rng.uniform(*rango_monto[p]) for p in producto]).round(2)
    saldo = np.where(estado == "Cancelado", 0.0, (monto_ap * rng.uniform(0.10, 1.0, N_PRESTAMOS)).round(2))
    dias_mora = np.where(estado == "En_Mora", rng.integers(1, 120, N_PRESTAMOS),
                         np.where(estado == "Castigado", rng.integers(120, 400, N_PRESTAMOS), 0))
    desembolso = pd.Timestamp("2022-01-01") + pd.to_timedelta(rng.integers(0, 1700, N_PRESTAMOS), unit="D")
    prestamos = pd.DataFrame(
        {
            "prestamo_id": [f"P{i:05d}" for i in range(1, N_PRESTAMOS + 1)],
            "cliente_id": elegidos,
            "producto": producto,
            "monto_aprobado": monto_ap,
            "plazo_meses": [int(rng.choice(plazos[p])) for p in producto],
            "tasa_anual": [round(float(rng.uniform(*rango_tasa[p])), 2) for p in producto],
            "fecha_desembolso": desembolso.strftime("%Y-%m-%d"),
            "estado": estado,
            "dias_mora": dias_mora,
            "saldo_capital": saldo,
        }
    )
    # --- errores de calidad intencionales ---
    prestamos.loc[rng.choice(N_PRESTAMOS, 10, replace=False), "tasa_anual"] = None
    i20 = rng.choice(N_PRESTAMOS, 20, replace=False)
    prestamos.loc[i20, "estado"] = prestamos.loc[i20, "estado"].str.lower()
    prestamos.loc[rng.choice(N_PRESTAMOS, 8, replace=False), "cliente_id"] = [f"C8{i:04d}" for i in range(8)]
    prestamos = pd.concat([prestamos, prestamos.sample(10, random_state=SEMILLA)], ignore_index=True)

    # ------------------------------------------------------------------ ESCRIBIR CSV
    for nombre, df in [("sucursales", sucursales), ("clientes", clientes), ("cuentas", cuentas),
                       ("transacciones", tx), ("prestamos", prestamos)]:
        df.to_csv(os.path.join(ruta_salida, f"{nombre}.csv"), index=False, encoding="utf-8")
        print(f"  {nombre}.csv -> {len(df):,} filas")
    print(f"Archivos creados en: {ruta_salida}")



# COMMAND ----------

CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
RUTA_LANDING = f"/Volumes/{CATALOGO}/banco_bronze/landing"
generar(RUTA_LANDING)

# COMMAND ----------

# Verificación: listamos los archivos creados en el volumen
for nombre in sorted(os.listdir(RUTA_LANDING)):
    print(f"{nombre:20s} {os.path.getsize(os.path.join(RUTA_LANDING, nombre)):>12,} bytes")
