import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
import logging
import pandas as pd
from adicionales import detectar_valores_atipicos_iqr

sns.set(style="whitegrid")

def ejecutar_eda_ubicaciones(df: pl.DataFrame):
    logging.info("Análisis exploratorio de ubicaciones iniciado")

    # Estadísticas descriptivas
    print(df.describe())

    df = df.with_columns([
        pl.col("lat").abs().alias("lat_absoluta"),  # Ejemplo de operación vectorizada
        pl.col("long").abs().alias("long_absoluta"),
    ])

    # Visualización de datos
    df_pandas = df.to_pandas()

    plt.figure(figsize=(12, 5))
    sns.histplot(df_pandas["lat"], kde=True, bins=100)
    plt.title("Distribución de latitudes")
    plt.xlabel("Latitud")
    plt.savefig("histograma_latitudes.png")
    plt.close()

    plt.figure(figsize=(12, 5))
    sns.histplot(df_pandas["long"], kde=True, bins=100, color="orange")
    plt.title("Distribución de longitudes")
    plt.xlabel("Longitud")
    plt.savefig("histograma_longitudes.png")
    plt.close()

    # Detección de valores atípicos
    atipicos_lat = detectar_valores_atipicos_iqr(df_pandas["lat"])
    atipicos_long = detectar_valores_atipicos_iqr(df_pandas["long"])
    logging.info(f"Valores atípicos en latitud: {len(atipicos_lat)}, Valores atípicos en longitud: {len(atipicos_long)}")

def ejecutar_eda_usuarios(df: pl.DataFrame):
    logging.info("Análisis exploratorio de usuarios iniciado")

    # Convertir a pandas para análisis más flexible
    df_pandas = df.to_pandas()

    # Cantidad de vecinos por usuario
    df_pandas["numero_vecinos"] = df_pandas["lista_adyacencia"].apply(lambda x: len(x.split()) if x else 0)

    print(df_pandas["numero_vecinos"].describe())

    # Visualización de datos
    plt.figure(figsize=(12, 5))
    sns.histplot(df_pandas["numero_vecinos"], bins=100, kde=True)
    plt.title("Distribución del número de vecinos por usuario")
    plt.xlabel("Cantidad de vecinos")
    plt.savefig("histograma_vecinos_usuarios.png")
    plt.close()

    # Detección de valores atípicos
    atipicos_vecinos = detectar_valores_atipicos_iqr(df_pandas["numero_vecinos"])
    logging.info(f"Valores atípicos en número de vecinos: {len(atipicos_vecinos)}")