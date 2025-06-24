from cargador import cargar_datos_ubicaciones, cargar_datos_usuarios
from eda import ejecutar_eda_ubicaciones, ejecutar_eda_usuarios
import logging
import polars as pl

# Configurar Polars para usar todos los hilos de la CPU
pl.Config.set_tbl_rows(20)               # Límite de filas mostradas en consola
pl.Config.set_fmt_str_lengths(100)       # Longitud máxima de strings al mostrar
pl.Config.set_streaming_chunk_size(8192) # Tamaño de fragmentos en modo streaming              

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def principal():
    # Cargar archivos de datos
    df_ubicaciones = cargar_datos_ubicaciones("10_million_location.txt")
    df_usuarios = cargar_datos_usuarios("10_million_user.txt")

    # Ejecutar análisis exploratorio de datos
    if df_ubicaciones is not None:
        ejecutar_eda_ubicaciones(df_ubicaciones)
    if df_usuarios is not None:
        ejecutar_eda_usuarios(df_usuarios)

if __name__ == "__main__":
    principal()