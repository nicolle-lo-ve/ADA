import polars as pl
import os
import logging

def cargar_datos_ubicaciones(ruta_archivo: str):
    try:
        logging.info(f"Cargando archivo de ubicaciones: {ruta_archivo}")
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"El archivo {ruta_archivo} no existe.")

        # Modo perezoso para optimización automática
        marco_perezoso = pl.scan_csv(
            ruta_archivo,
            has_header=False,
            new_columns=["lat", "long"]
        ).select([
            pl.col("lat").cast(pl.Float64),
            pl.col("long").cast(pl.Float64),
        ])
        
        # Ejecutar con paralelismo explícito
        df = marco_perezoso.collect(streaming=True)  # Usar streaming para conjuntos de datos muy grandes
        logging.info(f"Archivo cargado exitosamente: {df.height} registros")
        return df
    except Exception as error:
        logging.error(f"Error al cargar datos de ubicaciones: {error}")
        return None
    
def cargar_datos_usuarios(ruta_archivo: str):
    try:
        logging.info(f"Cargando archivo de usuarios: {ruta_archivo}")
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"El archivo {ruta_archivo} no existe.")

        with open(ruta_archivo, "r") as archivo:
            lineas = (linea.strip() for linea in archivo)
            df = pl.DataFrame({"lista_adyacencia": list(lineas)})

        logging.info(f"Archivo cargado exitosamente: {df.height} registros")
        return df
    except Exception as error:
        logging.error(f"Error al cargar datos de usuarios: {error}")
        return None
