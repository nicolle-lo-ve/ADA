from cargador import cargar_datos_ubicaciones, cargar_datos_usuarios
from eda import ejecutar_eda_ubicaciones, ejecutar_eda_usuarios
from grafo import crear_grafo_desde_datos
from analizador_grafo import (
    analizar_distribucion_grados, 
    encontrar_nodos_importantes,
    analizar_camino_mas_corto_promedio,
    detectar_comunidades_louvain
)
import logging
import polars as pl

# Configurar Polars para usar todos los hilos de la CPU
pl.Config.set_tbl_rows(10)
pl.Config.set_fmt_str_lengths(80)
pl.Config.set_streaming_chunk_size(100000) # Aumentar para IO más rápido

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def principal():
    # Cargar archivos de datos
    df_ubicaciones = cargar_datos_ubicaciones("10_million_location.txt")
    df_usuarios = cargar_datos_usuarios("10_million_user.txt")

    # Ejecutar análisis exploratorio de datos (opcional, puede comentarse para rapidez)
    if df_ubicaciones is not None:
        ejecutar_eda_ubicaciones(df_ubicaciones)
    if df_usuarios is not None:
        ejecutar_eda_usuarios(df_usuarios)
    
    # Crear y analizar grafo
    if df_ubicaciones is not None and df_usuarios is not None:
        grafo = crear_grafo_desde_datos(df_ubicaciones, df_usuarios)
        
        # --- Análisis del Grafo ---
        
        # 1. Distribución de Grados (comprensión básica de la red)
        stats_grados = analizar_distribucion_grados(grafo)
        logging.info(f"Estadísticas de grados: {stats_grados}")
        
        # 2. Centralidad de Grado (nodos más conectados)
        nodos_importantes = encontrar_nodos_importantes(grafo, top_n=15)
        logging.info(f"Top 15 nodos por grado: {nodos_importantes}")
        
        # 3. Análisis de Camino Más Corto (qué tan 'pequeño' es el mundo)
        # Usamos una muestra pequeña porque es computacionalmente intensivo.
        camino_promedio = analizar_camino_mas_corto_promedio(grafo, muestra=500)
        logging.info(f"Longitud promedio del camino más corto (aproximada): {camino_promedio:.2f}")

        # 4. Detección de Comunidades (encontrar 'grupos' o 'clusters')
        # El algoritmo de Louvain es eficiente, pero aún puede tardar en un grafo de este tamaño.
        comunidades = detectar_comunidades_louvain(grafo, pases=2)
        # El detalle de las comunidades se guarda en un log y una imagen.
        
        # Liberar memoria explícitamente
        del grafo
        del comunidades

if __name__ == "__main__":
    principal()
