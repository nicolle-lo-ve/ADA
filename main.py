from cargador import cargar_datos_ubicaciones, cargar_datos_usuarios, analizar_formato_usuarios
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
pl.Config.set_streaming_chunk_size(100000)

# Configuración de logging más detallada
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("analisis_grafo.log"),
        logging.StreamHandler()
    ]
)

def principal():
    logging.info("=== INICIANDO ANÁLISIS DE GRAFO DE REDES SOCIALES ===")
    
    # === PASO 1: CARGAR DATOS ===
    logging.info("PASO 1: Cargando archivos de datos...")
    df_ubicaciones = cargar_datos_ubicaciones("10_million_location.txt")
    df_usuarios = cargar_datos_usuarios("10_million_user.txt")
    
    if df_ubicaciones is None or df_usuarios is None:
        logging.error("Error crítico: No se pudieron cargar los archivos de datos")
        return
    
    # === PASO 2: ANÁLISIS EXPLORATORIO (Opcional) ===
    logging.info("PASO 2: Ejecutando análisis exploratorio...")
    
    # Analizar formato de usuarios antes del EDA
    analizar_formato_usuarios(df_usuarios)
    
    try:
        ejecutar_eda_ubicaciones(df_ubicaciones)
        ejecutar_eda_usuarios(df_usuarios)
    except Exception as e:
        logging.warning(f"Error en EDA: {e}. Continuando con el análisis del grafo...")
    
    # === PASO 3: CONSTRUCCIÓN DEL GRAFO ===
    logging.info("PASO 3: Construyendo el grafo...")
    
    try:
        grafo = crear_grafo_desde_datos(df_ubicaciones, df_usuarios)
        
        # Verificar que el grafo se construyó correctamente
        stats_iniciales = grafo.estadisticas_basicas()
        
        if stats_iniciales['num_aristas'] == 0:
            logging.error("ERROR CRÍTICO: El grafo no tiene aristas. Revisa el formato de datos.")
            return
        
        if stats_iniciales['grado_promedio'] < 0.1:
            logging.warning("ADVERTENCIA: Grado promedio muy bajo. Posible problema en el formato de datos.")
        
        # === PASO 4: ANÁLISIS DEL GRAFO ===
        logging.info("PASO 4: Ejecutando análisis del grafo...")
        
        # 1. Distribución de Grados
        logging.info("4.1 - Analizando distribución de grados...")
        stats_grados = analizar_distribucion_grados(grafo, muestra=500000)  # Muestra más pequeña
        logging.info(f"Estadísticas de grados: {stats_grados}")
        
        # 2. Nodos Importantes
        logging.info("4.2 - Identificando nodos importantes...")
        nodos_importantes = encontrar_nodos_importantes(grafo, top_n=20)
        logging.info(f"Top 20 nodos por grado:")
        for i, (nodo, grado) in enumerate(nodos_importantes[:10], 1):
            logging.info(f"  {i:2d}. Nodo {nodo}: {grado} conexiones")
        
        # 3. Análisis de Conectividad
        logging.info("4.3 - Analizando conectividad...")
        if stats_iniciales['grado_promedio'] > 1:  # Solo si hay suficientes conexiones
            camino_promedio = analizar_camino_mas_corto_promedio(grafo, muestra=200)
            logging.info(f"Longitud promedio del camino más corto: {camino_promedio:.2f}")
        else:
            logging.info("Saltando análisis de caminos cortos (grafo poco conectado)")
        
        # 4. Detección de Comunidades (solo si el grafo es suficientemente denso)
        if stats_iniciales['grado_promedio'] > 1.5:
            logging.info("4.4 - Detectando comunidades...")
            comunidades = detectar_comunidades_louvain(grafo, pases=1)
            
            # Análisis de comunidades
            from collections import Counter
            tamanos_comunidades = Counter(comunidades.values())
            comunidades_grandes = {k: v for k, v in tamanos_comunidades.items() if v > 100}
            
            logging.info(f"Se encontraron {len(comunidades_grandes)} comunidades con más de 100 miembros")
            
        else:
            logging.info("4.4 - Saltando detección de comunidades (grafo poco denso)")
        
        # === RESUMEN FINAL ===
        logging.info("=== RESUMEN FINAL DEL ANÁLISIS ===")
        stats_finales = grafo.estadisticas_basicas()
        logging.info(f"Nodos totales: {stats_finales['num_nodos']:,}")
        logging.info(f"Aristas totales: {stats_finales['num_aristas']:,}")
        logging.info(f"Grado promedio: {stats_finales['grado_promedio']:.2f}")
        logging.info(f"Densidad del grafo: {stats_finales['num_aristas'] / (stats_finales['num_nodos'] * (stats_finales['num_nodos'] - 1)):.6f}")
        
        # Limpieza de memoria
        grafo.limpiar_memoria()
        del grafo
        
        logging.info("=== ANÁLISIS COMPLETADO EXITOSAMENTE ===")
        
    except Exception as e:
        logging.error(f"Error durante la construcción o análisis del grafo: {e}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    principal()
