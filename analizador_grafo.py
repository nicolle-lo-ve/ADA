import logging
import time
import random
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
from grafo import Grafo
from collections import defaultdict

sns.set(style="whitegrid")

# (Las funciones analizar_distribucion_grados, encontrar_nodos_importantes y 
# analizar_camino_mas_corto_promedio permanecen sin cambios)

def analizar_distribucion_grados(grafo: Grafo, muestra: int = 1000000) -> Dict[str, float]:
    """
    Analiza la distribución de grados del grafo usando una muestra para eficiencia.
    """
    start_time = time.time()
    logging.info(f"Analizando distribución de grados con muestra de {muestra} nodos...")
    
    todos_nodos = list(grafo.nodos.keys())
    if muestra > len(todos_nodos):
        muestra = len(todos_nodos)
    nodos_muestra = random.sample(todos_nodos, muestra)
    
    grados = [grafo.grado_nodo(nodo) for nodo in nodos_muestra]
    
    stats = {
        'media': sum(grados) / len(grados) if grados else 0,
        'maximo': max(grados) if grados else 0,
        'minimo': min(grados) if grados else 0,
        'percentil_90': sorted(grados)[int(len(grados) * 0.9)] if grados else 0
    }
    
    plt.figure(figsize=(12, 6))
    sns.histplot(grados, bins=50, kde=False)
    plt.title(f"Distribución de Grados (Muestra: {muestra} nodos)")
    plt.xlabel("Grado del nodo")
    plt.ylabel("Frecuencia")
    plt.yscale('log')
    plt.savefig("distribucion_grados.png")
    plt.close()
    
    logging.info(f"Análisis de grados completado en {time.time() - start_time:.2f}s")
    return stats

def encontrar_nodos_importantes(grafo: Grafo, top_n: int = 10) -> List[tuple[int, int]]:
    """
    Encuentra los nodos con mayor grado en el grafo. Es una heurística de centralidad.
    """
    start_time = time.time()
    logging.info(f"Buscando top {top_n} nodos por grado (centralidad de grado)...")
    
    grados = {nodo: grafo.grado_nodo(nodo) for nodo in grafo.nodos.keys()}
    nodos_ordenados = sorted(grados.items(), key=lambda x: x[1], reverse=True)
    
    logging.info(f"Nodos importantes identificados en {time.time() - start_time:.2f}s")
    return nodos_ordenados[:top_n]

def analizar_camino_mas_corto_promedio(grafo: Grafo, muestra: int = 1000) -> float:
    """
    Calcula la longitud promedio del camino más corto usando BFS en una muestra de nodos.
    """
    start_time = time.time()
    logging.info(f"Iniciando análisis de camino más corto promedio con muestra de {muestra} nodos.")
    
    todos_nodos = list(grafo.nodos.keys())
    if muestra > len(todos_nodos):
        muestra = len(todos_nodos)
    nodos_muestra = random.sample(todos_nodos, muestra)

    longitudes_totales = 0
    caminos_contados = 0

    for i, nodo_inicio in enumerate(nodos_muestra):
        if (i + 1) % 100 == 0:
            logging.info(f"Procesando BFS desde el nodo {i+1}/{muestra}...")
        
        distancias = grafo.bfs(nodo_inicio)
        
        for distancia in distancias.values():
            if distancia > 0:
                longitudes_totales += distancia
                caminos_contados += 1

    promedio = longitudes_totales / caminos_contados if caminos_contados > 0 else 0
    logging.info(f"Análisis de camino más corto promedio completado en {time.time() - start_time:.2f}s")
    return promedio

def detectar_comunidades_louvain(grafo: Grafo, pases: int = 1) -> Dict[int, int]:
    """
    Implementación OPTIMIZADA del algoritmo de Louvain para detección de comunidades.
    Args:
        grafo: Grafo a analizar.
        pases: Número de pases del algoritmo.
    Returns:
        Un diccionario que mapea cada nodo a su ID de comunidad.
    """
    start_time = time.time()
    logging.info("Iniciando detección de comunidades con el método de Louvain (OPTIMIZADO).")

    # 1. Inicialización eficaz
    comunidades = {nodo: nodo for nodo in grafo.nodos.keys()}
    
    # Pre-calcular el grado de cada nodo para no llamarlo repetidamente en bucles.
    grados_nodos = {nodo: grafo.grado_nodo(nodo) for nodo in grafo.nodos.keys()}
    
    # Pre-calcular y mantener el total de grados para cada comunidad.
    # Esta es la optimización clave.
    grados_comunidad = grados_nodos.copy()
    
    # Calcular 2*m (el doble del número de aristas) una sola vez.
    m_doble = sum(grados_nodos.values())
    if m_doble == 0:
        logging.warning("El grafo no tiene aristas. No se pueden detectar comunidades.")
        return comunidades

    for pase in range(pases):
        logging.info(f"Pase {pase + 1}/{pases} del algoritmo de Louvain...")
        nodos_aleatorizados = list(grafo.nodos.keys())
        random.shuffle(nodos_aleatorizados)
        movimientos_realizados = 0

        for nodo in nodos_aleatorizados:
            comunidad_actual = comunidades[nodo]
            mejor_comunidad = comunidad_actual
            max_delta_q = 0.0

            # Contar los enlaces desde 'nodo' a las comunidades de sus vecinos.
            enlaces_a_comunidades = defaultdict(int)
            for vecino in grafo.adyacencia.get(nodo, []):
                enlaces_a_comunidades[comunidades[vecino]] += 1
            
            k_i = grados_nodos[nodo]

            # Quitar (temporalmente) el nodo de su comunidad para el cálculo de modularidad.
            grados_comunidad[comunidad_actual] -= k_i

            for comunidad_vecina, k_i_in in enlaces_a_comunidades.items():
                # sum_tot es ahora una búsqueda O(1) en el diccionario pre-calculado.
                sum_tot = grados_comunidad.get(comunidad_vecina, 0)
                
                # Calcular la ganancia de modularidad.
                delta_q = k_i_in - (sum_tot * k_i) / m_doble

                if delta_q > max_delta_q:
                    max_delta_q = delta_q
                    mejor_comunidad = comunidad_vecina
            
            # Si se encontró una mejor comunidad, mover el nodo permanentemente.
            if mejor_comunidad != comunidad_actual:
                comunidades[nodo] = mejor_comunidad
                # Actualizar el diccionario de grados de la nueva comunidad.
                grados_comunidad[mejor_comunidad] += k_i
                movimientos_realizados += 1
            else:
                # Si no se movió, restaurar el grado a su comunidad original.
                grados_comunidad[comunidad_actual] += k_i
        
        logging.info(f"Pase {pase + 1} completado. Se realizaron {movimientos_realizados} movimientos.")
        if movimientos_realizados == 0:
            logging.info("La modularidad ha convergido, no se requieren más pases.")
            break # Si no hay movimientos en un pase, el algoritmo ha convergido.

    # El post-procesamiento y la visualización permanecen igual
    resumen_comunidades = defaultdict(int)
    for comunidad_id in comunidades.values():
        resumen_comunidades[comunidad_id] += 1
    
    comunidades_relevantes = {k: v for k, v in resumen_comunidades.items() if v > 10}
    
    logging.info(f"Detección de comunidades Louvain completada en {time.time() - start_time:.2f}s")
    logging.info(f"Se encontraron {len(comunidades_relevantes)} comunidades con más de 10 miembros.")

    top_comunidades = sorted(comunidades_relevantes.items(), key=lambda x: x[1], reverse=True)[:15]
    if top_comunidades:
        ids, tamanos = zip(*top_comunidades)
        plt.figure(figsize=(12, 6))
        sns.barplot(x=list(map(str, ids)), y=list(tamanos))
        plt.title("Top 15 Comunidades por Tamaño")
        plt.xlabel("ID de Comunidad (arbitrario)")
        plt.ylabel("Número de Nodos")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("distribucion_comunidades.png")
        plt.close()

    return comunidades
