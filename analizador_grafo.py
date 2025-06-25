import logging
import time
import random
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
from grafo import Grafo
from collections import defaultdict

sns.set(style="whitegrid")

def analizar_distribucion_grados(grafo: Grafo, muestra: int = 1000000) -> Dict[str, float]:
    """
    Analiza la distribución de grados del grafo usando una muestra para eficiencia.
    
    Args:
        grafo: Instancia de Grafo a analizar
        muestra: Tamaño de la muestra a tomar (para eficiencia con grafos grandes)
        
    Returns:
        Diccionario con estadísticas de la distribución de grados
    """
    start_time = time.time()
    logging.info(f"Analizando distribución de grados con muestra de {muestra} nodos...")
    
    # Tomar una muestra aleatoria de nodos para mayor representatividad
    todos_nodos = list(grafo.nodos.keys())
    if muestra > len(todos_nodos):
        muestra = len(todos_nodos)
    nodos_muestra = random.sample(todos_nodos, muestra)
    
    grados = [grafo.grado_nodo(nodo) for nodo in nodos_muestra]
    
    # Calcular estadísticas
    stats = {
        'media': sum(grados) / len(grados) if grados else 0,
        'maximo': max(grados) if grados else 0,
        'minimo': min(grados) if grados else 0,
        'percentil_90': sorted(grados)[int(len(grados) * 0.9)] if grados else 0
    }
    
    # Visualización
    plt.figure(figsize=(12, 6))
    sns.histplot(grados, bins=50, kde=False) # KDE puede ser lento en muestras grandes
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
    
    Args:
        grafo: Instancia de Grafo a analizar
        top_n: Número de nodos a retornar
        
    Returns:
        Lista de tuplas (id_nodo, grado) ordenada por grado descendente
    """
    start_time = time.time()
    logging.info(f"Buscando top {top_n} nodos por grado (centralidad de grado)...")
    
    # Este enfoque es eficiente, ya que no requiere calcular para todos los nodos
    grados = {nodo: grafo.grado_nodo(nodo) for nodo in grafo.nodos.keys()}
    
    # Ordenar por grado descendente
    nodos_ordenados = sorted(grados.items(), key=lambda x: x[1], reverse=True)
    
    logging.info(f"Nodos importantes identificados en {time.time() - start_time:.2f}s")
    return nodos_ordenados[:top_n]

def analizar_camino_mas_corto_promedio(grafo: Grafo, muestra: int = 1000) -> float:
    """
    Calcula la longitud promedio del camino más corto usando BFS en una muestra de nodos.
    Esta es una aproximación, ya que calcularlo para todos los pares sería computacionalmente prohibitivo (O(V*(V+E))).

    Args:
        grafo: Instancia de Grafo a analizar.
        muestra: Número de nodos de inicio para el muestreo.

    Returns:
        Longitud promedio del camino más corto en la muestra.
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
        
        # Ejecutar BFS desde el nodo de inicio
        distancias = grafo.bfs(nodo_inicio)
        
        # Sumar las longitudes de los caminos encontrados (excluyendo el propio nodo)
        for distancia in distancias.values():
            if distancia > 0:
                longitudes_totales += distancia
                caminos_contados += 1

    promedio = longitudes_totales / caminos_contados if caminos_contados > 0 else 0
    logging.info(f"Análisis de camino más corto promedio completado en {time.time() - start_time:.2f}s")
    return promedio

def detectar_comunidades_louvain(grafo: Grafo, pases: int = 2) -> Dict[int, int]:
    """
    Implementación del algoritmo de Louvain para detección de comunidades.
    Optimizado para grafos grandes.

    Args:
        grafo: Grafo a analizar.
        pases: Número de pases del algoritmo.

    Returns:
        Un diccionario que mapea cada nodo a su ID de comunidad.
    """
    start_time = time.time()
    logging.info("Iniciando detección de comunidades con el método de Louvain.")

    # 1. Inicialización: Cada nodo es su propia comunidad.
    comunidades = {nodo: nodo for nodo in grafo.nodos.keys()}
    m = sum(len(vecinos) for vecinos in grafo.adyacencia.values()) # Número total de aristas

    for pase in range(pases):
        logging.info(f"Pase {pase + 1}/{pases} del algoritmo de Louvain...")
        nodos_aleatorizados = list(grafo.nodos.keys())
        random.shuffle(nodos_aleatorizados)

        for nodo in nodos_aleatorizados:
            mejor_comunidad = comunidades[nodo]
            max_delta_q = 0

            # 2. Ganancia de Modularidad: Probar mover el nodo a comunidades vecinas.
            comunidades_vecinas = {comunidades[vecino] for vecino in grafo.adyacencia.get(nodo, [])}

            for comunidad_vecina in comunidades_vecinas:
                # Calcular el cambio en la modularidad (delta Q)
                k_i = grafo.grado_nodo(nodo)
                k_i_in = sum(1 for vecino in grafo.adyacencia.get(nodo, []) if comunidades[vecino] == comunidad_vecina)
                sum_tot = sum(grafo.grado_nodo(n) for n, c in comunidades.items() if c == comunidad_vecina)
                
                delta_q = (k_i_in - (sum_tot * k_i) / (2 * m))
                
                if delta_q > max_delta_q:
                    max_delta_q = delta_q
                    mejor_comunidad = comunidad_vecina

            # 3. Mover el nodo si la ganancia es positiva.
            comunidades[nodo] = mejor_comunidad
    
    # Post-procesamiento para obtener un resumen de las comunidades
    resumen_comunidades = defaultdict(int)
    for comunidad_id in comunidades.values():
        resumen_comunidades[comunidad_id] += 1
    
    # Filtrar comunidades muy pequeñas para un análisis más claro
    comunidades_relevantes = {k: v for k, v in resumen_comunidades.items() if v > 10}
    
    logging.info(f"Detección de comunidades Louvain completada en {time.time() - start_time:.2f}s")
    logging.info(f"Se encontraron {len(comunidades_relevantes)} comunidades con más de 10 miembros.")

    # Visualización de las comunidades más grandes
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
