import polars as pl
import logging
from typing import Dict, List, Tuple, Optional
import numpy as np
from dataclasses import dataclass
import time
import gc
from collections import deque

@dataclass
class Nodo:
    id: int
    lat: float
    long: float

class Grafo:
    """
    Implementación manual de grafo optimizada para 10 millones de nodos.
    Utiliza estructuras eficientes en memoria y acceso rápido.
    """
    
    def __init__(self):
        self.nodos: Dict[int, Nodo] = {}
        self.adyacencia: Dict[int, List[int]] = {}
        self._indice_ubicacion: Optional[np.ndarray] = None
    
    def agregar_nodo(self, id_nodo: int, lat: float, long: float):
        """Añade un nodo al grafo con sus coordenadas."""
        if id_nodo not in self.nodos:
            self.nodos[id_nodo] = Nodo(id=id_nodo, lat=lat, long=long)
            self.adyacencia[id_nodo] = []
    
    def agregar_arista(self, origen: int, destino: int):
        """Añade una arista dirigida entre dos nodos."""
        if origen in self.adyacencia and destino in self.nodos:
            self.adyacencia[origen].append(destino)
    
    def construir_indice_ubicacion(self):
        """Construye un índice numpy para búsquedas rápidas por ubicación."""
        start = time.time()
        # Asegurarse de que los nodos están ordenados por ID para un mapeo consistente
        nodos_ordenados = sorted(self.nodos.values(), key=lambda n: n.id)
        coords = np.array([(nodo.lat, nodo.long) for nodo in nodos_ordenados], dtype=np.float32)
        self._indice_ubicacion = coords
        self._id_map = [nodo.id for nodo in nodos_ordenados] # Mapeo de índice a ID
        logging.info(f"Índice de ubicación construido en {time.time() - start:.2f}s")
    
    def vecinos_cercanos(self, lat: float, long: float, radio: float, max_vecinos: int = 10) -> List[int]:
        """
        Encuentra vecinos cercanos usando el índice de ubicación.
        
        Args:
            lat: Latitud del punto de referencia
            long: Longitud del punto de referencia
            radio: Radio de búsqueda en grados
            max_vecinos: Máximo número de vecinos a retornar
            
        Returns:
            Lista de IDs de nodos dentro del radio
        """
        if self._indice_ubicacion is None:
            self.construir_indice_ubicacion()
        
        punto = np.array([lat, long], dtype=np.float32)
        distancias = np.linalg.norm(self._indice_ubicacion - punto, axis=1)
        indices_cercanos = np.where(distancias <= radio)[0]
        
        if len(indices_cercanos) > max_vecinos:
            orden = np.argsort(distancias[indices_cercanos])[:max_vecinos]
            indices_cercanos = indices_cercanos[orden]
        
        return [self._id_map[i] for i in indices_cercanos]
    
    def grado_nodo(self, id_nodo: int) -> int:
        """Retorna el grado de salida de un nodo."""
        return len(self.adyacencia.get(id_nodo, []))

    def bfs(self, nodo_inicio: int) -> Dict[int, int]:
        """
        Realiza una Búsqueda en Amplitud (BFS) para encontrar el camino más corto
        desde un nodo de inicio a todos los demás nodos alcanzables.

        Args:
            nodo_inicio: El ID del nodo desde el cual comenzar la búsqueda.

        Returns:
            Un diccionario mapeando cada nodo alcanzable a su distancia (longitud del camino)
            desde el nodo de inicio.
        """
        if nodo_inicio not in self.nodos:
            return {}

        distancias = {nodo_inicio: 0}
        cola = deque([nodo_inicio])

        while cola:
            nodo_actual = cola.popleft()

            for vecino in self.adyacencia.get(nodo_actual, []):
                if vecino not in distancias:
                    distancias[vecino] = distancias[nodo_actual] + 1
                    cola.append(vecino)
        
        return distancias

    def limpiar_memoria(self):
        """Libera memoria de estructuras grandes."""
        self._indice_ubicacion = None
        self._id_map = None
        gc.collect()

def crear_grafo_desde_datos(df_ubicaciones: pl.DataFrame, df_usuarios: pl.DataFrame) -> Grafo:
    """
    Construye un grafo a partir de los DataFrames de ubicaciones y usuarios.
    
    Args:
        df_ubicaciones: DataFrame con columnas 'lat' y 'long'
        df_usuarios: DataFrame con columna 'lista_adyacencia' (formato: "id1 id2 id3...")
        
    Returns:
        Instancia de Grafo poblada con los datos
    """
    start_time = time.time()
    grafo = Grafo()
    
    # Procesar ubicaciones y agregar nodos
    logging.info("Agregando nodos al grafo...")
    for i in range(df_ubicaciones.height):
        row = df_ubicaciones.row(i, named=True)
        grafo.agregar_nodo(id_nodo=i, lat=row['lat'], long=row['long'])
    
    # Procesar usuarios (conexiones/aristas)
    logging.info("Agregando aristas al grafo...")
    listas_adyacencia = df_usuarios['lista_adyacencia'].to_list()
    
    for idx, ady_str in enumerate(listas_adyacencia):
        if idx in grafo.nodos:
            conexiones = [int(x) for x in ady_str.split() if x.isdigit()]
            for conexion in conexiones:
                if conexion in grafo.nodos:  # Validar que el nodo destino exista
                    grafo.agregar_arista(origen=idx, destino=conexion)
    
    total_aristas = sum(len(v) for v in grafo.adyacencia.values())
    logging.info(f"Grafo creado con {len(grafo.nodos)} nodos y {total_aristas} aristas en {time.time() - start_time:.2f}s")
    return grafo
