import polars as pl
import logging
from typing import Dict, List, Tuple, Optional
import numpy as np
from dataclasses import dataclass
import time
import gc
from collections import deque, defaultdict

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
        self.total_aristas = 0
    
    def agregar_nodo(self, id_nodo: int, lat: float, long: float):
        """Añade un nodo al grafo con sus coordenadas."""
        if id_nodo not in self.nodos:
            self.nodos[id_nodo] = Nodo(id=id_nodo, lat=lat, long=long)
            self.adyacencia[id_nodo] = []
    
    def agregar_arista(self, origen: int, destino: int):
        """Añade una arista dirigida entre dos nodos."""
        # Verificación simplificada - solo verificar que origen existe en adyacencia
        if origen in self.adyacencia:
            # Usar set para evitar duplicados automáticamente, luego convertir a lista
            if destino not in self.adyacencia[origen]:
                self.adyacencia[origen].append(destino)
                self.total_aristas += 1
                return True
        return False
    
    def agregar_aristas_lote(self, origen: int, destinos: List[int]):
        """Añade múltiples aristas desde un nodo origen de forma optimizada."""
        if origen in self.adyacencia:
            # Usar set para eliminar duplicados de forma eficiente
            destinos_actuales = set(self.adyacencia[origen])
            destinos_nuevos = []
            
            for destino in destinos:
                if destino not in destinos_actuales:
                    destinos_nuevos.append(destino)
                    destinos_actuales.add(destino)
            
            if destinos_nuevos:
                self.adyacencia[origen].extend(destinos_nuevos)
                self.total_aristas += len(destinos_nuevos)
                return len(destinos_nuevos)
        return 0
    
    def construir_indice_ubicacion(self):
        """Construye un índice numpy para búsquedas rápidas por ubicación."""
        start = time.time()
        nodos_ordenados = sorted(self.nodos.values(), key=lambda n: n.id)
        coords = np.array([(nodo.lat, nodo.long) for nodo in nodos_ordenados], dtype=np.float32)
        self._indice_ubicacion = coords
        self._id_map = [nodo.id for nodo in nodos_ordenados]
        logging.info(f"Índice de ubicación construido en {time.time() - start:.2f}s")
    
    def vecinos_cercanos(self, lat: float, long: float, radio: float, max_vecinos: int = 10) -> List[int]:
        """Encuentra vecinos cercanos usando el índice de ubicación."""
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
    
    def grado_entrada(self, id_nodo: int) -> int:
        """Retorna el grado de entrada de un nodo (cuántos nodos apuntan a él)."""
        contador = 0
        for vecinos in self.adyacencia.values():
            if id_nodo in vecinos:
                contador += 1
        return contador

    def bfs(self, nodo_inicio: int) -> Dict[int, int]:
        """Realiza una Búsqueda en Amplitud (BFS) para encontrar el camino más corto."""
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

    def estadisticas_basicas(self):
        """Retorna estadísticas básicas del grafo."""
        grados = [self.grado_nodo(nodo) for nodo in self.nodos.keys()]
        
        return {
            'num_nodos': len(self.nodos),
            'num_aristas': self.total_aristas,
            'grado_promedio': np.mean(grados) if grados else 0,
            'grado_max': max(grados) if grados else 0,
            'grado_min': min(grados) if grados else 0,
            'nodos_aislados': sum(1 for g in grados if g == 0)
        }

    def limpiar_memoria(self):
        """Libera memoria de estructuras grandes."""
        self._indice_ubicacion = None
        self._id_map = None
        gc.collect()

def crear_grafo_desde_datos(df_ubicaciones: pl.DataFrame, df_usuarios: pl.DataFrame) -> Grafo:
    """
    VERSIÓN ULTRA-OPTIMIZADA para 10 millones de nodos.
    Reduce significativamente el tiempo de construcción del grafo.
    """
    start_time = time.time()
    grafo = Grafo()
    
    # === PASO 1: Pre-inicializar todas las estructuras ===
    logging.info("Pre-inicializando estructuras del grafo...")
    num_nodos = df_ubicaciones.height
    
    # Pre-crear todos los nodos y listas de adyacencia de una vez
    ubicaciones_array = df_ubicaciones.to_numpy()
    
    # Inicialización en lote para mejor rendimiento
    for i in range(num_nodos):
        nodo_id = i + 1
        lat, long = ubicaciones_array[i]
        grafo.nodos[nodo_id] = Nodo(id=nodo_id, lat=lat, long=long)
        grafo.adyacencia[nodo_id] = []
    
    logging.info(f"Pre-inicialización completada: {len(grafo.nodos)} nodos en {time.time() - start_time:.2f}s")
    
    # === PASO 2: Procesamiento optimizado de aristas ===
    logging.info("Procesando aristas en lotes optimizados...")
    
    aristas_agregadas = 0
    aristas_fallidas = 0
    nodos_sin_conexiones = 0
    
    # Obtener todas las listas de adyacencia como array numpy para mejor rendimiento
    listas_adyacencia = df_usuarios['lista_adyacencia'].to_list()
    
    # Procesamiento en lotes
    batch_size = 50000  # Procesar en lotes de 50k nodos
    tiempo_procesamiento = time.time()
    
    for batch_start in range(0, len(listas_adyacencia), batch_size):
        batch_end = min(batch_start + batch_size, len(listas_adyacencia))
        
        for idx in range(batch_start, batch_end):
            linea = listas_adyacencia[idx]
            
            if not linea.strip():  # Línea vacía
                nodos_sin_conexiones += 1
                continue
                
            origen = idx + 1  # Indexación desde 1
            
            try:
                # Parseo optimizado
                if ', ' in linea:
                    elementos_str = linea.split(', ')
                elif ',' in linea:
                    elementos_str = linea.split(',')
                else:
                    elementos_str = linea.split()
                
                # Conversión y filtrado optimizado usando comprensión de listas
                destinos_validos = []
                for elem_str in elementos_str:
                    elem_str = elem_str.strip()
                    if elem_str.isdigit():
                        destino = int(elem_str)
                        if 1 <= destino <= num_nodos:
                            destinos_validos.append(destino)
                        else:
                            aristas_fallidas += 1
                
                if destinos_validos:
                    # Usar el método optimizado de lote
                    agregadas = grafo.agregar_aristas_lote(origen, destinos_validos)
                    aristas_agregadas += agregadas
                    if agregadas != len(destinos_validos):
                        aristas_fallidas += len(destinos_validos) - agregadas
                else:
                    nodos_sin_conexiones += 1
                    
            except Exception as e:
                logging.warning(f"Error procesando línea {idx+1}: {e}")
                nodos_sin_conexiones += 1
                continue
        
        # Log de progreso cada lote
        tiempo_batch = time.time() - tiempo_procesamiento
        nodos_procesados = batch_end
        velocidad = nodos_procesados / tiempo_batch if tiempo_batch > 0 else 0
        
        logging.info(
            f"Lote {batch_start//batch_size + 1}: "
            f"Procesados {nodos_procesados:,}/{len(listas_adyacencia):,} nodos "
            f"({velocidad:.0f} nodos/s) - "
            f"Aristas: {aristas_agregadas:,}"
        )
    
    # === ESTADÍSTICAS FINALES ===
    stats = grafo.estadisticas_basicas()
    tiempo_total = time.time() - start_time
    
    logging.info(f"=== RESUMEN DE CONSTRUCCIÓN DEL GRAFO (OPTIMIZADO) ===")
    logging.info(f"Tiempo total: {tiempo_total:.2f}s")
    logging.info(f"Velocidad promedio: {len(listas_adyacencia) / tiempo_total:.0f} nodos/s")
    logging.info(f"Nodos creados: {stats['num_nodos']:,}")
    logging.info(f"Aristas agregadas exitosamente: {aristas_agregadas:,}")
    logging.info(f"Aristas fallidas: {aristas_fallidas:,}")
    logging.info(f"Nodos sin conexiones: {nodos_sin_conexiones:,}")
    logging.info(f"Grado promedio: {stats['grado_promedio']:.2f}")
    logging.info(f"Grado máximo: {stats['grado_max']:,}")
    logging.info(f"Grado mínimo: {stats['grado_min']}")
    logging.info(f"Nodos aislados: {stats['nodos_aislados']:,}")
    
    return grafo
