import logging
import time
import random
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, DefaultDict
from collections import defaultdict
from tqdm import tqdm

sns.set(style="whitegrid")

class Louvain:
    """Implementación completa del algoritmo de Louvain para detección de comunidades."""
    
    def __init__(self, grafo):
        self.grafo = grafo
        self.nodos = list(grafo.nodos.keys())
        self.adyacencia = grafo.adyacencia
        self.comunidades = None
        self.modularidad = None
        
    def _inicializar_estructuras(self):
        """Inicializa las estructuras de datos necesarias para el algoritmo."""
        self.comunidades = {nodo: i for i, nodo in enumerate(self.nodos)}
        self.grados_nodos = {nodo: self._calcular_grado(nodo) for nodo in self.nodos}
        self.grados_comunidad = self.grados_nodos.copy()
        self.enlaces_comunidad = self._calcular_enlaces_internos_iniciales()
        
    def _calcular_grado(self, nodo):
        """Calcula el grado (pesado) de un nodo."""
        return sum(peso for _, peso in self.adyacencia.get(nodo, {}).items())
    
    def _calcular_enlaces_internos_iniciales(self):
        """Calcula los enlaces internos iniciales para cada comunidad."""
        enlaces = defaultdict(int)
        for nodo, vecinos in self.adyacencia.items():
            com_nodo = self.comunidades[nodo]
            for vecino, peso in vecinos.items():
                if self.comunidades[vecino] == com_nodo:
                    enlaces[com_nodo] += peso
        return enlaces
    
    def _calcular_modularidad(self):
        """Calcula la modularidad actual del grafo."""
        q = 0.0
        m_doble = sum(self.grados_nodos.values())
        if m_doble == 0:
            return 0.0
            
        for comunidad in set(self.comunidades.values()):
            sum_in = self.enlaces_comunidad.get(comunidad, 0)
            sum_tot = self.grados_comunidad.get(comunidad, 0)
            q += (sum_in / m_doble) - (sum_tot / m_doble) ** 2
        return q
    
    def _mover_nodo(self, nodo):
        """Intenta mover un nodo a una comunidad vecina para maximizar la modularidad."""
        comunidad_actual = self.comunidades[nodo]
        mejor_comunidad = comunidad_actual
        max_delta_q = 0.0
        
        # Calcular enlaces a comunidades vecinas
        enlaces_a_comunidades = defaultdict(int)
        for vecino, peso in self.adyacencia.get(nodo, {}).items():
            enlaces_a_comunidades[self.comunidades[vecino]] += peso
        
        k_i = self.grados_nodos[nodo]
        m_doble = sum(self.grados_nodos.values())
        
        # Quitar nodo de su comunidad actual (temporalmente)
        self.grados_comunidad[comunidad_actual] -= k_i
        sum_in_actual = self.enlaces_comunidad.get(comunidad_actual, 0)
        k_i_in_actual = enlaces_a_comunidades.get(comunidad_actual, 0)
        self.enlaces_comunidad[comunidad_actual] = sum_in_actual - k_i_in_actual
        
        # Evaluar todas las comunidades vecinas
        for comunidad_vecina, k_i_in in enlaces_a_comunidades.items():
            if comunidad_vecina == comunidad_actual:
                continue
                
            sum_tot = self.grados_comunidad.get(comunidad_vecina, 0)
            
            # Calcular ganancia de modularidad
            delta_q = (2 * k_i_in - k_i * sum_tot) / m_doble
            
            if delta_q > max_delta_q:
                max_delta_q = delta_q
                mejor_comunidad = comunidad_vecina
        
        # Mover el nodo a la mejor comunidad encontrada
        if mejor_comunidad != comunidad_actual:
            self.comunidades[nodo] = mejor_comunidad
            self.grados_comunidad[mejor_comunidad] += k_i
            sum_in_mejor = self.enlaces_comunidad.get(mejor_comunidad, 0)
            k_i_in_mejor = enlaces_a_comunidades.get(mejor_comunidad, 0)
            self.enlaces_comunidad[mejor_comunidad] = sum_in_mejor + k_i_in_mejor
            return True
        else:
            # Si no se mueve, restaurar valores originales
            self.grados_comunidad[comunidad_actual] += k_i
            self.enlaces_comunidad[comunidad_actual] = sum_in_actual
            return False
    
    def _fase_1(self):
        """Ejecuta la primera fase del algoritmo (optimización de modularidad)."""
        mejorado = False
        nodos_orden = self.nodos.copy()
        random.shuffle(nodos_orden)
        
        for nodo in tqdm(nodos_orden, desc="Optimizando modularidad"):
            if self._mover_nodo(nodo):
                mejorado = True
                
        return mejorado
    
    def _fase_2(self):
        """Ejecuta la segunda fase del algoritmo (agregación de comunidades)."""
        # Mapear comunidades antiguas a nuevas
        comunidades_unicas = sorted(set(self.comunidades.values()))
        com_to_nuevo_nodo = {com: i for i, com in enumerate(comunidades_unicas)}
        
        # Crear nuevo grafo
        nuevo_grafo = Grafo()
        
        # Agregar nodos (uno por comunidad)
        for com in comunidades_unicas:
            nuevo_grafo.agregar_nodo(com_to_nuevo_nodo[com])
        
        # Agregar aristas entre comunidades
        aristas_entre_comunidades = defaultdict(int)
        for nodo, vecinos in self.adyacencia.items():
            com_nodo = com_to_nuevo_nodo[self.comunidades[nodo]]
            for vecino, peso in vecinos.items():
                com_vecino = com_to_nuevo_nodo[self.comunidades[vecino]]
                if com_nodo <= com_vecino:  # Evitar duplicados
                    aristas_entre_comunidades[(com_nodo, com_vecino)] += peso
        
        for (com1, com2), peso in aristas_entre_comunidades.items():
            if com1 != com2:
                nuevo_grafo.agregar_arista(com1, com2, peso)
            else:
                # Para self-loops, dividir el peso por 2
                nuevo_grafo.agregar_arista(com1, com2, peso // 2)
        
        return nuevo_grafo
    
    def ejecutar(self, max_iter=100, tol=1e-6):
        """Ejecuta el algoritmo de Louvain completo."""
        start_time = time.time()
        logging.info("Iniciando algoritmo de Louvain para detección de comunidades...")
        
        self._inicializar_estructuras()
        q_actual = self._calcular_modularidad()
        iteracion = 0
        mejora = float('inf')
        
        while iteracion < max_iter and mejora > tol:
            # Fase 1: Optimización de modularidad
            mejoro = self._fase_1()
            
            if not mejoro:
                break
                
            # Calcular nueva modularidad
            q_nueva = self._calcular_modularidad()
            mejora = q_nueva - q_actual
            q_actual = q_nueva
            iteracion += 1
            
            logging.info(f"Iteración {iteracion}: Modularidad = {q_actual:.6f}")
            
            # Fase 2: Agregación de comunidades
            if iteracion < max_iter and mejora > tol:
                self.grafo = self._fase_2()
                self._inicializar_estructuras()
        
        self.modularidad = q_actual
        logging.info(f"Algoritmo de Louvain completado en {time.time() - start_time:.2f}s")
        logging.info(f"Modularidad final: {self.modularidad:.4f}")
        
        # Convertir a formato de comunidades
        comunidades_dict = defaultdict(list)
        for nodo, com in self.comunidades.items():
            comunidades_dict[com].append(nodo)
            
        return dict(comunidades_dict)

def detectar_comunidades_louvain(grafo: Grafo, max_iter: int = 100, tol: float = 1e-6) -> Dict[int, List[int]]:
    """
    Detecta comunidades usando el algoritmo de Louvain.
    
    Args:
        grafo: Grafo a analizar
        max_iter: Máximo número de iteraciones
        tol: Tolerancia para la convergencia
        
    Returns:
        Diccionario donde las claves son IDs de comunidad y los valores son listas de nodos
    """
    louvain = Louvain(grafo)
    return louvain.ejecutar(max_iter, tol)

def analizar_comunidades(comunidades: Dict[int, List[int]]) -> Dict[str, float]:
    """
    Analiza las comunidades detectadas y calcula métricas importantes.
    
    Args:
        comunidades: Diccionario de comunidades devuelto por detectar_comunidades_louvain
        
    Returns:
        Diccionario con métricas de las comunidades
    """
    if not comunidades:
        return {}
    
    tamanos = [len(nodos) for nodos in comunidades.values()]
    
    stats = {
        'num_comunidades': len(comunidades),
        'tamano_promedio': sum(tamanos) / len(tamanos),
        'tamano_maximo': max(tamanos),
        'tamano_minimo': min(tamanos),
        'razon_tamano_max_prom': max(tamanos) / (sum(tamanos) / len(tamanos))
    }
    
    # Visualización
    plt.figure(figsize=(12, 6))
    sns.histplot(tamanos, bins=50, kde=False)
    plt.title("Distribución de Tamaños de Comunidades")
    plt.xlabel("Tamaño de la comunidad")
    plt.ylabel("Frecuencia")
    plt.yscale('log')
    plt.savefig("distribucion_comunidades.png")
    plt.close()
    
    return stats

# Las funciones originales (analizar_distribucion_grados, encontrar_nodos_importantes, etc.) permanecen igual
