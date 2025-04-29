import numpy as np
import os
import time
import gc
from typing import Dict, List, Tuple, Optional, Set
from tqdm import tqdm
import psutil  # Para monitoreo de memoria

class SocialNetworkAnalysis:
    def __init__(self, location_path: str, connections_path: str):
        """
        Inicializa el análisis de la red social.
        
        Args:
            location_path: Ruta al archivo de ubicaciones
            connections_path: Ruta al archivo de conexiones
        """
        self.location_path = location_path
        self.connections_path = connections_path
        
        # Estadísticas
        self.total_nodes = 0
        self.total_valid_nodes = 0
        self.total_edges = 0
        self.invalid_locations = 0
        self.invalid_connections = 0
        
        # Datos
        self.locations = None  # Será un numpy array
        self.connections = None  # Lista optimizada de conexiones
        self.node_degrees = None  # Array para almacenar el grado de cada nodo
        self.max_connections_node = (0, 0)  # (id_nodo, num_conexiones)
        
        # Banderas de control
        self.data_loaded = False
    
    def load_data(self, chunk_size: int = 100_000, max_nodes: Optional[int] = None) -> None:
        """
        Carga los datos de ubicación y conexiones.
        
        Args:
            chunk_size: Tamaño de los chunks para cargar los datos
            max_nodes: Número máximo de nodos a cargar (None para todos)
        """
        start_time = time.time()
        print(f"Iniciando carga de datos...")
        
        # Monitoreo de memoria inicial
        self._print_memory_usage("Memoria antes de cargar datos")
        
        # Calcular el número total de nodos 
        if not max_nodes:
            try:
                # Contar líneas de forma eficiente
                with open(self.location_path, 'r') as f:
                    self.total_nodes = sum(1 for _ in f)
            except Exception as e:
                print(f"Error al contar líneas: {e}")
                self.total_nodes = 10_000_000  # Valor por defecto
        else:
            self.total_nodes = max_nodes
        
        print(f"Preparando estructuras para {self.total_nodes:,} nodos...")
        
        # Inicializar estructuras de forma optimizada
        # Usar float32 en lugar de float64 para ubicaciones (reduce memoria a la mitad)
        self.locations = np.zeros((self.total_nodes, 2), dtype=np.float32)
        
        # Para las conexiones, usaremos una lista de Python
        # No pre-inicializamos la lista completa para ahorrar memoria
        self.connections = []
        self.node_degrees = np.zeros(self.total_nodes, dtype=np.int32)
        
        # Cargar ubicaciones
        self._load_locations(chunk_size)
        gc.collect()  # Forzar liberación de memoria
        self._print_memory_usage("Memoria después de cargar ubicaciones")
        
        # Cargar conexiones
        self._load_connections(chunk_size)
        gc.collect()  # Forzar liberación de memoria
        self._print_memory_usage("Memoria después de cargar conexiones")
        
        self.data_loaded = True
        end_time = time.time()
        print(f"Datos cargados y procesados en {end_time - start_time:.2f} segundos")
        
        # Calcular estadísticas finales
        self._calculate_statistics()
    
    def _print_memory_usage(self, message: str) -> None:
        """Imprime el uso actual de memoria RAM"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_usage_mb = memory_info.rss / 1024 / 1024
        print(f"{message}: {memory_usage_mb:.2f} MB")
    
    def _load_locations(self, chunk_size: int) -> None:
        """Carga y procesa las ubicaciones de archivo TXT por chunks"""
        print("Cargando ubicaciones desde archivo de texto...")
        start_time = time.time()
        
        # Contador de ubicaciones inválidas
        self.invalid_locations = 0
        processed_lines = 0
        
        try:
            # Calcular el tamaño total para la barra de progreso
            total_size = os.path.getsize(self.location_path)
            
            with open(self.location_path, 'r') as f:
                # Crear barra de progreso
                pbar = tqdm(total=total_size, unit='B', unit_scale=True, 
                           desc="Ubicaciones")
                
                while True:
                    # Leer un chunk de líneas
                    lines = []
                    chunk_bytes = 0
                    for _ in range(chunk_size):
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line)
                        chunk_bytes += len(line)
                    
                    if not lines:
                        break
                    
                    # Actualizar barra de progreso
                    pbar.update(chunk_bytes)
                    
                    # Procesar las líneas de este chunk
                    for line in lines:
                        node_id = processed_lines
                        
                        if node_id >= self.total_nodes:
                            break
                        
                        try:
                            # Parsear las coordenadas de latitud y longitud de forma optimizada
                            parts = line.strip().split(',')
                            if len(parts) != 2:
                                self.invalid_locations += 1
                                continue
                                
                            lat = float(parts[0].strip())
                            lon = float(parts[1].strip())
                            
                            # Almacenar en el array
                            self.locations[node_id] = [lat, lon]
                            
                        except (ValueError, IndexError):
                            self.invalid_locations += 1
                        
                        processed_lines += 1
                    
                    # Verificar si hemos terminado
                    if processed_lines >= self.total_nodes:
                        break
                
                pbar.close()
        
        except Exception as e:
            print(f"Error al cargar ubicaciones: {e}")
        
        self.total_valid_nodes = processed_lines - self.invalid_locations
        end_time = time.time()
        print(f"Ubicaciones cargadas en {end_time - start_time:.2f} segundos")
        print(f"Total líneas procesadas: {processed_lines:,}")
        print(f"Ubicaciones inválidas: {self.invalid_locations:,}")
    
    def _load_connections(self, chunk_size: int) -> None:
        """Carga y procesa las conexiones de archivo TXT por chunks"""
        print("Cargando conexiones desde archivo de texto...")
        start_time = time.time()
        
        # Contador de líneas procesadas y conexiones inválidas
        processed_lines = 0
        self.invalid_connections = 0
        edges_count = 0
        
        # Pre-inicializar la lista de conexiones para evitar append constante
        # Esto mejora el rendimiento significativamente
        self.connections = [[] for _ in range(self.total_nodes)]
        
        try:
            # Calcular el tamaño total para la barra de progreso
            total_size = os.path.getsize(self.connections_path)
            
            with open(self.connections_path, 'r') as f:
                # Crear barra de progreso
                pbar = tqdm(total=total_size, unit='B', unit_scale=True, 
                           desc="Conexiones")
                
                node_id = 0
                
                while True:
                    # Leer un chunk de líneas
                    lines = []
                    chunk_bytes = 0
                    for _ in range(chunk_size):
                        line = f.readline()
                        if not line:
                            break
                        lines.append(line)
                        chunk_bytes += len(line)
                    
                    if not lines:
                        break
                    
                    # Actualizar barra de progreso
                    pbar.update(chunk_bytes)
                    
                    # Procesar cada línea en el chunk
                    for line in lines:
                        if node_id >= self.total_nodes:
                            break
                            
                        # Procesar conexiones de forma más eficiente
                        try:
                            # Parsear y filtrar conexiones en un solo paso
                            parts = [part.strip() for part in line.strip().split(',')]
                            valid_connections = []
                            
                            for part in parts:
                                if part:  # Verificar que no esté vacío
                                    try:
                                        conn_id = int(part)
                                        # Validar rango y convertir a índice basado en 0
                                        if 1 <= conn_id <= self.total_nodes:
                                            valid_connections.append(conn_id - 1)
                                        else:
                                            self.invalid_connections += 1
                                    except ValueError:
                                        self.invalid_connections += 1
                            
                            # Guardar conexiones válidas
                            self.connections[node_id] = valid_connections
                            
                            # Actualizar grado del nodo y contar aristas
                            conn_count = len(valid_connections)
                            self.node_degrees[node_id] = conn_count
                            edges_count += conn_count
                            
                            # Actualizar nodo con más conexiones
                            if conn_count > self.max_connections_node[1]:
                                self.max_connections_node = (node_id + 1, conn_count)
                            
                        except Exception:
                            self.invalid_connections += 1
                        
                        node_id += 1
                        processed_lines += 1
                    
                    # Liberación periódica de memoria
                    if processed_lines % (chunk_size * 10) == 0:
                        gc.collect()
                
                pbar.close()
        
        except Exception as e:
            print(f"Error al cargar conexiones: {e}")
        
        # En una red social las conexiones suelen ser dirigidas
        self.total_edges = edges_count
        
        end_time = time.time()
        print(f"Conexiones cargadas en {end_time - start_time:.2f} segundos")
        print(f"Total líneas procesadas: {processed_lines:,}")
        print(f"Conexiones inválidas: {self.invalid_connections:,}")
    
    def _calculate_statistics(self) -> None:
        """Calcula estadísticas sobre el grafo"""
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        print("\nCalculando estadísticas del grafo...")
        
        # La mayoría de las estadísticas ya están calculadas durante la carga
        print("\nEstadísticas de la red social:")
        print(f"Total de nodos cargados: {self.total_nodes:,}")
        print(f"Nodos con datos válidos: {self.total_valid_nodes:,}")
        print(f"Total de aristas (conexiones dirigidas): {self.total_edges:,}")
        print(f"Datos no limpios:")
        print(f"  - Ubicaciones inválidas: {self.invalid_locations:,}")
        print(f"  - Conexiones inválidas: {self.invalid_connections:,}")
        print(f"Nodo con más conexiones: ID {self.max_connections_node[0]} con {self.max_connections_node[1]} conexiones")
        
        # Calcular estadísticas adicionales
        degree_stats = self._calculate_degree_statistics()
        print(f"\nEstadísticas de grado:")
        print(f"  - Grado promedio: {degree_stats['average']:.2f}")
        print(f"  - Grado mediano: {degree_stats['median']}")
        print(f"  - Grado máximo: {degree_stats['max']}")
        print(f"  - Grado mínimo: {degree_stats['min']}")
    
    def _calculate_degree_statistics(self) -> Dict:
        """Calcula estadísticas sobre el grado de los nodos"""
        # Filtrar nodos sin conexiones para estadísticas más precisas
        active_degrees = self.node_degrees[self.node_degrees > 0]
        
        if len(active_degrees) == 0:
            return {
                'average': 0,
                'median': 0,
                'max': 0,
                'min': 0
            }
        
        return {
            'average': np.mean(active_degrees),
            'median': np.median(active_degrees),
            'max': np.max(active_degrees),
            'min': np.min(active_degrees)
        }
    
    def query_user(self, user_id: int) -> Optional[Dict]:
        """
        Consulta información de un usuario por su ID
        
        Args:
            user_id: ID del usuario (indexado desde 1)
            
        Returns:
            Diccionario con información del usuario o None si no existe
        """
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        # Convertir a índice basado en 0
        idx = user_id - 1
        
        if idx < 0 or idx >= self.total_nodes:
            print(f"Error: ID de usuario {user_id} fuera de rango (1-{self.total_nodes})")
            return None
        
        # Determinar si el usuario tiene datos válidos
        location = tuple(self.locations[idx])
        if location[0] == 0 and location[1] == 0:  # Asumimos que (0,0) es inválido
            location = None
        
        # Obtener conexiones (convertimos de vuelta a IDs basados en 1)
        connections = [conn_id + 1 for conn_id in self.connections[idx]]
        
        return {
            "user_id": user_id,
            "location": location,
            "connections": connections,
            "connection_count": len(connections)
        }
    
    def find_users_near(self, lat: float, lon: float, distance: float = 0.1, limit: int = 10) -> List[Tuple[int, float]]:
        """
        Encuentra usuarios cercanos a una ubicación geográfica
        
        Args:
            lat: Latitud
            lon: Longitud
            distance: Distancia máxima (en grados)
            limit: Número máximo de resultados
            
        Returns:
            Lista de tuplas (user_id, distancia)
        """
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        # Optimización: Solo calcular distancias para nodos con ubicaciones válidas
        valid_mask = ~np.all(self.locations == 0, axis=1)
        
        # Calcular distancia euclidiana (aproximación simple)
        diffs = self.locations[valid_mask] - np.array([lat, lon])
        distances = np.sqrt(np.sum(diffs**2, axis=1))
        
        # Obtener índices originales de nodos válidos
        valid_indices = np.where(valid_mask)[0]
        
        # Filtrar por distancia y obtener k mínimos
        nearby_mask = distances <= distance
        
        if not np.any(nearby_mask):
            return []
            
        # Encontrar los índices de los k elementos más cercanos
        nearby_indices = valid_indices[nearby_mask]
        nearby_distances = distances[nearby_mask]
        
        # Ordenar por distancia y limitar resultados
        order = np.argsort(nearby_distances)[:limit]
        
        # Convertir a lista de resultados (user_id basado en 1)
        results = [(int(nearby_indices[i]) + 1, float(nearby_distances[i])) 
                  for i in order]
        
        return results

    def find_common_connections(self, user_id1: int, user_id2: int) -> List[int]:
        """
        Encuentra conexiones comunes entre dos usuarios
        
        Args:
            user_id1: ID del primer usuario
            user_id2: ID del segundo usuario
            
        Returns:
            Lista de IDs de usuarios que son conexiones comunes
        """
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        # Convertir a índices basados en 0
        idx1 = user_id1 - 1
        idx2 = user_id2 - 1
        
        if idx1 < 0 or idx1 >= self.total_nodes or idx2 < 0 or idx2 >= self.total_nodes:
            print(f"Error: ID de usuario fuera de rango (1-{self.total_nodes})")
            return []
        
        # Encontrar la intersección de conexiones
        set1 = set(self.connections[idx1])
        set2 = set(self.connections[idx2])
        common = set1.intersection(set2)
        
        # Convertir de vuelta a IDs basados en 1
        return sorted([conn_id + 1 for conn_id in common])
    
    def find_users_with_most_connections(self, limit: int = 10) -> List[Tuple[int, int]]:
        """
        Encuentra los usuarios con más conexiones
        
        Args:
            limit: Número máximo de resultados
            
        Returns:
            Lista de tuplas (user_id, num_conexiones)
        """
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        # Encontrar los índices de los usuarios con más conexiones
        top_indices = np.argsort(-self.node_degrees)[:limit]
        
        # Convertir a lista de resultados (user_id basado en 1)
        results = [(int(idx) + 1, int(self.node_degrees[idx])) for idx in top_indices]
        
        return results
    
    def find_path_between_users(self, user_id1: int, user_id2: int, max_depth: int = 3) -> List[int]:
        """
        Busca un camino entre dos usuarios utilizando BFS
        
        Args:
            user_id1: ID del primer usuario
            user_id2: ID del segundo usuario
            max_depth: Profundidad máxima de búsqueda
            
        Returns:
            Lista de IDs de usuarios en el camino, o lista vacía si no se encuentra
        """
        if not self.data_loaded:
            raise ValueError("Los datos deben ser cargados primero")
        
        # Convertir a índices basados en 0
        start_idx = user_id1 - 1
        end_idx = user_id2 - 1
        
        if start_idx < 0 or start_idx >= self.total_nodes or end_idx < 0 or end_idx >= self.total_nodes:
            print(f"Error: ID de usuario fuera de rango (1-{self.total_nodes})")
            return []
        
        # Caso especial: son el mismo usuario
        if start_idx == end_idx:
            return [user_id1]
        
        # Caso especial: conexión directa
        if end_idx in self.connections[start_idx]:
            return [user_id1, user_id2]
        
        # BFS limitado por profundidad
        visited = set([start_idx])
        queue = [(start_idx, [user_id1])]  # (nodo, camino)
        depth = 0
        
        while queue and depth < max_depth:
            depth += 1
            level_size = len(queue)
            
            for _ in range(level_size):
                current, path = queue.pop(0)
                
                # Explorar vecinos
                for neighbor in self.connections[current]:
                    if neighbor == end_idx:
                        # Encontramos el destino
                        return path + [user_id2]
                    
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, path + [neighbor + 1]))  # +1 para ID basado en 1
        
        return []  # No se encontró camino dentro de la profundidad máxima


def main():
    # Rutas de los archivos
    location_path = "10_million_location.txt"  # Archivo de ubicaciones en formato txt
    connections_path = "10_million_user.txt"   # Archivo de conexiones en formato txt
    
    # Permitir cambiar los nombres de archivo
    print("\n=== CONFIGURACIÓN INICIAL ===")
    print(f"Archivos por defecto:")
    print(f"- Ubicaciones: {location_path}")
    print(f"- Conexiones: {connections_path}")
    
    change_files = input("¿Desea cambiar los nombres de archivo? (s/n): ").lower()
    if change_files == 's':
        location_path = input("Nombre del archivo de ubicaciones: ") or location_path
        connections_path = input("Nombre del archivo de conexiones: ") or connections_path
    
    # Verificar que los archivos existan
    for path in [location_path, connections_path]:
        if not os.path.exists(path):
            print(f"Error: El archivo {path} no existe.")
            return
    
    # Tamaño de chunks y límite de nodos
    chunk_size = 100_000
    max_nodes = None
    
    configure = input("¿Desea configurar parámetros de carga (tamaño de chunks, límite de nodos)? (s/n): ").lower()
    if configure == 's':
        try:
            chunk_size = int(input(f"Tamaño de chunks (actual: {chunk_size}): ") or chunk_size)
            max_input = input("Límite de nodos para pruebas (enter para cargar todos): ")
            if max_input:
                max_nodes = int(max_input)
        except ValueError:
            print("Error en los valores ingresados. Usando valores por defecto.")
    
    # Inicializar el análisis
    print("\nInicializando análisis de red social...")
    network = SocialNetworkAnalysis(location_path, connections_path)
    
    # Cargar datos con los parámetros configurados
    network.load_data(chunk_size=chunk_size, max_nodes=max_nodes)
    
    # Interfaz simple de consulta
    while True:
        print("\n=== MENÚ DE OPCIONES ===")
        print("1. Consultar usuario por ID")
        print("2. Encontrar usuarios cercanos a una ubicación")
        print("3. Encontrar conexiones comunes entre dos usuarios")
        print("4. Mostrar usuarios con más conexiones")
        print("5. Buscar camino entre dos usuarios")
        print("6. Mostrar estadísticas del grafo")
        print("0. Salir")
        
        choice = input("\nSeleccione una opción: ")
        
        if choice == "1":
            try:
                user_id = int(input("Ingrese ID de usuario (1-10,000,000): "))
                user_data = network.query_user(user_id)
                if user_data:
                    print(f"\n===== INFORMACIÓN DEL USUARIO {user_id} =====")
                    print(f"Ubicación: {user_data['location']}")
                    print(f"Número de conexiones (usuarios que sigue): {user_data['connection_count']}")
                    
                    # Mostrar algunas conexiones (limitadas para no saturar la pantalla)
                    if user_data['connection_count'] > 0:
                        show_limit = min(10, len(user_data['connections'])) 
                        connections_str = ", ".join(map(str, user_data['connections'][:show_limit]))
                        if show_limit < len(user_data['connections']):
                            connections_str += f", ... (y {len(user_data['connections']) - show_limit} más)"
                        print(f"Usuarios que sigue: {connections_str}")
                    else:
                        print("Este usuario no sigue a nadie")
            except ValueError:
                print("Error: Ingrese un ID de usuario válido (número entero)")
        
        elif choice == "2":
            try:
                lat = float(input("Ingrese latitud: "))
                lon = float(input("Ingrese longitud: "))
                distance = float(input("Ingrese distancia máxima (en grados): ") or "0.1")
                limit = int(input("Número máximo de resultados: ") or "10")
                
                nearby = network.find_users_near(lat, lon, distance, limit)
                
                print(f"\n===== USUARIOS CERCANOS A ({lat}, {lon}) =====")
                for user_id, dist in nearby:
                    print(f"Usuario ID {user_id}: distancia = {dist:.6f}")
                
                if not nearby:
                    print("No se encontraron usuarios en ese rango")
            
            except ValueError:
                print("Error: Ingrese valores numéricos válidos")
        
        elif choice == "3":
            try:
                user_id1 = int(input("Ingrese ID del primer usuario: "))
                user_id2 = int(input("Ingrese ID del segundo usuario: "))
                
                common = network.find_common_connections(user_id1, user_id2)
                
                print(f"\n===== CONEXIONES COMUNES ENTRE USUARIO {user_id1} Y {user_id2} =====")
                if common:
                    print(f"Total de conexiones comunes: {len(common)}")
                    
                    # Mostrar algunas conexiones (limitadas para no saturar la pantalla)
                    show_limit = min(20, len(common))
                    connections_str = ", ".join(map(str, common[:show_limit]))
                    if show_limit < len(common):
                        connections_str += f", ... (y {len(common) - show_limit} más)"
                    print(f"Conexiones comunes: {connections_str}")
                else:
                    print("No se encontraron conexiones comunes")
            
            except ValueError:
                print("Error: Ingrese IDs de usuario válidos (números enteros)")
        
        elif choice == "4":
            try:
                limit = int(input("Número de usuarios a mostrar: ") or "10")
                top_users = network.find_users_with_most_connections(limit)
                
                print(f"\n===== TOP {limit} USUARIOS CON MÁS CONEXIONES =====")
                for i, (user_id, count) in enumerate(top_users, 1):
                    print(f"{i}. Usuario ID {user_id}: {count} conexiones")
            
            except ValueError:
                print("Error: Ingrese un número válido")
                
        elif choice == "5":
            try:
                user_id1 = int(input("Ingrese ID del primer usuario: "))
                user_id2 = int(input("Ingrese ID del segundo usuario: "))
                max_depth = int(input("Profundidad máxima de búsqueda (1-5, default 3): ") or "3")
                
                if max_depth < 1 or max_depth > 5:
                    print("La profundidad debe estar entre 1 y 5. Usando 3 por defecto.")
                    max_depth = 3
                
                path = network.find_path_between_users(user_id1, user_id2, max_depth)
                
                print(f"\n===== CAMINO ENTRE USUARIO {user_id1} Y {user_id2} =====")
                if path:
                    print(f"Longitud del camino: {len(path) - 1} conexiones")
                    print(f"Camino: {' -> '.join(map(str, path))}")
                else:
                    print(f"No se encontró un camino con profundidad máxima {max_depth}")
            
            except ValueError:
                print("Error: Ingrese valores numéricos válidos")
                
        elif choice == "6":
            network._calculate_statistics()
        
        elif choice == "0":
            print("Saliendo del programa...")
            break
        
        else:
            print("Opción no válida. Intente de nuevo.")


if __name__ == "__main__":
    main()
