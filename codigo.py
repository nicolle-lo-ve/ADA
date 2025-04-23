import time
import pandas as pd
import numpy as np
from igraph import Graph
from tqdm import tqdm
import plotly.graph_objects as go
import multiprocessing as mp
from itertools import islice
import psutil
import os
import gc

# ---------------------------
# 1. Carga y preprocesamiento optimizado
# ---------------------------
def cargar_ubicaciones_optimizado(ruta_archivo: str, chunk_size=500_000) -> pd.DataFrame:
    """Carga las ubicaciones en chunks para optimizar memoria."""
    dtype = {'lat': np.float32, 'lon': np.float32}
    
    # Si el archivo es muy grande, solo cargar una muestra
    total_lines = sum(1 for _ in open(ruta_archivo, 'r'))
    sample_size = min(total_lines, 5_000_000)  # Limitar a 5M para pruebas
    
    if total_lines > sample_size:
        print(f"Cargando muestra de {sample_size} ubicaciones de {total_lines} total")
    
    # Leer solo una muestra
    df = pd.read_csv(ruta_archivo, header=None, names=['lat', 'lon'], 
                    dtype=dtype, nrows=sample_size)
    
    print(f"Ubicaciones cargadas: {len(df)}")
    return df

def procesar_chunk(args):
    """Procesa un chunk de datos y devuelve las aristas, filtrando por límite de nodos."""
    chunk_file, chunk_id, chunk_size, max_node_id = args
    
    try:
        edges = []
        lines_processed = 0
        
        with open(chunk_file, 'r') as f:
            for i, linea in enumerate(f):
                if i < chunk_id * chunk_size:
                    continue
                if i >= (chunk_id + 1) * chunk_size:
                    break
                
                # Saltar líneas vacías
                if not linea.strip():
                    continue
                
                # Verificar que el ID del nodo origen esté dentro del rango válido
                if i >= max_node_id:
                    continue
                
                try:
                    # Procesar solo las primeras 10 conexiones por usuario para pruebas
                    seguidores_raw = linea.strip().split(', ')[:10]
                    seguidores = [int(s) for s in seguidores_raw if s.strip()]
                    
                    # IMPORTANTE: Filtrar los seguidores para asegurar que están dentro del rango
                    seguidores_validos = [s for s in seguidores if 1 <= s <= max_node_id]
                    
                    # Añadir aristas solo para seguidores válidos
                    # Restar 1 para ajustar a índice base-0
                    edges.extend([(i, seguidor-1) for seguidor in seguidores_validos 
                                 if seguidor-1 < max_node_id])
                    
                    lines_processed += 1
                except ValueError:
                    continue
        
        return edges, lines_processed
    except Exception as e:
        print(f"Error en chunk {chunk_id}: {str(e)}")
        return [], 0

def construir_grafo_paralelo(ruta_usuarios: str, n_usuarios: int, chunk_size=10_000, 
                            max_chunks=20, max_procesos=2) -> Graph:
    """
    Construye el grafo con un enfoque de bajo consumo de recursos.
    Procesa solo una parte del archivo para pruebas iniciales.
    """
    # Usar menos procesos para evitar saturar el sistema
    n_procesos = min(max_procesos, max(1, mp.cpu_count() // 2))
    print(f"Usando {n_procesos} procesos (de {mp.cpu_count()} disponibles)")
    
    # Crear grafo con menos vértices para pruebas
    n_usuarios_prueba = min(n_usuarios, chunk_size * max_chunks)
    print(f"Creando grafo con {n_usuarios_prueba} nodos")
    
    grafo = Graph(directed=True)
    grafo.add_vertices(n_usuarios_prueba)
    
    all_edges = []
    total_lines = 0
    
    try:
        # Verificar tamaño total del archivo
        file_size_mb = os.path.getsize(ruta_usuarios) / (1024 * 1024)
        print(f"Tamaño del archivo: {file_size_mb:.2f} MB")
        
        # Generar chunks más pequeños para evitar saturar memoria
        chunks_args = []
        for i in range(max_chunks):
            # Añadir max_node_id como cuarto argumento
            chunks_args.append((ruta_usuarios, i, chunk_size, n_usuarios_prueba))
        
        # Procesar chunks uno por uno o en paralelo según capacidad
        if n_procesos == 1:
            # Procesamiento secuencial para sistemas con poca memoria
            print(f"Procesando {len(chunks_args)} chunks secuencialmente")
            results = []
            for args in tqdm(chunks_args, desc="Procesando chunks"):
                results.append(procesar_chunk(args))
        else:
            # Procesamiento paralelo con pocos procesos
            print(f"Procesando {len(chunks_args)} chunks en paralelo")
            with mp.Pool(processes=n_procesos) as pool:
                results = list(tqdm(
                    pool.imap(procesar_chunk, chunks_args),
                    total=len(chunks_args),
                    desc="Procesando chunks"
                ))
        
        # Recolectar resultados
        for edges, lines in results:
            all_edges.extend(edges)
            total_lines += lines
        
        print(f"Procesadas {total_lines} líneas con {len(all_edges)} conexiones")
        
        # Verificar rango de IDs antes de agregar
        invalid_edges = [(src, dst) for src, dst in all_edges 
                        if src >= n_usuarios_prueba or dst >= n_usuarios_prueba]
        if invalid_edges:
            print(f"ADVERTENCIA: Eliminando {len(invalid_edges)} aristas con IDs fuera de rango")
            all_edges = [(src, dst) for src, dst in all_edges 
                        if src < n_usuarios_prueba and dst < n_usuarios_prueba]
        
        # Agregar aristas al grafo en lotes pequeños para evitar picos de memoria
        batch_size = 50000
        for i in range(0, len(all_edges), batch_size):
            batch = all_edges[i:i+batch_size]
            if batch:
                print(f"Agregando lote de {len(batch)} aristas...")
                grafo.add_edges(batch)
                # Forzar liberación de memoria
                gc.collect()
        
        print(f"Grafo construido: {grafo.vcount()} nodos, {grafo.ecount()} aristas")
        
    except Exception as e:
        print(f"Error en procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
    
    return grafo

# ---------------------------
# 2. Análisis Básico (EDA) simplificado
# ---------------------------
def calcular_metricas_basicas(grafo: Graph) -> dict:
    """Calcula solo las métricas más básicas y rápidas."""
    return {
        "n_nodos": grafo.vcount(),
        "n_aristas": grafo.ecount(),
        "densidad": grafo.density(),
        "grado_promedio": np.mean(grafo.degree()),
    }

# ---------------------------
# 3. Detección de Comunidades simplificada
# ---------------------------
def detectar_comunidades(grafo: Graph, max_nodos=5000) -> list:
    """Aplica detección de comunidades solo a una muestra muy pequeña."""
    print(f"Aplicando detección de comunidades a una muestra de {max_nodos} nodos")
    
    # Si el grafo no tiene aristas, devolver comunidades vacías
    if grafo.ecount() == 0:
        print("El grafo no tiene aristas, no se pueden detectar comunidades")
        return [[] for _ in range(min(max_nodos, grafo.vcount()))]
    
    # Tomar una pequeña muestra de nodos
    nodos_muestra = np.random.choice(grafo.vcount(), min(max_nodos, grafo.vcount()), replace=False)
    subgrafo = grafo.subgraph(nodos_muestra)
    
    # Usar algoritmo rápido si el grafo tiene aristas
    if subgrafo.ecount() > 0:
        subgrafo_undir = subgrafo.as_undirected(mode="collapse")
        if subgrafo_undir.ecount() > 0:
            return subgrafo_undir.community_fastgreedy().as_clustering()
    
    # Fallback si no hay suficientes aristas
    return [[] for _ in range(subgrafo.vcount())]

# ---------------------------
# 4. Visualización simplificada
# ---------------------------
def visualizar_comunidades(grafo: Graph, comunidades, ubicaciones: pd.DataFrame, muestra: int = 200) -> None:
    """Visualiza una muestra muy pequeña del grafo."""
    # Verificar si hay aristas para visualizar
    if grafo.ecount() == 0:
        print("El grafo no tiene aristas para visualizar")
        return
    
    # Tomar muestra pequeña
    nodos_muestra = np.random.choice(grafo.vcount(), min(muestra, grafo.vcount()), replace=False)
    subgrafo = grafo.subgraph(nodos_muestra)
    
    # Coordenadas para la muestra
    if len(ubicaciones) >= max(nodos_muestra) + 1:
        coordenadas = ubicaciones.iloc[nodos_muestra]
    else:
        # Generar coordenadas aleatorias si no hay suficientes
        coordenadas = pd.DataFrame({
            'lat': np.random.uniform(-90, 90, len(nodos_muestra)),
            'lon': np.random.uniform(-180, 180, len(nodos_muestra))
        })
    
    # Verificar si hay aristas en el subgrafo
    if subgrafo.ecount() == 0:
        print("El subgrafo muestreado no tiene aristas para visualizar")
        return
    
    # Limitar aristas para visualización
    max_aristas = 300
    edge_x, edge_y = [], []
    
    # Extraer solo algunas aristas
    for i, edge in enumerate(subgrafo.es):
        if i >= max_aristas:
            break
        src, dest = edge.tuple
        edge_x.extend([coordenadas.lon.iloc[src], coordenadas.lon.iloc[dest], None])
        edge_y.extend([coordenadas.lat.iloc[src], coordenadas.lat.iloc[dest], None])
    
    # Colores para nodos
    colores = subgrafo.degree()
    
    # Crear figura
    fig = go.Figure(
        data=[
            go.Scattergeo(
                lon=edge_x, lat=edge_y, mode='lines',
                line=dict(width=0.5, color='#888'), showlegend=False
            ),
            go.Scattergeo(
                lon=coordenadas.lon, lat=coordenadas.lat,
                mode='markers', marker=dict(
                    size=5, color=colores,
                    colorscale='Plasma', opacity=0.8
                )
            )
        ]
    )
    fig.update_geos(projection_type="natural earth")
    fig.update_layout(title="Muestra de la red social (vista simplificada)")
    fig.show()

# ---------------------------
# Función principal para pruebas
# ---------------------------
def main():
    # Configuración inicial
    RUTA_UBICACIONES = "10_million_location.txt"
    RUTA_USUARIOS = "10_million_user.txt"
    N_USUARIOS = 10_000_000
    
    # Medición de tiempo
    tiempo_inicio = time.time()
    
    try:
        # Paso 1: Cargar datos (muestra)
        print("Cargando ubicaciones (muestra)...")
        ubicaciones = cargar_ubicaciones_optimizado(RUTA_UBICACIONES)
        
        # Paso 2: Construir grafo con muestra pequeña
        print(f"Construyendo grafo con muestra de usuarios...")
        grafo = construir_grafo_paralelo(
            RUTA_USUARIOS, 
            N_USUARIOS, 
            chunk_size=10_000,  # Chunks pequeños
            max_chunks=20,      # Limitar a 200k usuarios para prueba
            max_procesos=2      # Limitar procesos para evitar saturación
        )
        
        # Paso 3: Métricas básicas
        print("Calculando métricas básicas...")
        metricas = calcular_metricas_basicas(grafo)
        for k, v in metricas.items():
            print(f"  - {k}: {v}")
        
        # Continuar solo si hay aristas
        if grafo.ecount() > 0:
            # Paso 4: Detección de comunidades simplificada
            print("Detectando comunidades (muestra pequeña)...")
            comunidades = detectar_comunidades(grafo, max_nodos=2000)
            if hasattr(comunidades, '__len__'):
                print(f"Se detectaron {len(comunidades)} comunidades")
            
            # Paso 5: Visualizar muestra pequeña
            print("Preparando visualización simplificada...")
            visualizar_comunidades(grafo, comunidades, ubicaciones, muestra=200)
        else:
            print("No se detectaron aristas en el grafo. No se puede continuar con análisis de comunidades.")
        
        # Tiempo total
        tiempo_total = time.time() - tiempo_inicio
        print(f"Tiempo total de ejecución: {tiempo_total/60:.2f} minutos")
        
        print("\nPrueba completada!")
        print("Para procesar el conjunto completo, ajuste gradualmente los parámetros:")
        print("- Aumente max_chunks para procesar más usuarios")
        print("- Aumente max_procesos si su sistema lo permite")
        print("- Ajuste chunk_size según su memoria disponible")
        
    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Configurar límite de memoria para evitar crash
    if hasattr(os, 'getpid'):
        # Limitar uso de memoria en Linux/macOS (opcional)
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_AS)
            # Limitar a 4GB o menos si es necesario
            resource.setrlimit(resource.RLIMIT_AS, (4 * 1024 * 1024 * 1024, hard))
        except (ImportError, AttributeError):
            pass

    main()
