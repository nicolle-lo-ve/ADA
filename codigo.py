import time
import pandas as pd
import numpy as np
from igraph import Graph
from tqdm import tqdm  # Para barras de progreso (opcional)
import plotly.graph_objects as go

# ---------------------------
# 1. Carga y preprocesamiento
# ---------------------------
def cargar_ubicaciones(ruta_archivo: str) -> pd.DataFrame:
    """Carga las ubicaciones en un DataFrame optimizado con tipos de datos."""
    dtype = {'lat': np.float32, 'lon': np.float32}
    return pd.read_csv(ruta_archivo, header=None, names=['lat', 'lon'], dtype=dtype)

def construir_grafo(ruta_usuarios: str, n_usuarios: int) -> Graph:
    """Construye el grafo dirigido desde el archivo de usuarios, cargando por lotes."""
    grafo = Graph(directed=True)
    grafo.add_vertices(n_usuarios)  # Índices de 0 a n_usuarios-1

    # Leer archivo en bloques para evitar desbordamiento de memoria
    chunk_size = 10000  # Ajustar según RAM disponible
    with open(ruta_usuarios, 'r') as archivo:
        for i, linea in tqdm(enumerate(archivo), desc="Construyendo grafo"):
            seguidores = list(map(int, linea.strip().split(', ')))
            # Añadir aristas: usuario i+1 sigue a 'seguidores' (índices en archivo empiezan en 1)
            grafo.add_edges([(i, seguidor-1) for seguidor in seguidores])

            # Liberar memoria periódicamente
            if i % chunk_size == 0:
                grafo.simplify()  # Elimina duplicados
                grafo.es.clear()   # Limpia aristas temporalmente (ajustar según necesidad)

    return grafo

# ---------------------------
# 2. Análisis Básico (EDA)
# ---------------------------
def calcular_metricas_basicas(grafo: Graph) -> dict:
    """Calcula métricas básicas del grafo."""
    return {
        "n_nodos": grafo.vcount(),
        "n_aristas": grafo.ecount(),
        "densidad": grafo.density(),
        "grado_promedio": np.mean(grafo.degree()),
        "diametro": grafo.diameter(directed=True),
    }

# ---------------------------
# 3. Detección de Comunidades
# ---------------------------
def detectar_comunidades(grafo: Graph) -> list:
    """Aplica el algoritmo de Louvain para detectar comunidades."""
    # Convertir a no dirigido para Louvain (ajustar según necesidades)
    grafo_undir = grafo.as_undirected(mode="mutual")
    return grafo_undir.community_multilevel()

# ---------------------------
# 4. Visualización Interactiva
# ---------------------------
def visualizar_comunidades(grafo: Graph, comunidades: list, ubicaciones: pd.DataFrame, muestra: int = 1000) -> None:
    """Visualiza una muestra del grafo con Plotly, coloreando por comunidades."""
    # Muestrear nodos para evitar sobrecarga
    nodos_muestra = np.random.choice(grafo.vcount(), muestra, replace=False)
    subgrafo = grafo.subgraph(nodos_muestra)

    # Coordenadas geográficas de la muestra
    coordenadas = ubicaciones.iloc[nodos_muestra]
    edge_x, edge_y = [], []

    # Preparar aristas para Plotly
    for edge in subgrafo.es:
        src, dest = edge.tuple
        edge_x.extend([coordenadas.lon.iloc[src], coordenadas.lon.iloc[dest], None])
        edge_y.extend([coordenadas.lat.iloc[src], coordenadas.lat.iloc[dest], None])

    # Crear figura
    fig = go.Figure(
        data=[
            go.Scattergeo(  # Aristas
                lon=edge_x, lat=edge_y, mode='lines',
                line=dict(width=0.5, color='#888'), showlegend=False
            ),
            go.Scattergeo(  # Nodos
                lon=coordenadas.lon, lat=coordenadas.lat,
                mode='markers', marker=dict(
                    size=5, color=comunidades.membership[nodos_muestra],
                    colorscale='Viridis', opacity=0.8
                )
            )
        ]
    )
    fig.update_geos(projection_type="natural earth")
    fig.show()

# ---------------------------
# Ejecución Principal (Ajusta las rutas!)
# ---------------------------
if __name__ == "__main__":
    # Configuración inicial
    RUTA_UBICACIONES = "10_million_location.txt"
    RUTA_USUARIOS = "10_million_user.txt"
    N_USUARIOS = 10_000_000  # Ajustar si el dataset es más pequeño

    # Paso 1: Cargar datos
    print("Cargando ubicaciones...")
    ubicaciones = cargar_ubicaciones(RUTA_UBICACIONES)

    print("Construyendo grafo...")
    grafo = construir_grafo(RUTA_USUARIOS, N_USUARIOS)

    # Paso 2: Calcular métricas
    metricas = calcular_metricas_basicas(grafo)
    print(f"Métricas del grafo: {metricas}")

    # Paso 3: Detectar comunidades (¡MUY costoso para 10M nodos!)
    print("Detectando comunidades...")
    comunidades = detectar_comunidades(grafo)

    # Paso 4: Visualizar (muestra de 1000 nodos)
    visualizar_comunidades(grafo, comunidades, ubicaciones, muestra=1000)
