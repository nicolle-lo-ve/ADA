# Análisis-Algoritmos y Visualización del Grafo de la Red 'X'

##  Descripción del Proyecto
Este proyecto tiene como objetivo analizar y visualizar la estructura de un grafo masivo de red social para descubrir patrones interesantes y obtener información valiosa sobre la conectividad social, las estructuras de comunidad y las propiedades fundamentales de la red, se implementa un sistema completo para analizar una red social masiva de **10 millones de usuarios**, cumpliendo con los requisitos de la segunda parte del proyecto final. El sistema incluye:

- ✅ Carga eficiente de datos masivos  
- ✅ Construcción optimizada de estructuras de grafo  
- ✅ Análisis avanzado de propiedades de red  
- ✅ Detección de comunidades  
- ✅ Visualización de resultados  

---
###  Requisitos

Archivos necesarios
10_million_user.txt: Contiene las relaciones de amistad entre usuarios.
10_million_location.txt: Contiene las coordenadas geográficas de cada usuario
---
###  Librerias
- pandas
- polars
- matplotlib
- seaborn
- psutil
- pyarrow

 ---
### Objetivos Específicos

- Construcción del Grafo: Implementar la estructura de datos del grafo usando programación pura (sin herramientas externas)
- Análisis Exploratorio: Calcular métricas básicas de la red (nodos, aristas, grado promedio, etc.)
- Detección de Comunidades: Aplicar algoritmos como Girvan-Newman o Louvain
- Análisis de Conectividad: Calcular caminos más cortos y árboles de expansión mínima
- Visualización Interactiva: Crear representaciones visuales usando Plotly o Gephi
- Análisis Geoespacial: Incorporar la información de ubicación para análisis territorial
---  
### Metodología
El proyecto sigue una metodología estructurada que incluye:

- Preprocesamiento y limpieza de datos
- Análisis exploratorio de datos (EDA)
- Implementación de algoritmos de grafos
- Aplicación de métricas de centralidad y conectividad
- Visualización de resultados y comunidades detectada

  ---
###  Ejecución del Proyecto

Descomprimir los archivos de datos:
-  10_million_location.txt.zip
-  10_million_user.txt.zip

## Configurar las rutas en main.py:
- pythondf_ubicaciones = cargar_datos_ubicaciones("ruta/a/10_million_location.txt")
- df_usuarios = cargar_datos_usuarios("ruta/a/10_million_user.txt")

## Ejecutar el análisis completo:
bashpython main.py 

##  Estructura del Código

El proyecto está organizado en módulos especializados para mantener la claridad y separación de responsabilidades:
  ---
### 1. `cargador.py` – Carga de Datos

- Implementa carga optimizada de archivos de ubicaciones y usuarios
- Utiliza **Polars** para procesamiento eficiente en memoria
- Análisis automático de formatos y validación de datos
- Procesamiento por **lotes** para manejar grandes volúmenes

---

### 2.  `eda.py` – Análisis Exploratorio

- Generación de visualizaciones de distribución de datos
- Detección de valores atípicos utilizando el método **IQR**
- Análisis de patrones en conexiones entre usuarios

---

### 3. `grafo.py` – Estructura del Grafo

- Optimizado para grafos con hasta **10 millones de nodos**
- Usa estructuras eficientes: `diccionarios`, `NumPy arrays`
- Métodos implementados:
  - Búsqueda en amplitud (**BFS**)
  - Cálculo de grados
  - Búsqueda de vecinos cercanos
  - Estadísticas básicas del grafo

---

### 4. `analizador_grafo.py` – Análisis Avanzado

- Distribución de grados con **muestreo estadístico**
- Identificación de nodos importantes (centralidad de grado)
- Cálculo del **camino más corto promedio**
- Implementación optimizada del algoritmo **Louvain** para detección de comunidades

---

### 5. `main.py` – Flujo Principal

- Orquesta todo el proceso de análisis
- Configuración de `logging` y manejo de errores
- Generación de reportes y visualizaciones

---

##  Algoritmos Implementados

### Detección de Comunidades – Algoritmo Louvain

- Implementación **desde cero y optimizada**
- Pre-cálculo de métricas para mejorar el rendimiento
- Uso eficiente de estructuras de datos
- Visualización de las comunidades detectadas

### Análisis de Caminos Más Cortos

- Implementación de **BFS** optimizado
- Muestreo estadístico para reducir complejidad
- Cálculo de la **distancia promedio** entre nodos

### Otras Métricas de Red

- Distribución de grados
- Identificación de nodos centrales
- Cálculo de densidad del grafo
- Conteo de nodos aislados

---

##  Optimizaciones Clave

###  Carga de Datos

- Uso de **Polars** para procesamiento vectorizado
- **Streaming** para archivos grandes
- Procesamiento por **lotes**

###  Estructuras de Datos

- `dict` para acceso O(1) a nodos y aristas
- `NumPy arrays` para operaciones vectorizadas
- `set` para manejo rápido de adyacencias

###  Algoritmos

- Muestreo estadístico para análisis escalables
- Louvain optimizado con pre-cálculo de grados
- BFS con estructuras de baja sobrecarga

###  Visualización

- Visualizaciones con `matplotlib` y `seaborn`
- Escalas logarítmicas para mejor análisis visual
- Exportación directa a archivos `.png`

---

##  Archivos de Salida Esperados

- `comunidades.png` – Visualización de comunidades
- `distribucion_grados.png` – Histograma de grados
- `centralidad.png` – Gráfico de nodos más relevantes
- `analisis.log` – Log del proceso de análisis

--

