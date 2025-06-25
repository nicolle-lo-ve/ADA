#  Análisis de Redes Sociales a Gran Escala

##  Descripción del Proyecto

Este proyecto implementa un sistema completo para analizar una red social masiva de **10 millones de usuarios**, cumpliendo con los requisitos de la segunda parte del proyecto final. El sistema incluye:

- ✅ Carga eficiente de datos masivos  
- ✅ Construcción optimizada de estructuras de grafo  
- ✅ Análisis avanzado de propiedades de red  
- ✅ Detección de comunidades  
- ✅ Visualización de resultados  

---

##  Estructura del Código

El proyecto está organizado en módulos especializados para mantener la claridad y separación de responsabilidades:

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

---

##  Estado del Proyecto

- [x] Estructura modular funcional  
- [x] Detección de comunidades sin librerías externas  
- [x] BFS y métricas de caminos más cortos  
- [x] Visualización estática  


---

##  Autores

- 📌 Nombre Apellido 1 
- 📌 Nombre Apellido 2 
---

##  Requisitos

t
