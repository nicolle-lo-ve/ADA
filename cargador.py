import polars as pl
import os
import logging

def cargar_datos_ubicaciones(ruta_archivo: str):
    try:
        logging.info(f"Cargando archivo de ubicaciones: {ruta_archivo}")
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"El archivo {ruta_archivo} no existe.")

        # Primero, leer una muestra para determinar el formato
        with open(ruta_archivo, "r", encoding='utf-8') as archivo:
            primera_linea = archivo.readline().strip()
            logging.info(f"Primera línea del archivo: {primera_linea}")
        
        # Determinar el separador basado en la primera línea
        if ',' in primera_linea:
            separador = ','
            logging.info("Separador detectado: coma (,)")
        elif '\t' in primera_linea:
            separador = '\t'
            logging.info("Separador detectado: tabulación")
        else:
            separador = ' '
            logging.info("Separador detectado: espacio")
        
        # Modo perezoso para optimización automática
        marco_perezoso = pl.scan_csv(
            ruta_archivo,
            has_header=False,
            separator=separador
        )
        
        # Obtener información sobre las columnas
        df_sample = marco_perezoso.head(5).collect()
        num_columnas = len(df_sample.columns)
        logging.info(f"Número de columnas detectadas: {num_columnas}")
        
        # Seleccionar solo las primeras dos columnas como lat y long
        if num_columnas >= 2:
            marco_perezoso = marco_perezoso.select([
                pl.col(f"column_{i+1}").cast(pl.Float64).alias(nombre) 
                for i, nombre in enumerate(["lat", "long"])
            ])
        else:
            raise ValueError(f"El archivo debe tener al menos 2 columnas, pero tiene {num_columnas}")
        
        # Ejecutar con paralelismo explícito
        df = marco_perezoso.collect(streaming=True)
        
        # Validación de datos
        nulos_lat = df.filter(pl.col("lat").is_null()).height
        nulos_long = df.filter(pl.col("long").is_null()).height
        
        if nulos_lat > 0 or nulos_long > 0:
            logging.warning(f"Encontrados {nulos_lat} valores nulos en latitud y {nulos_long} en longitud")
        
        logging.info(f"Archivo cargado exitosamente: {df.height} registros")
        logging.info(f"Rango de latitudes: [{df['lat'].min():.6f}, {df['lat'].max():.6f}]")
        logging.info(f"Rango de longitudes: [{df['long'].min():.6f}, {df['long'].max():.6f}]")
        
        return df
    except Exception as error:
        logging.error(f"Error al cargar datos de ubicaciones: {error}")
        return None
    
def cargar_datos_usuarios(ruta_archivo: str):
    try:
        logging.info(f"Cargando archivo de usuarios: {ruta_archivo}")
        if not os.path.exists(ruta_archivo):
            raise FileNotFoundError(f"El archivo {ruta_archivo} no existe.")

        # Cargar línea por línea para manejar mejor los formatos variables
        lineas = []
        contador_lineas = 0
        lineas_vacias = 0
        
        with open(ruta_archivo, "r", encoding='utf-8') as archivo:
            for linea in archivo:
                linea_limpia = linea.strip()
                if linea_limpia:
                    lineas.append(linea_limpia)
                else:
                    lineas_vacias += 1
                    lineas.append("")  # Mantener la estructura
                
                contador_lineas += 1
                
                # Log de progreso cada millón de líneas
                if contador_lineas % 1000000 == 0:
                    logging.info(f"Procesadas {contador_lineas} líneas...")

        df = pl.DataFrame({"lista_adyacencia": lineas})
        
        # Análisis básico del formato
        lineas_no_vacias = df.filter(pl.col("lista_adyacencia") != "")
        muestra = lineas_no_vacias.head(5)["lista_adyacencia"].to_list()
        
        logging.info(f"Archivo cargado exitosamente: {df.height} registros")
        logging.info(f"Líneas vacías encontradas: {lineas_vacias}")
        logging.info(f"Muestra de formato de datos:")
        
        for i, linea in enumerate(muestra):
            elementos = len(linea.split(', ')) if linea else 0  # Usar ', ' como separador
            logging.info(f"  Línea {i}: {elementos} elementos - '{linea[:50]}{'...' if len(linea) > 50 else ''}'")
        
        return df
    except Exception as error:
        logging.error(f"Error al cargar datos de usuarios: {error}")
        return None

def analizar_formato_usuarios(df: pl.DataFrame, num_muestras: int = 100):
    """
    Analiza el formato de los datos de usuarios para entender su estructura.
    """
    logging.info("=== ANÁLISIS DE FORMATO DE DATOS DE USUARIOS ===")
    
    # Filtrar líneas no vacías
    lineas_validas = df.filter(pl.col("lista_adyacencia") != "")
    
    if lineas_validas.height == 0:
        logging.error("No se encontraron líneas válidas en el archivo de usuarios")
        return None
    
    # Tomar muestra aleatoria
    muestra = lineas_validas.sample(min(num_muestras, lineas_validas.height))
    
    conteo_elementos = []
    formatos_detectados = {
        "solo_numeros": 0,
        "contiene_no_numeros": 0,
        "una_conexion": 0,
        "multiples_conexiones": 0,
        "formato_coma_espacio": 0,
        "formato_solo_coma": 0,
        "formato_espacios": 0
    }
    
    for linea in muestra["lista_adyacencia"].to_list():
        # Detectar el formato de separación
        if ', ' in linea:
            elementos = linea.split(', ')
            formatos_detectados["formato_coma_espacio"] += 1
        elif ',' in linea:
            elementos = linea.split(',')
            formatos_detectados["formato_solo_coma"] += 1
        else:
            elementos = linea.split()
            formatos_detectados["formato_espacios"] += 1
            
        conteo_elementos.append(len(elementos))
        
        # Verificar si todos son números (limpiando espacios)
        elementos_limpios = [elem.strip() for elem in elementos]
        todos_numeros = all(elem.isdigit() for elem in elementos_limpios if elem)
        
        if todos_numeros:
            formatos_detectados["solo_numeros"] += 1
        else:
            formatos_detectados["contiene_no_numeros"] += 1
            
        if len(elementos) == 1:
            formatos_detectados["una_conexion"] += 1
        elif len(elementos) > 1:
            formatos_detectados["multiples_conexiones"] += 1
    
    logging.info(f"Estadísticas de la muestra de {len(conteo_elementos)} líneas:")
    logging.info(f"  - Promedio de elementos por línea: {sum(conteo_elementos)/len(conteo_elementos):.2f}")
    logging.info(f"  - Máximo elementos: {max(conteo_elementos)}")
    logging.info(f"  - Mínimo elementos: {min(conteo_elementos)}")
    logging.info(f"  - Solo números: {formatos_detectados['solo_numeros']}")
    logging.info(f"  - Contiene no números: {formatos_detectados['contiene_no_numeros']}")
    logging.info(f"  - Una conexión: {formatos_detectados['una_conexion']}")
    logging.info(f"  - Múltiples conexiones: {formatos_detectados['multiples_conexiones']}")
    logging.info(f"  - Formato 'coma + espacio': {formatos_detectados['formato_coma_espacio']}")
    logging.info(f"  - Formato 'solo coma': {formatos_detectados['formato_solo_coma']}")
    logging.info(f"  - Formato 'espacios': {formatos_detectados['formato_espacios']}")
    
    return formatos_detectados
