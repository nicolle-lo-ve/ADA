import numpy as np
import pandas as pd

def detectar_valores_atipicos_iqr(serie: pd.Series):
    """
    Detecta valores atípicos usando el método del rango intercuartílico (IQR)
    
    Args:
        serie: Serie de pandas con los datos a analizar
    
    Returns:
        Serie de pandas con los valores atípicos detectados
    """
    primer_cuartil = serie.quantile(0.25)
    tercer_cuartil = serie.quantile(0.75)
    rango_intercuartilico = tercer_cuartil - primer_cuartil
    limite_inferior = primer_cuartil - 1.5 * rango_intercuartilico
    limite_superior = tercer_cuartil + 1.5 * rango_intercuartilico
    return serie[(serie < limite_inferior) | (serie > limite_superior)]