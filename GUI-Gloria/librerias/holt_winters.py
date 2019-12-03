import pandas as pd
import numpy as np

#--------------------------------------------------------------------------------
#           holt_winters.py
#
# Esta funcion se utiliza para procesar el DataFrame correspondiente a las predicciones
# en base al indentificador "produto_name"
# Toma como parametros de entrada:
# df_holt: (DataFrame) predicciones
# product_prophet: (String) nombre del producto seleccionado a analizar.
# Parametros de salida: 
# x_train: (INT) valores predecidos.
# y_train: (TimeStamp) Fechas correspondientes a los valores predecidos.
#--------------------------------------------------------------------------------

def run(df_holt, product_prophet):

    df_holt = df_holt[df_holt['producto_name'] == product_prophet]

    px = df_holt.fecha.values.tolist()
    py = df_holt.prediccion.values.tolist()

    x_train = [px]
    y_train = [py]
    
    return x_train, y_train