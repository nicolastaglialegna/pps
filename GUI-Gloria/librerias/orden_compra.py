import pandas as pd
import numpy as np
import time
from datetime import datetime, date, time, timedelta
#---------------------------------------------------------------------------------------------
#                                      orden_compra.py
# 
# Variables de entrada:
# s_proveedor : (String) Proveedor seleccionado
# df_pp: (DataFrame) informacion de nombre, id de productos y sus proveedores
# df_monto_min_provider : (DataFrame)
# df_precios : (DataFrame) informacionde los precios de los productos
# df_min_orden : (DataFrame) informacion de los montos minimos requeridos por los proveedores
# porcentaje_guarda : (INT) Porcentaje de guarda para el stock de seguridad

# Variables de salida:
# df_min_orden : (DataFrame) es la orden de compra sugerida por el sistema
# demora : (INT) demora que tiene el proveedor en entregar los productos a gloria
#---------------------------------------------------------------------------------------------
def run(s_proveedor ,df_pp, df_monto_min_provider , df_precios ,df_min_orden,porcentaje_guarda):
    df_min_orden = df_min_orden[df_min_orden['Proveedor'] == s_proveedor]

    df_min_orden = df_min_orden.drop(['Proveedor'], axis=1)
    df_min_orden['Valor'] = round(df_min_orden['Valor'])
    df_min_orden.drop(['producto_id'], axis=1)

    df_min_orden['Pedido Nº 1'] = round(df_min_orden['Pedido Nº 1'] + ((df_min_orden['Pedido Nº 1']*porcentaje_guarda)/100))
    df_min_orden['Valor'] = round(df_min_orden['Pedido Nº 1'] * df_min_orden['Precio c/u'])

    demora = df_pp[df_pp['proveedor'] == s_proveedor].tiempo_total.values[0]
    demora = int(round(demora * 7))

    return  df_min_orden,demora