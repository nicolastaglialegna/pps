import pandas as pd
import numpy as np
#-----------------------------------------------------------------
#                        Sales.py
# Esta funcion toma como parametros de entrada:
# df_transaction: proveniente de la tabla m_transaction
# df_pp: Contiene el nombre y ID de cada producto y su proveedor
# producto: Es el nombre del producto seleccionado
# Los parametros de salida son 2 listas (x,y) para realizar los graficos
# de las ventas del producto seleccionado.
# Ademas devuelve 
#-----------------------------------------------------------------
def run(df_transaction, df_pp, producto):

    product_id = df_pp[df_pp['producto'] == producto]['m_product_id'].values[0]
    if not(df_transaction[(df_transaction['m_product_id'] == product_id) & (df_transaction['movementtype'] == 'C-') ].empty):
        y = (df_transaction[(df_transaction['m_product_id'] == product_id) & (df_transaction['movementtype'] == 'C-')].movementqty*(-1)).values
        x  = (df_transaction[(df_transaction['m_product_id'] == product_id) & (df_transaction['movementtype'] == 'C-')].movementdate).values
        flag_alert = False

    else:
        y = []
        x = []
        flag_alert = True

    return x , y, flag_alert