import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import datetime
from datetime import datetime
#------------------------------------------------------
#                   predictor.py
# Variables de entrada:
# df_transaction : (DataFrame) movimientos de stock
# df_proph : (DataFrame) prediccion de ventas modelo prophet
# df_pp : (DataFrame) informacion de id, name de productos y sus proveedores
# name : (DataFrame) nombre del producto
# Variables de retorno: 
# x_data : lista con listas de x real, x teorico (para graficar)
# y_data : lista con listas de y real y teorico (para graficar)
#------------------------------------------------------
def run(df_transaction, df_proph, df_pp, name):
    print(name)

    Idproduct = (((df_pp[df_pp["producto"] == name]).m_product_id).values)[0]
    print(Idproduct)
    df_proph = df_proph[ df_proph["m_product_id"] == Idproduct]
 
    if not(df_proph.empty):
        df_transaction = df_transaction[ df_transaction["m_product_id"] == Idproduct]
        df_transaction = df_transaction[ df_transaction["movementtype"] == "C-"].reset_index().drop('index', axis = 1)
        df_transaction['movementqty'] = df_transaction['movementqty']*(-1)
        
        xr = df_transaction['movementdate'].values 
        yr = df_transaction['movementqty'].values

        xff = df_proph['movementdate'].values
        yff = df_proph['movementqty'].values
        
        df_proph['movementdate'] = pd.to_datetime(df_proph.movementdate,format='%Y-%m-%d')
        df_proph = df_proph.groupby(["movementdate"]).sum().reset_index()

        df_proph = df_proph[(df_proph['movementdate'] > '2019-05') & (df_proph['movementdate'] < '2019-09')]

        x_data = [ xr ,xff ]
        y_data = [ yr ,yff ]
        
        flag_alert = False
        alert = ''
            
        str_periodo = (df_proph['movementdate'].apply(str)).to_list()
        str_ventas = ((np.ceil(df_proph['movementqty'])).apply(str)).to_list()

        df_aux = pd.DataFrame({
            'Periodo' :str_periodo,
            'Prophet' : str_ventas,
        })
        RMSE_new = 0

    else:
        alert = 'Datos insuficientes para realizar la prediccion.'
        flag_alert = True
        df_aux = pd.DataFrame({
            'Periodo' :[],
            'Prophet' : [],
        })
        x_data = [[],[]]
        y_data = [[],[]]
        RMSE_new = 0 

    return x_data, y_data