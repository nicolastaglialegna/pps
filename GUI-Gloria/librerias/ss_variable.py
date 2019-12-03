import pandas as pd
import os
import datetime
import numpy as np
from datetime import datetime, timedelta
import sys
from librerias import graf_product,demanda_media_product,time_provider,time_client

#-----------------------------------------------------------------------------------------------
#                                       ss_variable.py
# Variables de entrada:
# df_transaction - (DataFrame) informacion de los movimientos de stock
# df_compras - (DataFrame) informacion fechas de compras de producto de gloria a sus proveedores
# df_pp - (DataFrame)  informacion de nombre, id de productos y sus proveedores
# df_orderline - (DataFrame) informacion de ventas de gloria a sus clientes
# name - (STRING) nombre del producto
# porcentaje_guarda - (INT) porcentaje de guarda para el stock de seguridad
# df_predict - (DataFrame) predicciones de ventas por parte del modelo usado
# Variables de salida:
# ax : lista con las fechas de los movimientos de stock
# ay : lista con las cantidad (movimientos) de stock
# fechas_pedidos : fechas en que se estima hacer pedidos de compra de producto al proveeodr
# fechas_ingresos: fechas en las que se estima el arrivo del producto.
#-----------------------------------------------------------------------------------------------
z= 1.96
def run(df_transaction, df_compras, df_pp, df_orderline, name, porcentaje_guarda, df_predict):
    # df_transaction : movimientos de stock
    df_pp = (df_pp[df_pp["producto"] == name].reset_index().drop('index',axis=1))
    productId = df_pp["m_product_id"].values[0]
  
    df_transaction = df_transaction[df_transaction['m_product_id']==productId][['movementdate','movementqty','m_product_id', "movementtype"]]
   
    # DataFrame Con las predicciones
    df_predict = df_predict.rename(columns = {'product_id':'m_product_id','fecha':'movementdate','prediccion':'movementqty'})
    df_predict = df_predict[df_predict['m_product_id'] == productId]
    df_predict = df_predict[["movementdate","movementqty","m_product_id"]]
    df_predict.movementqty = abs(df_predict.movementqty)
    
    # compras totales: transacciones C- (compras de producto)
    compras_totales = df_transaction[df_transaction['movementtype']=='C-'][["m_product_id", "movementdate", "movementqty"]].reset_index().drop('index',axis=1)
    # pasamos las ventas a numero negativo
    df_predict.movementqty = df_predict.movementqty * -1

    compras_totales = compras_totales.append(df_predict).reset_index().drop('index', axis=1)
    compras_totales['movementdate'] = pd.to_datetime(compras_totales.movementdate,format='%Y-%m-%d %H:%M:%S')

    producto_1 = compras_totales[compras_totales['movementdate']>'2017-01-01']
    producto_1['movementdate'] = pd.to_datetime(producto_1.movementdate,format='%Y-%m').dt.to_period('M')
    producto_1 = producto_1.reset_index().drop('index',axis=1)

    producto_1_mean = producto_1.groupby(["m_product_id","movementdate"]).mean().reset_index()
    producto_1_std = producto_1.groupby(["m_product_id","movementdate"]).std().reset_index()
        
    std = []
    mean = []
    ss_prod_compra = []
    ss_min = []
    
    for j in range(0,len(producto_1_std)):
        for i in range(0,len(producto_1)):
            if producto_1["movementdate"][i] == producto_1_std["movementdate"][j]:
                std.append(producto_1_std["movementqty"][j])
            if producto_1["movementdate"][i] == producto_1_mean["movementdate"][j]:
                mean.append((producto_1_mean["movementqty"][j])*(-1))
                

    provider_mean, provider_std = time_provider.run(df_pp, df_compras,productId)
    
    compras_totales['mean'] = mean 
    compras_totales['std'] = std 
    compras_totales["std"].fillna(1, inplace = True)
    compras_totales = compras_totales.reset_index().drop('index',axis=1)
    
    for l in range(0,len(compras_totales)):
        
        if np.isnan(compras_totales["std"][l]):
            print("valores nulos")
            ss = 0
        if np.isnan(provider_mean) or np.isnan(provider_std):
            
            product = df_pp[df_pp["m_product_id"] == productId]

            provider_std = int((product["demora_devstd"].values)[0])
            provider_mean = int((product["demora_media"].values)[0])
            
            std_formula = (compras_totales["std"].values)[l]
            std_formula = np.round(std_formula)

            mean_formula = (compras_totales["mean"].values)[l]
            mean_formula = np.round(mean_formula)

            ss = std_formula + (z * mean_formula)
            ss = np.around(ss)

            stock_maximo = np.around(ss)

            
            ss_min.append(ss)
            ss_prod_compra.append(stock_maximo)
            
        else:            
        
            std_formula = (compras_totales["std"].values)[l]
            std_formula = np.round(std_formula)

            mean_formula = (compras_totales["mean"].values)[l]
            mean_formula = np.round(mean_formula)

            ss = std_formula + (z * mean_formula)
            ss = np.around(ss)
            ss_min.append(ss)

            stock_maximo = np.around(ss)

            ss_prod_compra.append(stock_maximo)
    
    limite_exceso = ss

    compras_totales["ss_maximo"] = ss_prod_compra
    compras_totales["ss_minimo"] = ss_min
    compras_totales["movementtype"] = "C-"

    ax, ay, fechas_pedidos, fechas_ingresos = predictor_estacionario(df_transaction,df_orderline, df_compras, df_pp, compras_totales, productId, name, porcentaje_guarda,limite_exceso,provider_mean,provider_std, df_predict)

    return ax, ay, fechas_pedidos, fechas_ingresos


def predictor_estacionario(df_transaction, df_orderline, df_compras, df_pp, compras_totales, productId, name, porcentaje_guarda, limite_exceso, provider_mean_data, provider_std_data, df_predict):
    # DataFrame para calcular movimiento de stock
    df_transaction = df_transaction[df_transaction['m_product_id'] == productId]
    
    df_predict = df_predict[['movementdate','movementqty']]

    compras = df_transaction[df_transaction['movementtype']=='C-'][['movementdate','movementqty']]
    compras = compras.append(df_predict)
    compras['movementdate'] = pd.to_datetime(compras.movementdate,format='%Y-%m-%d %H:%M:%S')

    stock_maximo = compras_totales["ss_maximo"][0]
  
    #--------------------------------------------------------------#
    i=0
    e_stock = []

    fecha_llegada = datetime.now()
    
    fechas_ingresos = []
    fechas_pedidos = []

    # Demora del proveedor en entregar los productos
    demora = df_pp[df_pp['m_product_id']== productId].tiempo_total.values[0]
    demora_dia = round(demora * 7)

    # Fecha de inicio hostorico para el analisis   
    fecha_inicio = pd.to_datetime(compras['movementdate'].values[0],format='%Y-%m-%d')

    # Etapa1 fecha de inicio + la demora
    fecha_etapa1 = fecha_inicio + pd.Timedelta(days=(demora_dia))
    
    # Recorte de las compras ocurridas en el periodo etapa1
    df_prophet1 = compras[(compras['movementdate'] >= fecha_inicio) & (compras['movementdate'] <= fecha_etapa1)]

    # Fecha en que se espera la recepcion del pedido
    fecha_llegada = fecha_inicio + pd.Timedelta(days=(demora_dia))

    stock_maximo = abs(df_prophet1['movementqty'].sum())
    stock_maximo = round(stock_maximo + ((stock_maximo * porcentaje_guarda)/100))

    e_stock = [stock_maximo]
    
    for index, row in compras_totales.iterrows():
        
        # Nos fijamos si en la fecha del movimiento llego un pedido
        if row['movementdate'] >= fecha_llegada:
            
            # intervalo de tiempo del proximo periodo
            fecha_1 = row['movementdate']
            fecha_2 = fecha_1 + pd.Timedelta(days=(demora_dia))

            # Recorte de ventas del siguiente periodo 
            df_pedido = compras[(compras['movementdate'] >= fecha_1) & (compras['movementdate'] <= fecha_2)]

            # Ventas estimadas al siguiente periodo
            stock_maximo = (df_pedido['movementqty'].sum())*(-1)
            # Ventas del siguiente periodo + un porcentaje de guarda
            stock_maximo = round(stock_maximo + ((stock_maximo * porcentaje_guarda)/100))
            
            fechas_pedidos.append(row['movementdate'])

            fecha_llegada = row['movementdate'] + pd.Timedelta(days=(demora_dia))
            
            fechas_ingresos.append(row['movementdate'])
            if e_stock[i-1] > 0:
                if e_stock[i-1] >= stock_maximo:
                    e_stock[i] = e_stock[i-1]
                else:
                    e_stock[i] = e_stock[i-1] + ((stock_maximo) - e_stock[i-1])
            else:
                e_stock[i] = e_stock[i-1] + (stock_maximo)
                       
        if e_stock[i] > 0 and (e_stock[i] > - row['movementqty']):
            y = e_stock[i] + row['movementqty']
        else:
            y = 0
        e_stock.append(y)
        e_stock[i] = e_stock[i+1]
        i+=1
  
    e_stock.pop()

    # Movimiento real de Stock
    ax_real, ay_real = graf_product.run(df_transaction,productId)

    # Movimiento teorico para el Stock
    ax_teorico = compras_totales['movementdate'].tolist()
    ay_teorico = e_stock
    
    # Stock variable (Linea Verde)
    ax_ss_variable = compras_totales['movementdate']
    ay_ss_variable = compras_totales['ss_maximo']

    inicio = ax_real[0]
    fin    = ax_teorico[-1]

    lista_fechas = [(inicio + timedelta(days=d)).strftime("%Y-%m-%d") for d in range((fin - inicio).days + 1)] 

    df_aux = pd.DataFrame({})
    df_aux['movementdate'] = ax_teorico
    df_aux['movementqty'] = ay_teorico
    df_aux['movementdate'] = pd.to_datetime(df_aux['movementdate'],format='%Y-%m-%d')
    df_aux2 = pd.DataFrame({})
    df_aux2['movementdate'] = lista_fechas
    df_aux2['movementdate'] = pd.to_datetime(df_aux2['movementdate'],format='%Y-%m-%d')
    
    df_aux3 = pd.merge(df_aux2, df_aux, on = 'movementdate', how = 'outer')
    #df_aux3 = df_aux3.fillna(0)
    df_aux3.fillna( method ='ffill', inplace = True) 

    # Lista Con todos los ejes a graficar
    ax = [ax_real, df_aux3.movementdate.tolist(), ax_ss_variable]
    ay = [ay_real, df_aux3.movementqty.tolist(), ay_ss_variable]

    return ax, ay, fechas_pedidos, fechas_ingresos
