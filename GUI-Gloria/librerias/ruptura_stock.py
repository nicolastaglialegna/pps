import pandas as pd
import numpy as np
from librerias import time_client, time_provider, demanda_media_product, movement_stock, graf_product
z = 1.96
#--------------------------------------------------------------------------------
#           ruptura_stock.py
#
# Esta funcion se utiliza para graficar el movimieno de stock real
# y teorico
# Toma como parametros de entrada:
# df_transaction: (DataFrame) movimiento de stock
# df_orderline : (DataFrame) compras de gloria a sus proveedores
# df_pp : (DataFrame) informacion de lo nombres junto a los id de los productos
# productID : (STRING) identificador del producto
# porcentaje_guarda: (INT) es el marjen de guarda para el sock de seguridad
# proyeccion: (INT) valor que se espera de crecimiento en las ventas
#--------------------------------------------------------------------------------

def run(df_transaction, df_orderline, df_pp, df_compras, productId, porcentaje_guarda, proyeccion):

    print(productId)
    #Obtenemos los datos de media y desviacion standar del tiempo de gloria al cliente
    client_mean, client_std = time_client.run(df_orderline,productId)
    #Obtenemos los datos de media y desviacion standar del tiempo del proveedor a gloria
    provider_mean, provider_std = time_provider.run(df_pp, df_compras, productId)
    #Vemos la demanda media por dia 
    demanda_media, demanda_desviacion = demanda_media_product.run(df_transaction,productId)

    if np.isnan(provider_std):
        provider_std = 1
    if np.isnan(client_std):
        client_std = 1
    if np.isnan(demanda_desviacion):
        demanda_desviacion = 1

    if np.isnan(provider_mean):
        provider_mean = 1
    if np.isnan(client_mean):
        client_mean = 1
    if np.isnan(demanda_media):
        demanda_media = 1
    
    p_dias = pd.Timedelta(days = (provider_mean*7 + provider_std*7))
    ss = demanda_media * (provider_mean+provider_std) + (z * demanda_desviacion)
    ss = np.around(ss)
    stock_maximo = ((ss*porcentaje_guarda)/100) + ss
    stock_maximo = np.around(stock_maximo) 
    
    #-------------------------------------------------------------#
    dataFrame = df_transaction
    dataFrame = dataFrame[['movementdate','movementqty','m_product_id', "movementtype"]]
    dataFrame = dataFrame[dataFrame['m_product_id'] == productId]

    compras = dataFrame[dataFrame['movementtype']=='C-']
    compras = compras[['movementdate','movementqty']]
    compras['movementdate'] = pd.to_datetime(compras.movementdate,format='%Y-%m-%d %H:%M:%S')
    #-------------------Grafico Real-------------------------#
    rax, ray = graf_product.run(df_transaction, productId)

    #------------ Movimientos Teoricos-------------
    pax, pay, say, fechas_pedidos, fechas_ingresos = movement_stock.run(compras, stock_maximo, stock_maximo, p_dias,df_pp,productId,porcentaje_guarda)
 
    #---------- Preparando variables de salida-------------
    # Orden de datos: real, teorico, reposicion, proyeccion
    x_data = [rax,pax,pax]
    y_data = [ray,pay,say]

    return x_data, y_data, fechas_ingresos, fechas_pedidos