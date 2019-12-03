import pandas as pd
import numpy as np
from librerias import time_client, time_provider, demanda_media_product, movement_stock, graf_product
z = 1.96
#--------------------------------------------------------------------------------------
#                   sobre_stock2.py
#
# Esta funcion devuelve listas para poder realizar los graficos
# Variables de entrada:
# df_transaction : (DataFrame) informacion de los movimientos de stock
# df_compras : (DataFram) informacion de las compras de gloria a sus proveedores
# df_pp : (DataFrame) informacion de nombre e identificadores de productos y proveedores
# name: (STRING) identificador del producto
# porcentaje_guarda: (INT) porcentaje de guarda para el stock de seguridad
#---------------------------------------------------------------------------------------
def run(df_transaction, df_compras, df_pp , name, porcentaje_guarda):
    dataFrame1 = df_transaction
    
    ProductosdeProveedores = df_pp[df_pp['producto'] == name]
    productId = ProductosdeProveedores['m_product_id'].values
    
    dataFrame1 = dataFrame1[['movementdate','movementqty','m_product_id', "movementtype"]]
    dataFrame1 = dataFrame1[dataFrame1['m_product_id'] == productId[0]]
   
    provider_mean, provider_std = time_provider.run(df_pp, df_compras, productId[0])
    demanda_media,demanda_desviacion = demanda_media_product.run(df_transaction, productId[0])
    
    if np.isnan(provider_mean) or np.isnan(provider_std):
       
        product = df_pp[df_pp["m_product_id"] == productId[0]]
        provider_std = int((product["demora_devstd"].values)[0])
        provider_mean = int((product["demora_media"].values)[0])

        ss = np.around(demanda_media * (provider_mean + provider_std) + (z * demanda_desviacion)) 

        stock_sum = dataFrame1['movementqty'].tolist()
        
        largo = len(stock_sum)
        analizamos = [stock_sum[0]]

        exceso = []
        exceso.append(analizamos[0] - (ss + (2*(provider_mean + provider_std))))
        

        #limite_exceso = (ss + (2*(provider_mean + provider_std)))

        
        for i in range(0, largo):
            if i < largo-1:
                analizamos.append(analizamos[i]+stock_sum[i+1])
                               
                if analizamos[i+1] > (ss + (2*(provider_mean + provider_std))):
                    exceso.append(analizamos[i+1] - (ss + (2*(provider_mean + provider_std))))
                else:
                    exceso.append(0)
        
        rango = len(analizamos)

        
        diferencia_stock = []
        for i in range(0, rango):
            if i < rango:
                if analizamos[i] > (ss + (2*(provider_mean + provider_std))):
                    diferencia_stock.append(analizamos[i] - exceso[i])
                else:
                    diferencia_stock.append(0)
        
    else:
        ss= demanda_media * (provider_mean+provider_std)+ (z * demanda_desviacion)
        ss=np.around(ss)
        
        stock_sum = dataFrame1['movementqty'].tolist()
        
        largo = len(stock_sum)

        analizamos = [stock_sum[0]]
        exceso = [(analizamos[0] - (ss + (2*(provider_mean + provider_std))))]
        
        #limite_exceso = (ss + (2*(provider_mean + provider_std)))
       
        
        for i in range(0, largo):
            if i < largo-1:
                analizamos.append(analizamos[i] + stock_sum[i+1])
                               
                if analizamos[i+1] > (ss + (2*(provider_mean + provider_std))):
                    exceso.append(analizamos[i+1] - (ss + (2*(provider_mean + provider_std))))
                else:
                    exceso.append(0)
        
        rango = len(analizamos)

        diferencia_stock = []
        for i in range(0, rango):
            if i < rango:
                if analizamos[i] > (ss + (2*(provider_mean + provider_std))):
                    diferencia_stock.append(analizamos[i] - exceso[i])
                else:
                    diferencia_stock.append(0)      
    
    dataFrame1['movementdate'] = pd.to_datetime(dataFrame1.movementdate,format='%Y-%m-%d %H:%M:%S')
    rax = dataFrame1['movementdate']

    ss_grafica = [] 
    for i in range(0, rango):
        ss_grafica.append(ss)
    
    ray_movreal = analizamos

    p_dias =  p_dias = pd.Timedelta(days = (provider_mean*7 + provider_std*7))
    stock_maximo = ((ss*porcentaje_guarda)/100) + ss
    stock_maximo = np.around(stock_maximo)

    dataFrame_c = dataFrame1[dataFrame1['m_product_id'] == productId[0]]
    dataFrame_c = dataFrame1[dataFrame1['movementtype'] == "C-"]

    #pax, pay, say, fechas_pedidos, fechas_ingresos = movement_stock.run(dataFrame_c, stock_maximo, stock_maximo, p_dias,df_pp,productId[0],porcentaje_guarda)
       
    x_data = [rax]
    y_data = [ray_movreal]

    return x_data, y_data
