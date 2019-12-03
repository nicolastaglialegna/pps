import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime
import numpy as np
import matplotlib.lines as mlines
from datetime import datetime, timedelta
from matplotlib.pylab import rcParams
import sys
rcParams['figure.figsize'] = 20,10
import plotly.offline as py
import warnings
warnings.filterwarnings("ignore")

path = '/var/www/FlaskApp/FlaskApp/DataBase/'

filename1 = 'ProductosdeProveedores.csv'
filename2 = 'df_transaction_week.csv'
filename3 = 'min_order.csv'
filename4 = 'precios.csv'
filename5 = 'df_predic_holt_week_ApiRest.csv'

df_pp = pd.read_csv(path + filename1,sep="|",header=0)

df_transaction = pd.read_csv(path + filename2,sep="|",header=0)
df_transaction['movementdate'] = pd.to_datetime(df_transaction['movementdate'])

df_compra = pd.read_csv(path + filename3,sep="|",header=0)

df_precios = pd.read_csv(path + filename4,sep="|",header=0)
df_precios['validfrom'] = pd.to_datetime(df_precios['validfrom'])
df_precios = df_precios[df_precios['validfrom']>= '2014']

df_predictor = pd.read_csv(path + filename5,sep="|",header=0)
df_predictor['movementdate'] = pd.to_datetime(df_predictor['movementdate'])

def stock_actual(df_transaction, productId):
    dataFrame = df_transaction
    dataFrame = dataFrame[['movementdate','movementqty','m_product_id', "movementtype"]]
    dataFrame = dataFrame[dataFrame['m_product_id'] == productId]
    if dataFrame.empty == False :
        stock_sum = dataFrame['movementqty'].tolist()
        largo = len(stock_sum)
        analizamos = []
        analizamos.append(stock_sum[0])
        for i in range(0, largo):
            if i < largo-1:
                analizamos.append(analizamos[i]+stock_sum[i+1])
        dataFrame['movementdate'] = pd.to_datetime(dataFrame.movementdate,format='%Y-%m-%d %H:%M:%S')

        ax = dataFrame['movementdate']
        ay = analizamos
    else:
        ax = []
        ay = [0]
    return ay[-1]

#Tomamos en primer instancia el proveedor a analizar
list_provider =  df_pp['proveedor'].unique().tolist()

df_compra_minima = pd.DataFrame({})

for proveedor in list_provider:
    print(proveedor)

    df_proveedor = df_pp[df_pp['proveedor'] == proveedor].reset_index().drop('index',axis=1)
    list_product = df_proveedor["producto"].values.tolist()
    list_product.sort()

    #Analizamos el stock actual cada uno de esos productos
    unidad_actual = list()
    list_ID = list()
    list_tiempo_demora = list()
    list_demora = list()
    list_prophet_etapa1 = list()
    list_prophet_etapa2 = list()
    list_ventas_perdidas = list()
    list_pedidos = list()
    list_price = list()
    list_date_price = list()

    x=0

    for producto in list_product:    
        productId = (df_pp[(df_pp['producto']==producto)]['m_product_id'].values)[0]
        list_ID.append(productId)
        unidad = stock_actual(df_transaction, productId)
        unidad_actual.append(unidad)

        tiempo_demora = df_proveedor[df_proveedor['m_product_id']== productId].tiempo_total.values[0]
        list_tiempo_demora.append(tiempo_demora)

        #Comparamos las unidades actuales con la prediccion de ventas durante el tiempo de demora del proveedor.
        fecha_inicio = datetime.now()
        fecha_inicio = pd.to_datetime(fecha_inicio,format='%Y-%m-%d')

        demora = df_proveedor[df_proveedor['producto']== producto].tiempo_total.values[0]
        demora = round(demora)
        list_demora.append(demora)

        fecha_etapa1 = fecha_inicio + timedelta(days=(demora * 7))
        fecha_etapa1 = pd.to_datetime(fecha_etapa1,format='%Y-%m-%d')

        df_prophet = df_predictor[df_predictor['m_product_id']==productId]

        df_prophet1 = df_prophet[df_prophet['movementdate']>=fecha_inicio]
        df_prophet1 = df_prophet1[df_prophet1['movementdate']<=fecha_etapa1]
        stock_prophet1 = df_prophet1['movementqty'].sum()

        fecha_etapa2 = fecha_etapa1 + timedelta(days=(demora * 7))
        fecha_etapa2 = pd.to_datetime(fecha_etapa2,format='%Y-%m-%d')

        df_prophet2 = df_prophet[df_prophet['movementdate']>=fecha_etapa1]
        df_prophet2 = df_prophet2[df_prophet2['movementdate']<=fecha_etapa2]
        stock_prophet2 = df_prophet2['movementqty'].sum()


        list_prophet_etapa1.append(stock_prophet1)
        list_prophet_etapa2.append(stock_prophet2)

        # Comparacion para compra minima

    df_date = pd.DataFrame({'Producto':list_product, 'm_product_id':list_ID,'Stock_actual':unidad_actual, 'venta_prophet_1':list_prophet_etapa1,'venta_prophet_2':list_prophet_etapa2})

    for producto in list_product: 
        productId = (df_pp[(df_pp['producto']==producto)]['m_product_id'].values)[0]
        if df_date['Stock_actual'][x] > df_date['venta_prophet_1'][x]:
            stock_inicial_etapa2 = df_date['Stock_actual'][x] - df_date['venta_prophet_1'][x]
            if stock_inicial_etapa2 > df_date['venta_prophet_2'][x]:
                pedido = 0
                ventas_perdidas_estimadas = 0
            else:
                pedido = df_date['venta_prophet_2'][x] - stock_inicial_etapa2
                ventas_perdidas_estimadas = 0
        #         print('El pedido es de: ', pedido)
        else:
            pedido = df_date['venta_prophet_2'][x]
            ventas_perdidas_estimadas = df_date['venta_prophet_1'][x] - df_date['Stock_actual'][x]
        #         print('Ventas perdidas estimadas: ', ventas_perdidas_estimadas)
        #         print('El pedido es de: ', pedido)
        x+=1

        list_ventas_perdidas.append(ventas_perdidas_estimadas)
        list_pedidos.append(pedido)

    #     print(productId)

        if not(df_precios[df_precios['m_product_id']==productId].empty):    
            product_price = df_precios[df_precios['m_product_id']==productId].pricelist.values[0]
            date_price = df_precios[df_precios['m_product_id']==productId].validfrom.values[0]
        else:
            product_price = 0
            date_price = fecha_inicio

        list_date_price.append(date_price)
        list_price.append(product_price)

    total_price_product = np.multiply(list_pedidos,list_price).tolist()
    df_minorder = pd.DataFrame({'Proveedor':proveedor,'Producto':list_product,'m_producto_id':list_ID,'Stock Actual':unidad_actual, 'Ventas Periodo Nº1':list_prophet_etapa1,'Ventas Periodo Nº2':list_prophet_etapa2,'Ventas Perdidas':list_ventas_perdidas,'Pedido Nº 1':list_pedidos,'Precio c/u':list_price,'Valor':total_price_product})

    total_orden = round(df_minorder['Valor'].sum())
    fecha_llegada = fecha_etapa1

    list_total_orden = list()
    list_total_orden.append(total_orden)
    list_fecha_llegada = list()
    list_fecha_llegada.append(fecha_llegada)

    # Vemos cual es el valor de orden minima impuesta por el proveedor
    if not(df_compra[df_compra['proveedor']== proveedor].empty):    
        orden_min = df_compra[df_compra['proveedor']== proveedor].orden_minima.values[0]
    else:
        orden_min = 0

    if total_orden >= orden_min:
        print('Se genero el pedido para el proveedor: ',proveedor)
        df_total = pd.DataFrame({'Total_compra':list_total_orden, 'Fecha_llegada':list_fecha_llegada})
    else:
        print('Recalcular pedido')
        
    df_compra_minima = df_compra_minima.append(df_minorder)
    
    print(df_compra_minima)

df_compra_minima = df_compra_minima.reset_index().drop('index',axis=1)

df_compra_minima.to_csv (path + 'df_compra_minima.csv',sep="|", index = None, header=True) 