from datetime import datetime
import pandas as pd
import numpy as np

def run(df, stock_maximo, stock_init, p_dias,df_pp,id_product,porcentaje_guarda):
    i = 0
    e_stock = [stock_init]
    aux = True
    fecha_pedido = datetime.now()
    fecha_llegada = datetime.now()
    fechas_ingresos = []
    fechas_pedidos = [] 

    ss = stock_maximo

    #-------------------------------------------------------------------
    
    demora = df_pp[df_pp['m_product_id']== id_product].tiempo_total.values[0]
    demora_dia = round(demora * 7)
    
    fecha_inicio = pd.to_datetime(df['movementdate'].values[0])
    fecha_inicio = pd.to_datetime(fecha_inicio,format='%Y-%m-%d')
    
    fecha_etapa1 = fecha_inicio + pd.Timedelta(days=(demora_dia))
    fecha_etapa1 = pd.to_datetime(fecha_etapa1,format='%Y-%m-%d')

    df_prophet1 = df[df['movementdate'] >= fecha_inicio]
    df_prophet1 = df_prophet1[df_prophet1['movementdate'] <= fecha_etapa1]

    fecha_llegada = fecha_inicio + pd.Timedelta(days=(demora_dia))
    stock_maximo = df_prophet1['movementqty'].sum()
    stock_maximo = (stock_maximo *(-1))

    # Agreamos porcentaje de guarda
    stock_maximo = round(stock_maximo + ((stock_maximo*porcentaje_guarda)/100))

    e_stock = [stock_maximo]

    #-------------------------------------------------------------------

    
    for index, row in df.iterrows():

        if row['movementdate'] >= fecha_llegada:# and not(aux):
            #Llego el pedido

            fecha_1 = pd.to_datetime(row['movementdate'])
            fecha_1 = pd.to_datetime(fecha_1,format='%Y-%m-%d')

            fecha_2 = fecha_1 + pd.Timedelta(days=(demora_dia))
            fecha_2 = pd.to_datetime(fecha_2,format='%Y-%m-%d')

            df_pedido = df[df['movementdate'] > fecha_1]
            df_pedido = df_pedido[df_pedido['movementdate'] <= fecha_2]

            stock_maximo = df_pedido['movementqty'].sum()
            stock_maximo = (stock_maximo *(-1))

            stock_maximo = round(stock_maximo + ((stock_maximo*porcentaje_guarda)/100))
            
            fechas_pedidos.append(row['movementdate'])

            fecha_llegada = row['movementdate'] + pd.Timedelta(days=(demora_dia)) 
            e_stock[i] = e_stock[i-1] + (stock_maximo) 
            fechas_ingresos.append(row['movementdate'] )
            
            aux = True
        
        if e_stock[i] > 0 and (e_stock[i] > -row['movementqty']):
            y = e_stock[i] + row['movementqty']
        else:
            y = 0
            
        e_stock.append(y)
        e_stock[i] = e_stock[i+1]
        i+=1
    # Grafico Stock teorico
    pay = e_stock
    pax = df['movementdate']
    say = []

    if (len(fechas_ingresos) != 0) and (fechas_ingresos[-1] > (df['movementdate'].tolist())[-1]):
        fechas_ingresos.pop()
    else:
        pass
    for i in range(0,len(pay)):
        say.append(ss)
    return pax, pay, say, fechas_pedidos, fechas_ingresos