import numpy as np
import time
from datetime import datetime, date, time, timedelta
import pandas as pd
from datetime import datetime
import subprocess
import psycopg2

def run(s_proveedor ,df_pp, df_monto_min_provider ,df_aws, df_precios ,df_min_orden,porcentaje_guarda):
    provider_id = df_pp.em_csgc_supplier_id.values[0]
    df_min_orden = df_min_orden[df_min_orden['provider_cod'] == s_proveedor]
   
    df_min_orden['Valor'] = round(df_min_orden['Valor'])
    df_min_orden.drop(['producto_id'], axis=1)
    df_min_orden['Pedido Nº 1'] = round(df_min_orden['Pedido Nº 1'] + ((df_min_orden['Pedido Nº 1']*porcentaje_guarda)/100))
    df_min_orden['Valor'] = round(df_min_orden['Pedido Nº 1'] * df_min_orden['Precio c/u'])
    demora = df_pp[df_pp['provider_cod'] == s_proveedor].tiempo_total.values[0]
    demora = int(round(demora * 7))
    demora = str(date.today() + timedelta(days=demora))
   
    fecha_llegada = list()
    provider_id_list = list()
    for i in range(0,len(df_min_orden)):
        fecha_llegada.append(demora)
        provider_id_list.append(provider_id)
       
    df_min_orden['date_promised'] = fecha_llegada
    df_min_orden['provider_id'] = provider_id_list
    return  df_min_orden

#path = '/home/futit/Documentos/MachineLearning/db_gloria/'
path = "/var/www/FlaskApp/FlaskApp/DataBase/"

file = 'df_compra_minima.csv'
file2 = 'ProductosdeProveedores.csv'
file3 = 'min_order.csv'
file4  = 'aws_week.csv'
file5 = 'precios.csv'
file6= 'df_compra_minima.csv'
file7 = 'aws_week_v3.csv'
file8 = 'df_transaction_week.csv'

df_transaction = pd.read_csv(path + file8, sep='|')
df_pp = pd.read_csv(path + file2, sep = '|')
df_monto_min_provider = pd.read_csv(path + file3 ,sep = '|')
df_aws = pd.read_csv(path +file4 )
df_precios = pd.read_csv( path + file5, sep = '|')
df_orden_compra = pd.read_csv(path + file6, sep='|')

df_pp['tiempo_total'] = df_pp.demora_devstd + df_pp.demora_media

# Tenemos que dividir el codigo del proveedor del nombre del mismo
codigo_proveedor = list()
nombre_proveedor = list()
codigo_producto = list()
nombre_producto = list()
for i in range(0,len(df_orden_compra)):
    codigo_proveedor.append(df_orden_compra.Proveedor.values[i].split('-')[0].strip())
    nombre_proveedor.append(df_orden_compra.Proveedor.values[i].split('-')[1].strip())
    codigo_producto.append(df_orden_compra.Producto.values[i].split('-')[0].strip())
    nombre_producto.append(df_orden_compra.Producto.values[i].split('-')[1].strip())

# Una vez dividido agregamos por separado codigo - nombre al dataframe    
df_orden_compra = df_orden_compra.drop(['Proveedor'], axis=1)
df_orden_compra = df_orden_compra.drop(['Producto'], axis=1)
df_orden_compra['provider_cod'] = codigo_proveedor
df_orden_compra['provider_name'] = nombre_proveedor
df_orden_compra['product_cod'] = codigo_producto
df_orden_compra['product_name'] = nombre_producto
df_orden_compra = df_orden_compra[['provider_cod','provider_name','product_cod','product_name','producto_id','Stock Actual','Ventas Periodo Nº1','Ventas Periodo Nº2','Ventas Perdidas','Pedido Nº 1','Precio c/u','Valor']]

# Divido el codigo del producto y proveedor de los nombres
codigo_proveedor = list()
nombre_proveedor = list()
codigo_producto = list()
nombre_producto = list()
for i in range(0,len(df_pp)):
    codigo_proveedor.append(df_pp.proveedor.values[i].split('-')[0].strip())
    nombre_proveedor.append(df_pp.proveedor.values[i].split('-')[1].strip())
    codigo_producto.append(df_pp.producto.values[i].split('-')[0].strip())
    nombre_producto.append(df_pp.producto.values[i].split('-')[1].strip())
    
df_pp = df_pp.drop(['proveedor'], axis=1)
df_pp = df_pp.drop(['producto'], axis=1)
df_pp['provider_cod'] = codigo_proveedor
df_pp['provider_name'] = nombre_proveedor
df_pp['product_cod'] = codigo_producto
df_pp['product_name'] = nombre_producto 
df_pp = df_pp[['provider_cod','provider_name','product_cod','product_name','em_csgc_supplier_id','m_product_id','demora_devstd','demora_media','tiempo_total']] 

# Generamos la orden de compra sugerida por el sistema df_orden
codigos = df_pp.provider_cod.unique().tolist()
df_orden = pd.DataFrame({})
for codigo in codigos:
    df_orden = df_orden.append(run(codigo ,df_pp, df_monto_min_provider ,df_aws, df_precios ,df_orden_compra ,0))
    
df_orden = df_orden.rename(columns={"Ventas Periodo Nº1": "forecast_sales_1", "Ventas Periodo Nº2": "forecast_sales_2"})

# Acomodamos las fechas al formato necesario para OpenBravo
lista_fechas = list()
for index, row in df_orden.iterrows():
    partes = row['date_promised'].split("-")
    convertida = "-".join(reversed(partes))
    lista_fechas.append(convertida)
df_orden.date_promised = lista_fechas


############################################
############ Subimos datos #################
############################################

connection = psycopg2.connect(user = "tad",
                              password = "tad",
                              host = "127.0.0.1",
                              port = "5432",
                              database = "machine")
cursor = connection.cursor()

query_values = list()
for index, values in df_orden.iterrows():
    values_list = list()
    values_list.append(values['provider_cod'])
    values_list.append(values['provider_name'])
    values_list.append(values['provider_id'])
    values_list.append(values['product_cod'])
    values_list.append(values['product_name'].replace("'",""))
    values_list.append(values['producto_id'])
    values_list.append(values['forecast_sales_1'])
    values_list.append(values['forecast_sales_2'])
    values_list.append(values['date_promised'])
    query_values.append(values_list)

# Limpieza de la tabla
cursor.execute('DELETE FROM forecast_api')
connection.commit()

for query in query_values:
    consulta = """ INSERT INTO forecast_api (provider_cod, provider_name, provider_id, product_cod, product_name, product_id, forecast_sales_1, forecast_sales_2, date_promised)
    VALUES """ + str(tuple(query))
    cursor.execute(consulta)

connection.commit()