import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from matplotlib.pylab import rcParams
import sys
rcParams['figure.figsize'] = 20,10
from keras.models import load_model, model_from_json
import keras
from os import listdir
from os.path import isfile, isdir
import h5py as h5
import tensorflow as tf
import pickle

#-------------------------------------------------------------------------------------------------------
#                   generacion_dataframe_holtwinters.py
#
# Esta funcion toma del DataFrame Productosdeproveedores una lista unica de los proveedores
# y de cada uno de los proveedores genera una lista de todos los productos.
#
# Posee un for que recorre todos los productos de todos los proveedores, por cada producto importamos
# el modelo generado por la funcion holtwinters_model.py
#
# Realizamos el entrenamiento bajo el modelo importado, en este proceso se hace uso del dataframe
# df_transaction del cual se extraen valores historicos.
#
# Se genera un nuevo dataframe por producto con los movementqty, movementdate, m_product_id correspondientes
# Finalmente se realiza un apendizado de los dataframe generando un unico csv que contiene todos los valores
# predecidos por el metodo holtwinters.
#
#-------------------------------------------------------------------------------------------------------




def ls1(path):    
    return [obj for obj in listdir(path) if isfile(path + obj)]

# Path con los modelos
path = '/var/www/FlaskApp/FlaskApp/DataBase/holtwinter_id_model/'

# Path con los DataFrame
path1 = '/var/www/FlaskApp/FlaskApp/DataBase/'

# Lista de los modelos
modelos = ls1(path)

df_transaction = pd.read_csv(path1 + 'df_transaction_week.csv', sep='|')
df_transaction = df_transaction[df_transaction['movementtype']=='C-']
df_transaction.movementqty = abs(df_transaction.movementqty)

df_pp = pd.read_csv(path1 + 'ProductosdeProveedores.csv' ,sep = '|')

df_predic_holt = pd.DataFrame({'movementdate':[], 
                               'movementqty': [], 
                               'proveedor_id': [], 
                               'proveedor_name': [], 
                               'producto_name':[],
                               'm_product_id':[]
                              })
for modelo in modelos:
    # Cargamos el modelo
    
    nombre = modelo.split('.')
    df = df_transaction[df_transaction['m_product_id'] == nombre[0]].reset_index().drop('index', axis = 1)
    df = df[['movementdate','movementqty']]
    df['movementdate'] = pd.to_datetime(df['movementdate'])
    
    df.movementqty = abs(df.movementqty)
    df.movementqty = df.movementqty.replace(0,0.1)
    df.index.freq = 'W'
    
    proveedor_name = df_pp[df_pp['m_product_id'] == nombre[0]].proveedor.unique()
    proveedor_id = df_pp[df_pp['m_product_id'] == nombre[0]].em_csgc_supplier_id.unique()
    producto_name = df_pp[df_pp['m_product_id'] == nombre[0]].producto.unique()
    
    if not(df.empty):
        val_train = round(len(df))
        train = df.iloc[:val_train, 0]
        model_fit_se = pickle.load(open(path + modelo, 'rb'))
        pred = model_fit_se.predict(start=train.index[len(train)-1], end=train.index[len(train)-1]+20)
        
        df_aux = pd.DataFrame({'movementdate':pred.index, 'movementqty':pred.values })
        df_aux['proveedor_id'] = proveedor_id[0]
        df_aux['proveedor_name'] = proveedor_name[0]
        df_aux['producto_name'] = producto_name[0]
        df_aux['m_product_id'] = nombre[0]
        df_predic_holt = df_predic_holt.append(df_aux)

df_predic_holt.movementqty = round(df_predic_holt.movementqty)
df_predic_holt.to_csv(path1 + 'df_predic_holt_week_ApiRest.csv',sep = '|', index=False)

df_predic_holt_month = df_predic_holt.groupby(['proveedor_id', 'proveedor_name','producto_name', 'm_product_id']).resample('M', on='movementdate').sum().reset_index().sort_values(by='movementdate')
df_predic_holt_month.to_csv(path1 + 'df_predic_holt_month_ApiRest.csv',sep='|', index=False)
