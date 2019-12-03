import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime
import numpy as np
import matplotlib.lines as mlines
from datetime import datetime
from matplotlib.pylab import rcParams
import sys
rcParams['figure.figsize'] = 20,10
from scipy import fftpack
import statsmodels.api as sm
from datetime import datetime, timedelta
from scipy.stats import boxcox
from scipy.special import inv_boxcox
import psycopg2
import warnings
warnings.filterwarnings("ignore")

#-------------------------------------------------------------------------------------------------------
#                       script_generador_csv.py
# 
# En esta funcion se lleva a cabo el proceso de generacion de los dataframe a utilizar 
# para la visualizacion grafica en la aplizacion web.
#
# Se conecta a la base de datos montada en el server, se realizan las consultas correspondientes
# para luego comenzar con la limpieza y analisis de los datos hasta concluir con el dataframe final.
#-------------------------------------------------------------------------------------------------------

path_guardar_datos = '/var/www/FlaskApp/FlaskApp/DataBase/'


connection = psycopg2.connect(user = "tad",
                                  password = "tad",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "machine")


def time_previder(df_compras, product_id):
    df_compras= df_compras[df_compras['m_product_id']==product_id]
    provider_mean = df_compras[df_compras['time_proveedor']<12]
    provider_mean = df_compras['time_proveedor'].mean(axis=0)
    provider_mean = np.around(provider_mean)
    provider_std  = df_compras['time_proveedor'].std(axis=0)
    provider_std  = np.around(provider_std)
    return provider_mean, provider_std

def time_client(df_orderline,product_id):
    df_orderline = df_orderline[df_orderline['m_product_id']==product_id]
    
    fecha_limite = 7
    df_orderline = df_orderline[df_orderline['time_cliente']<fecha_limite]
    
    client_mean = df_orderline['time_cliente'].mean(axis=0)
    client_mean = client_mean
    client_mean = np.around(client_mean)
    client_std  = df_orderline['time_cliente'].std(axis=0)
    client_std  = np.around(client_std)
    return client_mean, client_std
def freq_analizing(y):
    time_step = 0.02
    period = 5.

    time_vec = np.arange(0, 20, time_step)
    sig = np.sin(2 * np.pi / period * time_vec) + \
          0.5 * np.random.randn(time_vec.size)

    sample_freq = fftpack.fftfreq(y.size, d=time_step)
    sig_fft = fftpack.fft(sig)
    pidxs = np.where(sample_freq > 0)
    freqs, power = sample_freq[pidxs], np.abs(sig_fft)[pidxs]
    freq = freqs[power.argmax()]
    
    return freq.max()

#####################################
# DataFrame df_orderline
#####################################

filename = 'df_orderline.csv'
df_orderline = pd.read_sql_query('SELECT m_product_id,dateordered,dateinvoiced from c_orderline',con=connection)
df_orderline= df_orderline[df_orderline['dateordered']>'2017-01-01']
df_orderline = df_orderline.dropna()
df_orderline['dateordered'] = pd.to_datetime(df_orderline.dateordered,format='%Y-%m-%d %H:%M:%S')
df_orderline['dateinvoiced'] = pd.to_datetime(df_orderline.dateinvoiced,format='%Y-%m-%d %H:%M:%S')
df_orderline['time_cliente']=(df_orderline['dateinvoiced']-df_orderline['dateordered']).dt.days/7
df_orderline.to_csv (path_guardar_datos + filename,sep="|", index = None, header=True) 

print('Se genero el DataFrame df_orderline')

################################################
# df_transaction_week and df_transaction_month #
################################################

df_transaction = pd.read_sql_query('SELECT m_product_id, movementtype, movementdate, movementqty from m_transaction',con=connection)
df_transaction['movementdate'] = pd.to_datetime(df_transaction['movementdate'])
df_transaction1 = df_transaction.groupby(['m_product_id','movementtype']).resample('W', on='movementdate').sum().reset_index().sort_values(by='movementdate')
df_transaction1 =  df_transaction1.sort_values(['m_product_id', 'movementdate','movementtype'], ascending=[1,1, 0])
export_transaction = df_transaction1.to_csv (path_guardar_datos + 'df_transaction_week.csv',sep='|', index = None, header=True)

df_transaction2 = df_transaction.groupby(['m_product_id','movementtype']).resample('M', on='movementdate').sum().reset_index().sort_values(by='movementdate')
df_transaction2 =  df_transaction2.sort_values(['m_product_id', 'movementdate','movementtype'], ascending=[1,1, 0])
export_transaction1 = df_transaction2.to_csv (path_guardar_datos + 'df_transaction_month.csv',sep='|', index = None, header=True)

print('Se genero el DataFrame df_transaction_week and df_transaction_month')

#########################
# DataFRAME Precios.csv #
#########################

query = """select pl.validfrom, pl.name , pp.m_product_id  , pp.pricelist
from m_productprice pp, m_pricelist_version pl
where pl.m_pricelist_version_id = pp.m_pricelist_version_id
-- and m_product_id='35434284273745E787F035DAE0AEAC38'
order by pp.m_product_id  , 1"""

df_precios = pd.read_sql_query(query,con=connection)
df_precios.to_csv (path_guardar_datos + 'precios.csv', sep='|', index = None, header=True)

print('Se genero el DataFrame Precios.csv')


########################
# DataFRAME df_compras #
########################

query = """select m_product_id, dateordered, datedelivered, datepromised,
       qtyordered, qtyreserved, qtydelivered , datepromised-dateordered demora
from c_orderline
where ad_org_id ='37A30F4867C844B2AFBDC862A74A9076'
   and qtyordered>0 and qtydelivered<=0 and datepromised is not null
 order by dateordered desc"""

df_compras = pd.read_sql_query(query,con=connection)
df_compras= pd.DataFrame({'m_product_id':df_compras['m_product_id'], 'dateordered':df_compras['dateordered'],'datepromised':df_compras['datepromised'] })
df_compras= df_compras[df_compras['dateordered']>'2017-01-01']
df_compras['dateordered'] = pd.to_datetime(df_compras.dateordered,format='%Y-%m-%d %H:%M:%S')
df_compras['datepromised'] = pd.to_datetime(df_compras.datepromised,format='%Y-%m-%d %H:%M:%S')
df_compras['time_proveedor']=(df_compras['datepromised']-df_compras['dateordered']).dt.days/7
df_compras= df_compras[df_compras['time_proveedor']<=37]
df_compras.to_csv (path_guardar_datos + 'compras_gloria.csv',sep='|', index = None, header=True)

print('Se genero el DataFrame df_compras.csv')

######################################
# Productos por proveedores REST API #
######################################

query = """select p.EM_Csgc_Supplier_ID, bp.value as provider_cod, bp.name as Proveedor,
        p.m_product_id, p.value as product_cod, p.name as Producto , p.isactive, b.name as Marca
        from c_bpartner  bp  inner join m_product p  on p.EM_Csgc_Supplier_ID = bp.c_bpartner_id
        left join m_brand b on b.m_brand_id= p.m_brand_id
       where bp.isvendor='Y' and bp.isactive='Y' """

productodeproveedores = pd.read_sql_query(query,con=connection)

list_provider_mean = list()
list_provider_std = list()

for row in productodeproveedores.iterrows():

    product_id = row[1][2]

    if not(df_compras['m_product_id']== product_id).empty:
        provider_mean, provider_std = time_previder(df_compras, product_id)

    else:
        print('es nulo')
    list_provider_mean.append(provider_mean)
    list_provider_std.append(provider_std)

df_pp = pd.DataFrame({'em_csgc_supplier_id':productodeproveedores['em_csgc_supplier_id'],
                    'provider_cod':productodeproveedores['provider_cod'],
                    'proveedor':productodeproveedores['proveedor'],
                    'product_cod':productodeproveedores['product_cod'],
                    'm_product_id':productodeproveedores['m_product_id'],
                    'producto':productodeproveedores['producto'],
                    'isactive':productodeproveedores['isactive'],
                    'marca':productodeproveedores['marca'],
                     'mean_time_prod':list_provider_mean,
                     'std_time_prod':list_provider_std})

df_pp = df_pp.fillna(1)
list_provider = df_pp['proveedor'].unique()
df_pp_final = pd.DataFrame({})

for proveedor in list_provider:
    
    df_proveedores = df_pp[df_pp['proveedor'] == proveedor]
    df_proveedores = df_proveedores.dropna()
    mean_provider = df_proveedores['mean_time_prod'].mean(axis=0)
    std_provider = df_proveedores['std_time_prod'].mean(axis=0)
    df_proveedores['demora_devstd'] = mean_provider
    df_proveedores['demora_media'] = std_provider
    df_pp_final = df_pp_final.append(df_proveedores)
    
df_pp_final['tiempo_total'] = df_pp_final['demora_devstd'] + df_pp_final['demora_media']
df_pp = df_pp_final.copy()
df_week_original = df_transaction1.copy()
list_proveedores = df_pp['proveedor'].unique().tolist()
df_frecuencias = df_pp.copy()
df_frecuencias['Frecuencias'] = 0
df_final = pd.DataFrame({})
k=0

for i in list_proveedores:
    list_status = []
    df_frec = df_frecuencias[df_frecuencias['proveedor']==i].reset_index().drop('index',axis=1)
    list_product = df_frec[df_frec['proveedor']==i]['m_product_id'].tolist()
        
    for product in list_product:
        df_week = df_week_original.copy()
        df_week = df_week[df_week["m_product_id"] == product]
        df_week = df_week[df_week['movementtype'] == 'C-']
        df_week['movementdate'] = pd.to_datetime(df_week.movementdate,format='%Y-%m-%d')
        df_week["my_weekday"] = df_week.movementdate.dt.week
        df_week['movementqty'] = (df_week['movementqty']*(-1))
        df_week = df_week.rename(columns={'movementdate': 'ds', 'movementqty': 'y'})
        df_week = df_week[['ds', 'y']]
        df_week = df_week.replace(to_replace=0, value=df_week.y.mean(), regex=True)
        
        y = df_week['y'].values
        y = np.ceil(y).astype(int)

        if (y.shape[0] > 2):
            freq_value = freq_analizing(y)
            if (freq_value > 1) and (freq_value < 2) :
                 
                list_status.append(1)
            else:
             
                list_status.append(0)
        else:
            list_status.append(0)
        
    k+=1
            
    df_frec["Frecuencias"] = list_status

    df_final = df_final.append(df_frec)
    
df_final.reset_index().drop('index',axis=1)
    
df_pp_analisis = df_final[df_final['Frecuencias']==0].reset_index().drop('index',axis=1)
list_product = df_pp_analisis['producto'].tolist()
list_productID = df_pp_analisis['m_product_id'].tolist()

list_product_activate = []
list_product_inactive = []
lista_negra = []
lista_nuevos_2018 = []
lista_nuevos_2019 = []
lista_activos_posibles_predecibles = []
lista_activos_predecibles = []
lista_descripcion = []

for ID in list_productID:
    df_analisis = df_transaction[df_transaction["m_product_id"]==ID]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))

    df_week = df_week.rename(columns={'movementdate': 'ds',
                        'movementqty': 'y'})
    if df_week[df_week['ds']>'2018-12-31'].empty:
        list_product_inactive.append(ID)
        lista_negra.append(ID)
        lista_descripcion.append('No tiene datos durante el 2019')
        
    else:
        list_product_activate.append(ID)

        
for product_activate in list_product_activate:
    
    df_analisis = df_transaction[df_transaction["m_product_id"]==product_activate]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))

    df_week = df_week.rename(columns={'movementdate': 'ds','movementqty': 'y'})
    
    if df_week[df_week['ds']<'2018-01-01'].empty:
        if df_week[df_week['ds']<'2019-01-01'].empty:
            lista_nuevos_2019.append(product_activate)
            lista_negra.append(product_activate)
            lista_descripcion.append('Producto nuevo durante el 2019')
        else:
            lista_nuevos_2018.append(product_activate)
            lista_negra.append(product_activate)
            lista_descripcion.append('Producto nuevo durante el 2018')
    else:
        lista_activos_posibles_predecibles.append(product_activate)

for ID_activos in lista_activos_posibles_predecibles:
    
    df_analisis = df_transaction[df_transaction["m_product_id"]==ID_activos]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))

    df_week = df_week.rename(columns={'movementdate': 'ds','movementqty': 'y'})
    
    año2017 = df_week[df_week["ds"]<"2018-01-01"] 
    año2018 = df_week[df_week["ds"]>"2018-01-01"] 
    año2018 = año2018[año2018["ds"]<"2019-01-01"]
    año2019 = df_week[df_week["ds"]>="2019-01-01"]
    
    if año2017['y'].sum()<=0 or año2018['y'].sum()<=0 or año2019['y'].sum()<=0:
        lista_negra.append(ID_activos)
        if año2017['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2017')
        elif año2018['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2018')
        elif año2019['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2019')
    else:
        lista_activos_predecibles.append(ID_activos)

df_pp_frec_1 = df_final[df_final['Frecuencias']==1].reset_index().drop('index',axis=1)
df_pp_frec_0 = df_final[df_final['Frecuencias']==0].reset_index().drop('index',axis=1)
df_pp_frec_0_final = pd.DataFrame({}) 
df_pp_frec_2_final = pd.DataFrame({}) 

for product_predecible in lista_activos_predecibles:
    df_pp_frec_0_reducido = df_pp_frec_0[df_pp_frec_0['m_product_id']==product_predecible]
    df_pp_frec_0_final = df_pp_frec_0_final.append(df_pp_frec_0_reducido)
    
for product_no_predecible in lista_negra:
    df_pp_frec_2_reducido = df_pp_frec_0[df_pp_frec_0['m_product_id']==product_no_predecible]
    df_pp_frec_2_final = df_pp_frec_2_final.append(df_pp_frec_2_reducido)

df_pp_frec_0_final = df_pp_frec_0_final.reset_index().drop('index',axis=1)
df_pp_frec_2_final = df_pp_frec_2_final.reset_index().drop('index',axis=1)
df_pp_frec_2_final['Frecuencias'] = 2 

df_pp_frec_1['Descripcion'] = 'Corresponde al modelo 1'
df_pp_frec_0_final['Descripcion'] = 'Corresponde al modelo 0'
df_pp_frec_2_final['Descripcion'] = lista_descripcion

df_pp_final = df_pp_frec_1
df_pp_final = df_pp_final.append(df_pp_frec_0_final)
df_pp_final = df_pp_final.append(df_pp_frec_2_final)

df_pp_final = df_pp_final.sort_values(by=['product_cod'], ascending = True)
    
df_pp_final.to_csv (path_guardar_datos + 'ProductosdeProveedores_REST_API.csv', sep='|', index = None, header=True)
print('Se genero el DataFrame Productos por proveedores REST API')

#####################################
# Productos por proveedores APP web #
#####################################

query = """ select p.EM_Csgc_Supplier_ID, bp.value||' - '||bp.name as Proveedor,
        p.m_product_id, p.value||' - '||p.name as Producto , p.isactive, b.name as Marca
        from c_bpartner  bp  inner join m_product p  on p.EM_Csgc_Supplier_ID = bp.c_bpartner_id
        left join m_brand b on b.m_brand_id= p.m_brand_id
       where bp.isvendor='Y' and bp.isactive='Y'  """

productodeproveedores = pd.read_sql_query(query,con=connection)

list_provider_mean = list()
list_provider_std = list()

for row in productodeproveedores.iterrows():

    product_id = row[1][2]

    if not(df_compras['m_product_id']== product_id).empty:
        provider_mean, provider_std = time_previder(df_compras, product_id)

    else:
        print('es nulo')
    list_provider_mean.append(provider_mean)
    list_provider_std.append(provider_std)

df_pp = pd.DataFrame({'em_csgc_supplier_id':productodeproveedores['em_csgc_supplier_id'],
                    'proveedor':productodeproveedores['proveedor'],
                    'm_product_id':productodeproveedores['m_product_id'],
                    'producto':productodeproveedores['producto'],
                    'isactive':productodeproveedores['isactive'],
                    'marca':productodeproveedores['marca'],
                     'mean_time_prod':list_provider_mean,
                     'std_time_prod':list_provider_std})

df_pp = df_pp.fillna(1)
df_pp.head()
list_provider = df_pp['proveedor'].unique()
df_pp_final = pd.DataFrame({})

for proveedor in list_provider:
    
    df_proveedores = df_pp[df_pp['proveedor'] == proveedor]
    df_proveedores = df_proveedores.dropna()
    mean_provider = df_proveedores['mean_time_prod'].mean(axis=0)
    std_provider = df_proveedores['std_time_prod'].mean(axis=0)
    df_proveedores['demora_devstd'] = mean_provider
    df_proveedores['demora_media'] = std_provider
    
    df_pp_final = df_pp_final.append(df_proveedores)
    
df_pp_final['tiempo_total'] = df_pp_final['demora_devstd'] + df_pp_final['demora_media']
df_pp = df_pp_final.copy()
df_week_original = df_transaction1.copy()

list_proveedores = df_pp['proveedor'].unique().tolist()
df_frecuencias = df_pp.copy()
df_frecuencias['Frecuencias'] = 0
df_final = pd.DataFrame({})

k=0
for i in list_proveedores:
    
    list_status = []
    df_frec = df_frecuencias[df_frecuencias['proveedor']==i].reset_index().drop('index',axis=1)
    list_product = df_frec[df_frec['proveedor']==i]['m_product_id'].tolist()
        
    for product in list_product:
        df_week = df_week_original.copy()
        df_week = df_week[df_week["m_product_id"] == product]
        df_week = df_week[df_week['movementtype'] == 'C-']
        df_week['movementdate'] = pd.to_datetime(df_week.movementdate,format='%Y-%m-%d')
        df_week["my_weekday"] = df_week.movementdate.dt.week
        df_week['movementqty'] = (df_week['movementqty']*(-1))
        df_week = df_week.rename(columns={'movementdate': 'ds', 'movementqty': 'y'})
        df_week = df_week[['ds', 'y']]
        df_week = df_week.replace(to_replace=0, value=df_week.y.mean(), regex=True)
        
        y = df_week['y'].values
        y = np.ceil(y).astype(int)

        if (y.shape[0] > 2):
            freq_value = freq_analizing(y)
            if (freq_value > 1) and (freq_value < 2) :
                list_status.append(1)
            else:
                list_status.append(0)
        else:
            list_status.append(0)
        
    k+=1

    df_frec["Frecuencias"] = list_status
    df_final = df_final.append(df_frec)
    
df_final.reset_index().drop('index',axis=1)
df_pp_analisis = df_final[df_final['Frecuencias']==0].reset_index().drop('index',axis=1)
list_product = df_pp_analisis['producto']
list_productID = df_pp_analisis['m_product_id']

list_product_activate = []
list_product_inactive = []
lista_negra = []
lista_nuevos_2018 = []
lista_nuevos_2019 = []
lista_activos_posibles_predecibles = []
lista_activos_predecibles = []
lista_descripcion = []

for ID in list_productID:
    df_analisis = df_transaction[df_transaction["m_product_id"]==ID]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))
    df_week = df_week.rename(columns={'movementdate': 'ds','movementqty': 'y'})
    if df_week[df_week['ds']>'2018-12-31'].empty:
        list_product_inactive.append(ID)
        lista_negra.append(ID)
        lista_descripcion.append('No tiene datos durante el 2019')
        
    else:
        list_product_activate.append(ID)

        
for product_activate in list_product_activate:
    
    df_analisis = df_transaction[df_transaction["m_product_id"]==product_activate]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))

    df_week = df_week.rename(columns={'movementdate': 'ds','movementqty': 'y'})
    
    if df_week[df_week['ds']<'2018-01-01'].empty:
        if df_week[df_week['ds']<'2019-01-01'].empty:
            lista_nuevos_2019.append(product_activate)
            lista_negra.append(product_activate)
            lista_descripcion.append('Producto nuevo durante el 2019')
        else:
            lista_nuevos_2018.append(product_activate)
            lista_negra.append(product_activate)
            lista_descripcion.append('Producto nuevo durante el 2018')
    else:
        lista_activos_posibles_predecibles.append(product_activate)

for ID_activos in lista_activos_posibles_predecibles:
    
    df_analisis = df_transaction[df_transaction["m_product_id"]==ID_activos]
    df_analisis = df_analisis[df_analisis['movementtype']=='C-'].reset_index().drop('index',axis=1)
    df_week = df_analisis[['movementdate','movementqty']]
    df_week['movementqty'] = (df_week['movementqty']*(-1))
    df_week = df_week.rename(columns={'movementdate': 'ds','movementqty': 'y'})
    año2017 = df_week[df_week["ds"]<"2018-01-01"] 
    año2018 = df_week[df_week["ds"]>"2018-01-01"] 
    año2018 = año2018[año2018["ds"]<"2019-01-01"]
    año2019 = df_week[df_week["ds"]>="2019-01-01"]
    
    if año2017['y'].sum()<=0 or año2018['y'].sum()<=0 or año2019['y'].sum()<=0:
        lista_negra.append(ID_activos)
        if año2017['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2017')
        elif año2018['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2018')
        elif año2019['y'].sum()<=0:
            lista_descripcion.append('No tiene datos durante el 2019')
    else:
        lista_activos_predecibles.append(ID_activos)

df_pp_frec_1 = df_final[df_final['Frecuencias']==1].reset_index().drop('index',axis=1)
df_pp_frec_0 = df_final[df_final['Frecuencias']==0].reset_index().drop('index',axis=1)
df_pp_frec_0_final = pd.DataFrame({}) 
df_pp_frec_2_final = pd.DataFrame({}) 

for product_predecible in lista_activos_predecibles:
    df_pp_frec_0_reducido = df_pp_frec_0[df_pp_frec_0['m_product_id']==product_predecible]
    df_pp_frec_0_final = df_pp_frec_0_final.append(df_pp_frec_0_reducido)
    
for product_no_predecible in lista_negra:
    df_pp_frec_2_reducido = df_pp_frec_0[df_pp_frec_0['m_product_id']==product_no_predecible]
    df_pp_frec_2_final = df_pp_frec_2_final.append(df_pp_frec_2_reducido)

df_pp_frec_0_final = df_pp_frec_0_final.reset_index().drop('index',axis=1)
df_pp_frec_2_final = df_pp_frec_2_final.reset_index().drop('index',axis=1)
df_pp_frec_2_final['Frecuencias'] = 2 

df_pp_frec_1['Descripcion'] = 'Corresponde al modelo 1'
df_pp_frec_0_final['Descripcion'] = 'Corresponde al modelo 0'
df_pp_frec_2_final['Descripcion'] = lista_descripcion

df_pp_final = df_pp_frec_1
df_pp_final = df_pp_final.append(df_pp_frec_0_final)
df_pp_final = df_pp_final.append(df_pp_frec_2_final)

df_pp_final = df_pp_final.sort_values(by=['producto'], ascending = True)
df_pp_final.reset_index().drop('index',axis=1)
    
df_pp_final.to_csv (path_guardar_datos + 'ProductosdeProveedores.csv',sep='|', index = None, header=True)
print('Se genero el DataFrame Productos por proveedores APP web')

################
# df_min_order #
################

query = """select p.EM_Csgc_Supplier_ID, bp.value as provider_cod, bp.name as Proveedor,
      p.m_product_id, p.value as product_cod, p.name as Producto , p.isactive, b.name as Marca,
  coalesce(cb.EM_SMFGPO_Minorder,0) orden_minima
      from c_bpartner  bp  left join m_product p  on p.EM_Csgc_Supplier_ID = bp.c_bpartner_id
      left join m_brand b on b.m_brand_id= p.m_brand_id
  left join C_BPartner cb on cb.c_bpartner_id = p.EM_Csgc_Supplier_ID
     where bp.isvendor='Y' and bp.isactive='Y'
       and p.ad_org_id ='37A30F4867C844B2AFBDC862A74A9076' """

min_order = pd.read_sql_query(query,con=connection)
min_order.to_csv (path_guardar_datos + 'min_order.csv', sep='|', index = None, header=True)

print('Se genero el DataFrame df_min_order')

############################
# df top_20_gloria         #
############################

import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime
from numpy import arange
import numpy as np
import matplotlib.lines as mlines
from datetime import datetime
from matplotlib.pylab import rcParams
import sys
rcParams['figure.figsize'] = 20,10

path = '/var/www/FlaskApp/FlaskApp/DataBase/'

filename1 = "c_orderline.csv"
filename2 = "df_transaction_week.csv"
filename3 = "compras_gloria.csv"
filename4 = "ProductosdeProveedores.csv"

df_orderline = pd.read_csv(path + filename1,sep="|",header=0)
df_orderline['dateordered'] = pd.to_datetime(df_orderline.dateordered,format='%Y-%m-%d %H:%M:%S')

df_transaction = pd.read_csv(path + filename2,sep="|",header=0)
df_transaction['movementdate'] = pd.to_datetime(df_transaction.movementdate,format='%Y-%m-%d %H:%M:%S')

df_compras = pd.read_csv(path + filename3,sep="|",header=0)
df_pp = pd.read_csv(path + filename4,sep="|",header=0)

def time_previder(df_compras, product_id):
    df_compras= df_compras[df_compras['m_product_id']==product_id]
    provider_mean = df_compras['time_proveedor'].mean(axis=0)
    provider_mean = np.around(provider_mean)
    provider_std  = df_compras['time_proveedor'].std(axis=0)
    provider_std  = np.around(provider_std)
    return provider_mean, provider_std

def demanda_media_product(df_transaction, product_id):
    df_transaction = df_transaction[df_transaction['m_product_id']== product_id]
    df_transaction = df_transaction[df_transaction['movementtype']== 'C-']
    
    df_transaction['movementqty'] = df_transaction['movementqty']*(-1)
    
    #Al haber mas de una venta por dia, agrupamos el data frame por dia
    df_transaction_dia = df_transaction.groupby(['movementdate']).sum()
    demanda_media_dia = df_transaction_dia['movementqty'].mean()
    demanda_desviacion_dia = df_transaction_dia['movementqty'].std()
    
    return demanda_media_dia,demanda_desviacion_dia


def movement_stock(df, stock_maximo, p_dias):
    i = 0
    e_stock = [stock_maximo]
    aux = True
    fecha_pedido = datetime.now()
    fecha_llegada = datetime.now()
    fechas_ingresos = []
    fechas_pedidos = []
    for index, row in df.iterrows():
        if row['movementdate'] >= fecha_llegada and not(aux):
            e_stock[i] = e_stock[i-1] + (stock_maximo)
            aux = True
        
        if e_stock[i] > 0 and (e_stock[i] > -row['movementqty']):
            y = e_stock[i] + row['movementqty']
        else:
            y = 0

        if y <= stock_maximo and aux:
            fechas_pedidos.append(row['movementdate'])
            fechas_ingresos.append(row['movementdate'] + pd.Timedelta(days=(p_dias)))
            fecha_llegada = row['movementdate'] + pd.Timedelta(days=(p_dias))    
            aux = False

        else:
            pass
            
        e_stock.append(y)
        e_stock[i] = e_stock[i+1]
        i+=1
    # Grafico Stock teorico
    pay = e_stock
    pax = df['movementdate']
    say = []
    for i in range(0,len(pay)):
        say.append(stock_maximo)
    return pax, pay, say, fechas_pedidos, fechas_ingresos


def sobre_stock_final(df_transaction,df_compras,df_provider_mean_std, nombreID, porcentaje_guarda):
    dataFrame1 = df_transaction
    
    ProductosdeProveedores = df_provider_mean_std[df_provider_mean_std['producto']==nombreID]
    productId = ProductosdeProveedores['m_product_id'].values
    
    dataFrame1 = dataFrame1[['movementdate','movementqty','m_product_id', "movementtype"]]
    dataFrame1 = dataFrame1[dataFrame1['m_product_id']==productId[0]]
   
    provider_mean, provider_std = time_previder(df_compras, productId[0])
    demanda_media,demanda_desviacion = demanda_media_product(df_transaction, productId[0])
    
    if not dataFrame1.empty:
    
        if np.isnan(provider_mean) or np.isnan(provider_std):

            product = df_provider_mean_std[df_provider_mean_std["m_product_id"]==productId[0]]
            provider_std = int((product["demora_devstd"].values)[0])
            provider_mean = int((product["demora_media"].values)[0])

            z=1.96
            ss= demanda_media * (provider_mean+provider_std)+ (z * demanda_desviacion)
            ss=np.around(ss)

            stock_sum= dataFrame1['movementqty'].tolist()

            largo=len(stock_sum)
            analizamos=[]
            analizamos.append(stock_sum[0])

            exceso = []
            exceso.append(analizamos[0] - (ss + (2*(provider_mean + provider_std))))


            limite_exceso = (ss + (2*(provider_mean + provider_std)))

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

            z=1.96
            ss= demanda_media * (provider_mean+provider_std) + (z * demanda_desviacion)
            ss=np.around(ss)

            stock_sum= dataFrame1['movementqty'].tolist()

            largo=len(stock_sum)
            analizamos=[]
            analizamos.append(stock_sum[0])

            exceso = []
            exceso.append(analizamos[0] - (ss + (2*(provider_mean + provider_std))))

            limite_exceso = (ss + (2*(provider_mean + provider_std)))


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

        dataFrame1['movementdate'] = pd.to_datetime(dataFrame1.movementdate,format='%Y-%m-%d %H:%M:%S')
        fecha=dataFrame1['movementdate']

        #Calculo de la cantidad de periodos en sobre stock
        cantidad_periodos = 0
        aux = True

        df_calculo_dias_periodos = pd.DataFrame({'fecha':fecha, 'sobre_stock':diferencia_stock })

        for i in range(0, len(df_calculo_dias_periodos)):

            if diferencia_stock[i] > 0 and aux:
                cantidad_periodos = cantidad_periodos + 1
                aux= False
            if diferencia_stock[i]==0:
                cantidad_periodos = cantidad_periodos
                aux = True

        df_calculo_dias_periodos['fecha'] = pd.to_datetime(df_calculo_dias_periodos.fecha,format='%Y-%m-%d %H:%M:%S')

        fecha_sobrestock= df_calculo_dias_periodos['fecha'].tolist()
        dif_stock= df_calculo_dias_periodos['sobre_stock'].tolist()

        suma_dias = []

        for i in range (0, len(fecha_sobrestock)-1):
            if dif_stock[i]>0 and ((i+1) < len(fecha_sobrestock)):
                dif_fecha = (fecha_sobrestock[i+1] - fecha_sobrestock[i])
                suma_dias.append(dif_fecha)

        df_suma_dias = pd.DataFrame({"suma_dias":suma_dias})
        df_suma_dias = df_suma_dias.sum()
        if df_suma_dias["suma_dias"]==0:
            sum_dias = df_suma_dias["suma_dias"]
            sum_dias = int(sum_dias)
        else:
            sum_dias = df_suma_dias["suma_dias"].days
            sum_dias = int(sum_dias)

        p_dias = provider_mean + provider_std
        stock_maximo = ((ss*porcentaje_guarda)/100) + ss
        stock_maximo = np.around(stock_maximo)

        pax, pay, say, fechas_pedidos, fechas_ingresos = movement_stock(dataFrame1, stock_maximo, p_dias)
    
    else:
        ss = 0
        limite_exceso = 0
        cantidad_periodos = 0
        sum_dias = 0
    return ss , limite_exceso, cantidad_periodos, sum_dias, productId[0]


lista_product_rotura= df_pp["m_product_id"].tolist()
nombre_product_rotura=df_pp["producto"].tolist()

list_m_product_id = list()
list_producto = list()
list_ss_optimo = list()
list_limite_exceso = list()
list_cant_periodos = list()
list_sum_dias = list()

for nombre_product_rotura in nombre_product_rotura:
    ss_optimo, limite_exceso, cant_periodos,sum_dias,productId = sobre_stock_final(df_transaction,df_compras,df_pp, nombre_product_rotura,0)
    list_m_product_id.append(productId)
    list_producto.append(nombre_product_rotura)
    list_ss_optimo.append(ss_optimo)
    list_limite_exceso.append(limite_exceso)
    list_cant_periodos.append(cant_periodos)
    list_sum_dias.append(sum_dias)
    
    

df_sobre_stock = pd.DataFrame({'m_product_id':list_m_product_id, 'producto':list_producto,
                              'ss_optimo':list_ss_optimo, 'limite_exceso':list_limite_exceso,
                              'cant_periodos':list_cant_periodos,'sum_dias':list_sum_dias})


df_sobre_stock = df_sobre_stock.replace(np.nan, 0)

df_sobre_stock_cantidad =  df_sobre_stock.sort_values(['cant_periodos'], ascending=[0])
df_sobre_stock_cantidad = df_sobre_stock_cantidad.head(20)

df_sobre_stock_dias =  df_sobre_stock.sort_values(['sum_dias'], ascending=[0])
df_sobre_stock_dias = df_sobre_stock_dias.head(20)

df_top_20_gloria = df_sobre_stock_cantidad.append(df_sobre_stock_dias)

export_df_top_20_gloria = df_top_20_gloria.to_csv (path_guardar_datos + 'top_20_gloria.csv',sep='|', index = None, header=True)
print('Se genero el DataFrame top_20_gloria')

connection.close()
