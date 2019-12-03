import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from matplotlib.pylab import rcParams
import sys
rcParams['figure.figsize'] = 20,10
from keras.models import load_model, model_from_json

#-------------------------------------------------------------------------------------------------------
#                       holt_winters_model.py
#
# Esta funcion toma del DataFrame Productosdeproveedores una lista unica de los proveedores
# y de cada uno de los proveedores genera una lista de todos los productos.
# 
# Por cada proveedores, mediante un for, recorremos todos los productos del mismo. Para cada uno de los 
# productos realizamos un modelado y entenamientos con todas las variaciones posibles de parametros
# quedandonos con la que nos brinde un mejor valor de RSME.
#
# El modelo con mejor valor de RSME es guardado para luego ser entrenado bajo dichos pesos y realizar las predicciones.
#
#-------------------------------------------------------------------------------------------------------


#path = '/home/futit/Documentos/Gloria/DataBase/'
path = '/var/www/FlaskApp/FlaskApp/DataBase/'

df_transaction = pd.read_csv(path + 'df_transaction_week.csv', sep='|')
df_transaction = df_transaction[df_transaction['movementtype']=='C-']
df_transaction.movementqty = abs(df_transaction.movementqty)

df_pp = pd.read_csv(path + 'ProductosdeProveedores.csv', sep='|')
list_proveedor = df_pp.proveedor.unique().tolist()

list_id_product = list()
list_parametro = list()
list_RMSE = list()
list_name_modelo = list()

j=1

for proveedor in list_proveedor:
    
    df_pp_1 = df_pp[df_pp['proveedor'] == proveedor]
    #df_pp_1 = df_pp_1[df_pp_1['Frecuencias'] == 2]
    
    list_producto = df_pp_1.m_product_id.unique().tolist()
    
    for producto in list_producto:
        
        df_pp_2 = df_pp_1[df_pp_1['m_product_id']==producto]
        name_producto = df_pp_2.producto.values[0]
        print('Producto N°: ', j)    
        print(producto)
        
        df = df_transaction[df_transaction['m_product_id'] == producto].reset_index().drop('index', axis = 1)
        df = df[['movementdate','movementqty']]
        df['movementdate'] = pd.to_datetime(df['movementdate'])

        df.to_csv (path + 'df_holtwinters.csv',sep="|", index = None, header=True) 
        df = pd.read_csv(path + 'df_holtwinters.csv', sep='|',
                     parse_dates=['movementdate'], 
                     index_col='movementdate')
        
        df.movementqty = abs(df.movementqty)
        df.movementqty = df.movementqty.replace(0,0.1)
        
        #--------------------------------------------------------------------------------------------#
        # ENTRENAMIENTO
        if len(df) > 10:
            
            df.index.freq = 'W'
            val_train = round(len(df)*0.8)
            train, test = df.iloc[:val_train, 0], df.iloc[val_train:, 0]

            RMSE=999999999

            for i in range(2,val_train):

                model = ExponentialSmoothing(train, seasonal='mul', seasonal_periods=i).fit()
                # pred = model.predict(start=test.index[0], end=test.index[-1] + 10)
                pred = model.predict(start=test.index[0], end=test.index[-1])

                pred_1 = pred.reset_index()
                pred_1 = pred_1.rename(columns={'index': 'movementdate', 0: 'movementqty'})
                pred_1 = round(pred_1.resample('M', on='movementdate').sum().reset_index().sort_values(by='movementdate'))
                pred_1 = pred_1.tail(4)

                test_1 = test.reset_index()
                test_1 = test_1.rename(columns={'index': 'movementdate', 'movementqty': 'movementqty_test'})
                test_1 = round(test_1.resample('M', on='movementdate').sum().reset_index().sort_values(by='movementdate'))
                test_1 = test_1.tail(4)

                analisis = pd.DataFrame({'movementdate':pred_1['movementdate'],'prediccion':pred_1['movementqty'],'real':test_1['movementqty_test']})
                analisis['diferencia'] = (analisis['real'] - analisis['prediccion'])

                RMSE_1 = np.sqrt(((analisis.diferencia)**2).sum()/len(analisis))

                if RMSE_1 < RMSE:
                    RMSE = RMSE_1
                    parametro = i
                    model.save('/var/www/FlaskApp/FlaskApp/DataBase/holtwinter_id_model/' + producto + '.h5')

                i+=1

            list_id_product.append(producto)   
            list_parametro.append(parametro)  
            list_RMSE.append(RMSE)
            list_name_modelo.append(producto + '.h5')

            j+=1
        
        else:
            
            list_id_product.append(producto)   
            list_parametro.append(99999)  
            list_RMSE.append(99999)
            list_name_modelo.append('No pudo se entrenado')
            
            j+=1
df_holtwinter_parametros = pd.DataFrame({'m_product_id':list_id_product, 'name_model':list_name_modelo, 'parameters':list_parametro, 'RMSE':list_RMSE})
df_holtwinter_parametros.to_csv(path + 'df_holtwinter_parametros.csv',sep='|', index=False)
        