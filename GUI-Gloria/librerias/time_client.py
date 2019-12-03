import numpy as np
#-----------------------------------------------------------
# time_client.py
# Las variables de entrada son:
# df_orderline: cotiene datos de los periodos--
# --de timpo de venta de gloria para con sus clientes
# product_id : es el Id del producto.
# Las variables de retorno son:
# client_mean: El tiempo medio de retardo de entrega
# client_std : La desviacion estandar del tiempo de retardo
#-----------------------------------------------------------

def run(df_orderline, product_id):
    fecha_limite = 50

    df_orderline = df_orderline[df_orderline['m_product_id'] == product_id]
    df_orderline = df_orderline[df_orderline['time_cliente'] < fecha_limite]

    # Calculo de la media
    client_mean = df_orderline['time_cliente'].mean(axis=0)
    client_mean = np.around(client_mean)

    # Calculo de la Desviacion estandar
    client_std  = df_orderline['time_cliente'].std(axis=0)
    client_std  = np.around(client_std)

    return client_mean, client_std