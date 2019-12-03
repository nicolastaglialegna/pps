import pandas as pd
#--------------------------------------------------------------------
#               graf_product
# Esta funcion retorna 2 listas para graficar el movimiento de stock
# como parametro de entrada se tiene:
# df_transaction : contiene todos los movimientos de stock V+,C-,I+,I-
# productID : es el Id del producto a graficar
# Variables de retorno:
# ax: lista con las fechas de los movimientos
# ay: lista con el valor de stock 
#--------------------------------------------------------------------
def run(df_transaction, productId):
    df_transaction = df_transaction[df_transaction['m_product_id'] == productId][['movementdate','movementqty','m_product_id', "movementtype"]]
    df_transaction['movementdate'] = pd.to_datetime(df_transaction.movementdate,format='%Y-%m-%d %H:%M:%S')

    if df_transaction.empty == False :
        stock_sum = df_transaction['movementqty'].tolist()
        analizamos = list()
        analizamos.append(stock_sum[0])
        for i in range(0, len(stock_sum)):
            if i < len(stock_sum)-1:
                analizamos.append(analizamos[i]+stock_sum[i+1])
        ax = df_transaction['movementdate'].tolist()
        ay = analizamos
    else:
        ax = []
        ay = []
    return ax,ay