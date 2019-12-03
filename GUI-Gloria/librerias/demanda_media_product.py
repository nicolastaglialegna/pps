
def run(df_transaction, product_id):
    df_transaction = df_transaction[df_transaction['m_product_id']== product_id]
    df_transaction = df_transaction[df_transaction['movementtype']== 'C-']
    df_transaction['movementqty'] = df_transaction['movementqty']*(-1)
    
    #Al haber mas de una venta por dia, agrupamos el dataframe por dia
    df_transaction_dia = df_transaction.groupby(['movementdate']).sum()

    # Calculo de la demanda media y desviacion estandar
    demanda_media_dia = df_transaction_dia['movementqty'].mean()
    demanda_desviacion_dia = df_transaction_dia['movementqty'].std()
    
    return demanda_media_dia,demanda_desviacion_dia