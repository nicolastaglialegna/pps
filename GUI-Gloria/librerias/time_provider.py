import numpy as np
#--------------------------------------------------------------------------------
# time_provider.py
# parametros de entrada:
# df_pp : dataframe con nombre de los productos, id, nombre de proveedor asociado
# df_compras : compras de gloria hacia sus proveedores
# product_id : ID del producto 
# Retorno:
# provider_mean : el tiempo medio de demora en entregar el producto el proveedor
# provider_std : es el la desviacion estandar de la demora del proveedor
#--------------------------------------------------------------------------------
def run(df_pp, df_compras, product_id):
    df_comprasp = df_compras[df_compras['m_product_id'] == product_id]
    if not(df_comprasp.empty) or df_comprasp.shape[0] > 1:
        provider_mean = df_comprasp['time_proveedor'].mean(axis=0)
        provider_mean = np.around(provider_mean)
        provider_std  = df_comprasp['time_proveedor'].std(axis=0)
        provider_std  = np.around(provider_std)
    else: 
        provider_mean = int((df_pp[df_pp['m_product_id'] == product_id].demora_devstd.values)[0])
        provider_std = int((df_pp[df_pp['m_product_id'] == product_id].demora_media.values)[0])
 
    return provider_mean, provider_std