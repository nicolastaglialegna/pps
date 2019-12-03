
def run( df_transaction, product, porcentaje_guarda):

    productId = df_pp[df_pp["producto"] == product].m_product_id.values
    productId = productId[0]

    dataFrame = df_transaction
    dataFrame = dataFrame[['movementdate','movementqty','m_product_id', "movementtype"]]
    dataFrame = dataFrame[dataFrame['m_product_id'] == productId]

    compras = dataFrame[dataFrame['movementtype']=='C-']
    compras = compras[['movementdate','movementqty']]
   
    #Obtenemos los datos de media y desviacion standar del tiempo de gloria al cliente
    client_mean, client_std = time_client.run(df_orderline,productId)
    #Obtenemos los datos de media y desviacion standar del tiempo del proveedor a gloria
    provider_mean, provider_std = time_provider.run(df_pp, df_compras, productId)
    #Vemos la demanda media por dia
    demanda_media, demanda_desviacion = demanda_media_product.run(df_transaction, productId)
    
    if np.isnan(provider_std):
        provider_std=1
    if np.isnan(client_std):
        client_std=1
    if np.isnan(demanda_desviacion):
        demanda_desviacion=1
    
    if np.isnan(provider_mean):
        provider_mean=1
    if np.isnan(client_mean):
        client_mean=1
    if np.isnan(demanda_media):
        demanda_media=1

    ss = np.around(demanda_media * (provider_mean+provider_std) + (z * demanda_desviacion))
    stock_maximo = np.around(((ss*porcentaje_guarda)/100) + ss)
    
    compras['movementdate'] = pd.to_datetime(compras.movementdate,format='%Y-%m-%d %H:%M:%S')
    
    #--------------------------------------------------------------#
    
    i=0
    
    e_stock = []
    e_stock = [stock_maximo]
    
    aux = True
    fecha_pedido = datetime.now()
    fecha_llegada = datetime.now()
    p_dias = pd.Timedelta(days=(provider_mean*7+provider_std*7))
    fechas_ingresos=[]
    fechas_ingresos.append(pd.to_datetime('2017-01-01',format='%Y-%m-%d'))
    fechas_pedidos=[]
    fechas_pedidos.append(pd.to_datetime('2017-01-01',format='%Y-%m-%d'))
   
    
    
    for index, row in compras.iterrows():
        if row['movementdate'] >= fecha_llegada and not(aux):
            fechas_ingresos.append(row['movementdate'])
            e_stock[i]=e_stock[i-1]+(stock_maximo)
            aux = True
            
        if e_stock[i] > 0 and (e_stock[i] > -row['movementqty']):
            y = e_stock[i] + row['movementqty']
        else:
            y = 0
        
        if y <= stock_maximo and aux:
            fechas_pedidos.append(row['movementdate'])
            fecha_llegada = row['movementdate']+p_dias      
            aux = False

        else:
            pass
        e_stock.append(y)
        e_stock[i] = e_stock[i+1]
        i+=1
  
    e_stock.pop()

    # Array para grafico de stock real
    rax, ray = graf_product.run(df_transaction,productId)
    
    # Grafico Stock teorico
    pay = e_stock
    pax = compras['movementdate']
    
    say = []
    for i in range(0,len(pay)):
        say.append(stock_maximo)


    return rax,ray,pax,pay,say,fechas_ingresos,fechas_pedidos
