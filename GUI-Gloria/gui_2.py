# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
import dash_daq as daq
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import dash_table
from datetime import datetime
import os
import sys
import time
from librerias import time_provider, time_client, demanda_media_product, graf_product
from librerias import ruptura_stock, sobre_stock2
from librerias import predictor, ss_variable, sales, orden_compra, holt_winters
from dash.exceptions import CantHaveMultipleOutputs

#path = "/home/futit/Documentos/MachineLearning/db_gloria/"
path = "/var/www/FlaskApp/FlaskApp/DataBase/"
#path = "/home/futit/Documentos/Gloria/GUI-Gloria/Archivos_server/ServerGloria/DataBase/"

filename = "c_orderline.csv" 
filename3 = "roturas_ordenado_dias.csv" 
filename4 = "compras_gloria.csv"
filename6 = "top_20_gloria.csv" 
filename8 = 'ProductosdeProveedores.csv'

# DataFrame Amazon
filename9 = 'aws_week_v3.csv'
filename15 = 'aws_month.csv'
filename14 = 'aws_v3_month.csv'

# DataFrame Prophet
filename11 = 'df_prophet_week.csv'
filename13 = 'df_prophet_month.csv'

# DataFrame m_transaction
filename17 = 'df_transaction_aws_append.csv'
filename12 = 'df_transaction_month.csv'
filename2 = "df_transaction_week.csv" 
filename10 = 'df_transaction_JJA.csv'
filename16 = 'df_transaction_JJA_month.csv'

filename18 = 'precios.csv'
filename19 = 'df_compra_minima.csv'
filename20 = 'min_order.csv'

# DataFrame Holt-Winter
filename21 = 'df_predic_holt_week.csv'
filename22 = 'df_predic_holt_month.csv'
filename23 = 'df_predic_holt_week_ApiRest.csv'
filename24 = 'df_predic_holt_month_ApiRest.csv'
z= 1.96

try:
    #---------- File_1---------------
    df_orderline = pd.read_csv(path + filename, sep = "|")

    #---------- File_2---------------
    # Contiene el movimiento de stock de los productos V+ I+ C-
    df_transaction = pd.read_csv(path + filename2, sep = '|')

    #---------- File_3---------------
    # Contiene los producto en funcion de las rupturas historicas
    df_product = pd.read_csv(path + filename3, sep = "|")
    df_product_ordenados = df_product.sort_values(by = 'ruptura', ascending = False)

    #---------- File_4---------------
    # Contiene las compras de gloria hacia sus proveedores
    df_compras = pd.read_csv(path + filename4, sep = "|")

    #---------- File_5---------------
    # Contiene todos los productos de los proveedores
    df_pp = pd.read_csv(path + filename8, sep = "|")
    df_pp['tiempo_total'] = df_pp['demora_devstd'] + df_pp['demora_media']

    # Listado de productos con frecuencia distinto de 2 (son aquellos facil de predecir)
    df_ppf = df_pp[df_pp['Frecuencias'] != 2]
    list_products = df_ppf['producto'].tolist()

    # Listado de los proveedores de los producos anteriores
    list_proveedores_pr = []
    for producto in list_products:
        list_proveedores_pr.append(df_ppf[df_ppf['producto'] == producto].proveedor.values)
    list_proveedores_pr = np.unique(list_proveedores_pr)   

    del df_ppf

    # Listado de los productos con frecuencia = 2 (son dificiles de predecir)
    df_ppf = df_pp[df_pp['Frecuencias'] == 2]
    list_products_excep = df_ppf['producto'].tolist()

    # Listado de los proveedores de los productos anteriormene listados
    list_proveedores_excp = []
    for producto in list_products:
        list_proveedores_excp.append(df_ppf[df_ppf['producto'] == producto].proveedor.values)
    list_proveedores_excp = np.unique(list_proveedores_pr)   

    del df_ppf

    #---------- File_6---------------
    # Contien los producto en sobre stock
    df_top20 = pd.read_csv(path + filename6, sep = "|")

    #------------------------------Variables---------------------------------
    list_provider = df_pp['proveedor'].unique().tolist()

    #---- listas de los productos con ruptura----
    # --- por dias----
    d_product_rotura = df_product["name"].tolist()
    #----- por cantidad----
    lista_product_rotura = df_product_ordenados["m_product_id"].tolist()
    nombre_product_rotura = df_product_ordenados["name"].tolist()

    #----- Listas de productos sobre stock --------------
    lista_top20_peridodo = df_top20.sort_values(by = 'cant_periodos', ascending = False)['producto'].head(20).tolist()
    lista_top20_dias = df_top20.sort_values(by = 'sum_dias', ascending = False)['producto'].head(20).tolist()

    #-----------Filename 11-----------------
    df_prophet_week = pd.read_csv(path + filename11)
    #-----------Filename 13 ---------------
    df_prophet_month = pd.read_csv(path + filename13)
    #-----------Filename 12----------------
    df_transaction_month = pd.read_csv(path + filename12, sep = '|')
    # ----------Filename 18 ----------------
    df_precios = pd.read_csv(path + filename18, sep='|')
    # ----------Filename 19-----------------
    df_min_order = pd.read_csv(path + filename19, sep = '|')
    #-----------Filename20------------------
    df_monto_min_provider = pd.read_csv(path + filename20, sep = '|') 

    # DataFrame con informacion de la predicciones semanales y mensuales con el modelo holt-wilters
    df_holt_week = pd.read_csv(path + filename21, sep = '|') 
    df_holt_month = pd.read_csv(path + filename22, sep = '|')

    # DataFrame que usa la API para enviar la informacion a Openbravo (Predicciones a futuro)
    df_holt_api_week = pd.read_csv(path + filename23, sep = '|')
    df_holt_api_month = pd.read_csv(path + filename24, sep = '|')
except:
    print("Error al cargar CSV")
#---------------------------------------------------------------------------------------
#--------------------------------- Calculos --------------------------------------------
#---------------------------------------------------------------------------------------

df_compras = df_compras[df_compras['dateordered'] > '2017-01-01'][['m_product_id', 'dateordered', 'datepromised' ]]
df_compras['dateordered'] = pd.to_datetime(df_compras.dateordered,format='%Y-%m-%d %H:%M:%S')
df_compras['datepromised'] = pd.to_datetime(df_compras.datepromised,format='%Y-%m-%d %H:%M:%S')
df_compras['time_proveedor'] = (df_compras['datepromised']-df_compras['dateordered']).dt.days/7

#-------------------------------
#---------Funciones-------------
#-------------------------------

#################################################
def graph(x,y,nombre,color,type_linea,grupo , visible_state):
    traces = []
    if isinstance(x, list):
        for i in range(0,len(x)):
            traces.append( go.Scatter(
                x = [x[i], x[i]],
                y = [0, y[i]],
                visible = visible_state,
                mode ='lines+markers',
                legendgroup = grupo,
                showlegend = False,
                name = nombre,
                hoverinfo = 'name',
                line = dict(
                    color = color,
                    dash = type_linea,
                    )
                ))
    else:
        traces = go.Scatter(
            x = [x, x],
            y = [0, y],
            visible = visible_state,
            mode ='lines+markers',
            legendgroup = grupo,
            showlegend = True,
            name = nombre,
            hoverinfo = 'name',
            line = dict(
                color = color,
                dash = type_linea,
                )
            )
    return traces
def make_table(id_table, df_table, rows, filter):
    table_2 = dash_table.DataTable(
        id = id_table,
        columns = [{"name": i, "id": i} for i in df_table.columns],
        data = df_table.to_dict('records'),
        style_cell_conditional=[
        {'if': {'column_id': 'Periodo'},
        'width': '30%'},
        {'if': {'column_id': 'Prophet'},
        'width': '30%'},
        {'if': {'column_id': 'Amazon'},
        'width': '30%'},
        ],
        style_cell={'textAlign': 'center'},
        style_header={
        'backgroundColor': 'rgb(230, 230, 230)',
        'fontWeight': 'bold'
        },
        style_data_conditional=[{
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(248, 248, 248)'
        }],
        filter_action="native",
        page_action="native",
        page_current= 0,
        page_size= rows,
        editable=True,
        export_format='xlsx',
        export_headers='display',
    ),
    return table_2


#------------Variables------------------
proveedores = list_provider
proveedores.sort()
lista_top20_peridodo = lista_top20_peridodo
lista_top20_dias = lista_top20_dias
lista_ruptura_d = d_product_rotura
lista_ruptura_n = nombre_product_rotura
tb6_columns = [
    {'name': 'Producto', 'id': 'Producto', 'editable': False}, 
    {'name': 'Stock Actual', 'id': 'Stock Actual', 'editable': False}, 
    {'name': 'Ventas Periodo Nº1', 'id': 'Ventas Periodo Nº1', 'editable': False}, 
    {'name': 'Ventas Periodo Nº2', 'id': 'Ventas Periodo Nº2', 'editable': False}, 
    {'name': 'Ventas Perdidas', 'id': 'Ventas Perdidas', 'editable': False}, 
    {'name': 'Pedido Nº 1', 'id': 'Pedido Nº 1', 'editable': True}, 
    {'name': 'Precio c/u', 'id': 'Precio c/u', 'editable': False}, 
    {'name': 'Valor', 'id': 'Valor', 'editable': False}, ]


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    "/assets/main.css",
]
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
app.css.config.serve_locally = True
app.scripts.config.serve_locally = True

tab1_content = dbc.Card(
    dbc.CardBody(
        [
        html.P("Informe por proveedor", className="card-text"),
        html.Div(children=[
            dbc.Row([
                html.H5("Proveedor", className='col-auto'),
                dbc.Col(dcc.Dropdown(
                    id='proveedor',
                    options=[{'label': i, 'value': i} for i in proveedores],
                    value = proveedores[0], 
                    clearable = False,
                ),),
                html.H5("Producto",className = 'col-auto'),
                dbc.Col(dcc.Dropdown(id='producto_dropdown', clearable = False,),),
                ],
                align="center",
            ),
            dbc.Row([
                html.H5("Guarda%",className='col-auto'),
                dbc.Col(daq.NumericInput(
                        id = 'porcentaje_guarda',
                        min = 0,
                        max = 99999,
                        value = 0 ), width = "auto", className = 'ml-3'),
                html.H5("Proyeccion",className='col-auto'),
                dbc.Col(daq.NumericInput(
                        id = 'proyeccion',
                        min = 0,
                        max = 99999,
                        value = 100 ), width = "auto"),
                dbc.Button("Plot", id = 'btn-plot' ,outline = True, color = "primary", className = "mr-1"),
            ],
            align = "center",
            className = "my-2",
            ),
        ],),
            
            dbc.Alert(id = "alerta", color = "warning", is_open = False, className = "font-weight-bold"),
 
        html.Div(children = [
            dcc.Loading(id = "loading-1", children = [html.Div(id='grafico-2')], type = "default"), 
            dcc.Loading(id = "loading-2", children = [html.Div(id='grafico-3-1')], type = "default"),
            
        ]),
        dbc.Row([
            dbc.Col(id='table_2',width = 6, align="center"),
        ],className='row d-flex justify-content-center'),
        ],
    ),
    className="mt-3",
)

tab2_content = dbc.Card(
    dbc.CardBody(
        [
            html.P("Top productos con mayor ruptura de stock", className="card-text"),
            html.Div(children = [
                dbc.Row(
                    [
                        dbc.Col([
                            html.H5("Producto"),                   
                            dcc.Dropdown(
                                id='rotura_stock',
                                clearable = False,
                                ),
                            ],
                        ),
                        dbc.Col([
                                html.H5("Ordenar por:"),
                                dcc.Dropdown(
                                    id='option_list',
                                    options=[
                                        {'label': 'Dias', 'value': 'Dias'},
                                        {'label': 'Cantidad', 'value': 'Cantidad'},
                                    ],
                                    value='Dias',
                                    clearable = False,
                                ),],
                                className='col-2',
                        ),
                        dbc.Col([
                            html.H5("G%"),                    
                            daq.NumericInput(
                                id='porcentaje_guarda2',
                                min=0,
                                max=100,
                                value=0,
                                ),
                            ],
                            className='col-auto',
                        ),
                        dbc.Button("Plot", id = 'btn-plot-tb2' ,outline = True, color = "primary", className = "mr-1"),
                    ], className = 'row align-items-end',
                ),

                html.Hr(),

                dbc.Row([
                    dbc.Col([
                        html.H5('Informacion:'),
                        html.Div(id = 'output-info',className = "font-weight-bold"),
                    ],),
                ],),
            ],),

            html.Div(children = [
                dcc.Loading(id = "loading-3", children = [html.Div(id='grafico-3')], type = "default"),
                dcc.Loading(id = "loading-4", children = [html.Div(id='grafico-4')], type = "default"),
                dcc.Loading(id = "loading-10", children = [html.Div(id='grafico-10')], type = "default"),
            ]),
        ],
       
    ),
    className="mt-3",
)
tab3_content = dbc.Card(
    dbc.CardBody(
        [
            html.P('Top productos con mayor stock', className="card-text"),
            dbc.Row([
    
                dbc.Col([
                    html.H5("Producto"),
                    dcc.Dropdown(
                        id = 'product_mas_stock',
                        clearable = False,
                    ), 
                ],),

                dbc.Col([
                        html.H5("Ordenar por:"),
                        dcc.Dropdown(
                            id='option_list_2',
                            options=[
                                {'label': 'Dias', 'value': 'Dias'},
                                {'label': 'Periodos', 'value': 'Periodos'},
                            ],
                            value='Dias',
                            clearable = False,
                        ),],
                        className='col-2',
                ),
                dbc.Col([
                    html.H5("G%"),
                    daq.NumericInput(
                        id='porcentaje_guarda3',
                        min=0,
                        max=100,
                        value=0
                    ),
                ], className='col-auto',),

                dbc.Button("Plot", id = 'btn-plot-tb3' ,outline = True, color = "primary", className = "mr-1"),
            ],className = 'row align-items-end',),
            # Grafico
            html.Div(children = [
                dcc.Loading(id = "loading-5", children = [html.Div(id='grafico-5')], type = "default"), 
                dcc.Loading(id = "loading-11", children = [html.Div(id='grafico-11')], type = "default"), 
            ]),
        ],
    ),
    className="mt-3",
)
tab4_content = dbc.Card(
    dbc.CardBody(
        [
            html.P('Productos modelados', className="card-text"),

            html.Div(children=[
                dbc.Row([
                    html.H5('Proveedor: ',className = 'col-auto'),
                    dbc.Col(
                        dcc.Dropdown(
                        id = 'proveedor_prophet',
                        options=[{'label': i, 'value': i} for i in list_proveedores_pr],
                        value = list_proveedores_pr[0], 
                        clearable = False,
                        ), 
                    ),
                    html.H5('Producto: ',className = 'col-auto'),
                    dbc.Col(
                        dcc.Dropdown(
                            id = 'product_prophet',
                            clearable = False,
                        ),
                    ),
                ],align = "center",),
                dbc.Row([
                    html.H5('Analisis: ',className = 'col-auto'),
                    dbc.Col([
                        dcc.Dropdown(
                            id = 'analisis-option',
                            options=[
                                {'label': 'Semanal', 'value': 'semanal'},
                                {'label': 'Mensual', 'value': 'mensual'},
                            ],
                            value = 'semanal',
                            clearable = False,
                        ),
                    ],className = 'col-2'),
                    dbc.Button("Plot", id = 'btn-plot-tb4' , outline = True, color = "primary", className = "mr-1"),
                ],align = "center",className = 'mt-2'),
            ]),
 
            html.Hr(),
            html.Div(children = [
                dcc.Loading(id = "loading-prophet", children = [html.Div(id = 'grafico-prophet')], type = "default"),
                dcc.Loading(id = "loading-bar", children = [html.Div(id = 'grafico-bar')], type = "default"), 
            ]),
            dbc.Row([
                dbc.Col(id = 'table_tb4_',width = 'auto', align = "center"),

            ],className = 'row d-flex justify-content-center align-top'),
        ],
    ),
    className="mt-3",
)
tab5_content = dbc.Card(
    dbc.CardBody(
        [
            html.P(id = 'titulo-exception', className="card-text"),
            dbc.Row([
                html.H5("Proveedor: ", className='col-auto'),
                dbc.Col(
                    dcc.Dropdown(
                        id = 'products_excepcion',
                        options = [{'label': i, 'value': i} for i in list_proveedores_excp],
                        value = list_proveedores_excp[0], 
                        clearable = False,
                    ), 
                )
            ]),

            html.Hr(),

            dbc.Row([
                dbc.Col([
                    html.Div(id = 'info-excepciones',className = "font-weight-bold"),
                ],className='col-auto'),
                
            ],),
            html.Hr(),
            dbc.Row([
                html.Div([
                    dbc.Col(id = 'table_tb5',width = 6, align = "center"),
                ]),
            ],className = 'row d-flex justify-content-center'),
        ]
    ),
    className="mt-3 ",
)
tab6_content = dbc.Card(
    dbc.CardBody(
        [
            dbc.Row([
                
                dbc.Col([
                    html.H5("Proveedor"),
                    dcc.Dropdown(
                        id = 'proveedor_compra',
                        options = [{'label': i, 'value': i} for i in proveedores],
                        value = proveedores[0], 
                        clearable = False,
                        ),
                    ],
                ),

                dbc.Col([
                        html.H5("G%"),                    
                        daq.NumericInput(
                            id='porcentaje_guarda6',
                            min=0,
                            max=100,
                            value=0,
                            ),
                        ],
                        className='col-auto',
                    ),

                dbc.Button("Plot", id = 'btn_tb6' ,outline = True, color = "primary", className = "mr-1"),
                
                ],
                className = 'row align-items-end',
            ),

            dbc.Row([
                dbc.Col([

                    html.Div(id = 'output-info_tb6', className = "font-weight-bold"),
                ],),
            ],),

            html.Hr(),

            dbc.Alert(id = "alerta_tb6", is_open = False, className = "font-weight-bold"),

            dbc.Row([
                html.Div([
                    dash_table.DataTable(
                        id = 'tabla_tb6',
                        columns = tb6_columns,
                        data = [{}],
                        style_cell_conditional=[
                        {'if': {'column_id': 'Periodo'},
                        'width': '30%'},
                        {'if': {'column_id': 'Prophet'},
                        'width': '30%'},
                        {'if': {'column_id': 'Amazon'},
                        'width': '30%'},
                        ],
                        style_cell={'textAlign': 'center'},
                        style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                        },
                        style_data_conditional=[{
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(248, 248, 248)'
                        }],
           
                        filter_action="native",
                        page_action="native",
                        page_current= 0,
                        page_size= 9999,
                        editable=True,
                        export_format='csv',
                        export_headers='display',

                    ),
                ]),
            ],className = 'row d-flex justify-content-center'),

        ]
    ),
    className="mt-3 ",
)


tabs = dbc.Tabs(
    [
        dbc.Tab(tab1_content, label = "Informe por proveedor"),
        dbc.Tab(tab2_content, label = "Top Ruptura"),
        dbc.Tab(tab3_content, label = "Top Sobre-Stock"),
        dbc.Tab(tab4_content, label = "Predictores"),
        dbc.Tab(tab5_content, label = "Excepciones"),
        dbc.Tab(tab6_content, label = "Compras"),
    ]
)

app.layout = html.Div(children=[
    # Banner display
    html.Div([
        html.H2(
            'Proyeccion de Ventas',
            id='title',
            className="col align-self-center",
        ),
        html.Img(
            src="/assets/logo.png",
            className='logo',
        ),
    ],
        className="banner row justify-content-between"
    ),
    tabs,
])

#------------------------------------------------------------------------------------
#       Actualizar lista de productos por proveedor
#
# Este Callback toma como entrada de un dropdown el nombre del proveedor seleccionado
# Devolviendo una lista de sus productos ordenados alfabeticamente.
#-------------------------------------------------------------------------------------

@app.callback(
    [Output(component_id = 'producto_dropdown', component_property = 'options'),
     Output(component_id = 'producto_dropdown', component_property = 'value'),],
    [Input(component_id = 'proveedor', component_property = 'value')])

def name_product(proveedor):
    list_name = df_pp[df_pp['proveedor'] == proveedor].producto.tolist()
    list_name.sort() 
    if len(list_name) == 0:
        label = []
        value = 'None'
    else:
        label = [{'label': i, 'value': i} for i in list_name]
        value = list_name[0]

    return label, value

#---------------------------------------------------------------
#       Actualizacion de la tabla excepciones Tab5
#
# Este Callback toma como entrada un proveedor y devuelve
# una tabla con todos los productos que no se han podido modelar
# la tabla contiene informacion de la causa
#---------------------------------------------------------------

@app.callback(
    [Output(component_id = "table_tb5", component_property = "children"),
     Output(component_id = 'info-excepciones', component_property = 'children'),
     Output(component_id = 'titulo-exception', component_property = 'children'),],
    [Input(component_id = 'products_excepcion', component_property = 'value')])

def update_table(products_excepcion):
    productos = df_pp[(df_pp['Frecuencias'] == 2) & (df_pp['proveedor'] == products_excepcion )]['producto'].values.tolist()
    descripcion = df_pp[(df_pp['Frecuencias'] == 2) & (df_pp['proveedor'] == products_excepcion )].Descripcion.values.tolist()

    total =('Total de productos que no pudieron ser modelados: {} ').format(len(df_pp[(df_pp['Frecuencias'] == 2)].values.tolist()))
    
    informacion = (u' Cantidad de productos: "{}"').format(len(productos))

    df = pd.DataFrame({
        'Producto': productos,
        'Descripcion': descripcion,
    })
    tabla = make_table('tabla_excepciones',df, 9999999,True)

    return tabla, informacion, total

#------------------------------------------------------------------
#       Refresh lista de productos modelados
#
# Este Callback retorna la lista de productos del Tab4 en funcion
# del proveedor seleccionado
#-------------------------------------------------------------------
@app.callback(
    [Output(component_id = 'product_prophet', component_property = 'options'),
     Output(component_id = 'product_prophet', component_property = 'value'),],
    [Input(component_id = 'proveedor_prophet', component_property = 'value')])

def name_product(option_privider_prophet):
    list_name = df_pp[(df_pp['proveedor'] == option_privider_prophet) & (df_pp['Frecuencias'] != 2)].producto.tolist()

    if len(list_name) == 0:
        label = []
        value = 'None'
    else:
        label = [{'label': i, 'value': i} for i in list_name]
        value = list_name[0]

    return label, value

#-----------------------------------------------------------------
#           Cambio de lista en ruptura stock
#
# Este Callback cambia el orden de los productos en ruptura
# tomando como entrada la opcion de que sea en funcion de la
# cantidad de dias en rupura o la cantidad de rupturas historicas
#-----------------------------------------------------------------
@app.callback(
    [Output(component_id = 'rotura_stock', component_property = 'options'),
     Output(component_id = 'rotura_stock', component_property = 'value'),],
    [Input(component_id = 'option_list', component_property = 'value')])

def name_product(option):
    if option == 'Dias':
        label = [{'label': i, 'value': i} for i in lista_ruptura_d]
        value = lista_ruptura_d[0]
        return label,value
    else:
        label = [{'label': i, 'value': i} for i in lista_ruptura_n]
        value = lista_ruptura_n[0]
        return label,value

#-------------------------------------------------------------------------
#                    Cambio de lista en Top stock
#
# Este Callback Cambia el orden de la lista de productos en top sobrestock
# en funcion de optar por ordenarlos por dias o por periodos
#--------------------------------------------------------------------------
@app.callback(
    [Output(component_id = 'product_mas_stock', component_property = 'options'),
     Output(component_id = 'product_mas_stock', component_property = 'value'),],
    [Input(component_id = 'option_list_2', component_property = 'value')]
    )

def name_product(option):
    if option == 'Dias':
        label = [{'label': i, 'value': i} for i in lista_top20_dias]
        value = lista_top20_dias[0]
        return label,value
    else:
        label = [{'label': i, 'value': i} for i in lista_top20_peridodo]
        value = lista_top20_peridodo[0]
        return label,value
#------------------------------------------------------------------------------
#           CallBack Tab-1 (informe por proveedores)
#
# Toma como parametros de entrada el proveedor, producto, porcentaje de guarda
# y la proyeccion y retorna los graficos de movimiento de stock y ventas reales
# como tambien las predicciones.
#-------------------------------------------------------------------------------
@app.callback(
    [Output(component_id = 'grafico-2', component_property = 'children'),
     Output(component_id = 'grafico-3-1', component_property = 'children'),
     Output(component_id = "alerta", component_property = "is_open"),       
     Output(component_id = "alerta", component_property = "children"),],
    [Input(component_id = 'btn-plot', component_property = 'n_clicks'), ],
    [State(component_id = 'proveedor', component_property = 'value'),
     State(component_id = 'producto_dropdown', component_property = 'value'),
     State(component_id = 'porcentaje_guarda',component_property = 'value'),
     State(component_id = 'proyeccion',component_property = 'value'),])

def tab1(btn_tb1, provider, product, porcentaje_guarda, proyeccion):
    #-----------Traces y colores -------------------
    color_traces = ['rgb(7, 83, 245)','rgb(245, 119, 7)','rgb(16, 193, 10)','rgb(213, 0, 255)','rgb(16, 193, 10)','rgb(7, 83, 245)', 'rgb(213, 0, 255)']
    traces_2 = []
    traces_3 = []

    if btn_tb1 != None:
        # Recuperamos el ID del producto seleccionado
        product_id = df_pp[df_pp['producto'] == product].m_product_id.values[0]
        
        #-------------- Ventas Reales ------------
        xr, yr, flag_alert = sales.run(df_transaction, df_pp, product)
        #--------- Holt-wilter Comparativo ---------
        x_data2, y_data2 = holt_winters.run(df_holt_week, product)

        #---------- Holt-wilter a futuro------------
        y_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].prediccion.values.tolist())
        x_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].fecha.values.tolist())
        x_data2.append(xr)
        y_data2.append(yr)        

        #-------Informacion sobre alertas---------
        if flag_alert :
            alert = True
            info_alert = 'Datos insuficientes'
            graf_3 = None
            graf_2 = None
        else:
            info_alert = None
            alert = False

            #--------- Grafico Real y prediccion ----------------  
            name_traces_predict = ['Prediccion','Real']      
            color_aux = ['rgb(10, 10, 10)', 'rgb(7, 83, 245)']      

            for i in range(0,len(x_data2)):
                traces_2.append(go.Scatter(
                    x = x_data2[i],
                    y = y_data2[i],
                    mode = 'lines',
                    name = name_traces_predict[i],
                    connectgaps = True,
                    line = dict( color = color_aux[i], )
                ))

            layout_2 = go.Layout(
                xaxis = {'title': 'Tiempo'},
                yaxis = {'title': 'Ventas'},
                width = 1300,
                height = 500,
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x = 1, y = 0.5),
            )
            fig_2 = go.Figure(data = traces_2, layout = layout_2 )    
            graf_2 = dcc.Graph(id = 'gr-2', figure = fig_2) 

            #--------- Stock Variable ---------
            x_data3, y_data3, fechas_pedidos_sv, fechas_ingresos_sv = ss_variable.run(df_transaction, df_compras, df_pp, df_orderline, product, porcentaje_guarda, df_holt_api_week)
            
            #------------- Grafico Stock variable -----------------
            name_traces_sv = ['Real','Teorico','Stock optimo']
        
            for i in range(0,len(x_data3)):
                traces_3.append(go.Scatter(
                    x = x_data3[i],
                    y = y_data3[i],
                    mode = 'lines',
                    name = name_traces_sv[i],
                    connectgaps = True,
                    line = dict( color = color_traces[i], )
                ))

            if len(fechas_pedidos_sv) > 1:
                # Rectas de pedidos en color negro
                aux_traces = graph(fechas_pedidos_sv[0],[0], "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'grupo_pedidos',True)
                traces_3.append(aux_traces)

                aux_traces = graph(fechas_pedidos_sv, ([0]*len(fechas_pedidos_sv)), "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'grupo_pedidos',True)
                for i in aux_traces:
                    traces_3.append(i) 

                # Rectas de Ingresos en color Rojo
                aux_traces = graph(fechas_ingresos_sv[0], [0], "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'grupo_ingresos',True)
                traces_3.append(aux_traces)

                aux_traces = graph(fechas_ingresos_sv, ([0]*len(fechas_ingresos_sv)), "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'grupo_ingresos',True)
                for i in aux_traces:
                    traces_3.append(i) 

            layout = go.Layout(
                xaxis = {'title': 'Tiempo'},
                yaxis = {'title': 'Stock'},
                width = 1300,
                height = 500,
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x = 1, y = 0.5),)

            fig_3 = go.Figure(data = traces_3, layout = layout )    
            graf_3 = dcc.Graph(id = 'gr-3', figure = fig_3)

    else:
        graf_2 = None
        graf_3 = None
        alert = None
        info_alert = None
    return graf_2, graf_3, alert, info_alert

#----------------------------------------------------------------
#                       Callback Tb-2
#
# Este callback retorna los graficos para los productos en estado
# de ruptura tomando como parametros opciones:
# Poducto, porcentaje de guarda
#----------------------------------------------------------------
@app.callback(
    [Output(component_id = 'grafico-3', component_property = 'children'),
     Output(component_id = 'grafico-4', component_property = 'children'),
     Output(component_id = 'output-info', component_property = 'children'),],
    [Input(component_id = 'btn-plot-tb2', component_property = 'n_clicks'),],
    [State(component_id = 'rotura_stock', component_property = 'value'),
     State(component_id = 'porcentaje_guarda2',component_property = 'value'),]
)

def tab2(btn_tb2, producto, porcentaje_guarda2):
    if btn_tb2 != None:
        #------- Obtencion del ID de producto ---------
        product_id = df_pp[df_pp['producto'] == producto].m_product_id.values[0]

        #--------- Informe Top Ruptura---------
        x_data, y_data, fechas_ingresos, fechas_pedidos= ruptura_stock.run(df_transaction, df_orderline, df_pp, df_compras, product_id , porcentaje_guarda2, 0)

        #--------- Holt-Wilters--------------
        x_data2, y_data2 = holt_winters.run(df_holt_week, producto)
        y_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].prediccion.values.tolist())
        x_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].fecha.values.tolist())

        xr, yr, flag_alert = sales.run(df_transaction, df_pp, producto)
        x_data2.append(xr)
        y_data2.append(yr)

        #------Informacion extra-----------
        temp_df_product = df_product[ df_product['m_product_id'] == product_id ]
        dias_ruptura = (temp_df_product['total_dias_rotura'].values)[0]
        cant_ruptura = (temp_df_product['ruptura'].values)[0]
        informacion = (u' Dias sin stock: "{}", Cantidad de veces sin stock:  "{}"').format(dias_ruptura, cant_ruptura)

        #--------------- Graficos -----------------
        name_traces = ['Real','Teorico','Punto de reposicion']
        color_traces = ['rgb(7, 83, 245)','rgb(245, 119, 7)','rgb(16, 193, 10)']

        traces = []
        traces_2 = []
        #-------------------Real y Teorico------------------
        for i in range(0,len(x_data)):
            traces.append(go.Scatter(
                x = x_data[i],
                y = y_data[i],
                mode = 'lines',
                name = name_traces[i],
                connectgaps= True,
                line = dict( color = color_traces[i], )
                ))
        if len(fechas_pedidos) != 0:
            # Grafico de los Pedidos en color negro
            aux_traces = graph(fechas_pedidos[0],  y_data[2][0], "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'group_pedidos',True)
            traces.append(aux_traces) 
            aux_traces = graph(fechas_pedidos, y_data[2], "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'group_pedidos',True)
            for i in aux_traces:
                traces.append(i) 

            # Rectas de Ingresos en color Rojo
            aux_traces = graph(fechas_ingresos[0], y_data[2][0], "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'group_ingresos',True)
            traces.append(aux_traces)
            aux_traces = graph(fechas_ingresos, y_data[2], "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'group_ingresos',True)
            for i in aux_traces:
                traces.append(i) 

        #------------ Grafico Nº2--------------------- 
        name_traces_predict = ['Prediccion','Real']
        color_traces_predict = ['rgb(8, 8, 8)','rgb(7, 83, 245)']
        for i in range(0,len(x_data2)):
            traces_2.append(go.Scatter(
                x = x_data2[i],
                y = y_data2[i],
                mode = 'lines',
                name = name_traces_predict[i],
                connectgaps = True,
                line = dict( color = color_traces_predict[i], )
                ))

        layout = go.Layout(
                xaxis = {'title': 'Tiempo'},
                width = 1300,
                height = 500,
                yaxis = {'title': 'Stock'},
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x = 1, y = 0.5),)

        layout2 = go.Layout(
                xaxis = {'title': 'Tiempo'},
                width = 1300,
                height = 500,
                yaxis = {'title': 'Ventas'},
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x = 1, y = 0.5),)

        fig_1 = go.Figure(data = traces, layout = layout)
        graf_1 = dcc.Graph(id = 'gr-4', figure = fig_1)

        fig_2 = go.Figure(data = traces_2, layout = layout2 )    
        graf_2 = dcc.Graph(id = 'gr-5', figure = fig_2) 
    else:
        graf_1 = None
        graf_2 = None
        informacion = None
    return graf_1, graf_2, informacion

#-----------------------------------------------------------------
#            CallBack Tab-3 (TopSobrestock)              
#       
# Toma como paremetros el nombre del producto y el porcentaje
# de guarda, dando como salida los graficos de movimiento de stock
#------------------------------------------------------------------
@app.callback(
    [Output(component_id = 'grafico-5', component_property = 'children'),
     Output(component_id = 'grafico-11', component_property = 'children'),],
    [Input(component_id = 'btn-plot-tb3', component_property = 'n_clicks'),],
    [State(component_id = 'product_mas_stock', component_property = 'value'),
     State(component_id = 'porcentaje_guarda3',component_property = 'value'),]
)
def tab3(btn_tb3, product, porcentaje_guarda3):
    if btn_tb3 != None:
        product_id = (((df_pp[df_pp["producto"] == product]).m_product_id).values)[0]
        #x_data, y_data = sobre_stock2.run(df_transaction, df_compras, df_pp, product, porcentaje_guarda3)
        x_data, y_data, fechas_pedidos_sv, fechas_ingresos_sv = ss_variable.run(df_transaction, df_compras, df_pp, df_orderline, product, porcentaje_guarda3, df_holt_api_week)


        #--------- Holt-Wilters -------------
        x_data2, y_data2 = holt_winters.run(df_holt_week, product)
        y_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].prediccion.values.tolist())
        x_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].fecha.values.tolist())
        
        #---------- Ventas Reales------------
        xr, yr, flag_alert = sales.run(df_transaction, df_pp, product)
        x_data2.append(xr)
        y_data2.append(yr)

        traces = []
        traces_2 = []
        name_traces = ['Real','Teorico','Stock-optimo']
        color_traces = ['rgb(7, 83, 245)','rgb(245, 119, 7)','rgb(16, 193, 10)','rgb(213, 0, 255)','rgb(16, 193, 10)','rgb(7, 83, 245)', 'rgb(213, 0, 255)']

        #------------ Real y Teorico------------------
        for i in range(0,len(x_data)):
            line_type = 'lines'
            traces.append(go.Scatter(
                x = x_data[i],
                y = y_data[i],
                name = name_traces[i],
                mode = line_type,
                connectgaps= True,
                line = dict( color = color_traces[i], )
                ))
    
        if len(fechas_pedidos_sv) != 0 :
            # Rectas de pedidos en color negro
            aux_traces = graph(fechas_pedidos_sv[0], [0], "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'group_pedidos',True)
            traces.append(aux_traces)
            aux_traces = graph(fechas_pedidos_sv, ([0]*len(fechas_pedidos_sv)), "Pedidos", 'rgb(0, 0, 0)' ,'dot', 'group_pedidos',True)
            for i in aux_traces:
                traces.append(i) 

        if len(fechas_ingresos_sv) != 0:
            # Rectas de Ingresos en color Rojo
            aux_traces = graph(fechas_ingresos_sv[0], [0], "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'group_ingresos',True)
            traces.append(aux_traces)
            aux_traces = graph(fechas_ingresos_sv, ([0]*len(fechas_ingresos_sv)), "Ingresos", 'rgb(209, 16, 10)' ,'dot', 'group_ingresos',True)
            for i in aux_traces:
                traces.append(i) 
        
        #------------ Grafico Nº2--------------------- 
        name_traces_predict = ['Prediccion','Real']
        color_traces_predict = ['rgb(0, 0, 0)', 'rgb(7, 83, 245)']

        for i in range(0,len(x_data2)):
            traces_2.append(go.Scatter(
                x = x_data2[i],
                y = y_data2[i],
                mode = 'lines',
                name = name_traces_predict[i],
                connectgaps = True,
                line = dict( color = color_traces_predict[i], )
                ))

        layout = go.Layout(
                xaxis = {'title': 'Tiempo'},
                yaxis = {'title': 'Stock'},
                width = 1300,
                height = 500,
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x=1, y=0.5),)
        layout2= go.Layout(
                xaxis = {'title': 'Tiempo'},
                yaxis = {'title': 'Ventas'},
                width = 1300,
                height = 500,
                margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend = dict(x=1, y=0.5),)
        
        
        fig_1 = go.Figure(data = traces, layout = layout)
        graf_1 = dcc.Graph(id = 'gr-6', figure = fig_1)
        
        fig_2 = go.Figure(data = traces_2, layout = layout2)
        graf_2 = dcc.Graph(id = 'gr-7', figure = fig_2)
    else:
        graf_1 = None
        graf_2 = None
    return graf_1, graf_2 

#--------------------------------------------------------------------
#               CallBack Tab-4(Predictores)
#
# Toma como parametros de entrada el producto seleccionado,
# proveedor y agrupamiento (semanal o mensual), retorna el grafico
#  de las ventas historicas y las predicciones de los modelos 
# junto a un grafico de barras de forma comparativa para el analisis.
#---------------------------------------------------------------------
@app.callback(
    [Output(component_id = 'grafico-prophet', component_property = 'children'),
     Output(component_id = 'grafico-bar', component_property = 'children'),],
    [Input(component_id = 'btn-plot-tb4', component_property = 'n_clicks')],
    [State(component_id = 'proveedor_prophet', component_property = 'value'),
     State(component_id = 'product_prophet',component_property = 'value'),
     State(component_id = 'analisis-option',component_property = 'value'),]
)
def predictores(btn_tb4, proveedor_prophet, product_prophet, analisis):
    if btn_tb4 != None:
        product_id = (((df_pp[df_pp["producto"] == product_prophet]).m_product_id).values)[0]

        #---------- Analisis Semanal--------------
        if analisis == 'semanal':
            #--------- Modelo Prophet -------------
            x_data, y_data = predictor.run(df_transaction, df_prophet_week, df_pp, product_prophet )
            x_data2, y_data2 = holt_winters.run(df_holt_week, product_prophet)
            #--------- Holt-wilters ---------------
            y_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].prediccion.values.tolist())
            x_data2[0].extend(df_holt_api_week[df_holt_api_week['product_id'] ==  product_id].fecha.values.tolist())

        #-------- Analisis Mensual------------
        else: 
            #--------- Modelo Prophet -------------
            x_data, y_data = predictor.run(df_transaction_month, df_prophet_month, df_pp, product_prophet )
            x_data2, y_data2 = holt_winters.run(df_holt_month, product_prophet)
            #--------- Holt-wilters ---------------
            y_data2[0].extend(df_holt_api_month[df_holt_api_month['product_id'] ==  product_id].prediccion.values.tolist())
            x_data2[0].extend(df_holt_api_month[df_holt_api_month['product_id'] ==  product_id].fecha.values.tolist())

        for i in range(0,len(x_data2)):
            x_data.append(x_data2[i])
            y_data.append(y_data2[i])
        
        #-----------Graficos-------------------
        name_traces_predict = ['Ventas historicas','Prophet','Holt-Winter']
        color_traces_predict = ['rgb(7, 83, 245)','rgb(86, 211, 5)', 'rgb(10, 10, 10)']
        traces = []

        #----------Tabla---------------
 
        for i in range(0,len(x_data)):
            traces.append(go.Scatter(
                x = x_data[i],
                y = y_data[i],
                name = name_traces_predict[i],
                mode = 'lines',
                connectgaps= True,
                line = dict( color = color_traces_predict[i], )
                ))

        layout = go.Layout(
                font=dict(
                    family="Courier New, monospace",
                    size=12,
                    color="#7f7f7f"
                    ),
                xaxis = {'title': 'Tiempo'},
                yaxis = {'title': 'Ventas'},
                width = 1300, 
                height = 500,
                margin = {'l': 40, 'b': 40, 't': 50, 'r': 10},
                legend = dict(x = 1, y = 0.5))

        fig_1 = go.Figure(data = traces, layout = layout)
        graf_1 = dcc.Graph(id = 'gr-prophet', figure = fig_1)          
       
        #------------Grafico de Barras-------------------
        meses = ['','Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Dieciembre']
        
        def dateConvert(fecha):
            date_o = (fecha).split('-')
            date_i = int(date_o[1])-4
            if date_i < 10:
                date_o[1] = '0' + str(date_i)
            else:
                date_o[1] = str(date_i)   
            return ("-".join(date_o))
        
        df_transaction_aux = df_transaction_month[df_transaction_month['m_product_id'] == product_id]
        df_holt_month_aux = df_holt_month[df_holt_month['producto_name'] == product_prophet]

        meses = meses[int((df_transaction_aux.tail(1).movementdate.values[0]).split('-')[1])-4 : int((df_transaction_aux.tail(1).movementdate.values[0]).split('-')[1])-1]

        y_real = abs(df_transaction_month[(df_transaction_month['movementdate'] >= dateConvert(df_transaction_aux.tail(1).movementdate.values[0])) 
                                & (df_transaction_month['movementdate'] < df_transaction_aux.tail(1).movementdate.values[0]) 
                                & (df_transaction_month['m_product_id']== product_id)
                                & (df_transaction_month['movementtype']== 'C-')].movementqty).tolist()

        y_prediccion = df_holt_month[(df_holt_month['fecha'] > dateConvert(df_holt_month_aux.tail(1).fecha.values[0])) 
                        & (df_holt_month['fecha'] < df_holt_month_aux.tail(1).fecha.values[0]) 
                        & (df_holt_month['producto_name'] == product_prophet)].prediccion.values.tolist()

        y_prediccion_prophet = df_prophet_month[(df_prophet_month['movementdate'] > dateConvert(df_transaction_aux.tail(1).movementdate.values[0])) 
                                & (df_prophet_month['movementdate'] < df_transaction_aux.tail(1).movementdate.values[0]) 
                                & (df_prophet_month['m_product_id'] == product_id)].movementqty.values.tolist()
        fig_3 = go.Figure()

        fig_3.add_trace(go.Bar(
            x = meses,
            y = y_real,
            name = 'Real',
            marker_color = 'rgb(7, 83, 245)'
        ))
        fig_3.add_trace(go.Bar(
            x = meses,
            y = y_prediccion,
            name = 'Prediccion',
            marker_color = 'rgb(10, 10, 10)'
        ))
        fig_3.add_trace(go.Bar(
            x = meses,
            y = y_prediccion_prophet,
            name = 'Prediccion-prophet',
            marker_color = 'rgb(86, 211, 5)'
        ))
        # Change the bar mode
        fig_3.update_layout(barmode='group')
        graf_2 = dcc.Graph(id = 'gr-prophet', figure = fig_3)

    else:
        graf_1 = None
        graf_2 = None
        
    return graf_1 , graf_2

#------------------------------------------------------
#         Calback TAB-6 (Compras)
#
# Este callback actualiza la solapa de compras
# Devolviendo la tabla con la sugerencias de compra
# ademas da informacion sobre el monto minimo requerido
# por el proveedor seleccionado
#------------------------------------------------------
timestamp_actual = None
timestamp_anterior = None

@app.callback(
    [Output(component_id ='tabla_tb6', component_property = 'data'),
     Output(component_id = "alerta_tb6", component_property = "is_open"), 
     Output(component_id = "alerta_tb6", component_property = "children"),
     Output(component_id = "alerta_tb6", component_property = "color"),
     Output(component_id = 'output-info_tb6', component_property = 'children'),],
    [Input(component_id = 'tabla_tb6', component_property = 'data_timestamp'),
     Input(component_id = 'btn_tb6', component_property = 'n_clicks'),],
    [State(component_id = 'tabla_tb6', component_property = 'data'),
     State(component_id = 'porcentaje_guarda6',component_property = 'value'),
     State(component_id = 'proveedor_compra', component_property = 'value'),])

def tab6(timestamp, btn_6,rows,porcentaje_guarda6 ,proveedor_compra  ):
    # Variables para saber cuando se realiza una modificacion en la tabla
    global timestamp_anterior
    global timestamp_actual
    timestamp_actual = timestamp
    
    if btn_6 != None:
        # En caso de detectar una modificacion recalculamos la compra       
        if timestamp_actual != timestamp_anterior:
            timestamp_anterior = timestamp_actual
            gasto_total = 0
            
            # Iteracion sobre la tabla y recalculo del monto total a gastar
            for row in rows:
                try:
                    row['Valor'] = round(float(row['Pedido Nº 1']) * float(row['Precio c/u']))
                    gasto_total += float(row['Valor'])
                except:
                    row['Valor'] = 'NA'
       
            # Se busca la orden minima pretendidad por el proveedor en caso de no existir asignamos 0
            if not(df_monto_min_provider[df_monto_min_provider['proveedor'] == proveedor_compra].empty):
                orden_minima = df_monto_min_provider[df_monto_min_provider['proveedor'] == proveedor_compra].orden_minima.values[0]
            else:
                orden_minima = 0
            
            # Chequeamos si el gasto total alcanza al monto minimo de compra y damos aviso del estado
            if gasto_total >= orden_minima:
                flag_alert = True
                color = "success"
                alert = 'El pedido puede ser generado'
            else:
                flag_alert = True
                color = "danger"
                alert = 'No se alcanza al monto minimo requerido por el proveedor por favor realice un reajuste'

            info = 'Gasto registrado: ' + str(gasto_total)

        else:
            # Inicia si una modificacion en tabla por lo que generamos el pedido de compra con la sig funcion
            df_info, demora = orden_compra.run(proveedor_compra ,df_pp, df_monto_min_provider, df_precios,df_min_order,porcentaje_guarda6)
            rows = df_info.to_dict('records')
            
            gasto_total = df_info['Valor'].sum()
            
            # Se busca la orden minima pretendidad por el proveedor en caso de no existir asignamos 0
            if not(df_monto_min_provider[df_monto_min_provider['proveedor'] == proveedor_compra].empty):
                orden_minima = df_monto_min_provider[df_monto_min_provider['proveedor'] == proveedor_compra].orden_minima.values[0]
            else:
                orden_minima = 0

            # Chequeamos si el gasto total alcanza al monto minimo de compra y damos aviso del estado
            if gasto_total >= orden_minima:
                flag_alert = True
                color = "success"
                alert = 'El pedido puede ser generado'
            else:
                flag_alert = True
                color = "danger"
                alert = 'No se alcanza al monto minimo requerido por el proveedor por favor realice un reajuste'

            info = 'Gasto registrado: ' + '€ ' + str(gasto_total) + ' | Demora del proveedor: ' + str(demora) + ' días.'
    else:
        flag_alert = None
        alert = None
        color = None
        info = None
    return rows, flag_alert ,alert, color, info
    
server = app.server


if __name__ == '__main__':
    #app.run_server(debug = True)
    app.run_server(port = 6500, debug = False, host = '95.179.129.98')

