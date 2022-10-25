# Databricks notebook source
import os
from pyspark.sql.functions import col, substring, row_number, concat, monotonically_increasing_id, lit, when
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

"""Parametros enviado desde Factory """

dbutils.widgets.text("CarpetaBlob", "","")  
dbutils.widgets.get("CarpetaBlob")
rowKey = getArgument("CarpetaBlob")


if rowKey.count('/') > 0:
  schema, appication = rowKey.split('/') 
else: 
  appication = rowKey

  
#Variavles Globales Staticas

file_location = "/mnt/app-aldon/"
mount = '/dbfs'
zona_crudos = '/raw/'
zona_result = '/results/'
name_panel = 'panel_'
data_base = 'aldon'
name_table = 'Zdr_'
tabla_metadatos = 'aldon.zdr_aldon_src_metadatos_inventario_tablas_bd'
tabla_indices = 'aldon.zdr_aldon_src_metadatos'

# Diccionario Dataframe Dinamicos
panels_frames = {}
DataFrames = {}

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = "|"
file_type = "csv"
sep_line = 100

# COMMAND ----------

"""
Descripción: Crea la tabla principal con todas las tablas
Responsable: Juan David Escobar E.
Fecha: 03/05/2021
"""

def ft_crear_panel_principal(ar_df):
  try:
    Identity = Window.orderBy(F.col('NombreSubAplicacion'))
    df_final = df.select('NombreSubAplicacion').withColumn('Id', F.row_number().over(Identity))
    df_final.repartition(1).write.format('delta'
                                         ).mode("overwrite"
                                         ).option('path', file_location + 'panel_principal' + zona_result + 'panel_principal' 
                                         ).saveAsTable(data_base + "." + name_table +  data_base + '_' 'panel_principal')
    print('Se creo la tabla principal Correctamente' + '\n')
  
  except Exception as error:
    print('error al crear la tabla principal en la funcion ft_crear_panel_principal' '\n' + str(error))

# COMMAND ----------

"""
Descripción: crea un archivo en el Blob Storage con el describe de la data
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_describe_tabla(ar_df, ar_application):
  try:
    path_escritura = '/mnt/app-aldon-storage/' + ar_application 
    dbutils.fs.rm(path_escritura + '/metadata_aldon_db.txt', True)
    sqlContext.registerDataFrameAsTable(ar_df, ar_application)
    meta = sqlContext.sql('describe ' + ar_application)
    pmeta = meta.select("*").toPandas()
    pmeta.to_csv("/dbfs" + path_escritura + '/metadata_aldon_db.txt', index=False, columns=['col_name', 'data_type'], sep='|')
    print('Archivo metadata creado correctamente')
  except Exception as error:
    print('No se creo el archivo metadata de la aplicacion ' + ar_application)

# COMMAND ----------

"""
Descripción: Crea los dataframe de cada subaplicacion Dinammicos y los alamcena en un diccionar de datos.
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_lectura_df_app(ar_name_app):
  try:
    panels_frames[ar_name_app] = spark.read.format(file_type) \
                                           .option("inferSchema", infer_schema) \
                                           .option("header", first_row_is_header) \
                                           .option("sep", delimiter) \
                                           .load(file_location + ar_name_app + zona_crudos)
    
    ft_describe_tabla(panels_frames[ar_name_app], ar_name_app)
    print('la aplicacion', ar_name_app, 'se creo correctamente')
    
  except Exception as error:
    print('error al leer la aplicacion ',  ar_name_app + ' en la funcion ft_lectura_df_app' '\n' + str(error))

# COMMAND ----------

"""
Descripción: crea el indice o el zorder para el campo de busqueda.
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_zorder_table(ar_tabla, ar_tabla_result):
  try:
    df_indice = sqlContext.sql("select NombreCampoTecnico from " + tabla_indices + " where OrigenSubAplicacion = '" + appication  + "' and Upper(EsCampoBusqueda) = 'SI'")
    for index in df_indice.collect():
      sqlContext.sql('OPTIMIZE ' + ar_tabla_result + ' ZORDER BY ' + index['NombreCampoTecnico'])
      print('se creo el indice para la tabla ' + ar_tabla_result + ' para el campo ' + index['NombreCampoTecnico'])
      
  except Exception as error:
    print('Error en la funcion ft_zorder_table al crear el indice en la tabla ' + ar_tabla_result + '\n' + str(error))

# COMMAND ----------

"select NombreCampoTecnico from " + tabla_indices + " where OrigenSubAplicacion = '" + appication  + "' and Upper(EsCampoBusqueda) = 'SI'"

# COMMAND ----------

def ft_autoidentity(ar_df, ar_consecutido):
  try:
    window = Window.orderBy(F.monotonically_increasing_id())
    DataFrames['df_' + str(ar_consecutido)] = ar_df.withColumn('index', F.row_number().over(window))

  except Exception as error:
    print('Error en la funcion ft_autoidenity al crear el indice dinamico ' + '\n' + str(error))

# COMMAND ----------

"""
Descripción: Materializar y Almacenar las tablas en la zona de resultados
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

from pyspark.sql import DataFrame
from functools import reduce

def ft_materializar_tablas(ar_name_df):
  try:
    table_result = data_base + "." + name_table +  data_base + '_' + name_panel + ar_name_df    
    sqlContext.sql('drop table if exists ' + data_base + "." + name_table +  data_base + '_' + name_panel + ar_name_df)
    panels_frames[ar_name_df].write.format('delta'
                                           ).mode("overwrite"                                           
                                           ).option('path', file_location + ar_name_df + zona_result + name_panel + ar_name_df
                                           ).saveAsTable(table_result)
    
    ft_zorder_table(ar_name_df , table_result)
    print('Tabla: ' + data_base + "." + name_table +  data_base + '_' + ar_name_df + ' Creada correctamente')
   
  except Exception as error:
    print('Error al almacenar la informacion o la tabla:' , ar_name_df + ' en la funcion ft_materializar_tablas' +  '\n' + str(error))

# COMMAND ----------

"""
Descripción: Llama todos los metodo para el funcionamiento correcto de la aplicacion aldon
             Crea la base de datos en caso de que no exita
             Construye la tabla principal con el nombre de la tabla y la fecha para extraer
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""


sqlContext.sql('create database if not exists ' + data_base)
sqlContext.sql('refresh table ' + tabla_metadatos)
###df = sqlContext.sql("select NombreSubAplicacion, CampoFechaBusqueda from " +  tabla_metadatos)
df = sqlContext.sql("select NombreSubAplicacion  from " +  tabla_metadatos)
ft_crear_panel_principal(df)

display(df)

for app in df.filter(col('NombreSubAplicacion') == appication).collect():
  try:
    print(app['NombreSubAplicacion'])    
    ft_lectura_df_app(app['NombreSubAplicacion'])
    ####ft_substring_fecha(app['NombreSubAplicacion'], app['CampoFechaBusqueda'])
    ft_materializar_tablas(app['NombreSubAplicacion'])
    print('todo el proceso se corrio correctamente para ', app['NombreSubAplicacion'] + '\n' + '*' * sep_line)
    
  except Exception as error:
    print('error en la funcion al procesar la aplicacion ' + app['NombreSubAplicacion'] + '\n' + '*' * sep_line)

# COMMAND ----------

tabla_metadatos

# COMMAND ----------

#dbutils.notebook.run('/app/aldon/4_CreateFilterSearch', 259200)
