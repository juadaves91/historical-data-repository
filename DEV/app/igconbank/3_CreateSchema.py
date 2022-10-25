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

file_location = "/mnt/app-igconbank/"
mount = '/dbfs'
zona_crudos = '/raw/'
zona_result = '/results/'
name_panel = 'panel_'
data_base = 'igconbank'
name_table = 'Zdr_'
tabla_metadatos = 'igconbank.zdr_igconbank_src_metadatos_inventario_tablas_bd'
tabla_indices = 'igconbank.zdr_igconbank_src_metadatos'
tabla_paginado = 'cruceautomatico'
cantidad_min = 1
cantidad_max = 150000

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
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_crear_panel_principal(ar_df):
  try:
    Identity = Window.orderBy(F.col('NombreSubAplicacion2'))
    df_final = df.select('NombreSubAplicacion2').withColumn('Id', F.row_number().over(Identity))
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
    path_escritura = '/mnt/app-igconbank-storage/' + ar_application 
    dbutils.fs.rm(path_escritura + '/metadata_igconbank_db.txt', True)
    sqlContext.registerDataFrameAsTable(ar_df, ar_application)
    meta = sqlContext.sql('describe ' + ar_application)
    pmeta = meta.select("*").toPandas()
    pmeta.to_csv("/dbfs" + path_escritura + '/metadata_igconbank_db.txt', index=False, columns=['col_name', 'data_type'], sep='|')
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
Descripción: Genera los campos anio, mes y dia para el particionamiento.
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""

def ft_substring_fecha(ar_df, ar_campo_fecha):
  try:
    panels_frames[ar_df] = panels_frames[ar_df].withColumn('dia', substring(col(ar_campo_fecha), 1,2))
    panels_frames[ar_df] = panels_frames[ar_df].withColumn('mes', substring(col(ar_campo_fecha), 4,2))
    panels_frames[ar_df] = panels_frames[ar_df].withColumn('anio', substring(col(ar_campo_fecha),7,4))
    panels_frames[ar_df] = panels_frames[ar_df].withColumn('Fecha_Busqueda', concat(col('anio'),col('mes'),col('dia')))
    
    print('se extrajo el dia, mes y anio correctamente para la aplicacion', ar_df)
    
  except Exception as error:
    print('error al extraer el dia, mes o anio', 'de la aplicacion', ar_df + ' en la funcion ft_substring_fecha' + '\n' + str(error))

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
    if  appication == tabla_paginado:
        df = panels_frames[ar_name_df].select('*', concat(col('NUM_CTA'),col('ANIO'),col('MES'),col('DIA')).alias('LLAVE_COMPUESTA'))
        for i, index in enumerate(df.select(col('LLAVE_COMPUESTA')).distinct().collect()):
          df_tmp = df.filter(col('LLAVE_COMPUESTA') == index['LLAVE_COMPUESTA'])
          ft_autoidentity(df_tmp, i)
        df_temp = reduce(DataFrame.unionAll, [DataFrames[index] for index in DataFrames.keys()])
        panels_frames[ar_name_df] = df_temp.withColumn('PAGINADO',
                                                       when((col('index') >= cantidad_min) & (col('index') <= cantidad_max), 'PAGINA1').otherwise('PAGINA2'))


    table_result = data_base + "." + name_table +  data_base + '_' + name_panel + ar_name_df
    sqlContext.sql('drop table if exists ' + data_base + "." + name_table +  data_base + '_' + name_panel + ar_name_df)
    panels_frames[ar_name_df].repartition('anio','mes'
                                           ).write.format('delta'
                                           ).mode("overwrite"
                                           ).partitionBy("anio","mes"
                                           ).option('path', file_location + ar_name_df + zona_result + name_panel + ar_name_df
                                           ).saveAsTable(table_result)
    
    ft_zorder_table(ar_name_df , table_result)
    print('Tabla: ' + data_base + "." + name_table +  data_base + '_' + ar_name_df + ' Creada correctamente')
   
  except Exception as error:
    print('Error al almacenar la informacion o la tabla:' , ar_name_df + ' en la funcion ft_materializar_tablas' +  '\n' + str(error))

# COMMAND ----------

"""
Descripción: Llama todos los metodo para el funcionamiento correcto de la aplicacion Igconbank
             Crea la base de datos en caso de que no exita
             Construye la tabla principal con el nombre de la tabla y la fecha para extraer
Responsable: Blaimir Ospina Cardona.
Fecha: 08/25/2020
"""


sqlContext.sql('create database if not exists ' + data_base)
sqlContext.sql('refresh table ' + tabla_metadatos)
df = sqlContext.sql("select NombreSubAplicacion2, CampoFechaBusqueda from " +  tabla_metadatos)
ft_crear_panel_principal(df)

for app in df.filter(col('NombreSubAplicacion2') == appication).collect():
  try:
    ft_lectura_df_app(app['NombreSubAplicacion2'])
    ft_substring_fecha(app['NombreSubAplicacion2'], app['CampoFechaBusqueda'])
    ft_materializar_tablas(app['NombreSubAplicacion2'])
    print('todo el proceso se corrio correctamente para ', app['NombreSubAplicacion2'] + '\n' + '*' * sep_line)
    
  except Exception as error:
    print('error en la funcion al procesar la aplicacion ' + app['NombreSubAplicacion2'] + '\n' + '*' * sep_line)
