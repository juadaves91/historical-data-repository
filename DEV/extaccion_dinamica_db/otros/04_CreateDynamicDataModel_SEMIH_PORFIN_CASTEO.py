# Databricks notebook source
# MAGIC %md
# MAGIC #CREACION MODELO DE DATOS

# COMMAND ----------

from pyspark.sql.types import ArrayType, IntegerType, ShortType, StringType, StructType, DoubleType, TimestampType, FloatType, MapType, ByteType, LongType, BooleanType, BinaryType, DecimalType, DateType
from pyspark import SparkContext, SparkConf, SQLContext
from datetime import datetime
import pytz
from pyspark.sql import Row
from pyspark.sql.functions import udf, col, concat, lit, when, regexp_replace
from pyspark.sql import functions as F
import hashlib
import os
import pandas as pd
import numpy as np
import math
import time
import logging, sys

# COMMAND ----------

"""
Variables para eliminación de registro de aud en tabla: auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

folder_blob = {"FolderBlob":"app-semih/metadatos"} (Muy importante llevar conservar la taxonomia app-nombre_app)
"""



#Variables globales -- Rutas

path_results = '/mnt/rdh-auditoria/results/zdr_aux_paginado'
path_index = '/mnt/rdh-auditoria/results/'
name_file_index = 'Zdr_indice_aux.csv'
mount_point = '/dbfs'
folder_aux = 'indice_aux/'

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""---------------------------Parámetros de entrada Info tabla----------------------------"""

# FOLDER BLOB: app-semih.
dbutils.widgets.text("folder_blob", "","")
dbutils.widgets.get("folder_blob")
folder_blob = getArgument("folder_blob")

# APP
application_folder_blob = folder_blob
print(type(application_folder_blob), application_folder_blob)
application_folder_blob = folder_blob.split('/')[0]
print(application_folder_blob)
application = application_folder_blob.split('-')[1]
application = application.strip().upper()
print(application)

# TABLAS METADATOS
tbl_metadatos =  application + '.' + 'Zdr_' + application + '_Src_Metadatos'

# TABLAS INVENTARIO
tbl_inventario =  application + '.' + 'Zdr_' + application + '_src_metadatos_inventario_tablas_bd'

# COMMAND ----------

# MAGIC %md
# MAGIC #ZONA DE CRUDOS

# COMMAND ----------

"""
Antes de ejecutar el proximo paso se debe hacer un left join entre la tabla auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob
y la tabla auditoria.Zdr_Auditoria_Log_Carga_BD_Tbl_In_Result_DL EN EL NOOTEBOK 01_CreacionTablasMetadata...
"""

# COMMAND ----------

"""
Descripción: Loop tabla auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob where status = 'succeded' para
             la creación de sentencia DDL dinámica a partir del inventario de tablas cargadas previamente en 
             el recurso Azure Blob Storage en formao .parquet, se excluyen la tablas que ya se encuentran cargadas previamente en el 
             Azure DATA LAKE.
             Sentencia SQL:
             --Tablas a reprocesar / re cargar
             --UNION
             --Tablas nuevas previamente cargadas en blob y que se van a cargar que no existen en el DataLake
             Tipos de datos: https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html.
Autor: Juan David Escobar E.
Fecha creción: 22/09/2020.
Fecha modificación: 22/09/2020.
# query = 'SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob WHERE status = ' + "'" + status  + "'" + ' and nombre_app_origen = '  + "'" + application  + "'" 
"""
status = 'Succeeded'

query = 'SELECT *  \
         FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob  \
         WHERE rowKey in (SELECT UPPER(concat(NombreAplicacion, "-", NombreSubAplicacion, "-", NombreTablaAplicacion)) as rowKey  \
         FROM  SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd \
         WHERE RequiereReprocesarCarga = "Si") \
         UNION \
         SELECT TBL_BLOB.* \
         FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob AS TBL_BLOB \
         LEFT JOIN auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake AS TBL_DL \
         ON TBL_BLOB.rowKey = TBL_DL.row_key \
         WHERE TBL_DL.row_key IS NULL \
             AND TBL_BLOB.status = '  + "'" + status  + "'" + ' and TBL_BLOB.nombre_app_origen = ' + "'" + application  + "'"  + ' ORDER BY rowKey asc '

print(query)
df_pys_list_loaded_tbls_in_blob = sqlContext.sql(query)

if df_pys_list_loaded_tbls_in_blob.count() > 0:
  df_pd_list_loaded_tbls_in_blob = df_pys_list_loaded_tbls_in_blob.toPandas()
  display(df_pd_list_loaded_tbls_in_blob)

# COMMAND ----------

# MAGIC %sql 
# MAGIC REFRESH TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob;
# MAGIC refresh TABLE auditoria.zdr_auditoria_log_extraction_bd_tbl_in_data_lake;
# MAGIC refresh TABLE SEMIH.zdr_semih_src_metadatos_inventario_tablas_bd;
# MAGIC refresh TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob

# COMMAND ----------

"""
Descripción: Retorna objeto tipo dic con el esquema de la tabla definida en el origen.
             en la tabla de la ZDC.
Autor: Juan David Escobar E.
Fecha creción: 06/10/2020.
"""

def getSchemaZdc(prm_list_fields):
  
  schema_dict = {'fields': [], 'type': 'struct'}
  
  for i in  prm_list_fields:
    is_nullable = True if i.ValorNulo == 'Si' else False 
    schema_dict['fields'].append({'metadata': {}, 'name': i.NombreCampoFuncional, 'nullable': is_nullable, 'type': i.TipoDatoSpark})
    
  return schema_dict

# COMMAND ----------

"""
Descripción: Obtener df y list de los metadatos de la tabla iterada y campos de busqueda
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getObjectsCurrentMetadata(prm_nombre_tbl_app_origen, prm_flag):
  
  sqlContext.sql('REFRESH TABLE ' + tbl_metadatos)
  query_metadata = 'SELECT * FROM ' +  tbl_metadatos +\
                   ' WHERE TablaOrigen = ' + "'" + prm_nombre_tbl_app_origen  +\
                   "'" + ' and EsMigrable = ' + "'" + prm_flag  + "'" + ' and EsVisible = ' + "'" + prm_flag  + "'"
    
  campos_str = '"NombreCampoFuncional", "Formato", "TipoDatoHive", "TipoDatoSpark", "ValorNulo", "EsquemaSpark"'
  
  df_current_metadata = sqlContext.sql(query_metadata)
  
  lista_campo_fecha = df_current_metadata.filter("FechaBusqueda  == 'Si'").select("NombreCampoTecnico",
                                                                             "NombreCampoFuncional",
                                                                             "Formato",
                                                                             "TipoDatoHive",
                                                                             "TipoDatoSpark",
                                                                             "ValorNulo",
                                                                             "EsquemaSpark")
  
  lista_campos_visualizar = df_current_metadata.filter("FechaBusqueda  == 'No'").select("NombreCampoTecnico",
                                                                                        "NombreCampoFuncional",
                                                                                        "Formato",
                                                                                        "TipoDatoHive",
                                                                                        "TipoDatoSpark",
                                                                                        "ValorNulo",
                                                                                        "EsquemaSpark")
  if lista_campo_fecha.count() > 0:
    lista_campo_fecha_pd = lista_campo_fecha.toPandas()
    lista_campos_visualizar_pd = lista_campos_visualizar.toPandas()
    lista_campos_visualizar_pd = pd.concat([lista_campo_fecha_pd, lista_campos_visualizar_pd]) 
    lista_campos_visualizar = spark.createDataFrame(lista_campos_visualizar_pd).collect()
  else:
    lista_campos_visualizar = lista_campos_visualizar.collect()
  
  lista_campos_busqueda = df_current_metadata.filter("EsCampoBusqueda  == 'Si'").select("NombreCampoTecnico",
                                                                                         "NombreCampoFuncional",
                                                                                         "Formato",
                                                                                         "TipoDatoHive",
                                                                                         "TipoDatoSpark",
                                                                                         "ValorNulo",
                                                                                         "EsquemaSpark").collect()

  return df_current_metadata, lista_campos_visualizar, lista_campos_busqueda

# COMMAND ----------

# MAGIC %md
# MAGIC #ZONA DE PROCESADOS

# COMMAND ----------

"""
Descripción: Obtener list str de listado de campos tecnicos y funcionales para query dinámico.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getObjectsListSearchFiles(prm_lista_campos_visualizar, prm_lista_campos_busqueda):
  
  campos_lista_zdr = []
  campos_lista_zdr_md5 = []
    
  for i in prm_lista_campos_visualizar:
    campos_lista_zdr_md5.append(str('CAST(' + i.NombreCampoTecnico + " AS " + i.TipoDatoHive + ") "))  

    if ('DECIMAL') in i.TipoDatoHive or ('FLOAT') in i.TipoDatoHive or ('INT') in i.TipoDatoHive or ('BIGINT') in i.TipoDatoHive:
#       str_query_trailing = "TRIM(TRAILING '.' FROM TRIM(TRAILING '0' from " +  i.NombreCampoTecnico + ")) "  
#       str_query_trailing = 'CAST(' + str_query_trailing + " AS " + i.TipoDatoHive + ") " +  i.NombreCampoTecnico 
      str_query_trailing = 'CAST(REPLACE(' + i.NombreCampoTecnico + ', ",", ".") AS ' + i.TipoDatoHive + ') ' + i.NombreCampoTecnico
      
      campos_lista_zdr.append(str_query_trailing)
      
    elif ('DATE') in i.TipoDatoHive:
      campos_lista_zdr.append('CAST(REPLACE(' + i.NombreCampoTecnico + ', "/", "-") AS DATE) AS ' + i.NombreCampoTecnico)

    else:
      campos_lista_zdr.append(str('CAST(' + i.NombreCampoTecnico + " AS " + i.TipoDatoHive + ") "))
  
  
  campos_lista_zdc = [i.NombreCampoTecnico for i in prm_lista_campos_visualizar]
  campos_str_zdr = ', '.join([str(x) for (i, x) in enumerate(campos_lista_zdr)])
  campos_str_zdr_md5 = ', '.join([str(x) for (i, x) in enumerate(campos_lista_zdr_md5)])
  campos_str_zdc = ', '.join([str(x) for (i, x) in enumerate(campos_lista_zdc)])
  nombre_campo_tecnico_lista = [str(i.NombreCampoTecnico) for i in prm_lista_campos_busqueda]
  nombre_campo_funcional_lista = [str(i.NombreCampoFuncional) for i in prm_lista_campos_busqueda]
  
  lista_campos_consulta = []  
  for i in prm_lista_campos_busqueda:
    
    if ('DECIMAL') in i.TipoDatoHive or ('FLOAT') in i.TipoDatoHive or ('INT') in i.TipoDatoHive or ('BIGINT') in i.TipoDatoHive:
#       str_query_trailing = "TRIM(TRAILING '.' FROM TRIM(TRAILING '0' from " +  i.NombreCampoTecnico + "))" 
#       str_query_trailing = 'CAST(' + str_query_trailing + " AS " + i.TipoDatoHive + ") "
      str_query_trailing = 'CAST(REPLACE(' + i.NombreCampoTecnico + ', ",", ".") AS ' + i.TipoDatoHive + ') '
      
      lista_campos_consulta.append(str_query_trailing)
      
    elif ('DATE') in i.TipoDatoHive:
      lista_campos_consulta.append('CAST(REPLACE(' + i.NombreCampoTecnico + ', "/", "-") AS DATE)')
      
    else:      
      lista_campos_consulta.append(str(i.NombreCampoTecnico))
  
#   campos_consulta_str = ', '.join([str(x) if i == len(nombre_campo_tecnico_lista) - 1 else str(x) + ','  for (i, x) in enumerate(lista_campos_consulta)])
  campos_consulta_str = ', '.join(lista_campos_consulta)


  return campos_str_zdr_md5, campos_str_zdr, campos_str_zdc , nombre_campo_funcional_lista, campos_consulta_str

# COMMAND ----------

"""
Descripción: Obtener fecha actual.
Autor: Juan David Escobar E.
Fecha creción: 08/01/2021.
"""

def fn_get_current_load_date():
  
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  current_date = str(date.strftime("%d/%m/%Y"))
  
  campo_anio = current_date[6:10]
  campo_mes = current_date[3:5]
  campo_dia = current_date[0:2]
      
  return current_date, campo_anio, campo_mes, campo_dia

# COMMAND ----------

"""
Descripción: Obtiene cadena SQL que incluye los campos calculados: fecha, busqueda, AÑO y MES.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getCalculatedFields(prm_df_current_metadata, prm_query_result):
  
  df_current_metadata_campos_busqueda_fecha = prm_df_current_metadata.filter("FechaBusqueda  == 'Si'").select("NombreCampoTecnico",
                                                                                                          "NombreCampoFuncional",
                                                                                                          "Formato")
  
  if df_current_metadata_campos_busqueda_fecha.count() > 0:
  
    campo_fecha_tecnico = df_current_metadata_campos_busqueda_fecha.first()["NombreCampoTecnico"]  
    campo_fecha_funcional = df_current_metadata_campos_busqueda_fecha.first()["NombreCampoFuncional"]  
    campo_anio = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 0, 4) AS STRING) ANIO_CALCULADO_RDH'
    campo_mes  = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 6, 2) AS STRING) MES_CALCULADO_RDH'
    campo_dia  = 'CAST(SUBSTR(' + campo_fecha_tecnico + ', 9, 2) AS STRING) DIA_CALCULADO_RDH'

    campo_dim_tiempo = 'CAST(CONCAT(SUBSTR(' + campo_fecha_tecnico + ', 9, 2), SUBSTR(' + campo_fecha_tecnico +   ', 6, 2) , SUBSTR(' + campo_fecha_tecnico + ' , 0, 4)) AS STRING) AS FECHA_DIM_TIEMPO_CALCULADO_RDH'
    prm_query_result = prm_query_result + ', ' + campo_anio + ', ' + campo_mes + ', ' + campo_dia + ', ' + campo_dim_tiempo
  
  else:
    campo_fecha_tecnico = 'FECARGARDH'
    campo_fecha_funcional = 'FECARGARDH'
    current_date, campo_anio, campo_mes, campo_dia = fn_get_current_load_date()

    campo_dim_tiempo = 'CAST(CONCAT(' + campo_dia + ', ' + campo_mes + ', ' + campo_anio + ') AS STRING) AS FECHA_DIM_TIEMPO_CALCULADO_RDH'
    
    prm_query_result = prm_query_result + ', ' + 'CAST(date_format(CURRENT_DATE(),"yyyy-MM-dd") as DATE) AS ' + campo_fecha_funcional + ', ' + campo_anio + ' AS ANIO_CALCULADO_RDH, ' + campo_mes + '  AS MES_CALCULADO_RDH, ' + campo_dia + ' AS DIA_CALCULADO_RDH, ' + campo_dim_tiempo
  
  return prm_query_result

# COMMAND ----------

"""
Descripción: Obtiene cadena SQL concatenando el campo calculado MD5 a partir de la concatenación de la lista de campos no calculados de la tabla.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getCalculatedFieldMD5(prm_query_result, prm_nombre_campo_funcional_lista, prm_campos_str_zdc):
    
  prm_nombre_tbl_app_origen_tmp = prm_nombre_campo_funcional_lista
  prm_nombre_campo_funcional_lista = prm_campos_str_zdc
  query_md5_zdc = prm_nombre_campo_funcional_lista
  df_tmp_current_tbl_blob = sqlContext.sql(prm_query_result + ' FROM ' + prm_nombre_tbl_app_origen_tmp)
  df_tmp_current_tbl_blob.registerTempTable(prm_nombre_tbl_app_origen_tmp)
  
  query_accum_tecnicos_cols = ''.join([str(x) for (i, x) in enumerate(prm_nombre_campo_funcional_lista)]) 

  if query_accum_tecnicos_cols.endswith(","):   
    query_accum_tecnicos_cols = query_accum_tecnicos_cols[:-1]

  query_tbl_blob_md5 = ", MD5(LOWER(CONCAT_WS(" + query_md5_zdc + "))) AS COL_MD5_ZDC, LOWER(CONCAT_WS(" + query_md5_zdc + ")) AS ZDC  FROM " +     prm_nombre_tbl_app_origen_tmp

  prm_query_result = prm_query_result + query_tbl_blob_md5
  df_tmp_current_tbl_blob = sqlContext.sql(prm_query_result)
  df_tmp_current_tbl_blob.registerTempTable(prm_nombre_tbl_app_origen_tmp)
  prm_query_result = ' SELECT COL_MD5_ZDC, ZDC '
  
  return prm_query_result

# COMMAND ----------

"""
Descripción: Obtiene el nombre del campo identificador para crear indexación tipo z-order en tabla ZDR.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getIdentifierField(prm_tbl_inventario, prm_nombre_tbl_app_origen):

  sqlContext.sql('REFRESH TABLE ' + prm_tbl_inventario)
  query_tbl_inventario = 'SELECT CampoIndentificadorBusqueda FROM ' +  prm_tbl_inventario +\
                         ' WHERE NombreTablaAplicacion = ' + "'" + prm_nombre_tbl_app_origen  + "'" 

  df_tbl_inventario = sqlContext.sql(query_tbl_inventario)
  campo_indentificador_busqueda = df_tbl_inventario.first()["CampoIndentificadorBusqueda"].strip()
  
  return campo_indentificador_busqueda

# COMMAND ----------

"""
Descripción: Obtiene el nombre de la o las columnas primary key de la tabla.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def getPrimaryKeyFields(prm_nombre_tbl_app_origen):
  
  sqlContext.sql('REFRESH TABLE ' + tbl_metadatos)
  query_tbl_metadata_pk = 'SELECT NombreCampoTecnico FROM ' +  tbl_metadatos +\
                          ' WHERE LlavePrimaria = "Si" and ' +\
                          'TablaOrigen = ' + "'" + prm_nombre_tbl_app_origen  + "'"

  df_tbl_tbl_metadata_pk = sqlContext.sql(query_tbl_metadata_pk)
    
  list_df_tbl_tbl_metadata_pk = df_tbl_tbl_metadata_pk.collect()
  list_campo_primary_key = [str(i.NombreCampoTecnico)  for i in list_df_tbl_tbl_metadata_pk]
  
  return list_campo_primary_key

# COMMAND ----------

# MAGIC %md
# MAGIC #ZONA DE RESULTADOS

# COMMAND ----------

"""
Descripción: Reiniciar ruta de almacenamiento para la tabla actual (Elimina datos y estructura Delta en caso de que se haya cargado antes la tabla iterada).
Autor: Juan David Escobar E.
Fecha creción: 23/10/2020.
"""

def restart_delta_path_to_tabl_data_lake(prm_ruta_destino_tabla_data_lake):
  
  if os.path.exists('/dbfs' + prm_ruta_destino_tabla_data_lake):
    dbutils.fs.rm(prm_ruta_destino_tabla_data_lake, recurse = True)
    
  else: 
    print('Ruta no existe: ', prm_ruta_destino_tabla_data_lake)

# COMMAND ----------

"""
Descripción: Genera columna calculada indice con un número autogenerado [1 - n_rows] por cada uno de los registros de la tabla iterada.
Autor: Juan David Escobar E.
Fecha creción: 29/10/2020.
"""

def generate_col_index_auto(prm_df_current_tbl_data_lake):
  
  sqlContext.registerDataFrameAsTable(prm_df_current_tbl_data_lake, 'temp_table_index')
  df_current_tbl_data_lake_wt_index = sqlContext.sql('select Row_Number() over ( partition by busqueda \
                                                                                 order by busqueda) as indice, * \
                                                      from temp_table_index \
                                                      order by indice desc')
  
#   window = Window.orderBy(F.monotonically_increasing_id())
#   df_current_tbl_data_lake_wt_index = prm_df_current_tbl_data_lake.withColumn('indice', F.row_number().over(window))
  
  return df_current_tbl_data_lake_wt_index

# COMMAND ----------

"""
Descripción: Generar columna calculada 'paginado' cada 150.000 registros (máximo de registros permitidos export Power BI y páginación)
Autor: Blaimir Ospina Cardona.
Fecha creción: 20/11/2020.
"""

#1. Inner join between df_page and df_data_lake
def ft_inner_join_paginado(prm_df_current_tbl_data_lake):
  df_pys_with_col_pager = sqlContext.sql('select * from auditoria.zdr_panel_indice_aux')
  df1 = prm_df_current_tbl_data_lake.alias('df1')
  df2 = df_pys_with_col_pager.alias('df2')
  inner_join = df1.join(df2, col('df1.indice') == col('df2.indice')).select(col('df1.*'), col('df2.PAGINADO')).sort("indice")
  
  return inner_join

#2. Se crea tabla indice desde un dato parametrizado hasta otro parametro
def ft_crear_table_index(ar_path, ar_name_file,  ar_end_value):
  
  try:
    dbutils.fs.mkdirs(ar_path + folder_aux) #'/mnt/rdh-auditoria/results/indice_aux/'
    if os.path.exists(mount_point + ar_path + folder_aux + ar_name_file) == False: #'/mnt/rdh-auditoria/results/indice_aux/Zdr_indice_aux.csv'
      with open(mount_point + ar_path + folder_aux + ar_name_file, 'w') as f:
        f.write('indice' + '\n')
        [f.write(str(i)  + '\n') for i in range(1, ar_end_value)]
        
#       print("Tabla Indice Creada Correctamente")
      time.sleep(500)
    
    return True
  except Exception as error:
    print("error al crear la tabla con el indice \n" + str(error))

#. Se crea el paginado apartir de un query sql
def generate_col_paginator(prm_df_current_tbl_data_lake, ar_exist_path):
  if ar_exist_path == False:
    min_pager = 1
    incremento = 150000
    end_value = 200000000
    max_pager = incremento
    count_page = int(end_value / max_pager) #200.000.000/150.000 = 1.333,33333
    query = ''
    query_1 = 'select indice, case'
    query_2 = ' end as paginado from  zdr_prueba'

    if ft_crear_table_index(path_index, name_file_index,  end_value): 
      df = spark.read.option("inferSchema", 'true').csv(path_index + folder_aux + name_file_index, header = True) #'/mnt/rdh-auditoria/results/indice_aux/Zdr_indice_aux.csv'

      sqlContext.registerDataFrameAsTable(df ,"zdr_prueba")

      for consecutivo in range(1, count_page + 2): 
        page = 'pagina_' + str(consecutivo)
        query = query + ' when indice >=' + str(min_pager) + ' and indice <=' + str(max_pager) + ' then ' +  "'" + page + "'"
        min_pager = max_pager + 1
        max_pager = max_pager + incremento # 150.000 + 150.000 = 300.000
        query_final = query_1 + query + query_2 
        
#       select indice, 
#              case pagina_1 when indice >= 2 and indice <= 300.000 then pagina_1
#              end as paginado 
#       from  zdr_prueba

      df_final = sqlContext.sql(query_final)
      sqlContext.sql('drop table if exists auditoria.zdr_panel_indice_aux')
      df_final.repartition(5
                           ).write.format('delta'
                           ).mode('overwrite'
                           ).option('path', path_results
                           ).saveAsTable('auditoria.' + 'zdr_panel_indice_aux')

      df_temp = ft_inner_join_paginado(prm_df_current_tbl_data_lake)
    print('Se materializa la tabla correctamente')
  else:
    sqlContext.sql('drop table if exists auditoria.zdr_panel_indice_aux')
    sqlContext.sql('create table auditoria.zdr_panel_indice_aux using Delta location "' + path_results + '"')
    df_temp = ft_inner_join_paginado(prm_df_current_tbl_data_lake)
    
  return df_temp

# COMMAND ----------

"""
Descripción: We can check if a string is NaN by using the property of NaN object that a NaN != NaN.
Autor: Juan David Escobar E.
Fecha creción: 18/01/2021.
"""
def isNaN(string):
    print(string)
    return 'nan' == string

# COMMAND ----------

"""
Descripción: Materializa tabla ZDC en la tabla de la ZDR, formato delta, particiona por año y mes, agrega uno o varios indices por el campo identificador.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def materializeTablZdr(prm_query_result, prm_ruta_destino_tabla_data_lake, prm_nombre_app_origen, prm_nombre_tbl_sub_app_origen, prm_nombre_tbl_app_origen, prm_campo_indentificador_busqueda, prm_nombre_tbl_zdr):
    
  #1. Materializar tabla en HIVE .
  df_current_tbl_data_lake = sqlContext.sql(prm_query_result)
    
  if df_current_tbl_data_lake.count() > 0:
    #1.1 Generar columna calculada indice autogenerado 
    df_current_tbl_data_lake = generate_col_index_auto(df_current_tbl_data_lake)

    #1.2 Generar columna calculada paginador, paginado cada 150.000 registros (máximo de registros permitidos export Power BI y páginación)  
    df_current_tbl_data_lake_wt_pager = generate_col_paginator(df_current_tbl_data_lake, os.path.exists(mount_point + path_results))
  else:
    df_current_tbl_data_lake_wt_pager = df_current_tbl_data_lake
    
          
  #2. Habilitar almacenado formato delta
  sqlContext.sql("SET spark.databricks.delta.formatCheck.enabled=false")
  
  #3. Borrar tabla actual si existe
  sqlContext.sql('DROP TABLE IF EXISTS ' + prm_nombre_tbl_zdr)
  
  #4. Escribir tabla actual
  df_current_tbl_data_lake_wt_pager.repartition("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"
                                               ).write.format("delta"
                                               ).mode("overwrite"
                                               ).partitionBy("ANIO_CALCULADO_RDH", "MES_CALCULADO_RDH"
                                               ).option('path', prm_ruta_destino_tabla_data_lake
                                               ).saveAsTable(prm_nombre_tbl_zdr)
  
  #5. Refresh table actual
  sqlContext.sql('REFRESH TABLE ' + prm_nombre_tbl_zdr)  
  
  #6. generar indice de busqueda
  #prm_campo_indentificador_busqueda = ''.join(prm_campo_indentificador_busqueda.split(','))
  
  if not isNaN(prm_campo_indentificador_busqueda):  
    query_index = 'OPTIMIZE ' + prm_nombre_tbl_zdr + ' ZORDER BY (' + prm_campo_indentificador_busqueda + ')' 

    sqlContext.sql(query_index)

  #7. Refresh table actual
  sqlContext.sql('REFRESH TABLE ' + prm_nombre_tbl_zdr)
  
  return df_current_tbl_data_lake_wt_pager

# COMMAND ----------

# MAGIC %md
# MAGIC #FUNCIONES AUDITORIA

# COMMAND ----------

# MAGIC %run /extaccion_dinamica_db/auditoria/03_LogAuditoriaRegistrosBD

# COMMAND ----------

def procesar_dinamyc_tables():
                    
  for index, row in df_pd_list_loaded_tbls_in_blob.iterrows():

    fmt = '%Y-%m-%d %H:%M:%S' 
    date = datetime.now()
    date = date.strftime(fmt)
    d1 = datetime.strptime(date, fmt)

    #.......................................................................................... VARIABLES  ..........................................................................................................
    flag = 'Si' 
    flag_negative = 'No'
    query_result = 'SELECT '
    ruta_destino_tabla_blob = row['rutaDestinoTablaBlob'].strip()
    ruta_destino_tabla_blob = '/mnt/' + ruta_destino_tabla_blob 
    ruta_destino_tabla_data_lake = row['rutaDestinoTablaDataLake'].strip()
    ruta_destino_tabla_data_lake = '/mnt/' + ruta_destino_tabla_data_lake.strip() 
    nombre_tbl_app_origen = row['nombre_tbl_app_origen'].upper().strip()
    nombre_tbl_app_origen_tmp = "TEMP_CURRENT_TABLE_" + nombre_tbl_app_origen
    nombre_app_origen = row['nombre_app_origen'].upper().strip()
    nombre_tbl_sub_app_origen = row['nombre_tbl_sub_app_origen'].upper().strip()

    print('Tabla: ', nombre_tbl_app_origen)

    #.......................................................................................... ZONA DE CRUDOS .....................................................................................................

    # 1. Obtener metadatos de la tabla actual leída del Azure Blob Storage y crear campos de busqueda, filtro por campos migrables y visibles.  
    df_current_metadata, lista_campos_visualizar, lista_campos_busqueda = getObjectsCurrentMetadata(nombre_tbl_app_origen, flag) 

    # 1.1 Crear esquema tabla temporal de tabla actualmente iterada, lectura datos crudos en formato .parquet, origen Blob Storage
    # 1.2 Registrar tabla temporal para creación de query
    df_current_tbl_blob = spark.read.format('parquet').load(ruta_destino_tabla_blob)
    df_current_tbl_blob = df_current_tbl_blob.limit(100000)
    df_current_tbl_blob.registerTempTable(nombre_tbl_app_origen_tmp)


    #....................................................................................... ZONA DE PROCESADOS .....................................................................................................

    # 2 Crear lista de campos con tipos de datos de HIVE SQL
    prm_lista_campos_visualizar = lista_campos_visualizar
    prm_lista_campos_busqueda = lista_campos_busqueda
    campos_str_zdr_md5, campos_str_zdr, campos_str_zdc , nombre_campo_funcional_lista, campos_consulta_str = getObjectsListSearchFiles(lista_campos_visualizar, lista_campos_busqueda)
    

    # 2.0.1 Crear campo calculado MD5 ZDC
    query_result = query_result + campos_str_zdc 
    query_result = getCalculatedFieldMD5(query_result, nombre_tbl_app_origen_tmp, campos_str_zdc)

    # 2.1 Crear campo calculado busqueda
    # 2.2 concatenenar query dinámico campos tabla ZDC + Campo calculado busqueda
    if campos_consulta_str.endswith(", ' - '"):
      campos_consulta_str = campos_consulta_str[:-7]

    query_result = query_result + ' , ' + campos_str_zdr + ', translate(lower(concat(' + campos_consulta_str + ')),"áéíóúÁÉÍÓÚñÑü","aeiouAEIOUnNu") AS busqueda ' 


    # 2.3 Crear campos calculados: fecha, busqueda, AÑO, MES, DIA, FECHA_DIM_TIEMPO Y MD5. 
    query_result = getCalculatedFields(df_current_metadata, query_result)
    query_result = query_result + ' FROM ' + nombre_tbl_app_origen_tmp 

    # 2.5 Obtener de tabla inventario tablas el campo identificador para Crear indice Z-ORDER.
    campo_indentificador_busqueda = getIdentifierField(tbl_inventario, nombre_tbl_app_origen)

    # 2.6 Obtener de tabla metadatos campo o campos primary_key (pk1 || pk1, pk2, .. pkn) para validar duplicados.
    campo_primary_key = getPrimaryKeyFields(nombre_tbl_app_origen)

    #........................................................................................ ZONA DE RESULTADOS ....................................................................................................

    # 3. Reiniciar ruta de almacenamiento para la tabla actual (Elimina datos y estructura Delta en caso de que se halla cargado antes la tabla iterada).   
    restart_delta_path_to_tabl_data_lake(ruta_destino_tabla_data_lake)    

    # 3.1 Materializar tabla en HIVE .    
    print('campo_indentificador_busqueda: ', campo_indentificador_busqueda, ' tipo: ', type(campo_indentificador_busqueda), 'isNan: ', isNaN(campo_indentificador_busqueda))
    nombre_tbl_zdr = nombre_app_origen + '.zdr_' + nombre_app_origen + '_' + nombre_tbl_sub_app_origen + '_'  + nombre_tbl_app_origen
    df_current_tbl_data_lake = materializeTablZdr(query_result, ruta_destino_tabla_data_lake, nombre_app_origen, nombre_tbl_sub_app_origen,nombre_tbl_app_origen, campo_indentificador_busqueda, nombre_tbl_zdr)

    #........................................................................................... AUDITORIA ...........................................................................................................

    # 4. Crete count of cols calculate MD5 tabla ZDR after insert
    df_current_tbl_data_lake = get_row_md5_zdr(campos_str_zdr_md5, nombre_app_origen, nombre_tbl_app_origen, nombre_tbl_app_origen_tmp, nombre_tbl_zdr) # --Ver funcion y prms

    # 4.1 Comparar cantidad de registros tabla ZDC vs tabla ZDR
    cantidad_registros_ZDC = df_current_tbl_blob.count()
    cantidad_registros_ZDR = df_current_tbl_data_lake.count()
    cantidad_no_cargados = cantidad_registros_ZDC  -  cantidad_registros_ZDR
    dic_aud_registros = compararCantidadFilasZdcZdr(cantidad_registros_ZDC, cantidad_registros_ZDR, nombre_tbl_app_origen)
    error_reg_zdc_zdr = dic_aud_registros['msgError']

    # 4.2 Comparar MD5 tabla ZDC vs tabla ZDR
    dic_aud_md5 = compararMd5ZdcZdr(df_current_tbl_data_lake, nombre_tbl_zdr)
    cantidad_md5_invalid = dic_aud_md5['cantidad_md5_invalid']
    error_invalid_md5 = dic_aud_md5['msgError']
    print(error_invalid_md5)

     # 4.3 Validar existencia de registros duplicados  
    dic_aud_duplicates = validateDuplicates(df_current_tbl_data_lake, nombre_tbl_zdr, campo_primary_key)
    cantidad_duplicados = dic_aud_duplicates['cantidad_duplicados']
    error_reg_duplicados = dic_aud_duplicates['msgError']
    print(error_reg_duplicados)

    # 4.5 Validar existencia de registros corruptos
    dic_aud_corrupts = fn_corrupt_records(df_current_tbl_data_lake, campo_primary_key)
    cantidad_corrupts = dic_aud_corrupts['cantidad_corrupts']
    error_reg_corruptos = dic_aud_corrupts['msgError']     

    #......................................................................................... END TIME ..........................................................................................................

    date = datetime.now()
    date = date.strftime(fmt)
    d2 = datetime.strptime(date, fmt)
    days_diff = (d2 - d1).days
    seconds_diff = (d2 - d1).seconds
    minutes_diff =  (days_diff * 24 * 60) + (seconds_diff/60)
    minutes_diff = str(round(minutes_diff, 2))

    #....................................................................................REGISTRO LOG AUDITORIA ..................................................................................................

    # 5. Registrar log de auditoria
    fn_procesar_log_aud(nombre_app_origen, 
                        nombre_tbl_sub_app_origen, 
                        nombre_tbl_app_origen, 
                        nombre_tbl_zdr,
                        ruta_destino_tabla_data_lake, 
                        cantidad_registros_ZDC, 
                        cantidad_registros_ZDR, 
                        cantidad_md5_invalid, 
                        cantidad_duplicados, 
                        cantidad_corrupts, 
                        cantidad_no_cargados, 
                        error_reg_zdc_zdr, 
                        error_reg_duplicados, 
                        error_invalid_md5, 
                        error_reg_corruptos,
                        minutes_diff)

    print(nombre_tbl_zdr + ' ' + 'ok', datetime.now(), minutes_diff)

# COMMAND ----------

"""
Documentacion esquma: https://chih-ling-hsu.github.io/2017/03/28/how-to-change-schema-of-a-spark-sql-dataframe


def procesar_dinamyc_tables():
                    
  for index, row in df_pd_list_loaded_tbls_in_blob.iterrows():
        
    #......................................................................................... START TIME ..........................................................................................................
      fmt = '%Y-%m-%d %H:%M:%S' 
      date = datetime.now()
      date = date.strftime(fmt)
      d1 = datetime.strptime(date, fmt)

      #.......................................................................................... VARIABLES ..........................................................................................................
      flag = 'Si' 
      flag_negative = 'No'
      query_result = 'SELECT '
      ruta_destino_tabla_blob = row['rutaDestinoTablaBlob'].strip()
      ruta_destino_tabla_blob = '/mnt/' + ruta_destino_tabla_blob 
      ruta_destino_tabla_data_lake = row['rutaDestinoTablaDataLake'].strip()
      ruta_destino_tabla_data_lake = '/mnt/' + ruta_destino_tabla_data_lake.strip() 
      nombre_tbl_app_origen = row['nombre_tbl_app_origen'].upper().strip()
      nombre_tbl_app_origen_tmp = "TEMP_CURRENT_TABLE_" + nombre_tbl_app_origen
      nombre_app_origen = row['nombre_app_origen'].upper().strip()
      nombre_tbl_sub_app_origen = row['nombre_tbl_sub_app_origen'].upper().strip()
      
      print('Tabla: ', nombre_tbl_app_origen)
      
      #.......................................................................................... ZONA DE CRUDOS .....................................................................................................

      # 1. Obtener metadatos de la tabla actual leída del Azure Blob Storage y crear campos de busqueda, filtro por campos migrables y visibles.  
      df_current_metadata, lista_campos_visualizar, lista_campos_busqueda  = getObjectsCurrentMetadata(nombre_tbl_app_origen, flag)

      # 1.1 Crear esquema tabla temporal de tabla actualmente iterada, lectura datos crudos en formato .parquet, origen Blob Storage
      # 1.2 Registrar tabla temporal para creación de query
      df_current_tbl_blob = spark.read.format('parquet').load(ruta_destino_tabla_blob)
      df_current_tbl_blob.registerTempTable(nombre_tbl_app_origen_tmp)

      #....................................................................................... ZONA DE PROCESADOS .....................................................................................................

      # 2 Crear lista de campos con tipos de datos de HIVE SQL
      campos_str, nombre_campo_funcional_lista, campos_consulta_str = getObjectsListSearchFiles(lista_campos_visualizar, lista_campos_busqueda)


      # 2.1 Crear campo calculado busqueda
      # 2.2 concatenenar query dinámico campos tabla ZDC + Campo calculado busqueda
      if campos_consulta_str.endswith(", ' - '"):
        campos_consulta_str = campos_consulta_str[:-7]

      query_result = query_result + campos_str + ", translate(lower(concat(" + campos_consulta_str + ")),'áéíóúÁÉÍÓÚñÑü','aeiouAEIOUnNu') AS busqueda"

      # 2.3 Crear campos calculados: fecha, busqueda, AÑO, MES, DIA, FECHA_DIM_TIEMPO Y MD5. 
      query_result = getCalculatedFields(df_current_metadata, query_result)
      query_result = getCalculatedFieldMD5(query_result, nombre_tbl_app_origen_tmp, nombre_campo_funcional_lista)

      # 2.5 Obtener de tabla inventario tablas el campo identificador para Crear indice Z-ORDER.
      campo_indentificador_busqueda = getIdentifierField(tbl_inventario, nombre_tbl_app_origen)

      # 2.6 Obtener de tabla metadatos campo o campos primary_key (pk1 || pk1, pk2, .. pkn) para validar duplicados.
      campo_primary_key = getPrimaryKeyFields(nombre_tbl_app_origen)
      
      #........................................................................................ ZONA DE RESULTADOS ....................................................................................................

      # 3. Reiniciar ruta de almacenamiento para la tabla actual (Elimina datos y estructura Delta en caso de que se halla cargado antes la tabla iterada).   
      restart_delta_path_to_tabl_data_lake(ruta_destino_tabla_data_lake)    

      # 3.1 Materializar tabla en HIVE .    
      print('campo_indentificador_busqueda: ', campo_indentificador_busqueda, ' tipo: ', type(campo_indentificador_busqueda), 'isNan: ', isNaN(campo_indentificador_busqueda))
      nombre_tbl_zdr = nombre_app_origen + '.zdr_' + nombre_app_origen + '_' + nombre_tbl_sub_app_origen + '_'  + nombre_tbl_app_origen
      df_current_tbl_data_lake = materializeTablZdr(query_result, ruta_destino_tabla_data_lake, nombre_app_origen, nombre_tbl_sub_app_origen,nombre_tbl_app_origen, campo_indentificador_busqueda, nombre_tbl_zdr)

      #........................................................................................... AUDITORIA ...........................................................................................................

      # 4. Crete count of cols calculate MD5 tabla ZDR after insert
      df_current_tbl_data_lake = get_row_md5_zdr(nombre_campo_funcional_lista, nombre_app_origen, nombre_tbl_app_origen, nombre_tbl_app_origen_tmp, nombre_tbl_zdr)

      # 4.1 Comparar cantidad de registros tabla ZDC vs tabla ZDR
      cantidad_registros_ZDC = df_current_tbl_blob.count()
      cantidad_registros_ZDR = df_current_tbl_data_lake.count()
      cantidad_no_cargados = cantidad_registros_ZDC  -  cantidad_registros_ZDR
      dic_aud_registros = compararCantidadFilasZdcZdr(cantidad_registros_ZDC, cantidad_registros_ZDR, nombre_tbl_app_origen)
      error_reg_zdc_zdr = dic_aud_registros['msgError']

      # 4.2 Comparar MD5 tabla ZDC vs tabla ZDR
      dic_aud_md5 = compararMd5ZdcZdr(df_current_tbl_data_lake, nombre_tbl_zdr)
      cantidad_md5_invalid = dic_aud_md5['cantidad_md5_invalid']
      error_invalid_md5 = dic_aud_md5['msgError']
      print(error_invalid_md5)
      
       # 4.3 Validar existencia de registros duplicados  
      dic_aud_duplicates = validateDuplicates(df_current_tbl_data_lake, nombre_tbl_zdr, campo_primary_key)
      cantidad_duplicados = dic_aud_duplicates['cantidad_duplicados']
      error_reg_duplicados = dic_aud_duplicates['msgError']
      print(error_reg_duplicados)

      # 4.5 Validar existencia de registros corruptos
      dic_aud_corrupts = fn_corrupt_records(df_current_tbl_data_lake, campo_primary_key)
      cantidad_corrupts = dic_aud_corrupts['cantidad_corrupts']
      error_reg_corruptos = dic_aud_corrupts['msgError']     
     
      #......................................................................................... END TIME ..........................................................................................................

      date = datetime.now()
      date = date.strftime(fmt)
      d2 = datetime.strptime(date, fmt)
      days_diff = (d2 - d1).days
      seconds_diff = (d2 - d1).seconds
      minutes_diff =  (days_diff * 24 * 60) + (seconds_diff/60)
      minutes_diff = str(round(minutes_diff, 2))

      #....................................................................................REGISTRO LOG AUDITORIA ..................................................................................................

      # 5. Registrar log de auditoria
      fn_procesar_log_aud(nombre_app_origen, 
                          nombre_tbl_sub_app_origen, 
                          nombre_tbl_app_origen, 
                          nombre_tbl_zdr,
                          ruta_destino_tabla_data_lake, 
                          cantidad_registros_ZDC, 
                          cantidad_registros_ZDR, 
                          cantidad_md5_invalid, 
                          cantidad_duplicados, 
                          cantidad_corrupts, 
                          cantidad_no_cargados, 
                          error_reg_zdc_zdr, 
                          error_reg_duplicados, 
                          error_invalid_md5, 
                          error_reg_corruptos,
                          minutes_diff)

      print(index, nombre_tbl_zdr + ' ' + 'ok', datetime.now(), minutes_diff)
    #......................................................................................................................................................................"""

# COMMAND ----------

if df_pys_list_loaded_tbls_in_blob.count() > 0:
  procesar_dinamyc_tables()

# COMMAND ----------



# COMMAND ----------

# df_Duplicates = sqlContext.sql('SELECT * FROM semih.zdr_semih_sistema_t_mih_catalogo')

# columns  = ['CODMAESTRO','CODIGO']
# campo_primary_key = str(columns )

# print(campo_primary_key, type(campo_primary_key))

# df_Duplicates2 = df_Duplicates.groupby(eval(campo_primary_key)).count()

# columns.append('count')
# print(columns)

# df_Duplicates3 = df_Duplicates2.select(columns).filter('count > 1')
# display(df_Duplicates2)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC REFRESH TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake;
# MAGIC SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake

# COMMAND ----------

dbutils.notebook.run('/extaccion_dinamica_db/06_CreateFilterSearch', 259200)

# COMMAND ----------

# sqlContext.sql('use semih')
# def tablas_campo_fecha():
#   return sqlContext.sql('select NombreTablaAplicacion, CampoFechaBusqueda from semih.zdr_semih_src_metadatos_inventario_tablas_bd \
#                            where upper(RequiereReprocesarCarga) = "SI"').collect()

# tablas = [[x['tableName'], y['CampoFechaBusqueda'], y['NombreTablaAplicacion']] for x in sqlContext.sql('show tables').collect() 
#                                                     for y in tablas_campo_fecha()  
#                                                     if y['NombreTablaAplicacion'].lower() in x['tableName']]
# for i, x in enumerate(tablas):
#   try:
#     print(i, ' semih.{}'.format( x[0]))
#     df = sqlContext.sql('select count(1) as CANTIDAD, busqueda as BUSQUEDA, {} \
#                         from semih.{} group by BUSQUEDA, {} \
#                         order by CANTIDAD desc \
#                         limit 10'.format(x[1], x[0], x[1])).toPandas()
#     df.to_csv('/dbfs/mnt/app-semih-storage/casodeusos/{}.csv'.format(x[0][10:]),  index=False, sep='|')
#   except Exception as error:
#       print("no se proceso la tabla {}".format(x[0][10:]) + '\n' + str(error))

# COMMAND ----------

# sqlContext.sql('use semih')
# def tablas_campo_fecha():
#   return sqlContext.sql('select NombreTablaAplicacion, CampoFechaBusqueda from semih.zdr_semih_src_metadatos_inventario_tablas_bd \
#                            where upper(RequiereReprocesarCarga) = "SI" ').collect()
         
# tablas = ['zdr_semih_sicco_sicco']          
          
          
# for x in tablas:
    
#   try:
#     #print(i, ' semih.{}'.format( x[0]))
#     print('select * from semih.' + x + ' limit 10')
#     df = sqlContext.sql('select * from semih.' + x + ' limit 10').toPandas()
#     df.to_csv('/dbfs/mnt/app-semih-storage/casodeusos/{}.csv'.format(x),  index=False, sep='|')
#   except Exception as error:
#       print("no se proceso la tabla {}".format(x[0][10:]) + '\n' + str(error))
