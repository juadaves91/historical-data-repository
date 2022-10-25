# Databricks notebook source
# MAGIC %md
# MAGIC # FUNCIONES AUXILIARES AUDITORIA DE REGISTROS INSERTADOS - BD

# COMMAND ----------

import os
import pandas as pd
from datetime import datetime
import pytz
from pyspark import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DataType, TimestampType

# COMMAND ----------

# DBTITLE 1,Obtener fecha actual
def fn_get_current_date():
  
  zone = pytz.timezone('America/Bogota')
  date = datetime.now(zone)
  current_date = date.strftime("%d/%m/%Y %H:%M:%S")
  current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S") 
  
  return current_date

# COMMAND ----------

# MAGIC %md
# MAGIC # ACTUALIZACION REGISTRO AUD EN TABLA (ZDR_AUDITORIA_LOG_EXTACCION_BD_REGISTROS_ZDR)

# COMMAND ----------

def create_schema_source_tbl_aud(prm_nombre_app_origen,
                                 prm_nombre_tbl_sub_app_origen,
                                 prm_nombre_tbl_app_origen,
                                 prm_nombre_tbl_zdr,
                                 prm_ruta_destino_tabla_data_lake,
                                 prm_cantidad_registros_ZDC, 
                                 prm_cantidad_registros_ZDR,
                                 prm_cantidad_md5_invalid, 
                                 prm_cantidad_duplicados,
                                 prm_cantidad_corrupts, 
                                 prm_cantidad_no_cargados, 
                                 prm_error_reg_zdc_zdr,
                                 prm_error_reg_duplicados, 
                                 prm_error_invalid_md5,
                                 prm_error_reg_corruptos, 
                                 prm_current_date, 
                                 prm_duracion, 
                                 prm_tbl_aud):
    
  rowKey =  prm_nombre_app_origen.strip().upper() + '-' + prm_nombre_tbl_sub_app_origen.strip().upper() + '-' + prm_nombre_tbl_app_origen.strip().upper()
  partitionKey = prm_nombre_tbl_sub_app_origen.strip() + '-' + prm_nombre_tbl_app_origen.strip()
  
  data_source = [Row(rowKey,
                     partitionKey, 
                     prm_nombre_app_origen, 
                     prm_nombre_tbl_sub_app_origen, 
                     prm_nombre_tbl_app_origen, 
                     prm_nombre_tbl_zdr,
                     prm_ruta_destino_tabla_data_lake,
                     prm_cantidad_registros_ZDC,
                     prm_cantidad_registros_ZDR,
                     prm_cantidad_md5_invalid,
                     prm_cantidad_duplicados,
                     prm_cantidad_corrupts,
                     prm_cantidad_no_cargados,
                     prm_error_reg_zdc_zdr,
                     prm_error_reg_duplicados,
                     prm_error_invalid_md5,
                     prm_error_reg_corruptos,
                     prm_duracion,
                     prm_current_date)]
  
  schema_source = StructType([StructField("src_row_key", StringType(), True),  
                              StructField("src_partition_key", StringType(), True),
                              StructField("src_nombre_app_origen", StringType(), True),
                              StructField("src_nombre_tbl_sub_app_origen", StringType(), True),
                              StructField("src_nombre_tbl_app_origen", StringType(), True),
                              StructField("src_nombre_tbl_zdr", StringType(), True),
                              StructField("src_ruta_destino_tabla_data_lake", StringType(), True),                          
                              StructField("src_cantidad_registros_ZDC", StringType(), True),
                              StructField("src_cantidad_registros_ZDR", StringType(), True),
                              StructField("src_cantidad_md5_invalid", StringType(), True),
                              StructField("src_cantidad_duplicados", StringType(), True),
                              StructField("src_cantidad_corrupts", StringType(), True),
                              StructField("src_cantidad_no_cargados", StringType(), True),
                              StructField("src_error_reg_zdc_zdr", StringType(), True),
                              StructField("src_error_reg_duplicados", StringType(), True),
                              StructField("src_error_invalid_md5", StringType(), True),
                              StructField("src_error_reg_corruptos", StringType(), True),
                              StructField("src_duracion", StringType(), True),
                              StructField("src_fecha_registro", TimestampType(), True)
  ])

  df_source = sqlContext.createDataFrame(sc.parallelize(data_source), schema_source)
  df_source.registerTempTable("Temp_Tbl_Src_" + prm_tbl_aud)
  return df_source

# COMMAND ----------

# DBTITLE 1,Creación tabla auditoria
"""
Descripción: Log Carga Tablas DL: Se utiliza una tabla de log de auditoria para llevar el control y la trazabilidad de las tablas creadas en la zona de resultados, adicional
             almacena la información asociada a los datos que se insertaron, a continuación se describe cada uno de los campos de tabla:
             
             -row_key: Identificador unico de registro.
             -partition_key: Identificador unico de registro resumido.
             -nombre_app_origen: nombre de la BD / APP migrada.
             -nombre_tbl_sub_app_origen: Nombre de de la suaplicación en caso de que exista.
             -nombre_tbl_app_origen: Nombre de la tabla en BD Origen.
             -nombre_tbl_zdr: Nombre de la tabla en ZDR.
             -ruta_destino_tabla_data_lake: Ruta en la que se almacenan los datos de la tabla en el Datalake.
             -cantidad_registros_ZDC: Cantidad de registros leidos en un DF a partir de un archivo en formato .parquet en Blob storage.
             -cantidad_registros_ZDR: Cantidad de registros leidos en un DF a partir de la tabal creada en ZDR.
             -cantidad_md5_invalid: MD5 creado a partir de la concatencacion de campos de la tabla de ZDC vs ZDR, valida la integridad de los datos.
             -cantidad_duplicados: Cantidad de registros duplicados hallados en la tabla ZDR.
             -cantidad_corrupts: Cantidad de registros corruptos encontrados en la tabla ZDR.
             -cantidad_no_cargados: Resta de registros origen - registros cargados en tabla ZDR.
             -error_reg_zdc_zdr: Mensaje de error y detalle del campo cantidad_registros_ZDR.
             -error_reg_duplicados: Mensaje de error y detalle del campo cantidad_duplicados.
             -error_invalid_md5: Mensaje de error y detalle del campo cantidad_md5_invalid.
             -error_reg_corruptos: Mensaje de error y detalle del campo cantidad_corrupts.
             -duracion: Tiempo en minutos de procesamiento para crear la tabla en script 04_CreateDynamicDataModel.
             -fecha_registro: Fecha en que se registro el log.
             
Responsables: Juan David Escobar E.
Fecha Creación: 08/10/2020.
rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
"""

def create_table_aud_bd():

  sql_db_aud =  "CREATE DATABASE IF NOT EXISTS auditoria"
  sql_drop =  "DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake"
  sql_tbl_aud = "CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake \
                 ( \
                    row_key string, \
                    partition_key string, \
                    nombre_app_origen string, \
                    nombre_tbl_sub_app_origen string, \
                    nombre_tbl_app_origen string, \
                    nombre_tbl_zdr string, \
                    ruta_destino_tabla_data_lake string, \
                    cantidad_registros_ZDC string, \
                    cantidad_registros_ZDR string, \
                    cantidad_md5_invalid string, \
                    cantidad_duplicados string, \
                    cantidad_corrupts string, \
                    cantidad_no_cargados string, \
                    error_reg_zdc_zdr string, \
                    error_reg_duplicados string, \
                    error_invalid_md5 string, \
                    error_reg_corruptos string, \
                    duracion string, \
                    fecha_registro timestamp \
                 ) \
                 USING DELTA \
                 PARTITIONED BY (nombre_app_origen) \
                 LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake'"
  
  sqlContext.sql(sql_db_aud)
  sqlContext.sql(sql_drop)
  sqlContext.sql(sql_tbl_aud)

# COMMAND ----------

"""
Descripción: Actualiza o inserta la tabla de log precarga "auditoria.Zdr_Auditoria_Log_Extraction_BD_BD_Tbl_In_Data_Lake" mediante instruccion merge
Responsables: Juan David Escobar E.
Fecha Creación: 08/10/2020.
Fecha Modificación: 08/10/2020..
rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
Documentacion: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-update
"""

def execute_query_merge():

  query_merge_aud = "MERGE INTO auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake \
  USING Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake \
    ON auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.row_key = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_row_key \
  WHEN MATCHED THEN \
    UPDATE SET auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.nombre_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_nombre_app_origen, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.nombre_tbl_sub_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_nombre_tbl_sub_app_origen, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.nombre_tbl_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_nombre_tbl_sub_app_origen, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.nombre_tbl_zdr = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_nombre_tbl_zdr, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.ruta_destino_tabla_data_lake = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_ruta_destino_tabla_data_lake, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_registros_ZDC = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_registros_ZDC, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_registros_ZDR = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_registros_ZDR, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_md5_invalid = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_md5_invalid, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_duplicados = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_duplicados, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_corrupts = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_corrupts, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.cantidad_no_cargados = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_cantidad_no_cargados, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.error_reg_zdc_zdr = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_error_reg_zdc_zdr, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.error_reg_duplicados = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_error_reg_duplicados, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.error_invalid_md5 = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_error_invalid_md5, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.error_reg_corruptos = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_error_reg_corruptos, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.duracion = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_duracion, \
               auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.fecha_registro = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake.src_fecha_registro \
  WHEN NOT MATCHED \
    THEN INSERT (row_key, \
                 partition_key, \
                 nombre_app_origen, \
                 nombre_tbl_sub_app_origen, \
                 nombre_tbl_app_origen, \
                 nombre_tbl_zdr, \
                 ruta_destino_tabla_data_lake, \
                 cantidad_registros_ZDC, \
                 cantidad_registros_ZDR, \
                 cantidad_md5_invalid, \
                 cantidad_duplicados, \
                 cantidad_corrupts, \
                 cantidad_no_cargados, \
                 error_reg_zdc_zdr, \
                 error_reg_duplicados, \
                 error_invalid_md5, \
                 error_reg_corruptos, \
                 duracion, \
                 fecha_registro) VALUES (src_row_key, \
                                         src_partition_key, \
                                         src_nombre_app_origen, \
                                         src_nombre_tbl_sub_app_origen, \
                                         src_nombre_tbl_app_origen, \
                                         src_nombre_tbl_zdr, \
                                         src_ruta_destino_tabla_data_lake, \
                                         src_cantidad_registros_ZDC, \
                                         src_cantidad_registros_ZDR, \
                                         src_cantidad_md5_invalid, \
                                         src_cantidad_duplicados, \
                                         src_cantidad_corrupts, \
                                         src_cantidad_no_cargados, \
                                         src_error_reg_zdc_zdr, \
                                         src_error_reg_duplicados, \
                                         src_error_invalid_md5, \
                                         src_error_reg_corruptos, \
                                         src_duracion, \
                                         src_fecha_registro)"
  sqlContext.sql(query_merge_aud)

# COMMAND ----------

# DBTITLE 1,Refrescar tabla
def refesh_table_aud():
  sql_tbl_aud = "refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake;\
                 SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake"

# COMMAND ----------

# DBTITLE 1,Registro Log de Auditoria
"""
Descripción: valida los registros corruptos de un df para la tabla ZDR.
Fecha Modificación: 08/10/2020.
Autor: Juan David Escobar E.
"""

def fn_procesar_log_aud(prm_nombre_app_origen, 
                        prm_nombre_tbl_sub_app_origen,
                        prm_nombre_tbl_app_origen,
                        prm_nombre_tbl_zdr,
                        prm_ruta_destino_tabla_data_lake,
                        prm_cantidad_registros_ZDC, 
                        prm_cantidad_registros_ZDR, 
                        prm_cantidad_md5_invalid,
                        prm_cantidad_duplicados, 
                        prm_cantidad_corrupts,
                        prm_cantidad_no_cargados, 
                        prm_error_reg_zdc_zdr, 
                        prm_error_reg_duplicados,
                        prm_error_invalid_md5, 
                        prm_error_reg_corruptos, 
                        prm_duracion):
  try:
    
    tbl_aud = "Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake"
    current_date = fn_get_current_date()
    
    df_source = create_schema_source_tbl_aud(prm_nombre_app_origen,
                                             prm_nombre_tbl_sub_app_origen,
                                             prm_nombre_tbl_app_origen,
                                             prm_nombre_tbl_zdr,
                                             prm_ruta_destino_tabla_data_lake,
                                             prm_cantidad_registros_ZDC, 
                                             prm_cantidad_registros_ZDR,
                                             prm_cantidad_md5_invalid, 
                                             prm_cantidad_duplicados,
                                             prm_cantidad_corrupts, 
                                             prm_cantidad_no_cargados, 
                                             prm_error_reg_zdc_zdr,
                                             prm_error_reg_duplicados, 
                                             prm_error_invalid_md5,
                                             prm_error_reg_corruptos, 
                                             current_date, 
                                             prm_duracion, 
                                             tbl_aud)
    
    create_table_aud_bd()
    refesh_table_aud()
    df = execute_query_merge()
    refesh_table_aud()
    df = sqlContext.sql('SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Data_Lake')
        
    return "Ok"
  except Exception as error:
    print(error, "\nerror al registrar la auditoria")

# COMMAND ----------

# MAGIC %md
# MAGIC # FUNCIONES AUXILIARES

# COMMAND ----------

# DBTITLE 1,AUDITORIA FN AUX GET MD5 - ZDC
"""
Descripción: Retorna codigo MD5 de la concatenacion de columnas.
Autor: Juan David Escobar E.
Fecha creción: 02/10/2020.
Fecha modificación: 02/10/2020.
"""

def get_row_md5_zdc(cols):
    md5 = hashlib.md5(cols).hexdigest()
    return md5

# COMMAND ----------

# DBTITLE 1,AUDITORIA (VALIDAR CANTIDAD REGISTROS ZDC vs ZDR)
"""
Descripción: Retorna un valor Booleano indicando la igualdad de cantidad de registros
             en la tabla de la ZDC y la tabla de la ZDR.
Autor: Juan David Escobar E.
Fecha creción: 05/10/2020.
"""
    
def compararCantidadFilasZdcZdr(prm_cantidadRegistrosZDC, prm_cantidadRegistrosZDR, prm_nombreTabla):
    
  if prm_cantidadRegistrosZDC == prm_cantidadRegistrosZDR:
    return {'isValid' : True, 'msgError' : ''}
  else:
    return {'isValid' : False, 'msgError' : 'Error cantidad de registros no coincide, la tabla: {0} tiene una cantidad de registos = {1} diferente a la cantidad de registros en su origen ZDC con {2} de registros'.format(prm_nombreTabla, prm_cantidadRegistrosZDC, prm_cantidadRegistrosZDR)}

# COMMAND ----------

# DBTITLE 1,AUDITORIA (VALIDAR INTEGRIDAD DE DATOS - MD5)
"""
Descripción: Retorna un valor Booleano indicando la igualdad de cantidad de registros MD5
             en la tabla de la ZDC y la tabla de la ZDR.
Autor: Juan David Escobar E.
Fecha creción: 05/10/2020.
"""

def compararMd5ZdcZdr(prm_df_zdr, nombreTabla):
  
  count_invalid_rows = prm_df_zdr.where("MD5_VALID == 'FALSE'").count()
  
  if count_invalid_rows > 0:
    return {'isValid' : False, 'msgError' : 'Error de integridad MD5, la tabla en ZDC: {0} no concuerdan en integridad de datos al insertar en la tabla de ZDR = {1}, existen {2} registros que no coinciden en su valor unico MD5'.format(nombreTabla + '_ZDC', nombreTabla + '_ZDR', str(count_invalid_rows)), 'cantidad_md5_invalid':str(count_invalid_rows)}
  else:
    return {'isValid' : True, 'msgError' : '',  'cantidad_md5_invalid':str(count_invalid_rows)}

# COMMAND ----------

# DBTITLE 1,AUDITORIA (VALIDAR REGISTROS DUPLICADOS)
"""
Descripción: Retorna un dic con un valor Booleano y un mensaje de error indicando si existen o no duplicados
             en la tabla de la ZDR.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def validateDuplicates(prm_df_current_tbl_data_lake, prm_nombreTabla, prm_fiel_key):

  if len(prm_fiel_key) > 0:
  
    list_prm_fiel_key = prm_fiel_key
    prm_fiel_key = str(prm_fiel_key) 
    df_Duplicates = prm_df_current_tbl_data_lake.groupby(eval(prm_fiel_key)).count()
    list_prm_fiel_key.append('count')
    df_Duplicates = df_Duplicates.select(list_prm_fiel_key).filter('count > 1')

    if df_Duplicates.count() > 0:
      lista_duplicados = ft_registrar_error(df_Duplicates, list_prm_fiel_key)
      return {'isValid' : False, 'msgError' : 'Error duplicados campo: {0}, la tabla en ZDR: {1} tiene los siguientes registros duplicados: {2}'.format(prm_fiel_key, prm_nombreTabla + '_ZDC',  lista_duplicados),  'cantidad_duplicados': str(df_Duplicates.count())}
    else:
      return {'isValid' : True, 'msgError' : '', 'cantidad_duplicados': str(df_Duplicates.count())}
  else:  
    return {'isValid' : True, 'msgError' : '', 'cantidad_duplicados': '0'}

# COMMAND ----------

# DBTITLE 1,AUDITORIA REGISTRAR ERROR (LISTA DUPLICADOS) - REVISAR Y BORRAR
"""
Descripción: Retorna un una lista de valores duplicados por pk encontrados en 
             en la tabla de la ZDR.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

# def ft_registrar_error(ar_df, fiel_key):
#   print(fiel_key)
#   try:
#     df = ar_df.toPandas()
#     df = df.astype(str)
#     df["registro"] = df[fiel_key] + ' | ' + df["count"]
#     df = df.drop([fiel_key], axis=1)
#     df = df.drop(["count"], axis=1)
#     lista_duplicados = df["registro"].tolist()
        
#     return lista_duplicados
#   except Exception as error:
#     print(error, "\nError al validar los duplicados")

# COMMAND ----------

# DBTITLE 1,AUDITORIA REGISTRAR ERROR (LISTA DUPLICADOS) 
"""
Descripción: Retorna un una lista de valores duplicados por pk encontrados en 
             en la tabla de la ZDR.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def ft_registrar_error(ar_df, fiel_key):
    print(fiel_key)
    try:
      df = ar_df.toPandas()
      df = df.astype(str)
      
      registro = str()
      for ind in df.index: 
        for key in fiel_key: 
          registro = registro + df[key][ind] + ', '
      registro = registro[:-2] + ' | ' + df["count"]

      df["registro"] = registro
      display(df)
    
      df = df.drop(["count"], axis=1)
      for key in fiel_key:
        print(key)
        df = df.drop([key], axis=1)        
      
      lista_duplicados = df["registro"].tolist()

      return lista_duplicados
    except Exception as error:
      print(error, "\nError al validar los duplicados")

# COMMAND ----------

# DBTITLE 1,BORRAR
# from pyspark import SQLContext
# import pandas as pd

# fiel_key = ['CODMAESTRO', 'CODIGO']

# ar_df = spark.createDataFrame([(2, 6, 2), (4, 2, 2), (4, 3, 1), (2, 2, 2)],
#                               ['CODMAESTRO', 'CODIGO', 'count'])


# df = ar_df.toPandas()
# df = df.astype(str)

# registro = str()
# for ind in df.index: 
#   for key in fiel_key: 
#     registro = registro + df[key][ind] + ', '
# registro = registro[:-2] + ' | ' + df["count"]

# df["registro"] = registro

# #print(df)

# for key in fiel_key:
#   print(key)
#   df = df.drop([key], axis=1)
# df = df.drop(["count"], axis=1)
  
# lista_duplicados = df["registro"].tolist()
# print(lista_duplicados)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,AUDITORIA FN AUX GET MD5 - ZDR
"""
Descripción: Crea tabla temporal la cual crea dos columnas adicionales IS_VALID_MD5 y MD5 en la tabla ZDR la cual se compara con la generada en la tabla ZDC y asi validar la integridad de la información insertada en la tabla final.
Autor: Juan David Escobar E.
Fecha creción: 07/10/2020.
"""

def get_row_md5_zdr(prm_nombre_campo_funcional_lista, prm_nombre_app_origen, prm_nombre_tbl_app_origen, prm_nombre_tbl_app_origen_tmp, prm_nombre_tbl_zdr):

#   query_accum_funcionales_cols = ''.join(["COALESCE(CAST(" + str(x) + " AS STRING)" + ", ''" + ")"  + "," for (i, x) in enumerate(prm_nombre_campo_funcional_lista)])
  
  query_accum_funcionales_cols = ''.join([str(x) for (i, x) in enumerate(prm_nombre_campo_funcional_lista)])

  if query_accum_funcionales_cols.endswith(","):
    query_accum_funcionales_cols = query_accum_funcionales_cols[:-1]

  query_tbl_data_lake_md5 = "SELECT *, MD5(LOWER(CONCAT_WS(" + query_accum_funcionales_cols + "))) AS COL_MD5_ZDR, LOWER(CONCAT_WS(" + query_accum_funcionales_cols + ")) AS ZDR  FROM " + prm_nombre_tbl_zdr
  
  df_current_tbl_data_lake = sqlContext.sql(query_tbl_data_lake_md5)
  df_current_tbl_data_lake.registerTempTable(prm_nombre_tbl_app_origen_tmp + "aux_1")

  query_tbl_data_lake_md5 = "SELECT *, IF(COL_MD5_ZDC = COL_MD5_ZDR, 'TRUE', 'FALSE') as MD5_VALID FROM " + prm_nombre_tbl_app_origen_tmp + "aux_1"
  print('Query md5: ', query_tbl_data_lake_md5)
  
  
  df_current_tbl_data_lake = sqlContext.sql(query_tbl_data_lake_md5)  
  df_current_tbl_data_lake.registerTempTable(prm_nombre_tbl_app_origen_tmp + "aux_2")

  return df_current_tbl_data_lake

# COMMAND ----------

# DBTITLE 1,AUDITORIA CORRUPT RECORDS - ZDR
"""
Descripción: valida los registros corruptos de un df para la tabla ZDR
Fecha Modificación: 08/10/2020
Autor: Juan David Escobar E.
"""

def fn_corrupt_records(prm_db_dataframe, prm_fiel_key):
  error = ""
  lista_malos = list()
  
  if len(prm_fiel_key) > 0:
          
    if "_corrupt_record" in prm_db_dataframe.columns and int(prm_db_dataframe.filter(col(prm_fiel_key).isNull()).count()) > 0:
        print("error: registro invalido")
        error = "PK ({0}) invalidos: ".format(prm_fiel_key)
        prm_db_dataframe.registerTempTable('current_tbl_temp')
        
        # Obtener las columnas de los registros errados
        query_filter_corrupts = 'SELECT * FROM current_tbl_temp WHERE '
        for key in prm_fiel_key:
          query_filter_corrupts += key + ' IS NULL AND '
        
        query_filter_corrupts = query_filter_corrupts[:-4]
        malos = sqlContext.sql(query_filter_corrupts).toPandas()
        
        # malos = prm_db_dataframe.filter(col(prm_fiel_key).isNull()).toPandas()
                
        lista_malos = malos["_corrupt_record"].tolist()
        for i in range(len(lista_malos)):
          campo_pk = lista_malos[i].split(",")
          campo_pk = campo_pk[0].split(":")
          error += "[" + (campo_pk[1].replace('"','')) + "] "
  #       dbutils.fs.mv(json_filename, json_filename_corrupt) 
        return {'isValid' : False, 'msgError' : error,  'cantidad_corrupts': str(len(lista_malos))}
    else:
      return {'isValid' : True, 'msgError' : '',  'cantidad_corrupts': '0'} 
  else:
      error = 'La tabla no cuenta con pk, por lo cual no se validaron registros corruptos en las columnas pk'
      return {'isValid' : False, 'msgError' : error,  'cantidad_corrupts': str(len(lista_malos))}
      
