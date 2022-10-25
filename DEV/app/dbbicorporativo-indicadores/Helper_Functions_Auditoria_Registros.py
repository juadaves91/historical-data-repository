# Databricks notebook source
# MAGIC %md
# MAGIC # FUNCIONES AUXILIARES AUDITORIA DE REGISTROS - BD

# COMMAND ----------

import os
import pandas as pd

# COMMAND ----------

if origen != "":
  df_audutoria = pd.DataFrame(data=None, columns = ['APLICACION',
                                                    'JSON',
                                                    'REGISTROS_ORIGEN_APP',
                                                    'FECHA',
                                                    'CARPETA',
                                                    'REGISTROS_TABLA_INTERMEDIA',
                                                    'REGISTRO_PANEL_PRINCIPAL',
                                                    'REGISTROS_PANEL_DETALLE',
                                                    'REGISTROS_DUPLICADOS',
                                                    'REGISTROS_CORRUPTOS',
                                                    'REGISTROS_NO_CARGADOS'])

# COMMAND ----------

# MAGIC %md
# MAGIC #Registro Tabla Auditoria

# COMMAND ----------

def ft_create_table_auditoria(ar_df, columns_duplicates):
  try:
    auditoria_df = sqlContext.createDataFrame(ar_df)
    sqlContext.sql("create database if not exists " + data_base)
    auditoria_df.write.mode("append").format("delta").save(path_auditoria)
    auditoria_df = spark.read.format("delta").load(path_auditoria)
    sqlContext.sql("drop table if exists " + data_base + "." + table_name)
    auditoria_df = auditoria_df.drop_duplicates(columns_duplicates)
    auditoria_df.write.mode("overwrite").format("delta").option('path', path_auditoria).saveAsTable(data_base + "." + table_name)
    sqlContext.sql("refresh table " + data_base + "." + table_name)
  except Exception as error:
    print(error, "\n Error al crear la base de datos o la tabla auditoria")

# COMMAND ----------

# MAGIC %md
# MAGIC #Validacion duplicados

# COMMAND ----------

def ft_registrar_error(ar_df, fiel_key):
  try:
    df = ar_df.toPandas()
    df = df.astype(str)
    df["registro"] = df[fiel_key] + ' | ' + df["count"]
    df = df.drop([fiel_key], axis=1)
    df = df.drop(["count"], axis=1)
    lista_duplicados = df["registro"].tolist()
    
    return lista_duplicados
  except Exception as error:
    print(error, "\nError al validar los duplicados")

# COMMAND ----------

# MAGIC %md
# MAGIC #Validacion UniversalIDCompuesto

# COMMAND ----------

def ft_validacion_schema(ar_df, fiel_key):
  try:
    if int(ar_df.select(col(fiel_key)).count()) > 0:
      return True
  except Exception as Error:
    return False

# COMMAND ----------

def ft_get_records_table_storage(ar_rowkey):
  try:
    registros = sqlContext.sql("select registros from auditoria.zdr_auditoria_log_load_data_file_blob where registro_activo = True and row_key = " + "'" + ar_rowkey.strip().lower() + "'")
    df_f = registros.toPandas()
    return df_f["registros"][0]
  except Exception as error:
    print(error, "Error al devolver la cantidad de registros del JSON")

# COMMAND ----------

def ft_precesar(fiel_key, columns_duplicates, ar_error = ""):
  try:
    sqlContext.registerDataFrameAsTable(registros_tabla_intermedia, "Zdc_Application_Auditoria")
    if ft_validacion_schema(registros_tabla_intermedia, fiel_key):
      ar_df = sqlContext.sql("select " + fiel_key + " from Zdc_Application_Auditoria")
      
    df_CampoCompuesto = registros_tabla_intermedia.groupby(fiel_key).count()
    df_CampoCompuesto = df_CampoCompuesto.select(col(fiel_key), col("count")).filter(col("count") > 1)
    lista_duplicados = ft_registrar_error(df_CampoCompuesto, fiel_key)
    registro_tabla_intermedia = registros_tabla_intermedia.count()
    registros_origen_app = ft_get_records_table_storage(file_name.lower().strip())
    registros_no_cargados = str(int(registros_origen_app) - registro_tabla_intermedia)
    
    new_row = {'APLICACION': str.replace(application, '.', '/'),
               'JSON': file_name,
               'REGISTROS_ORIGEN_APP': str(registros_origen_app),
               'FECHA': date.strftime('%d/%m/%Y/ %H:%M:%S'),
               'CARPETA': path_file,
               'REGISTROS_TABLA_INTERMEDIA': str(registro_tabla_intermedia),
               'REGISTRO_PANEL_PRINCIPAL': str(registros_tabla_principal),
               'REGISTROS_PANEL_DETALLE': registros_tabla_detalle,
               'REGISTROS_DUPLICADOS': str(lista_duplicados),
               'REGISTROS_CORRUPTOS': error_json,
               'REGISTROS_NO_CARGADOS': registros_no_cargados}
    
    df = df_audutoria.append(new_row,ignore_index=True)
    ft_create_table_auditoria(df, columns_duplicates)
    
    return "Ok"
  except Exception as error:
    print(error, "\nerror al registrar la auditoria")
