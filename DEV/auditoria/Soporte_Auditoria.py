# Databricks notebook source
# MAGIC %md
# MAGIC #Librerias

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC #Variables

# COMMAND ----------

dbutils.widgets.text("json_name", "","")
dbutils.widgets.get("json_name")
json_name = getArgument("json_name")

dbutils.widgets.text("aplication", "","")
dbutils.widgets.get("aplication")
aplication = getArgument("aplication")

dbutils.widgets.text("error", "","")
dbutils.widgets.get("error")
error_json = getArgument("error")

name_archive = 'execute_json_auditoria.txt'
source = '/dbfs'
source_mount = '/mnt/lotus-'

zone = pytz.timezone('America/Bogota')
date = datetime.now(zone)
data_base = "auditoria"
table_name = "zdr_lotus_auditoria_json"
path_auditoria = '/mnt/utilidades/panel_auditoria'

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

def ft_create_table_auditoria(ar_df):
  try:
    auditoria_df = sqlContext.createDataFrame(ar_df)
    sqlContext.sql("create database if not exists " + data_base)
    auditoria_df.write.mode("append").save(path_auditoria)
    auditoria_df = spark.read.format("parquet").load(path_auditoria)
    sqlContext.sql("drop table if exists " + data_base + "." + table_name)
    auditoria_df.write.mode("overwrite").option('path', path_auditoria).saveAsTable(data_base + "." + table_name)
    sqlContext.sql("refresh table " + data_base + "." + table_name)
  except Exception as error:
    print(error, "\n Error al crear la base de datos o la tabla auditoria")

# COMMAND ----------

# MAGIC %md
# MAGIC #Validacion duplicados

# COMMAND ----------

def ft_registrar_error(ar_df):
  try:
    df = ar_df.toPandas()
    df = df.astype(str)
    df["registro"] = df["UniversalID"] + ' | ' + df["count"]
    df = df.drop(["UniversalID"], axis=1)
    df = df.drop(["count"], axis=1)
    lista_duplicados = df["registro"].tolist()
    
    return lista_duplicados
  except Except as error:
    print(error, "\nError al validar los duplicados")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Registro JSON

# COMMAND ----------

def ft_create_path(ar_aplication, ar_json_name):
  try:
    with open( source + '/mnt/' + "utilidades/" + name_archive,"w") as json_path:
        json_path.write(source_mount + ar_aplication + '/raw/' + ar_json_name + '|' + str.replace(ar_aplication, '/', '.'))
        print("Archivo creado coreectamente")
  except Exception as error:
    print(error, "Error al crear la ruta de procesamiento")

ft_create_path(aplication, json_name)

# COMMAND ----------

# MAGIC %run /auditoria/Panel_Auditoria

# COMMAND ----------

registros_tabla_intermedia = df_registros
registros_tabla_principal = panel_principal.count()
registros_tabla_detalle = panel_detalle.select(col("UniversalID")).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Validacion UniversalIDCompuesto

# COMMAND ----------

def ft_validacion_schema(ar_df):
  try:
    if int(ar_df.select(col("UniversalIDCompuesto")).count()) > 0:
      return True
  except Exception as Error:
    return False

# COMMAND ----------

# MAGIC %run /auditoria/Apis_HTTPS

# COMMAND ----------

def ft_precesar(ar_error = ""):
  try:
    sqlContext.registerDataFrameAsTable(registros_tabla_intermedia, "Zdc_Lotus_Auditoria")
    if ft_validacion_schema(registros_tabla_intermedia):
      ar_df = sqlContext.sql("select if (UniversalIDCompuesto is null, UniversalID , UniversalIDCompuesto) \
                                                             as UniversalID from Zdc_Lotus_Auditoria")
      
    df_CampoCompuesto = registros_tabla_intermedia.groupby("UniversalID").count()
    df_CampoCompuesto = df_CampoCompuesto.select(col("UniversalID"), col("count")).filter(col("count") > 1)
    lista_duplicados = ft_registrar_error(df_CampoCompuesto)
    registro_tabla_intermedia = registros_tabla_intermedia.count()
    registros_origen_app = ft_get_records_table_storage(json_name.lower().strip())
    registros_no_cargados = str(int(registros_origen_app) - registro_tabla_intermedia)
    
    new_row = {'APLICACION': str.replace(aplication, '.', '/'),
               'JSON':json_name,
               'REGISTROS_ORIGEN_APP': str(registros_origen_app),
               'FECHA': date.strftime('%d/%m/%Y/ %H:%M:%S'),
               'CARPETA':source_mount + aplication + '/raw/' + json_name,
               'REGISTROS_TABLA_INTERMEDIA':str(registro_tabla_intermedia),
               'REGISTRO_PANEL_PRINCIPAL':str(registros_tabla_principal),
               'REGISTROS_PANEL_DETALLE':registros_tabla_detalle,
               'REGISTROS_DUPLICADOS':str(lista_duplicados),
               'REGISTROS_CORRUPTOS':error_json,
               'REGISTROS_NO_CARGADOS':registros_no_cargados}
    
    df = df_audutoria.append(new_row,ignore_index=True)
    ft_create_table_auditoria(df)

    return "Ok"
  except Exception as error:
    print(error, "\nerror al registrar la auditoria")

ft_precesar()
