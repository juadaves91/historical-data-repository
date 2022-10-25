# Databricks notebook source
# MAGIC %md
# MAGIC #ZONA DE CRUDOS

# COMMAND ----------

# DBTITLE 1,Import Libs
import os
import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from pyspark.sql.functions import col,array_join,substring,substring_index

# COMMAND ----------

"""SrcAplicacion"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
CarpetaBlob = getArgument("CarpetaBlob")

application = CarpetaBlob

dbutils.widgets.text("origen", "","")
dbutils.widgets.get("origen")
origen = getArgument("origen")

dbutils.widgets.text("path_file", "","")
dbutils.widgets.get("path_file")
path_file = getArgument("path_file")

# COMMAND ----------

print('Parametros de entrada')
print("CarpetaBlob: {0}, origen:{1}, path_file:{2}".format(CarpetaBlob, origen, path_file))

# COMMAND ----------

# DBTITLE 1,Obtener DataFrame Tabla Intermedia
def read_files_csv(ar_path_files_csv):
  arr_files = []
  df_csv_pandas = pd.DataFrame(data = None, columns = [])
  for root, dirs, files in os.walk(ar_path_files_csv):
    for file in files:
      if file.endswith(".csv"):
        ar_file = ar_path_files_csv + file
        ar_file = ar_file[5:]
        arr_files.append(file)
        df_csv = spark.read.csv(ar_file, header=True, sep="|")
        df_csv_pandas_aux = df_csv.toPandas()
        df_csv_pandas = df_csv_pandas.append(df_csv_pandas_aux)
  df_csv = sqlContext.createDataFrame(df_csv_pandas)
  return arr_files, df_csv

# COMMAND ----------

def read_file_csv(ar_path_file_csv):
  ar_file = ar_path_file_csv[5:]
  df_csv = spark.read.csv(ar_file, header=True, sep="|")
  return df_csv

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
if origen == "":
  arr_files, indicadores_df = read_files_csv("/dbfs/mnt/dbbicorporativo-indicadores/raw/")
else:
  indicadores_df = read_file_csv(path_file)

sqlContext.registerDataFrameAsTable(indicadores_df, "Zdc_Dbbicorporativo_Indicadores_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
if origen == "":
  path_escritura = '/mnt/dbbicorporativo-indicadores-storage'
  # Eliminar archivo de propiedades existente
  dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_dbbicorporativo_db.txt", True)
  # Enable Arrow-based columnar data transfers
  spark.conf.set("spark.sql.execution.arrow.enabled", "true")
  # Describe de la tabla
  meta = spark.sql("""DESCRIBE Zdc_Dbbicorporativo_Indicadores_TablaIntermedia""")
  # Conversion a Pandas Dataframe
  pmeta = meta.select("*").toPandas()
  pmeta.to_csv("/dbfs"+path_escritura+"/metadata_dbbicorporativo_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
"""
Descripción: Creación de columnas calculadas annio y mes
"""

indicadores_df = indicadores_df.select("IdRegistro",
                                       "IdFact_CifrasPeriodo",
                                       "IdProyecto",
                                       "IdTema",
                                       "IdEstructura",
                                       "IdReporte",
                                       "IdIndicador",
                                       "IdEmpresa",
                                       "IdGeografia",
                                       "IdCanal",
                                       "IdProducto",
                                       "IdClasificacion",
                                       "IdTiempo",
                                       "Anexos",
                                       "Valor",
                                       "Activo",
                                       "FechaActualizacion",
                                       "dtmFecha",
                                       "CodProyecto",
                                       "Proyecto",
                                       "CodEstructura",
                                       "Estructura",
                                       "CodIndicador",
                                       "Indicador",
                                       "Estado",                                           
                                       substring(substring_index('dtmFecha', ' ', 1),-10,4).cast('int').alias('anio'),
                                       substring(substring_index('dtmFecha', ' ', 1),-5,2).cast('int').alias('mes'))

sqlContext.registerDataFrameAsTable(indicadores_df, "Zdc_Dbbicorporativo_Indicadores_TablaIntermedia")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /dbbicorporativo/dbbicorporativo-indicadores/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /dbbicorporativo/dbbicorporativo-indicadores/5_LoadSchema $from_create_schema = True

# COMMAND ----------

# MAGIC %md
# MAGIC #Auditoria

# COMMAND ----------

# MAGIC %run /dbbicorporativo/dbbicorporativo-indicadores/7_Auditoria
