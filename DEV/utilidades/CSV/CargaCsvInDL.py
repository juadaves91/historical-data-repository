# Databricks notebook source
# DBTITLE 1,Import Libs
import pandas as pd
from pyspark.sql.functions import lit, when, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as f
from datetime import datetime
import pytz

# COMMAND ----------

# DBTITLE 1,Variables globales
zone = pytz.timezone('America/Bogota')
date = datetime.now(zone)
current_date = date.strftime("%d/%m/%Y %H:%M:%S")
PathParquet = '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Data_File_DL'

# COMMAND ----------

# DBTITLE 1,Parámetros del CSV
"""Archivo CSV a validar"""
dbutils.widgets.text("Csv_file", "","")
dbutils.widgets.get("Csv_file")
rowKey = getArgument("Csv_file")

"""SrcAplicacion"""
dbutils.widgets.text("SrcAplicacion", "","")
dbutils.widgets.get("SrcAplicacion")
application = getArgument("SrcAplicacion")

"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
partitionKey = getArgument("CarpetaBlob")

file = "/mnt/" + application + "-" + partitionKey + "/raw/" + rowKey

# COMMAND ----------

# DBTITLE 1,Parámetros del Resultado de Ejecución
"""Status"""
dbutils.widgets.text("Status", "","")
dbutils.widgets.get("Status")
status = getArgument("Status")

"""Throughput"""
dbutils.widgets.text("Throughput", "","")
dbutils.widgets.get("Throughput")
throughput = getArgument("Throughput")

"""Duration"""
dbutils.widgets.text("Duration", "","")
dbutils.widgets.get("Duration")
duration = getArgument("Duration")

"""FilesRead"""
dbutils.widgets.text("FilesRead", "","")
dbutils.widgets.get("FilesRead")
filesRead = getArgument("FilesRead")

"""FilesWritten"""
dbutils.widgets.text("FilesWritten", "","")
dbutils.widgets.get("FilesWritten")
filesWritten = getArgument("FilesWritten")

"""DatosLeidos"""
dbutils.widgets.text("datosleidos", "","")
dbutils.widgets.get("datosleidos")
datosLeidos = getArgument("datosleidos")

"""DatosEscritos"""
dbutils.widgets.text("datosescritos", "","")
dbutils.widgets.get("datosescritos")
datosEscritos = getArgument("datosescritos")

"""MD5"""
dbutils.widgets.text("md5", "","")
dbutils.widgets.get("md5")
md5 = getArgument("md5")

"""Código Parametro contenido correo"""
dbutils.widgets.text("prm_cod_email", "","")
dbutils.widgets.get("prm_cod_email")
prm_cod_email = getArgument("prm_cod_email")

"""Numero de cargas"""
dbutils.widgets.text("prm_count_load", "","")
dbutils.widgets.get("prm_count_load")
prm_count_load = getArgument("prm_count_load")



#Variables globales
schema = partitionKey[:partitionKey.index('/')] if partitionKey.count('/') > 0 else partitionKey
table_metadatos = 'Zdr_' + schema + '_src_metadatos'
filter = rowKey[:rowKey.index('_')].lower()
parameters_file_fuente = 'app-' + schema + '/metadatos'

# COMMAND ----------

# MAGIC %md
# MAGIC ##CREAR TABLAS APARTIR DEL ARCHIVO FORMATO FUENTE

# COMMAND ----------

dbutils.notebook.run("/utilidades/CSV/CreacionTablasMetadata", 1800, {"folder_blob":parameters_file_fuente})

# COMMAND ----------

# DBTITLE 1,Inicialización de dependencias (Notebooks)
# MAGIC %run /utilidades/CSV/ValidacionCSV

# COMMAND ----------

# DBTITLE 1,Create the target table database
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de las cargas de archivos en formato CSV, esta tabla
# MAGIC              es de tipo SCD T2, la cual guarda la historia registro a registro de cada uno de los archivos CSV cargados al DataLake.
# MAGIC Responsables: Juan David Escobar E, Diego Alexander Velasco.
# MAGIC Fecha Creación: 01/04/2020
# MAGIC Fecha Modificación: 01/04/2020
# MAGIC 
# MAGIC 1. Crear tabla de auditoria.
# MAGIC */
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Load_Data_File_DL;
# MAGIC CREATE EXTERNAL TABLE auditoria.Zdr_Auditoria_Log_Load_Data_File_DL
# MAGIC (
# MAGIC     row_key string,
# MAGIC     partition_key string,
# MAGIC     application string,
# MAGIC     status string,
# MAGIC     duration string,
# MAGIC     files_read string,
# MAGIC     files_written string,
# MAGIC     data_read string,
# MAGIC     data_written string,
# MAGIC     throughput string,
# MAGIC     err_encoding string,
# MAGIC     err_md5 string,
# MAGIC     err_row_empty string,
# MAGIC     err_duplicates string,
# MAGIC     err_format_dates string,
# MAGIC     registro_activo boolean,
# MAGIC     fecha_inicio timestamp,
# MAGIC     fecha_fin timestamp
# MAGIC )
# MAGIC LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Data_File_DL';

# COMMAND ----------

# DBTITLE 1,Create the target data frame
df_aud_log_file = sqlContext.sql('SELECT * FROM auditoria.Zdr_Auditoria_Log_Load_Data_File_DL')

# COMMAND ----------

def validacion_duplicados(ar_codigo = ''):
  if ar_codigo == 'validar_duplicados':
    
    df = sqlContext.sql("select count(*) as Result from " + schema + "." + table_metadatos + \
                        " where lower(OrigenSubAplicacion)  = " + "'" + filter + "'" + " and upper(LlavePrimaria) = 'SI'")

    validacion_duplicados = df.select(f.collect_list('Result')).first()[0]
    
  else:
    df = sqlContext.sql("select NombreCampoTecnico  from " + schema + "." + table_metadatos + \
                        " where lower(OrigenSubAplicacion)  = " + "'" + filter + "'" + " and upper(LlavePrimaria) = 'SI'")
    
    validacion_duplicados = df.select(f.collect_list('NombreCampoTecnico')).first()[0]
    
  return validacion_duplicados

def get_campo_fecha():
  df = sqlContext.sql("select NombreCampoTecnico from " + schema + "." + table_metadatos + \
                      " where lower(OrigenSubAplicacion)  = " + "'" + filter + "'" + " and upper(FechaBusqueda) = 'SI'")
  campo_fecha = df.select(f.collect_list('NombreCampoTecnico')).first()[0]
  
  return campo_fecha

# COMMAND ----------

# DBTITLE 1,Set error variables
campos_fecha = get_campo_fecha()

if len(campos_fecha) > 0:
  ar_columns = [{'column_name': campos_fecha[0], 'format_column':'%Y-%m-%d %H:%M:%S.%m'}]

else:
  ar_columns = [{'column_name': '', 'format_column':'%Y-%m-%d %H:%M:%S.%m'}]

"""
Validacion tipo de base de datos para validar Duplicados
0 --> es una tabla de muchos a muchos
1 --> Posee un primary key por ende se valida que no exista los duplicados

"""
if validacion_duplicados('validar_duplicados')[0] == 0:
  dic_prm_init = {'file': file, 'md5': md5, 'records_unique': '', 'columns_date': ar_columns}
else:
  dic_prm_init = {'file': file, 'md5': md5, 'records_unique':validacion_duplicados()[0] , 'columns_date': ar_columns}

errors = init_validations(dic_prm_init)

err_encoding = errors[0]
err_md5 = errors[1]
err_row_empty = errors[2]
err_duplicates = errors[3]
err_format_dates = errors[4]

# COMMAND ----------

# DBTITLE 1,Create source data frame
data_source = [
 Row(
  rowKey,
  partitionKey,
  application,
  status,
  duration,
  filesRead,
  filesWritten,
  datosLeidos,
  datosEscritos,
  throughput,
  err_encoding,
  err_md5,
  err_row_empty,
  err_duplicates,
  err_format_dates
 )
]

schema_source = StructType([
StructField("src_row_key", StringType(), True),
StructField("src_partition_key", StringType(), True),
StructField("src_application", StringType(), True),
StructField("src_status", StringType(), True),
StructField("src_duration", StringType(), True),
StructField("src_files_read", StringType(), True),
StructField("src_files_written", StringType(), True),
StructField("src_data_read", StringType(), True),
StructField("src_data_written", StringType(), True),
StructField("src_throughput", StringType(), True),
StructField("src_err_encoding", StringType(), True),
StructField("src_err_md5", StringType(), True),
StructField("src_err_row_empty", StringType(), True),
StructField("src_err_duplicates", StringType(), True),
StructField("src_err_format_dates", StringType(), True)
])

df_source = sqlContext.createDataFrame(
sc.parallelize(data_source),
schema_source
)

# COMMAND ----------

# DBTITLE 1,Create merge data frame
high_date_aux = datetime(9999, 12, 31, 00, 00, 00)
high_date = high_date_aux.strftime("%d/%m/%Y %H:%M:%S")

high_date = datetime.strptime(high_date, "%d/%m/%Y %H:%M:%S")
current_date = date.strftime("%d/%m/%Y %H:%M:%S")
current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

# # Prepare for merge - Added effective and end date
df_source_new = df_source.withColumn('src_fecha_inicio', lit(current_date)).withColumn('src_fecha_fin', lit(high_date))

# # FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = df_aud_log_file.join(df_source_new, (df_source_new.src_row_key == df_aud_log_file.row_key) & (df_source_new.src_fecha_fin == df_aud_log_file.fecha_fin), how='fullouter')

# COMMAND ----------

# DBTITLE 1,Merge actions
# Derive new column to indicate the action
df_merge = df_merge.withColumn('action', when(
                                          (df_merge.status != df_merge.src_status) |
                                          (df_merge.duration != df_merge.src_duration) |
                                          (df_merge.files_read != df_merge.src_files_read) |
                                          (df_merge.files_written != df_merge.src_files_written) |
                                          (df_merge.data_read != df_merge.src_data_read) |
                                          (df_merge.data_written != df_merge.src_data_written) |
                                          (df_merge.throughput != df_merge.src_throughput) |
                                          (df_merge.src_err_encoding != df_merge.src_err_encoding) |
                                          (df_merge.src_err_md5 != df_merge.src_err_md5) |
                                          (df_merge.src_err_row_empty != df_merge.src_err_row_empty) |
                                          (df_merge.src_err_duplicates != df_merge.src_err_duplicates) |
                                          (df_merge.src_err_format_dates != df_merge.src_err_format_dates),
                                        'UPSERT')
                                        .when(df_merge.row_key.isNull(), 'INSERT')
                                        .otherwise('NOACTION'))

# COMMAND ----------

display(df_merge)

# COMMAND ----------

# DBTITLE 1,Implement the SCD type 2 actions
# Generate the new data frames based on action code
column_names = ['row_key', 'partition_key', 'application', 'status', 'duration', 'files_read', 'files_written',  'data_read', 'data_written', 'throughput', 'err_encoding', 'err_md5', 'err_row_empty', 'err_duplicates', 'err_format_dates', 'registro_activo', 'fecha_inicio', 'fecha_fin']

# For records that needs no action
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_application.alias('application'),
                                                                  df_merge.src_status.alias('status'),
                                                                  df_merge.src_duration.alias('duration'),
                                                                  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_throughput.alias('throughput'),
																  df_merge.src_err_encoding.alias('err_encoding'),
                                                                  df_merge.src_err_md5.alias('err_md5'),
                                                                  df_merge.src_err_row_empty.alias('err_row_empty'),
                                                                  df_merge.src_err_duplicates.alias('err_duplicates'),
                                                                  df_merge.src_err_format_dates.alias('err_format_dates'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_1 = df_merge.filter(df_merge.action == 'UPSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_application.alias('application'),
                                                                  df_merge.src_status.alias('status'),
                                                                  df_merge.src_duration.alias('duration'),
                                                                  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_throughput.alias('throughput'),
																  df_merge.src_err_encoding.alias('err_encoding'),
                                                                  df_merge.src_err_md5.alias('err_md5'),
                                                                  df_merge.src_err_row_empty.alias('err_row_empty'),
                                                                  df_merge.src_err_duplicates.alias('err_duplicates'),
                                                                  df_merge.src_err_format_dates.alias('err_format_dates'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('fecha_fin', df_merge.src_fecha_inicio).withColumn('registro_activo', lit(False)).select(column_names)

# COMMAND ----------

# DBTITLE 1,Union the data frames and set DB
# Union all records together     
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3_1).unionAll(df_merge_p3_2)
sqlContext.registerDataFrameAsTable(df_merge_final, "Zdr_Auditoria_Log_Load_Data_File_DL_Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE Zdr_Auditoria_Log_Load_Data_File_DL_Temp;
# MAGIC INSERT OVERWRITE TABLE auditoria.Zdr_Auditoria_Log_Load_Data_File_DL
# MAGIC SELECT * FROM Zdr_Auditoria_Log_Load_Data_File_DL_Temp;

# COMMAND ----------

#%run /utilidades/notificaciones/SendEmailNotificationLoadFiles
