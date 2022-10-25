# Databricks notebook source
# DBTITLE 1,Import Libs
import pandas as pd
from pyspark.sql.functions import lit, when
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
from datetime import datetime
import pytz

# COMMAND ----------

# DBTITLE 1,Variables globales
zone = pytz.timezone('America/Bogota')
date = datetime.now(zone)
current_date = date.strftime("%d/%m/%Y %H:%M:%S")
PathParquet = '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Anexos_BLOB'

# COMMAND ----------

# DBTITLE 1,Parámetros de los Anexos
"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
partitionKey = getArgument("CarpetaBlob")

"""CarpetaNAS"""
dbutils.widgets.text("CarpetaOrigen", "","")
dbutils.widgets.get("CarpetaOrigen")
carpetaOrigen = getArgument("CarpetaOrigen")

rowKey = str.replace(carpetaOrigen, '/', '.')
rowKey = str.replace(rowKey, '\\', '.')
rowKey = rowKey[0:rowKey.find('.')]

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

# COMMAND ----------

# DBTITLE 1,Test Vars (Borrar)
# rowKey = 'comex'
# partitionKey = 'comex'
# carpetaOrigen = 'comex/comex_1'
# datosLeidos = '1118920'
# datosEscritos = '1118920'
# duration = '92'
# filesRead = '10'
# filesWritten = '9'
# status = 'Succeeded'
# throughput = '11.877'

# COMMAND ----------

# DBTITLE 1,Create the target table database
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de las cargas de archivos zip, esta tabla
# MAGIC              es de tipo SCD T2, la cual guarda la historia registro a registro de cada uno de los archivos CSV cargados al BlobStorage.
# MAGIC Responsables: Juan David Escobar E, Diego Alexander Velasco.
# MAGIC Fecha Creación: 01/04/2020
# MAGIC Fecha Modificación: 01/04/2020
# MAGIC 
# MAGIC 1. Crear tabla de auditoria.
# MAGIC */
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Load_Anexos_BLOB;
# MAGIC CREATE EXTERNAL TABLE auditoria.Zdr_Auditoria_Log_Load_Anexos_BLOB
# MAGIC (
# MAGIC     row_key string,
# MAGIC     partition_key string,
# MAGIC     carpeta_origen string,
# MAGIC     data_read string,
# MAGIC     data_written string,
# MAGIC     duration string,
# MAGIC     files_read string,
# MAGIC     files_written string,
# MAGIC     status string,
# MAGIC     throughput string,
# MAGIC     registro_activo boolean,
# MAGIC     fecha_inicio timestamp,
# MAGIC     fecha_fin timestamp
# MAGIC )
# MAGIC LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Anexos_BLOB';

# COMMAND ----------

# DBTITLE 1,Create the target data frame
df_aud_log_file = sqlContext.sql('SELECT * FROM auditoria.Zdr_Auditoria_Log_Load_Anexos_BLOB')

# COMMAND ----------

# DBTITLE 1,Create source data frame
data_source = [
Row(rowKey, partitionKey, carpetaOrigen, datosLeidos, datosEscritos, duration, filesRead, filesWritten, status, throughput)
]

schema_source = StructType([
StructField("src_row_key", StringType(), True),
StructField("src_partition_key", StringType(), True),
StructField("src_carpeta_origen", StringType(), True),
StructField("src_data_read", StringType(), True),
StructField("src_data_written", StringType(), True),
StructField("src_duration", StringType(), True),
StructField("src_files_read", StringType(), True),
StructField("src_files_written", StringType(), True),
StructField("src_status", StringType(), True),
StructField("src_throughput", StringType(), True)
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
current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

# Prepare for merge - Added effective and end date
df_source_new = df_source.withColumn('src_fecha_inicio', lit(current_date)).withColumn('src_fecha_fin', lit(high_date))

# FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = df_aud_log_file.join(df_source_new, (df_source_new.src_row_key == df_aud_log_file.row_key) & (df_source_new.src_fecha_fin == df_aud_log_file.fecha_fin), how='fullouter')

# COMMAND ----------

# DBTITLE 1,Merge actions
# Derive new column to indicate the action
df_merge = df_merge.withColumn('action', when((df_merge.carpeta_origen != df_merge.src_carpeta_origen) | (df_merge.data_read != df_merge.src_data_read) | (df_merge.data_written != df_merge.src_data_written) | (df_merge.duration != df_merge.src_duration) | (df_merge.files_read != df_merge.src_files_read) | (df_merge.files_written != df_merge.src_files_written) | (df_merge.status != df_merge.src_status) | (df_merge.throughput != df_merge.src_throughput), 'UPSERT')
                                        .when(df_merge.row_key.isNull(), 'INSERT')
                                        .otherwise('NOACTION'))

# COMMAND ----------

# DBTITLE 1,Implement the SCD type 2 actions
# Generate the new data frames based on action code
column_names = ['row_key', 'partition_key', 'carpeta_origen', 'data_read', 'data_written', 'duration', 'files_read', 'files_written', 'status', 'throughput', 'registro_activo', 'fecha_inicio', 'fecha_fin']

# For records that needs no action
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_carpeta_origen.alias('carpeta_origen'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_duration.alias('duration'),
																  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_status.alias('status'),
																  df_merge.src_throughput.alias('throughput'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_1 = df_merge.filter(df_merge.action == 'UPSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_carpeta_origen.alias('carpeta_origen'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_duration.alias('duration'),
																  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_status.alias('status'),
																  df_merge.src_throughput.alias('throughput'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('fecha_fin', df_merge.src_fecha_inicio).withColumn('registro_activo', lit(False)).select(column_names)

# COMMAND ----------

# DBTITLE 1,Union the data frames and set DB
# Union all records together     
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3_1).unionAll(df_merge_p3_2)
sqlContext.registerDataFrameAsTable(df_merge_final, "Zdr_Auditoria_Log_Load_Anexos_BLOB_Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE Zdr_Auditoria_Log_Load_Anexos_BLOB_Temp;
# MAGIC INSERT OVERWRITE TABLE auditoria.Zdr_Auditoria_Log_Load_Anexos_BLOB
# MAGIC SELECT * FROM Zdr_Auditoria_Log_Load_Anexos_BLOB_Temp;

# COMMAND ----------


