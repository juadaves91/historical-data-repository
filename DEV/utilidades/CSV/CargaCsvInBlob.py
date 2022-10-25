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
PathParquet = '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Data_File_BLOB'

# COMMAND ----------

# DBTITLE 1,Parámetros del CSV
"""Archivo CSV a validar"""
dbutils.widgets.text("Csv_file", "","")
dbutils.widgets.get("Csv_file")
rowKey = getArgument("Csv_file")

"""SrcAplicacion"""
dbutils.widgets.text("SrcAplicacion", "","")
dbutils.widgets.get("SrcAplicacion")
sa = getArgument("SrcAplicacion")

"""CarpetaBlob"""
dbutils.widgets.text("CarpetaBlob", "","")
dbutils.widgets.get("CarpetaBlob")
partitionKey = getArgument("CarpetaBlob")


# COMMAND ----------

# DBTITLE 1,Parámetros del Resultado de Ejecución
"""Status"""
dbutils.widgets.text("Status", "","")
dbutils.widgets.get("Status")
status = getArgument("Status")

"""CopyTime"""
dbutils.widgets.text("CopyTime", "","")
dbutils.widgets.get("CopyTime")
copyTime = getArgument("CopyTime")

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

"""CarpetaOrigen"""
dbutils.widgets.text("CarpetaOrigen", "","")
dbutils.widgets.get("CarpetaOrigen")
carpetaOrigen = getArgument("CarpetaOrigen")

"""MD5"""
dbutils.widgets.text("md5", "","")
dbutils.widgets.get("md5")
md5 = getArgument("md5")

"""Registros"""
dbutils.widgets.text("registros", "","")
dbutils.widgets.get("registros")
registros = getArgument("registros")

"""DatosLeidos"""
dbutils.widgets.text("datosleidos", "","")
dbutils.widgets.get("datosleidos")
datosLeidos = getArgument("datosleidos")

"""DatosEscritos"""
dbutils.widgets.text("datosescritos", "","")
dbutils.widgets.get("datosescritos")
datosEscritos = getArgument("datosescritos")


#Variables 

if partitionKey.find('/') > 0:
  carpeta, subcarpeta = partitionKey.split('/')
else:
  carpeta, subcarpeta =  partitionKey, ''

# COMMAND ----------

"""
Me permite almacenar los difernetes archivos de propiedades
para aplicaciones que tengan distribuida la informacion en 
caperta consecutivas
"""

#crea una carpeta donde se va alamcenar los diferentes archivos de propiedades
dbutils.fs.mkdirs('/mnt/app-' + carpeta + '-storage/archivos_propiedades')


path_read = '/mnt/app-' +  carpeta + '-storage/' + subcarpeta + '/propiedades_' +  carpeta + '_csv.txt'
path_write = '/mnt/app-' + carpeta + '-storage/archivos_propiedades/propiedades_' +  carpeta + '_csv_' +  carpetaOrigen + '.txt'

#copia el archivo de propiedades de una ruta a otra
dbutils.fs.cp(path_read , path_write)

# COMMAND ----------

# DBTITLE 1,Test Vars (Borrar)
# RowKey = 'lotus_cayman_20200620_154.json'
# PartitionKey = 'cayman'
# CarpetaOrigen = 'cayman_3'
# CopyTime = '2020-02-27T23:30:16.0807018Z'
# DataRead = '1042281'
# DataWritten = '1042281'
# Duration = '14'
# datosLeidos = '1'
# datosEscritos = '1'
# LoadDate = '20200227 23:30:42'
# Status = 'Succeeded'
# Throughput = '72.704'
# md5 = 'bb9e8705e3619f350515b86dfc595c55'
# registros = '690'
# RegistroActivo = 'True'
# FechaInicio = current_date
# FechaFin = datetime.strptime('31/12/9999 00:00:00', '%d/%m/%Y %H:%M:%S').date()

# COMMAND ----------

# DBTITLE 1,Create the target table database
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de las cargas de archivos en formato CSV, esta tabla
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
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Load_Data_File_BLOB;
# MAGIC CREATE EXTERNAL TABLE auditoria.Zdr_Auditoria_Log_Load_Data_File_BLOB
# MAGIC (
# MAGIC     row_key string,
# MAGIC     partition_key string,
# MAGIC     carpeta_origen string,
# MAGIC     copy_time string,
# MAGIC     data_read string,
# MAGIC     data_written string,
# MAGIC     duration string,
# MAGIC     files_read string,
# MAGIC     files_written string,
# MAGIC     status string,
# MAGIC     throughput string,
# MAGIC     md5 string,
# MAGIC     registros string,
# MAGIC     registro_activo boolean,
# MAGIC     fecha_inicio timestamp,
# MAGIC     fecha_fin timestamp
# MAGIC )
# MAGIC LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Load_Data_File_BLOB';

# COMMAND ----------

# DBTITLE 1,Create the target data frame
df_aud_log_file = sqlContext.sql('SELECT * FROM auditoria.Zdr_Auditoria_Log_Load_Data_File_BLOB')

# COMMAND ----------

# DBTITLE 1,Create source data frame
data_source = [
Row(rowKey, partitionKey, carpetaOrigen, copyTime, datosLeidos, datosEscritos, duration, filesRead, filesWritten, status, throughput, md5, registros)
]

schema_source = StructType([
StructField("src_row_key", StringType(), True),
StructField("src_partition_key", StringType(), True),
StructField("src_carpeta_origen", StringType(), True),
StructField("src_copy_time", StringType(), True),
StructField("src_data_read", StringType(), True),
StructField("src_data_written", StringType(), True),
StructField("src_duration", StringType(), True),
StructField("src_files_read", StringType(), True),
StructField("src_files_written", StringType(), True),
StructField("src_status", StringType(), True),
StructField("src_throughput", StringType(), True),
StructField("src_md5", StringType(), True),
StructField("src_registros", StringType(), True)
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
df_merge = df_merge.withColumn('action', when((df_merge.carpeta_origen != df_merge.src_carpeta_origen) | (df_merge.md5 != df_merge.src_md5) | (df_merge.registros != df_merge.src_registros) | (df_merge.copy_time != df_merge.src_copy_time) | (df_merge.data_read != df_merge.src_data_read) | (df_merge.data_written != df_merge.src_data_written) | (df_merge.duration != df_merge.src_duration) | (df_merge.files_read != df_merge.src_files_read) | (df_merge.files_written != df_merge.src_files_written) | (df_merge.status != df_merge.src_status) | (df_merge.throughput != df_merge.src_throughput), 'UPSERT')
                                        .when(df_merge.row_key.isNull(), 'INSERT')
                                        .otherwise('NOACTION'))

# COMMAND ----------

# DBTITLE 1,Implement the SCD type 2 actions
# Generate the new data frames based on action code
column_names = ['row_key', 'partition_key', 'carpeta_origen', 'copy_time', 'data_read', 'data_written', 'duration', 'files_read', 'files_written', 'status', 'throughput', 'md5', 'registros', 'registro_activo', 'fecha_inicio', 'fecha_fin']

# For records that needs no action
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_carpeta_origen.alias('carpeta_origen'),
																  df_merge.src_copy_time.alias('copy_time'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_duration.alias('duration'),
																  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_status.alias('status'),
																  df_merge.src_throughput.alias('throughput'),
																  df_merge.src_md5.alias('md5'),
																  df_merge.src_registros.alias('registros'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_1 = df_merge.filter(df_merge.action == 'UPSERT').select(df_merge.src_row_key.alias('row_key'),
                                                                  df_merge.src_partition_key.alias('partition_key'),
																  df_merge.src_carpeta_origen.alias('carpeta_origen'),
																  df_merge.src_copy_time.alias('copy_time'),
																  df_merge.src_data_read.alias('data_read'),
																  df_merge.src_data_written.alias('data_written'),
																  df_merge.src_duration.alias('duration'),
																  df_merge.src_files_read.alias('files_read'),
																  df_merge.src_files_written.alias('files_written'),
																  df_merge.src_status.alias('status'),
																  df_merge.src_throughput.alias('throughput'),
																  df_merge.src_md5.alias('md5'),
																  df_merge.src_registros.alias('registros'),
                                                                  lit(True).alias('registro_activo'),
                                                                  df_merge.src_fecha_inicio.alias('fecha_inicio'),
                                                                  df_merge.src_fecha_fin.alias('fecha_fin'))

df_merge_p3_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('fecha_fin', df_merge.src_fecha_inicio).withColumn('registro_activo', lit(False)).select(column_names)

# COMMAND ----------

# DBTITLE 1,Union the data frames and set DB
# Union all records together     
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3_1).unionAll(df_merge_p3_2)
sqlContext.registerDataFrameAsTable(df_merge_final, "Zdr_Auditoria_Log_Load_Data_File_BLOB_Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE Zdr_Auditoria_Log_Load_Data_File_BLOB_Temp;
# MAGIC INSERT OVERWRITE TABLE auditoria.Zdr_Auditoria_Log_Load_Data_File_BLOB
# MAGIC SELECT * FROM Zdr_Auditoria_Log_Load_Data_File_BLOB_Temp;
