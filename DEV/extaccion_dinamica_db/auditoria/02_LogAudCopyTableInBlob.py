# Databricks notebook source
# MAGIC %md
# MAGIC # REGISTRO LOG AUD -  TAREA EXTRACCION Y COPY TABLAS DE LA BD ORIGEN

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DataType, TimestampType
from pyspark import SparkContext, SparkConf, SQLContext
from datetime import datetime
import pytz
from pyspark.sql import Row

# COMMAND ----------

"""
Variables para eliminacion de registro de aud en tabla: auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

nombre_app_origen = SEMI-H
nombre_tbl_sub_app_origen = PEOPLESOFT
nombre_tbl_app_origen = HISACUMANOCONCEPTO
"""

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""---------------------------Parámetros de entrada Info tabla----------------------------"""

# APP.
dbutils.widgets.text("nombre_app_origen", "", "")
dbutils.widgets.get("nombre_app_origen")
nombre_app_origen = getArgument("nombre_app_origen")

# SUB-APP.
dbutils.widgets.text("nombre_tbl_sub_app_origen", "", "")
dbutils.widgets.get("nombre_tbl_sub_app_origen")
nombre_tbl_sub_app_origen = getArgument("nombre_tbl_sub_app_origen")

# Tabla actual a extraer de la BD.
dbutils.widgets.text("nombre_tbl_app_origen", "", "")
dbutils.widgets.get("nombre_tbl_app_origen")
nombre_tbl_app_origen = getArgument("nombre_tbl_app_origen")


"""------------------------Parámetros de entrada copy task DF------------------------------"""

"""CarpetaBlobTabla"""
dbutils.widgets.text("CarpetaBlobTabla", "","")
dbutils.widgets.get("CarpetaBlobTabla")
carpetaBlobTabla = getArgument("CarpetaBlobTabla")

"""CopyTime"""
dbutils.widgets.text("CopyTime", "","")
dbutils.widgets.get("CopyTime")
copyTime = getArgument("CopyTime")

"""Duration"""
dbutils.widgets.text("Duration", "","")
dbutils.widgets.get("Duration")
duration = getArgument("Duration")

"""RowsRead"""
dbutils.widgets.text("RowsRead", "","")
dbutils.widgets.get("RowsRead")
rowsRead = getArgument("RowsRead")

"""FilesWritten"""
dbutils.widgets.text("FilesWritten", "","")
dbutils.widgets.get("FilesWritten")
filesWritten = getArgument("FilesWritten")

"""Status"""
dbutils.widgets.text("Status", "","")
dbutils.widgets.get("Status")
status = getArgument("Status")

"""Throughput"""
dbutils.widgets.text("Throughput", "","")
dbutils.widgets.get("Throughput")
throughput = getArgument("Throughput")

"""CarpetaOrigen"""
dbutils.widgets.text("CarpetaOrigen", "","")
dbutils.widgets.get("CarpetaOrigen")
carpetaOrigen = getArgument("CarpetaOrigen")

"""DatosLeidos"""
dbutils.widgets.text("datosleidos", "","")
dbutils.widgets.get("datosleidos")
datosLeidos = getArgument("datosleidos")

"""DatosEscritos"""
dbutils.widgets.text("datosescritos", "","")
dbutils.widgets.get("datosescritos")
datosEscritos = getArgument("datosescritos")

"""RowsCopied"""
dbutils.widgets.text("RowsCopied", "","")
dbutils.widgets.get("RowsCopied")
rowsCopied = getArgument("RowsCopied")

"""Errores"""
dbutils.widgets.text("errores", "","")
dbutils.widgets.get("errores")
errores = getArgument("errores")

"""Ruta tabla in Blob"""
dbutils.widgets.text("RutaDestinoTablaBlob", "","")
dbutils.widgets.get("RutaDestinoTablaBlob")
rutaDestinoTablaBlob = getArgument("RutaDestinoTablaBlob")
rutaDestinoTablaBlob = rutaDestinoTablaBlob.strip().replace(" ", "")

"""Ruta tabla in DataLake"""
dbutils.widgets.text("RutaDestinoTablaDataLake", "","")
dbutils.widgets.get("RutaDestinoTablaDataLake")
rutaDestinoTablaDataLake = getArgument("RutaDestinoTablaDataLake")
rutaDestinoTablaDataLake = rutaDestinoTablaDataLake.strip().replace(" ", "")

# COMMAND ----------

str_variables = 'nombre_app_origen: {0}, nombre_tbl_sub_app_origen: {1}, nombre_tbl_app_origen: {2}, CarpetaBlobTabla: {3}, SrcAplicacion: {4}, SrcSubAplicacion: {5}, CopyTime: {6}, Duration: {7}, RowsRead: {8}, FilesWritten: {9}, Status: {10}, Throughput: {11}, CarpetaOrigen: {12}, datosleidos: {13}, datosescritos: {14}, rowsCopied{15}, errores: {16}, rutaDestinoTablaBlob: {17}, rutaDestinoTablaBlob: {18}'.format(nombre_app_origen, nombre_tbl_sub_app_origen, nombre_tbl_app_origen, carpetaBlobTabla, nombre_app_origen, nombre_tbl_sub_app_origen, copyTime, duration, rowsRead, filesWritten, status, throughput, carpetaOrigen, datosLeidos, datosEscritos, rowsCopied, errores, rutaDestinoTablaBlob, rutaDestinoTablaDataLake)

# COMMAND ----------

print(str_variables)

# COMMAND ----------

# DBTITLE 1,Variables globales
zone = pytz.timezone('America/Bogota')
date = datetime.now(zone)
current_date = date.strftime("%d/%m/%Y %H:%M:%S")
current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

print(current_date)

rowKey =  nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen

# COMMAND ----------

# MAGIC %md
# MAGIC # ELIMINACION REGISTRO AUD EN TABLA (ZDR_AUDITORIA_LOG_EXTACCION_BD_VALIDATIONS_METADATA)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata;
# MAGIC select * from auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

# COMMAND ----------

'''
Descripción: Elimina registro de auditoria en la cual se registro tabla con errores de metadatos, detectados en las validaciones de 
             pre carga o pre copy al blob storage.
Autor: Juan David Escobar Escobar.
Fecha: 21/09/2020
DELETE BY PRIMARY KEY rowKey = (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
'''

if errores == "" or errores == '':

  query = 'DELETE FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata WHERE rowKey = ' + "'" + rowKey  + "'"
  print(query)
  sqlContext.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata;
# MAGIC select * from auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata

# COMMAND ----------

# MAGIC %md
# MAGIC # ACTUALIZACION REGISTRO AUD EN TABLA (ZDR_AUDITORIA_LOG_EXTACCION_BD_VALIDATIONS_METADATA)

# COMMAND ----------

rowKey =  nombre_app_origen.strip().upper() + '-' + nombre_tbl_sub_app_origen.strip().upper() + '-' + nombre_tbl_app_origen.strip().upper()
partitionKey = nombre_tbl_sub_app_origen.strip() + '-' + nombre_tbl_app_origen.strip()

data_source = [
Row(rowKey, partitionKey, nombre_app_origen, nombre_tbl_sub_app_origen, nombre_tbl_app_origen, carpetaBlobTabla, copyTime, duration, rowsRead, filesWritten, status, throughput, carpetaOrigen, datosLeidos, datosEscritos, rowsCopied, errores, rutaDestinoTablaBlob, rutaDestinoTablaDataLake, current_date)
]

schema_source = StructType([StructField("src_row_key", StringType(), True),  
                            StructField("src_partition_key", StringType(), True),
                            StructField("src_nombre_app_origen", StringType(), True),
                            StructField("src_nombre_tbl_sub_app_origen", StringType(), True),
                            StructField("src_nombre_tbl_app_origen", StringType(), True),
                            StructField("src_carpetaBlobTabla", StringType(), True),                          
                            StructField("src_copyTime", StringType(), True),
                            StructField("src_duration", StringType(), True),
                            StructField("src_rowsRead", StringType(), True),
                            StructField("src_filesWritten", StringType(), True),
                            StructField("src_status", StringType(), True),
                            StructField("src_throughput", StringType(), True),
                            StructField("src_carpetaOrigen", StringType(), True),
                            StructField("src_datosleidos", StringType(), True),
                            StructField("src_datosescritos", StringType(), True),
                            StructField("src_rowsCopied", StringType(), True),
                            StructField("src_errores", StringType(), True),
                            StructField("src_rutaDestinoTablaBlob", StringType(), True),
                            StructField("src_rutaDestinoTablaDataLake", StringType(), True),
                            StructField("src_fecha_registro", TimestampType(), True)
])

df_source = sqlContext.createDataFrame(sc.parallelize(data_source), schema_source)
df_source.registerTempTable("Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob")

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Log PosCarga: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de los las tablas extraidas de la BD y copiadas en
# MAGIC              el BLOB STORAGE en formato .parquet
# MAGIC              Se utiliza en un paso posterior a la extracción dinámica (COPY TASK) de la infromación con origen BD y destino Blob Storage (.Parquet).
# MAGIC Responsables: Juan David Escobar E.
# MAGIC Fecha Creación: 21/09/2020.
# MAGIC rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
# MAGIC */
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob;
# MAGIC CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob
# MAGIC (
# MAGIC       rowKey string,
# MAGIC       partitionKey string,
# MAGIC       nombre_app_origen string,
# MAGIC       nombre_tbl_sub_app_origen string,
# MAGIC       nombre_tbl_app_origen string,
# MAGIC       carpetaBlobTabla string,
# MAGIC       copyTime string,
# MAGIC       duration string,
# MAGIC       rowsRead string,
# MAGIC       filesWritten string,
# MAGIC       status string,
# MAGIC       throughput string,
# MAGIC       carpetaOrigen string,
# MAGIC       datosleidos string,
# MAGIC       datosescritos string,
# MAGIC       rowsCopied string,
# MAGIC       errores string,
# MAGIC       rutaDestinoTablaBlob string,
# MAGIC       rutaDestinoTablaDataLake string,
# MAGIC       fecha_registro timestamp     
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (nombre_app_origen)
# MAGIC LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob';

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob;
# MAGIC SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob

# COMMAND ----------

# DBTITLE 1,UPDATE AUD TBL
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Actualiza o inserta la tabla de log precarga "auditoria.Zdr_Auditoria_Log_Extraction_BD_BD_Tbl_In_Blob" mediante instruccion merge
# MAGIC Responsables: Juan David Escobar E.
# MAGIC Fecha Creación: 21/09/2020.
# MAGIC Fecha Modificación: 17/09/2020.
# MAGIC rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
# MAGIC Documentacion: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-update
# MAGIC */
# MAGIC 
# MAGIC MERGE INTO auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob
# MAGIC USING Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob
# MAGIC   ON auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.rowKey = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_row_key
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.nombre_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_nombre_app_origen, 
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.nombre_tbl_sub_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_nombre_tbl_sub_app_origen, 
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.nombre_tbl_app_origen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_nombre_tbl_app_origen,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.carpetaBlobTabla = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_carpetaBlobTabla,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.copyTime = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_copyTime,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.duration = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_duration,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.rowsRead = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_rowsRead,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.filesWritten = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_filesWritten,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.status = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_status,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.throughput = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_throughput,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.carpetaOrigen = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_carpetaOrigen,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.datosleidos = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_datosleidos,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.datosescritos = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_datosescritos,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.rowsCopied = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_rowsCopied,             
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.errores = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_errores,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.rutaDestinoTablaBlob = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_rutaDestinoTablaBlob,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.rutaDestinoTablaDataLake = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_rutaDestinoTablaDataLake,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob.fecha_registro = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Tbl_In_Blob.src_fecha_registro
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (rowKey, partitionKey, nombre_app_origen, nombre_tbl_sub_app_origen, nombre_tbl_app_origen, carpetaBlobTabla, copyTime, duration , rowsRead, filesWritten, status, throughput, carpetaOrigen, datosleidos, datosescritos, rowsCopied, errores, rutaDestinoTablaBlob, rutaDestinoTablaDataLake, fecha_registro) VALUES (src_row_key, src_partition_key, src_nombre_app_origen, src_nombre_tbl_sub_app_origen, src_nombre_tbl_app_origen, src_carpetaBlobTabla, src_copyTime, src_duration, src_rowsRead, src_filesWritten, src_status, src_throughput, src_carpetaOrigen, src_datosleidos, src_datosescritos, src_rowsCopied, src_errores, src_rutaDestinoTablaBlob, src_rutaDestinoTablaDataLake, src_fecha_registro)

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob;
# MAGIC SELECT * FROM auditoria.Zdr_Auditoria_Log_Extraction_BD_Tbl_In_Blob