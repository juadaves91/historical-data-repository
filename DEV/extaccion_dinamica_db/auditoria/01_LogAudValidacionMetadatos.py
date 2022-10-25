# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC # REGISTRO LOG AUD -  VALIDACION METADATOS USUARIO EXTRACCION Y CREACION DE TABLAS METADATOS

# COMMAND ----------

# DBTITLE 1,Importación de librerías
import pandas as pd
from pyspark.sql.functions import lit, when
from pyspark.sql.types import StructType, StructField, StringType, DataType, TimestampType
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import Row
import json
from datetime import datetime
import pytz

# COMMAND ----------

'\nlist_result_validations_metadata = "[{"dic_result_metadata_cout_rows":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_valid":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_count_rows_current_tbl":{"is_valid":false,"msg_error":"La cantidad de registros de la tabla: HISDATOSEMPLEO no coinciden, CantidadRegistros Formato_ArchivosFuente_Generico.xlsx: 99999, cantidad registros hallados en la BD: 32533"}}]"\n\nnombre_app_origen = SEMI-H\nnombre_bd_origen = SEMIHDDB\nnombre_esquema_tbl_origen = SCHSEMIH\nnombre_tbl_app_origen = HISACUMANOCONCEPTO\nnombre_tbl_sub_app_origen = PEOPLESOFT\nnombre_tipo_bd_origen = ORACLE\n'

# COMMAND ----------

# DBTITLE 1,EJEMPLO EJECUCIÓN SCRIPT
"""
list_result_validations_metadata = "[{"dic_result_metadata_cout_rows":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_valid":{"is_valid":true,"msg_error":""}},{"dic_result_metadata_count_rows_current_tbl":{"is_valid":false,"msg_error":"La cantidad de registros de la tabla: HISDATOSEMPLEO no coinciden, CantidadRegistros Formato_ArchivosFuente_Generico.xlsx: 99999, cantidad registros hallados en la BD: 32533"}}]"

nombre_app_origen = SEMI-H
nombre_bd_origen = SEMIHDDB
nombre_esquema_tbl_origen = SCHSEMIH
nombre_tbl_app_origen = HISACUMANOCONCEPTO
nombre_tbl_sub_app_origen = PEOPLESOFT
nombre_tipo_bd_origen = ORACLE
"""

# COMMAND ----------

# DBTITLE 1,Mapeo de variables
"""Path Ruta de la Aplicacion"""

# Lista de errores Metadatos.
# - err_number_cols_table string
# - err_metadata string
# - err_number_rows_table string
dbutils.widgets.text("list_result_validations_metadata", "","")
dbutils.widgets.get("list_result_validations_metadata")
list_result_validations_metadata = getArgument("list_result_validations_metadata")

# BD.
dbutils.widgets.text("nombre_bd_origen", "","")
dbutils.widgets.get("nombre_bd_origen")
nombre_bd_origen = getArgument("nombre_bd_origen")

# TipoBD.
dbutils.widgets.text("nombre_tipo_bd_origen", "","")
dbutils.widgets.get("nombre_tipo_bd_origen")
nombre_tipo_bd_origen = getArgument("nombre_tipo_bd_origen")

# APP.
dbutils.widgets.text("nombre_app_origen", "","")
dbutils.widgets.get("nombre_app_origen")
nombre_app_origen = getArgument("nombre_app_origen")

# SUB-APP.
dbutils.widgets.text("nombre_tbl_sub_app_origen", "","")
dbutils.widgets.get("nombre_tbl_sub_app_origen")
nombre_tbl_sub_app_origen = getArgument("nombre_tbl_sub_app_origen")

# Esquema
dbutils.widgets.text("nombre_esquema_tbl_origen", "","")
dbutils.widgets.get("nombre_esquema_tbl_origen")
nombre_esquema_tbl_origen = getArgument("nombre_esquema_tbl_origen")

# Tabla actual a extraer de la BD.
dbutils.widgets.text("nombre_tbl_app_origen", "","")
dbutils.widgets.get("nombre_tbl_app_origen")
nombre_tbl_app_origen = getArgument("nombre_tbl_app_origen")


# COMMAND ----------

print(list_result_validations_metadata)


# COMMAND ----------

print(nombre_bd_origen)

# COMMAND ----------

print(nombre_tipo_bd_origen)

# COMMAND ----------

print(nombre_app_origen)

# COMMAND ----------

print(nombre_tbl_sub_app_origen)

# COMMAND ----------

print(nombre_esquema_tbl_origen)

# COMMAND ----------

print(nombre_tbl_app_origen)

# COMMAND ----------

# DBTITLE 1,Variables globales
zone = pytz.timezone('America/Bogota')
date = datetime.now(zone)
current_date = date.strftime("%d/%m/%Y %H:%M:%S")
current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

# Conversion a objeto list_esquema_tbl_origen to list-json[]
list_result_esquema_tbl_origen = json.loads(list_result_validations_metadata)

error_metadata_cout_rows = list_result_esquema_tbl_origen[0]['dic_result_metadata_cout_rows']['msg_error']
error_metadata_valid = list_result_esquema_tbl_origen[1]['dic_result_metadata_valid']['msg_error']
error_metadata_count_rows_current_tbl = list_result_esquema_tbl_origen[2]['dic_result_metadata_count_rows_current_tbl']['msg_error']

print(type(current_date), current_date, error_metadata_cout_rows, error_metadata_valid, error_metadata_count_rows_current_tbl)

# COMMAND ----------

print(nombre_bd_origen, nombre_tipo_bd_origen, nombre_app_origen, nombre_tbl_sub_app_origen, nombre_esquema_tbl_origen, nombre_tbl_app_origen)

# COMMAND ----------

# DBTITLE 1,CREACION TABLA AUD (Zdr_Auditoria_Log_Load_Data_File_DL- Create the target table database)
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Log PreCarga: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de los errores y hallazgos de la metadata registrada
# MAGIC              por el usuario dueño de la información y la fuente de datos tipo BD.
# MAGIC              Se utiliza en un paso previo a la extracción dinámica de la infromación con origen BD y destino Blob Storage (.Parquet).
# MAGIC Responsables: Juan David Escobar E.
# MAGIC Fecha Creación: 31/08/2020.
# MAGIC Fecha Modificación: 17/09/2020.
# MAGIC rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
# MAGIC */
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata;
# MAGIC CREATE TABLE auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata
# MAGIC (
# MAGIC     rowKey string,
# MAGIC     partitionKey string,
# MAGIC     nombre_bd_origen string,
# MAGIC     nombre_tipo_bd_origen string,
# MAGIC     nombre_app_origen string,
# MAGIC     nombre_sub_app_origen string,
# MAGIC     nombre_esquema_tbl_origen string,
# MAGIC     nombre_tbl_app_origen string,
# MAGIC     err_number_cols_table string,
# MAGIC     err_metadata string,
# MAGIC     err_number_rows_table string,
# MAGIC     fecha_registro timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (nombre_app_origen)
# MAGIC LOCATION '/mnt/rdh-auditoria/results/Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata';

# COMMAND ----------

print(type(current_date), current_date)

# COMMAND ----------

# DBTITLE 1,Create source data frame
rowKey =  nombre_app_origen.strip().upper() + '-' + nombre_tbl_sub_app_origen.strip().upper() + '-' + nombre_tbl_app_origen.strip().upper()
partitionKey = nombre_tbl_sub_app_origen.strip().upper() + '-' + nombre_tbl_app_origen.strip().upper()

data_source = [
Row(rowKey, partitionKey, nombre_bd_origen, nombre_tipo_bd_origen, nombre_app_origen, nombre_tbl_sub_app_origen, nombre_esquema_tbl_origen, nombre_tbl_app_origen, error_metadata_cout_rows, error_metadata_valid,  error_metadata_count_rows_current_tbl, current_date)
]

schema_source = StructType([StructField("src_row_key", StringType(), True),  
                            StructField("src_partition_key", StringType(), True),
                            StructField("nombre_bd_origen", StringType(), True),
                            StructField("nombre_tipo_bd_origen", StringType(), True),
                            StructField("nombre_app_origen", StringType(), True),
                            StructField("nombre_sub_app_origen", StringType(), True),
                            StructField("nombre_esquema_tbl_origen", StringType(), True),
                            StructField("nombre_tbl_app_origen", StringType(), True),
                            StructField("err_number_cols_table", StringType(), True),
                            StructField("err_metadata", StringType(), True),
                            StructField("err_number_rows_table", StringType(), True),
                            StructField("fecha_registro", TimestampType(), True)
])

df_source = sqlContext.createDataFrame(sc.parallelize(data_source), schema_source)
df_source.registerTempTable("Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata")

# COMMAND ----------

display(df_source)

# COMMAND ----------

df_source.count()

# COMMAND ----------

# DBTITLE 1,UPDATE AUD TBL
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Actualiza o inserta la tabla de log precarga "auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata" mediante instruccion merge
# MAGIC Responsables: Juan David Escobar E.
# MAGIC Fecha Creación: 21/09/2020.
# MAGIC Fecha Modificación: 17/09/2020.
# MAGIC rowKey = llave primaria (nombre_app_origen + '-' + nombre_tbl_sub_app_origen + '-' + nombre_tbl_app_origen)
# MAGIC Documentacion: https://docs.microsoft.com/en-us/azure/databricks/delta/delta-update
# MAGIC */
# MAGIC 
# MAGIC MERGE INTO auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata
# MAGIC USING Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata
# MAGIC   ON auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata.rowKey = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata.src_row_key
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata.err_number_cols_table = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata.err_number_cols_table, 
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata.err_metadata = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata.err_metadata, 
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata.err_number_rows_table = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata.err_number_rows_table,
# MAGIC              auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata.fecha_registro = Temp_Tbl_Src_Auditoria_Log_Extraction_BD_Validations_Metadata.fecha_registro
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (rowKey, partitionKey, nombre_bd_origen, nombre_tipo_bd_origen, nombre_app_origen, nombre_sub_app_origen, nombre_esquema_tbl_origen, nombre_tbl_app_origen, err_number_cols_table, err_metadata, err_number_rows_table, fecha_registro) VALUES (src_row_key, src_partition_key, nombre_bd_origen, nombre_tipo_bd_origen, nombre_app_origen, nombre_sub_app_origen, nombre_esquema_tbl_origen, nombre_tbl_app_origen, err_number_cols_table, err_metadata,  err_number_rows_table, fecha_registro)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC refresh table auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata;
# MAGIC select * from auditoria.Zdr_Auditoria_Log_Extraction_BD_Validations_Metadata