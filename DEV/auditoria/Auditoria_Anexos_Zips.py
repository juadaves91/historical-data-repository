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
PathParquet = '/mnt/rdh-auditoria/results/lotus_auditoria_anexos_zip'

high_date_aux = datetime(9999, 12, 31, 00, 00, 00)
high_date = high_date_aux.strftime("%d/%m/%Y %H:%M:%S")

high_date = datetime.strptime(high_date, "%d/%m/%Y %H:%M:%S")
current_date = datetime.strptime(current_date, "%d/%m/%Y %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Create the target table database
# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se utiliza una tabla de log de auditoria para llevar la trazabilidad de los zips que no se encuentran relacionados con algun UniversalID del los JSON
# MAGIC Columnas:
# MAGIC   application: Nombre de la aplicación a ejecutar
# MAGIC   application_sub: Aplicación con subcarpeta, en caso que contenga. EJ: Pedidos-Pedidos, Pedidos-Subpedidos
# MAGIC   name_zip: Nombre del anexo .zip que no encontro en los JSON de la aplicación
# MAGIC   is_current: Variable que indica que si registro se encuentra activo
# MAGIC   is_deleted: Variable que indica si el registro se encuentra eliminado
# MAGIC   start_date: Fecha en que se almaceno el registro
# MAGIC   end_date:: Fecha en que se da cierre del registro
# MAGIC 
# MAGIC Responsables: Diego Alexander Velasco.
# MAGIC Fecha Creación: 07/04/2020
# MAGIC Fecha Modificación: 01/04/2020
# MAGIC */
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS auditoria.Zdr_lotus_auditoria_anexos_zip;
# MAGIC CREATE EXTERNAL TABLE auditoria.Zdr_lotus_auditoria_anexos_zip
# MAGIC (
# MAGIC     application string,
# MAGIC     application_sub string,
# MAGIC     name_zip string,
# MAGIC     length string,
# MAGIC     is_current boolean,
# MAGIC     is_deleted boolean,
# MAGIC     start_date timestamp,
# MAGIC     end_date timestamp
# MAGIC )
# MAGIC LOCATION '/mnt/rdh-auditoria/results/lotus_auditoria_anexos_zip';

# COMMAND ----------

# DBTITLE 1,Create the target data frame
df_aud = sqlContext.sql('SELECT * FROM auditoria.Zdr_lotus_auditoria_anexos_zip')

# COMMAND ----------

# DBTITLE 1,Create merge data frame
# Prepare for merge - Added effective and end date
df_source_new = df_source.withColumn('src_start_date', lit(current_date)).withColumn('src_end_date', lit(high_date))

# FULL Merge, join on key column and also high date column to make only join to the latest records
df_merge = df_aud.join(df_source_new, (df_source_new.src_name_zip == df_aud.name_zip) & (df_source_new.src_end_date == df_aud.end_date), how='fullouter')

# COMMAND ----------

# DBTITLE 1,Merge actions
# Derive new column to indicate the action
df_merge = df_merge.withColumn('action', when(((df_merge.name_zip != df_merge.src_name_zip) & (df_merge.length != df_merge.src_length) & (df_merge.application == application)), 'UPSERT')
                                        .when(((df_merge.src_name_zip.isNull() & df_merge.is_current) & (df_merge.application == application)), 'DELETE')
                                        .when(df_merge.name_zip.isNull(), 'INSERT')
                                        .otherwise('NOACTION'))

# COMMAND ----------

# DBTITLE 1,Implement the SCD type 2 actions
# Generate the new data frames based on action code
column_names = ['application', 'application_sub', 'name_zip', 'src_length', 'is_current','is_deleted', 'start_date', 'end_date']

# For records that needs no action 
df_merge_p1 = df_merge.filter(df_merge.action == 'NOACTION').select(column_names)

df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.src_application.alias('application'),
                                                                  df_merge.src_application_sub.alias('application_sub'),
                                                                  df_merge.src_name_zip.alias('name_zip'),
                                                                  df_merge.src_length.alias('length'),
                                                                  lit(True).alias('is_current'),
                                                                  lit(False).alias('is_deleted'),
                                                                  df_merge.src_start_date.alias('start_date'),
                                                                  df_merge.src_end_date.alias('end_date'))

df_merge_p3 = df_merge.filter(df_merge.action == 'DELETE').select(column_names).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(True)).withColumn('end_date', lit(current_date))

df_merge_p4_1 = df_merge.filter(df_merge.action == 'UPSERT').select(df_merge.src_application.alias('application'),
                                                                    df_merge.src_application_sub.alias('application_sub'),
                                                                    df_merge.src_name_zip.alias('name_zip'),
                                                                    df_merge.src_length.alias('length'),
                                                                    lit(True).alias('is_current'),
                                                                    lit(False).alias('is_deleted'),
                                                                    df_merge.src_start_date.alias('start_date'),
                                                                    df_merge.src_end_date.alias('end_date'))

df_merge_p4_2 = df_merge.filter(df_merge.action == 'UPSERT').withColumn('end_date', df_merge.src_start_date).withColumn('is_current', lit(False)).withColumn('is_deleted', lit(False)).select(column_names)

# COMMAND ----------

# DBTITLE 1,Union the data frames and set DB
# Union all records together     
df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3).unionAll(df_merge_p4_1).unionAll(df_merge_p4_2)

sqlContext.registerDataFrameAsTable(df_merge_final, "Zdr_lotus_auditoria_anexos_zip_Temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE Zdr_lotus_auditoria_anexos_zip_Temp;
# MAGIC INSERT OVERWRITE TABLE auditoria.Zdr_lotus_auditoria_anexos_zip
# MAGIC SELECT * FROM Zdr_lotus_auditoria_anexos_zip_Temp;
