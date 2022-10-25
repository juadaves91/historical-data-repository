# Databricks notebook source
# MAGIC %run /utilidades/notificaciones/EmailNotifierGenerico

# COMMAND ----------

permanent_table_name = "auditoria.Zdr_email_notificacion"
PathParquet = '/mnt/rdh-auditoria/results/email_notificacion'

# COMMAND ----------

def createNotified(email_df):
  email_df.write.format("parquet").mode("append").save(PathParquet)
  email_df_temp = spark.read.parquet(PathParquet)
  email_df = email_df_temp.drop_duplicates()
  email_df.write.format("parquet").mode("overwrite").save(PathParquet)

# COMMAND ----------

display(df_source)

# COMMAND ----------

createNotified(df_source)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS auditoria;
# MAGIC USE auditoria;
# MAGIC DROP TABLE IF EXISTS Zdr_email_notificacion;
# MAGIC 
# MAGIC CREATE TABLE Zdr_email_notificacion 
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/rdh-auditoria/results/email_notificacion';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Descripción: Sentencia requerida para consultar la tabla Default.parametros antes de ser actualizada.
# MAGIC --Autor: Juan Escobar
# MAGIC --Fecha: 20/05/220
# MAGIC 
# MAGIC REFRESH TABLE Default.parametros;

# COMMAND ----------

str_obj_weight_folder_ir = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='" + prm_cod_email + "' LIMIT 1").first()["Valor1"]

dic_limit_weigth_folder_ir = literal_eval(str_obj_weight_folder_ir)

# COMMAND ----------

count_parameters = 1#prm_count_load
notificacion_df = sqlContext.sql("SELECT * FROM " + permanent_table_name)

notificacion_df_pandas = notificacion_df.toPandas()

if count_parameters == notificacion_df.count():
  init_sendMessage(notificacion_df, dic_limit_weigth_folder_ir)
  dbutils.fs.rm(PathParquet, recurse=True)
