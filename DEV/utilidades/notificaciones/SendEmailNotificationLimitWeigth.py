# Databricks notebook source
"""Código Parametro contenido correo"""
dbutils.widgets.text("prm_cod_email", "","")
dbutils.widgets.get("prm_cod_email")
prm_cod_email = getArgument("prm_cod_email")

# COMMAND ----------

# MAGIC %run /utilidades/notificaciones/EmailNotifierGenerico

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

df = pd.DataFrame ({}, columns = []) 

init_sendMessage(df, dic_limit_weigth_folder_ir)
