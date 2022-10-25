# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# DBTITLE 1,Import Libs
import distutils.util

# COMMAND ----------

# DBTITLE 1,Parámetros de entrada
"""Archivo JSON a validar"""
dbutils.widgets.text("from_create_schema", "","")
dbutils.widgets.get("from_create_schema")
from_create_schema = str(getArgument("from_create_schema"))

print(str(from_create_schema))

# COMMAND ----------

def exc_bck(from_create_schema):  
  if from_create_schema == '' or from_create_schema == 'False':
    return False
  elif from_create_schema == 'True':
    return True

# COMMAND ----------

from_create_schema_aux = str(getArgument("from_create_schema")).strip()
from_create_schema = exc_bck(from_create_schema_aux)

# COMMAND ----------

print(from_create_schema)

# COMMAND ----------

# DBTITLE 1,Panel Principal
if origen == "":
  if from_create_schema:
    panel_principal_indicadores.write.format("delta").mode("overwrite").partitionBy("anio","mes").save("/mnt/dbbicorporativo-indicadores/results/panel_principal")
  
  sqlContext.sql("CREATE DATABASE IF NOT EXISTS Dbbicorporativo_Indicadores")
  
  sqlContext.sql("DROP TABLE IF EXISTS Dbbicorporativo_Indicadores.Zdr_Dbbicorporativo_Indicadores_Panel_Principal")
  sqlContext.sql("CREATE TABLE Dbbicorporativo_Indicadores.Zdr_Dbbicorporativo_Indicadores_Panel_Principal \
  USING DELTA \
  LOCATION '/mnt/dbbicorporativo-indicadores/results/panel_principal/'")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
if origen == "":
  if from_create_schema:
    panel_detalle_indicadores.write.format("delta").mode("overwrite").partitionBy("anio","mes").save("/mnt/dbbicorporativo-indicadores/results/panel_detalle")
  sqlContext.sql("DROP TABLE IF EXISTS Dbbicorporativo_Indicadores.Zdr_Dbbicorporativo_Indicadores_Panel_Detalle")
  sqlContext.sql("CREATE TABLE Dbbicorporativo_Indicadores.Zdr_Dbbicorporativo_Indicadores_Panel_Detalle \
  USING DELTA \
  LOCATION '/mnt/dbbicorporativo-indicadores/results/panel_detalle'")
  
  sqlContext.sql("OPTIMIZE Dbbicorporativo_Indicadores.Zdr_Dbbicorporativo_Indicadores_Panel_Detalle ZORDER BY (IdRegistro)")
