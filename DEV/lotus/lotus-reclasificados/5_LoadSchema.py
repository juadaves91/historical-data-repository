# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

# DBTITLE 1,Parámetros de entrada
"""Archivo JSON a validar"""
dbutils.widgets.text("from_create_schema", "","")
dbutils.widgets.get("from_create_schema")
from_create_schema = str(getArgument("from_create_schema"))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC def exc_bck(from_create_schema):  
# MAGIC   if from_create_schema == '' or from_create_schema == 'False':
# MAGIC     return False
# MAGIC   elif from_create_schema == 'True':
# MAGIC     return True

# COMMAND ----------

# MAGIC %python
# MAGIC from_create_schema_aux = str(getArgument("from_create_schema")).strip()
# MAGIC from_create_schema = exc_bck(from_create_schema_aux)

# COMMAND ----------

# MAGIC %python
# MAGIC print(from_create_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Reclasificados;

# COMMAND ----------

# DBTITLE 1,Panel Principal
if from_create_schema:
  panel_principal_reclasificados.repartition("anio","mes"
                                             ).write.format("delta"
                                             ).mode("overwrite"
                                             ).partitionBy("anio","mes"
                                             ).save("/mnt/lotus-reclasificados/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Principal 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclasificados/results/panel_principal/';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
if from_create_schema:
  panel_detalle_reclasificados.repartition("anio","mes"
                                           ).write.format("delta"
                                           ).mode("overwrite"
                                           ).partitionBy("anio","mes"
                                           ).save("/mnt/lotus-reclasificados/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Detalle
# MAGIC using DELTA
# MAGIC LOCATION '/mnt/lotus-reclasificados/results/panel_detalle/';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Hijos
if from_create_schema:
  panel_hijos_reclasificados.repartition("anio","mes"
                                         ).write.format("delta"
                                         ).mode("overwrite"
                                         ).partitionBy("anio","mes"
                                         ).save("/mnt/lotus-reclasificados/results/panel_hijos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Hijos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Hijos
# MAGIC using DELTA
# MAGIC LOCATION '/mnt/lotus-reclasificados/results/panel_hijos/';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Panel_Hijos ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Masivos
if from_create_schema:
  masivos_reclasificados_df.repartition("anio","mes"
                                        ).write.format("delta"
                                        ).mode("overwrite"
                                        ).partitionBy("anio","mes"
                                        ).save("/mnt/lotus-reclasificados/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclasificados.Zdr_Lotus_Reclasificados_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclasificados/results/masivos/';
