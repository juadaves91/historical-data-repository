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
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Reclamos;

# COMMAND ----------

# DBTITLE 1,Panel Principal
if from_create_schema:
  panel_principal_subreclamos_old.repartition("anio","mes"
                                              ).write.format("delta"
                                              ).mode("overwrite"
                                              ).partitionBy("anio","mes"
                                              ).save("/mnt/lotus-reclamos/subreclamos-old/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_SubReclamos_Old_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_SubReclamos_Old_Panel_Principal
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
if from_create_schema:
  panel_detalle_subreclamos_old.repartition("anio","mes"
                                            ).write.format("delta"
                                            ).mode("overwrite"
                                            ).partitionBy("anio","mes"
                                            ).save("/mnt/lotus-reclamos/subreclamos-old/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_SubReclamos_Old_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_SubReclamos_Old_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.zdr_lotus_SubReclamos_Old_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Masivos
if from_create_schema:
  masivos_subreclamos_old_df.repartition("anio","mes"
                                         ).write.format("delta"
                                         ).mode("overwrite"
                                         ).partitionBy("anio","mes"
                                         ).save("/mnt/lotus-reclamos/subreclamos-old/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_subreclamos_Old_Masivos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_subreclamos_Old_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/masivos/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_SubReclamos_Old_TablaIntermedia_Aux;
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Aux;
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Solicitudes_Aux;
