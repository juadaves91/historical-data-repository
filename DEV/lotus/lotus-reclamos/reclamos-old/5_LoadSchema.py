# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

"""Archivo JSON a validar"""
dbutils.widgets.text("from_create_schema", "","")
dbutils.widgets.get("from_create_schema")
from_create_schema = bool(getArgument("from_create_schema"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Reclamos;

# COMMAND ----------

# DBTITLE 1,Panel Principal
if from_create_schema:
  panel_principal_reclamos_old.repartition("anio","mes"
                                           ).write.format("delta"
                                           ).mode("overwrite"
                                           ).partitionBy("anio","mes"
                                           ).save("/mnt/lotus-reclamos/reclamos-old/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Principal
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos-old/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
if from_create_schema:
  panel_detalle_reclamos_old.repartition("anio","mes"
                                         ).write.format("delta"
                                         ).mode("overwrite"
                                         ).partitionBy("anio","mes"
                                         ).save("/mnt/lotus-reclamos/reclamos-old/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos-old/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Trasacciones
if from_create_schema:
  panel_trasacciones_reclamos_old.repartition("anio","mes"
                                              ).write.format("delta"
                                              ).mode("overwrite"
                                              ).partitionBy("anio","mes"
                                              ).save("/mnt/lotus-reclamos/reclamos-old/results/panel_transacciones")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Trasacciones;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Trasacciones
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos-old/results/panel_transacciones';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Panel_Trasacciones ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Masivos
"""Dataframe Panel Masivos"""
if from_create_schema:
  masivos_reclamos_old_df.repartition("anio","mes"
                                      ).write.format("delta"
                                      ).mode("overwrite"
                                      ).partitionBy("anio","mes"
                                      ).save("/mnt/lotus-reclamos/reclamos-old/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Old_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos-old/results/masivos/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_old_TablaIntermedia_Aux;
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes_Aux;
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes;
