# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

# DBTITLE 1,Parámetros de entrada
"""Archivo JSON a validar"""
dbutils.widgets.text("from_create_schema", "","")
dbutils.widgets.get("from_create_schema")
from_create_schema = bool(getArgument("from_create_schema"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Reclamos;

# COMMAND ----------

# DBTITLE 1,Panel Principal
"""Dataframe Panel Principal Reclamos"""
if from_create_schema:
  panel_principal_reclamos.repartition("anio", "mes"
                                      ).write.format("delta"
                                      ).mode("Overwrite"
                                      ).partitionBy("anio","mes"
                                      ).save("/mnt/lotus-reclamos/reclamos/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Principal
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/results/panel_principal/';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
"""Dataframe Panel Detalle Reclamos"""
if from_create_schema:
  panel_detalle_reclamos.repartition("anio", "mes"
                                    ).write.format("delta"
                                    ).mode("Overwrite"
                                    ).partitionBy("anio","mes"
                                    ).save("/mnt/lotus-reclamos/reclamos/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Trasacciones
"""Dataframe Panel Transacciones Reclamos"""
if from_create_schema:
  panel_transacciones_reclamos.repartition("anio", "mes"
                                          ).write.format("delta"
                                          ).mode("Overwrite"
                                          ).partitionBy("anio","mes"
                                          ).save("/mnt/lotus-reclamos/reclamos/results/panel_transacciones")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Transaccion;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Transaccion
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/results/panel_transacciones';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Transaccion ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Historia
"""Dataframe Panel Detalle catalogo Historia"""
if from_create_schema:
  panel_historia_reclamos.repartition("anio", "mes"
                                     ).write.format("delta"
                                     ).mode("Overwrite"
                                     ).partitionBy("anio","mes"
                                     ).save("/mnt/lotus-reclamos/reclamos/results/panel_historia")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Historia;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Historia
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/results/panel_historia';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Reclamos_Panel_Historia ZORDER BY (UniversalId)

# COMMAND ----------

"""Dataframe Panel Masivos"""
if from_create_schema:  
  masivos_reclamos_df.repartition("anio", "mes"
                                 ).write.format("delta"
                                 ).mode("overwrite"
                                 ).partitionBy("anio","mes"
                                 ).save("/mnt/lotus-reclamos/reclamos/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Reclamos_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Reclamos_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/results/masivos/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux;
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux_1