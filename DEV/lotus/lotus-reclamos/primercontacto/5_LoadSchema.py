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
"""Dataframe Panel Principal Primer Contacto"""
if from_create_schema:
  panel_principal_primercontacto.repartition("anio", "mes"
                                            ).write.format("delta"
                                            ).mode("Overwrite"
                                            ).partitionBy("anio","mes"
                                            ).save("/mnt/lotus-reclamos/primercontacto/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_PrimerContacto_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_PrimerContacto_Panel_Principal 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/primercontacto/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
"""Dataframe Detalle Primer Contacto"""
if from_create_schema:
  panel_detalle_primercontacto.repartition("anio", "mes"
                                          ).write.format("delta"
                                          ).mode("Overwrite"
                                          ).partitionBy("anio","mes"
                                          ).save("/mnt/lotus-reclamos/primercontacto/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_PrimerContacto_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_PrimerContacto_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/primercontacto/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.zdr_lotus_PrimerContacto_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

if from_create_schema:
  masivos_primercontacto_df.repartition("anio", "mes"
                                       ).write.format("delta"
                                       ).mode("overwrite"
                                       ).partitionBy("anio","mes"
                                       ).save("/mnt/lotus-reclamos/primercontacto/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_PrimerContacto_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_PrimerContacto_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/primercontacto/results/masivos';