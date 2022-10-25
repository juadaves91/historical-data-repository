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
"""Dataframe Panel Principal SubReclamos"""

if from_create_schema:
  panel_principal_subreclamos.repartition("anio","mes"
                                          ).write.format("delta"
                                          ).mode("Overwrite"
                                          ).partitionBy("anio","mes"
                                          ).save("/mnt/lotus-reclamos/subreclamos/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_SubReclamos_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_SubReclamos_Panel_Principal
# MAGIC USING DELTA
# MAGIC LOCATION 'mnt/lotus-reclamos/subreclamos/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Historia
"""Dataframe Catalogo Historia SubReclamos"""
if from_create_schema:
  panel_historia_subreclamos.repartition("anio","mes"
                                         ).write.format("delta"
                                         ).mode("Overwrite"
                                         ).partitionBy("anio","mes"
                                         ).save("/mnt/lotus-reclamos/subreclamos/results/Panel_historia")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.Zdr_Lotus_Subreclamos_Panel_Historia;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.Zdr_Lotus_Subreclamos_Panel_Historia
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos/results/Panel_historia';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.Zdr_Lotus_Subreclamos_Panel_Historia ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Detalle
"""Dataframe Detalle SubReclamos"""
if from_create_schema:
  panel_detalle_subreclamos.repartition("anio","mes"
                                        ).write.format("delta"
                                        ).mode("Overwrite"
                                        ).partitionBy("anio","mes"
                                        ).save("/mnt/lotus-reclamos/subreclamos/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_Subreclamos_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_Subreclamos_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION 'mnt/lotus-reclamos/subreclamos/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Reclamos.zdr_lotus_Subreclamos_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_SubReclamos_TablaIntermedia_Aux;

# COMMAND ----------

if from_create_schema:
  masivos_subreclamos_df.repartition("anio","mes"
                                     ).write.format("delta"
                                     ).mode("overwrite"
                                     ).partitionBy("anio","mes"
                                     ).save("/mnt/lotus-reclamos/subreclamos/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Reclamos.zdr_lotus_subreclamos_Masivos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Reclamos.zdr_lotus_subreclamos_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos/results/masivos/';
