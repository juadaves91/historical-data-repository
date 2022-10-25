# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

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
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_RequerimientosLegales;

# COMMAND ----------

# DBTITLE 1,Panel Principal
"""Dataframe Panel Principal Requerimientos-Legales"""
if from_create_schema:
  panel_principal_req_leg.repartition("anio", "mes"
                                      ).write.format("Delta"
                                      ).mode("Overwrite"
                                      ).partitionBy("anio", "mes"
                                      ).save("/mnt/lotus-requerimientos-legales/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_RequerimientosLegales.Zdr_Lotus_Requerimientos_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_RequerimientosLegales.Zdr_Lotus_Requerimientos_Panel_Principal
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-requerimientos-legales/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
"""Dataframe Panel Detalle Requerimientos-Legales"""
if from_create_schema:
  panel_detalle_req_leg.repartition("anio", "mes"
                                    ).write.format("Delta"
                                    ).mode("Overwrite"
                                    ).partitionBy("anio", "mes"
                                    ).save("/mnt/lotus-requerimientos-legales/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_RequerimientosLegales.Zdr_Lotus_Requerimientos_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_RequerimientosLegales.Zdr_Lotus_Requerimientos_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-requerimientos-legales/results/panel_detalle/';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_RequerimientosLegales.Zdr_Lotus_Requerimientos_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Masivos
"""Dataframe Panel Masivos Requerimientos-Legales"""
if from_create_schema:
  masivos_req_legales_df.repartition("anio", "mes"
                                     ).write.format("Delta"
                                     ).mode("Overwrite"
                                     ).partitionBy("anio", "mes"
                                     ).save("/mnt/lotus-requerimientos-legales/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_RequerimientosLegales.Zdr_Lotus_Req_Legales_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_RequerimientosLegales.Zdr_Lotus_Req_Legales_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-requerimientos-legales/results/masivos/';
