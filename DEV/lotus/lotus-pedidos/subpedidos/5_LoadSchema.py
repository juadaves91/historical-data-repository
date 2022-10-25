# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

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
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Pedidos;

# COMMAND ----------

# DBTITLE 1,Creación Panel Principal
if from_create_schema:
  panel_principal_subpedidos.repartition("anio","mes"
                                         ).write.format("delta"
                                         ).mode("overwrite"
                                         ).partitionBy("anio","mes"
                                         ).save("/mnt/lotus-pedidos/subpedidos/results/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Pedidos.Zdr_Lotus_Subpedidos_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Pedidos.Zdr_Lotus_Subpedidos_Panel_Principal 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-pedidos/subpedidos/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Creación Panel Detalle
if from_create_schema:
  panel_detalle_subpedidos.repartition("anio","mes"
                                       ).write.format("delta"
                                       ).mode("overwrite"
                                       ).partitionBy("anio","mes"
                                       ).save("/mnt/lotus-pedidos/subpedidos/results/panel_detalle")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Pedidos.Zdr_Lotus_Subpedidos_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Pedidos.Zdr_Lotus_Subpedidos_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-pedidos/subpedidos/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Pedidos.Zdr_Lotus_Subpedidos_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Creación Panel Masivos
"""Dataframe Panel Masivos"""
if from_create_schema:
  masivos_subpedidos_df.repartition("anio","mes"
                                    ).write.format("delta"
                                    ).mode("overwrite"
                                    ).partitionBy("anio","mes"
                                    ).save("/mnt/lotus-pedidos/subpedidos/results/masivos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Pedidos.Zdr_Lotus_SubPedidos_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Pedidos.Zdr_Lotus_SubPedidos_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-pedidos/subpedidos/results/masivos';
