# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# DBTITLE 1,Variables de entrada
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
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_Solicitudes;

# COMMAND ----------

# DBTITLE 1,Panel Principal
if from_create_schema:
  panel_principal_solicitudes.repartition("anio","mes"
                                          ).write.format("delta"
                                          ).mode("overwrite"
                                          ).partitionBy("anio","mes"
                                          ).save("/mnt/lotus-solicitudes/results/panel_principal")

# COMMAND ----------

"""
Nota: Este código esta inactivo de forma temporal, se debe activar una vez se retome el desarrollo del append, el cual contempla la eliminación o movimiento de los json leidos en Raw a una carpeta 
de procesados o se debe considerar reprocesaro copiar los json procesados desde el blob Storage al Datalake
Fecha: 25/02/2020.

panel_principal_solicitudes = spark.read.format('Delta').load('/mnt/lotus-solicitudes/results/panel_principal')
panel_principal_solicitudes = panel_principal_solicitudes.drop_duplicates()
panel_principal_solicitudes.write.format("delta").mode("overwrite").partitionBy("anio","mes").save("/mnt/lotus-solicitudes/results/panel_principal/")
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Panel_Principal 
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-solicitudes/results/panel_principal';

# COMMAND ----------

# DBTITLE 1,Panel Detalle
if from_create_schema:
  panel_detalle_solicitudes.repartition("anio","mes"
                                        ).write.format("delta"
                                        ).mode("overwrite"
                                        ).partitionBy("anio","mes"
                                        ).save("/mnt/lotus-solicitudes/results/panel_detalle")

# COMMAND ----------

"""
Nota: Este código esta inactivo de forma temporal, se debe activar una vez se retome el desarrollo del append, el cual contempla la eliminación o movimiento de los json leidos en Raw a una carpeta 
de procesados o se debe considerar reprocesaro copiar los json procesados desde el blob Storage al Datalake
Fecha: 25/02/2020.

panel_detalle_solicitudes = spark.read.format('Delta').load('/mnt/lotus-solicitudes/results/panel_detalle')
panel_detalle_solicitudes = panel_detalle_solicitudes.drop_duplicates()
panel_detalle_solicitudes.write.format("delta").mode("overwrite").partitionBy("anio","mes").save("/mnt/lotus-solicitudes/results/panel_detalle")
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Panel_Detalle;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Panel_Detalle
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-solicitudes/results/panel_detalle';

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Panel_Detalle ZORDER BY (UniversalId)

# COMMAND ----------

# DBTITLE 1,Panel Masivos
"""Dataframe Panel Masivos"""
if from_create_schema:
  masivos_reclamos_df.repartition("anio","mes"
                                  ).write.format("delta"
                                  ).mode("overwrite"
                                  ).partitionBy("anio","mes"
                                  ).save("/mnt/lotus-solicitudes/results/masivos")

# COMMAND ----------

"""
Nota: Este código esta inactivo de forma temporal, se debe activar una vez se retome el desarrollo del append, el cual contempla la eliminación o movimiento de los json leidos en Raw a una carpeta 
de procesados o se debe considerar reprocesaro copiar los json procesados desde el blob Storage al Datalake
Fecha: 25/02/2020.

masivos_reclamos_df = spark.read.format('Delta').load('/mnt/lotus-solicitudes/results/masivos')
masivos_reclamos_df = masivos_reclamos_df.drop_duplicates()
masivos_reclamos_df.write.format("delta").mode("overwrite").partitionBy("anio","mes").save('/mnt/lotus-solicitudes/results/masivos')
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Masivos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Lotus_Solicitudes.Zdr_Lotus_Solicitudes_Masivos
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/lotus-solicitudes/results/masivos';
