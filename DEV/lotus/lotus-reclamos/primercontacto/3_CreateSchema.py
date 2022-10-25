# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"cbResponsable":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"dtFechaCreacion":{"label":"string","value":"string"},"TXTSUCCAPTURA":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtCodCausalidad":{"label":"string","value":"string"},"txtRadicado":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"cbSucCaptura":{"label":"string","value":"string"},"txthistoria":{"Label":"string","value":["array"]},"txtComentario":{"Label":"string","value":["array"]}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

primercontacto_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")
sqlContext.registerDataFrameAsTable(primercontacto_df, "Zdc_Lotus_PrimerContacto_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/primercontacto'
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_PrimerContacto_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"cbResponsable":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"dtFechaCreacion":{"label":"string","value":"string"},"TXTSUCCAPTURA":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtCodCausalidad":{"label":"string","value":"string"},"txtRadicado":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"cbSucCaptura":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

primercontacto_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")
sqlContext.registerDataFrameAsTable(primercontacto_df, "Zdc_Lotus_PrimerContacto_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txthistoria como String"""
primercontacto_txthistoria_string_payload =  ["{'UniversalID':'string','txtHistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
primercontacto_txthistoria_string_rdd = sc.parallelize(primercontacto_txthistoria_string_payload)
primercontacto_txthistoria_string_json = spark.read.json(primercontacto_txthistoria_string_rdd)

primercontacto_txthistoria_string_df = spark.read.schema(primercontacto_txthistoria_string_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")

primercontacto_txthistoria_string_df = primercontacto_txthistoria_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txthistoria")
primercontacto_txthistoria_string_df = primercontacto_txthistoria_string_df.na.fill('')
primercontacto_txthistoria_string_df = primercontacto_txthistoria_string_df.filter(primercontacto_txthistoria_string_df.txthistoria.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txthistoria como String"""
primercontacto_txthistoria_array_payload = ["{'UniversalID':'string','txtHistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
primercontacto_txthistoria_array_rdd = sc.parallelize(primercontacto_txthistoria_array_payload)
primercontacto_txthistoria_array_json = spark.read.json(primercontacto_txthistoria_array_rdd)

primercontacto_txthistoria_array_df = spark.read.schema(primercontacto_txthistoria_array_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")

from pyspark.sql.functions import col,array_join
primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txthistoria")
primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.filter(col("UniversalID").isNotNull())
primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.select("UniversalID","dtFechaCreacion",array_join("txthistoria", '\r\n ').alias("txthistoria"))
primercontacto_txthistoria_array_df = primercontacto_txthistoria_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_array")

# COMMAND ----------

primercontacto_txthistoria_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txthistoria_array")
sqlContext.registerDataFrameAsTable(primercontacto_txthistoria_df, "Zdc_Lotus_primercontacto_Tablaintermedia_txthistoria")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como String
"""txtComentario como String"""
primercontacto_txtComentario_string_payload =  ["{'UniversalID':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
primercontacto_txtComentario_string_rdd = sc.parallelize(primercontacto_txtComentario_string_payload)
primercontacto_txtComentario_string_json = spark.read.json(primercontacto_txtComentario_string_rdd)

primercontacto_txtComentario_string_df = spark.read.schema(primercontacto_txtComentario_string_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")

primercontacto_txtComentario_string_df = primercontacto_txtComentario_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
primercontacto_txtComentario_string_df = primercontacto_txtComentario_string_df.na.fill('')
primercontacto_txtComentario_string_df = primercontacto_txtComentario_string_df.filter(primercontacto_txtComentario_string_df.txtComentario.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como Array
"""txtComentario como String"""
primercontacto_txtComentario_array_payload = ["{'UniversalID':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
primercontacto_txtComentario_array_rdd = sc.parallelize(primercontacto_txtComentario_array_payload)
primercontacto_txtComentario_array_json = spark.read.json(primercontacto_txtComentario_array_rdd)

primercontacto_txtComentario_array_df = spark.read.schema(primercontacto_txtComentario_array_json.schema).json("/mnt/lotus-reclamos/primercontacto/raw/")

from pyspark.sql.functions import col,array_join
primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.filter(col("UniversalID").isNotNull())
primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.select("UniversalID","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
primercontacto_txtComentario_array_df = primercontacto_txtComentario_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_array")

# COMMAND ----------

primercontacto_txtComentario_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_string \
                                                   UNION \
                                                  SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_txtComentario_array")

sqlContext.registerDataFrameAsTable(primercontacto_txtComentario_df, "Zdc_Lotus_primercontacto_Tablaintermedia_txtComentario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/primercontacto/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # CREACION TABLA MASIVOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/primercontacto/5_LoadSchema $from_create_schema = True