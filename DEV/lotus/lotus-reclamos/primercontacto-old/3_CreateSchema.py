# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"txt_CedNit":{"label":"string","value":"string"},"nom_Responsable":{"label":"string","value":"string"},"key_Estado":{"label":"string","value":"string"},"txt_CodCausalidad":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

primercontacto_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")
sqlContext.registerDataFrameAsTable(primercontacto_old_df, "Zdc_Lotus_PrimerContacto_Old_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/primercontacto-old'
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_PrimerContacto_Old_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"txt_Comentarios":{"Label":"string","value":["array"]},"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"txt_CedNit":{"label":"string","value":"string"},"nom_Responsable":{"label":"string","value":"string"},"key_Estado":{"label":"string","value":"string"},"txt_CodCausalidad":{"label":"string","value":"string"},"txt_Historia":{"Label":"string","value":["array"]}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

primercontacto_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")
sqlContext.registerDataFrameAsTable(primercontacto_old_df, "Zdc_Lotus_PrimerContacto_Old_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como String
"""txt_Comentarios como String"""
primercontacto_old_txt_Comentarios_string_payload =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
primercontacto_old_txt_Comentarios_string_rdd = sc.parallelize(primercontacto_old_txt_Comentarios_string_payload)
primercontacto_old_txt_Comentarios_string_json = spark.read.json(primercontacto_old_txt_Comentarios_string_rdd)

primercontacto_old_txt_Comentarios_string_df = spark.read.schema(primercontacto_old_txt_Comentarios_string_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")

primercontacto_old_txt_Comentarios_string_df = primercontacto_old_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
primercontacto_old_txt_Comentarios_string_df = primercontacto_old_txt_Comentarios_string_df.filter(primercontacto_old_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como Array
"""txt_Comentarios como String"""
primercontacto_old_txt_Comentarios_array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
primercontacto_old_txt_Comentarios_array_rdd = sc.parallelize(primercontacto_old_txt_Comentarios_array_payload)
primercontacto_old_txt_Comentarios_array_json = spark.read.json(primercontacto_old_txt_Comentarios_array_rdd)

primercontacto_old_txt_Comentarios_array_df = spark.read.schema(primercontacto_old_txt_Comentarios_array_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")

from pyspark.sql.functions import col,array_join
primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
primercontacto_old_txt_Comentarios_array_df = primercontacto_old_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_array")

# COMMAND ----------

primercontacto_old_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Comentarios_array")
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Comentarios_df, "Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Comentarios")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txt_Historia como String"""
primercontacto_old_txt_Historia_string_payload =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
primercontacto_old_txt_Historia_string_rdd = sc.parallelize(primercontacto_old_txt_Historia_string_payload)
primercontacto_old_txt_Historia_string_json = spark.read.json(primercontacto_old_txt_Historia_string_rdd)

primercontacto_old_txt_Historia_string_df = spark.read.schema(primercontacto_old_txt_Historia_string_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")

primercontacto_old_txt_Historia_string_df = primercontacto_old_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
primercontacto_old_txt_Historia_string_df = primercontacto_old_txt_Historia_string_df.filter(primercontacto_old_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txt_Historia como String"""
primercontacto_old_txt_Historia_array_payload = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
primercontacto_old_txt_Historia_array_rdd = sc.parallelize(primercontacto_old_txt_Historia_array_payload)
primercontacto_old_txt_Historia_array_json = spark.read.json(primercontacto_old_txt_Historia_array_rdd)

primercontacto_old_txt_Historia_array_df = spark.read.schema(primercontacto_old_txt_Historia_array_json.schema).json("/mnt/lotus-reclamos/primercontacto-old/raw/")

from pyspark.sql.functions import col,array_join
primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
primercontacto_old_txt_Historia_array_df = primercontacto_old_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_array")

# COMMAND ----------

primercontacto_old_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_primercontacto_old_txt_Historia_array")
sqlContext.registerDataFrameAsTable(primercontacto_old_txt_Historia_df, "Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Historia")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/primercontacto-old/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # CREACION TABLA MASIVOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/primercontacto-old/5_LoadSchema $from_create_schema = True