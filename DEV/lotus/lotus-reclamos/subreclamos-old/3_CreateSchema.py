# Databricks notebook source
# MAGIC %md
# MAGIC #ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"txt_NumProducto":{"label":"string","value":"string"},"key_TipoCuenta":{"label":"string","value":"string"},"num_NroCuenta":{"label":"string","value":"string"},"num_VlrTotal_1":{"label":"string","value":"string"},"num_VlrGMF":{"label":"string","value":"string"},"num_VlrComis":{"label":"string","value":"string"},"num_VlrRever":{"label":"string","value":"string"},"key_ClienteRazon":{"label":"string","value":"string"},"key_CargaClte":{"label":"string","value":"string"},"key_PGAreaAdmin":{"label":"string","value":"string"},"txt_Comentarios":{"Label":"string","value":["array"]},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"num_VlrTotal":{"label":"string","value":"string"},"key_PGTarjetas":{"Label":"string","value":["array"]},"key_Prefijo":{"label":"string","value":"string"},"num_DAbierto":{"label":"string","value":"string"},"dat_Solucionado":{"label":"string","value":"string"},"num_DV":{"label":"string","value":"string"},"txt_Radicado":{"label":"string","value":"string"},"key_Estado":{"label":"string","value":"string"},"key_Estado_1":{"label":"string","value":"string"},"nom_Responsable":{"label":"string","value":"string"},"SSREF":{"label":"string","value":"string"},"nom_Responsable_1":{"label":"string","value":"string"},"txt_CedNit":{"label":"string","value":"string"},"txt_NomCliente":{"label":"string","value":"string"},"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"txt_NumProductoCalc":{"label":"string","value":"string"},"key_CampoDilig":{"label":"string","value":"string"},"key_PGSucError":{"label":"string","value":"string"},"txt_CodPGSucError":{"label":"string","value":"string"},"num_NroCtaTercero":{"label":"string","value":"string"},"key_TipoCtaTercero":{"label":"string","value":"string"},"txt_Historia":{"Label":"string","value":["array"]}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd) 

# COMMAND ----------

subreclamos_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")
sqlContext.registerDataFrameAsTable(subreclamos_old_df, "Zdc_Lotus_SubReclamos_old_TablaIntermedia")

# COMMAND ----------

import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/subreclamos-old' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_SubReclamos_old_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"txt_NumProducto":{"label":"string","value":"string"},"key_TipoCuenta":{"label":"string","value":"string"},"num_NroCuenta":{"label":"string","value":"string"},"num_VlrTotal_1":{"label":"string","value":"string"},"num_VlrGMF":{"label":"string","value":"string"},"num_VlrComis":{"label":"string","value":"string"},"SSREF":{"label":"string","value":"string"},"key_Estado":{"label":"string","value":"string"},"num_VlrRever":{"label":"string","value":"string"},"key_ClienteRazon":{"label":"string","value":"string"},"key_CargaClte":{"label":"string","value":"string"},"key_PGAreaAdmin":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"num_VlrTotal":{"label":"string","value":"string"},"key_Prefijo":{"label":"string","value":"string"},"num_DAbierto":{"label":"string","value":"string"},"dat_Solucionado":{"label":"string","value":"string"},"num_DV":{"label":"string","value":"string"},"txt_Radicado":{"label":"string","value":"string"},"key_Estado_1":{"label":"string","value":"string"},"nom_Responsable":{"label":"string","value":"string"},"nom_Responsable_1":{"label":"string","value":"string"},"txt_CedNit":{"label":"string","value":"string"},"txt_NomCliente":{"label":"string","value":"string"},"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"txt_NumProductoCalc":{"label":"string","value":"string"},"key_CampoDilig":{"label":"string","value":"string"},"num_DVSlnado":{"label":"string","value":"string"},"key_PGSucError":{"label":"string","value":"string"},"txt_CodPGSucError":{"label":"string","value":"string"},"num_NroCtaTercero":{"label":"string","value":"string"},"key_TipoCtaTercero":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd) 

# COMMAND ----------

subreclamos_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")
sqlContext.registerDataFrameAsTable(subreclamos_old_df, "Zdc_Lotus_SubReclamos_old_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con key_PGTarjetas como String
"""key_PGTarjetas como String"""
subreclamos_old_key_PGTarjetas_string_payload =  ["{'UniversalID':'string','key_PGTarjetas':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_old_key_PGTarjetas_string_rdd = sc.parallelize(subreclamos_old_key_PGTarjetas_string_payload)
subreclamos_old_key_PGTarjetas_string_json = spark.read.json(subreclamos_old_key_PGTarjetas_string_rdd)

subreclamos_old_key_PGTarjetas_string_df = spark.read.schema(subreclamos_old_key_PGTarjetas_string_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

subreclamos_old_key_PGTarjetas_string_df = subreclamos_old_key_PGTarjetas_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_PGTarjetas.value as key_PGTarjetas")
subreclamos_old_key_PGTarjetas_string_df = subreclamos_old_key_PGTarjetas_string_df.filter(subreclamos_old_key_PGTarjetas_string_df.key_PGTarjetas.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_old_key_PGTarjetas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_string")

# COMMAND ----------

# DBTITLE 1,Esquema con key_PGTarjetas como Array
"""key_PGTarjetas como String"""
subreclamos_old_key_PGTarjetas_array_payload = ["{'UniversalID':'string','key_PGTarjetas':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_old_key_PGTarjetas_array_rdd = sc.parallelize(subreclamos_old_key_PGTarjetas_array_payload)
subreclamos_old_key_PGTarjetas_array_json = spark.read.json(subreclamos_old_key_PGTarjetas_array_rdd)

subreclamos_old_key_PGTarjetas_array_df = spark.read.schema(subreclamos_old_key_PGTarjetas_array_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_PGTarjetas.value as key_PGTarjetas")
subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.filter(col("UniversalID").isNotNull())
subreclamos_old_key_PGTarjetas_array_df = subreclamos_old_key_PGTarjetas_array_df.select("UniversalID","dat_FchCreac",array_join("key_PGTarjetas", '\r\n ').alias("key_PGTarjetas"))
sqlContext.registerDataFrameAsTable(subreclamos_old_key_PGTarjetas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_array")

# COMMAND ----------

subreclamos_old_cbTarjetas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_key_PGTarjetas_array")
sqlContext.registerDataFrameAsTable(subreclamos_old_cbTarjetas_df, "Zdc_Lotus_subreclamos_Tablaintermedia_key_PGTarjetas")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como String
"""txt_Comentarios como String"""
subreclamos_old_txt_Comentarios_string_payload =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_old_txt_Comentarios_string_rdd = sc.parallelize(subreclamos_old_txt_Comentarios_string_payload)
subreclamos_old_txt_Comentarios_string_json = spark.read.json(subreclamos_old_txt_Comentarios_string_rdd)

subreclamos_old_txt_Comentarios_string_df = spark.read.schema(subreclamos_old_txt_Comentarios_string_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

subreclamos_old_txt_Comentarios_string_df = subreclamos_old_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
subreclamos_old_txt_Comentarios_string_df = subreclamos_old_txt_Comentarios_string_df.filter(subreclamos_old_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como Array
"""txt_Comentarios como String"""
subreclamos_old_txt_Comentarios_array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_old_txt_Comentarios_array_rdd = sc.parallelize(subreclamos_old_txt_Comentarios_array_payload)
subreclamos_old_txt_Comentarios_array_json = spark.read.json(subreclamos_old_txt_Comentarios_array_rdd)

subreclamos_old_txt_Comentarios_array_df = spark.read.schema(subreclamos_old_txt_Comentarios_array_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
subreclamos_old_txt_Comentarios_array_df = subreclamos_old_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_array")

# COMMAND ----------

subreclamos_old_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Comentarios_array")
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Comentarios_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txt_Comentarios")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txt_Historia como String"""
subreclamos_old_txt_Historia_string_payload =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_old_txt_Historia_string_rdd = sc.parallelize(subreclamos_old_txt_Historia_string_payload)
subreclamos_old_txt_Historia_string_json = spark.read.json(subreclamos_old_txt_Historia_string_rdd)

subreclamos_old_txt_Historia_string_df = spark.read.schema(subreclamos_old_txt_Historia_string_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

subreclamos_old_txt_Historia_string_df = subreclamos_old_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
subreclamos_old_txt_Historia_string_df = subreclamos_old_txt_Historia_string_df.filter(subreclamos_old_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txt_Historia como String"""
subreclamos_old_txt_Historia_array_payload = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_old_txt_Historia_array_rdd = sc.parallelize(subreclamos_old_txt_Historia_array_payload)
subreclamos_old_txt_Historia_array_json = spark.read.json(subreclamos_old_txt_Historia_array_rdd)

subreclamos_old_txt_Historia_array_df = spark.read.schema(subreclamos_old_txt_Historia_array_json.schema).json("/mnt/lotus-reclamos/subreclamos-old/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
subreclamos_old_txt_Historia_array_df = subreclamos_old_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_array")

# COMMAND ----------

subreclamos_old_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_old_txt_Historia_array")
sqlContext.registerDataFrameAsTable(subreclamos_old_txt_Historia_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txt_Historia")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_ProcessOptimizado;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_SubReclamos_Old_TablaIntermedia_Aux;
# MAGIC CREATE EXTERNAL TABLE Lotus_ProcessOptimizado.Zdp_Lotus_SubReclamos_Old_TablaIntermedia_Aux
# MAGIC (
# MAGIC   UniversalID string,
# MAGIC   num_VlrTotal map<string,string>,
# MAGIC   num_VlrTotal_1 map<string,string>,
# MAGIC   num_VlrTotalDef map<string,string>,
# MAGIC   key_NumTransa map<string,string>,
# MAGIC   key_CamposTransa map<string,array<string>>,
# MAGIC   dat_FchCreac map<string,string>,
# MAGIC   key_Prefijo map<string,string>,
# MAGIC   txt_NumProducto map<string,string>,
# MAGIC   key_CamposRecl map<string,array<string>>
# MAGIC )
# MAGIC ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos-old/raw/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Aux;
# MAGIC CREATE VIEW Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Aux AS
# MAGIC SELECT UniversalID,
# MAGIC        num_VlrTotal.value AS num_VlrTotal,
# MAGIC        num_VlrTotal_1.value AS num_VlrTotal_1,
# MAGIC        num_VlrTotalDef.value AS num_VlrTotalDef,
# MAGIC      
# MAGIC         CASE WHEN (key_NumTransa.value != '' AND COALESCE(cast(key_NumTransa.value as int), 0)  >= 1) OR !array_contains(key_CamposRecl.value, '8') OR array_contains(key_CamposTransa.value, '4')  THEN 
# MAGIC              TRUE
# MAGIC            ELSE FALSE        
# MAGIC         END AS EsnVlrTotalVisible,
# MAGIC 
# MAGIC         CASE WHEN array_contains(key_CamposRecl.value, '8') OR !array_contains(key_CamposTransa.value, '4') THEN 
# MAGIC               TRUE
# MAGIC             ELSE FALSE        
# MAGIC         END AS EsVlrTotal_1Visible,
# MAGIC         
# MAGIC         CASE WHEN !array_contains(key_CamposRecl.value, '8') AND array_contains(key_CamposTransa.value, '4') THEN 
# MAGIC               TRUE
# MAGIC             ELSE FALSE        
# MAGIC         END AS EsVlrTotalDefVisible
# MAGIC  FROM 
# MAGIC   Lotus_ProcessOptimizado.Zdp_Lotus_SubReclamos_Old_TablaIntermedia_Aux;

# COMMAND ----------

# MAGIC  %sql 
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Solicitudes_Aux;
# MAGIC CREATE VIEW Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Solicitudes_Aux AS
# MAGIC   SELECT 
# MAGIC   TblIntermedia.UniversalID As UniversalID,
# MAGIC   dat_FchCreac.value As FechaCreacion, 
# MAGIC   CASE
# MAGIC         WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN ''
# MAGIC         WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN TblIntermedia.num_VlrTotalDef.value
# MAGIC         WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal_1.value || ',' || TblIntermedia.num_VlrTotalDef.value) 
# MAGIC         WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value, TblIntermedia.num_VlrTotal_1.value, TblIntermedia.num_VlrTotalDef.value)
# MAGIC         WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotal_1.value)
# MAGIC         WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotalDef.value)
# MAGIC         WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN TblIntermedia.num_VlrTotal_1.value
# MAGIC     END AS ValorTotal
# MAGIC FROM
# MAGIC       Lotus_ProcessOptimizado.Zdp_Lotus_SubReclamos_Old_TablaIntermedia_Aux AS TblIntermedia 
# MAGIC       INNER JOIN Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Aux As TblAuxIntermedia
# MAGIC       ON TblIntermedia.UniversalID = TblAuxIntermedia.UniversalID;

# COMMAND ----------

# MAGIC %md
# MAGIC #ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/subreclamos-old/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC #ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/subreclamos-old/5_LoadSchema $from_create_schema = True