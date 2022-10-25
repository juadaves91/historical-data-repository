# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"txtIdDocPadre":{"label":"string","value":"string"},"txtRadicado":{"label":"string","value":"string"},"dtFechaCreacion":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"},"dtFechasTrans":{"Label":"string","value":["array"]},"txtEstadoTrans":{"Label":"string","value":["array"]},"numDiasTrans":{"Label":"string","value":["array"]},"numVencidosTrans":{"Label":"string","value":["array"]},"numContabilizaTrans":{"Label":"string","value":["array"]},"cbResponsable":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtNomCliente":{"label":"string","value":"string"},"txtPrefijo":{"label":"string","value":"string"},"txtNumProducto":{"label":"string","value":"string"},"rdTipoCuenta":{"label":"string","value":"string"},"txtNumCuenta":{"label":"string","value":"string"},"numValorPesos":{"label":"string","value":"string"},"numValorGMF":{"label":"string","value":"string"},"numValorComisiones":{"label":"string","value":"string"},"numValorReversion":{"label":"string","value":"string"},"rdClienteRazon":{"label":"string","value":"string"},"cbCampoDiligencia":{"label":"string","value":"string"},"rdCargaCliente":{"label":"string","value":"string"},"cbSucursalSobrante":{"label":"string","value":"string"},"dtFechaSobrante":{"label":"string","value":"string"},"cbSucursalError":{"label":"string","value":"string"},"txtAreaAdministrativa":{"label":"string","value":"string"},"txtCuentaTercero":{"label":"string","value":"string"},"rdTipoCuentaTercero":{"label":"string","value":"string"},"cbTarjetas":{"Label":"string","value":["array"]},"numTiempoSolucionReal":{"label":"string","value":"string"},"dtFechaSolucionReal":{"label":"string","value":"string"},"txtComentario":{"Label":"string","value":["array"]},"txtHistoria":{"Label":"string","value":["array"]}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

subreclamos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")
sqlContext.registerDataFrameAsTable(subreclamos_df, "Zdc_Lotus_SubReclamos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/subreclamos' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_SubReclamos_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"txtIdDocPadre":{"label":"string","value":"string"},"txtRadicado":{"label":"string","value":"string"},"dtFechaCreacion":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"cbResponsable":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtNomCliente":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtPrefijo":{"label":"string","value":"string"},"txtNumProducto":{"label":"string","value":"string"},"rdTipoCuenta":{"label":"string","value":"string"},"txtNumCuenta":{"label":"string","value":"string"},"numValorPesos":{"label":"string","value":"string"},"numValorGMF":{"label":"string","value":"string"},"numValorComisiones":{"label":"string","value":"string"},"numValorReversion":{"label":"string","value":"string"},"rdClienteRazon":{"label":"string","value":"string"},"cbCampoDiligencia":{"label":"string","value":"string"},"rdCargaCliente":{"label":"string","value":"string"},"cbSucursalSobrante":{"label":"string","value":"string"},"dtFechaSobrante":{"label":"string","value":"string"},"cbSucursalError":{"label":"string","value":"string"},"txtAreaAdministrativa":{"label":"string","value":"string"},"txtCuentaTercero":{"label":"string","value":"string"},"rdTipoCuentaTercero":{"label":"string","value":"string"},"numTiempoSolucionReal":{"label":"string","value":"string"},"dtFechaSolucionReal":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

subreclamos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")
sqlContext.registerDataFrameAsTable(subreclamos_df, "Zdc_Lotus_SubReclamos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con cbTarjetas como String
"""cbTarjetas como String"""
subreclamos_cbTarjetas_string_payload =  ["{'UniversalID':'string','cbTarjetas':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_cbTarjetas_string_rdd = sc.parallelize(subreclamos_cbTarjetas_string_payload)
subreclamos_cbTarjetas_string_json = spark.read.json(subreclamos_cbTarjetas_string_rdd)

subreclamos_cbTarjetas_string_df = spark.read.schema(subreclamos_cbTarjetas_string_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

subreclamos_cbTarjetas_string_df = subreclamos_cbTarjetas_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","cbTarjetas.value as cbTarjetas")
subreclamos_cbTarjetas_string_df = subreclamos_cbTarjetas_string_df.filter(subreclamos_cbTarjetas_string_df.cbTarjetas.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_cbTarjetas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_string")

# COMMAND ----------

# DBTITLE 1,Esquema con cbTarjetas como Array
"""cbTarjetas como String"""
subreclamos_cbTarjetas_array_payload = ["{'UniversalID':'string','cbTarjetas':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_cbTarjetas_array_rdd = sc.parallelize(subreclamos_cbTarjetas_array_payload)
subreclamos_cbTarjetas_array_json = spark.read.json(subreclamos_cbTarjetas_array_rdd)

subreclamos_cbTarjetas_array_df = spark.read.schema(subreclamos_cbTarjetas_array_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","cbTarjetas.value as cbTarjetas")
subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.filter(col("UniversalID").isNotNull())
subreclamos_cbTarjetas_array_df = subreclamos_cbTarjetas_array_df.select("UniversalID","dtFechaCreacion",array_join("cbTarjetas", '\r\n ').alias("cbTarjetas"))
sqlContext.registerDataFrameAsTable(subreclamos_cbTarjetas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_array")

# COMMAND ----------

subreclamo_cbTarjetas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_cbTarjetas_array")
sqlContext.registerDataFrameAsTable(subreclamo_cbTarjetas_df, "Zdc_Lotus_subreclamos_Tablaintermedia_cbTarjetas")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como String
"""txtComentario como String"""
subreclamos_txtComentario_string_payload =  ["{'UniversalID':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_txtComentario_string_rdd = sc.parallelize(subreclamos_txtComentario_string_payload)
subreclamos_txtComentario_string_json = spark.read.json(subreclamos_txtComentario_string_rdd)

subreclamos_txtComentario_string_df = spark.read.schema(subreclamos_txtComentario_string_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

subreclamos_txtComentario_string_df = subreclamos_txtComentario_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
subreclamos_txtComentario_string_df = subreclamos_txtComentario_string_df.filter(subreclamos_txtComentario_string_df.txtComentario.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como Array
"""txtComentario como String"""
subreclamos_txtComentario_array_payload = ["{'UniversalID':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_txtComentario_array_rdd = sc.parallelize(subreclamos_txtComentario_array_payload)
subreclamos_txtComentario_array_json = spark.read.json(subreclamos_txtComentario_array_rdd)

subreclamos_txtComentario_array_df = spark.read.schema(subreclamos_txtComentario_array_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.filter(col("UniversalID").isNotNull())
subreclamos_txtComentario_array_df = subreclamos_txtComentario_array_df.select("UniversalID","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
sqlContext.registerDataFrameAsTable(subreclamos_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_array")

# COMMAND ----------

subreclamo_txtComentario_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtComentario_array")
sqlContext.registerDataFrameAsTable(subreclamo_txtComentario_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txtComentario")

# COMMAND ----------

# DBTITLE 1,Esquema con txtHistoria como String
"""txtHistoria como String"""
subreclamos_txtHistoria_string_payload =  ["{'UniversalID':'string','txtHistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subreclamos_txtHistoria_string_rdd = sc.parallelize(subreclamos_txtHistoria_string_payload)
subreclamos_txtHistoria_string_json = spark.read.json(subreclamos_txtHistoria_string_rdd)

subreclamos_txtHistoria_string_df = spark.read.schema(subreclamos_txtHistoria_string_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

subreclamos_txtHistoria_string_df = subreclamos_txtHistoria_string_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
subreclamos_txtHistoria_string_df = subreclamos_txtHistoria_string_df.filter(subreclamos_txtHistoria_string_df.txtHistoria.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subreclamos_txtHistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtHistoria como Array
"""txtHistoria como String"""
subreclamos_txtHistoria_array_payload = ["{'UniversalID':'string','txtHistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
subreclamos_txtHistoria_array_rdd = sc.parallelize(subreclamos_txtHistoria_array_payload)
subreclamos_txtHistoria_array_json = spark.read.json(subreclamos_txtHistoria_array_rdd)

subreclamos_txtHistoria_array_df = spark.read.schema(subreclamos_txtHistoria_array_json.schema).json("/mnt/lotus-reclamos/subreclamos/raw/")

from pyspark.sql.functions import col,array_join
subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.selectExpr("UniversalID","dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.filter(col("UniversalID").isNotNull())
subreclamos_txtHistoria_array_df = subreclamos_txtHistoria_array_df.select("UniversalID","dtFechaCreacion",array_join("txtHistoria", '\r\n ').alias("txtHistoria"))
sqlContext.registerDataFrameAsTable(subreclamos_txtHistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_array")

# COMMAND ----------

subreclamo_txtHistoria_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_subreclamos_txtHistoria_array")
sqlContext.registerDataFrameAsTable(subreclamo_txtHistoria_df, "Zdc_Lotus_subreclamos_Tablaintermedia_txtHistoria")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_ProcessOptimizado;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_SubReclamos_TablaIntermedia_Aux;
# MAGIC CREATE EXTERNAL TABLE Lotus_ProcessOptimizado.Zdc_Lotus_SubReclamos_TablaIntermedia_Aux
# MAGIC (
# MAGIC   UniversalID string,
# MAGIC   dtFechasTrans map<string, array<string>>,
# MAGIC   txtEstadoTrans map<string, array<string>>,
# MAGIC   numDiasTrans map<string, array<string>>,
# MAGIC   numVencidosTrans map<string, array<string>>,
# MAGIC   dtFechaCreacion map<string,string>,
# MAGIC   numContabilizaTrans map<string, array<string>>,
# MAGIC   numTotVencidos map<string,string>,
# MAGIC   numTotContabiliza map<string,string>,
# MAGIC   numTotDias map<string,string>
# MAGIC )
# MAGIC ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# MAGIC LOCATION '/mnt/lotus-reclamos/subreclamos/raw/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/subreclamos/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ##ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/subreclamos/5_LoadSchema $from_create_schema = True