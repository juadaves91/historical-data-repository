# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","attachments":["array"],"key_ViceAtdPed":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"key_Prioridad":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_ValidarResp":{"label":"string","value":"string"},"rch_DetallePed":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"rch_AnexosAdic":{"Label":"string","value":["array"]},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_RgnEmp_A":{"Label":"string","value":["array"]},"txt_UbicEmp_A":{"Label":"string","value":["array"]},"txt_ExtEmp_A":{"Label":"string","value":["array"]},"txt_EmpEnteExt":{"Label":"string","value":["array"]},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"key_Prioridad_1":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_EmpresaEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"},"SSREF":{"label":"string","value":"string"},"txt_Historia":{"Label":"string","value":["array"]}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
subpedidos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")
sqlContext.registerDataFrameAsTable(subpedidos_df, "Zdc_Lotus_Subpedidos_TablaIntermedia")

# COMMAND ----------

import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-pedidos-storage/subpedidos/' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Subpedidos_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

  dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","attachments":["array"],"key_ViceAtdPed":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"key_Prioridad":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_ValidarResp":{"label":"string","value":"string"},"rch_DetallePed":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"key_Prioridad_1":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_EmpresaEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"SSREF":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
from pyspark.sql.functions import col,array_join,substring,substring_index

subpedidos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw")
subpedidos_df = subpedidos_df.select(col("UniversalID").alias("UniversalIDRespaldo"),
                                     col("attachments").alias("attachments"),
                                     col("UniversalIDCompuesto").alias("UniversalID"),
                                     col("SSREF.value").alias("UniversalIDPadre"),
                                     col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                     col("key_NomPedido.value").alias("key_NomPedido"),
                                     col("txt_Autor.value").alias("txt_Autor"),
                                     col("txt_Creador.value").alias("txt_Creador"),
                                     col("dat_FchCreac.value").alias("dat_FchCreac"),
                                     col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                     col("key_Prioridad.value").alias("key_Prioridad"),
                                     col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                     col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                     col("txt_ValidarResp.value").alias("txt_ValidarResp"),
                                     col("rch_DetallePed.value").alias("rch_DetallePed"),
                                     col("txt_Comentarios.value").alias("txt_Comentarios"),
                                     col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                                     col("txt_Titulo1.value").alias("txt_Titulo1"),
                                     col("txt_Titulo2.value").alias("txt_Titulo2"),
                                     col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                     col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                     col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                     col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                     col("num_DVTotal.value").alias("num_DVTotal"),
                                     col("num_DVParcial.value").alias("num_DVParcial"),
                                     col("key_Prioridad_1.value").alias("key_Prioridad_1"),
                                     col("txt_Estado.value").alias("txt_Estado"),
                                     col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                     col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                     col("dat_FchMaxEstado_1.value").alias("dat_FchMaxEstado_1"),
                                     col("num_TCotizado.value").alias("num_TCotizado"),
                                     col("num_TCotizado_1.value").alias("num_TCotizado_1"),
                                     col("num_PesosCotiz.value").alias("num_PesosCotiz"),
                                     col("num_PesosCotiz_1.value").alias("num_PesosCotiz_1"),
                                     col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                     col("txt_Responsable.value").alias("txt_Responsable"),
                                     col("txt_Responsable_1.value").alias("txt_Responsable_1"),
                                     col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                     col("txt_EmpresaEmp_R.value").alias("txt_EmpresaEmp_R"),
                                     col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                     col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                     col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                     col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                     substring(substring_index('dat_FchCreac.value', ' ', 1),-4,4).cast('int').alias('anio'),
                                     substring(substring_index('dat_FchCreac.value', ' ', 1),-7,2).cast('int').alias('mes'))
                                      
sqlContext.registerDataFrameAsTable(subpedidos_df, "Zdc_Lotus_Subpedidos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_RgnEmp_A como String
"""txt_RgnEmp_A como String"""
subpedidos_txt_RgnEmp_A_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_RgnEmp_A':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subpedidos_txt_RgnEmp_A_string_rdd = sc.parallelize(subpedidos_txt_RgnEmp_A_string_payload)
subpedidos_txt_RgnEmp_A_string_json = spark.read.json(subpedidos_txt_RgnEmp_A_string_rdd)

subpedidos_txt_RgnEmp_A_string_df = spark.read.schema(subpedidos_txt_RgnEmp_A_string_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_RgnEmp_A.value as txt_RgnEmp_A")
subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.na.fill('')
subpedidos_txt_RgnEmp_A_string_df = subpedidos_txt_RgnEmp_A_string_df.filter(subpedidos_txt_RgnEmp_A_string_df.txt_RgnEmp_A.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_RgnEmp_A_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_RgnEmp_A como Array
"""txt_RgnEmp_A como Array"""
subpedidos_txt_RgnEmp_A_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_RgnEmp_A':{'Label':'string','value':['array']}}"]

sc = spark.sparkContext
subpedidos_txt_RgnEmp_A_array_rdd = sc.parallelize(subpedidos_txt_RgnEmp_A_array_payload)
subpedidos_txt_RgnEmp_A_array_json = spark.read.json(subpedidos_txt_RgnEmp_A_array_rdd)

subpedidos_txt_RgnEmp_A_array_df = spark.read.schema(subpedidos_txt_RgnEmp_A_array_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

from pyspark.sql.functions import col,array_join


subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_RgnEmp_A.value as txt_RgnEmp_A")
subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.filter(col("UniversalID").isNotNull())
subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.select("UniversalID","dat_FchCreac", array_join("txt_RgnEmp_A", '\r\n ').alias("txt_RgnEmp_A"))
subpedidos_txt_RgnEmp_A_array_df = subpedidos_txt_RgnEmp_A_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_array_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_RgnEmp_A
subpedidos_txt_RgnEmp_A_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_RgnEmp_A_string UNION SELECT * FROM Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A_array")
sqlContext.registerDataFrameAsTable(subpedidos_txt_RgnEmp_A_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_RgnEmp_A")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_UbicEmp_A como String
"""txt_UbicEmp_A como String"""
subpedidos_txt_UbicEmp_A_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_UbicEmp_A':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subpedidos_txt_UbicEmp_A_string_rdd = sc.parallelize(subpedidos_txt_UbicEmp_A_string_payload)
subpedidos_txt_UbicEmp_A_string_json = spark.read.json(subpedidos_txt_UbicEmp_A_string_rdd)

subpedidos_txt_UbicEmp_A_string_df = spark.read.schema(subpedidos_txt_UbicEmp_A_string_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_UbicEmp_A.value as txt_UbicEmp_A")
subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.na.fill('')
subpedidos_txt_UbicEmp_A_string_df = subpedidos_txt_UbicEmp_A_string_df.filter(subpedidos_txt_UbicEmp_A_string_df.txt_UbicEmp_A.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_UbicEmp_A_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_UbicEmp_A como Array
"""txt_UbicEmp_A como Array"""
subpedidos_txt_UbicEmp_A_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_UbicEmp_A':{'Label':'string','value':['array']}}"]

sc = spark.sparkContext
subpedidos_txt_UbicEmp_A_array_rdd = sc.parallelize(subpedidos_txt_UbicEmp_A_array_payload)
subpedidos_txt_UbicEmp_A_array_json = spark.read.json(subpedidos_txt_UbicEmp_A_array_rdd)

subpedidos_txt_UbicEmp_A_array_df = spark.read.schema(subpedidos_txt_UbicEmp_A_array_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

from pyspark.sql.functions import col,array_join


subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_UbicEmp_A.value as txt_UbicEmp_A")
subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.filter(col("UniversalID").isNotNull())
subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.select("UniversalID","dat_FchCreac", array_join("txt_UbicEmp_A", '\r\n ').alias("txt_UbicEmp_A"))
subpedidos_txt_UbicEmp_A_array_df = subpedidos_txt_UbicEmp_A_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_array_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_UbicEmp_A
subpedidos_txt_UbicEmp_A_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_UbicEmp_A_string UNION SELECT * FROM Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A_array")
sqlContext.registerDataFrameAsTable(subpedidos_txt_UbicEmp_A_df, "Zdc_Lotus_Subpedidos_Tablaintermedia_txt_UbicEmp_A")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txt_Historia como String"""
subpedidos_txt_Historia_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
subpedidos_txt_Historia_string_rdd = sc.parallelize(subpedidos_txt_Historia_string_payload)
subpedidos_txt_Historia_string_json = spark.read.json(subpedidos_txt_Historia_string_rdd)

subpedidos_txt_Historia_string_df = spark.read.schema(subpedidos_txt_Historia_string_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.selectExpr("UniversalIDCompuesto as UniversalID" ,"dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.na.fill('')
subpedidos_txt_Historia_string_df = subpedidos_txt_Historia_string_df.filter(subpedidos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_string_df, "Zdc_Lotus_Subpedidos_TablaIntermedia_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txt_Historia como Array"""
subpedidos_txt_Historia_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]

sc = spark.sparkContext
subpedidos_txt_Historia_array_rdd = sc.parallelize(subpedidos_txt_Historia_array_payload)
subpedidos_txt_Historia_array_json = spark.read.json(subpedidos_txt_Historia_array_rdd)

subpedidos_txt_Historia_array_df = spark.read.schema(subpedidos_txt_Historia_array_json.schema).json("/mnt/lotus-pedidos/subpedidos/raw/")

from pyspark.sql.functions import col,array_join
subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
subpedidos_txt_Historia_array_df = subpedidos_txt_Historia_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_array_df, "Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_Historia
subpedidos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Subpedidos_TablaIntermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia_array")
sqlContext.registerDataFrameAsTable(subpedidos_txt_Historia_df, "Zdc_Lotus_subpedidos_Tablaintermedia_txt_Historia")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-pedidos/subpedidos/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-pedidos/subpedidos/5_LoadSchema $from_create_schema = True