# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

# DBTITLE 1,Esquema Principal
dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","attachments":["array"],"rch_DetallePed":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"key_Prioridad":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_ValidarResp":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"rch_AnexosAdic":{"label":"string","value":"string"},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_RgnEmp_A":{"label":"string","value":"string"},"txt_UbicEmp_A":{"label":"string","value":"string"},"txt_ExtEmp_A":{"label":"string","value":"string"},"txt_EmpEnteExt":{"label":"string","value":"string"},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"key_Prioridad_1":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_EmpresaEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"},"txt_Historia":{"Label":"string","value":["array"]}}']


sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
pedidos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-pedidos/pedidos/raw/")
sqlContext.registerDataFrameAsTable(pedidos_df, "Zdc_Lotus_Pedidos_TablaIntermedia")

# COMMAND ----------

import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-pedidos-storage/pedidos/' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Pedidos_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","attachments":["array"],"rch_DetallePed":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"key_Prioridad":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_ValidarResp":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"rch_AnexosAdic":{"label":"string","value":"string"},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_RgnEmp_A":{"label":"string","value":"string"},"txt_UbicEmp_A":{"label":"string","value":"string"},"txt_ExtEmp_A":{"label":"string","value":"string"},"txt_EmpEnteExt":{"label":"string","value":"string"},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"key_Prioridad_1":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_EmpresaEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"}}']


sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
from pyspark.sql.functions import col,array_join,substring,substring_index

pedidos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-pedidos/pedidos/raw")
pedidos_df = pedidos_df.select(col("UniversalID").alias("UniversalIDPadre"),
                               col("UniversalIDCompuesto").alias("UniversalID"),
                               col("attachments").alias("attachments"),
                               col("rch_DetallePed.value").alias("rch_DetallePed"),
                               col("txt_Autor.value").alias("txt_Autor"),
                               col("txt_Creador.value").alias("txt_Creador"),
                               col("dat_FchCreac.value").alias("dat_FchCreac"),
                               col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                               col("key_NomPedido.value").alias("key_NomPedido"),
                               col("key_Prioridad.value").alias("key_Prioridad"),
                               col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                               col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                               col("txt_ValidarResp.value").alias("txt_ValidarResp"),
                               col("txt_Comentarios.value").alias("txt_Comentarios"),
                               col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                               col("rch_AnexosAdic.value").alias("rch_AnexosAdic"),
                               col("txt_Titulo1.value").alias("txt_Titulo1"),
                               col("txt_Titulo2.value").alias("txt_Titulo2"),
                               col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                               col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                               col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                               col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                               col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                               col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
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

sqlContext.registerDataFrameAsTable(pedidos_df, "Zdc_Lotus_Pedidos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txt_Historia como String"""
pedidos_txt_Historia_string_payload =  ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
pedidos_txt_Historia_string_rdd = sc.parallelize(pedidos_txt_Historia_string_payload)
pedidos_txt_Historia_string_json = spark.read.json(pedidos_txt_Historia_string_rdd)

pedidos_txt_Historia_string_df = spark.read.schema(pedidos_txt_Historia_string_json.schema).json("/mnt/lotus-pedidos/pedidos/raw/")

pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.selectExpr("UniversalIDCompuesto as UniversalID" ,"dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.na.fill('')
pedidos_txt_Historia_string_df = pedidos_txt_Historia_string_df.filter(pedidos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_string_df, "Zdc_Lotus_Pedidos_TablaIntermedia_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txt_Historia como Array"""
pedidos_txt_Historia_array_payload = ["{'UniversalIDCompuesto':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]

sc = spark.sparkContext
pedidos_txt_Historia_array_rdd = sc.parallelize(pedidos_txt_Historia_array_payload)
pedidos_txt_Historia_array_json = spark.read.json(pedidos_txt_Historia_array_rdd)

pedidos_txt_Historia_array_df = spark.read.schema(pedidos_txt_Historia_array_json.schema).json("/mnt/lotus-pedidos/pedidos/raw/")

from pyspark.sql.functions import col,array_join
pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.selectExpr("UniversalIDCompuesto as UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
pedidos_txt_Historia_array_df = pedidos_txt_Historia_array_df.na.fill('')
sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_array_df, "Zdc_Lotus_pedidos_Tablaintermedia_txt_Historia_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_Historia
pedidos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Pedidos_TablaIntermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_pedidos_Tablaintermedia_txt_Historia_array")
sqlContext.registerDataFrameAsTable(pedidos_txt_Historia_df, "Zdc_Lotus_Pedidos_Tablaintermedia_txt_Historia")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-pedidos/pedidos/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-pedidos/pedidos/5_LoadSchema $from_create_schema = True