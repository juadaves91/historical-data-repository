# Databricks notebook source
# MAGIC %md
# MAGIC ##ZONA DE CRUDOS

# COMMAND ----------

# DBTITLE 1,Esquema Principal

dummy = ['{"UniversalID":"string","attachments":["array"],"key_ViceAtdPed":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"rch_AnexosAdic":{"label":"string","value":"string"},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_RgnEmp_A":{"label":"string","value":"string"},"txt_ExtEmp_A":{"label":"string","value":"string"},"txt_EmpEnteExt":{"label":"string","value":"string"},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"},"txt_Historia":{"Label":"string","value":["array"]},"txt_UbicEmp_A":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"rch_DetallePed":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
solicitudes_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-solicitudes/raw/")
sqlContext.registerDataFrameAsTable(solicitudes_df, "Zdc_Lotus_Solicitudes_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-solicitudes-storage' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Solicitudes_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"key_ViceAtdPed":{"label":"string","value":"string"},"key_NomPedido":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_TipoEnteExt":{"label":"string","value":"string"},"txt_AsuntoPed":{"label":"string","value":"string"},"dat_FchMaxEstado":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":"string"},"rch_AnexosPed":{"label":"string","value":"string"},"rch_AnexosAdic":{"label":"string","value":"string"},"txt_Titulo1":{"label":"string","value":"string"},"txt_Titulo2":{"label":"string","value":"string"},"txt_NomCompEmp_A":{"label":"string","value":"string"},"txt_DepEmp_A":{"label":"string","value":"string"},"txt_RgnEmp_A":{"label":"string","value":"string"},"txt_ExtEmp_A":{"label":"string","value":"string"},"txt_EmpEnteExt":{"label":"string","value":"string"},"txt_TelEnteExt":{"label":"string","value":"string"},"dat_FchIniTram":{"label":"string","value":"string"},"num_DVTotal":{"label":"string","value":"string"},"num_DVParcial":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"dat_FchEstadoAct":{"label":"string","value":"string"},"num_TMaxEstado":{"label":"string","value":"string"},"dat_FchMaxEstado_1":{"label":"string","value":"string"},"num_TCotizado":{"label":"string","value":"string"},"num_TCotizado_1":{"label":"string","value":"string"},"num_PesosCotiz":{"label":"string","value":"string"},"num_PesosCotiz_1":{"label":"string","value":"string"},"dat_FchVencEstado":{"label":"string","value":"string"},"txt_Responsable":{"label":"string","value":"string"},"txt_Responsable_1":{"label":"string","value":"string"},"txt_NomCompEmp_R":{"label":"string","value":"string"},"txt_DepEmp_R":{"label":"string","value":"string"},"txt_RgnEmp_R":{"label":"string","value":"string"},"txt_ExtEmp_R":{"label":"string","value":"string"},"txt_UbicEmp_A":{"label":"string","value":"string"},"txt_UbicEmp_R":{"label":"string","value":"string"},"rch_DetallePed":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
from pyspark.sql.functions import col,array_join,substring,substring_index

solicitudes_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-solicitudes/raw")
solicitudes_df = solicitudes_df.select("UniversalID","attachments",
                                       col("txt_Autor.value").alias("txt_Autor"),
                                       col("dat_FchCreac.value").alias("dat_FchCreac"),
                                       col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                       col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                       col("key_NomPedido.value").alias("key_NomPedido"),
                                       col("txt_Creador.value").alias("txt_Creador"),
                                       col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                       col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                       col("txt_Comentarios.value").alias("txt_Comentarios"),
                                       col("rch_AnexosPed.value").alias("rch_AnexosPed"),
                                       col("rch_AnexosAdic.value").alias("rch_AnexosAdic"),
                                       col("txt_Titulo1.value").alias("txt_Titulo1"),
                                       col("txt_Titulo2.value").alias("txt_Titulo2"),
                                       col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                       col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                       col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                                       col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                                       col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
                                       col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                       col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                       col("num_DVTotal.value").alias("num_DVTotal"),
                                       col("num_DVParcial.value").alias("num_DVParcial"),
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
                                       col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                       col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                       col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                       col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                                       col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                       col("rch_DetallePed.value").alias("rch_DetallePed"),
                                       substring(substring_index('dat_FchCreac.value', ' ', 1),-4,4).cast('int').alias('anio'),
                                       substring(substring_index('dat_FchCreac.value', ' ', 1),-7,2).cast('int').alias('mes'))

sqlContext.registerDataFrameAsTable(solicitudes_df, "Zdc_Lotus_Solicitudes_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
"""txt_Historia como String"""
solicitudes_txt_Historia_string_payload =  ["{'UniversalID':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
solicitudes_txt_Historia_string_rdd = sc.parallelize(solicitudes_txt_Historia_string_payload)
solicitudes_txt_Historia_string_json = spark.read.json(solicitudes_txt_Historia_string_rdd)

solicitudes_txt_Historia_string_df = spark.read.schema(solicitudes_txt_Historia_string_json.schema).json("/mnt/lotus-solicitudes/raw/")

solicitudes_txt_Historia_string_df = solicitudes_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
solicitudes_txt_Historia_string_df = solicitudes_txt_Historia_string_df.filter(solicitudes_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_string_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
"""txt_Historia como Array"""
solicitudes_txt_Historia_array_payload = ["{'UniversalID':'string','dat_FchCreac':{'label':'string','value':'string'},'txt_Historia':{'Label':'string','value':['array']}}"]

sc = spark.sparkContext
solicitudes_txt_Historia_array_rdd = sc.parallelize(solicitudes_txt_Historia_array_payload)
solicitudes_txt_Historia_array_json = spark.read.json(solicitudes_txt_Historia_array_rdd)

solicitudes_txt_Historia_array_df = spark.read.schema(solicitudes_txt_Historia_array_json.schema).json("/mnt/lotus-solicitudes/raw/")

from pyspark.sql.functions import col,array_join
solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
solicitudes_txt_Historia_array_df = solicitudes_txt_Historia_array_df.select("UniversalID","dat_FchCreac", array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_array_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_Historia
solicitudes_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_string UNION SELECT * FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia_array")
sqlContext.registerDataFrameAsTable(solicitudes_txt_Historia_df, "Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-solicitudes/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-solicitudes/5_LoadSchema $from_create_schema = True