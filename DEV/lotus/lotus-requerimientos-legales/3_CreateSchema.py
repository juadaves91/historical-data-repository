# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE CRUDOS

# COMMAND ----------

# DBTITLE 1,Esquema Principal (txt_CedNit, rch_DetallePed, txt_Comentarios definidos como Arrays)
schema_payload = ["{'UniversalID':'string','attachments':['array'],'key_TipoReqmto':{'label':'string', 'value':'string'},'dat_FchCreac':{'label':'string', 'value':'string'},'txt_AsuntoPed':{'label':'string', 'value':'string'},'key_Filial':{'label':'string', 'value':'string'},'key_SucArea':{'label':'string', 'value':'string'},'key_EnteSol':{'label':'string', 'value':'string'},'txt_OtroEnte':{'label':'string', 'value':'string'},'dat_FechOficio':{'label':'string', 'value':'string'},'txt_OtroReqmto':{'label':'string', 'value':'string'},'num_DiaSol':{'label':'string', 'value':'string'},'key_TipoConteo':{'label':'string', 'value':'string'},'num_DiasPro':{'label':'string', 'value':'string'},'txt_CedNit':{'label':'string', 'value':['array']},'txt_Pcto':{'label':'string', 'value':'string'},'key_Ciudad':{'label':'string', 'value':'string'},'txt_Funcionario':{'label':'string', 'value':'string'},'txt_Direccion':{'label':'string', 'value':'string'},'txt_Banco':{'label':'string', 'value':'string'},'txt_CtaDep':{'label':'string', 'value':'string'},'txt_NumOficio':{'label':'string', 'value':'string'},'dat_FechVto':{'label':'string', 'value':'string'},'key_ViceAtdPed':{'label':'string', 'value':'string'},'key_NomPedido':{'label':'string', 'value':'string'},'txt_Autor':{'label':'string', 'value':'string'},'txt_Creador':{'label':'string', 'value':'string'},'key_TipoEnteExt':{'label':'string', 'value':'string'},'dat_FchMaxEstado':{'label':'string', 'value':'string'},'rch_DetallePed':{'label':'string', 'value':['array']},'txt_Comentarios':{'label':'string', 'value':['array']},'txt_NomCompEmp_A':{'label':'string', 'value':'string'},'txt_DepEmp_A':{'label':'string', 'value':'string'},'txt_RgnEmp_A':{'label':'string', 'value':'string'},'txt_UbicEmp_A':{'label':'string', 'value':'string'},'txt_ExtEmp_A':{'label':'string', 'value':'string'},'txt_EmpEnteExt':{'label':'string', 'value':'string'},'txt_TelEnteExt':{'label':'string', 'value':'string'},'dat_FchIniTram':{'label':'string', 'value':'string'},'num_DVTotal':{'label':'string', 'value':'string'},'num_DVParcial':{'label':'string', 'value':'string'},'txt_Estado':{'label':'string', 'value':'string'},'dat_FchEstadoAct':{'label':'string', 'value':'string'},'num_TMaxEstado':{'label':'string', 'value':'string'},'num_TCotizado':{'label':'string', 'value':'string'},'dat_FchVencEstado':{'label':'string', 'value':'string'},'txt_Responsable':{'label':'string', 'value':'string'},'txt_NomCompEmp_R':{'label':'string', 'value':'string'},'txt_DepEmp_R':{'label':'string', 'value':'string'},'txt_RgnEmp_R':{'label':'string', 'value':'string'},'txt_UbicEmp_R':{'label':'string', 'value':'string'},'txt_ExtEmp_R':{'label':'string', 'value':'string'},'txt_Historia':{'label':'string', 'value':['array']}}"]

schema_list = list()
schema_list.append(schema_payload)

sc = spark.sparkContext
schema_rdd = sc.parallelize(schema_payload)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia (temporal)
req_legales_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")
sqlContext.registerDataFrameAsTable(req_legales_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia")

# COMMAND ----------

# DBTITLE 1,MetaData
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-requerimientos-legales-storage' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_RequerimientosLegales_Tablaintermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

# DBTITLE 1,Esquema Principal (txt_CedNit, rch_DetallePed, txt_Comentarios Omitidos)
dummy = ["{'UniversalID':'string','attachments':['array'],'key_TipoReqmto':{'label':'string', 'value':'string'},'dat_FchCreac':{'label':'string', 'value':'string'},'txt_AsuntoPed':{'label':'string', 'value':'string'},'key_Filial':{'label':'string', 'value':'string'},'key_SucArea':{'label':'string', 'value':'string'},'key_EnteSol':{'label':'string', 'value':'string'},'txt_OtroEnte':{'label':'string', 'value':'string'},'dat_FechOficio':{'label':'string', 'value':'string'},'txt_OtroReqmto':{'label':'string', 'value':'string'},'num_DiaSol':{'label':'string', 'value':'string'},'key_TipoConteo':{'label':'string', 'value':'string'},'num_DiasPro':{'label':'string', 'value':'string'},'txt_Pcto':{'label':'string', 'value':'string'},'key_Ciudad':{'label':'string', 'value':'string'},'txt_Funcionario':{'label':'string', 'value':'string'},'txt_Direccion':{'label':'string', 'value':'string'},'txt_Banco':{'label':'string', 'value':'string'},'txt_CtaDep':{'label':'string', 'value':'string'},'txt_NumOficio':{'label':'string', 'value':'string'},'dat_FechVto':{'label':'string', 'value':'string'},'key_ViceAtdPed':{'label':'string', 'value':'string'},'key_NomPedido':{'label':'string', 'value':'string'},'txt_Autor':{'label':'string', 'value':'string'},'txt_Creador':{'label':'string', 'value':'string'},'key_TipoEnteExt':{'label':'string', 'value':'string'},'dat_FchMaxEstado':{'label':'string', 'value':'string'},'txt_NomCompEmp_A':{'label':'string', 'value':'string'},'txt_DepEmp_A':{'label':'string', 'value':'string'},'txt_RgnEmp_A':{'label':'string', 'value':'string'},'txt_UbicEmp_A':{'label':'string', 'value':'string'},'txt_ExtEmp_A':{'label':'string', 'value':'string'},'txt_EmpEnteExt':{'label':'string', 'value':'string'},'txt_TelEnteExt':{'label':'string', 'value':'string'},'dat_FchIniTram':{'label':'string', 'value':'string'},'num_DVTotal':{'label':'string', 'value':'string'},'num_DVParcial':{'label':'string', 'value':'string'},'txt_Estado':{'label':'string', 'value':'string'},'dat_FchEstadoAct':{'label':'string', 'value':'string'},'num_TMaxEstado':{'label':'string', 'value':'string'},'num_TCotizado':{'label':'string', 'value':'string'},'dat_FchVencEstado':{'label':'string', 'value':'string'},'txt_Responsable':{'label':'string', 'value':'string'},'txt_NomCompEmp_R':{'label':'string', 'value':'string'},'txt_DepEmp_R':{'label':'string', 'value':'string'},'txt_RgnEmp_R':{'label':'string', 'value':'string'},'txt_UbicEmp_R':{'label':'string', 'value':'string'},'txt_ExtEmp_R':{'label':'string', 'value':'string'},'txt_Historia':{'label':'string', 'value':['array']}}"]

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
from pyspark.sql.functions import col,array_join
req_legales_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")
req_legales_df = req_legales_df.select("UniversalID","attachments",
                                       col("dat_FchCreac.value").alias("dat_FchCreac"),
                                       col("dat_FchEstadoAct.value").alias("dat_FchEstadoAct"),
                                       col("dat_FchIniTram.value").alias("dat_FchIniTram"),
                                       col("dat_FchMaxEstado.value").alias("dat_FchMaxEstado"),
                                       col("dat_FchVencEstado.value").alias("dat_FchVencEstado"),
                                       col("dat_FechOficio.value").alias("dat_FechOficio"),
                                       col("dat_FechVto.value").alias("dat_FechVto"),
                                       col("key_Ciudad.value").alias("key_Ciudad"),
                                       col("key_EnteSol.value").alias("key_EnteSol"),
                                       col("key_Filial.value").alias("key_Filial"),
                                       col("key_NomPedido.value").alias("key_NomPedido"),
                                       col("key_SucArea.value").alias("key_SucArea"),
                                       col("key_TipoConteo.value").alias("key_TipoConteo"),
                                       col("key_TipoEnteExt.value").alias("key_TipoEnteExt"),
                                       col("key_TipoReqmto.value").alias("key_TipoReqmto"),
                                       col("key_ViceAtdPed.value").alias("key_ViceAtdPed"),
                                       col("num_DVParcial.value").alias("num_DVParcial"),
                                       col("num_DVTotal.value").alias("num_DVTotal"),
                                       col("num_DiaSol.value").alias("num_DiaSol"),
                                       col("num_DiasPro.value").alias("num_DiasPro"),
                                       col("num_TCotizado.value").alias("num_TCotizado"),
                                       col("num_TMaxEstado.value").alias("num_TMaxEstado"),
                                       col("txt_AsuntoPed.value").alias("txt_AsuntoPed"),
                                       col("txt_Autor.value").alias("txt_Autor"),
                                       col("txt_Banco.value").alias("txt_Banco"),
                                       col("txt_Creador.value").alias("txt_Creador"),
                                       col("txt_CtaDep.value").alias("txt_CtaDep"),
                                       col("txt_DepEmp_A.value").alias("txt_DepEmp_A"),
                                       col("txt_DepEmp_R.value").alias("txt_DepEmp_R"),
                                       col("txt_Direccion.value").alias("txt_Direccion"),
                                       col("txt_EmpEnteExt.value").alias("txt_EmpEnteExt"),
                                       col("txt_Estado.value").alias("txt_Estado"),
                                       col("txt_ExtEmp_A.value").alias("txt_ExtEmp_A"),
                                       col("txt_ExtEmp_R.value").alias("txt_ExtEmp_R"),
                                       col("txt_Funcionario.value").alias("txt_Funcionario"),
                                       col("txt_NomCompEmp_A.value").alias("txt_NomCompEmp_A"),
                                       col("txt_NomCompEmp_R.value").alias("txt_NomCompEmp_R"),
                                       col("txt_NumOficio.value").alias("txt_NumOficio"),
                                       col("txt_OtroEnte.value").alias("txt_OtroEnte"),
                                       col("txt_OtroReqmto.value").alias("txt_OtroReqmto"),
                                       col("txt_Pcto.value").alias("txt_Pcto"),
                                       col("txt_Responsable.value").alias("txt_Responsable"),
                                       col("txt_RgnEmp_A.value").alias("txt_RgnEmp_A"),
                                       col("txt_RgnEmp_R.value").alias("txt_RgnEmp_R"),
                                       col("txt_TelEnteExt.value").alias("txt_TelEnteExt"),
                                       col("txt_UbicEmp_A.value").alias("txt_UbicEmp_A"),
                                       col("txt_UbicEmp_R.value").alias("txt_UbicEmp_R"),
                                       array_join("txt_Historia.value", '\r\n ').alias("txt_Historia"))

sqlContext.registerDataFrameAsTable(req_legales_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_CedNit como String
"""txt_CedNit como String"""
req_legales_CedNit_String_payload = ["{'UniversalID':'string','txt_CedNit':{'label':'string', 'value':'string'}}"]

sc = spark.sparkContext
req_legales_CedNit_String_rdd = sc.parallelize(req_legales_CedNit_String_payload)
req_legales_CedNit_String_json = spark.read.json(req_legales_CedNit_String_rdd)

req_legales_CedNit_String_df = spark.read.schema(req_legales_CedNit_String_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

req_legales_CedNit_String_df = req_legales_CedNit_String_df.selectExpr("UniversalID","txt_CedNit.value as txt_CedNit")
req_legales_CedNit_String_df = req_legales_CedNit_String_df.na.fill('')
req_legales_CedNit_String_df = req_legales_CedNit_String_df.filter(req_legales_CedNit_String_df.txt_CedNit.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(req_legales_CedNit_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_CedNit como Array
"""txt_CedNit como Array"""
req_legales_CedNit_Array_payload = ["{'UniversalID':'string','txt_CedNit':{'label':'string', 'value':['array']}}"]

sc = spark.sparkContext
req_legales_CedNit_Array_rdd = sc.parallelize(req_legales_CedNit_Array_payload)
req_legales_CedNit_Array_json = spark.read.json(req_legales_CedNit_Array_rdd)

req_legales_CedNit_Array_df = spark.read.schema(req_legales_CedNit_Array_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

from pyspark.sql.functions import col,array_join

req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.selectExpr("UniversalID","txt_CedNit.value as txt_CedNit")
req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.filter(col("UniversalID").isNotNull())
req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.select("UniversalID", array_join("txt_CedNit", '\r\n ').alias("txt_CedNit"))
req_legales_CedNit_Array_df = req_legales_CedNit_Array_df.na.fill('')
sqlContext.registerDataFrameAsTable(req_legales_CedNit_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_CedNit 
req_legales_CedNit_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit_string")
sqlContext.registerDataFrameAsTable(req_legales_CedNit_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_DetallePed como String
"""rch_DetallePed como String"""
req_legales_DetallePed_String_payload = ["{'UniversalID':'string','rch_DetallePed':{'label':'string', 'value':'string'}}"]

sc = spark.sparkContext
req_legales_DetallePed_String_rdd = sc.parallelize(req_legales_DetallePed_String_payload)
req_legales_DetallePed_String_json = spark.read.json(req_legales_DetallePed_String_rdd)

req_legales_DetallePed_String_df = spark.read.schema(req_legales_DetallePed_String_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.selectExpr("UniversalID","rch_DetallePed.value as rch_DetallePed")
req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.na.fill('')
req_legales_DetallePed_String_df = req_legales_DetallePed_String_df.filter(req_legales_DetallePed_String_df.rch_DetallePed.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(req_legales_DetallePed_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_string")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_DetallePed como Array
"""rch_DetallePed como Array"""
req_legales_DetallePed_Array_payload = ["{'UniversalID':'string','rch_DetallePed':{'label':'string', 'value':['array']}}"]

sc = spark.sparkContext
req_legales_DetallePed_Array_rdd = sc.parallelize(req_legales_DetallePed_Array_payload)
req_legales_DetallePed_Array_json = spark.read.json(req_legales_DetallePed_Array_rdd)

req_legales_DetallePed_Array_df = spark.read.schema(req_legales_DetallePed_Array_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

from pyspark.sql.functions import col,array_join

req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.selectExpr("UniversalID","rch_DetallePed.value as rch_DetallePed")
req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.filter(col("UniversalID").isNotNull())
req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.select("UniversalID", array_join("rch_DetallePed", '\r\n ').alias("rch_DetallePed"))
req_legales_DetallePed_Array_df = req_legales_DetallePed_Array_df.na.fill('')
sqlContext.registerDataFrameAsTable(req_legales_DetallePed_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_array")

# COMMAND ----------

# DBTITLE 1,Dataframe rch_DetallePed 
req_legales_DetallePed_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed_string")
sqlContext.registerDataFrameAsTable(req_legales_DetallePed_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como String
"""txt_Comentarios como String"""
req_legales_Comentarios_String_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':'string'}}"]

sc = spark.sparkContext
req_legales_Comentarios_String_rdd = sc.parallelize(req_legales_Comentarios_String_payload)
req_legales_Comentarios_String_json = spark.read.json(req_legales_Comentarios_String_rdd)

req_legales_Comentarios_String_df = spark.read.schema(req_legales_Comentarios_String_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.selectExpr("UniversalID","txt_Comentarios.value as txt_Comentarios")
req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.na.fill('')
req_legales_Comentarios_String_df = req_legales_Comentarios_String_df.filter(req_legales_Comentarios_String_df.txt_Comentarios.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(req_legales_Comentarios_String_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como Array
"""txt_Comentarios como Array"""
req_legales_Comentarios_Array_payload = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']}}"]

sc = spark.sparkContext
req_legales_Comentarios_Array_rdd = sc.parallelize(req_legales_Comentarios_Array_payload)
req_legales_Comentarios_Array_json = spark.read.json(req_legales_Comentarios_Array_rdd)

req_legales_Comentarios_Array_df = spark.read.schema(req_legales_Comentarios_Array_json.schema).json("/mnt/lotus-requerimientos-legales/raw/")

from pyspark.sql.functions import col,array_join
req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.selectExpr("UniversalID","txt_Comentarios.value as txt_Comentarios")
req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.filter(col("UniversalID").isNotNull())
req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.select("UniversalID", array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
req_legales_Comentarios_Array_df = req_legales_Comentarios_Array_df.na.fill('')
sqlContext.registerDataFrameAsTable(req_legales_Comentarios_Array_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_array")

# COMMAND ----------

# DBTITLE 1,Dataframe txt_Comentarios 
req_legales_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_array UNION SELECT * FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios_string")
sqlContext.registerDataFrameAsTable(req_legales_Comentarios_df, "Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios")

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-requerimientos-legales/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-requerimientos-legales/5_LoadSchema $from_create_schema = True