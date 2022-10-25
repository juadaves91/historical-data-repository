# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE CRUDOS

# COMMAND ----------

dummy = ["{'UniversalID':'string','attachments':['array'],'hijos':[{'DADDY':{'label':'string','value':'string'},'Doc1':{'label':'string','value':'string'},'Doc10':{'label':'string','value':'string'},'Doc11':{'label':'string','value':'string'},'Doc12':{'label':'string','value':'string'},'Doc13':{'label':'string','value':'string'},'Doc14':{'label':'string','value':'string'},'Doc15':{'label':'string','value':'string'},'Doc16':{'label':'string','value':'string'},'Doc17':{'label':'string','value':'string'},'Doc18':{'label':'string','value':'string'},'Doc19':{'label':'string','value':'string'},'Doc2':{'label':'string','value':'string'},'Doc3':{'label':'string','value':'string'},'Doc4':{'label':'string','value':'string'},'Doc5':{'label':'string','value':'string'},'Doc6':{'label':'string','value':'string'},'Doc7':{'label':'string','value':'string'},'Doc8':{'label':'string','value':'string'},'Doc9':{'label':'string','value':'string'},'ok1':{'label':'string','value':'string'},'ok10':{'label':'string','value':'string'},'ok10_1':{'label':'string','value':'string'},'ok11':{'label':'string','value':'string'},'ok11_1':{'label':'string','value':'string'},'ok12':{'label':'string','value':'string'},'ok12_1':{'label':'string','value':'string'},'ok13':{'label':'string','value':'string'},'ok13_1':{'label':'string','value':'string'},'ok14':{'label':'string','value':'string'},'ok14_1':{'label':'string','value':'string'},'ok15':{'label':'string','value':'string'},'ok15_1':{'label':'string','value':'string'},'ok16':{'label':'string','value':'string'},'ok16_1':{'label':'string','value':'string'},'ok17':{'label':'string','value':'string'},'ok17_1':{'label':'string','value':'string'},'ok18':{'label':'string','value':'string'},'ok18_1':{'label':'string','value':'string'},'ok19':{'label':'string','value':'string'},'ok19_1':{'label':'string','value':'string'},'ok1_1':{'abel':'string','value':'string'},'ok2':{'label':'string','value':'string'},'ok2_1':{'label':'string','value':'string'},'ok3':{'label':'string','value':'string'},'ok3_1':{'label':'string','value':'string'},'ok4':{'label':'string','value':'string'},'ok4_1':{'label':'string','value':'string'},'ok5':{'label':'string','value':'string'},'ok5_1':{'label':'string','value':'string'},'ok6':{'label':'string','value':'string'},'ok6_1':{'label':'string','value':'string'},'ok7':{'label':'string','value':'string'},'ok7_1':{'label':'string','value':'string'},'ok8':{'label':'string','value':'string'},'ok8_1':{'label':'string','value':'string'},'ok9':{'label':'string','value':'string'},'ok9_1':{'label':'string','value':'string'}}],'txtTipoReclasificado':{'label':'string', 'value':'string'},'Dat_Fcreacion':{'label':'string', 'value':'string'},'Str_Autor':{'label':'string', 'value':'string'},'str_cliidentificacion':{'label':'string', 'value':'string'},'str_cliTipIdentificacion':{'label':'string', 'value':'string'},'Str_TipIdentificacion':{'label':'string', 'value':'string'},'str_cliNombre':{'label':'string', 'value':'string'},'str_cliGerente':{'label':'string', 'value':'string'},'str_CodGerente':{'label':'string', 'value':'string'},'Str_Region':{'label':'string', 'value':'string'},'txtSucursal':{'label':'string', 'value':'string'},'txtCalificacionInternaCliente':{'label':'string', 'value':'string'},'Historia':{'label':'string', 'value':['array']},'Str_Radicado':{'label':'string', 'value':'string'},'Dat_Fdesicion':{'label':'string', 'value':'string'},'Str_Reclasificador':{'label':'string', 'value':'string'},'Str_Estado':{'label':'string', 'value':'string'},'str_Responsable':{'label':'string', 'value':'string'},'str_cliSegmento':{'label':'string', 'value':'string'},'txtPuntajeCliente':{'label':'string', 'value':'string'},'Str_NumIdentificacion':{'label':'string', 'value':'string'}}"]

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
from pyspark.sql.functions import col,array_join
reclasificados_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclasificados/raw/")
reclasificados_df = reclasificados_df.select(col('UniversalID'),
                                             col('attachments'),
                                             col('txtTipoReclasificado.value').alias('txtTipoReclasificado'),
                                             col('Dat_Fcreacion.value').alias('Dat_Fcreacion'),
                                             col('Str_Autor.value').alias('Str_Autor'),
                                             col('str_cliidentificacion.value').alias('str_cliidentificacion'),
                                             col('str_cliTipIdentificacion.value').alias('str_cliTipIdentificacion'),
                                             col('str_cliNombre.value').alias('str_cliNombre'),
                                             col('str_cliGerente.value').alias('str_cliGerente'),
                                             col('str_CodGerente.value').alias('str_CodGerente'),
                                             col('Str_TipIdentificacion.value').alias('Str_TipIdentificacion'),
                                             col('Str_Region.value').alias('Str_Region'),
                                             col('txtSucursal.value').alias('txtSucursal'),
                                             col('txtCalificacionInternaCliente.value').alias('txtCalificacionInternaCliente'),
                                             col('Str_Radicado.value').cast('int').alias('Str_Radicado'),
                                             col('Dat_Fdesicion.value').alias('Dat_Fdesicion'),
                                             col('Str_Reclasificador.value').alias('Str_Reclasificador'),
                                             col('Str_Estado.value').alias('Str_Estado'),
                                             col('str_Responsable.value').alias('str_Responsable'),
                                             col('str_cliSegmento.value').alias('str_cliSegmento'),col('txtPuntajeCliente.value').alias('txtPuntajeCliente'),
                                             col('Str_NumIdentificacion.value').alias('Str_NumIdentificacion'),array_join('Historia.value', '\r\n').alias('Historia'),
                                             col('hijos.Doc1.value').getItem(0).alias('Doc1'),
                                             col('hijos.Doc2.value').getItem(0).alias('Doc2'),
                                             col('hijos.Doc3.value').getItem(0).alias('Doc3'),
                                             col('hijos.Doc4.value').getItem(0).alias('Doc4'),
                                             col('hijos.Doc5.value').getItem(0).alias('Doc5'),
                                             col('hijos.Doc6.value').getItem(0).alias('Doc6'),
                                             col('hijos.Doc7.value').getItem(0).alias('Doc7'),
                                             col('hijos.Doc8.value').getItem(0).alias('Doc8'),
                                             col('hijos.Doc9.value').getItem(0).alias('Doc9'),
                                             col('hijos.Doc10.value').getItem(0).alias('Doc10'),
                                             col('hijos.Doc11.value').getItem(0).alias('Doc11'),
                                             col('hijos.Doc12.value').getItem(0).alias('Doc12'),
                                             col('hijos.Doc13.value').getItem(0).alias('Doc13'),
                                             col('hijos.Doc14.value').getItem(0).alias('Doc14'),
                                             col('hijos.Doc15.value').getItem(0).alias('Doc15'),
                                             col('hijos.Doc16.value').getItem(0).alias('Doc16'),
                                             col('hijos.Doc17.value').getItem(0).alias('Doc17'),
                                             col('hijos.Doc18.value').getItem(0).alias('Doc18'),
                                             col('hijos.Doc19.value').getItem(0).alias('Doc19'),
                                             col('hijos.ok1.value').getItem(0).alias('ok1'),
                                             col('hijos.ok2.value').getItem(0).alias('ok2'),
                                             col('hijos.ok3.value').getItem(0).alias('ok3'),
                                             col('hijos.ok4.value').getItem(0).alias('ok4'),
                                             col('hijos.ok5.value').getItem(0).alias('ok5'),
                                             col('hijos.ok6.value').getItem(0).alias('ok6'),
                                             col('hijos.ok7.value').getItem(0).alias('ok7'),
                                             col('hijos.ok8.value').getItem(0).alias('ok8'),
                                             col('hijos.ok9.value').getItem(0).alias('ok9'),
                                             col('hijos.ok10.value').getItem(0).alias('ok10'),
                                             col('hijos.ok11.value').getItem(0).alias('ok11'),
                                             col('hijos.ok12.value').getItem(0).alias('ok12'),
                                             col('hijos.ok13.value').getItem(0).alias('ok13'),
                                             col('hijos.ok14.value').getItem(0).alias('ok14'),
                                             col('hijos.ok15.value').getItem(0).alias('ok15'),
                                             col('hijos.ok16.value').getItem(0).alias('ok16'),
                                             col('hijos.ok17.value').getItem(0).alias('ok17'),
                                             col('hijos.ok18.value').getItem(0).alias('ok18'),
                                             col('hijos.ok19.value').getItem(0).alias('ok19'),
                                             col('hijos.ok1_1.value').getItem(0).alias('ok1_1'),
                                             col('hijos.ok2_1.value').getItem(0).alias('ok2_1'),
                                             col('hijos.ok3_1.value').getItem(0).alias('ok3_1'),
                                             col('hijos.ok4_1.value').getItem(0).alias('ok4_1'),
                                             col('hijos.ok5_1.value').getItem(0).alias('ok5_1'),
                                             col('hijos.ok6_1.value').getItem(0).alias('ok6_1'),
                                             col('hijos.ok7_1.value').getItem(0).alias('ok7_1'),
                                             col('hijos.ok8_1.value').getItem(0).alias('ok8_1'),
                                             col('hijos.ok9_1.value').getItem(0).alias('ok9_1'),
                                             col('hijos.ok10_1.value').getItem(0).alias('ok10_1'),
                                             col('hijos.ok11_1.value').getItem(0).alias('ok11_1'),
                                             col('hijos.ok12_1.value').getItem(0).alias('ok12_1'),
                                             col('hijos.ok13_1.value').getItem(0).alias('ok13_1'),
                                             col('hijos.ok14_1.value').getItem(0).alias('ok14_1'),
                                             col('hijos.ok15_1.value').getItem(0).alias('ok15_1'),
                                             col('hijos.ok16_1.value').getItem(0).alias('ok16_1'),
                                             col('hijos.ok17_1.value').getItem(0).alias('ok17_1'),
                                             col('hijos.ok18_1.value').getItem(0).alias('ok18_1'),
                                             col('hijos.ok19_1.value').getItem(0).alias('ok19_1'))

sqlContext.registerDataFrameAsTable(reclasificados_df, "Zdc_Lotus_Reclasificados_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,MetaData
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclasificados-storage' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Reclasificados_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclasificados/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclasificados/5_LoadSchema $from_create_schema = True