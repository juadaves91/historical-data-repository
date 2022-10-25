# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE CRUDOS

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"num_VlrTotal":{"label":"string","value":"string"},"num_VlrTotalD":{"label":"string","value":"string"},"num_VlrTotalD_1":{"label":"string","value":"string"},"num_VlrTotal_1":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_NumTransa":{"label":"string","value":"string"},"key_CamposTransa":{"label":"string","value":["array"]},"key_CamposRecl":{"label":"string","value":["array"]},"txt_CedNit":{"label":"string","value":"string"},"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"num_AbonoDefinit":{"label":"string","value":"string"},"txt_NomCliente":{"label":"string","value":"string"},"num_DV":{"label":"string","value":"string"},"num_DVPendiente":{"label":"string","value":"string"},"num_DVMalRad":{"label":"string","value":"string"},"num_DVFraudeTrj":{"label":"string","value":"string"},"num_DVSlnado":{"label":"string","value":"string"},"num_DVSlnadoCarta":{"label":"string","value":"string"},"num_DVPendAjuste":{"label":"string","value":"string"},"num_DVVerif":{"label":"string","value":"string"},"num_DVInvest":{"label":"string","value":"string"},"num_DVRedes":{"label":"string","value":"string"},"num_DVBancos":{"label":"string","value":"string"},"num_DVTrmInterno":{"label":"string","value":"string"},"num_DVSolicRptaEsc":{"label":"string","value":"string"},"num_DVClteNoCont":{"label":"string","value":"string"},"num_DVCartaEnRev":{"label":"string","value":"string"},"num_DVAboTempA":{"label":"string","value":"string"},"num_DVAboTempP":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"key_NomSucCaptura":{"label":"string","value":"string"},"key_SucursalFilial":{"label":"string","value":"string"},"txt_RgnCaptura":{"label":"string","value":"string"},"txt_ZonaCaptura":{"label":"string","value":"string"},"key_ReportadoPor":{"label":"string","value":"string"},"key_OrigenReporte":{"label":"string","value":"string"},"nom_Responsable_1":{"label":"string","value":"string"},"num_AbonoTemp":{"label":"string","value":"string"},"key_ResponError":{"label":"string","value":"string"},"txt_NomRespError":{"label":"string","value":"string"},"key_ClienteRazon":{"label":"string","value":"string"},"key_CausaNoAbono":{"label":"string","value":"string"},"nom_UsuCierra":{"label":"string","value":"string"},"num_DRecl":{"label":"string","value":"string"},"key_MedioRpta":{"label":"string","value":"string"},"key_TipoDocumento":{"label":"string","value":"string"},"txt_TipoCliente":{"label":"string","value":"string"},"txt_FuncEmpresa":{"label":"string","value":"string"},"txt_SubSgmtoCliente":{"label":"string","value":"string"},"txt_ClteGer":{"label":"string","value":"string"},"txt_GteCuenta":{"label":"string","value":"string"},"txt_ClteColombia":{"label":"string","value":"string"},"txt_CiudadCorr":{"label":"string","value":"string"},"txt_DescCiudadCorr":{"label":"string","value":"string"},"txt_DptoCorr":{"label":"string","value":"string"},"txt_DirCorr":{"label":"string","value":"string"},"txt_Pais":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"txt_DirPostal":{"label":"string","value":"string"},"txt_ApartAereo":{"label":"string","value":"string"},"txt_IdDireccion":{"label":"string","value":"string"},"key_RptaReclamo":{"label":"string","value":"string"},"txt_TeleCliente":{"label":"string","value":"string"},"txt_FaxCliente":{"label":"string","value":"string"},"txt_EmailCliente":{"label":"string","value":"string"},"nom_RespGer":{"label":"string","value":"string"},"txt_TelRespGer":{"label":"string","value":"string"},"num_TpoEstSol":{"label":"string","value":"string"},"dat_FchAproxSol":{"label":"string","value":"string"},"num_DAbierto":{"label":"string","value":"string"},"dat_Solucionado":{"label":"string","value":"string"},"txt_Cia":{"label":"string","value":"string"},"key_TipoTarjeta":{"label":"string","value":"string"},"key_Prefijo":{"label":"string","value":"string"},"txt_NumProducto":{"label":"string","value":"string"},"txt_NroCompraRapp":{"label":"string","value":"string"},"txt_NroProductoRapp":{"label":"string","value":"string"},"txt_SubprocRecl":{"label":"string","value":"string"},"txt_NumTarjDeb":{"label":"string","value":"string"},"txt_NumPoliza":{"label":"string","value":"string"},"txt_NumEncargo":{"label":"string","value":"string"},"key_NomSucRadica":{"label":"string","value":"string"},"txt_TipoPoliza":{"label":"string","value":"string"},"txt_NumTjtaCtaDeb":{"label":"string","value":"string"},"key_CiudadTransa":{"label":"string","value":"string"},"dat_FchTransac":{"label":"string","value":"string"},"key_NomSucVarios":{"label":"string","value":"string"},"txt_CodEstab":{"label":"string","value":"string"},"txt_MensajeAyuda":{"label":"string","value":"string"},"txt_MensajeSoportes":{"label":"string","value":"string"},"txt_ComentRapp":{"label":"string","value":"string"},"txt_SopRecCte":{"label":"string","value":"string"},"txt_SopNoRec":{"label":"string","value":"string"},"key_TodosSoportes":{"label":"string","value":"string"},"rch_Pantallas":{"label":"string","value":"string"},"rch_PantallaSln":{"label":"string","value":"string"},"txt_Comentarios":{"label":"string","value":["array"]},"txt_Historia":{"label":"string","value":["array"]},"rch_RptaCliente":{"label":"string","value":"string"},"txt_NomOpTblM":{"label":"string","value":"string"},"key_VlrsOpTblM":{"label":"string","value":"string"},"num_ValorTransa_1":{"label":"string","value":"string"},"num_ValorTransa_2":{"label":"string","value":"string"},"num_ValorTransa_3":{"label":"string","value":"string"},"num_ValorTransa_4":{"label":"string","value":"string"},"num_ValorTransa_5":{"label":"string","value":"string"},"num_ValorTransa_6":{"label":"string","value":"string"},"num_ValorTransa_7":{"label":"string","value":"string"},"num_ValorTransa_8":{"label":"string","value":"string"},"num_ValorTransa_9":{"label":"string","value":"string"},"dat_FechaTransa_1":{"label":"string","value":"string"},"dat_FechaTransa_2":{"label":"string","value":"string"},"dat_FechaTransa_3":{"label":"string","value":"string"},"dat_FechaTransa_4":{"label":"string","value":"string"},"dat_FechaTransa_5":{"label":"string","value":"string"},"dat_FechaTransa_6":{"label":"string","value":"string"},"dat_FechaTransa_7":{"label":"string","value":"string"},"dat_FechaTransa_8":{"label":"string","value":"string"},"dat_FechaTransa_9":{"label":"string","value":"string"},"key_OficinaTransa_1":{"label":"string","value":"string"},"key_OficinaTransa_2":{"label":"string","value":"string"},"key_OficinaTransa_3":{"label":"string","value":"string"},"key_OficinaTransa_4":{"label":"string","value":"string"},"key_OficinaTransa_5":{"label":"string","value":"string"},"key_OficinaTransa_6":{"label":"string","value":"string"},"key_OficinaTransa_7":{"label":"string","value":"string"},"key_OficinaTransa_8":{"label":"string","value":"string"},"key_OficinaTransa_9":{"label":"string","value":"string"},"txt_Referencia_1":{"label":"string","value":"string"},"txt_Referencia_2":{"label":"string","value":"string"},"txt_Referencia_3":{"label":"string","value":"string"},"txt_Referencia_4":{"label":"string","value":"string"},"txt_Referencia_5":{"label":"string","value":"string"},"txt_Referencia_6":{"label":"string","value":"string"},"txt_Referencia_7":{"label":"string","value":"string"},"txt_Referencia_8":{"label":"string","value":"string"},"txt_Referencia_9":{"label":"string","value":"string"},"key_Moneda_1":{"label":"string","value":"string"},"key_Moneda_2":{"label":"string","value":"string"},"key_Moneda_3":{"label":"string","value":"string"},"key_Moneda_4":{"label":"string","value":"string"},"key_Moneda_5":{"label":"string","value":"string"},"key_Moneda_6":{"label":"string","value":"string"},"key_Moneda_7":{"label":"string","value":"string"},"key_Moneda_8":{"label":"string","value":"string"},"key_Moneda_9":{"label":"string","value":"string"},"key_CiudadTransa_1":{"label":"string","value":"string"},"key_CiudadTransa_2":{"label":"string","value":"string"},"key_CiudadTransa_3":{"label":"string","value":"string"},"key_CiudadTransa_4":{"label":"string","value":"string"},"key_CiudadTransa_5":{"label":"string","value":"string"},"key_CiudadTransa_6":{"label":"string","value":"string"},"key_CiudadTransa_7":{"label":"string","value":"string"},"key_CiudadTransa_8":{"label":"string","value":"string"},"key_CiudadTransa_9":{"label":"string","value":"string"},"txt_NumeroCuenta_1":{"label":"string","value":"string"},"txt_NumeroCuenta_2":{"label":"string","value":"string"},"txt_NumeroCuenta_3":{"label":"string","value":"string"},"txt_NumeroCuenta_4":{"label":"string","value":"string"},"txt_NumeroCuenta_5":{"label":"string","value":"string"},"txt_NumeroCuenta_6":{"label":"string","value":"string"},"txt_NumeroCuenta_7":{"label":"string","value":"string"},"txt_NumeroCuenta_8":{"label":"string","value":"string"},"txt_NumeroCuenta_9":{"label":"string","value":"string"},"num_ChequeVouch_1":{"label":"string","value":"string"},"num_ChequeVouch_2":{"label":"string","value":"string"},"num_ChequeVouch_3":{"label":"string","value":"string"},"num_ChequeVouch_4":{"label":"string","value":"string"},"num_ChequeVouch_5":{"label":"string","value":"string"},"num_ChequeVouch_6":{"label":"string","value":"string"},"num_ChequeVouch_7":{"label":"string","value":"string"},"num_ChequeVouch_8":{"label":"string","value":"string"},"num_ChequeVouch_9":{"label":"string","value":"string"},"dat_FechaConsigna_1":{"label":"string","value":"string"},"dat_FechaConsigna_2":{"label":"string","value":"string"},"dat_FechaConsigna_3":{"label":"string","value":"string"},"dat_FechaConsigna_4":{"label":"string","value":"string"},"dat_FechaConsigna_5":{"label":"string","value":"string"},"dat_FechaConsigna_6":{"label":"string","value":"string"},"dat_FechaConsigna_7":{"label":"string","value":"string"},"dat_FechaConsigna_8":{"label":"string","value":"string"},"dat_FechaConsigna_9":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
reclamos_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")
sqlContext.registerDataFrameAsTable(reclamos_old_df, "Zdc_Lotus_Reclamos_old_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/reclamos-old' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Reclamos_old_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

dummy = ['{"UniversalID":"string","attachments":["array"],"num_VlrTotal":{"label":"string","value":"string"},"num_VlrTotalD":{"label":"string","value":"string"},"num_VlrTotalD_1":{"label":"string","value":"string"},"num_VlrTotal_1":{"label":"string","value":"string"},"dat_FchCreac":{"label":"string","value":"string"},"key_NumTransa":{"label":"string","value":"string"},"txt_CedNit":{"label":"string","value":"string"},"key_Producto":{"label":"string","value":"string"},"key_TipoReclamo":{"label":"string","value":"string"},"num_AbonoDefinit":{"label":"string","value":"string"},"txt_NomCliente":{"label":"string","value":"string"},"num_DV":{"label":"string","value":"string"},"num_DVPendiente":{"label":"string","value":"string"},"num_DVMalRad":{"label":"string","value":"string"},"num_DVFraudeTrj":{"label":"string","value":"string"},"num_DVSlnado":{"label":"string","value":"string"},"num_DVSlnadoCarta":{"label":"string","value":"string"},"num_DVPendAjuste":{"label":"string","value":"string"},"num_DVVerif":{"label":"string","value":"string"},"num_DVInvest":{"label":"string","value":"string"},"num_DVRedes":{"label":"string","value":"string"},"num_DVBancos":{"label":"string","value":"string"},"num_DVTrmInterno":{"label":"string","value":"string"},"num_DVSolicRptaEsc":{"label":"string","value":"string"},"num_DVClteNoCont":{"label":"string","value":"string"},"num_DVCartaEnRev":{"label":"string","value":"string"},"num_DVAboTempA":{"label":"string","value":"string"},"num_DVAboTempP":{"label":"string","value":"string"},"txt_Autor":{"label":"string","value":"string"},"txt_Creador":{"label":"string","value":"string"},"key_NomSucCaptura":{"label":"string","value":"string"},"key_SucursalFilial":{"label":"string","value":"string"},"txt_RgnCaptura":{"label":"string","value":"string"},"txt_ZonaCaptura":{"label":"string","value":"string"},"key_ReportadoPor":{"label":"string","value":"string"},"key_OrigenReporte":{"label":"string","value":"string"},"nom_Responsable_1":{"label":"string","value":"string"},"num_AbonoTemp":{"label":"string","value":"string"},"key_ResponError":{"label":"string","value":"string"},"txt_NomRespError":{"label":"string","value":"string"},"key_ClienteRazon":{"label":"string","value":"string"},"key_CausaNoAbono":{"label":"string","value":"string"},"nom_UsuCierra":{"label":"string","value":"string"},"num_DRecl":{"label":"string","value":"string"},"key_MedioRpta":{"label":"string","value":"string"},"key_TipoDocumento":{"label":"string","value":"string"},"txt_TipoCliente":{"label":"string","value":"string"},"txt_FuncEmpresa":{"label":"string","value":"string"},"txt_SubSgmtoCliente":{"label":"string","value":"string"},"txt_ClteGer":{"label":"string","value":"string"},"txt_GteCuenta":{"label":"string","value":"string"},"txt_ClteColombia":{"label":"string","value":"string"},"txt_CiudadCorr":{"label":"string","value":"string"},"txt_DescCiudadCorr":{"label":"string","value":"string"},"txt_DptoCorr":{"label":"string","value":"string"},"txt_DirCorr":{"label":"string","value":"string"},"txt_Pais":{"label":"string","value":"string"},"txt_Estado":{"label":"string","value":"string"},"txt_DirPostal":{"label":"string","value":"string"},"txt_ApartAereo":{"label":"string","value":"string"},"txt_IdDireccion":{"label":"string","value":"string"},"key_RptaReclamo":{"label":"string","value":"string"},"txt_TeleCliente":{"label":"string","value":"string"},"txt_FaxCliente":{"label":"string","value":"string"},"txt_EmailCliente":{"label":"string","value":"string"},"nom_RespGer":{"label":"string","value":"string"},"txt_TelRespGer":{"label":"string","value":"string"},"num_TpoEstSol":{"label":"string","value":"string"},"dat_FchAproxSol":{"label":"string","value":"string"},"num_DAbierto":{"label":"string","value":"string"},"dat_Solucionado":{"label":"string","value":"string"},"txt_Cia":{"label":"string","value":"string"},"key_TipoTarjeta":{"label":"string","value":"string"},"key_Prefijo":{"label":"string","value":"string"},"txt_NumProducto":{"label":"string","value":"string"},"txt_NroCompraRapp":{"label":"string","value":"string"},"txt_NroProductoRapp":{"label":"string","value":"string"},"txt_SubprocRecl":{"label":"string","value":"string"},"txt_NumTarjDeb":{"label":"string","value":"string"},"txt_NumPoliza":{"label":"string","value":"string"},"txt_NumEncargo":{"label":"string","value":"string"},"key_NomSucRadica":{"label":"string","value":"string"},"txt_TipoPoliza":{"label":"string","value":"string"},"txt_NumTjtaCtaDeb":{"label":"string","value":"string"},"key_CiudadTransa":{"label":"string","value":"string"},"dat_FchTransac":{"label":"string","value":"string"},"key_NomSucVarios":{"label":"string","value":"string"},"txt_CodEstab":{"label":"string","value":"string"},"txt_MensajeAyuda":{"label":"string","value":"string"},"txt_MensajeSoportes":{"label":"string","value":"string"},"txt_ComentRapp":{"label":"string","value":"string"},"txt_SopRecCte":{"label":"string","value":"string"},"txt_SopNoRec":{"label":"string","value":"string"},"key_TodosSoportes":{"label":"string","value":"string"},"rch_Pantallas":{"label":"string","value":"string"},"rch_PantallaSln":{"label":"string","value":"string"},"rch_RptaCliente":{"label":"string","value":"string"},"txt_NomOpTblM":{"label":"string","value":"string"},"key_VlrsOpTblM":{"label":"string","value":"string"},"num_ValorTransa_1":{"label":"string","value":"string"},"num_ValorTransa_2":{"label":"string","value":"string"},"num_ValorTransa_3":{"label":"string","value":"string"},"num_ValorTransa_4":{"label":"string","value":"string"},"num_ValorTransa_5":{"label":"string","value":"string"},"num_ValorTransa_6":{"label":"string","value":"string"},"num_ValorTransa_7":{"label":"string","value":"string"},"num_ValorTransa_8":{"label":"string","value":"string"},"num_ValorTransa_9":{"label":"string","value":"string"},"dat_FechaTransa_1":{"label":"string","value":"string"},"dat_FechaTransa_2":{"label":"string","value":"string"},"dat_FechaTransa_3":{"label":"string","value":"string"},"dat_FechaTransa_4":{"label":"string","value":"string"},"dat_FechaTransa_5":{"label":"string","value":"string"},"dat_FechaTransa_6":{"label":"string","value":"string"},"dat_FechaTransa_7":{"label":"string","value":"string"},"dat_FechaTransa_8":{"label":"string","value":"string"},"dat_FechaTransa_9":{"label":"string","value":"string"},"key_OficinaTransa_1":{"label":"string","value":"string"},"key_OficinaTransa_2":{"label":"string","value":"string"},"key_OficinaTransa_3":{"label":"string","value":"string"},"key_OficinaTransa_4":{"label":"string","value":"string"},"key_OficinaTransa_5":{"label":"string","value":"string"},"key_OficinaTransa_6":{"label":"string","value":"string"},"key_OficinaTransa_7":{"label":"string","value":"string"},"key_OficinaTransa_8":{"label":"string","value":"string"},"key_OficinaTransa_9":{"label":"string","value":"string"},"txt_Referencia_1":{"label":"string","value":"string"},"txt_Referencia_2":{"label":"string","value":"string"},"txt_Referencia_3":{"label":"string","value":"string"},"txt_Referencia_4":{"label":"string","value":"string"},"txt_Referencia_5":{"label":"string","value":"string"},"txt_Referencia_6":{"label":"string","value":"string"},"txt_Referencia_7":{"label":"string","value":"string"},"txt_Referencia_8":{"label":"string","value":"string"},"txt_Referencia_9":{"label":"string","value":"string"},"key_Moneda_1":{"label":"string","value":"string"},"key_Moneda_2":{"label":"string","value":"string"},"key_Moneda_3":{"label":"string","value":"string"},"key_Moneda_4":{"label":"string","value":"string"},"key_Moneda_5":{"label":"string","value":"string"},"key_Moneda_6":{"label":"string","value":"string"},"key_Moneda_7":{"label":"string","value":"string"},"key_Moneda_8":{"label":"string","value":"string"},"key_Moneda_9":{"label":"string","value":"string"},"key_CiudadTransa_1":{"label":"string","value":"string"},"key_CiudadTransa_2":{"label":"string","value":"string"},"key_CiudadTransa_3":{"label":"string","value":"string"},"key_CiudadTransa_4":{"label":"string","value":"string"},"key_CiudadTransa_5":{"label":"string","value":"string"},"key_CiudadTransa_6":{"label":"string","value":"string"},"key_CiudadTransa_7":{"label":"string","value":"string"},"key_CiudadTransa_8":{"label":"string","value":"string"},"key_CiudadTransa_9":{"label":"string","value":"string"},"txt_NumeroCuenta_1":{"label":"string","value":"string"},"txt_NumeroCuenta_2":{"label":"string","value":"string"},"txt_NumeroCuenta_3":{"label":"string","value":"string"},"txt_NumeroCuenta_4":{"label":"string","value":"string"},"txt_NumeroCuenta_5":{"label":"string","value":"string"},"txt_NumeroCuenta_6":{"label":"string","value":"string"},"txt_NumeroCuenta_7":{"label":"string","value":"string"},"txt_NumeroCuenta_8":{"label":"string","value":"string"},"txt_NumeroCuenta_9":{"label":"string","value":"string"},"num_ChequeVouch_1":{"label":"string","value":"string"},"num_ChequeVouch_2":{"label":"string","value":"string"},"num_ChequeVouch_3":{"label":"string","value":"string"},"num_ChequeVouch_4":{"label":"string","value":"string"},"num_ChequeVouch_5":{"label":"string","value":"string"},"num_ChequeVouch_6":{"label":"string","value":"string"},"num_ChequeVouch_7":{"label":"string","value":"string"},"num_ChequeVouch_8":{"label":"string","value":"string"},"num_ChequeVouch_9":{"label":"string","value":"string"},"dat_FechaConsigna_1":{"label":"string","value":"string"},"dat_FechaConsigna_2":{"label":"string","value":"string"},"dat_FechaConsigna_3":{"label":"string","value":"string"},"dat_FechaConsigna_4":{"label":"string","value":"string"},"dat_FechaConsigna_5":{"label":"string","value":"string"},"dat_FechaConsigna_6":{"label":"string","value":"string"},"dat_FechaConsigna_7":{"label":"string","value":"string"},"dat_FechaConsigna_8":{"label":"string","value":"string"},"dat_FechaConsigna_9":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

reclamos_old_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")
sqlContext.registerDataFrameAsTable(reclamos_old_df, "Zdc_Lotus_Reclamos_old_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_MensajeAyuda como String
reclamos_txt_MensajeAyuda_string =  ["{'UniversalID':'string','txt_MensajeAyuda':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_MensajeAyuda_string_rdd = sc.parallelize(reclamos_txt_MensajeAyuda_string)
reclamos_txt_MensajeAyuda_string_json = spark.read.json(reclamos_txt_MensajeAyuda_string_rdd)

reclamos_txt_MensajeAyuda_string_df = spark.read.schema(reclamos_txt_MensajeAyuda_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_txt_MensajeAyuda_string_df = reclamos_txt_MensajeAyuda_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeAyuda.value as txt_MensajeAyuda")
reclamos_txt_MensajeAyuda_string_df = reclamos_txt_MensajeAyuda_string_df.filter(reclamos_txt_MensajeAyuda_string_df.txt_MensajeAyuda.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeAyuda_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_MensajeAyuda como Array
reclamos_txt_MensajeAyuda_array = ["{'UniversalID':'string','txt_MensajeAyuda':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_MensajeAyuda_array_rdd = sc.parallelize(reclamos_txt_MensajeAyuda_array)
reclamos_txt_MensajeAyuda_array_json = spark.read.json(reclamos_txt_MensajeAyuda_array_rdd)

reclamos_txt_MensajeAyuda_array_df = spark.read.schema(reclamos_txt_MensajeAyuda_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeAyuda.value as txt_MensajeAyuda")
reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.filter(col("UniversalID").isNotNull())
reclamos_txt_MensajeAyuda_array_df = reclamos_txt_MensajeAyuda_array_df.select("UniversalID","dat_FchCreac",array_join("txt_MensajeAyuda", '\r\n ').alias("txt_MensajeAyuda"))
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeAyuda_array")

# COMMAND ----------

reclamos_txt_MensajeAyuda_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeAyuda_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeAyuda_array")
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeAyuda_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeAyuda")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_MensajeSoportes como String
reclamos_txt_MensajeSoportes_string =  ["{'UniversalID':'string','txt_MensajeSoportes':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_MensajeSoportes_string_rdd = sc.parallelize(reclamos_txt_MensajeSoportes_string)
reclamos_txt_MensajeSoportes_string_json = spark.read.json(reclamos_txt_MensajeSoportes_string_rdd)

reclamos_txt_MensajeSoportes_string_df = spark.read.schema(reclamos_txt_MensajeSoportes_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_txt_MensajeSoportes_string_df = reclamos_txt_MensajeSoportes_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeSoportes.value as txt_MensajeSoportes")
reclamos_txt_MensajeSoportes_string_df = reclamos_txt_MensajeSoportes_string_df.filter(reclamos_txt_MensajeSoportes_string_df.txt_MensajeSoportes.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeSoportes_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_MensajeSoportes como Array
reclamos_txt_MensajeSoportes_array = ["{'UniversalID':'string','txt_MensajeSoportes':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_MensajeSoportes_array_rdd = sc.parallelize(reclamos_txt_MensajeSoportes_array)
reclamos_txt_MensajeSoportes_array_json = spark.read.json(reclamos_txt_MensajeSoportes_array_rdd)

reclamos_txt_MensajeSoportes_array_df = spark.read.schema(reclamos_txt_MensajeSoportes_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_MensajeSoportes.value as txt_MensajeSoportes")
reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.filter(col("UniversalID").isNotNull())
reclamos_txt_MensajeSoportes_array_df = reclamos_txt_MensajeSoportes_array_df.select("UniversalID","dat_FchCreac",array_join("txt_MensajeSoportes", '\r\n ').alias("txt_MensajeSoportes"))
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeSoportes_array")

# COMMAND ----------

reclamos_txt_MensajeSoportes_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_MensajeSoportes_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txt_MensajeSoportes_array")
sqlContext.registerDataFrameAsTable(reclamos_txt_MensajeSoportes_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeSoportes")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_Pantalla como String
reclamos_rch_Pantallas_string =  ["{'UniversalID':'string','rch_Pantallas':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_Pantallas_string_rdd = sc.parallelize(reclamos_rch_Pantallas_string)
reclamos_rch_Pantallas_string_json = spark.read.json(reclamos_rch_Pantallas_string_rdd)

reclamos_rch_Pantallas_string_df = spark.read.schema(reclamos_rch_Pantallas_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_rch_Pantallas_string_df = reclamos_rch_Pantallas_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_Pantallas.value as rch_Pantallas")
reclamos_rch_Pantallas_string_df = reclamos_rch_Pantallas_string_df.filter(reclamos_rch_Pantallas_string_df.rch_Pantallas.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_string")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_Pantalla como Array
reclamos_rch_Pantallas_array = ["{'UniversalID':'string','rch_Pantallas':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_Pantallas_array_rdd = sc.parallelize(reclamos_rch_Pantallas_array)
reclamos_rch_Pantallas_array_json = spark.read.json(reclamos_rch_Pantallas_array_rdd)

reclamos_rch_Pantallas_array_df = spark.read.schema(reclamos_rch_Pantallas_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_Pantallas.value as rch_Pantallas")
reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.filter(col("UniversalID").isNotNull())
reclamos_rch_Pantallas_array_df = reclamos_rch_Pantallas_array_df.select("UniversalID","dat_FchCreac",array_join("rch_Pantallas", '\r\n ').alias("rch_Pantallas"))
sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_array")

# COMMAND ----------

reclamos_rch_Pantallas_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_Pantallas_array")
sqlContext.registerDataFrameAsTable(reclamos_rch_Pantallas_df, "Zdc_Lotus_Reclamos_Tablaintermedia_rch_Pantallas")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_PantallaSln como String
reclamos_rch_PantallaSln_string =  ["{'UniversalID':'string','rch_PantallaSln':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_PantallaSln_string_rdd = sc.parallelize(reclamos_rch_PantallaSln_string)
reclamos_rch_PantallaSln_string_json = spark.read.json(reclamos_rch_PantallaSln_string_rdd)

reclamos_rch_PantallaSln_string_df = spark.read.schema(reclamos_rch_PantallaSln_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_rch_PantallaSln_string_df = reclamos_rch_PantallaSln_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_PantallaSln.value as rch_PantallaSln")
reclamos_rch_PantallaSln_string_df = reclamos_rch_PantallaSln_string_df.filter(reclamos_rch_PantallaSln_string_df.rch_PantallaSln.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_string")

# COMMAND ----------

# DBTITLE 1,Esquema con rch_PantallaSln como Array
reclamos_rch_PantallaSln_array = ["{'UniversalID':'string','rch_PantallaSln':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_PantallaSln_array_rdd = sc.parallelize(reclamos_rch_PantallaSln_array)
reclamos_rch_PantallaSln_array_json = spark.read.json(reclamos_rch_PantallaSln_array_rdd)

reclamos_rch_PantallaSln_array_df = spark.read.schema(reclamos_rch_PantallaSln_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_PantallaSln.value as rch_PantallaSln")
reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.filter(col("UniversalID").isNotNull())
reclamos_rch_PantallaSln_array_df = reclamos_rch_PantallaSln_array_df.select("UniversalID","dat_FchCreac",array_join("rch_PantallaSln", '\r\n ').alias("rch_PantallaSln"))
sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_array")

# COMMAND ----------

reclamos_rch_PantallaSln_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln_array")
sqlContext.registerDataFrameAsTable(reclamos_rch_PantallaSln_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como String
reclamos_txt_Comentarios_string =  ["{'UniversalID':'string','txt_Comentarios':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_Comentarios_string_rdd = sc.parallelize(reclamos_txt_Comentarios_string)
reclamos_txt_Comentarios_string_json = spark.read.json(reclamos_txt_Comentarios_string_rdd)

reclamos_txt_Comentarios_string_df = spark.read.schema(reclamos_txt_Comentarios_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_txt_Comentarios_string_df = reclamos_txt_Comentarios_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
reclamos_txt_Comentarios_string_df = reclamos_txt_Comentarios_string_df.filter(reclamos_txt_Comentarios_string_df.txt_Comentarios.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Comentarios como Array
reclamos_txt_Comentarios_array = ["{'UniversalID':'string','txt_Comentarios':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_Comentarios_array_rdd = sc.parallelize(reclamos_txt_Comentarios_array)
reclamos_txt_Comentarios_array_json = spark.read.json(reclamos_txt_Comentarios_array_rdd)

reclamos_txt_Comentarios_array_df = spark.read.schema(reclamos_txt_Comentarios_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Comentarios.value as txt_Comentarios")
reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.filter(col("UniversalID").isNotNull())
reclamos_txt_Comentarios_array_df = reclamos_txt_Comentarios_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Comentarios", '\r\n ').alias("txt_Comentarios"))
sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_array")

# COMMAND ----------

reclamos_txt_Comentarios_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios_array")
sqlContext.registerDataFrameAsTable(reclamos_txt_Comentarios_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como String
reclamos_txt_Historia_string =  ["{'UniversalID':'string','txt_Historia':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_Historia_string_rdd = sc.parallelize(reclamos_txt_Historia_string)
reclamos_txt_Historia_string_json = spark.read.json(reclamos_txt_Historia_string_rdd)

reclamos_txt_Historia_string_df = spark.read.schema(reclamos_txt_Historia_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_txt_Historia_string_df = reclamos_txt_Historia_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
reclamos_txt_Historia_string_df = reclamos_txt_Historia_string_df.filter(reclamos_txt_Historia_string_df.txt_Historia.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_Historia como Array
reclamos_txt_Historia_array = ["{'UniversalID':'string','txt_Historia':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_Historia_array_rdd = sc.parallelize(reclamos_txt_Historia_array)
reclamos_txt_Historia_array_json = spark.read.json(reclamos_txt_Historia_array_rdd)

reclamos_txt_Historia_array_df = spark.read.schema(reclamos_txt_Historia_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_Historia.value as txt_Historia")
reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.filter(col("UniversalID").isNotNull())
reclamos_txt_Historia_array_df = reclamos_txt_Historia_array_df.select("UniversalID","dat_FchCreac",array_join("txt_Historia", '\r\n ').alias("txt_Historia"))
sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_array")

# COMMAND ----------

reclamos_txt_Historia_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia_string")
sqlContext.registerDataFrameAsTable(reclamos_txt_Historia_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia")

# COMMAND ----------

# DBTITLE 1,Esquema con RptaClient como String
reclamos_rch_RptaCliente_string =  ["{'UniversalID':'string','rch_RptaCliente':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_RptaCliente_string_rdd = sc.parallelize(reclamos_rch_RptaCliente_string)
reclamos_rch_RptaCliente_string_json = spark.read.json(reclamos_rch_RptaCliente_string_rdd)

reclamos_rch_RptaCliente_string_df = spark.read.schema(reclamos_rch_RptaCliente_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_rch_RptaCliente_string_df = reclamos_rch_RptaCliente_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_RptaCliente.value as rch_RptaCliente")
reclamos_rch_RptaCliente_string_df = reclamos_rch_RptaCliente_string_df.filter(reclamos_rch_RptaCliente_string_df.rch_RptaCliente.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_string")

# COMMAND ----------

# DBTITLE 1,Esquema con RptaClient como Array
reclamos_rch_RptaCliente_array = ["{'UniversalID':'string','rch_RptaCliente':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_rch_RptaCliente_array_rdd = sc.parallelize(reclamos_rch_RptaCliente_array)
reclamos_rch_RptaCliente_array_json = spark.read.json(reclamos_rch_RptaCliente_array_rdd)

reclamos_rch_RptaCliente_array_df = spark.read.schema(reclamos_rch_RptaCliente_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","rch_RptaCliente.value as rch_RptaCliente")
reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.filter(col("UniversalID").isNotNull())
reclamos_rch_RptaCliente_array_df = reclamos_rch_RptaCliente_array_df.select("UniversalID","dat_FchCreac",array_join("rch_RptaCliente", '\r\n ').alias("rch_RptaCliente"))
sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_array")

# COMMAND ----------

reclamos_rch_RptaCliente_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente_string")
sqlContext.registerDataFrameAsTable(reclamos_rch_RptaCliente_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_NomOpTblM como String
reclamos_txt_NomOpTblM_string =  ["{'UniversalID':'string','txt_NomOpTblM':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_NomOpTblM_string_rdd = sc.parallelize(reclamos_txt_NomOpTblM_string)
reclamos_txt_NomOpTblM_string_json = spark.read.json(reclamos_txt_NomOpTblM_string_rdd)

reclamos_txt_NomOpTblM_string_df = spark.read.schema(reclamos_txt_NomOpTblM_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_txt_NomOpTblM_string_df = reclamos_txt_NomOpTblM_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_NomOpTblM.value as txt_NomOpTblM")
reclamos_txt_NomOpTblM_string_df = reclamos_txt_NomOpTblM_string_df.filter(reclamos_txt_NomOpTblM_string_df.txt_NomOpTblM.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txt_NomOpTblM como Array
reclamos_txt_NomOpTblM_array = ["{'UniversalID':'string','txt_NomOpTblM':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txt_NomOpTblM_array_rdd = sc.parallelize(reclamos_txt_NomOpTblM_array)
reclamos_txt_NomOpTblM_array_json = spark.read.json(reclamos_txt_NomOpTblM_array_rdd)

reclamos_txt_NomOpTblM_array_df = spark.read.schema(reclamos_txt_NomOpTblM_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","txt_NomOpTblM.value as txt_NomOpTblM")
reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.filter(col("UniversalID").isNotNull())
reclamos_txt_NomOpTblM_array_df = reclamos_txt_NomOpTblM_array_df.select("UniversalID","dat_FchCreac",array_join("txt_NomOpTblM", '\r\n ').alias("txt_NomOpTblM"))
sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_array")

# COMMAND ----------

reclamos_txt_NomOpTblM_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_array UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM_string")
sqlContext.registerDataFrameAsTable(reclamos_txt_NomOpTblM_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM")

# COMMAND ----------

# DBTITLE 1,Esquema con key_VlrsOpTblM como String
reclamos_key_VlrsOpTblM_string =  ["{'UniversalID':'string','key_VlrsOpTblM':{'label':'string','value':'string'},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_key_VlrsOpTblM_string_rdd = sc.parallelize(reclamos_key_VlrsOpTblM_string)
reclamos_key_VlrsOpTblM_string_json = spark.read.json(reclamos_key_VlrsOpTblM_string_rdd)

reclamos_key_VlrsOpTblM_string_df = spark.read.schema(reclamos_key_VlrsOpTblM_string_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw/")

reclamos_key_VlrsOpTblM_string_df = reclamos_key_VlrsOpTblM_string_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_VlrsOpTblM.value as key_VlrsOpTblM")
reclamos_key_VlrsOpTblM_string_df = reclamos_key_VlrsOpTblM_string_df.filter(reclamos_key_VlrsOpTblM_string_df.key_VlrsOpTblM.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_string")

# COMMAND ----------

# DBTITLE 1,Esquema con key_VlrsOpTblM como Array
reclamos_key_VlrsOpTblM_array = ["{'UniversalID':'string','key_VlrsOpTblM':{'label':'string', 'value':['array']},'dat_FchCreac':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_key_VlrsOpTblM_array_rdd = sc.parallelize(reclamos_key_VlrsOpTblM_array)
reclamos_key_VlrsOpTblM_array_json = spark.read.json(reclamos_key_VlrsOpTblM_array_rdd)

reclamos_key_VlrsOpTblM_array_df = spark.read.schema(reclamos_key_VlrsOpTblM_array_json.schema).json("/mnt/lotus-reclamos/reclamos-old/raw")

from pyspark.sql.functions import col,array_join
reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.selectExpr("UniversalID","dat_FchCreac.value as dat_FchCreac","key_VlrsOpTblM.value as key_VlrsOpTblM")
reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.filter(col("UniversalID").isNotNull())
reclamos_key_VlrsOpTblM_array_df = reclamos_key_VlrsOpTblM_array_df.select("UniversalID","dat_FchCreac",array_join("key_VlrsOpTblM", '\r\n ').alias("key_VlrsOpTblM"))
sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_array")

# COMMAND ----------

reclamos_key_VlrsOpTblM_df = sqlContext.sql("SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_string UNION SELECT * FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM_array")
sqlContext.registerDataFrameAsTable(reclamos_key_VlrsOpTblM_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM")

# COMMAND ----------

# DBTITLE 1,Base de Datos Auxiliar
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_ProcessOptimizado;
# MAGIC USE Lotus_ProcessOptimizado;

# COMMAND ----------

# DBTITLE 1,Tabla Intermedia Auxiliar
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_old_TablaIntermedia_Aux;
# MAGIC CREATE EXTERNAL TABLE Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_old_TablaIntermedia_Aux
# MAGIC (
# MAGIC   UniversalID string,
# MAGIC   num_VlrTotal map<string,string>,
# MAGIC   dat_FchCreac map<string,string>,
# MAGIC   num_VlrTotal_1 map<string,string>,
# MAGIC   num_VlrTotalD map<string,string>,
# MAGIC   num_VlrTotalD_1 map<string,string>,
# MAGIC   key_NumTransa map<string,string>,
# MAGIC   key_CamposTransa map<string,array<string>>,
# MAGIC   key_CamposRecl map<string,array<string>>
# MAGIC  )
# MAGIC ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos-old/raw/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes_Aux;
# MAGIC CREATE VIEW Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes_Aux AS
# MAGIC SELECT UniversalID,
# MAGIC        dat_FchCreac.value,
# MAGIC        num_VlrTotal.value AS num_VlrTotal,
# MAGIC        num_VlrTotal_1.value AS num_VlrTotal_1,
# MAGIC        num_VlrTotalD.value AS num_VlrTotalD,
# MAGIC        num_VlrTotalD_1.value AS num_VlrTotalD_1,
# MAGIC        CASE 
# MAGIC          WHEN (key_NumTransa.value != '' AND COALESCE(cast(key_NumTransa.value as int), 0)  >= 1) OR !array_contains(key_CamposRecl.value, '8') OR array_contains(key_CamposTransa.value, '4') THEN TRUE
# MAGIC          ELSE FALSE
# MAGIC        END AS EsnVlrTotalVlrTotalDVisible,
# MAGIC        CASE
# MAGIC          WHEN array_contains(key_CamposRecl.value, '8') OR !array_contains(key_CamposTransa.value, '4') THEN TRUE
# MAGIC          ELSE FALSE
# MAGIC        END AS EsVlrTotal_1VlrTotalD_1Visible
# MAGIC FROM 
# MAGIC   Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_old_TablaIntermedia_Aux;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes;
# MAGIC CREATE VIEW Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes AS
# MAGIC SELECT
# MAGIC   TblIntermedia.UniversalID As UniversalID,
# MAGIC   dat_FchCreac.value dat_FchCreac,
# MAGIC   CASE
# MAGIC     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN ''
# MAGIC     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotal.value
# MAGIC     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotal_1.value
# MAGIC     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotal_1.value)        
# MAGIC   END AS ValorTotalEnPesos,
# MAGIC   CASE
# MAGIC     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN ''
# MAGIC     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotalD.value
# MAGIC     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotalD_1.value
# MAGIC     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN concat_ws('\n', TblIntermedia.num_VlrTotalD.value || ',' || TblIntermedia.num_VlrTotalD_1.value)        
# MAGIC   END AS ValorTotalEnDolares
# MAGIC FROM 
# MAGIC   Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_old_TablaIntermedia_Aux AS TblIntermedia
# MAGIC   INNER JOIN Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes_Aux As TblAuxIntermedia ON TblIntermedia.UniversalID = TblAuxIntermedia.UniversalID;

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/reclamos-old/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/reclamos-old/5_LoadSchema $from_create_schema = True