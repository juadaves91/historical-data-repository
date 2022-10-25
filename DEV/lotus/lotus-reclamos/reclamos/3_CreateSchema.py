# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE CRUDOS

# COMMAND ----------

# DBTITLE 1,Dummy con los campos txtComentario 
dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","TituloDB":"string","attachments":["array"],"cbCiudadCliente":{"label":"string","value":"string"},"cbCiudadTransaccion":{"label":"string","value":"string"},"cbOrigenReporte":{"label":"string","value":"string"},"cbProductoAfectado":{"label":"string","value":"string"},"cbReportadoPor":{"label":"string","value":"string"},"cbResponsableError":{"label":"string","value":"string"},"cbSucursalRadicacion":{"label":"string","value":"string"},"cbSucursalVarios":{"label":"string","value":"string"},"cbTipoPoliza":{"label":"string","value":"string"},"dtConsignacionesTransac":{"Label":"string","value":["array"]},"dtFechaCreacion":{"label":"string","value":"string"},"dtFechaSlnRpta":{"label":"string","value":"string"},"dtFechaSolucionReal":{"label":"string","value":"string"},"dtFechasTrans":{"Label":"string","value":["array"]},"dtFechasTransac":{"Label":"string","value":["array"]},"dtFechaTransaccion":{"label":"string","value":"string"},"numAbonoDefinitivo":{"label":"string","value":"string"},"numAbonoTemporal":{"label":"string","value":"string"},"numContabilizaTrans":{"Label":"string","value":["array"]},"numDiasTrans":{"Label":"string","value":["array"]},"numTiempoSlnRpta":{"label":"string","value":"string"},"numTiempoSolucion":{"label":"string","value":"string"},"numTiempoSolucionReal":{"label":"string","value":"string"},"numTotContabiliza":{"label":"string","value":"string"},"numTotDias":{"label":"string","value":"string"},"NumTotVencidos":{"label":"string","value":"string"},"numValorDolares":{"label":"string","value":"string"},"numValorDolaresC":{"label":"string","value":"string"},"numValoresTransac":{"Label":"string","value":["array"]},"numValorPesos":{"label":"string","value":"string"},"numValorPesosC":{"label":"string","value":"string"},"numVencidosTrans":{"Label":"string","value":["array"]},"rdAutorizaEmail":{"label":"string","value":"string"},"rdClienteRazon":{"label":"string","value":"string"},"rdTodosSoportes":{"label":"string","value":"string"},"rtPantallasReclamo":{"label":"string","value":"string"},"rtPantallasSolucion":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"txtCelular":{"label":"string","value":"string"},"txtChequesTransac":{"Label":"string","value":["array"]},"txtCiudadesTransac":{"Label":"string","value":["array"]},"txtClasificacion":{"label":"string","value":"string"},"txtCodCanal":{"label":"string","value":"string"},"txtCodCiudadCliente":{"label":"string","value":"string"},"txtCodConvenio":{"label":"string","value":"string"},"txtCodigoCompra":{"label":"string","value":"string"},"txtCodigoEstablecimiento":{"label":"string","value":"string"},"txtCodigoProducto":{"label":"string","value":"string"},"txtComentario":{"Label":"string","value":["array"]},"txtCuentasTransac":{"Label":"string","value":["array"]},"txtDeptoCliente":{"label":"string","value":"string"},"txtDireccionCliente":{"label":"string","value":"string"},"txtEmailCliente":{"label":"string","value":"string"},"txtEstadoTrans":{"Label":"string","value":["array"]},"txtFax":{"label":"string","value":"string"},"txtFuncionarioEmpresa":{"label":"string","value":"string"},"txtGerenteCuenta":{"label":"string","value":"string"},"txtHistoria":{"Label":"string","value":["array"]},"txtMensajeAyuda":{"Label":"string","value":["array"]},"txtMonedasTransac":{"Label":"string","value":["array"]},"txtNitEmpresa":{"label":"string","value":"string"},"txtNomCanal":{"label":"string","value":"string"},"txtNomCliente":{"label":"string","value":"string"},"txtNomEmpresa":{"label":"string","value":"string"},"txtNomRespError":{"label":"string","value":"string"},"txtNumEncargoFiducia":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtNumPoliza":{"label":"string","value":"string"},"txtNumProducto":{"label":"string","value":"string"},"txtNumTarjetaCtaDebita":{"label":"string","value":"string"},"txtNumTarjetaDebito":{"label":"string","value":"string"},"txtObservaciones":{"Label":"string","value":["array"]},"txtOficinasTransac":{"Label":"string","value":["array"]},"txtPantallasReclamo":{"Label":"string","value":["array"]},"txtPantallasSolucion":{"Label":"string","value":["array"]},"txtRadicado":{"label":"string","value":"string"},"txtReferenciasTransac":{"Label":"string","value":["array"]},"txtResponsable":{"label":"string","value":"string"},"txtRespuesta":{"Label":"string","value":["array"]},"txtSegmento":{"label":"string","value":"string"},"txtSoportesNoRecibidos":{"label":"string","value":"string"},"txtSoportesRecibidos":{"label":"string","value":"string"},"txtSoportesRequeridos":{"Label":"string","value":["array"]},"TXTSUCCAPTURA":{"label":"string","value":"string"},"txtTelefonosCliente":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

reclamos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")
sqlContext.registerDataFrameAsTable(reclamos_df, "Zdc_Lotus_Reclamos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Metadata
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-reclamos-storage/reclamos' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE Zdc_Lotus_Reclamos_TablaIntermedia""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

# DBTITLE 1,Dummy con los campos omitidos
dummy = ['{"UniversalID":"string","UniversalIDCompuesto":"string","TituloDB":"string","attachments":["array"],"cbCiudadCliente":{"label":"string","value":"string"},"cbCiudadTransaccion":{"label":"string","value":"string"},"cbOrigenReporte":{"label":"string","value":"string"},"cbProductoAfectado":{"label":"string","value":"string"},"cbReportadoPor":{"label":"string","value":"string"},"cbResponsableError":{"label":"string","value":"string"},"cbResponsable":{"label":"string","value":"string"},"cbSucursalRadicacion":{"label":"string","value":"string"},"cbSucursalVarios":{"label":"string","value":"string"},"cbTipoPoliza":{"label":"string","value":"string"},"dtFechaCreacion":{"label":"string","value":"string"},"dtFechaSlnRpta":{"label":"string","value":"string"},"dtFechaSolucionReal":{"label":"string","value":"string"},"dtFechaTransaccion":{"label":"string","value":"string"},"numAbonoDefinitivo":{"label":"string","value":"string"},"numAbonoTemporal":{"label":"string","value":"string"},"numTiempoSlnRpta":{"label":"string","value":"string"},"numTiempoSolucion":{"label":"string","value":"string"},"numTiempoSolucionReal":{"label":"string","value":"string"},"numTotContabiliza":{"label":"string","value":"string"},"numTotDias":{"label":"string","value":"string"},"NumTotVencidos":{"label":"string","value":"string"},"numValorDolares":{"label":"string","value":"string"},"numValorDolaresC":{"label":"string","value":"string"},"numValorPesos":{"label":"string","value":"string"},"numValorPesosC":{"label":"string","value":"string"},"rdAutorizaEmail":{"label":"string","value":"string"},"rdClienteRazon":{"label":"string","value":"string"},"rdTodosSoportes":{"label":"string","value":"string"},"rtPantallasReclamo":{"label":"string","value":"string"},"rtPantallasSolucion":{"label":"string","value":"string"},"txtAutor":{"label":"string","value":"string"},"txtCelular":{"label":"string","value":"string"},"txtClasificacion":{"label":"string","value":"string"},"txtCodCanal":{"label":"string","value":"string"},"txtCodCiudadCliente":{"label":"string","value":"string"},"txtCodConvenio":{"label":"string","value":"string"},"txtCodigoCompra":{"label":"string","value":"string"},"txtCodigoEstablecimiento":{"label":"string","value":"string"},"txtCodigoProducto":{"label":"string","value":"string"},"txtDeptoCliente":{"label":"string","value":"string"},"txtDireccionCliente":{"label":"string","value":"string"},"txtEmailCliente":{"label":"string","value":"string"},"txtFax":{"label":"string","value":"string"},"txtFuncionarioEmpresa":{"label":"string","value":"string"},"txtGerenteCuenta":{"label":"string","value":"string"},"txtNitEmpresa":{"label":"string","value":"string"},"txtNomCanal":{"label":"string","value":"string"},"txtNomCliente":{"label":"string","value":"string"},"txtNomEmpresa":{"label":"string","value":"string"},"txtNomRespError":{"label":"string","value":"string"},"txtNumEncargoFiducia":{"label":"string","value":"string"},"txtNumIdCliente":{"label":"string","value":"string"},"txtNumPoliza":{"label":"string","value":"string"},"txtNumProducto":{"label":"string","value":"string"},"txtNumTarjetaCtaDebita":{"label":"string","value":"string"},"txtNumTarjetaDebito":{"label":"string","value":"string"},"txtRadicado":{"label":"string","value":"string"},"txtResponsable":{"label":"string","value":"string"},"txtSegmento":{"label":"string","value":"string"},"txtSubsegmento":{"label":"string","value":"string"},"txtSoportesNoRecibidos":{"label":"string","value":"string"},"txtSoportesRecibidos":{"label":"string","value":"string"},"TXTSUCCAPTURA":{"label":"string","value":"string"},"txtTelefonosCliente":{"label":"string","value":"string"},"txtTipoProducto":{"label":"string","value":"string"},"txtEstado":{"label":"string","value":"string"},"txtTipoReclamo":{"label":"string","value":"string"}}']

sc = spark.sparkContext
schema_rdd = sc.parallelize(dummy)
schema_json = spark.read.json(schema_rdd)

# COMMAND ----------

# DBTITLE 1,Dataframe Tabla Intermedia
reclamos_df = spark.read.schema(schema_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")
sqlContext.registerDataFrameAsTable(reclamos_df, "Zdc_Lotus_Reclamos_TablaIntermedia_Aux")

# COMMAND ----------

reclamos_df = sqlContext.sql("\
  SELECT \
      if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, \
      UniversalIDCompuesto, \
      UniversalID as UniversalIDPadre, \
      TituloDB, \
      NumTotVencidos.value NumTotVencidos, \
      txtEstado.value Estado, \
      TXTSUCCAPTURA.value TXTSUCCAPTURA, \
      attachments, \
      cbCiudadCliente.value cbCiudadCliente, \
      cbCiudadTransaccion.value cbCiudadTransaccion, \
      cbOrigenReporte.value cbOrigenReporte, \
      cbProductoAfectado.value cbProductoAfectado, \
      cbReportadoPor.value cbReportadoPor, \
      cbResponsableError.value cbResponsableError, \
      cbResponsable.value cbResponsable, \
      cbSucursalRadicacion.value cbSucursalRadicacion, \
      cbSucursalVarios.value cbSucursalVarios, \
      cbTipoPoliza.value cbTipoPoliza, \
      dtFechaCreacion.value FechaCreacion, \
      dtFechaSlnRpta.value dtFechaSlnRpta, \
      dtFechaSolucionReal.value dtFechaSolucionReal, \
      dtFechaTransaccion.value dtFechaTransaccion, \
      if (numAbonoDefinitivo.value is null, '', numAbonoDefinitivo.value) AbonoDefinitivo, \
      if (numAbonoTemporal.value is null, '', numAbonoTemporal.value ) AbonoTemporal, \
      numTiempoSlnRpta.value numTiempoSlnRpta, \
      numTiempoSolucion.value numTiempoSolucion, \
      numTiempoSolucionReal.value numTiempoSolucionReal, \
      numTotContabiliza.value numTotContabiliza, \
      numTotDias.value numTotDias, \
      numValorDolares.value numValorDolares, \
      numValorDolaresC.value numValorDolaresC, \
      numValorPesos.value numValorPesos, \
      numValorPesosC.value numValorPesosC, \
      rdAutorizaEmail.value rdAutorizaEmail, \
      if (rdClienteRazon.value is null, '', rdClienteRazon.value) ClientetieneRazon, \
      rdTodosSoportes.value rdTodosSoportes, \
      rtPantallasReclamo.value rtPantallasReclamo, \
      rtPantallasSolucion.value rtPantallasSolucion, \
      txtAutor.value txtAutor, \
      txtCelular.value txtCelular, \
      txtClasificacion.value txtClasificacion, \
      txtCodCanal.value txtCodCanal, \
      txtCodCiudadCliente.value txtCodCiudadCliente, \
      txtCodConvenio.value txtCodConvenio, \
      txtCodigoCompra.value txtCodigoCompra, \
      txtCodigoEstablecimiento.value txtCodigoEstablecimiento, \
      txtCodigoProducto.value txtCodigoProducto, \
      txtDeptoCliente.value txtDeptoCliente, \
      txtDireccionCliente.value txtDireccionCliente, \
      txtEmailCliente.value txtEmailCliente, \
      txtFax.value txtFax, \
      txtFuncionarioEmpresa.value txtFuncionarioEmpresa, \
      txtGerenteCuenta.value txtGerenteCuenta, \
      txtNitEmpresa.value txtNitEmpresa, \
      txtNomCanal.value txtNomCanal, \
      txtNomCliente.value txtNomCliente, \
      txtNomEmpresa.value txtNomEmpresa, \
      txtNomRespError.value txtNomRespError, \
      txtNumEncargoFiducia.value txtNumEncargoFiducia, \
      if (txtNumIdCliente.value is null, '', txtNumIdCliente.value) Cedula, \
      txtNumPoliza.value txtNumPoliza, \
      txtNumProducto.value txtNumProducto, \
      txtNumTarjetaCtaDebita.value txtNumTarjetaCtaDebita, \
      txtNumTarjetaDebito.value txtNumTarjetaDebito, \
      if (txtRadicado.value is null, '',txtRadicado.value) Radicado, \
      txtResponsable.value txtResponsable, \
      txtSegmento.value txtSegmento, \
      txtSubsegmento.value txtSubsegmento, \
      txtSoportesNoRecibidos.value txtSoportesNoRecibidos, \
      txtSoportesRecibidos.value txtSoportesRecibidos, \
      txtTelefonosCliente.value txtTelefonosCliente, \
      if (txtTipoProducto.value is null, '', txtTipoProducto.value) Producto, \
      if (txtTipoReclamo.value is null, '', txtTipoReclamo.value) TipoReclamo, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, \
      cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes \
  FROM \
      Zdc_Lotus_Reclamos_TablaIntermedia_Aux")

sqlContext.registerDataFrameAsTable(reclamos_df, "Zdc_Lotus_Reclamos_TablaIntermedia")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como String
"""txtComentario como String"""
reclamos_txtComentario_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtComentario':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtComentario_string_rdd = sc.parallelize(reclamos_txtComentario_string_payload)
reclamos_txtComentario_string_json = spark.read.json(reclamos_txtComentario_string_rdd)

reclamos_txtComentario_string_df = spark.read.schema(reclamos_txtComentario_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtComentario_string_df = reclamos_txtComentario_string_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
reclamos_txtComentario_string_df = reclamos_txtComentario_string_df.filter(reclamos_txtComentario_string_df.txtComentario.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtComentario_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtComentario como Array
reclamos_txtComentario_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtComentario':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}}"]

sc = spark.sparkContext
reclamos_txtComentario_array_rdd = sc.parallelize(reclamos_txtComentario_array_payload)
reclamos_txtComentario_array_json = spark.read.json(reclamos_txtComentario_array_rdd)

reclamos_txtComentario_array_df = spark.read.schema(reclamos_txtComentario_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtComentario.value as txtComentario")
reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtComentario_array_df = reclamos_txtComentario_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtComentario", '\r\n ').alias("txtComentario"))
sqlContext.registerDataFrameAsTable(reclamos_txtComentario_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_array")

# COMMAND ----------

reclamos_txtComentario_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                            if (txtComentario is null, '', txtComentario) txtComentario \
                                            FROM \
                                               Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_string \
                                            UNION \
                                            SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                            if (txtComentario is null, '', txtComentario) txtComentario \
                                            FROM \
                                               Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtComentario_array")

sqlContext.registerDataFrameAsTable(reclamos_txtComentario_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtComentario")

# COMMAND ----------

# DBTITLE 1,Esquema con txtMensajeAyuda como String
"""txtMensajeAyuda como String"""
reclamos_txtMensajeAyuda_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtMensajeAyuda':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtMensajeAyuda_string_rdd = sc.parallelize(reclamos_txtMensajeAyuda_string_payload)
reclamos_txtMensajeAyuda_string_json = spark.read.json(reclamos_txtMensajeAyuda_string_rdd)

reclamos_txtMensajeAyuda_string_df = spark.read.schema(reclamos_txtMensajeAyuda_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtMensajeAyuda_string_df = reclamos_txtMensajeAyuda_string_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtMensajeAyuda.value as txtMensajeAyuda")
reclamos_txtMensajeAyuda_string_df = reclamos_txtMensajeAyuda_string_df.filter(reclamos_txtMensajeAyuda_string_df.txtMensajeAyuda.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtMensajeAyuda como Array
reclamos_txtMensajeAyuda_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtMensajeAyuda':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtMensajeAyuda_array_rdd = sc.parallelize(reclamos_txtMensajeAyuda_array_payload)
reclamos_txtMensajeAyuda_array_json = spark.read.json(reclamos_txtMensajeAyuda_array_rdd)

reclamos_txtMensajeAyuda_array_df = spark.read.schema(reclamos_txtMensajeAyuda_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtMensajeAyuda.value as txtMensajeAyuda")
reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtMensajeAyuda_array_df = reclamos_txtMensajeAyuda_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtMensajeAyuda", '\r\n ').alias("txtMensajeAyuda"))
sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_array")

# COMMAND ----------

reclamos_txtMensajeAyuda_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                              if (txtMensajeAyuda is null, '', txtMensajeAyuda) txtMensajeAyuda \
                                              FROM \
                                                 Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_string \
                                              UNION \
                                              SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID,dtFechaCreacion, \
                                              if( txtMensajeAyuda is null, '', txtMensajeAyuda) txtMensajeAyuda \
                                              FROM \
                                                 Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtMensajeAyuda_array")
sqlContext.registerDataFrameAsTable(reclamos_txtMensajeAyuda_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtMensajeAyuda")

# COMMAND ----------

# DBTITLE 1,Esquema con txtObservaciones como String
"""txtObservaciones como String"""
reclamos_txtObservaciones_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtObservaciones':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtObservaciones_string_rdd = sc.parallelize(reclamos_txtObservaciones_string_payload)
reclamos_txtObservaciones_string_json = spark.read.json(reclamos_txtObservaciones_string_rdd)

reclamos_txtObservaciones_string_df = spark.read.schema(reclamos_txtObservaciones_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")
reclamos_txtObservaciones_string_df = reclamos_txtObservaciones_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtObservaciones.value as txtObservaciones")
reclamos_txtObservaciones_string_df = reclamos_txtObservaciones_string_df.filter(reclamos_txtObservaciones_string_df.txtObservaciones.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtObservaciones como Array
reclamos_txtObservaciones_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtObservaciones':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtObservaciones_array_rdd = sc.parallelize(reclamos_txtObservaciones_array_payload)
reclamos_txtObservaciones_array_json = spark.read.json(reclamos_txtObservaciones_array_rdd)

reclamos_txtObservaciones_array_df = spark.read.schema(reclamos_txtObservaciones_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtObservaciones.value as txtObservaciones")
reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtObservaciones_array_df = reclamos_txtObservaciones_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion",array_join("txtObservaciones", '\r\n ').alias("txtObservaciones"))
sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_array")

# COMMAND ----------

reclamos_txtObservaciones_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                               if (txtObservaciones is null, '', txtObservaciones) txtObservaciones\
                                               FROM \
                                                  Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_string \
                                               UNION \
                                               SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                               if (txtObservaciones is null, '', txtObservaciones) txtObservaciones \
                                               FROM  \
                                                  Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtObservaciones_array")
sqlContext.registerDataFrameAsTable(reclamos_txtObservaciones_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtObservaciones")

# COMMAND ----------

# DBTITLE 1,Esquema con txtSoportesRequeridos como String
"""txtSoportesRequeridos como String"""
reclamos_txtSoportesRequeridos_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtSoportesRequeridos':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtSoportesRequeridos_string_rdd = sc.parallelize(reclamos_txtSoportesRequeridos_string_payload)
reclamos_txtSoportesRequeridos_string_json = spark.read.json(reclamos_txtSoportesRequeridos_string_rdd)

reclamos_txtSoportesRequeridos_string_df = spark.read.schema(reclamos_txtSoportesRequeridos_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtSoportesRequeridos_string_df = reclamos_txtSoportesRequeridos_string_df.selectExpr("UniversalID", "UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtSoportesRequeridos.value as txtSoportesRequeridos")
reclamos_txtSoportesRequeridos_string_df = reclamos_txtSoportesRequeridos_string_df.filter(reclamos_txtSoportesRequeridos_string_df.txtSoportesRequeridos.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtSoportesRequeridos como Array
reclamos_txtSoportesRequeridos_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtSoportesRequeridos':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtSoportesRequeridos_array_rdd = sc.parallelize(reclamos_txtSoportesRequeridos_array_payload)
reclamos_txtSoportesRequeridos_array_json = spark.read.json(reclamos_txtSoportesRequeridos_array_rdd)

reclamos_txtSoportesRequeridos_array_df = spark.read.schema(reclamos_txtSoportesRequeridos_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.selectExpr("UniversalID","UniversalIDCompuesto","dtFechaCreacion.value as dtFechaCreacion","txtSoportesRequeridos.value as txtSoportesRequeridos")
reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtSoportesRequeridos_array_df = reclamos_txtSoportesRequeridos_array_df.select("UniversalID","UniversalIDCompuesto","dtFechaCreacion",array_join("txtSoportesRequeridos", '\r\n ').alias("txtSoportesRequeridos"))
sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_array")

# COMMAND ----------

reclamos_txtSoportesRequeridos_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                    if (txtSoportesRequeridos is null, '', txtSoportesRequeridos) txtSoportesRequeridos \
                                                    FROM \
                                                       Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_string \
                                                    UNION \
                                                    SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                    if (txtSoportesRequeridos is null, '', txtSoportesRequeridos) txtSoportesRequeridos \
                                                    FROM \
                                                       Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtSoportesRequeridos_array")
sqlContext.registerDataFrameAsTable(reclamos_txtSoportesRequeridos_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtSoportesRequeridos")

# COMMAND ----------

# DBTITLE 1,Esquema con txtPantallasReclamo como String
"""txtPantallasReclamo como String"""
reclamos_txtPantallasReclamo_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasReclamo':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtPantallasReclamo_string_rdd = sc.parallelize(reclamos_txtPantallasReclamo_string_payload)
reclamos_txtPantallasReclamo_string_json = spark.read.json(reclamos_txtPantallasReclamo_string_rdd)

reclamos_txtPantallasReclamo_string_df = spark.read.schema(reclamos_txtPantallasReclamo_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtPantallasReclamo_string_df = reclamos_txtPantallasReclamo_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasReclamo.value as txtPantallasReclamo")
reclamos_txtPantallasReclamo_string_df = reclamos_txtPantallasReclamo_string_df.filter(reclamos_txtPantallasReclamo_string_df.txtPantallasReclamo.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtPantallasReclamo como Array
reclamos_txtPantallasReclamo_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasReclamo':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtPantallasReclamo_array_rdd = sc.parallelize(reclamos_txtPantallasReclamo_array_payload)
reclamos_txtPantallasReclamo_array_json = spark.read.json(reclamos_txtPantallasReclamo_array_rdd)

reclamos_txtPantallasReclamo_array_df = spark.read.schema(reclamos_txtPantallasReclamo_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasReclamo.value as txtPantallasReclamo")
reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtPantallasReclamo_array_df = reclamos_txtPantallasReclamo_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtPantallasReclamo", '\r\n ').alias("txtPantallasReclamo"))
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_array")

# COMMAND ----------

reclamos_txtPantallasReclamo_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                  if (txtPantallasReclamo is null, '', txtPantallasReclamo) txtPantallasReclamo \
                                                  FROM \
                                                     Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_string \
                                                  UNION \
                                                  SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                  if (txtPantallasReclamo is null, '', txtPantallasReclamo) txtPantallasReclamo \
                                                  FROM \
                                                     Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasReclamo_array")
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasReclamo_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasReclamo")

# COMMAND ----------

# DBTITLE 1,Esquema con txtPantallasSolucion como String
"""txtPantallasSolucion como String"""
reclamos_txtPantallasSolucion_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasSolucion':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtPantallasSolucion_string_rdd = sc.parallelize(reclamos_txtPantallasSolucion_string_payload)
reclamos_txtPantallasSolucion_string_json = spark.read.json(reclamos_txtPantallasSolucion_string_rdd)

reclamos_txtPantallasSolucion_string_df = spark.read.schema(reclamos_txtPantallasSolucion_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")
reclamos_txtPantallasSolucion_string_df = reclamos_txtPantallasSolucion_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasSolucion.value as txtPantallasSolucion")
reclamos_txtPantallasSolucion_string_df = reclamos_txtPantallasSolucion_string_df.filter(reclamos_txtPantallasSolucion_string_df.txtPantallasSolucion.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtPantallasSolucion como Array
reclamos_txtPantallasSolucion_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtPantallasSolucion':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtPantallasSolucion_array_rdd = sc.parallelize(reclamos_txtPantallasSolucion_array_payload)
reclamos_txtPantallasSolucion_array_json = spark.read.json(reclamos_txtPantallasSolucion_array_rdd)

reclamos_txtPantallasSolucion_array_df = spark.read.schema(reclamos_txtPantallasSolucion_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtPantallasSolucion.value as txtPantallasSolucion")
reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtPantallasSolucion_array_df = reclamos_txtPantallasSolucion_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtPantallasSolucion", '\r\n ').alias("txtPantallasSolucion"))
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_array")

# COMMAND ----------

reclamos_txtPantallasSolucion_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                   if (txtPantallasSolucion is null, '', txtPantallasSolucion) txtPantallasSolucion \
                                                   FROM \
                                                      Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_string \
                                                   UNION \
                                                   SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                                   if (txtPantallasSolucion is null, '', txtPantallasSolucion) txtPantallasSolucion \
                                                   FROM \
                                                      Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtPantallasSolucion_array")
sqlContext.registerDataFrameAsTable(reclamos_txtPantallasSolucion_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasSolucion")

# COMMAND ----------

# DBTITLE 1,Esquema con txtRespuesta como String
"""txtRespuesta como String"""
reclamos_txtRespuesta_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtRespuesta':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtRespuesta_string_rdd = sc.parallelize(reclamos_txtRespuesta_string_payload)
reclamos_txtRespuesta_string_json = spark.read.json(reclamos_txtRespuesta_string_rdd)

reclamos_txtRespuesta_string_df = spark.read.schema(reclamos_txtRespuesta_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtRespuesta_string_df = reclamos_txtRespuesta_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtRespuesta.value as txtRespuesta")
reclamos_txtRespuesta_string_df = reclamos_txtRespuesta_string_df.filter(reclamos_txtRespuesta_string_df.txtRespuesta.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtRespuesta como Array
reclamos_txtRespuesta_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtRespuesta':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtRespuesta_array_rdd = sc.parallelize(reclamos_txtRespuesta_array_payload)
reclamos_txtRespuesta_array_json = spark.read.json(reclamos_txtRespuesta_array_rdd)

reclamos_txtRespuesta_array_df = spark.read.schema(reclamos_txtRespuesta_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtRespuesta.value as txtRespuesta")
reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtRespuesta_array_df = reclamos_txtRespuesta_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtRespuesta", '\r\n ').alias("txtRespuesta"))
sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_array")

# COMMAND ----------

reclamos_txtRespuesta_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                           if (txtRespuesta is null, '', txtRespuesta) txtRespuesta\
                                           FROM \
                                              Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_string \
                                           UNION \
                                           SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion, \
                                           if (txtRespuesta is null, '', txtRespuesta) txtRespuesta\
                                           FROM \
                                              Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtRespuesta_array")
sqlContext.registerDataFrameAsTable(reclamos_txtRespuesta_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtRespuesta")

# COMMAND ----------

# DBTITLE 1,Esquema con txtHistoria como String
"""txtHistoria como String"""
reclamos_txtHistoria_string_payload =  ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtHistoria':{'label':'string','value':'string'},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtHistoria_string_rdd = sc.parallelize(reclamos_txtHistoria_string_payload)
reclamos_txtHistoria_string_json = spark.read.json(reclamos_txtHistoria_string_rdd)

reclamos_txtHistoria_string_df = spark.read.schema(reclamos_txtHistoria_string_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

reclamos_txtHistoria_string_df = reclamos_txtHistoria_string_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
reclamos_txtHistoria_string_df = reclamos_txtHistoria_string_df.filter(reclamos_txtHistoria_string_df.txtHistoria.substr(0,1) != '[')
sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_string_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_string")

# COMMAND ----------

# DBTITLE 1,Esquema con txtHistoria como Array
reclamos_txtHistoria_array_payload = ["{'UniversalID':'string','UniversalIDCompuesto':'string','txtHistoria':{'label':'string', 'value':['array']},'dtFechaCreacion':{'label':'string','value':'string'}}"]

sc = spark.sparkContext
reclamos_txtHistoria_array_rdd = sc.parallelize(reclamos_txtHistoria_array_payload)
reclamos_txtHistoria_array_json = spark.read.json(reclamos_txtHistoria_array_rdd)

reclamos_txtHistoria_array_df = spark.read.schema(reclamos_txtHistoria_array_json.schema).json("/mnt/lotus-reclamos/reclamos/raw/")

from pyspark.sql.functions import col,array_join
reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.selectExpr("UniversalID","UniversalIDCompuesto", "dtFechaCreacion.value as dtFechaCreacion","txtHistoria.value as txtHistoria")
reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.filter(col("UniversalID").isNotNull())
reclamos_txtHistoria_array_df = reclamos_txtHistoria_array_df.select("UniversalID","UniversalIDCompuesto", "dtFechaCreacion", array_join("txtHistoria", '\r\n ').alias("txtHistoria"))
sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_array_df, "Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_array")

# COMMAND ----------

reclamos_txtHistoria_df = sqlContext.sql("SELECT if(UniversalIDCompuesto is Null, UniversalID, UniversalIDCompuesto) As UniversalID, dtFechaCreacion , \
                                          if (txtHistoria is null, '', txtHistoria) txtHistoria \
                                          FROM \
                                             Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_string \
                                          UNION \
                                          SELECT if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID, dtFechaCreacion , \
                                           if (txtHistoria is null, '', txtHistoria) txtHistoria \
                                          FROM \
                                             Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_txtHistoria_array")
sqlContext.registerDataFrameAsTable(reclamos_txtHistoria_df, "Zdc_Lotus_Reclamos_Tablaintermedia_txtHistoria")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Lotus_ProcessOptimizado;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux_1;
# MAGIC CREATE EXTERNAL TABLE Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux_1
# MAGIC (
# MAGIC   UniversalID string,
# MAGIC   UniversalIDCompuesto string,
# MAGIC   TituloDB string,
# MAGIC   numValoresTransac map<string,array<string>>,
# MAGIC   dtFechasTransac map<string,array<string>>,
# MAGIC   txtOficinasTransac map<string,array<string>>,
# MAGIC   txtReferenciasTransac map<string,array<string>>,
# MAGIC   txtMonedasTransac map<string,array<string>>,
# MAGIC   txtCiudadesTransac map<string,array<string>>,
# MAGIC   txtCuentasTransac map<string,array<string>>,
# MAGIC   txtChequesTransac map<string,array<string>>,
# MAGIC   dtConsignacionesTransac map<string,array<string>>,
# MAGIC   dtFechasTrans map<string,array<string>>,
# MAGIC   txtEstadoTrans map<string,array<string>>,
# MAGIC   numDiasTrans map<string,array<string>>,
# MAGIC   numVencidosTrans map<string,array<string>>,
# MAGIC   dtFechaCreacion map<string,string>,
# MAGIC   numContabilizaTrans map<string,array<string>>,
# MAGIC   numTotDias map<string,string>,
# MAGIC   umTotVencidos map<string,string>,
# MAGIC   numTotVencidos map<string,string>,
# MAGIC   numTotContabiliza map<string,string>
# MAGIC )
# MAGIC ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# MAGIC LOCATION '/mnt/lotus-reclamos/reclamos/raw/';

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripcion: La siguiente sentencia descrimina los campos "Key Duplicate", se debe ejecutar antes consultar una tabla la cual define su metadata mediante la libreira serde de Hive y evitar que se genere una excepcion de mal              formato de row JSON. Esta sentencia no elimina registro de la tabla, solomente omite la columna duplicada.
# MAGIC Responsable: Juan David Escobar Escobar 
# MAGIC Fecha: 03/03/2020
# MAGIC */
# MAGIC 
# MAGIC alter table Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux_1 set serdeproperties ("ignore.malformed.json" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux;
# MAGIC CREATE TABLE Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux
# MAGIC SELECT 
# MAGIC   if(UniversalIDCompuesto is Null,UniversalID,UniversalIDCompuesto) As UniversalID,
# MAGIC   UniversalID as UniversalIDPadre,
# MAGIC   TituloDB,
# MAGIC   numValoresTransac, 
# MAGIC   dtFechasTransac,
# MAGIC   txtOficinasTransac,
# MAGIC   txtReferenciasTransac,
# MAGIC   txtMonedasTransac,
# MAGIC   txtCiudadesTransac,
# MAGIC   txtCuentasTransac,
# MAGIC   txtChequesTransac,
# MAGIC   dtConsignacionesTransac,
# MAGIC   dtFechasTrans,
# MAGIC   txtEstadoTrans,
# MAGIC   numDiasTrans,
# MAGIC   numVencidosTrans,
# MAGIC   dtFechaCreacion,
# MAGIC   numContabilizaTrans,
# MAGIC   numTotDias,
# MAGIC   umTotVencidos,
# MAGIC   numTotVencidos,
# MAGIC   numTotContabiliza
# MAGIC FROM 
# MAGIC   Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux_1

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/reclamos/4_SaveSchema

# COMMAND ----------

# MAGIC %md
# MAGIC # ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %run /lotus/lotus-reclamos/reclamos/5_LoadSchema $from_create_schema = True
