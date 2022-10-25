# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se requiere efectuar refresco de la tabla cada vez que se insertan o actualizan datos antes de ser consultada.
# MAGIC Fecha: 25/02/2020
# MAGIC Responsable: Juan David Escobar
# MAGIC */
# MAGIC 
# MAGIC REFRESH TABLE Default.parametros;

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Panel Principal
panel_principal_subreclamos = sqlContext.sql("\
SELECT \
  UniversalID, \
  txtIdDocPadre.value UniversalID_Sub, \
  txtRadicado.value Subreclamo, \
  dtFechaCreacion.value FechaCreacion, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  txtAutor.value Autor, \
  txtTipoProducto.value Producto, \
  txtTipoReclamo.value TipoReclamo, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/subreclamos/', UniversalID,'.zip'" + "), '') link \
FROM \
  Zdc_Lotus_SubReclamos_TablaIntermedia")
                                                 
sqlContext.registerDataFrameAsTable(panel_principal_subreclamos,"Zdp_Lotus_SubReclamos_PanelPrincipal")

# COMMAND ----------

# DBTITLE 1,Panel Historia Aux
panel_historia_subreclamos_Aux = sqlContext.sql("\
SELECT \
  UniversalID, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  posicion, \
  FechaEntrada, \
  Estado, \
  TotalDias, \
  DiasVencidos, \
  DiasContabilizados \
FROM \
  Lotus_ProcessOptimizado.Zdc_Lotus_SubReclamos_TablaIntermedia_Aux \
  LATERAL VIEW posexplode(dtFechasTrans.value) FechaEntrada AS posicion, FechaEntrada \
  LATERAL VIEW posexplode(txtEstadoTrans.value) Estado AS posicion1, Estado \
  LATERAL VIEW posexplode(numDiasTrans.value) TotalDias AS posicion2, TotalDias \
  LATERAL VIEW posexplode(numVencidosTrans.value) DiasVencidos AS posicion3, DiasVencidos \
  LATERAL VIEW posexplode(numContabilizaTrans.value) DiasContabilizados AS posicion4, DiasContabilizados \
WHERE \
  posicion = posicion1 \
  AND posicion = posicion2 \
  AND posicion = posicion3 \
  AND posicion = posicion4")
                                                     
sqlContext.registerDataFrameAsTable(panel_historia_subreclamos_Aux,"Zdp_Lotus_SubReclamos_Catalogo_Historia_Aux")

# COMMAND ----------

# DBTITLE 1,Panel Historia
panel_historia_subreclamos = sqlContext.sql("\
SELECT \
  UniversalID, \
  FechaEntrada, \
  anio, \
  mes, \
  Estado, \
  TotalDias, \
  DiasVencidos, \
  DiasContabilizados \
FROM \
  Zdp_Lotus_SubReclamos_Catalogo_Historia_Aux \
UNION ALL \
SELECT \
   UniversalID, \
  '' FechaEntrada, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  'Totales' Estado, \
  numTotDias.value TotalDias, \
  numTotVencidos.value DiasVencidos, \
  numTotContabiliza.value DiasContabilizados \
FROM \
  Lotus_ProcessOptimizado.Zdc_Lotus_SubReclamos_TablaIntermedia_Aux")

sqlContext.registerDataFrameAsTable(panel_historia_subreclamos,"Zdp_Lotus_SubReclamos_Catalogo_Historia")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_subreclamos = sqlContext.sql("\
SELECT UniversalID, 'Datos Generales' AS vista, 1 as posicion, 'Autor' AS key, txtAutor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 2 as posicion, 'FechaCreacion' AS key, dtFechaCreacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 3 as posicion, 'Responsable' AS key, cbResponsable.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 4 as posicion, 'Estado' AS key, txtEstado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 5 as posicion, 'Subreclamo' AS key, txtRadicado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 6 as posicion, 'NumeroIdentificacion' AS key, txtNumIdCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 7 as posicion, 'Nombre' AS key, txtNomCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 8 as posicion, 'Producto' AS key, txtTipoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 9 as posicion, 'TipoReclamo' AS key, txtTipoReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 10 as posicion, 'NumeroProducto' AS key, concat(txtPrefijo.value,'-',txtNumProducto.value) AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 11 as posicion, 'TipoCuenta' AS key, rdTipoCuenta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 12 as posicion, 'NumeroCuenta' AS key, txtNumCuenta.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 13 as posicion, 'ValorTotalPesos' AS key, numValorPesos.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 14 as posicion, 'ValorGMF' AS key, numValorGMF.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 15 as posicion, 'ValorComisiones' AS key, numValorComisiones.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 16 as posicion, 'ValorReversion' AS key, numValorReversion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 17 as posicion, 'ClienteTieneRazon' AS key, rdClienteRazon.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 18 as posicion, 'CampoDiligenciar' AS key, cbCampoDiligencia.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 19 as posicion, 'CargaCliente' AS key, rdCargaCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 20 as posicion, 'SucursalSobrante' AS key, cbSucursalSobrante.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 21 as posicion, 'FechaSobrante' AS key, dtFechaSobrante.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 22 as posicion, 'PyGSucursalError' AS key, cbSucursalError.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 23 as posicion, 'PyGAreaAdministrativa' AS key, txtAreaAdministrativa.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 24 as posicion, 'CuentaTercero' AS key, txtCuentaTercero.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 25 as posicion, 'TipoCuentaTercero' AS key, rdTipoCuentaTercero.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Subreclamos' AS vista, 26 as posicion, 'PyGTarjetas' AS key, cbTarjetas AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_cbTarjetas \
UNION ALL \
SELECT UniversalID, 'Fecha de Solucion' AS vista, 27 as posicion, 'TiempoRealSolucion' AS key, numTiempoSolucionReal.value  AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Fecha de Solucion' AS vista, 28 as posicion, 'FechaRealSolucion' AS key, dtFechaSolucionReal.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Comentario' AS vista, 29 as posicion, 'Comentario' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txtComentario \
UNION ALL \
SELECT UniversalID, 'Historia' AS vista, 30 as posicion, 'Historia' AS key, txtHistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txtHistoria")

sqlContext.registerDataFrameAsTable(panel_detalle_subreclamos,"zdp_lotus_Reclamos_Detalle")

# COMMAND ----------

masivos_subreclamos_df = sqlContext.sql("\
SELECT \
  ZDC.UniversalID, \
  cast(substr(substring_index(ZDC.dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(ZDC.dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  ZDC.txtIdDocPadre.value UniversalID_Sub, \
  ZDC.txtRadicado.value Subreclamo, \
  ZDC.dtFechaCreacion.value FechaCreacion, \
  ZDC.txtAutor.value Autor, \
  ZDC.txtTipoProducto.value Producto, \
  ZDC.txtTipoReclamo.value TipoReclamo, \
  ZDC.cbResponsable.value Responsable, \
  ZDC.txtEstado.value Estado, \
  ZDC.txtNumIdCliente.value NumeroIdentificacion, \
  ZDC.txtNomCliente.value Nombre, \
  concat(ZDC.txtPrefijo.value,'-',ZDC.txtNumProducto.value) NumeroProducto, \
  ZDC.rdTipoCuenta.value TipoCuenta, \
  ZDC.txtNumCuenta.value NumeroCuenta, \
  ZDC.numValorPesos.value ValorTotalPesos, \
  ZDC.numValorGMF.value ValorGMF, \
  ZDC.numValorComisiones.value ValorComisiones, \
  ZDC.numValorReversion.value ValorReversion, \
  ZDC.rdClienteRazon.value ClienteTieneRazon, \
  ZDC.cbCampoDiligencia.value CampoDiligenciar, \
  ZDC.rdCargaCliente.value CargaCliente, \
  ZDC.cbSucursalSobrante.value SucursalSobrante, \
  ZDC.dtFechaSobrante.value FechaSobrante, \
  ZDC.cbSucursalError.value PyGSucursalError, \
  ZDC.txtAreaAdministrativa.value PyGAreaAdministrativa, \
  ZDC.txtCuentaTercero.value CuentaTercero, \
  ZDC.rdTipoCuentaTercero.value TipoCuentaTercero, \
  TAR.cbTarjetas PyGTarjetas, \
  ZDC.numTiempoSolucionReal.value TiempoRealSolucion, \
  ZDC.dtFechaSolucionReal.value FechaRealSolucion, \
  COM.txtComentario Comentario, \
  HIS.txtHistoria Historia, \
  CAT.FechaEntrada, \
  CAT.Estado Estado_cat, \
  CAT.TotalDias, \
  CAT.DiasVencidos, \
  CAT.DiasContabilizados \
FROM \
  Zdc_Lotus_SubReclamos_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_cbTarjetas TAR ON TAR.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_txtComentario COM ON COM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_txtHistoria HIS ON HIS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdp_Lotus_SubReclamos_Catalogo_Historia CAT ON CAT.UniversalID = ZDC.UniversalID")