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
panel_principal_primercontacto = sqlContext.sql("\
SELECT \
  UniversalID, \
  dtFechaCreacion.value FechaSolucionado, \
  substring_index(dtFechaCreacion.value, ' ', 1) fecha_solicitudes, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  txtNumIdCliente.value AS NumeroIdentificacion, \
  txtRadicado.value Radicado, \
  cbResponsable.value Responsable, \
  txtEstado.value Estado, \
  txtTipoProducto.value TipoProducto, \
  txtTipoReclamo.value TipoReclamo, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/primercontacto/', UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value), ' - ', \
  upper(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value)), ' - ',  \
  lower(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
FROM \
  Zdc_Lotus_PrimerContacto_TablaIntermedia \
")
                                                
sqlContext.registerDataFrameAsTable(panel_principal_primercontacto,"Zdp_Lotus_PrimerContacto_PanelPrincipal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_primercontacto = sqlContext.sql("\
SELECT UniversalId, 'Datos Generales' as vista, 1 as posicion, 'Autor' AS key, txtAutor.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 2 as posicion, 'Fecha Solucionado Primer Contacto' AS key, dtFechaCreacion.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 3 as posicion, 'Sucursal Captura' AS key, TXTSUCCAPTURA.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 4 as posicion, 'Tipo Producto' AS key, txtTipoProducto.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 5 as posicion, 'Tipo Reclamo' AS key, txtTipoReclamo.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 6 as posicion, 'Numero Identificacion' AS key, txtNumIdCliente.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 7 as posicion, 'Responsable' AS key, cbResponsable.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 8 as posicion, 'Codigo Causalidad' AS key, txtCodCausalidad.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 9 as posicion, 'Radicado' AS key, txtRadicado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 10 as posicion, 'Estado' AS key, txtEstado.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' as vista, 11 as posicion, 'Código Sucursal Captura' AS key, cbSucCaptura.value AS value, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Historia' as vista, 12 as posicion, 'Historia' AS key, txthistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_primercontacto_Tablaintermedia_txthistoria \
UNION ALL \
SELECT UniversalId, 'Comentario' as vista, 13 as posicion, 'Comentarios' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_primercontacto_Tablaintermedia_txtComentario \
")

sqlContext.registerDataFrameAsTable(panel_detalle_primercontacto,"zdp_lotus_PrimerContacto_Panel_Detalle")

# COMMAND ----------

# DBTITLE 1,Panel Masivos
 masivos_primercontacto_df = sqlContext.sql("\
SELECT \
  ZDC.UniversalID, \
  ZDC.dtFechaCreacion.value FechaSolucionado, \
  substring_index(ZDC.dtFechaCreacion.value, ' ', 1) fecha_solicitudes, \
  cast(substr(substring_index(ZDC.dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(ZDC.dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  ZDC.txtNumIdCliente.value AS NumeroIdentificacion, \
  ZDC.txtRadicado.value Radicado, \
  ZDC.cbResponsable.value Responsable, \
  ZDC.txtEstado.value Estado, \
  ZDC.txtTipoProducto.value TipoProducto, \
  ZDC.txtTipoReclamo.value TipoReclamo, \
  ZDC.txtAutor.value Autor, \
  ZDC.dtFechaCreacion.value FechaSolucionadoPrimerContacto, \
  ZDC.TXTSUCCAPTURA.value SucursalCaptura, \
  ZDC.txtCodCausalidad.value CodigoCausalidad, \
  ZDC.cbSucCaptura.value CodigoSucursalCaptura, \
  HIS.txthistoria Historia, \
  COM.txtComentario Comentarios \
  FROM \
  Zdc_Lotus_PrimerContacto_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_primercontacto_Tablaintermedia_txthistoria HIS ON HIS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_primercontacto_Tablaintermedia_txtComentario COM ON COM.UniversalID = ZDC.UniversalID")