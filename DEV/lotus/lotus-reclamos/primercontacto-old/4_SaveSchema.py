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
sql_string = "SELECT \
  UniversalID, \
  txt_CedNit.value CedNit, \
  substring_index(dat_FchCreac.value, ' ', 1) fecha_solicitudes, \
  cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
  key_Estado.value Estado, \
  nom_Responsable.value Responsable, \
  key_Producto.value Producto, \
  key_TipoReclamo.value TipoReclamo, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/primercontacto-old/', UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value), ' - ', \
  upper(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value)), ' - ', \
  lower(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') Busqueda \
FROM \
  Zdc_Lotus_PrimerContacto_Old_TablaIntermedia"
panel_principal_primercontacto_old = sqlContext.sql(sql_string)
sqlContext.registerDataFrameAsTable(panel_principal_primercontacto_old,"Zdp_Lotus_PrimerContacto_Old_PanelPrincipal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_primercontacto_old = sqlContext.sql("\
SELECT UniversalId, '' AS vista, 1 as posicion, 'Estado' AS key, key_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 2 as posicion, 'Cédula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 3 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 4 as posicion, 'Tipo de Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 5 as posicion, 'Responsable' AS key, nom_Responsable.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 6 as posicion, 'Código de Causalidad' AS key, txt_CodCausalidad.value  AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Comentarios' AS vista, 7 as posicion, ' ' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Comentarios \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 8 as posicion, ' ' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Historia \
")

sqlContext.registerDataFrameAsTable(panel_detalle_primercontacto_old,"zdp_lotus_PrimerContacto_Old_Detalle")

# COMMAND ----------

masivos_primercontacto_old_df = sqlContext.sql("\
SELECT \
  ZDC.UniversalID, \
  ZDC.txt_CedNit.value CedNit, \
  substring_index(ZDC.dat_FchCreac.value, ' ', 1) fecha_solicitudes, \
  cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
  ZDC.key_Estado.value Estado, \
  ZDC.nom_Responsable.value Responsable, \
  ZDC.key_Producto.value Producto, \
  ZDC.key_TipoReclamo.value TipoReclamo, \
  ZDC.txt_CedNit.value CedulaNit, \
  ZDC.key_TipoReclamo.value TipodeReclamo, \
  ZDC.txt_CodCausalidad.value CodCausalidad, \
  COM.txt_Comentarios Comentarios, \
  HIS.txt_Historia Historia \
  FROM \
  Zdc_Lotus_PrimerContacto_Old_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Comentarios COM ON COM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_PrimerContacto_Old_Tablaintermedia_txt_Historia HIS ON HIS.UniversalID = ZDC.UniversalID")