# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se requiere efectuar refresco de la tabla cada vez que se insertan o actualizan datos antes de ser consultada.
# MAGIC Fecha: 25/02/2020
# MAGIC Responsable: Juan David Escobar
# MAGIC */
# MAGIC 
# MAGIC refresh table default.parametros;

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_SOL_CLI_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Panel Principal
panel_principal_solicitudes = sqlContext.sql ("\
SELECT \
  UniversalID, \
  key_NomPedido, \
  txt_Autor Autor, \
  dat_FchCreac, \
  txt_AsuntoPed, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
                          if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                          if(txt_AsuntoPed is null, '', txt_AsuntoPed)), ' - ',  \
  upper(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
               if(key_NomPedido is null, '',key_NomPedido), ' - ', \
               if(txt_AsuntoPed is null, '', txt_AsuntoPed))), ' - ', \
  lower(concat(if(txt_Autor is null, '',txt_Autor), ' - ', \
               if(key_NomPedido is null, '',key_NomPedido), ' - ', \
               if(txt_AsuntoPed is null, '', txt_AsuntoPed)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
  anio, \
  mes \
FROM \
  Zdc_Lotus_Solicitudes_TablaIntermedia")

sqlContext.registerDataFrameAsTable(panel_principal_solicitudes,"Zdp_Lotus_Solicitudes_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_solicitudes = sqlContext.sql("\
SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Fecha Creación' AS key, dat_FchCreac AS value, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 3 as posicion, 'Nombres' AS key, txt_NomCompEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 4 as posicion, 'Dependencia' AS key, txt_DepEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 5 as posicion, 'Región' AS key, txt_RgnEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 6 as posicion, 'Ubicación' AS key, txt_UbicEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitantes' AS vista, 7 as posicion, 'teléfono' AS key, txt_ExtEmp_A, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'datos Generales' AS vista, 8 as posicion, 'Tipo Solicitud' AS key, key_NomPedido, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'datos Generales' AS vista, 9 as posicion, 'Asunto' AS key, txt_AsuntoPed, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 10 as posicion, 'Estado' AS key, txt_Estado, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 11 as posicion, 'Fecha del estado' AS key, dat_FchEstadoAct, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 12 as posicion, 'Asignación de Responsable' AS key, txt_Responsable, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 14 as posicion, 'Nombres' AS key, txt_NomCompEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 15 as posicion, 'Dependencia' AS key, txt_DepEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 16 as posicion, 'Región' AS key, txt_RgnEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 17 as posicion, 'Ubicación' AS key, txt_UbicEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado responsable' AS vista, 18 as posicion, 'Teléfono' AS key, txt_ExtEmp_R, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Detalle del Pedido' AS vista, 19 as posicion, '' AS key, rch_DetallePed, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Comentarios' AS vista, 20 as posicion, '' AS key, txt_Comentarios, anio, mes FROM  Zdc_Lotus_Solicitudes_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 21 as posicion, '' AS key, txt_Historia, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes  FROM Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia \
")

sqlContext.registerDataFrameAsTable(panel_detalle_solicitudes,"Zdp_Lotus_Solicitudes_panel_detalle")

# COMMAND ----------

# DBTITLE 1,Creación Panel Masivos
masivos_reclamos_df = sqlContext.sql("\
  SELECT \
    ZDC.UniversalID, \
    ZDC.key_NomPedido, \
    ZDC.txt_Autor Autor, \
    ZDC.txt_AsuntoPed, \
    ZDC.anio, \
    ZDC.mes, \
    ZDC.txt_Autor, \
    ZDC.dat_FchCreac, \
    ZDC.txt_NomCompEmp_A, \
    ZDC.txt_DepEmp_A, \
    ZDC.txt_RgnEmp_A, \
    ZDC.txt_UbicEmp_A, \
    ZDC.txt_ExtEmp_A, \
    ZDC.txt_Estado, \
    ZDC.dat_FchEstadoAct, \
    ZDC.txt_Responsable, \
    ZDC.txt_NomCompEmp_R, \
    ZDC.txt_DepEmp_R, \
    ZDC.txt_RgnEmp_R, \
    ZDC.txt_UbicEmp_R, \
    ZDC.txt_ExtEmp_R, \
    ZDC.rch_DetallePed, \
    ZDC.txt_Comentarios, \
    HIST.txt_Historia \
FROM \
  Zdc_Lotus_Solicitudes_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_Solicitudes_Tablaintermedia_txt_Historia HIST ON HIST.UniversalID = ZDC.UniversalID \
")