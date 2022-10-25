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
# MAGIC REFRESH TABLE Default.parametros;

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_PEDIDOS_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Creación Panel Principal
panel_principal_pedidos = sqlContext.sql ("\
SELECT \
  UniversalID, \
  UniversalIDPadre, \
  key_NomPedido, \
  txt_Autor Autor, \
  txt_AsuntoPed, \
  dat_FchCreac, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/pedidos/', UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
                          if(txt_Autor is null, '',txt_Autor), ' - ', \
                          if(dat_FchCreac is null, '',dat_FchCreac), ' - ', \
                          if(txt_AsuntoPed is null, '',txt_AsuntoPed)), ' - ',  \
  upper(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
               if(txt_Autor is null, '',txt_Autor), ' - ', \
               if(txt_AsuntoPed is null, '',txt_AsuntoPed))), ' - ', \
  lower(concat(if(key_NomPedido is null, '',key_NomPedido), ' - ', \
               if(txt_Autor is null, '',txt_Autor), ' - ', \
               if(txt_AsuntoPed is null, '',txt_AsuntoPed)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
  anio, \
  mes \
FROM \
  Zdc_Lotus_Pedidos_TablaIntermedia")

sqlContext.registerDataFrameAsTable(panel_principal_pedidos,"Zdp_Lotus_Pedidos_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Creación Panel Detalle
panel_detalle_pedidos = sqlContext.sql("\
SELECT UniversalID, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Encabezado' AS vista, 2 as posicion, 'Fecha Creación' AS key, dat_FchCreac AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 3 as posicion, 'Nombres' AS key, txt_NomCompEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Dependencia' AS key, txt_DepEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Región' AS key, txt_RgnEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Ubicación' AS key, txt_UbicEmp_A AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Teléfono' AS key, txt_TelEnteExt AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Fecha de Entrega de Definición' AS key, dat_FchIniTram AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 9 as posicion, 'Tipo Pedido' AS key, key_NomPedido AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 10 as posicion, 'Prioridad' AS key, key_Prioridad AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Datos Generales' AS vista, 11 as posicion, 'Asunto' AS key, txt_AsuntoPed AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Estado y Acuerdos de Servicio' AS vista, 12 as posicion, 'Estado' AS key, txt_Estado AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Estado y Acuerdos de Servicio' AS vista, 13 as posicion, 'Fecha del Estado' AS key, dat_FchEstadoAct AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Asignación de Responsable' AS vista, 14 as posicion, 'Responsable' AS key, txt_Responsable AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 15 as posicion, 'Nombres' AS key, txt_NomCompEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 16 as posicion, 'Empresa' AS key, txt_EmpresaEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 17 as posicion, 'Dependencia' AS key, txt_DepEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 18 as posicion, 'Región' AS key, txt_RgnEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 19 as posicion, 'Ubicación' AS key, txt_UbicEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Información del Empleado Responsable' AS vista, 20 as posicion, 'Teléfono' AS key, txt_ExtEmp_R AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Comentarios' AS vista, 21 as posicion, '' AS key, txt_Comentarios AS value, anio, mes FROM  Zdc_Lotus_Pedidos_TablaIntermedia \
UNION ALL \
SELECT UniversalID, 'Historia' AS vista, 22 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4)as int) AS Anio,  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2)as int) AS Mes FROM  Zdc_Lotus_Pedidos_Tablaintermedia_txt_Historia \
")

sqlContext.registerDataFrameAsTable(panel_detalle_pedidos,"Zdp_Lotus_Pedidos_panel_detalle")

# COMMAND ----------

# DBTITLE 1,Creación Panel Masivos
masivos_pedidos_df = sqlContext.sql("\
  SELECT \
    ZDC.UniversalID, \
    ZDC.UniversalIDPadre, \
    ZDC.key_NomPedido, \
    ZDC.txt_Autor Autor, \
    ZDC.txt_AsuntoPed, \
    ZDC.dat_FchCreac, \
    ZDC.txt_Autor, \
    ZDC.txt_NomCompEmp_A, \
    ZDC.txt_DepEmp_A, \
    ZDC.txt_RgnEmp_A, \
    ZDC.txt_UbicEmp_A, \
    ZDC.txt_TelEnteExt, \
    ZDC.dat_FchIniTram, \
    ZDC.key_Prioridad, \
    ZDC.txt_Estado, \
    ZDC.dat_FchEstadoAct, \
    ZDC.txt_Responsable, \
    ZDC.txt_NomCompEmp_R, \
    ZDC.txt_EmpresaEmp_R, \
    ZDC.txt_DepEmp_R, \
    ZDC.txt_RgnEmp_R, \
    ZDC.txt_UbicEmp_R, \
    ZDC.txt_ExtEmp_R, \
    ZDC.txt_Comentarios, \
    HIS.txt_Historia, \
    ZDC.anio, \
    ZDC.mes \
FROM \
  Zdc_Lotus_Pedidos_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_Pedidos_Tablaintermedia_txt_Historia HIS ON  HIS.UniversalID = ZDC.UniversalID")