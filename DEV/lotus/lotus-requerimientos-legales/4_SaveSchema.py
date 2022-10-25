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
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_REQ_LEG_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Panel Principal
sql_string = "SELECT TI.UniversalId, \
  TI.txt_AsuntoPed AS Asunto, \
  TI.key_TipoReqmto AS TipoRequerimiento, \
  substring_index(TI.dat_FchCreac, ' ', 1) AS fecha_requerimiento_legal, \
  CN.txt_CedNit, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', TI.UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
                          if(key_TipoReqmto is null,'',key_TipoReqmto), ' - ', \
                          if(CN.txt_CedNit is null, '', txt_CedNit)), ' - ', \
  upper(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
               if(key_TipoReqmto is null, '', key_TipoReqmto))), ' - ', \
  lower(concat(if(txt_AsuntoPed is null, '',txt_AsuntoPed), ' - ', \
               if(key_TipoReqmto is null, '', key_TipoReqmto)))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS busqueda, \
  cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, \
  cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes \
FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia AS TI \
  LEFT JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit AS CN ON TI.UniversalId = CN.UniversalId"

panel_principal_req_leg = sqlContext.sql(sql_string)
sqlContext.registerDataFrameAsTable(panel_principal_req_leg, "Zdp_Lotus_Requerimientos_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_req_leg = sqlContext.sql("\
SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, txt_Autor AS value, \
cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM \
Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Creador' AS key, txt_Creador AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado' AS vista, 3 as posicion, 'Fecha creación' AS key, dat_FchCreac AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Nombres' AS key, txt_NomCompEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Dependencia' AS key, txt_DepEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Región' AS key, txt_RgnEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Ubicación' AS key, txt_UbicEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Teléfono' AS key, txt_ExtEmp_A AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 9 as posicion, 'Tipo Empresa' AS key, key_TipoEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 10 as posicion, 'Nombre Empresa' AS key, txt_EmpEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 11 as posicion, 'Ext Teléfono Empresa' AS key, txt_TelEnteExt AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 12 as posicion, 'Fecha de entrega de definición' AS key, dat_FchIniTram AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 13 as posicion, 'Vencido (días hábiles totales)' AS key, IF(num_DVTotal = '', '0', num_DVTotal) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 14 as posicion, 'Vencido (días hábiles estado actual)' AS key, IF(num_DVParcial = '', '0', num_DVParcial) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 15 as posicion, '' AS key, key_ViceAtdPed AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 16 as posicion, 'Tipo Solicitud' AS key, key_NomPedido AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Generales' AS vista, 17 as posicion, 'Asunto' AS key, txt_AsuntoPed AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 18 as posicion, 'Estado' AS key, txt_Estado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 19 as posicion, 'Fecha del estado' AS key, dat_FchEstadoAct AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 20 as posicion, 'Tiempo máximo en el estado (días)' AS key, num_TMaxEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 21 as posicion, 'Fecha máxima del estado' AS key, dat_FchMaxEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 22 as posicion, 'Tiempo cotizado (días)' AS key, num_TCotizado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 23 as posicion, 'Fecha vencimiento' AS key, dat_FchVencEstado AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Asignación de Responsable' AS vista, 24 as posicion, 'Responsable' AS key, txt_Responsable AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 25 as posicion, 'Nombres' AS key, txt_NomCompEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 26 as posicion, 'Dependencia' AS key, txt_DepEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 27 as posicion, 'Región' AS key, txt_RgnEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 28 as posicion, 'Ubicación' AS key, txt_UbicEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 29 as posicion, 'Teléfono' AS key, txt_ExtEmp_R AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT ti.UniversalId, 'Detalle del Pedido' AS vista, 30 as posicion, 'Detalle pedido' AS key, dp.rch_DetallePed AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_DetallePed AS dp ON ti.UniversalId = dp.UniversalId \
UNION ALL \
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 31 as posicion, 'Filial' AS key, key_Filial AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 32 as posicion, 'Sucursal o Área origen' AS key, key_SucArea AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 33 as posicion, 'Ente solicitante' AS key, key_EnteSol AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 34 as posicion, 'Nombre del Ente' AS key, txt_OtroEnte AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 35 as posicion, 'Número del oficio' AS key, txt_NumOficio AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 36 as posicion, 'Fecha del oficio' AS key, dat_FechOficio AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 37 as posicion, 'Tipo de requerimiento' AS key, key_TipoReqmto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 38 as posicion, 'Requerimiento' AS key, txt_OtroReqmto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 39 as posicion, 'Días para su solución' AS key, concat(num_DiaSol, ' - ', CASE \
     WHEN UPPER(key_TipoConteo) = 'C' THEN 'Calendario' \
     WHEN UPPER(key_TipoConteo) = 'H' THEN 'Hábiles' \
     ELSE '' \
    END) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 40 as posicion, 'Días de prórroga' AS key, num_DiasPro AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Requerimiento' AS vista, 41 as posicion, 'Fecha de vencimiento del oficio' AS key, dat_FechVto AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT ti.UniversalId, 'Datos Detalle' AS vista, 42 as posicion, 'Cédula o Nit' AS key, cn.txt_CedNit AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_CedNit AS cn ON ti.UniversalId = cn.UniversalId \
UNION ALL \
SELECT UniversalId, 'Datos Detalle' AS vista, 43 as posicion, '¿Vinculado?' AS key, CASE \
     WHEN UPPER(txt_Pcto) = 'S' THEN 'Si' \
     WHEN UPPER(txt_Pcto) = 'N' THEN 'No' \
     ELSE '' \
    END AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Entidad' AS vista, 44 as posicion, 'Ciudad' AS key, key_Ciudad AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Entidad' AS vista, 45 as posicion, 'Funcionario' AS key, txt_Funcionario AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Entidad' AS vista, 46 as posicion, 'Dirección' AS key, txt_Direccion AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Entidad' AS vista, 47 as posicion, 'Banco' AS key, txt_Banco AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT UniversalId, 'Datos Entidad' AS vista, 48 as posicion, 'Cuenta déposito' AS key, txt_CtaDep AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia \
UNION ALL \
SELECT ti.UniversalId, 'Comentarios' AS vista, 49 as posicion, 'Comentarios' AS key, c.txt_Comentarios AS value, substr(substring_index(ti.dat_FchCreac, ' ', 1),-4,4) AS anio, substr(substring_index(ti.dat_FchCreac, ' ', 1),-7,2) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ti \
INNER JOIN Zdc_Lotus_RequerimientosLegales_Tablaintermedia_Comentarios AS c ON ti.UniversalId = c.UniversalId \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 50 as posicion, 'Historia' AS key, concat_ws('\n', txt_Historia) AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_RequerimientosLegales_Tablaintermedia ")
sqlContext.registerDataFrameAsTable(panel_detalle_req_leg, "Zdp_Lotus_Reclasificados_Panel_Detalle")

# COMMAND ----------

# DBTITLE 1,Panel Masivos
"""
Descripción: Objeto dataframe para crear sabana para descarga de información masiva, se compone de los campos de la tabla principal y la 
             tabla detallada de la aplicación Req Legales, omite los campos Link y Busqueda de la tabla ppal.
Campos de particionamiento: anio (int), mes(int).
Fecha de creción: 08/10/2019.
Autor: Juan David Escobar Escobar.
"""
from pyspark.sql.functions import col,substring,substring_index,translate
from pyspark.sql import functions as F

masivos_req_legales_df = req_legales_df.alias('ZT').join(req_legales_DetallePed_df.alias('DP'),
                                   col('ZT.UniversalId') == col('DP.UniversalId'),
                                   how='left'
                         ).join(req_legales_CedNit_df.alias('CN'),
                                   col('ZT.UniversalId') == col('CN.UniversalId'),
                                   how='left'
                         ).join(req_legales_Comentarios_df.alias('CO'),
                                   col('ZT.UniversalId') == col('CO.UniversalId'),
                                   how='left'
                         ).select(col('ZT.UniversalID'),                      
                                  col('ZT.txt_AsuntoPed').alias('Asunto'),
                                  col('ZT.key_TipoReqmto').alias('TipoRequerimiento'),
                                  substring_index('ZT.dat_FchCreac', ' ', 1).alias('fecha_requerimiento_legal'),
                                  substring(substring_index('ZT.dat_FchCreac', ' ', 1),-4,4).cast('int').alias('anio'),
                                  substring(substring_index('ZT.dat_FchCreac', ' ', 1),-7,2).cast('int').alias('mes'),
                                  col('ZT.txt_Autor').alias('Autor'),
                                  col('ZT.txt_Creador').alias('Creador'),
                                  col('ZT.txt_NomCompEmp_A').alias('Nombres'),               
                                  col('ZT.txt_DepEmp_A').alias('Dependencia'),
                                  col('ZT.txt_RgnEmp_A').alias('Region'),
                                  col('ZT.txt_UbicEmp_A').alias('Ubicacion'),
                                  col('ZT.txt_ExtEmp_A').alias('Telefono'),
                                  col('ZT.key_TipoEnteExt').alias('TipoEmpresa'),
                                  col('ZT.txt_EmpEnteExt').alias('NombreEmpresa'),
                                  col('ZT.txt_TelEnteExt').alias('ExtTelefonoEmpresa'),
                                  col('ZT.dat_FchIniTram').alias('FechaDeEntregaDeDefinición'),                                 
                                  F.when(col('ZT.num_DVTotal') == '', '0').otherwise(col('ZT.num_DVTotal')).alias('VencidoDiasHabilesTotales'),
                                  F.when(col('ZT.num_DVParcial') == '', '0').otherwise(col('ZT.num_DVParcial')).alias('VencidoDiasHabilesEstadoActua'),
                                  col('ZT.key_ViceAtdPed').alias('ViceAtdPed'),
                                  col('ZT.key_NomPedido').alias('TipoSolicitud'),
                                  col('ZT.txt_AsuntoPed').alias('AsuntoPerdido'),
                                  col('ZT.txt_Estado').alias('Estado'),
                                  col('ZT.dat_FchEstadoAct').alias('FechaDelEstado'),
                                  col('ZT.num_TMaxEstado').alias('TiempoMaximoEnElEstadoDias'),
                                  col('ZT.dat_FchMaxEstado').alias('FechaMaximaDelEstado'),
                                  col('ZT.num_TCotizado').alias('TiempoCotizadoDias'),
                                  col('ZT.dat_FchVencEstado').alias('FechaVencimiento'),
                                  col('ZT.txt_Responsable').alias('Responsable'),
                                  col('ZT.txt_NomCompEmp_R').alias('NombresResponsable'),
                                  col('ZT.txt_DepEmp_R').alias('DependenciaResponsable'),
                                  col('ZT.txt_RgnEmp_R').alias('RegionEmpleadoResponsable'),
                                  col('ZT.txt_UbicEmp_R').alias('UbicacionResponsable'),
                                  col('ZT.txt_ExtEmp_R').alias('TelefonoResponsable'),
                                  col('DP.rch_DetallePed').alias('DetallePedido'),
                                  col('ZT.key_Filial').alias('Filial'),
                                  col('ZT.key_SucArea').alias('SucursalAreaOrigen'),
                                  col('ZT.key_EnteSol').alias('EnteSolicitante'),
                                  col('ZT.txt_OtroEnte').alias('NombreDelEnte'),
                                  col('ZT.txt_NumOficio').alias('NUmeroDelOficio'),
                                  col('ZT.dat_FechOficio').alias('FechaDelOficio'),                                  
                                  col('ZT.txt_OtroReqmto').alias('Requerimiento'),
                                  F.when(F.upper(col('ZT.key_TipoConteo')) == 'C', 'Calendario')
                                   .when(F.upper(col('ZT.key_TipoConteo')) == 'H', 'Hábiles')
                                   .otherwise('').alias('DiasParaSuSolucion'),
                                  col('ZT.num_DiasPro').alias('DiasDeProrroga'),
                                  col('ZT.dat_FechVto').alias('FechaDevencimientoDelOficio'),
                                  col('CN.txt_CedNit').alias('CedulaNit'),
                                  F.when(F.upper(col('ZT.txt_Pcto')) == 'S', 'Si')
                                   .when(F.upper(col('ZT.txt_Pcto')) == 'N', 'No')
                                   .otherwise('').alias('Vinculado'),
                                  col('ZT.key_Ciudad').alias('Ciudad'),
                                  col('ZT.txt_Funcionario').alias('Funcionario'),
                                  col('ZT.txt_Direccion').alias('Direccion'),
                                  col('ZT.txt_Banco').alias('Banco'),
                                  col('ZT.txt_CtaDep').alias('CuentaDeposito'),
                                  col('CO.txt_Comentarios').alias('Comentarios'),
                                  F.concat_ws('\n' ,col('ZT.txt_Historia')).alias('Historia')                                
                          ).distinct()