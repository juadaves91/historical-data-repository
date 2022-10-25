# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se requiere efectuar refresco de la tabla cada vez que se insertan o actualizan datos antes de ser consultada.
# MAGIC Fecha: 25/02/2020
# MAGIC Responsable: Juan David Escobar E.
# MAGIC */
# MAGIC 
# MAGIC REFRESH TABLE Default.parametros;

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLAMOS_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Panel Principal
sql_string = "SELECT \
    UniversalID \
    ,txt_CedNit.value CedNit \
    ,substring_index(dat_FchCreac.value, ' ', 1) fecha_solicitudes \
    ,key_Producto.value Producto \
    ,key_TipoReclamo.value TipoReclamo \
    ,num_AbonoDefinit.value AbonoDefinitivo \
    ,txt_NomCliente.value NomCliente \
    ,cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio \
    ,cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes \
    ,IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/reclamos-old/', UniversalID,'.zip'" + "), '') link \
    ,translate(concat(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value), ' - ' \
    ,upper(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value)), ' - ' \
    ,lower(concat(txt_CedNit.value, ' - ' , key_TipoReclamo.value, ' - ', key_Producto.value, ' - ', num_AbonoDefinit.value, ' - ', txt_NomCliente.value))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') Busqueda \
FROM \
  Zdc_Lotus_Reclamos_old_TablaIntermedia"
panel_principal_reclamos_old = sqlContext.sql(sql_string)
sqlContext.registerDataFrameAsTable(panel_principal_reclamos_old,"Zdp_Lotus_Reclamos_old_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_reclamos_old = sqlContext.sql("\
SELECT UniversalId, 'Reclamos' AS vista, 1 as posicion, 'Días Vencidos Totales' AS key, num_DV.value value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 2 as posicion, 'Días Vencidos de Soportes' AS key, num_DVPendiente.value value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 3 as posicion, 'Días Vencidos Mala radicación' AS key, num_DVMalRad.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 4 as posicion, 'Días Vencidos Pendiente Documentación' AS key, num_DVFraudeTrj.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 5 as posicion, 'Días Vencidos respuesta al CLiente' AS key, num_DVSlnado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 6 as posicion, 'Días Vencidos Solucionado con Carta' AS key, num_DVSlnadoCarta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 7 as posicion, 'Días Vencidos Pendiente Ajuste' AS key, num_DVPendAjuste.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 8 as posicion, 'Días Vencidos Verificación' AS key, num_DVVerif.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 9 as posicion, 'Días Vencidos Investigación' AS key, num_DVInvest.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 10 as posicion, 'Días Vencidos Redes' AS key, num_DVRedes.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 11 as posicion, 'Días Vencidos Bancos' AS key, num_DVBancos.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 12 as posicion, 'Días Vencidos Tramite Interno' AS key, num_DVTrmInterno.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 13 as posicion, 'Días Vencidos Solicita Respuesta Escrita' AS key, num_DVSolicRptaEsc.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 14 as posicion, 'Días Vencidos Cliente No Contactado' AS key, num_DVClteNoCont.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 15 as posicion, 'Días Vencidos Carta en Revisión' AS key, num_DVCartaEnRev.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 16 as posicion, 'Días Vencidos Susceptible Abono Temporal Abierto' AS key, num_DVAboTempA.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamos' AS vista, 17 as posicion, 'Días Vencidos Susceptible Abono Temporal Pendiente' AS key, num_DVAboTempP.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 18 as posicion, 'Autor' AS key, txt_Autor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 19 as posicion, 'Creador' AS key, txt_Creador.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 20 as posicion, 'Fecha de Creación' AS key, dat_FchCreac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 21 as posicion, 'Sucursal de Captura' AS key, concat(key_NomSucCaptura.value, '\n', key_SucursalFilial.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 22 as posicion, 'Region de Captura' AS key, txt_RgnCaptura.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 23 as posicion, 'Zona de Captura' AS key, txt_ZonaCaptura.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 24 as posicion, 'Reportado Por' AS key, key_ReportadoPor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 25 as posicion, 'Origen del reporte' AS key, key_OrigenReporte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 26 as posicion, 'Responsable' AS key, nom_Responsable_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 27 as posicion, 'Abono Temporal' AS key, num_AbonoTemp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 28 as posicion, 'Abono Definitivo' AS key, num_AbonoDefinit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 29 as posicion, 'Responsable del Error' AS key, key_ResponError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 30 as posicion, 'Nombre del Responsable Error' AS key, txt_NomRespError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 31 as posicion, 'El Cliente tiene la Razon?' AS key, key_ClienteRazon.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Radicación' AS vista, 32 as posicion, 'Causa de no Abono' AS key, key_CausaNoAbono.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 33 as posicion, 'Usuario que Cierra' AS key, nom_UsuCierra.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 34 as posicion, 'Tiempo Total (Creado - Cerrado)' AS key, num_DRecl.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 35 as posicion, 'Medio de Respuesta al Cliente' AS key, key_MedioRpta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 36 as posicion, 'Tipo de Documento' AS key, key_TipoDocumento.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 37 as posicion, 'Cedula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 38 as posicion, 'Nombre' AS key, txt_NomCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 39 as posicion, 'Tipo Cliente' AS key, txt_TipoCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 40 as posicion, 'Funcionario de la Empresa' AS key, txt_FuncEmpresa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 41 as posicion, 'SubSegmento' AS key, txt_SubSgmtoCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 42 as posicion, 'Es Cliente Gerenciado?' AS key, CASE \
    WHEN LENGTH(COALESCE(txt_clteger.value,'')) = 0 AND LENGTH(txt_CedNit.value) = 0 THEN '' \
    WHEN LENGTH(COALESCE(txt_clteger.value,'')) = 0 AND LENGTH(txt_CedNit.value) > 0 THEN 'No' \
    WHEN UPPER(txt_clteger.value) = 'G' AND LENGTH(txt_CedNit.value) >= 0  THEN 'Si' \
    WHEN UPPER(txt_clteger.value) = 'N' AND LENGTH(txt_CedNit.value) >= 0  THEN 'No' \
  END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 43 as posicion, 'Gerente de Cuenta' AS key, txt_GteCuenta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 44 as posicion, 'Es Cliente Colombia?' AS key, CASE \
    WHEN LENGTH(COALESCE(txt_ClteColombia.value,'')) = 0 AND LENGTH(txt_CedNit.value) = 0 THEN '' \
    WHEN LENGTH(COALESCE(txt_ClteColombia.value,'')) = 0 AND LENGTH(txt_CedNit.value) > 0 THEN 'No' \
    WHEN UPPER(txt_ClteColombia.value) = 'CC' AND LENGTH(txt_CedNit.value) >= 0  THEN 'Si' \
    WHEN UPPER(txt_ClteColombia.value) = 'N' AND LENGTH(txt_CedNit.value) >= 0  THEN 'No' \
  END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 45 as posicion, 'Ciudad de Correspondencia' AS key, concat(txt_CiudadCorr.value, ' - ', txt_DescCiudadCorr.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 46 as posicion, 'Dpto Correspondencia' AS key, txt_DptoCorr.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 47 as posicion, 'Dirección Correspondencia' AS key, txt_DirCorr.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 48 as posicion, 'País' AS key, txt_Pais.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 49 as posicion, 'Estado' AS key, txt_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 50 as posicion, 'Dirección Postal' AS key, txt_DirPostal.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 51 as posicion, 'Apartado Aereo (P.O BOX)' AS key, txt_ApartAereo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 52 as posicion, 'Identificador Dirección' AS key, txt_IdDireccion.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 53 as posicion, 'Como Desea Recibir Respuesta a su Reclamo?' AS key, key_RptaReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 55 as posicion, 'Telefonos' AS key, txt_TeleCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 56 as posicion, 'Fax' AS key, txt_FaxCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Cliente' AS vista, 57 as posicion, 'E-mail' AS key, txt_EmailCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 58 as posicion, 'Nombre del Responsable' AS key, nom_respGer.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 59 as posicion, 'Telefono Directo del Responsable' AS key, txt_TelRespGer.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 60 as posicion, 'Tiempo Estimado de Solución' AS key, num_TpoEstSol.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 61 as posicion, 'Fecha Aprox de Solución' AS key, dat_FchAproxSol.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 62 as posicion, 'Tiempo Real de Solución' AS key, num_DAbierto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 63 as posicion, 'Fecha real de Solución' AS key, dat_Solucionado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 64 as posicion, 'Compañia' AS key, txt_Cia.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 65 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 66 as posicion, 'Clasificación' AS key, key_TipoTarjeta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 67 as posicion, 'Tipo Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 68 as posicion, 'Número de Producto' AS key,concat(key_Prefijo.value, ' - ', txt_NumProducto.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 69 as posicion, 'Codigo de Compra' AS key, txt_NroCompraRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 70 as posicion, 'Código de Producto' AS key, txt_NroProductoRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 71 as posicion, 'Sub-Proceso Dueño del Reclamo' AS key, txt_SubprocRecl.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 72 as posicion, 'Número de Tarjeta Debito' AS key, txt_NumTarjDeb.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 73 as posicion, 'Número de Poliza' AS key, txt_NumPoliza.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 74 as posicion, 'Número de Encargo Fiduciario' AS key, txt_NumEncargo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 75 as posicion, 'Sucursal de Radicacion' AS key, key_NomSucRadica.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 76 as posicion, 'Tipo de Poliza' AS key, txt_TipoPoliza.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 77 as posicion, 'Número de la Tarjeta/Cuenta que se Debita' AS key, txt_NumTjtaCtaDeb.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 78 as posicion, 'Ciudad de Transacción/Suceso' AS key, key_CiudadTransa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 79 as posicion, 'Fecha Transacción' AS key, dat_FchTransac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 80 as posicion, 'Suc/Area de Queja, Felicitacion o Sugerencia' AS key, key_NomSucVarios.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 81 as posicion, 'Código Unico de Establecimiento' AS key, txt_CodEstab.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 82 as posicion, 'Número Transacciones' AS key, key_NumTransa.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 83 as posicion, 'Valor Total en Pesos' AS key, ValorTotalEnPesos AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 84 as posicion, 'Valor Total en Dolares' AS key, ValorTotalEnDolares AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 85 as posicion, 'Mensaje de Ayuda' AS key, txt_MensajeAyuda AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeAyuda \
UNION ALL \
SELECT UniversalId, 'Datos del Reclamo' AS vista, 86 as posicion, 'Soportes requeridos' AS key, txt_MensajeSoportes AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeSoportes \
UNION ALL \
SELECT UniversalId, 'Observaciones' AS vista, 87 as posicion, '' AS key, txt_ComentRapp.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes Recibidos' AS vista, 88 as posicion, 'Soportes recibidos del Cliente' AS key, txt_SopRecCte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes Recibidos' AS vista, 89 as posicion, 'Cuales no se han Recibido' AS key, txt_SopNoRec.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes Recibidos' AS vista, 90 as posicion, 'Se recibieron todos los Soportes?' AS key, key_TodosSoportes.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Pantallas que Documentan el Reclamo' AS vista, 91 as posicion, '' AS key, rch_Pantallas AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_rch_Pantallas \
UNION ALL \
SELECT UniversalId, 'Pantallas que Certifican la Solución' AS vista, 92 as posicion, '' AS key, rch_PantallaSln AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln \
UNION ALL \
SELECT UniversalId, 'Comentarios' AS vista, 93 as posicion, '' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 94 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia \
UNION ALL \
SELECT UniversalId, 'Respuesta al Cliente' AS vista, 95 as posicion, '' AS key, rch_RptaCliente AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente \
UNION ALL \
SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 96 as posicion, '' AS key, txt_NomOpTblM AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM \
UNION ALL \
SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 97 as posicion, '' AS key, key_VlrsOpTblM AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM")

sqlContext.registerDataFrameAsTable(panel_detalle_reclamos_old,"Zdp_Lotus_Reclamos_old_Panel_Detalle")

# COMMAND ----------

# DBTITLE 1,Panel Trasacciones
panel_trasacciones_reclamos_old = sqlContext.sql("\
SELECT UniversalId, 'TRANSACCIONES 1' AS vista, 1 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 2 as posicion, 'Valor transacción' AS key, num_ValorTransa_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 3 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 4 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 5 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_1.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 6 as posicion, 'Moneda' AS key, key_Moneda_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 7 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 8 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 9 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_1' AS vista, 10 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 2' AS vista, 11 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 12 as posicion, 'Valor transacción' AS key, num_ValorTransa_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 13 as posicion, 'Fecha de transacción' AS key, Dat_FechaTransa_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 14 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 15 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_2.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 16 as posicion, 'Moneda' AS key, key_Moneda_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 17 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 18 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 19 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_2' AS vista, 20 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_2.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 3' AS vista, 21 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 22 as posicion, 'Valor transacción' AS key, num_ValorTransa_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 23 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 24 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 25 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_3.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 26 as posicion, 'Moneda' AS key, key_Moneda_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 27 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 28 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 29 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_3' AS vista, 30 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_3.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 4' AS vista, 31 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 32 as posicion, 'Valor transacción' AS key, num_ValorTransa_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 33 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 34 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 35 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_4.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 36 as posicion, 'Moneda' AS key, key_Moneda_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 37 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 38 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 39 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_4' AS vista, 40 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_4.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 5' AS vista, 41 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 42 as posicion, 'Valor transacción' AS key, num_ValorTransa_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 43 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 44 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 45 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_5.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 46 as posicion, 'Moneda' AS key, key_Moneda_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 47 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 48 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 49 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_5' AS vista, 50 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_5.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 6' AS vista, 51 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 52 as posicion, 'Valor transacción' AS key, num_ValorTransa_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 53 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 54 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 55 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_6.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 56 as posicion, 'Moneda' AS key, key_Moneda_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 57 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 58 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 59 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_6' AS vista, 60 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_6.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 7' AS vista, 61 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 62 as posicion, 'Valor transacción' AS key, num_ValorTransa_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 63 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 64 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 65 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_7.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 66 as posicion, 'Moneda' AS key, key_Moneda_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 67 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 68 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 69 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_7' AS vista, 70 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_7.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 8' AS vista, 71 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 72 as posicion, 'Valor transacción' AS key, num_ValorTransa_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 73 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 74 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 75 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_8.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 76 as posicion, 'Moneda' AS key, key_Moneda_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 77 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 78 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 79 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_8' AS vista, 80 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_8.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'TRANSACCIONES 9' AS vista, 81 as posicion, '' AS key, '' AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 82 as posicion, 'Valor transacción' AS key, num_ValorTransa_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 83 as posicion, 'Fecha de transacción' AS key, dat_FechaTransa_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 84 as posicion, 'Oficina de Transacción' AS key, key_OficinaTransa_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 85 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(txt_Referencia_9.value) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 86 as posicion, 'Moneda' AS key, key_Moneda_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 87 as posicion, 'Ciudad Transacción' AS key, key_CiudadTransa_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 88 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, txt_NumeroCuenta_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 89 as posicion, 'Número de Cheque/Voucher' AS key, num_ChequeVouch_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Transacciones_9' AS vista, 90 as posicion, 'Fecha de Consignación' AS key, dat_FechaConsigna_9.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_Reclamos_old_TablaIntermedia \
")
sqlContext.registerDataFrameAsTable(panel_trasacciones_reclamos_old,"Zdp_Lotus_Reclamos_old_Panel_Transacciones")

# COMMAND ----------

masivos_reclamos_old_df = sqlContext.sql("\
SELECT \
   ZDC.UniversalID, \
   cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
   cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
   ZDC.txt_CedNit.value CedNit, \
   substring_index(ZDC.dat_FchCreac.value, ' ', 1) fecha_solicitudes, \
   ZDC.key_Producto.value Producto, \
   ZDC.key_TipoReclamo.value TipoReclamo, \
   ZDC.num_AbonoDefinit.value AbonoDefinitivo, \
   ZDC.txt_NomCliente.value NomCliente, \
   ZDC.num_DV.value DiasVencidosTotales, \
   ZDC.num_DVPendiente.value DiasVencidosSoportes, \
   ZDC.num_DVMalRad.value DiasVencidosMalaRadicacion, \
   ZDC.num_DVFraudeTrj.value DiasVencidosPendienteDocumentacion, \
   ZDC.num_DVSlnado.value DiasVencidosRespuestaCLiente, \
   ZDC.num_DVSlnadoCarta.value DiasVencidosSolucionadoCarta, \
   ZDC.num_DVPendAjuste.value DiasVencidosPendienteAjuste, \
   ZDC.num_DVVerif.value DiasVencidosVerificacion, \
   ZDC.num_DVInvest.value DiasVencidosInvestigacion, \
   ZDC.num_DVRedes.value DiasVencidosRedes, \
   ZDC.num_DVBancos.value DiasVencidosBancos, \
   ZDC.num_DVTrmInterno.value DiasVencidosTramiteInterno, \
   ZDC.num_DVSolicRptaEsc.value DiasVencidosSolicitaRespuestaEscrita, \
   ZDC.num_DVClteNoCont.value DiasVencidosClienteContactado, \
   ZDC.num_DVCartaEnRev.value DiasVencidosCartaRevision, \
   ZDC.num_DVAboTempA.value DiasVencidosSusceptibleAbonoTemporalAbierto, \
   ZDC.num_DVAboTempP.value DiasVencidosSusceptibleAbonoTemporalPendiente, \
   ZDC.txt_Autor.value Autor, \
   ZDC.txt_Creador.value Creador, \
   concat(ZDC.key_NomSucCaptura.value, '\n', ZDC.key_SucursalFilial.value) SucursalCaptura, \
   txt_RgnCaptura.value RegionCaptura, \
   ZDC.txt_ZonaCaptura.value ZonaCaptura, \
   ZDC.key_ReportadoPor.value ReportadoPor, \
   ZDC.key_OrigenReporte.value OrigenReporte, \
   ZDC.nom_Responsable_1.value Responsable, \
   ZDC.num_AbonoTemp.value AbonoTemporal, \
   ZDC.key_ResponError.value ResponsableError, \
   ZDC.txt_NomRespError.value NombreResponsableError, \
   ZDC.key_ClienteRazon.value ClientetieneRazon, \
   ZDC.key_CausaNoAbono.value CausaAbono, \
   ZDC.nom_UsuCierra.value UsuarioCierra, \
   ZDC.num_DRecl.value TiempoTotalCreadoCerrado, \
   ZDC.key_MedioRpta.value MedioRespuestaCliente, \
   ZDC.key_TipoDocumento.value TipoDocumento, \
   ZDC.txt_CedNit.value CedulaNit, \
   ZDC.txt_NomCliente.value Nombre, \
   ZDC.txt_TipoCliente.value TipoCliente, \
   ZDC.txt_FuncEmpresa.value FuncionarioEmpresa, \
   ZDC.txt_SubSgmtoCliente.value SubSegmento, \
   CASE \
    WHEN LENGTH(COALESCE(ZDC.txt_clteger.value,'')) = 0 AND LENGTH(ZDC.txt_CedNit.value) = 0 THEN '' \
    WHEN LENGTH(COALESCE(ZDC.txt_clteger.value,'')) = 0 AND LENGTH(ZDC.txt_CedNit.value) > 0 THEN 'No' \
    WHEN UPPER(ZDC.txt_clteger.value) = 'G' AND LENGTH(ZDC.txt_CedNit.value) >= 0  THEN 'Si' \
    WHEN UPPER(ZDC.txt_clteger.value) = 'N' AND LENGTH(ZDC.txt_CedNit.value) >= 0  THEN 'No' \
  END ClienteGerenciado, \
  ZDC.txt_GteCuenta.value GerenteCuenta, \
  CASE \
    WHEN LENGTH(COALESCE(ZDC.txt_ClteColombia.value,'')) = 0 AND LENGTH(ZDC.txt_CedNit.value) = 0 THEN '' \
    WHEN LENGTH(COALESCE(ZDC.txt_ClteColombia.value,'')) = 0 AND LENGTH(ZDC.txt_CedNit.value) > 0 THEN 'No' \
    WHEN UPPER(ZDC.txt_ClteColombia.value) = 'CC' AND LENGTH(ZDC.txt_CedNit.value) >= 0  THEN 'Si' \
    WHEN UPPER(ZDC.txt_ClteColombia.value) = 'N' AND LENGTH(ZDC.txt_CedNit.value) >= 0  THEN 'No' \
  END ClienteColombia, \
  concat(ZDC.txt_CiudadCorr.value, ' - ', ZDC.txt_DescCiudadCorr.value) CiudadCorrespondencia, \
  txt_DptoCorr.value DptoCorrespondencia, \
  ZDC.txt_DirCorr.value DireccionCorrespondencia, \
  ZDC.txt_Pais.value Pais, \
  ZDC.txt_Estado.value Estado, \
  ZDC.txt_DirPostal.value DireccionPostal, \
  ZDC.txt_ApartAereo.value ApartadoAereo, \
  ZDC.txt_IdDireccion.value IdentificadorDireccion, \
  ZDC.key_RptaReclamo.value DeseaRecibirRespuestaReclamo, \
  ZDC.txt_TeleCliente.value Telefonos, \
  ZDC.txt_FaxCliente.value Fax, \
  ZDC.txt_EmailCliente.value Email, \
  ZDC.nom_respGer.value NombreResponsable, \
  ZDC.txt_TelRespGer.value TelefonoDirectoResponsable, \
  ZDC.num_TpoEstSol.value TiempoEstimadoSolucion, \
  ZDC.dat_FchAproxSol.value FechaAproxSolucion, \
  ZDC.num_DAbierto.value TiempoRealSolucion, \
  ZDC.dat_Solucionado.value FecharealSolucion, \
  ZDC.txt_Cia.value Compania, \
  ZDC.key_TipoTarjeta.value Clasificacion, \
  concat(ZDC.key_Prefijo.value, ' - ', ZDC.txt_NumProducto.value) NumeroProducto, \
  ZDC.txt_NroCompraRapp.value CodigoCompra, \
  ZDC.txt_NroProductoRapp.value CodigoProducto, \
  ZDC.txt_SubprocRecl.value SubProcesoDuenoReclamo, \
  ZDC.txt_NumTarjDeb.value NumeroTarjetaDebito, \
  ZDC.txt_NumPoliza.value NumeroPoliza, \
  ZDC.txt_NumEncargo.value NumeroEncargoFiduciario, \
  ZDC.key_NomSucRadica.value SucursalRadicacion, \
  ZDC.txt_TipoPoliza.value TipoPoliza, \
  ZDC.txt_NumTjtaCtaDeb.value NumeroTarjetaCuentaDebita, \
  ZDC.key_CiudadTransa.value CiudadTransaccionSuceso, \
  ZDC.dat_FchTransac.value FechaTransaccion, \
  ZDC.key_NomSucVarios.value SucAreaQuejaelicitacionSugerencia, \
  ZDC.txt_CodEstab.value CodigoUnicoEstablecimiento, \
  ZDC.key_NumTransa.value NumeroTransacciones, \
  SOL.ValorTotalEnPesos, \
  SOL.ValorTotalEnDolares, \
  MEA.txt_MensajeAyuda MensajeAyuda, \
  MES.txt_MensajeSoportes Soportesrequeridos, \
  ZDC.txt_ComentRapp.value txt_ComentRapp, \
  ZDC.txt_SopRecCte.value SoportesRecibidosCliente, \
  ZDC.txt_SopNoRec.value CualesNoRecibido, \
  ZDC.key_TodosSoportes.value SeRecibieronSoportes, \
  PAN.rch_Pantallas PantallasDocumentanReclamo, \
  PANSLN.rch_PantallaSln PantallasCertificanSolucion, \
  COM.txt_Comentarios Comentarios, \
  HIS.txt_Historia Historia, \
  CLI.rch_RptaCliente RespuestaCliente, \
  NOM.txt_NomOpTblM DatosAdicionalesReclamo, \
  VLR.key_VlrsOpTblM VlrDatosAdicionalesReclamo, \
  ZDC.num_ValorTransa_1.value ValorTransaccion_1, \
  ZDC.dat_FechaTransa_1.value FechaTransaccion_1, \
  ZDC.key_OficinaTransa_1.value OficinaTransaccion_1, \
  ZDC.txt_Referencia_1.value NombreEstablecimientoReferencia_1, \
  ZDC.key_Moneda_1.value Moneda_1, \
  ZDC.key_CiudadTransa_1.value CiudadTransaccion_1, \
  ZDC.txt_NumeroCuenta_1.value NumeroCuentaTarjetaAsociada_1, \
  ZDC.num_ChequeVouch_1.value NumeroChequeVoucher_1, \
  ZDC.dat_FechaConsigna_1.value FechaConsignacion_1, \
  ZDC.num_ValorTransa_2.value ValorTransaccion_2, \
  ZDC.dat_FechaTransa_2.value FechaTransaccion_2, \
  ZDC.key_OficinaTransa_2.value OficinaTransaccion_2, \
  ZDC.txt_Referencia_2.value NombreEstablecimientoReferencia_2, \
  ZDC.key_Moneda_2.value Moneda_2, \
  ZDC.key_CiudadTransa_2.value CiudadTransaccion_2, \
  ZDC.txt_NumeroCuenta_2.value NumeroCuentaTarjetaAsociada_2, \
  ZDC.num_ChequeVouch_2.value NumeroChequeVoucher_2, \
  ZDC.dat_FechaConsigna_2.value FechaConsignacion_2, \
  ZDC.num_ValorTransa_3.value ValorTransaccion_3, \
  ZDC.dat_FechaTransa_3.value FechaTransaccion_3, \
  ZDC.key_OficinaTransa_3.value OficinaTransaccion_3, \
  ZDC.txt_Referencia_3.value NombreEstablecimientoReferencia_3, \
  ZDC.key_Moneda_3.value Moneda_3, \
  ZDC.key_CiudadTransa_3.value CiudadTransaccion_3, \
  ZDC.txt_NumeroCuenta_3.value NumeroCuentaTarjetaAsociada_3, \
  ZDC.num_ChequeVouch_3.value NumeroChequeVoucher_3, \
  ZDC.dat_FechaConsigna_3.value FechaConsignacion_3, \
  ZDC.num_ValorTransa_4.value ValorTransaccion_4, \
  ZDC.dat_FechaTransa_4.value FechaTransaccion_4, \
  ZDC.key_OficinaTransa_4.value OficinaTransaccion_4, \
  ZDC.txt_Referencia_4.value NombreEstablecimientoReferencia_4, \
  ZDC.key_Moneda_4.value Moneda_4, \
  ZDC.key_CiudadTransa_4.value CiudadTransaccion_4, \
  ZDC.txt_NumeroCuenta_4.value NumeroCuentaTarjetaAsociada_4, \
  ZDC.num_ChequeVouch_4.value NumeroChequeVoucher_4, \
  ZDC.dat_FechaConsigna_4.value FechaConsignacion_4, \
  ZDC.num_ValorTransa_5.value ValorTransaccion_5, \
  ZDC.dat_FechaTransa_5.value FechaTransaccion_5, \
  ZDC.key_OficinaTransa_5.value OficinaTransaccion_5, \
  ZDC.txt_Referencia_5.value NombreEstablecimientoReferencia_5, \
  ZDC.key_Moneda_5.value Moneda_5, \
  ZDC.key_CiudadTransa_5.value CiudadTransaccion_5, \
  ZDC.txt_NumeroCuenta_5.value NumeroCuentaTarjetaAsociada_5, \
  ZDC.num_ChequeVouch_5.value NumeroChequeVoucher_5, \
  ZDC.dat_FechaConsigna_5.value FechaConsignacion_5, \
  ZDC.num_ValorTransa_6.value ValorTransaccion_6, \
  ZDC.dat_FechaTransa_6.value FechaTransaccion_6, \
  ZDC.key_OficinaTransa_6.value OficinaTransaccion_6, \
  ZDC.txt_Referencia_6.value NombreEstablecimientoReferencia_6, \
  ZDC.key_Moneda_6.value Moneda_6, \
  ZDC.key_CiudadTransa_6.value CiudadTransaccion_6, \
  ZDC.txt_NumeroCuenta_6.value NumeroCuentaTarjetaAsociada_6, \
  ZDC.num_ChequeVouch_6.value NumeroChequeVoucher_6, \
  ZDC.dat_FechaConsigna_6.value FechaConsignacion_6, \
  ZDC.num_ValorTransa_7.value ValorTransaccion_7, \
  ZDC.dat_FechaTransa_7.value FechaTransaccion_7, \
  ZDC.key_OficinaTransa_7.value OficinaTransaccion_7, \
  ZDC.txt_Referencia_7.value NombreEstablecimientoReferencia_7, \
  ZDC.key_Moneda_7.value Moneda_7, \
  ZDC.key_CiudadTransa_7.value CiudadTransaccion_7, \
  ZDC.txt_NumeroCuenta_7.value NumeroCuentaTarjetaAsociada_7, \
  ZDC.num_ChequeVouch_7.value NumeroChequeVoucher_7, \
  ZDC.dat_FechaConsigna_7.value FechaConsignacion_7, \
  ZDC.num_ValorTransa_8.value ValorTransaccion_8, \
  ZDC.dat_FechaTransa_8.value FechaTransaccion_8, \
  ZDC.key_OficinaTransa_8.value OficinaTransaccion_8, \
  ZDC.txt_Referencia_8.value NombreEstablecimientoReferencia_8, \
  ZDC.key_Moneda_8.value Moneda_8, \
  ZDC.key_CiudadTransa_8.value CiudadTransaccion_8, \
  ZDC.txt_NumeroCuenta_8.value NumeroCuentaTarjetaAsociada_8, \
  ZDC.num_ChequeVouch_8.value NumeroChequeVoucher_8, \
  ZDC.dat_FechaConsigna_8.value FechaConsignacion_8, \
  ZDC.num_ValorTransa_9.value ValorTransaccion_9, \
  ZDC.dat_FechaTransa_9.value FechaTransaccion_9, \
  ZDC.key_OficinaTransa_9.value OficinaTransaccion_9, \
  ZDC.txt_Referencia_9.value NombreEstablecimientoReferencia_9, \
  ZDC.key_Moneda_9.value Moneda_9, \
  ZDC.key_CiudadTransa_9.value CiudadTransaccion_9, \
  ZDC.txt_NumeroCuenta_9.value NumeroCuentaTarjetaAsociada_9, \
  ZDC.num_ChequeVouch_9.value NumeroChequeVoucher_9, \
  ZDC.dat_FechaConsigna_9.value FechaConsignacion_9 \
  FROM \
  Zdc_Lotus_Reclamos_old_TablaIntermedia ZDC \
  LEFT JOIN Lotus_ProcessOptimizado.Zdp_Lotus_Reclamos_Old_Solicitudes SOL ON SOL.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeAyuda MEA ON MEA.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txt_MensajeSoportes MES ON MES.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_rch_Pantallas PAN ON PAN.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_PantallaSln PANSLN ON PANSLN.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Comentarios COM ON COM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_Historia HIS ON HIS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_rch_RptaCliente CLI ON CLI.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_txt_NomOpTblM NOM ON NOM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_reclamos_old_key_VlrsOpTblM VLR ON VLR.UniversalID = ZDC.UniversalID") 