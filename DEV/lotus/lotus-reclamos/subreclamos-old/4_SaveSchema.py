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
  UniversalId, \
  dat_FchCreac.value FechaCreacion, \
  substring_index(dat_FchCreac.value, ' ', 1)fecha_solicitudes, \
  cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
  key_Estado.value Estado, \
  txt_NomCliente.value NomCliente, \
  key_Producto.value Producto, \
  key_TipoReclamo.value TipoReclamo, \
  SSREF.value UniversalID_Padre, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/subreclamos-old/', UniversalID,'.zip'" + "), '') link \
FROM \
  Zdc_Lotus_SubReclamos_old_TablaIntermedia"

panel_principal_subreclamos_old = sqlContext.sql(sql_string)
sqlContext.registerDataFrameAsTable(panel_principal_subreclamos_old,"Zdp_Lotus_SubReclamos_Old_PanelPrincipal")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_subreclamos_old = sqlContext.sql("\
SELECT UniversalId, 'Servicio al Cliente' AS vista, 1 as posicion, 'Días vencidos Totales' AS key, num_DV.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Servicio al Cliente' AS vista, 2 as posicion, 'Días vencidos Abono Temporal' AS key, num_DVSlnado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 3 as posicion, 'Autor' AS key, txt_Autor.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 4 as posicion, 'Creador' AS key, txt_Creador.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 5 as posicion, 'Fecha Creación' AS key, dat_FchCreac.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 6 as posicion, 'Número de Radicación del Subreclamo' AS key, txt_Radicado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 7 as posicion, ' ' AS key, key_Estado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, '' AS vista, 8 as posicion, 'Estado Subreclamo' AS key, key_Estado_1.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Asignación' AS vista, 9 as posicion, 'Responsable' AS key, nom_Responsable.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 10 as posicion, 'Cédula/Nit' AS key, txt_CedNit.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 11 as posicion, 'Nombre' AS key, txt_NomCliente.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 12 as posicion, 'Producto' AS key, key_Producto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 13 as posicion, 'Tipo de Reclamo' AS key, key_TipoReclamo.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 14 as posicion, 'Número de Producto' AS key, concat(cast((key_Prefijo.value) as string), ' - ',  cast((txt_NumProducto.value)as string)) AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 15 as posicion, 'Tipo de Cuenta' AS key, CASE \
    WHEN UPPER(key_TipoCuenta.value) = 'S' THEN \
      'Ahorros' \
    WHEN UPPER(key_TipoCuenta.value) = 'D' THEN \
      'Corriente' \
    ELSE \
      '' \
  END  AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 16 as posicion, 'Número de Cuenta' AS key, num_NroCuenta.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 17 as posicion, 'Valor Total en Pesos' AS key, ValorTotal AS value, cast(substr(substring_index(FechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(FechaCreacion, ' ', 1),-7,2) as int) mes FROM Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Solicitudes_Aux \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 18 as posicion, 'Valor GMF' AS key, num_VlrGMF.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 19 as posicion, 'Valor Comisiones' AS key, num_VlrComis.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 20 as posicion, 'Valor Reversión' AS key, num_VlrRever.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 21 as posicion, 'El Cliente Tiene la Razón?' AS key, key_ClienteRazon.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 22 as posicion, 'Se le Carga al Cliente?' AS key, key_CargaClte.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 23 as posicion, 'Campo a Diligenciar' AS key, CASE \
    WHEN UPPER(key_CampoDilig.value) = 'C1' THEN \
      'P y G Sucursal del Error' \
    WHEN UPPER(key_CampoDilig.value) = 'C2' THEN \
      'Cuenta de Tercero' \
    WHEN UPPER(key_CampoDilig.value) = 'N' THEN \
      'Ninguno' \
    ELSE \
      '' \
  END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 24 as posicion, 'P y G Sucursal Error' AS key, key_PGSucError.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 25 as posicion, 'P y G Area Administrativa' AS key, key_PGAreaAdmin.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 26 as posicion, 'Cuenta Tercero' AS key, num_NroCtaTercero.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 27 as posicion, 'Tipo de Cuenta del Tercero' AS key, CASE \
    WHEN UPPER(key_TipoCtaTercero.value) = 'S' THEN \
      'Ahorros' \
    WHEN UPPER(key_TipoCtaTercero.value) = 'D' THEN \
      'Corriente' \
    ELSE \
      '' \
  END AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos del subreclamo' AS vista, 28 as posicion, 'P y G tarjetas' AS key, key_PGTarjetas AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_key_PGTarjetas \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 29 as posicion, 'Tiempo Real de la Solución' AS key, num_DAbierto.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 30 as posicion, 'Fecha Real de la Solución' AS key, dat_Solucionado.value AS value, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac.value, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_SubReclamos_old_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Comentarios' AS vista, 31 as posicion, '' AS key, txt_Comentarios AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txt_Comentarios \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 32 as posicion, '' AS key, txt_Historia AS value, cast(substr(substring_index(dat_FchCreac, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dat_FchCreac, ' ', 1),-7,2) as int) mes FROM Zdc_Lotus_subreclamos_Tablaintermedia_txt_Historia")

sqlContext.registerDataFrameAsTable(panel_detalle_subreclamos_old,"zdr_uvw_lotus_SubReclamos_Detalle_Old")

# COMMAND ----------

masivos_subreclamos_old_df = sqlContext.sql(" \
SELECT \
  ZDC.UniversalID, \
  cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(ZDC.dat_FchCreac.value, ' ', 1),-7,2)as int) mes, \
  ZDC.num_DV.value DiasvencidosTotales, \
  ZDC.num_DVSlnado.value DiasvencidosAbonoTemporal, \
  ZDC.txt_Autor.value Autor, \
  ZDC.txt_Creador.value Creador, \
  ZDC.dat_FchCreac.value FechaCreacion, \
  ZDC.txt_Radicado.value NumeroRadicacionSubreclamo, \
  ZDC.key_Estado.value key_Estado, \
  ZDC.key_Estado_1.value EstadoSubreclamo, \
  ZDC.nom_Responsable.value Responsable, \
  ZDC.txt_CedNit.value CedulaNit, \
  ZDC.txt_NomCliente.value Nombre, \
  ZDC.key_Producto.value Producto, \
  ZDC.key_TipoReclamo.value TipoReclamo, \
  concat(cast((ZDC.key_Prefijo.value) as string), ' - ',cast((ZDC.txt_NumProducto.value)as string)) NumeroProducto, \
  CASE \
      WHEN UPPER(ZDC.key_TipoCuenta.value) = 'S' THEN \
        'Ahorros' \
      WHEN UPPER(ZDC.key_TipoCuenta.value) = 'D' THEN \
        'Corriente' \
      ELSE \
        '' \
    END TipoCuenta, \
  ZDC.num_NroCuenta.value NumeroCuenta, \
  AUX.ValorTotal ValorTotalPesos, \
  ZDC.num_VlrGMF.value ValorGMF, \
  ZDC.num_VlrComis.value ValorComisiones, \
  ZDC.num_VlrRever.value ValorReversion, \
  ZDC.key_ClienteRazon.value ClienteTieneRazon, \
  ZDC.key_CargaClte.value CargaCliente, \
  CASE \
      WHEN UPPER(ZDC.key_CampoDilig.value) = 'C1' THEN \
        'P y G Sucursal del Error' \
      WHEN UPPER(ZDC.key_CampoDilig.value) = 'C2' THEN \
        'Cuenta de Tercero' \
      WHEN UPPER(ZDC.key_CampoDilig.value) = 'N' THEN \
        'Ninguno' \
      ELSE \
        '' \
    END CampoDiligenciar, \
  ZDC.key_PGSucError.value PGSucursalError, \
  ZDC.key_PGAreaAdmin.value PGAreaAdministrativa, \
  ZDC.num_NroCtaTercero.value CuentaTercero, \
  CASE \
    WHEN UPPER(ZDC.key_TipoCtaTercero.value) = 'S' THEN \
      'Ahorros' \
    WHEN UPPER(ZDC.key_TipoCtaTercero.value) = 'D' THEN \
      'Corriente' \
    ELSE \
      '' \
  END TipoCuentaTercero, \
TAR.key_PGTarjetas PyGtarjetas, \
ZDC.num_DAbierto.value TiempoRealSolucion, \
ZDC.dat_Solucionado.value FechaRealSolucion, \
COM.txt_Comentarios Comentarios, \
HIS.txt_Historia Historia \
FROM \
  Zdc_Lotus_SubReclamos_old_TablaIntermedia ZDC \
  LEFT JOIN Lotus_ProcessOptimizado.Zdp_Lotus_Subreclamos_Old_Solicitudes_Aux AUX ON AUX.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_key_PGTarjetas TAR ON TAR.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_txt_Comentarios COM ON COM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_subreclamos_Tablaintermedia_txt_Historia HIS ON HIS.UniversalID = ZDC.UniversalID")