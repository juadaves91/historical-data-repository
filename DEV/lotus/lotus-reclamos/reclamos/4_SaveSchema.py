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
panel_principal_reclamos = sqlContext.sql("\
  SELECT \
  UniversalID, \
  UniversalIDPadre, \
  TituloDB, \
  Cedula, \
  Radicado, \
  FechaCreacion, \
  cast(substr(substring_index(FechaCreacion, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(FechaCreacion, ' ', 1),-7,2)as int) mes, \
  ClientetieneRazon, \
  AbonoTemporal, \
  AbonoDefinitivo, \
  Producto, \
  TipoReclamo, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/reclamos/', UniversalID,'.zip'" + "), '') link, \
  translate(concat(concat(Radicado, ' - ', \
                          AbonoTemporal, ' - ', \
                          Cedula, ' - ', \
                          ClientetieneRazon, ' - ', \
                          Producto,' - ', \
                          TipoReclamo, ' - ', \
                          AbonoDefinitivo), ' - ', \
  upper(concat(Radicado, ' - ', \
               ClientetieneRazon, ' - ', \
               Producto, ' - ', \
               AbonoTemporal, ' - ', \
               TipoReclamo, ' - ', \
               AbonoDefinitivo)), ' - ',  \
  lower(concat(Radicado, ' - ', ' - ', \
               ClientetieneRazon, ' - ', \
               Producto, ' - ', \
               AbonoTemporal, ' - ', \
               TipoReclamo, ' - ', \
               AbonoDefinitivo))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
FROM \
  Zdc_Lotus_Reclamos_TablaIntermedia")
                                          
sqlContext.registerDataFrameAsTable(panel_principal_reclamos,"Zdp_Lotus_Reclamos_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Panel Trasacciones
panel_transacciones_reclamos = sqlContext.sql("\
SELECT \
  UniversalId, \
  concat_ws('\n', numValoresTransac.value) Valor, \
  concat_ws('\n', dtFechasTransac.value) Fecha, \
  concat_ws('\n', txtOficinasTransac.value) Oficina, \
  concat_ws('\n', txtReferenciasTransac.value) Referencia, \
  concat_ws('\n', txtMonedasTransac.value) Moneda, \
  concat_ws('\n', txtCiudadesTransac.value) Ciudad, \
  concat_ws('\n', txtCuentasTransac.value) NoCuenta, \
  concat_ws('\n', txtChequesTransac.value) NoCheque, \
  concat_ws('\n', dtConsignacionesTransac.value) FechaConsignacion, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes \
FROM \
  Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux")

sqlContext.registerDataFrameAsTable(panel_transacciones_reclamos,"Zdp_Lotus_Reclamos_Cat_Transaccion")

# COMMAND ----------

panel_historia_reclamos_aux = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  posicion, \
  FechaEntrada, \
  Estado, \
  TotalDias, \
  DiasVencidos, \
  DiasContabilizados \
FROM \
  Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux \
  LATERAL VIEW posexplode(dtFechasTrans.value) FechaEntrada AS posicion, FechaEntrada \
  LATERAL VIEW posexplode(txtEstadoTrans.value) Estado  AS posicion2, Estado \
  LATERAL VIEW posexplode(numDiasTrans.value) TotalDias AS posicion3, TotalDias \
  LATERAL VIEW posexplode(numVencidosTrans.value) DiasVencidos AS posicion4, DiasVencidos \
  LATERAL VIEW posexplode(numContabilizaTrans.value) DiasContabilizados AS posicion5, DiasContabilizados \
WHERE \
  posicion = posicion2 \
  AND posicion = posicion3 \
  AND posicion = posicion4 \
  AND posicion = posicion5")

sqlContext.registerDataFrameAsTable(panel_historia_reclamos_aux,"Zdp_Lotus_Reclamos_Cat_Historia_Aux")

# COMMAND ----------

# DBTITLE 1,Panel Historia
panel_historia_reclamos = sqlContext.sql("\
SELECT \
 UniversalID, \
 anio, \
 mes, \
 FechaEntrada, \
 Estado, \
 TotalDias, \
 DiasVencidos, \
 DiasContabilizados \
FROM \
  Zdp_Lotus_Reclamos_Cat_Historia_Aux \
UNION ALL \
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4)as int) anio, \
  cast(substr(substring_index(dtFechaCreacion.value, ' ', 1),-7,2)as int) mes, \
  '' AS FechaEntrada, \
  'Totales' AS Estado, \
  numTotDias.value AS TotalDias, \
  numTotVencidos.value AS DiasVencidos, \
  numTotContabiliza.value AS DiasContabilizados \
FROM \
Lotus_ProcessOptimizado.Zdc_Lotus_Reclamos_TablaIntermedia_Aux")

sqlContext.registerDataFrameAsTable(panel_historia_reclamos,"Zdp_Lotus_Reclamos_Cat_Historia")

# COMMAND ----------

# DBTITLE 1,Panel Detalle
panel_detalle_reclamos = sqlContext.sql("\
SELECT UniversalId, 'Radicación' AS vista, 0 as posicion, 'Radicado' AS key, Radicado AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 1 as posicion, 'Estado' AS key, Estado AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 2 as posicion, 'Autor' AS key, txtAutor AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 3 as posicion, 'Fecha de creación' AS key, FechaCreacion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 4 as posicion, 'Sucursal Captura' AS key, TXTSUCCAPTURA AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 5 as posicion, 'Reportado Por' AS key, cbReportadoPor AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 6 as posicion, 'Origen del Reporte' AS key, cbOrigenReporte AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 7 as posicion, 'Responsable' AS key, cbResponsable AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 8 as posicion, 'Abono Definitivo' AS key, AbonoDefinitivo AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 9 as posicion, 'Responsable del Error' AS key, cbResponsableError AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 10 as posicion, 'Nombre del Responsable del Error' AS key, txtNomRespError AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Radicación' AS vista, 11 as posicion, 'Cliente tiene la Razon' AS key, ClientetieneRazon AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 12 as posicion, 'Número de identificación' AS key, Cedula AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 13 as posicion, 'Nombre' AS key, txtNomCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 14 as posicion, 'Segmento' AS key, txtSegmento AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 15 as posicion, 'Subsegmento' AS key, txtSubsegmento AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 16 as posicion, 'Gerente de cuenta' AS key, txtGerenteCuenta AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 17 as posicion, 'Funcionario de la empresa' AS key, txtFuncionarioEmpresa AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 18 as posicion, 'Dirección de correspondencia' AS key, txtDireccionCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 19 as posicion, 'Ciudad de correspondencia' AS key, cbCiudadCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 20 as posicion, 'Código de ciudad' AS key, txtCodCiudadCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 21 as posicion, 'Departamento de correspondencia' AS key, txtDeptoCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 22 as posicion, 'Teléfono(s)' AS key, txtTelefonosCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 23 as posicion, 'Celular' AS key, txtCelular AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 24 as posicion, 'Autoriza envío de información via Email' AS key, rdAutorizaEmail AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 25 as posicion, 'Email' AS key, txtEmailCliente AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Cliente' AS vista, 26 as posicion, 'Fax' AS key, txtFax AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 27 as posicion, 'Tiempo de respuesta' AS key, numTiempoSlnRpta AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 28 as posicion, 'Fecha de respuesta (dd/mm/aaaa)' AS key, dtFechaSlnRpta AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 29 as posicion, 'Tiempo real de solución' AS key, numTiempoSolucionReal AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 30 as posicion, 'Fecha real de solución (dd/mm/aaaa)' AS key, dtFechaSolucionReal AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 31 as posicion, 'Tiempo estimado de solución' AS key, numTiempoSolucion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 32 as posicion, 'Producto' AS key, Producto AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 33 as posicion, 'Clasificación' AS key, txtClasificacion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 34 as posicion, 'Tipo de reclamo' AS key, TipoReclamo AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 35 as posicion, 'Número de producto' AS key, txtNumProducto AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 36 as posicion, 'Código de compra' AS key, txtCodigoCompra AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 37 as posicion, 'Código de producto' AS key, txtCodigoProducto AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 38 as posicion, 'Producto afectado' AS key, cbProductoAfectado AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 39 as posicion, 'Número de tarjeta débito' AS key, txtNumTarjetaDebito AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 40 as posicion, 'Número de póliza' AS key, txtNumPoliza AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 41 as posicion, 'Número de encargo fiduciario' AS key, txtNumEncargoFiducia AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 42 as posicion, 'Sucursal de radicación del producto' AS key, cbSucursalRadicacion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 43 as posicion, 'Tipo de póliza ' AS key, cbTipoPoliza AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 44 as posicion, 'Número de tarjeta / cuenta que se debita' AS key, txtNumTarjetaCtaDebita AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 45 as posicion, 'Ciudad de transacción / suceso' AS key, cbCiudadTransaccion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 46 as posicion, 'Fecha de transacción' AS key, dtFechaTransaccion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 47 as posicion, 'Sucursal / área de queja, felicitación o sugerencia' AS key, cbSucursalVarios AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 48 as posicion, 'Código único de establecimiento' AS key, txtCodigoEstablecimiento AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 49 as posicion, 'Valor total en pesos' AS key, numValorPesosC AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 50 as posicion, 'Valor total en dólares' AS key, numValorDolaresC AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 51 as posicion, 'Comentario' AS key, txtComentario AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtComentario \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 52 as posicion, 'Mensaje de ayuda' AS key, txtMensajeAyuda AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtMensajeAyuda \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 53 as posicion, 'Observaciones (RappCollins)' AS key, txtObservaciones AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtObservaciones \
UNION ALL \
SELECT UniversalId, 'Reclamo' AS vista, 54 as posicion, 'Soportes requeridos' AS key, txtSoportesRequeridos AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtSoportesRequeridos \
UNION ALL \
SELECT UniversalId, 'Convenios' AS vista, 55 as posicion, 'Código del convenio	' AS key, txtCodConvenio AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Convenios' AS vista, 56 as posicion, 'Nombre de la empresa' AS key, txtNomEmpresa AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Convenios' AS vista, 57 as posicion, 'Nit de la empresa' AS key, txtNitEmpresa AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Convenios' AS vista, 58 as posicion, 'Código del canal' AS key, txtCodCanal AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Convenios' AS vista, 59 as posicion, 'Nombre del canal' AS key, txtNomCanal AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes' AS vista, 60 as posicion, 'Soportes recibidos del cliente' AS key, txtSoportesRecibidos AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes' AS vista, 61 as posicion, 'Cuáles no se han recibido' AS key, txtSoportesNoRecibidos AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Soportes' AS vista, 62 as posicion, 'Se recibieron todos los soportes?' AS key, rdTodosSoportes AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Pantallas' AS vista, 63 as posicion, 'Pantallas que documentan el reclamo - Anexos' AS key, rtPantallasReclamo AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Pantallas' AS vista, 64 as posicion, 'Pantallas que certifican la solución - Anexos' AS key, rtPantallasSolucion AS value, anio, mes FROM Zdc_Lotus_Reclamos_TablaIntermedia \
UNION ALL \
SELECT UniversalId, 'Pantallas' AS vista, 65 as posicion, 'Pantallas que documentan el reclamo - Texto	' AS key, txtPantallasReclamo AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasReclamo \
UNION ALL \
SELECT UniversalId, 'Pantallas' AS vista, 66 as posicion, 'Pantallas que certifican la solución - Texto' AS key, txtPantallasSolucion AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasSolucion \
UNION ALL \
SELECT UniversalId, 'Respuesta' AS vista, 67 as posicion, 'Respuesta (Respuesta)' AS key, txtRespuesta AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtRespuesta \
UNION ALL \
SELECT UniversalId, 'Historia' AS vista, 68 as posicion, 'Comentarios y eventos' AS key, txtHistoria AS value, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-4,4) as int) anio, cast(substr(substring_index(dtFechaCreacion, ' ', 1),-7,2)as int) mes FROM Zdc_Lotus_Reclamos_Tablaintermedia_txtHistoria")

sqlContext.registerDataFrameAsTable(panel_detalle_reclamos,"Zdp_Lotus_Reclamos_Panel_Detalle")

# COMMAND ----------

masivos_reclamos_df = sqlContext.sql("\
  SELECT\
    ZDC.UniversalID, \
    ZDC.TituloDB, \
    ZDC.Radicado, \
    ZDC.FechaCreacion, \
    cast(substr(substring_index(ZDC.FechaCreacion, ' ', 1),-4,4)as int) anio, \
    cast(substr(substring_index(ZDC.FechaCreacion, ' ', 1),-7,2)as int) mes, \
    ZDC.ClientetieneRazon, \
    ZDC.AbonoTemporal, \
    ZDC.AbonoDefinitivo, \
    ZDC.Producto, \
    ZDC.TipoReclamo, \
    ZDC.txtAutor Autor, \
    ZDC.Estado, \
    ZDC.TXTSUCCAPTURA SucursalCaptura, \
    ZDC.cbReportadoPor ReportadoPor, \
    ZDC.cbOrigenReporte OrigenReporte, \
    ZDC.txtResponsable Responsable, \
    ZDC.cbResponsableError ResponsableError, \
    ZDC.txtNomRespError NombreResponsableError, \
    ZDC.Cedula, \
    ZDC.txtNomCliente Nombre, \
    ZDC.txtSegmento Segmento, \
    ZDC.txtGerenteCuenta GerenteCuenta, \
    ZDC.txtFuncionarioEmpresa FuncionarioEmpresa, \
    ZDC.txtDireccionCliente DireccionCorrespondencia, \
    ZDC.cbCiudadCliente CiudadCorrespondencia, \
    ZDC.txtCodCiudadCliente CodigoCiudad, \
    ZDC.txtDeptoCliente DepartamentoCorrespondencia, \
    ZDC.txtTelefonosCliente Telefono, \
    ZDC.txtCelular Celular, \
    ZDC.rdAutorizaEmail AutorizaEmail, \
    ZDC.txtEmailCliente Email, \
    ZDC.txtFax Fax, \
    ZDC.numTiempoSlnRpta TiempoRespuesta, \
    ZDC.dtFechaSlnRpta FechaRespuesta, \
    ZDC.numTiempoSolucionReal TiempoRealSolucion, \
    ZDC.dtFechaSolucionReal Fechasolucion, \
    ZDC.numTiempoSolucion TiempoEstimadoSolucion, \
    ZDC.txtClasificacion Clasificacion, \
    ZDC.txtNumProducto NumeroProducto, \
    ZDC.txtCodigoCompra CodigoCcompra, \
    ZDC.txtCodigoProducto CodigoProducto, \
    ZDC.cbProductoAfectado ProductoAfectado, \
    ZDC.txtNumTarjetaDebito NumeroTtarjetaDebito, \
    ZDC.txtNumPoliza NumeroPpoliza, \
    ZDC.txtNumEncargoFiducia NumeroEencargoFiduciario, \
    ZDC.cbSucursalRadicacion SucursalRadicacionPproducto, \
    ZDC.cbTipoPoliza TipoPoliza, \
    ZDC.txtNumTarjetaCtaDebita NumeroTarjetaCuentaDebita, \
    ZDC.cbCiudadTransaccion CiudadTransaccionSuceso, \
    ZDC.dtFechaTransaccion FechaTransaccion, \
    ZDC.cbSucursalVarios SucursalQuejaFelicitacionSsugerencia, \
    ZDC.txtCodigoEstablecimiento CodigoUnicoEstablecimiento, \
    ZDC.numValorPesos ValorTotalPesos, \
    ZDC.numValorDolares ValorTotalDolares, \
    COM.txtComentario Comentario, \
    AYU.txtMensajeAyuda MensajeAyuda, \
    OBS.txtObservaciones Observaciones, \
    REQ.txtSoportesRequeridos SoportesRequeridos, \
    ZDC.txtCodConvenio CodigoConvenio, \
    ZDC.txtNomEmpresa NombreEmpresa, \
    ZDC.txtNitEmpresa NitEmpresa, \
    ZDC.txtCodCanal CodigoCanal, \
    ZDC.txtNomCanal NombreCanal, \
    ZDC.txtSoportesRecibidos SoportesRecibidosCliente, \
    ZDC.txtSoportesNoRecibidos CualesNoRecibido, \
    ZDC.rdTodosSoportes SeRecibieronSoportes, \
    ZDC.rtPantallasReclamo PantallasDocumentanReclamoAnexos, \
    ZDC.rtPantallasSolucion PantallasCertificanSolucionAnexos, \
    PAN.txtPantallasReclamo PantallasDocumentanReclamoTexto, \
    SOL.txtPantallasSolucion PantallasCertificanSolucionTexto, \
    RES.txtRespuesta Respuesta, \
    HIS.txtHistoria ComentariosEventos, \
    TRANS.Valor, \
    TRANS.Fecha, \
    TRANS.Oficina, \
    TRANS.Referencia, \
    TRANS.Moneda, \
    TRANS.Ciudad, \
    TRANS.NoCuenta, \
    TRANS.NoCheque, \
    TRANS.FechaConsignacion, \
    HIST.FechaEntrada, \
    HIST.Estado His_estado, \
    HIST.TotalDias, \
    HIST.DiasVencidos, \
    HIST.DiasContabilizados \
FROM \
  Zdc_Lotus_Reclamos_TablaIntermedia ZDC \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtComentario COM ON COM.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtMensajeAyuda AYU ON AYU.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtObservaciones OBS ON OBS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtSoportesRequeridos REQ ON REQ.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasReclamo PAN ON PAN.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtPantallasSolucion SOL ON SOL.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtRespuesta RES ON RES.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdc_Lotus_Reclamos_Tablaintermedia_txtHistoria HIS ON HIS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdp_Lotus_Reclamos_Cat_Transaccion TRANS ON TRANS.UniversalID = ZDC.UniversalID \
  LEFT JOIN Zdp_Lotus_Reclamos_Cat_Historia HIST ON HIST.UniversalID = ZDC.UniversalID")