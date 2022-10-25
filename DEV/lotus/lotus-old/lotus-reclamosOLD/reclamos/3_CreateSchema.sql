-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de tiempo
CREATE TABLE IF NOT EXISTS process.zdp_lotus_reclamos_cat_tiempo AS
WITH fechas AS (
    SELECT date_add("2000-01-01", a.pos) as fecha
    FROM (SELECT posexplode(split(repeat(",", 7300), ","))) a
  ),
dates_expanded AS (
  SELECT
    fecha,
    year(fecha) as anio,
    month(fecha) as num_mes,
    day(fecha) as dia,
    date_format(fecha, 'u') as dias_semana 
  FROM fechas
  )
SELECT fecha,
    anio,
    cast(month(fecha)/4 + 1 AS BIGINT) as trimestre,
    num_mes,
    date_format(fecha, 'W') as semana_mes,
    Case num_mes 
      when 1 then 'Enero'
      when 2 then 'Febrero'
      when 3 then 'Marzo'
      when 4 then 'Abril'
      when 5 then 'Mayo'
      
      when 6 then 'Junio'
      when 7 then 'Julio'
      when 8 then 'Agosto'
      when 9 then 'Septiembre'
      when 10 then 'Octubre'
      when 11 then 'Noviembre'
      when 11 then 'Diciembre'
    End nombre_mes,
    Case num_mes 
      when 1 then '01. Ene'
      when 2 then '02. Feb'
      when 3 then '03. Mar'
      when 4 then '04. Abr'
      when 5 then '05. May'
      when 6 then '06. Jun'
      when 7 then '07. Jul'
      when 8 then '08. Agos'
      when 9 then '09. Sep'
      when 10 then '10. Oct'
      when 11 then '11. Nov'
      when 11 then '12. Dic'
    End nombre_mes_abre,
    dias_semana,
    Case dias_semana 
      when 1 then 'Lunes'
      when 2 then 'Martes'
      when 3 then 'Miércoles'
      when 4 then 'Jueves'
      when 5 then 'Viernes'
      when 6 then 'Sabado'
      when 7 then 'Domingo'
    end nombre_dia_semana,
    Case dias_semana 
      when 1 then 'Lun'
      when 2 then 'Mar'
      when 3 then 'Mie'
      when 4 then 'Jue'
      when 5 then 'Vie'
      when 6 then 'Sab'
      when 7 then 'Dom'
    end nombre_dia_semana_abre,
    date_format(fecha, 'D') as dia_anio
FROM dates_expanded;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia
( 
  UniversalID string,
  attachments array<string>,
  SSConflictAction map<string,string>,
  SSRevisions map<string,array<string>>,
  SSUpdatedBy map<string,array<string>>,
  SSWebFlags map<string,string>,
  txtNumIdCliente map<string,string>,
  txtIdTipoProducto map<string,string>,
  txtIdTipoReclamo map<string,string>,
  Form map<string,string>,
  txtTipoProducto map<string,string>,
  txtTipoReclamo map<string,string>,
  txtEtiquetaMasDatos map<string,string>,
  txtIdsEdTransacciones map<string,array<string>>,
  txtSubprocesoDueno map<string,string>,
  txtMensajeAyuda map<string,array<string>>,
  txtNecesitaSoporte map<string,string>,
  txtSoportesRequeridos map<string,array<string>>,
  dtFechaSolucion map<string,string>,
  cbResponsable map<string,string>,
  cbClasificacion map<string,string>,
  txtNumProducto map<string,string>,
  txtCodigoCompra map<string,string>,
  txtCodigoProducto map<string,string>,
  cbProductoAfectado map<string,string>,
  txtNumTarjetaDebito map<string,string>,
  txtNumPoliza map<string,string>,
  txtNumEncargoFiducia map<string,string>,
  cbSucursalRadicacion map<string,string>,
  cbTipoPoliza map<string,string>,
  txtNumTarjetaCtaDebita map<string,string>,
  cbCiudadTransaccion map<string,string>,
  dtFechaTransaccion map<string,string>,
  cbSucursalVarios map<string,string>,
  txtCodigoEstablecimiento map<string,string>,
  numValorPesos map<string,string>,
  numValorPesosC map<string,string>,
  numValorDolares map<string,string>,
  numValorDolaresC map<string,string>,
  cbOpcionesMasDatos map<string,string>,
  txtObservaciones map<string,array<string>>,
  numTransacciones map<string,string>,
  numValoresTransac map<string,array<string>>,
  dtFechasTransac map<string,array<string>>,
  txtOficinasTransac map<string,array<string>>,
  txtReferenciasTransac map<string,array<string>>,
  txtMonedasTransac map<string,array<string>>,
  txtCiudadesTransac map<string,array<string>>,
  txtCuentasTransac map<string,array<string>>,
  txtChequesTransac map<string,array<string>>,
  dtConsignacionesTransac map<string,array<string>>,
  txtComentario map<string,array<string>>,
  numLongProducto map<string,string>,
  txtIdsEdSubreclamo map<string,array<string>>,
  txtIdsEdReclamo map<string,array<string>>,
  txtCargaDatosReclamo map<string,string>,
  txtAutor map<string,string>,
  dtFechaCreacion map<string,string>,
  txtRadicado map<string,string>,
  cbSucCaptura map<string,string>,
  TXTSUCCAPTURA map<string,string>,
  cbReportadoPor map<string,string>,
  cbOrigenReporte map<string,string>,
  numAbonoTemporal map<string,string>,
  numAbonoDefinitivo map<string,string>,
  cbResponsableError map<string,string>,
  txtNomRespError map<string,string>,
  rdClienteRazon map<string,string>,
  cbMedioRespuestaCierre map<string,string>,
  txtUsuarioQueCierra map<string,string>,
  cbCausaNoAbono map<string,string>,
  txtCerrado map<string,string>,
  txtEstado map<string,string>,
  TXTIDESTADO map<string,string>,
  txtNomCliente map<string,string>,
  txtSegmento map<string,string>,
  txtSubsegmento map<string,string>,
  txtGerenteCuenta map<string,string>,
  txtFuncionarioEmpresa map<string,string>,
  txtDireccionCliente map<string,string>,
  cbCiudadCliente map<string,string>,
  txtDeptoCliente map<string,string>,
  txtTelefonosCliente map<string,string>,
  txtEmailCliente map<string,string>,
  txtFax map<string,string>,
  txtPrefijo map<string,string>,
  numTiempoSolucion map<string,string>,
  numTiempoSolucionReal map<string,string>,
  dtFechaSolucionReal map<string,string>,
  txtSoportesRecibidos map<string,string>,
  txtSoportesNoRecibidos map<string,string>,
  rdTodosSoportes map<string,string>,
  txtHistoria map<string,array<string>>,
  dtFechasTrans map<string,array<string>>,
  numDiasTrans map<string,array<string>>,
  numVencidosTrans map<string,array<string>>,
  numContabilizaTrans map<string,array<string>>,
  numTotDias map<string,string>,
  numTotVencidos map<string,string>,
  numTotContabiliza map<string,string>,
  txtIdEstadoTrans map<string,array<string>>,
  txtEstadoTrans map<string,array<string>>,
  txtGuardo map<string,string>,
  rtPantallasReclamo map<string,string>,
  rtPantallasSolucion map<string,string>,
  txtRespuesta map<string,array<string>>,
  rtAnexos map<string,string>,
  cbRespSegRecl map<string,string>,
  txtRespSegRecl map<string,string>,
  txtTelefonoResp map<string,string>,
  txtTelefonoRespC map<string,string>,
  txtCodCiudadCliente map<string,string>,
  txtCelular map<string,string>,
  rdAutorizaEmail map<string,string>,
  txtAutorizaEmail map<string,string>,
  numTiempoSlnRpta map<string,string>,
  dtFechaSlnRpta map<string,string>,
  txtCodConvenio map<string,string>,
  txtNomEmpresa map<string,string>,
  txtNitEmpresa map<string,string>,
  txtCodCanal map<string,string>,
  txtNomCanal map<string,string>,
  numValorTransac map<string,string>,
  dtFechaTransac map<string,string>,
  cbOficinaTransac map<string,string>,
  txtReferenciaTransac map<string,string>,
  cbMonedaTransac map<string,string>,
  cbCiudadTransac map<string,string>,
  txtNumCuentaTransac map<string,string>,
  txtNumChequeTransac map<string,string>,
  dtFechaConsigTransac map<string,string>,
  numFilaTransac map<string,array<string>>,
  txtPantallasReclamo map<string,array<string>>,
  txtPantallasSolucion map<string,array<string>>,
  txtIdsEdGeneral map<string,array<string>>,
  txtCodSegmento map<string,string>,
  txtCodGerencia map<string,string>,
  txtCausaRA map<string,string>,
  txtCambRespManual map<string,string>,
  txtIdSubreclamo map<string,string>,
  numTransaccionEditada map<string,string>,
  numVecesEstadoPendiente map<string,string>,
  txtResponsableOrig map<string,string>,
  txtIdsCartasRespuesta map<string,array<string>>,
  numCartasRespuesta map<string,string>,
  txtUsuarioActual map<string,string>,
  txtAplicaConvenio map<string,string>,
  dtmHoy map<string,string>,
  txtCargaDatosCliente map<string,string>,
  txtIdTipoProductoAnt map<string,string>,
  txtCodSubSegmento map<string,string>,
  txtCrearSubreclamo map<string,string>,
  txtCausaMR map<string,string>,
  txtIdEstadoSubreclamo map<string,string>,
  txtRappCollins map<string,string>,
  txtRequiereSoporte map<string,string>,
  txtNotificarVencidosPend map<string,string>,
  dtmAsignacionResp map<string,string>,
  txtBancaEspecial map<string,string>,
  txtCambioConcepto map<string,string>,
  txtSucursalesBanca map<string,string>,
  txtNotificarRecepSoportes map<string,string>,
  txtIdClasificacionAnt map<string,string>,
  txtClasificacion map<string,string>,
  txtResponsable map<string,string>,
  SSReturn map<string,string>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/reclamos/raw';

-- COMMAND ----------

-- DBTITLE 1,FALLO: lotus_reclamos_20190430_026.json línea: 37132 columna repetida: "Form"
alter table Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia set serdeproperties ("ignore.malformed.json" = "true")

-- COMMAND ----------

-- DBTITLE 1,Tablas Reclamos (Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes AS
SELECT
  UniversalID AS UniversalID,
  substring_index(dtFechaCreacion.value, ' ', 1) AS fecha_solicitudes,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
  txtNumIdCliente.value AS NumeroIdentificacion,
  txtTipoProducto.value AS Producto,
  txtTipoReclamo.value AS TipoReclamo,
  txtEtiquetaMasDatos.value AS EtiquetasAdicionales,
  concat_ws('\n', txtMensajeAyuda.value) AS MensajeAyuda,
  concat_ws('\n', txtSoportesRequeridos.value) AS SoportesRequeridos,
  cbClasificacion.value AS ClasificacionLista,
  txtNumProducto.value AS NumeroProducto,
  txtCodigoCompra.value AS CodigoCompra,
  txtCodigoProducto.value AS CodigoProducto,
  cbProductoAfectado.value AS ProductoAfectado,
  txtNumTarjetaDebito.value AS NumeroTarjetaDebito,
  txtNumPoliza.value AS NumeroPoliza,
  txtNumEncargoFiducia.value AS NumeroEncargoFiduciario,
  cbSucursalRadicacion.value AS SucursalRadicacionProducto,
  cbTipoPoliza.value AS TipoPoliza,
  txtNumTarjetaCtaDebita.value AS NumeroTarjetaCuentaDebita,
  cbCiudadTransaccion.value AS CiudadTransaccion,
  dtFechaTransaccion.value AS FechaTransaccionReclamo,
  cbSucursalVarios.value AS SucursalAreaQueja,
  txtCodigoEstablecimiento.value AS CodigoUnicoEstablecimiento,
  numValorPesos.value AS ValortotalPesos,
  numValorDolares.value AS ValortotalDolares,
  cbOpcionesMasDatos.value AS ValoresEtiquetasAdicionales,
  concat_ws('\n', txtObservaciones.value) AS Observaciones,
  concat_ws('\n', txtComentario.value) AS Comentario,
  txtAutor.value AS Autor,
  dtFechaCreacion.value AS FechaCreacion,
  txtRadicado.value AS Radicado,
  TXTSUCCAPTURA.value AS SucursalCaptura,
  cbReportadoPor.value AS ReportadoPor,
  numAbonoDefinitivo.value AS AbonoDefinitivo,
  txtNomRespError.value AS NombreResponsableError,
  cbResponsableError.value AS ResponsableError,
  rdClienteRazon.value AS ClientetieneRazon,
  cbMedioRespuestaCierre.value AS MedioRespuestaCliente,
  txtUsuarioQueCierra.value AS UsuarioCierra,
  cbCausaNoAbono.value AS CausadAbono,
  txtEstado.value AS Estado,
  txtNomCliente.value AS Nombre,
  txtSegmento.value AS Segmento,
  txtSubsegmento.value AS Subsegmento,
  txtGerenteCuenta.value AS GerenteCuenta,
  txtFuncionarioEmpresa.value AS FuncionarioEmpresa,
  txtDireccionCliente.value AS DireccionCorrespondencia,
  cbCiudadCliente.value AS CiudadCorrespondencia,
  txtDeptoCliente.value AS DepartamentoCorrespondencia,
  txtTelefonosCliente.value AS Telefono,
  txtEmailCliente.value AS Email,
  txtFax.value AS Fax,
  cbOrigenReporte.value AS OrigenReporte,
  txtPrefijo.value AS PrefijoNumeroProducto,
  numTiempoSolucion.value AS TiempoEstimadoSolucion,
  numTiempoSolucionReal.value AS TiempoRealSolucion,
  dtFechaSolucionReal.value AS FecharealSolucion,
  txtSoportesRecibidos.value AS SoportesRecibidosCliente,
  txtSoportesNoRecibidos.value AS CualesnoseHanRecibido,
  rdTodosSoportes.value AS RecibieronSoportes,
  concat_ws('\n', txtHistoria.value) AS ComentariosEventos,
  numTotDias.value AS TotalesTotalDias,
  numTotVencidos.value AS TotalesDiasVencidos,
  numTotContabiliza.value AS TotalesDiasContabilizados,
  rtPantallasReclamo.value AS PantallasDocumentanReclamoAnexos,
  rtPantallasSolucion.value AS PantallasCertificanSolucionAnexos,
  concat_ws('\n', txtRespuesta.value) AS Respuesta,
  rtAnexos.value AS Anexos,
  cbRespSegRecl.value AS ResponsaReclamoLista,
  txtRespSegRecl.value AS ResponsableReclamo,
  txtTelefonoResp.value AS TelefonoResponsable_1,
  txtTelefonoRespC.value AS TelefonoResponsable_2,
  txtCodCiudadCliente.value AS CodigoCiudad,
  txtCelular.value AS Celular,
  rdAutorizaEmail.value AS AutorizaEmail,
  numTiempoSlnRpta.value AS TiempoRespuesta,
  dtFechaSlnRpta.value AS FechaRespuesta,
  txtCodConvenio.value AS CodigoConvenio,
  txtNomEmpresa.value AS NombreEmpresa,
  txtNitEmpresa.value AS NitEmpresa,
  txtCodCanal.value AS CodigoCanal,
  txtNomCanal.value AS NombreCanal,
  numValorTransac.value AS ValorTransaccion,
  dtFechaTransac.value AS FechaTransaccionTransacciones,
  cbOficinaTransac.value AS OficinaTransaccion,
  txtReferenciaTransac.value AS NombreEstablecimiento,
  cbMonedaTransac.value AS Moneda,
  cbCiudadTransac.value AS CiudadConsignacion,
  txtNumCuentaTransac.value AS NumeroCuenta,
  txtNumChequeTransac.value AS NumeroCheque,
  dtFechaConsigTransac.value AS FechaConsignacion,
  concat_ws('\n', txtPantallasReclamo.value) AS PantallasDocumentanreclamoTexto,
  concat_ws('\n', txtPantallasSolucion.value) AS PantallasCertificanSolucionTexto,
  txtClasificacion.value AS Clasificacion,
  txtResponsable.value AS Responsable,
  numAbonoTemporal.value AS AbonoTemporal,
  txtIdSubreclamo.value AS IdSubreclamo,
  concat(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value), ' - ', upper(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value)), ' - ', lower(concat(txtRadicado.value, ' - ', txtNumIdCliente.value, ' - ', rdClienteRazon.value, ' - ', numAbonoTemporal.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value, ' - ', numAbonoDefinitivo.value))) AS busqueda
FROM 
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos (No visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_SolicitudesOcultas;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_SolicitudesOcultas AS
SELECT 
  UniversalID AS UniversalID,
  SSConflictAction.value AS SSConflictAction,
  concat_ws('\n', SSRevisions.value) AS SSRevisions,
  concat_ws('\n', SSUpdatedBy.value) AS SSUpdatedBy,
  SSWebFlags.value AS SSWebFlags,
  txtIdTipoProducto.value AS txtIdTipoProducto,
  txtIdTipoReclamo.value AS txtIdTipoReclamo,
  Form.value AS Form,
  concat_ws('\n', txtIdsEdTransacciones.value) AS txtIdsEdTransacciones,
  txtSubprocesoDueno.value AS txtSubprocesoDueno,
  txtNecesitaSoporte.value AS txtNecesitaSoporte,
  dtFechaSolucion.value AS dtFechaSolucion,
  numTransacciones.value AS numTransacciones,
  numLongProducto.value AS numLongProducto,
  concat_ws('\n', txtIdsEdSubreclamo.value) AS txtIdsEdSubreclamo,
  concat_ws('\n', txtIdsEdReclamo.value) AS txtIdsEdReclamo,
  txtCargaDatosReclamo.value AS txtCargaDatosReclamo,
  txtCerrado.value AS txtCerrado,
  TXTIDESTADO.value AS TXTIDESTADO,
  concat_ws('\n', txtIdEstadoTrans.value) AS txtIdEstadoTrans,
  txtGuardo.value AS txtGuardo,
  concat_ws('\n', txtIdsEdGeneral.value) AS txtIdsEdGeneral,
  txtCodSegmento.value AS txtCodSegmento,
  txtCodGerencia.value AS txtCodGerencia,
  txtCausaRA.value AS txtCausaRA,
  txtCambRespManual.value AS txtCambRespManual,
  numTransaccionEditada.value AS numTransaccionEditada,
  numVecesEstadoPendiente.value AS numVecesEstadoPendiente,
  txtResponsableOrig.value AS txtResponsableOrig,
  concat_ws('\n', txtIdsCartasRespuesta.value) AS txtIdsCartasRespuesta,
  numCartasRespuesta.value AS numCartasRespuesta,
  txtUsuarioActual.value AS txtUsuarioActual,
  txtAplicaConvenio.value AS txtAplicaConvenio,
  dtmHoy.value AS dtmHoy,
  txtCargaDatosCliente.value AS txtCargaDatosCliente,
  txtIdTipoProductoAnt.value AS txtIdTipoProductoAnt,
  txtCodSubSegmento.value AS txtCodSubSegmento,
  txtCrearSubreclamo.value AS txtCrearSubreclamo,
  cbSucCaptura.value AS cbSucCaptura,
  cbReportadoPor.value AS cbReportadoPor,
  numValorDolaresC.value AS numValorDolaresC,
  cbResponsable.value AS cbResponsable,
  numAbonoTemporal.value AS numAbonoTemporal,
  numValorPesosC.value AS numValorPesosC,
  txtAutorizaEmail.value AS txtAutorizaEmail,
  txtCausaMR.value AS txtCausaMR,
  txtIdEstadoSubreclamo.value AS txtIdEstadoSubreclamo,
  txtRappCollins.value AS txtRappCollins,
  txtRequiereSoporte.value AS txtRequiereSoporte,
  txtNotificarVencidosPend.value AS txtNotificarVencidosPend,
  dtmAsignacionResp.value AS dtmAsignacionResp,
  txtBancaEspecial.value AS txtBancaEspecial,
  txtCambioConcepto.value AS txtCambioConcepto,
  txtSucursalesBanca.value AS txtSucursalesBanca,
  txtNotificarRecepSoportes.value AS txtNotificarRecepSoportes,
  txtIdClasificacionAnt.value AS txtIdClasificacionAnt,
  SSReturn.value AS SSReturn
FROM 
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Catalogo Historia
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_CatHistoria;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_CatHistoria AS
SELECT
  UniversalId,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
  posicion,
  FechaEntrada,
  Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados
FROM
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia
  LATERAL VIEW posexplode(dtFechasTrans.value) FechaEntrada AS posicion, FechaEntrada
  LATERAL VIEW posexplode(txtEstadoTrans.value) Estado  AS posicion2, Estado
  LATERAL VIEW posexplode(numDiasTrans.value) TotalDias AS posicion3, TotalDias
  LATERAL VIEW posexplode(numVencidosTrans.value) DiasVencidos AS posicion4, DiasVencidos
  LATERAL VIEW posexplode(numContabilizaTrans.value) DiasContabilizados AS posicion5, DiasContabilizados
WHERE
  posicion = posicion2
  AND posicion = posicion3
  AND posicion = posicion4
  AND posicion = posicion5; 

-- COMMAND ----------

-- DBTITLE 1,Catalogo Transacciones
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_CatTransacciones; 
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_CatTransacciones AS
SELECT
  UniversalId,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
  Posicion,
  Valor,
  Fecha,
  Oficina,
  Referencia,
  Moneda,
  Ciudad,
  NoCuenta,
  NoCheque,
  FechaConsignacion
FROM
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia
  LATERAL VIEW posexplode(numValoresTransac.value) Valor  AS Posicion, Valor
  LATERAL VIEW posexplode(dtFechasTransac.value) Fecha AS Posicion1, Fecha
  LATERAL VIEW posexplode(txtOficinasTransac.value) Oficina AS Posicion2, Oficina
  LATERAL VIEW posexplode(txtReferenciasTransac.value) Referencia AS Posicion3, Referencia
  LATERAL VIEW posexplode(txtMonedasTransac.value) Moneda AS Posicion4, Moneda
  LATERAL VIEW posexplode(txtCiudadesTransac.value) Ciudad AS Posicion5, Ciudad
  LATERAL VIEW posexplode(txtCuentasTransac.value) NoCuenta AS Posicion6, NoCuenta
  LATERAL VIEW posexplode(txtChequesTransac.value) NoCheque AS Posicion7, NoCheque
  LATERAL VIEW posexplode(dtConsignacionesTransac.value) FechaConsignacion AS Posicion8, FechaConsignacion
  WHERE
  Posicion = Posicion1 
  AND Posicion = Posicion2
  AND Posicion = Posicion3
  AND Posicion = Posicion4
  AND Posicion = Posicion5
  AND Posicion = Posicion6
  AND Posicion = Posicion7
  AND Posicion = Posicion8;

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP VIEW IF EXISTS Process.Zdp_Lotus_reclamos_Reclamos_Anexos;
CREATE VIEW Process.Zdp_Lotus_reclamos_Reclamos_Anexos AS
SELECT
  W.UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/reclamos/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(W.dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart
FROM Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia W 
CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN")