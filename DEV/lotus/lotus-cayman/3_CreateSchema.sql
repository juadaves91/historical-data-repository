-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de tiempo
DROP TABLE IF EXISTS process.zdp_lotus_cayman_dimtiempo;
CREATE TABLE process.zdp_lotus_cayman_dimtiempo AS
WITH fechas AS (
    SELECT date_add("2014-01-01", a.pos) as fecha
    FROM (SELECT posexplode(split(repeat(",", 2190), ","))) a
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
DROP TABLE IF EXISTS Process.Zdp_Lotus_Cayman_Tablaintermedia;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Cayman_Tablaintermedia
( 
  UniversalID string,
  attachments array<string>,
  hijos array<struct<
    UniversalID:string, 
    form:map<string,array<string>>, 
    txtMsgErrorDescarga:map<string,array<string>>, 
    dtmUltimaConsultaAS:map<string,array<string>>, 
    txtPrimeraDescarga:map<string,array<string>>, 
    txtRadicado:map<string,array<string>>, 
    txtNumIdCorto:map<string,array<string>>, 
    txtTipoId:map<string,array<string>>, 
    txtNomCliente:map<string,array<string>>, 
    dtmNacimiento:map<string,array<string>>, 
    txtActividadEconomica:map<string,array<string>>, 
    txtCentralIxInterna:map<string,array<string>>, 
    txtCentralIxExterna:map<string,array<string>>, 
    dtmVinculacion:map<string,array<string>>, 
    SSUpdatedBy:map<string,array<string>>, 
    SSRevision:map<string,array<string>>>>,
    SSConflictAction map<string,string>,
    SSRevisions map<string,array<string>>,
    SSUpdatedBy map<string,array<string>>,
    SSWebFlags map<string,string>,
    alertaSocio map<string,string>,
    cbNomTipoSolicitud map<string,string>,
    cbOtroCliTipoIds map<string,string>,
    cbTipoCliente map<string,string>,
    cbTipoSolicitud map<string,string>,
    chkCN map<string,array<string>>,
    chkCNC map<string,array<string>>,
    chkCS map<string,array<string>>,
    chkCSC map<string,array<string>>,
    chkDocumentos map<string,array<string>>,
    chkEN map<string,array<string>>,
    chkENC map<string,array<string>>,
    chkFA map<string,array<string>>,
    chkFAC map<string,array<string>>,
    chkIND map<string,array<string>>,
    chkINDC map<string,array<string>>,
    chkRE map<string,array<string>>,
    chkREC map<string,array<string>>,
    dtmCamComConstitucion map<string,string>,
    dtmCamComExpedicion map<string,string>,
    dtmCMLFecVen map<string,array<string>>,
    dtmCreado map<string,string>,
    dtmDecision map<string,string>,
    dtmNacimiento map<string,string>,
    dtmNacimientoC map<string,string>,
    dtmNacimientoE map<string,string>,
    dtmPapelera map<string,string>,
    dtmTrans map<string,array<string>>,
    dtmUltimaConsultaAS map<string,string>,
    dtmVinculacion map<string,string>,
    form map<string,string>,
    numActFijosGPO map<string,array<string>>,
    numBienesGRA map<string,string>,
    numBienesGRO map<string,string>,
    numCantidadGRA map<string,array<string>>,
    numCartAct map<string,string>,
    numCartAsi map<string,string>,
    numCartGlbAsig map<string,string>,
    numCartGlbAsigC map<string,string>,
    numCartGlbDis map<string,string>,
    numCartGlbDisC map<string,string>,
    numCartPIC map<string,string>,
    numCartSol map<string,string>,
    numCEGPartUltTrimestre map<string,array<string>>,
    numCEGPorcCrecimiento map<string,array<string>>,
    numCEGTotal map<string,array<string>>,
    numCEGTotEntidades map<string,string>,
    numCEGVarMonto map<string,array<string>>,
    numCMLAct map<string,array<string>>,
    numCMLPic map<string,array<string>>,
    numCMLRA map<string,array<string>>,
    numCMLSol map<string,array<string>>,
    numCodeudoresGPO map<string,string>,
    numDeudaActual map<string,string>,
    numDiasCreacion map<string,string>,
    numDiasEstado map<string,string>,
    numIndVisualTiempoEstado map<string,string>,
    numPatBrutoGPO map<string,array<string>>,
    numPatLiquidoGPO map<string,array<string>>,
    numPorcPartSocios map<string,array<string>>,
    numPromTrimCtaAhorro map<string,string>,
    numPromTrimCtaCorriente map<string,string>,
    numRenLiquidaGPO map<string,array<string>>,
    numTiempoTrans map<string,array<string>>,
    numTotActFijosGPO map<string,string>,
    numTotAvalGRA map<string,string>,
    numTotAvalGRO map<string,string>,
    NUMTOTCMLACT map<string,string>,
    NUMTOTCMLPIC map<string,string>,
    NUMTOTCMLRA map<string,string>,
    NUMTOTCMLSOL map<string,string>,
    numTotCubGRO map<string,string>,
    numTotPatBrutoGPO map<string,string>,
    numTotPatLiquidoGPO map<string,string>,
    numTotRenLiquidaGPO map<string,string>,
    numVlrAvalGRA map<string,array<string>>,
    numVlrAvalGRO map<string,array<string>>,
    numVlrCubGRO map<string,array<string>>,
    OriginalModTime map<string,string>,
    SaveOptions map<string,string>,
    txtAbriendoDesde map<string,string>,
    txtActividadEconomica map<string,string>,
    txtAnalista map<string,string>,
    txtAprobador map<string,string>,
    txtAprobadorC map<string,string>,
    txtAsignador map<string,string>,
    txtAutor map<string,string>,
    txtAuxArchivo map<string,string>,
    txtAuxDocumentacion map<string,string>,
    txtAuxGrabacion map<string,string>,
    txtAuxReclasificado map<string,string>,
    txtAvaluoGRO map<string,array<string>>,
    txtCalifAnioParcial map<string,string>,
    txtCalifInternaAnterior map<string,string>,
    txtCalifSuperbancaria map<string,string>,
    txtCalifUltimoAnio map<string,string>,
    txtCamComActEconomica map<string,string>,
    txtCamComCertificadoVigente map<string,string>,
    txtCamComEmbargo map<string,string>,
    txtCamComVigencia map<string,string>,
    txtCarJus map<string,string>,
    txtCarJusC map<string,string>,
    txtCarRec map<string,string>,
    txtCarRecC map<string,string>,
    txtCEGEntidad map<string,array<string>>,
    txtCentralIxExterna map<string,string>,
    txtCentralIxInterna map<string,string>,
    txtCentroCostos map<string,string>,
    txtClaseGRA map<string,array<string>>,
    txtCliEditable map<string,string>,
    txtCMLNumTar map<string,array<string>>,
    txtCodAutor map<string,string>,
    txtCodControl map<string,string>,
    txtCodEstadoVin map<string,string>,
    txtCodGerente map<string,string>,
    txtCodigoCIIU map<string,string>,
    txtCodOficial map<string,string>,
    txtCodRegion map<string,string>,
    txtCodRiesgo map<string,string>,
    txtCodRiesgoC map<string,string>,
    txtCodRiesgoE map<string,string>,
    txtCodSector map<string,string>,
    txtCodSegmento map<string,string>,
    txtCodZona map<string,string>,
    txtConceptoGRO map<string,array<string>>,
    txtDesIdEstado map<string,array<string>>,
    txtDocumentos map<string,array<string>>,
    txtEliminar map<string,string>,
    txtEnviados map<string,string>,
    txtEstado map<string,string>,
    txtEstadoControl map<string,string>,
    txtEstadoTrans map<string,array<string>>,
    txtFacultadRepLegal map<string,string>,
    TXTFALTANTES map<string,array<string>>,
    txtFlujo map<string,string>,
    txtFlujoTrans map<string,array<string>>,
    TXTFORZARDESCARGA map<string,string>,
    txtGerente map<string,string>,
    txtGerenteCredito map<string,string>,
    txtGrabadorLME map<string,string>,
    txtGrupoRiesgo map<string,string>,
    txtGrupoRiesgoC map<string,string>,
    txtGrupoRiesgoE map<string,string>,
    txtIdDoc map<string,string>,
    txtIdDocumentos map<string,array<string>>,
    txtIdentificaciones map<string,array<string>>,
    txtIdentificacionesR map<string,array<string>>,
    txtIdEstado map<string,array<string>>,
    txtIdEstadoSig map<string,string>,
    txtIdEstadoTrans map<string,array<string>>,
    txtIdEstAnterior map<string,array<string>>,
    txtIdFinCredito map<string,string>,
    txtIdFlujo map<string,string>,
    txtIdFlujoSig map<string,string>,
    txtIdFlujoTrans map<string,array<string>>,
    txtIdRepLegal map<string,string>,
    txtIdSocios map<string,array<string>>,
    txtIdsServicios map<string,array<string>>,
    txtIdTipoId map<string,string>,
    txtIndexados map<string,array<string>>,
    txtMenControl map<string,string>,
    txtMsgErrorDescarga map<string,string>,
    txtNombreRepLegal map<string,string>,
    txtNombres map<string,array<string>>,
    txtNombreSocios map<string,array<string>>,
    txtNomCliente map<string,string>,
    txtNroGar map<string,array<string>>,
    txtNumActaJunta map<string,string>,
    txtNumActaJuntaC map<string,string>,
    txtNumId map<string,string>,
    txtNumIdCorto map<string,string>,
    TXTOBLIGATORIOS map<string,array<string>>,
    txtObsCausales map<string,string>,
    txtObsDocumentos map<string,string>,
    txtObsGarantias map<string,string>,
    txtOtroCliNumId map<string,string>,
    txtOtrosSocios map<string,string>,
    txtPrimeraDescarga map<string,string>,
    txtRadicado map<string,string>,
    txtRecCadReciente map<string,string>,
    txtRecComReciente map<string,string>,
    txtRecibidos map<string,string>,
    txtRegion map<string,string>,
    txtRelLaboral map<string,string>,
    txtReprocesos map<string,array<string>>,
    txtResponsable map<string,string>,
    txtResponsableControl map<string,string>,
    txtResponTrans map<string,array<string>>,
    txtSector map<string,string>,
    txtSectorC map<string,string>,
    txtSectorE map<string,string>,
    txtSegmento map<string,string>,
    txtSegmentoC map<string,string>,
    txtSegmentoE map<string,string>,
    txtSocEditable map<string,string>,
    txtTipCliAso map<string,string>,
    txtTipCliAsoC map<string,array<string>>,
    txtTipo map<string,string>,
    txtTipoCliente map<string,string>,
    txtTipoClienteVis map<string,string>,
    txtTipoGRO map<string,array<string>>,
    txtTipoId map<string,string>,
    txtTipoIdAvalistas map<string,array<string>>,
    txtTipoIdRepLegal map<string,string>,
    txtTipoIdSocios map<string,array<string>>,
    txtTipoPersona map<string,string>,
    txtTipoRegMTS map<string,string>,
    txtTiposId map<string,array<string>>,
    txtTiposIdDes map<string,array<string>>,
    txtZona map<string,string>,
    validarSarlaft map<string,string>,
    txtIdEstadosFin map<string,array<string>>,
    txtHistorico map<string,array<string>>,
    txtRecCadena map<string,array<string>>,
    txtRecComerciales map<string,array<string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-cayman/raw';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 1
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat1;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat1 AS
SELECT
  UniversalId,
  posicion,
  edtmTrans,
  etxtFlujoTrans,
  etxtEstadoTrans,
  etxtResponTrans,
  enumTiempoTrans,
  etxtReprocesos
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(dtmTrans['value'])       edtmTrans       AS posicion,    edtmTrans
  LATERAL VIEW posexplode(txtFlujoTrans['value'])  etxtFlujoTrans  AS flujo,       etxtFlujoTrans
  LATERAL VIEW posexplode(txtEstadoTrans['value']) etxtEstadoTrans AS estado,      etxtEstadoTrans
  LATERAL VIEW posexplode(txtResponTrans['value']) etxtResponTrans AS responsable, etxtResponTrans
  LATERAL VIEW posexplode(numTiempoTrans['value']) enumTiempoTrans AS tiempo,      enumTiempoTrans
  LATERAL VIEW posexplode(txtReprocesos['value'])  etxtReprocesos  AS reproceso,   etxtReprocesos
WHERE
  posicion = flujo
  AND posicion = estado
  AND posicion = responsable
  AND posicion = tiempo
  AND posicion = reproceso;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 2
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat2;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat2 AS
SELECT
  UniversalId,
  posicion,
  etxtCEGEntidad,
  enumCEGTotal,
  enumCEGPartUltTrimestre,
  enumCEGVarMonto,
  enumCEGPorcCrecimiento
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtCEGEntidad['value'])          etxtCEGEntidad          AS posicion,    etxtCEGEntidad
  LATERAL VIEW posexplode(numCEGTotal['value'])            enumCEGTotal            AS total,       enumCEGTotal
  LATERAL VIEW posexplode(numCEGPartUltTrimestre['value']) enumCEGPartUltTrimestre AS trimestre,   enumCEGPartUltTrimestre
  LATERAL VIEW posexplode(numCEGVarMonto['value'])         enumCEGVarMonto         AS monto,       enumCEGVarMonto
  LATERAL VIEW posexplode(numCEGPorcCrecimiento['value'])  enumCEGPorcCrecimiento  AS crecimiento, enumCEGPorcCrecimiento
WHERE
  posicion = total
  AND posicion = trimestre
  AND posicion = monto
  AND posicion = crecimiento;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 3
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat3;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat3 AS
SELECT
  UniversalId,
  posicion,
  etxtClaseGRA,
  enumCantidadGRA,
  enumVlrAvalGRA
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtClaseGRA['value'])    etxtClaseGRA    AS posicion, etxtClaseGRA
  LATERAL VIEW posexplode(numCantidadGRA['value']) enumCantidadGRA AS cantidad, enumCantidadGRA
  LATERAL VIEW posexplode(numVlrAvalGRA['value'])  enumVlrAvalGRA  AS aval,     enumVlrAvalGRA
WHERE
  posicion = cantidad
  AND posicion = aval;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 4
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat4;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat4 AS
SELECT
  UniversalId,
  posicion,
  etxtIdDocumentos,
  etxtDocumentos,
  CASE echkIND When '' Then '' Else 'X' End AS Anexado,
  CASE echkEN  When '' Then '' Else 'X' End AS Enviado,
  CASE echkRE  When '' Then '' Else 'X' End AS Recibido,
  CASE echkFA  When '' Then '' Else 'X' End AS Faltante
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdDocumentos['value']) etxtIdDocumentos AS posicion,   etxtIdDocumentos
  LATERAL VIEW posexplode(txtDocumentos['value'])   etxtDocumentos   AS doc,        etxtDocumentos
  LATERAL VIEW posexplode(chkIND['value'])          echkIND          AS ind,        echkIND
  LATERAL VIEW posexplode(chkEN['value'])           echkEN           AS en,         echkEN
  LATERAL VIEW posexplode(chkRE['value'])           echkRE           AS re,         echkRE
  LATERAL VIEW posexplode(chkFA['value'])           echkFA           AS fa,         echkFA
WHERE
  posicion = ind
  AND posicion = doc
  AND posicion = en
  AND posicion = re
  AND posicion = fa;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 5
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat5;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat5 AS
SELECT
  UniversalId,
  posicion,
  etxtIdSocios,
  etxtTipoIdSocios,
  etxtNombreSocios,
  enumPorcPartSocios
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdSocios['value'])       etxtIdSocios       AS posicion,    etxtIdSocios
  LATERAL VIEW posexplode(txtTipoIdSocios['value'])   etxtTipoIdSocios   AS tiposocio,   etxtTipoIdSocios
  LATERAL VIEW posexplode(txtNombreSocios['value'])   etxtNombreSocios   AS nombresocio, etxtNombreSocios
  LATERAL VIEW posexplode(numPorcPartSocios['value']) enumPorcPartSocios AS partesocio,  enumPorcPartSocios
WHERE
  posicion = tiposocio
  AND posicion = nombresocio
  AND posicion = partesocio;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 6
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat6;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat6 AS
SELECT
  UniversalId,
  posicion,
  etxtNroGar,
  etxtTipoGRO,
  etxtConceptoGRO,
  etxtAvaluoGRO,
  enumVlrAvalGRO,
  enumVlrCubGRO
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtNroGar['value'])      etxtNroGar      AS posicion,    etxtNroGar
  LATERAL VIEW posexplode(txtTipoGRO['value'])     etxtTipoGRO     AS tipogro,     etxtTipoGRO
  LATERAL VIEW posexplode(txtConceptoGRO['value']) etxtConceptoGRO AS conceptogro, etxtConceptoGRO
  LATERAL VIEW posexplode(txtAvaluoGRO['value'])   etxtAvaluoGRO   AS avaluogro,   etxtAvaluoGRO
  LATERAL VIEW posexplode(numVlrAvalGRO['value'])  enumVlrAvalGRO  AS avalgro,     enumVlrAvalGRO
  LATERAL VIEW posexplode(numVlrCubGRO['value'])   enumVlrCubGRO   AS cubgro,      enumVlrCubGRO
WHERE
  posicion = tipogro
  AND posicion = conceptogro
  AND posicion = avaluogro
  AND posicion = avalgro
  AND posicion = cubgro;

-- COMMAND ----------

-- DBTITLE 1,Catálogo 7
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat7;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat7 AS
SELECT
  UniversalId,
  posicion,
  etxtTipCliAsoC,
  etxtNombres,
  enumCMLAct,
  enumCMLSol,
  enumCMLPic,
  enumCMLRA,
  etxtCMLNumTar,
  edtmCMLFecVen
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtTipCliAsoC['value']) etxtTipCliAsoC AS posicion,     etxtTipCliAsoC
  LATERAL VIEW posexplode(txtNombres['value'])    etxtNombres    AS ztxtnombres,   etxtNombres
  LATERAL VIEW posexplode(numCMLAct['value'])     enumCMLAct     AS znumcmlact,    enumCMLAct
  LATERAL VIEW posexplode(numCMLSol['value'])     enumCMLSol     AS znumcmlsol,    enumCMLSol
  LATERAL VIEW posexplode(numCMLPic['value'])     enumCMLPic     AS znumcmlpic,    enumCMLPic
  LATERAL VIEW posexplode(numCMLRA['value'])      enumCMLRA      AS znumcmlra,     enumCMLRA
  LATERAL VIEW posexplode(txtCMLNumTar['value'])  etxtCMLNumTar  AS ztxtcmlnumtar, etxtCMLNumTar
  LATERAL VIEW posexplode(dtmCMLFecVen['value'])  edtmCMLFecVen  AS ztmcmlfecven,  edtmCMLFecVen
WHERE
  posicion = ztxtnombres
  AND posicion = znumcmlact
  AND posicion = znumcmlsol
  AND posicion = znumcmlpic
  AND posicion = znumcmlra
  AND posicion = ztxtcmlnumtar
  AND posicion = ztmcmlfecven;

-- COMMAND ----------

-- DBTITLE 1,Catalogos adicionales (arreglos sin relaciones)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat8;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat8 AS
SELECT
  UniversalId,
  posicion,
  etxtRecComerciales
FROM
  Process.Zdp_Lotus_Cayman_TablaIntermedia
  LATERAL VIEW posexplode(txtRecComerciales['value']) etxtRecComerciales AS posicion, etxtRecComerciales;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat9;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat9 AS
SELECT
  UniversalId,
  posicion,
  etxtHistorico
FROM
  Process.Zdp_Lotus_Cayman_TablaIntermedia
  LATERAL VIEW posexplode(txtHistorico['value']) etxtHistorico AS posicion, etxtHistorico;
  
  DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Cat10;
CREATE VIEW Process.Zdp_Lotus_Cayman_Cat10 AS
SELECT
  UniversalId,
  posicion,
  etxtRecCadena
FROM
  Process.Zdp_Lotus_Cayman_TablaIntermedia
  LATERAL VIEW posexplode(txtRecCadena['value']) etxtRecCadena AS posicion, etxtRecCadena;

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes (campos tipo string)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Solicitudes;
CREATE VIEW Process.Zdp_Lotus_Cayman_Solicitudes AS
SELECT
    UniversalID,
	NUMTOTCMLACT.value AS NUMTOTCMLACT,
	NUMTOTCMLSOL.value AS NUMTOTCMLSOL,
	NUMTOTCMLPIC.value AS NUMTOTCMLPIC,
	NUMTOTCMLRA.value AS NUMTOTCMLRA,
	txtRadicado.value AS txtRadicado,
	dtmCreado.value AS dtmCreado,
	txtAutor.value AS txtAutor,
	txtCodAutor.value AS txtCodAutor,
	cbTipoSolicitud.value AS cbTipoSolicitud,
	cbTipoCliente.value AS cbTipoCliente,
	txtEstado.value AS txtEstado,
	txtResponsable.value AS txtResponsable,
	dtmDecision.value AS dtmDecision,
	txtAprobadorC.value AS txtAprobadorC,
	txtCodOficial.value AS txtCodOficial,
	txtNumActaJunta.value AS txtNumActaJunta,
	txtNumIdCorto.value AS txtNumIdCorto,
	txtCalifSuperbancaria.value AS txtCalifSuperbancaria,
	txtTipoId.value AS txtTipoId,
	txtCalifInternaAnterior.value AS txtCalifInternaAnterior,
	txtNomCliente.value AS txtNomCliente,
	txtCalifAnioParcial.value AS txtCalifAnioParcial,
    txtTipoClienteVis.value AS txtTipoClienteVis,
	txtRegion.value AS txtRegion,
	txtCalifUltimoAnio.value AS txtCalifUltimoAnio,
	txtZona.value AS txtZona,
	txtSegmento.value AS txtSegmento,
	txtSector.value AS txtSector,
	txtActividadEconomica.value AS txtActividadEconomica,
	txtCodigoCIIU.value AS txtCodigoCIIU,
	txtGerente.value AS txtGerente,
	txtCodGerente.value AS txtCodGerente,
	numPromTrimCtaCorriente.value AS numPromTrimCtaCorriente,
	txtCentroCostos.value AS txtCentroCostos,
	numPromTrimCtaAhorro.value AS numPromTrimCtaAhorro,
	txtGrupoRiesgo.value AS txtGrupoRiesgo,
	txtCodRiesgo.value AS txtCodRiesgo,
	numDeudaActual.value AS numDeudaActual,
	dtmNacimiento.value AS dtmNacimiento,
	dtmVinculacion.value AS dtmVinculacion,
	txtCentralIxInterna.value AS txtCentralIxInterna,
	txtCentralIxExterna.value AS txtCentralIxExterna,
	numCEGTotEntidades.value AS numCEGTotEntidades,
	dtmCamComConstitucion.value AS dtmCamComConstitucion,
	txtCamComVigencia.value AS txtCamComVigencia,
	txtCamComActEconomica.value AS txtCamComActEconomica,
	txtCamComEmbargo.value AS txtCamComEmbargo,
	dtmCamComExpedicion.value AS dtmCamComExpedicion,
	txtCamComCertificadoVigente.value AS txtCamComCertificadoVigente,
	txtIdRepLegal.value AS txtIdRepLegal,
	txtTipoIdRepLegal.value AS txtTipoIdRepLegal,
	txtNombreRepLegal.value AS txtNombreRepLegal,
	txtFacultadRepLegal.value AS txtFacultadRepLegal,
	numCartSol.value AS numCartSol,
	numCartPIC.value AS numCartPIC,
	numCartAsi.value AS numCartAsi,
	numCartGlbAsig.value AS numCartGlbAsig,
	numCartGlbDis.value AS numCartGlbDis,
	txtCarRec.value AS txtCarRec,
    cbNomTipoSolicitud.value AS cbNomTipoSolicitud,
	txtCarJus.value AS txtCarJus,
	numBienesGRA.value AS numBienesGRA,
	numTotAvalGRA.value AS numTotAvalGRA,
	numBienesGRO.value AS numBienesGRO,
	numTotAvalGRO.value AS numTotAvalGRO,
	numTotCubGRO.value AS numTotCubGRO,
	txtObsGarantias.value AS txtObsGarantias,
	txtRecComReciente.value AS txtRecComReciente,
	txtRecCadReciente.value AS txtRecCadReciente,
	txtObsDocumentos.value AS txtObsDocumentos,
	numCartAct.value AS numCartAct,
	numCartGlbAsigC.value AS numCartGlbAsigC,
	numCartGlbDisC.value AS numCartGlbDisC,
	txtCarRecC.value AS txtCarRecC,
	txtCarJusC.value AS txtCarJusC,
    concat(concat(if(txtAprobadorC.value is null,'',txtAprobadorC.value), ' - ', 
                  if(txtAutor.value is null,'',txtAutor.value), ' - ', 
                  if(txtNomCliente.value is null,'',txtNomCliente.value), ' - ', 
                  if(txtNumIdCorto.value is null,'',txtNumIdCorto.value), ' - ', 
                  if(txtRadicado.value is null,'',txtRadicado.value)),' - ',
    upper (concat(if(txtAprobadorC.value is null,'',txtAprobadorC.value), ' - ', 
                  if(txtNomCliente.value is null,'',txtNomCliente.value), ' - ', 
                  if(txtAutor.value is null, '',txtAutor.value))),' - ',
    lower (concat(if(txtAprobadorC.value is null,'',txtAprobadorC.value), ' - ', 
                  if(txtNomCliente.value is null,'',txtNomCliente.value), ' - ', 
                  if(txtAutor.value is null, '',txtAutor.value)))) AS busqueda
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia;

-- COMMAND ----------

-- DBTITLE 1,Tabla SolicitudesOcultas (campos tipo string) - no se visualizan por el usuario
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_SolicitudesOcultas;
CREATE VIEW Process.Zdp_Lotus_Cayman_SolicitudesOcultas AS
SELECT
	UniversalID,
	cbOtroCliTipoIds.value AS cbOtroCliTipoIds,
	txtOtroCliNumId.value AS txtOtroCliNumId,
	SaveOptions.value AS SaveOptions,
	OriginalModTime.value AS OriginalModTime,
	SSWebFlags.value AS SSWebFlags,
	SSConflictAction.value AS SSConflictAction,
	form.value AS form,
	txtIdFlujo.value AS txtIdFlujo,
	txtIdFlujoSig.value AS txtIdFlujoSig,
	txtIdEstadoSig.value AS txtIdEstadoSig,
	txtNumId.value AS txtNumId,
	txtIdFinCredito.value AS txtIdFinCredito,
	txtIdTipoId.value AS txtIdTipoId,
	txtCodRegion.value AS txtCodRegion,
	txtCodZona.value AS txtCodZona,
	txtCodSegmento.value AS txtCodSegmento,
	txtCodSector.value AS txtCodSector,
	txtTipoRegMTS.value AS txtTipoRegMTS,
	numIndVisualTiempoEstado.value AS numIndVisualTiempoEstado,
	dtmPapelera.value AS dtmPapelera,
	txtEliminar.value AS txtEliminar,
	numDiasCreacion.value AS numDiasCreacion,
	numDiasEstado.value AS numDiasEstado,
	txtFlujo.value AS txtFlujo,
	txtTipCliAso.value AS txtTipCliAso,
	txtAsignador.value AS txtAsignador,
	txtGerenteCredito.value AS txtGerenteCredito,
	txtAnalista.value AS txtAnalista,
	txtAprobador.value AS txtAprobador,
	txtGrabadorLME.value AS txtGrabadorLME,
	txtAuxArchivo.value AS txtAuxArchivo,
	txtAuxReclasificado.value AS txtAuxReclasificado,
	txtAuxDocumentacion.value AS txtAuxDocumentacion,
	txtResponsableControl.value AS txtResponsableControl,
	txtTipo.value AS txtTipo,
	txtMsgErrorDescarga.value AS txtMsgErrorDescarga,
	dtmUltimaConsultaAS.value AS dtmUltimaConsultaAS,
	txtPrimeraDescarga.value AS txtPrimeraDescarga,
	txtAbriendoDesde.value AS txtAbriendoDesde,
	alertaSocio.value AS alertaSocio,
	validarSarlaft.value AS validarSarlaft,
	txtSegmentoE.value AS txtSegmentoE,
	txtSectorE.value AS txtSectorE,
	txtGrupoRiesgoE.value AS txtGrupoRiesgoE,
	txtCodRiesgoE.value AS txtCodRiesgoE,
	dtmNacimientoE.value AS dtmNacimientoE,
	numTotPatBrutoGPO.value AS numTotPatBrutoGPO,
	numTotPatLiquidoGPO.value AS numTotPatLiquidoGPO,
	numTotRenLiquidaGPO.value AS numTotRenLiquidaGPO,
	numTotActFijosGPO.value AS numTotActFijosGPO,
	txtObsCausales.value AS txtObsCausales,
	txtTipoPersona.value AS txtTipoPersona,
	txtSocEditable.value AS txtSocEditable,
	txtCliEditable.value AS txtCliEditable,
	txtRelLaboral.value AS txtRelLaboral,
	txtCodControl.value AS txtCodControl,
	txtMenControl.value AS txtMenControl,
	txtCodEstadoVin.value AS txtCodEstadoVin,
	txtTipoCliente.value AS txtTipoCliente,
	txtNumActaJuntaC.value AS txtNumActaJuntaC,
	txtSegmentoC.value AS txtSegmentoC,
	txtSectorC.value AS txtSectorC,
	txtGrupoRiesgoC.value AS txtGrupoRiesgoC,
	txtCodRiesgoC.value AS txtCodRiesgoC,
	dtmNacimientoC.value AS dtmNacimientoC,
	numCodeudoresGPO.value AS numCodeudoresGPO,
	txtIdDoc.value AS txtIdDoc,
	txtAuxGrabacion.value AS txtAuxGrabacion,
	txtEnviados.value AS txtEnviados,
	txtEstadoControl.value AS txtEstadoControl,
	TXTFORZARDESCARGA.value AS TXTFORZARDESCARGA,
	txtOtrosSocios.value AS txtOtrosSocios,
	txtRecibidos.value AS txtRecibidos
FROM Process.Zdp_Lotus_Cayman_Tablaintermedia;

-- COMMAND ----------

-- DBTITLE 1,Catalogos adicionales (arreglos sin relaciones) - no visibles por el usuario

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_numPatBrutoGPO;
CREATE VIEW Process.Zdp_Lotus_Cayman_numPatBrutoGPO AS
SELECT
  UniversalId,
  posicion,
  enumPatBrutoGPO
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(numPatBrutoGPO['value']) enumPatBrutoGPO AS posicion, enumPatBrutoGPO;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_numPatLiquidoGPO;
CREATE VIEW Process.Zdp_Lotus_Cayman_numPatLiquidoGPO AS
SELECT
  UniversalId,
  posicion,
  enumPatLiquidoGPO
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(numPatLiquidoGPO['value']) enumPatLiquidoGPO AS posicion, enumPatLiquidoGPO;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_numRenLiquidaGPO;
CREATE VIEW Process.Zdp_Lotus_Cayman_numRenLiquidaGPO AS
SELECT
  UniversalId,
  posicion,
  enumRenLiquidaGPO
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(numRenLiquidaGPO['value']) enumRenLiquidaGPO AS posicion, enumRenLiquidaGPO;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_numActFijosGPO;
CREATE VIEW Process.Zdp_Lotus_Cayman_numActFijosGPO AS
SELECT
  UniversalId,
  posicion,
  enumActFijosGPO
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(numActFijosGPO['value']) enumActFijosGPO AS posicion, enumActFijosGPO;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_TXTOBLIGATORIOS;
CREATE VIEW Process.Zdp_Lotus_Cayman_TXTOBLIGATORIOS AS
SELECT
  UniversalId,
  posicion,
  eTXTOBLIGATORIOS
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(TXTOBLIGATORIOS['value']) eTXTOBLIGATORIOS AS posicion, eTXTOBLIGATORIOS;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIndexados;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIndexados AS
SELECT
  UniversalId,
  posicion,
  etxtIndexados
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIndexados['value']) etxtIndexados AS posicion, etxtIndexados;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_TXTFALTANTES;
CREATE VIEW Process.Zdp_Lotus_Cayman_TXTFALTANTES AS
SELECT
  UniversalId,
  posicion,
  eTXTFALTANTES
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(TXTFALTANTES['value']) eTXTFALTANTES AS posicion, eTXTFALTANTES;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdEstado;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdEstado AS
SELECT
  UniversalId,
  posicion,
  etxtIdEstado
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdEstado['value']) etxtIdEstado AS posicion, etxtIdEstado;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdFlujoTrans;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdFlujoTrans AS
SELECT
  UniversalId,
  posicion,
  etxtIdFlujoTrans
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdFlujoTrans['value']) etxtIdFlujoTrans AS posicion, etxtIdFlujoTrans;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdEstadoTrans;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdEstadoTrans AS
SELECT
  UniversalId,
  posicion,
  etxtIdEstadoTrans
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdEstadoTrans['value']) etxtIdEstadoTrans AS posicion, etxtIdEstadoTrans;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtDesIdEstado;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtDesIdEstado AS
SELECT
  UniversalId,
  posicion,
  etxtDesIdEstado
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtDesIdEstado['value']) etxtDesIdEstado AS posicion, etxtDesIdEstado;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtTipoIdAvalistas;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtTipoIdAvalistas AS
SELECT
  UniversalId,
  posicion,
  etxtTipoIdAvalistas
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtTipoIdAvalistas['value']) etxtTipoIdAvalistas AS posicion, etxtTipoIdAvalistas;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdsServicios;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdsServicios AS
SELECT
  UniversalId,
  posicion,
  etxtIdsServicios
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdsServicios['value']) etxtIdsServicios AS posicion, etxtIdsServicios;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdentificaciones;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdentificaciones AS
SELECT
  UniversalId,
  posicion,
  etxtIdentificaciones
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdentificaciones['value']) etxtIdentificaciones AS posicion, etxtIdentificaciones;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdentificacionesR;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdentificacionesR AS
SELECT
  UniversalId,
  posicion,
  etxtIdentificacionesR
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdentificacionesR['value']) etxtIdentificacionesR AS posicion, etxtIdentificacionesR;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtTiposId;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtTiposId AS
SELECT
  UniversalId,
  posicion,
  etxtTiposId
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtTiposId['value']) etxtTiposId AS posicion, etxtTiposId;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtDocumentos;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtDocumentos AS
SELECT
  UniversalId,
  posicion,
  etxtDocumentos
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtDocumentos['value']) etxtDocumentos AS posicion, etxtDocumentos;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdEstadosFin;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdEstadosFin AS
SELECT
  UniversalId,
  posicion,
  etxtIdEstadosFin
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdEstadosFin['value']) etxtIdEstadosFin AS posicion, etxtIdEstadosFin;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtTiposIdDes;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtTiposIdDes AS
SELECT
  UniversalId,
  posicion,
  etxtTiposIdDes
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtTiposIdDes['value']) etxtTiposIdDes AS posicion, etxtTiposIdDes;
  
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_chkCNC;
CREATE VIEW Process.Zdp_Lotus_Cayman_chkCNC AS
SELECT
  UniversalId,
  posicion,
  echkCNC
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(chkCNC['value']) echkCNC AS posicion, echkCNC;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_chkCSC;
CREATE VIEW Process.Zdp_Lotus_Cayman_chkCSC AS
SELECT
  UniversalId,
  posicion,
  echkCSC
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(chkCSC['value']) echkCSC AS posicion, echkCSC;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_SSUpdatedBy;
CREATE VIEW Process.Zdp_Lotus_Cayman_SSUpdatedBy AS
SELECT
  UniversalId,
  posicion,
  eSSUpdatedBy
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(SSUpdatedBy['value']) eSSUpdatedBy AS posicion, eSSUpdatedBy;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_SSRevisions;
CREATE VIEW Process.Zdp_Lotus_Cayman_SSRevisions AS
SELECT
  UniversalId,
  posicion,
  eSSRevisions
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(SSRevisions['value']) eSSRevisions AS posicion, eSSRevisions;

DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_txtIdEstAnterior;
CREATE VIEW Process.Zdp_Lotus_Cayman_txtIdEstAnterior AS
SELECT
  UniversalId,
  posicion,
  etxtIdEstAnterior
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(txtIdEstAnterior['value']) etxtIdEstAnterior AS posicion, etxtIdEstAnterior;

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_Anexos;
refresh table Default.parametros;
CREATE VIEW Process.Zdp_Lotus_Cayman_Anexos AS
SELECT
  W.UniversalID,
  if (length(concat_ws('\n',w.attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/',W.UniversalID,'.zip'),"") link
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia W
  CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
  CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_CAYMAN_APP")

-- COMMAND ----------

-- DBTITLE 1,Tabla IntermediaHijos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_TablaintermediaHijos;
CREATE VIEW Process.Zdp_Lotus_Cayman_TablaintermediaHijos AS
SELECT
  UniversalID,
  posicion,
  ehijos
FROM
  Process.Zdp_Lotus_Cayman_Tablaintermedia
  LATERAL VIEW posexplode(hijos) ehijos AS posicion, ehijos;

-- COMMAND ----------

-- DBTITLE 1,Tabla SolicitudesHijos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Cayman_SolicitudesHijos;
CREATE VIEW Process.Zdp_Lotus_Cayman_SolicitudesHijos AS
SELECT
  UniversalID,
  posicion, 
  ehijos.txtnumidcorto.value[0] AS txtnumidcorto,
  ehijos.txttipoid.value[0] AS txttipoid,
  ehijos.txtnomcliente.value[0] AS txtnomcliente,
  ehijos.dtmnacimiento.value[0] AS dtmnacimiento,
  ehijos.txtactividadeconomica.value[0] as txtactividadeconomica,
  ehijos.txtcentralixinterna.value[0] AS txtcentralixinterna,
  ehijos.txtcentralixexterna.value[0] AS txtcentralixexterna,
  ehijos.dtmvinculacion.value[0] AS dtmvinculacion
FROM
  Process.Zdp_Lotus_Cayman_TablaintermediaHijos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ZONA DE PROCESADOS

-- COMMAND ----------

-- MAGIC %run /lotus/lotus-cayman/4_SaveSchema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ZONA DE PROCESADOS

-- COMMAND ----------

-- MAGIC %run /lotus/lotus-cayman/5_LoadSchema $from_create_schema = True