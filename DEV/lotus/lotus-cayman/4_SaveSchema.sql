-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Lotus_Cayman;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de Tiempo
DROP TABLE IF EXISTS lotus_cayman.zdr_lotus_cayman_cattiempo;
CREATE EXTERNAL TABLE lotus_cayman.zdr_lotus_cayman_cattiempo
(  
  fecha date,
  fecha_solicitudes string,
  anio int,
  trimestre BIGINT,
  num_mes INT,
  semana_mes STRING,
  nombre_mes STRING,
  nombre_mes_abre STRING,
  dias_semana STRING,
  nombre_dia_semana STRING, 
  nombre_dia_semana_abre STRING,
  dia_anio INT
)
LOCATION '/mnt/lotus-cayman/results/cattiempo';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 1
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat1;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat1
(
  UniversalId string,
  posicion int,
  dtmTrans string,
  txtFlujoTrans string,
  txtEstadoTrans string,
  txtResponTrans string,
  numTiempoTrans string,
  txtReprocesos string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat1';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 2
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat2;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat2
(
  UniversalId string,
  posicion int,
  txtCEGEntidad string,
  numCEGTotal string,
  numCEGPartUltTrimestre string,
  numCEGVarMonto string,
  numCEGPorcCrecimiento string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat2';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 3
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat3;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat3
(
  UniversalId string,
  posicion int,
  txtClaseGRA string,
  numCantidadGRA string,
  numVlrAvalGRA string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat3';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 4
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat4;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat4
(
  UniversalId string,
  posicion int,
  txtIdDocumentos string,
  txtDocumentos string,
  chkIND string,
  chkEN string,
  chkRE string,
  chkFA string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat4';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 5 (cliente a evaluar - información de los socios)
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat5;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat5
(
  UniversalId string,
  posicion int,
  txtIdSocios string,
  txtTipoIdSocios string,
  txtNombreSocios string,
  numPorcPartSocios string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat5';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 6
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat6;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat6
(
  UniversalId string,
  posicion int,
  txtNroGar string,
  txtTipoGRO string,
  txtConceptoGRO string,
  txtAvaluoGRO string,
  numVlrAvalGRO string,
  numVlrCubGRO string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat6';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 7
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat7;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat7
(
  UniversalId string,
  posicion int,
  txtTipCliAsoC string,
  txtNombres string,
  numCMLAct string,
  numCMLSol string,
  numCMLPic string,
  numCMLRA string,
  txtCMLNumTar string,
  dtmCMLFecVen string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat7';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 8
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat8;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat8
(
  UniversalId string,
  posicion int,
  txtRecComerciales string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat8';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 9
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat9;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat9
(
  UniversalId string,
  posicion int,
  txtHistorico string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat9';

-- COMMAND ----------

-- DBTITLE 1,Catálogo 10
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat10;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat10
(
  UniversalId string,
  posicion int,
  txtRecCadena string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/cat10';

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes (pilas con la fecha)
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Solicitudes; 
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Solicitudes 
(
    UniversalID string,
    NUMTOTCMLACT string,
    NUMTOTCMLSOL string,
    NUMTOTCMLPIC string,
    NUMTOTCMLRA string,
    txtRadicado string,
    dtmCreado string,
    Fecha_Solicitudes string,
    txtAutor string,
    txtCodAutor string,
    cbTipoSolicitud string,
    cbTipoCliente string,
    txtEstado string,
    cbNomTipoSolicitud string,
    txtTipoClienteVis string,
    txtResponsable string,
    dtmDecision string,
    txtAprobadorC string,
    txtCodOficial string,
    txtNumActaJunta string,
    txtNumIdCorto string,
    txtCalifSuperbancaria string,
    txtTipoId string,
    txtCalifInternaAnterior string,
    txtNomCliente string,
    txtCalifAnioParcial string,
    txtRegion string,
    txtCalifUltimoAnio string,
    txtZona string,
    txtSegmento string,
    txtSector string,
    txtActividadEconomica string,
    txtCodigoCIIU string,
    txtGerente string,
    txtCodGerente string,
    numPromTrimCtaCorriente string,
    txtCentroCostos string,
    numPromTrimCtaAhorro string,
    txtGrupoRiesgo string,
    txtCodRiesgo string,
    numDeudaActual string,
    dtmNacimiento string,
    dtmVinculacion string,
    txtCentralIxInterna string,
    txtCentralIxExterna string,
    numCEGTotEntidades string,
    dtmCamComConstitucion string,
    txtCamComVigencia string,
    txtCamComActEconomica string,
    txtCamComEmbargo string,
    dtmCamComExpedicion string,
    txtCamComCertificadoVigente string,
    txtIdRepLegal string,
    txtTipoIdRepLegal string,
    txtNombreRepLegal string,
    txtFacultadRepLegal string,
    numCartSol string,
    numCartPIC string,
    numCartAsi string,
    numCartGlbAsig string,
    numCartGlbDis string,
    txtCarRec string,
    txtCarJus string,
    numBienesGRA string,
    numTotAvalGRA string,
    numBienesGRO string,
    numTotAvalGRO string,
    numTotCubGRO string,
    txtObsGarantias string,
    txtRecComReciente string,
    txtRecCadReciente string,
    txtObsDocumentos string,
    numCartAct string,
    numCartGlbAsigC string,
    numCartGlbDisC string,
    txtCarRecC string,
    txtCarJusC string,
    busqueda string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/solicitudes';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Anexos;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Anexos
(
  UniversalId string,
  link string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/anexos';

-- COMMAND ----------

-- DBTITLE 1,Tabla SolicitudesHijos
DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_SolicitudesHijos;
CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_SolicitudesHijos
(
  UniversalId string,
  posicion int,
  txtnumidcorto string,
  txttipoid string,
  txtnomcliente string,
  dtmnacimiento string,
  txtactividadeconomica string,
  txtcentralixinterna string,
  txtcentralixexterna string,
  dtmvinculacion string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-cayman/results/solicitudeshijos';

-- COMMAND ----------

-- DBTITLE 1,Datos Generales
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_datosgenerales;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_datosgenerales AS
SELECT UniversalId, 'Fecha de creacion' AS key, dtmCreado AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Nombre del autor' AS key, txtautor AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Código del autor' AS key, txtCodAutor AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Tipo de solicitud' AS key, cbNomTipoSolicitud AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Tipo de cliente' AS key, txtTipoClienteVis AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes 
UNION ALL
SELECT UniversalId, 'Estado' AS key, txtEstado AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Responsable actual' AS key, txtResponsable AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_datosflujoaprobacion;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_datosflujoaprobacion AS
SELECT UniversalId, 'Fecha de decisión' AS key, dtmDecision AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Nombre de quien aprueba' AS key, txtAprobadorC AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Código de quien aprueba' AS key, txtCodOficial AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Número de acta de comité de crédito' AS key, txtNumActaJunta AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

-- COMMAND ----------

-- DBTITLE 1,Cliente a evaluar
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_clienteevaluar;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_clienteevaluar AS
SELECT UniversalId, 'Número de identificación' AS key, txtNumIdCorto AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Tipo de identificación' AS key, txtTipoId AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Nombre y apellidos o razón social' AS key, txtNomCliente AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Región' AS key, txtRegion AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Zona' AS key, txtZona AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Segmento' AS key, txtSegmento AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Sector' AS key, txtSector AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Actividad económica del cliente' AS key, txtActividadEconomica AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Código CIIU' AS key, txtCodigoCIIU AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Gerente' AS key, txtGerente AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Código del gerente' AS key, txtCodGerente AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Centro de costos' AS key, txtCentroCostos AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Grupo de riesgo' AS key, txtGrupoRiesgo AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Código de riesgo' AS key, txtCodRiesgo AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de nacimiento' AS key, dtmNacimiento AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de vinculación' AS key, dtmVinculacion AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_calificaciones;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_calificaciones AS
SELECT UniversalId, 'Superbancaria' AS key, txtCalifSuperbancaria AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Interna anterior' AS key, txtCalifInternaAnterior AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Año parcial' AS key, txtCalifAnioParcial AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Último año' AS key, txtCalifUltimoAnio AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_promediocuenta;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_promediocuenta AS
SELECT UniversalId, 'Corriente' AS key, numPromTrimCtaCorriente AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Ahorro' AS key, numPromTrimCtaAhorro AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Deuda actual' AS key, numDeudaActual AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

-- COMMAND ----------

-- DBTITLE 1,CIFIN
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_cifin;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_cifin AS
SELECT UniversalId, 
       txtCEGEntidad AS Entidad, 
       numCEGTotal AS Total, 
       numCEGPartUltTrimestre AS Participacion_ultimo_trimestre,
       numCEGVarMonto AS Variacion_monto,
       numCEGPorcCrecimiento AS  Crecimiento
FROM lotus_cayman.zdr_lotus_cayman_cat2
UNION ALL
SELECT UniversalId, 
       'Total entidades' AS Entidad, 
       numCEGTotEntidades AS Total, 
       '' AS Participacion_ultimo_trimestre, 
       '' AS Variacion_monto, 
       '' AS Crecimiento
FROM lotus_cayman.zdr_lotus_cayman_solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Cámara de Comercio
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_camaradecomercio;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_camaradecomercio AS
SELECT UniversalId, 'Fecha de constitución' AS key, dtmCamComConstitucion AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Vigencia' AS key, txtCamComVigencia AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Actividad económica' AS key, txtCamComActEconomica AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Posee embargos la sociedad?' AS key, txtCamComEmbargo AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de expedición cámara de comercio' AS key, dtmCamComExpedicion AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Certificado de cámara de comercio vigente?' AS key, txtCamComCertificadoVigente AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_representantelegal;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_representantelegal AS
SELECT UniversalId, 'Número de identificación' AS key, txtIdRepLegal AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Tipo de identificación' AS key, txtTipoIdRepLegal AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Nombre' AS key, txtNombreRepLegal AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT UniversalId, 'Facultad' AS key, txtFacultadRepLegal AS value FROM lotus_cayman.zdr_lotus_cayman_solicitudes
ORDER BY UniversalId,key;

-- COMMAND ----------

-- DBTITLE 1,Cupo Tarjeta Detalle
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_detalle;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_detalle AS

SELECT  
        S.UniversalId,
        C7.txtTipCliAsoC  as TipoDeClientesAsociados,
        C7.txtNombres as NombresClientesAvalistas,        
        C7.numCMLAct as CupoActualAsignado,
        C7.numCMLSol as CupoAdicionalSolicitado,
        C7.numCMLPic as CupoAdicionalPIC,
        C7.numCMLRA as CupoAdicionalAprobado,
        C7.txtCMLNumTar as NumeroDeTarjeta,
        C7.dtmCMLFecVen as FechaDeVencimiento           
FROM lotus_cayman.zdr_lotus_cayman_solicitudes S
INNER JOIN lotus_cayman.zdr_lotus_cayman_cat7 C7
  ON S.UniversalId = C7.UniversalId 
UNION ALL
SELECT 
        S2.UniversalId,
        '' as TipoDeClientesAsociados,
        'TOTAL' as NombresClientesAvalistas,   
        S2.NUMTOTCMLACT as TotalCupoAdicionalAsignado,
        S2.NUMTOTCMLSOL as TotalCupoAdicionalSolicitado,
        S2.NUMTOTCMLPIC as TotalCupoaAicionalPIC,
        S2.NUMTOTCMLRA as TotalCupoAdicionalAprobado,
        '' as NumeroDeTarjeta,
        '' as FechaDeVencimiento        
FROM lotus_cayman.zdr_lotus_cayman_solicitudes S2
INNER JOIN lotus_cayman.zdr_lotus_cayman_cat7 C72
  ON S2.UniversalId = C72.UniversalId;
  
  
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_recomendacion;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_recomendacion AS

SELECT 
   UniversalId,
  '0' as posicion, 
  'Recomendacion' as informacion,
  txtCarRec as Recomendacion
  FROM lotus_cayman.zdr_lotus_cayman_solicitudes
union all
SELECT 
  UniversalId,
  '1' as posicion, 
  'Justificacion' as informacion,
  txtCarJus as Justificacion
FROM lotus_cayman.zdr_lotus_cayman_solicitudes;


DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta AS

select 
UniversalId,
'0' as posicion, 
'Tarjeta de Credito Bancolombia CAYMAN Visa' as Producto,
'' as Detalle,
numCartAct  as CupoActualAsignado,
numCartSol as CupoAdicionalSolicitado,
numCartPIC as CupoAdicionalPIC,
numCartAsi as CupoAdicionalAprobado

from lotus_cayman.zdr_lotus_cayman_solicitudes
union all
select 
UniversalId,
'1' as posicion,
'Cupo Global Asignado' as Producto,
numCartGlbAsig as Detalle,
''  as CupoActualAsignado,
'' as CupoAdicionalSolicitado,
'' as CupoAdicionalPIC,
'' as CupoAdicionalAprobado
from lotus_cayman.zdr_lotus_cayman_solicitudes
union all
select 
UniversalId,
'2' as posicion,
'Cupo Global Disponible' as Producto,
numCartGlbDis as Detalle,
''  as CupoActualAsignado,
'' as CupoAdicionalSolicitado,
'' as CupoAdicionalPIC,
'' as CupoAdicionalAprobado
from lotus_cayman.zdr_lotus_cayman_solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Garantías
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_garantias_reales;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_garantias_reales AS
SELECT
  UniversalId,
  "Actual" AS Detalle_garantia_real,
  numBienesGRA AS Numero_bienes,
  numTotAvalGRA AS Valor_avaluo,
  "N/A" AS Valor_cubierto
FROM
  lotus_cayman.zdr_lotus_cayman_solicitudes
UNION ALL
SELECT
  UniversalId,
  "Ofrecida" AS Detalle_garantia_real,
  numBienesGRO AS Numero_bienes,
  numTotAvalGRO AS Valor_avaluo,
  numTotCubGRO AS Valor_cubierto
FROM
  lotus_cayman.zdr_lotus_cayman_solicitudes;

DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_garantias_actual;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_garantias_actual AS
SELECT
  UniversalId,
  txtClaseGRA AS Clase_garantia,
  numCantidadGRA AS Cantidad,
  numVlrAvalGRA AS Valor_avaluo
FROM
  lotus_cayman.zdr_lotus_cayman_cat3
UNION ALL
SELECT
  UniversalId,
  "TOTAL" AS Clase_garantia,
  numBienesGRA AS Cantidad,
  numTotAvalGRA AS Valor_avaluo
FROM
  lotus_cayman.zdr_lotus_cayman_solicitudes;
  
DROP VIEW IF EXISTS lotus_cayman.zdr_uvw_lotus_cayman_garantias_ofrecida;
CREATE VIEW lotus_cayman.zdr_uvw_lotus_cayman_garantias_ofrecida AS
SELECT
  UniversalId,
  txtNroGar AS Nro,
  txtTipoGRO AS Tipo_garantia,
  txtConceptoGRO AS Concepto_garantia,
  txtAvaluoGRO AS Fecha_avaluo,
  numVlrAvalGRO AS Valor_avaluo,
  numVlrCubGRO AS Valor_cubierto
FROM
  lotus_cayman.zdr_lotus_cayman_cat6
UNION ALL
SELECT
  UniversalId,
  "" AS Nro,
  "" AS Tipo_garantia,
  "" AS Concepto_garantia,
  "TOTAL" AS Fecha_avaluo,
  numTotAvalGRO AS Valor_avaluo,
  numTotCubGRO AS Valor_cubierto
FROM
  lotus_cayman.zdr_lotus_cayman_solicitudes;