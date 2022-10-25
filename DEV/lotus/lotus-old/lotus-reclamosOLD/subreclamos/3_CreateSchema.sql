-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia
( 
	UniversalID string,
	attachments array<string>,
	SSConflictAction map<string,string>,
	SSRevisions map<string, array<string>>,
	SSUpdatedBy map<string, array<string>>,
	SSWebFlags map<string,string>,
	Form map<string,string>,
	txtNumIdCliente map<string,string>,
	txtIdTipoProducto map<string,string>,
	txtIdTipoReclamo map<string,string>,
	numLongProducto map<string,string>,
	txtNomCliente map<string,string>,
	txtTipoProducto map<string,string>,
	txtTipoReclamo map<string,string>,
	txtNumProducto map<string,string>,
	txtPrefijo map<string,string>,
	txtIdsEdReclamo map<string, array<string>>,
	txtIdsEdTransacciones map<string, array<string>>,
	txtIdsEdSubreclamo map<string, array<string>>,
	txtIdDocPadre map<string,string>,
	numValorPesos map<string,string>,
	txtRadicado map<string,string>,
	txtAutor map<string,string>,
	dtFechaCreacion map<string,string>,
	cbResponsable map<string,string>,
	rdTipoCuenta map<string,string>,
	txtNumCuenta map<string,string>,
	numValorGMF map<string,string>,
	numValorComisiones map<string,string>,
	numValorReversion map<string,string>,
	rdClienteRazon map<string,string>,
	rdCargaCliente map<string,string>,
	cbCampoDiligencia map<string,string>,
	cbSucursalError map<string,string>,
	txtAreaAdministrativa map<string,string>,
	txtCuentaTercero map<string,string>,
	rdTipoCuentaTercero map<string,string>,
	cbTarjetas map<string, array<string>>,
	numTiempoSolucionReal map<string,string>,
	dtFechaSolucionReal map<string,string>,
	txtComentario map<string, array<string>>,
	txtHistoria map<string, array<string>>,
	txtEstado map<string,string>,
	TXTIDESTADO map<string,string>,
	dtFechasTrans map<string, array<string>>,
	numDiasTrans map<string, array<string>>,
	numVencidosTrans map<string, array<string>>,
	numContabilizaTrans map<string, array<string>>,
	numTotDias map<string,string>,
	numTotVencidos map<string,string>,
	numTotContabiliza map<string,string>,
	txtIdEstadoTrans map<string, array<string>>,
	txtEstadoTrans map<string, array<string>>,
	txtGuardo map<string,string>,
	cbSucursalSobrante map<string,string>,
	dtFechaSobrante map<string,string>,
	txtCodSucursalSob map<string,string>,
	txtCambRespManual map<string,string>,
	txtCodSucursalError map<string,string>,
	txtIdsEdGeneral map<string, array<string>>,
	txtValorPesos map<string,string>,
	txtValorGMF map<string,string>,
	txtValorComisiones map<string,string>,
	txtValorReversion map<string,string>,
	SSREF map<string,string>,
	SSSSReturn map<string,string>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/subreclamos/raw';

-- COMMAND ----------

-- DBTITLE 1,Vista Subreclamos (Campos Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos AS
SELECT
	UniversalID AS UniversalID_Sub,
    txtIdDocPadre.value AS UniversalID,
    -- Datos Generales
    txtAutor.value AS Autor,
	dtFechaCreacion.value AS FechaCreacion,
    substring_index(dtFechaCreacion.value, ' ', 1) AS fecha_solicitudes,
    substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
    cbResponsable.value AS Responsable,
    txtEstado.value AS Estado,
    txtRadicado.value AS Subreclamo,
    -- Subreclamos
    txtNumIdCliente.value AS NumeroIdentificacion,
    txtNomCliente.value AS Nombre,
    txtTipoProducto.value AS Producto,
    txtTipoReclamo.value AS TipoReclamo,
    concat(txtPrefijo.value,'-',txtNumProducto.value) AS NumeroProducto,
    rdTipoCuenta.value AS TipoCuenta,
    txtNumCuenta.value AS NumeroCuenta,
    numValorPesos.value AS ValorTotalPesos,
    numValorGMF.value AS ValorGMF,
    numValorComisiones.value AS ValorComisiones,
    numValorReversion.value AS ValorReversion,
    rdClienteRazon.value AS ClienteTieneRazon,
    cbCampoDiligencia.value AS CampoDiligenciar,
    rdCargaCliente.value AS CargaCliente,
    cbSucursalSobrante.value AS SucursalSobrante,
    dtFechaSobrante.value AS FechaSobrante,
    cbSucursalError.value AS PyGSucursalError,
    txtAreaAdministrativa.value AS PyGAreaAdministrativa,
    txtCuentaTercero.value AS CuentaTercero,
    rdTipoCuentaTercero.value AS TipoCuentaTercero,
    concat_ws('\n', cbTarjetas.value) AS PyGTarjetas,
    -- Fecha de solucion
    numTiempoSolucionReal.value AS TiempoRealSolucion,
    dtFechaSolucionReal.value AS FechaRealSolucion,
    -- Comentario
    concat_ws('\n', txtComentario.value) AS Comentario,
    -- Historia
    concat_ws('\n', txtHistoria.value) AS Historia,
    numTotDias.value AS TotalDias,
    numTotVencidos.value AS DiasVencidos,
    numTotContabiliza.value AS DiasContabilizados
FROM
  Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Vista ReclamosOcultos (Campos No Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos_Ocultos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos_Ocultos AS
SELECT
  UniversalID as UniversalID_Sub,
  SSConflictAction.value AS SSConflicAction,
  concat_ws('\n', SSRevisions.value) AS SSRevisions,
  concat_ws('\n', SSUpdatedBy.value) AS SSUpdatedBy,
  SSWebFlags.value AS SSWebFlags,
  Form.value AS Formulario,
  txtIdTipoProducto.value AS IdTipoProducto,
  txtIdTipoReclamo.value AS IdTipoReclamo,
  numLongProducto.value AS LongitudNumeroProducto,
  concat_ws('\n', txtIdsEdReclamo.value) AS CamposReclamo,
  concat_ws('\n', txtIdsEdTransacciones.value) AS CamposTransacciones,
  concat_ws('\n', txtIdsEdSubreclamo.value) AS CamposSubreclamo,
  TXTIDESTADO.value AS IdEstado,
  concat_ws('\n', txtIdEstadoTrans.value) AS IdsEstadosTransitorios,
  txtGuardo.value AS SubreclamosGuardadoUsuario,
  txtCodSucursalSob.value AS CodigoSucursalSobrante,
  txtCambRespManual.value AS CambioResponsableManual,
  txtCodSucursalError.value AS CodigoSucursalError,
  concat_ws('\n', txtIdsEdGeneral.value) AS IdsEdicionEspecial,
  txtValorPesos.value AS ValorTotalPesos,
  txtValorGMF.value AS ValorGMF,
  txtValorComisiones.value AS ValorComisiones,
  txtValorReversion.value AS ValorRevision,
  SSREF.value AS SSREF,
  SSSSReturn.value AS SSSSReturn
FROM
  Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Vista Anexos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Anexos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Anexos AS
SELECT
  W.UniversalID AS UniversalID_Sub,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/subreclamos/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(W.dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart
FROM Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia W 
CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN");

-- COMMAND ----------

-- DBTITLE 1,Vista Catalogos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_CatHistoria;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_CatHistoria AS
SELECT
  UniversalID AS UniversalID_Sub,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
  posicion,
  FechaEntrada,
  Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados
FROM
  Process.Zdp_Lotus_Reclamos_Subreclamos_TablaIntermedia
  LATERAL VIEW posexplode(dtFechasTrans['value']) FechaEntrada AS posicion, FechaEntrada
  LATERAL VIEW posexplode(txtEstadoTrans['value']) Estado AS posicion1, Estado
  LATERAL VIEW posexplode(numDiasTrans['value']) TotalDias AS posicion2, TotalDias
  LATERAL VIEW posexplode(numVencidosTrans['value']) DiasVencidos AS posicion3, DiasVencidos
  LATERAL VIEW posexplode(numContabilizaTrans['value']) DiasContabilizados AS posicion4, DiasContabilizados
WHERE
  posicion = posicion1
  AND posicion = posicion2
  AND posicion = posicion3
  AND posicion = posicion4;