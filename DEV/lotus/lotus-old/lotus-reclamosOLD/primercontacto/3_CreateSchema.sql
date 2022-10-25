-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia
( 
	UniversalID string,
	attachments array<string>,
	SSConflictAction map<string,string>,
	SSRevisions map<string, array<string>>,
	SSUpdatedBy map<string, array<string>>,
	SSWebFlags map<string,string>,
	SSSSReturn map<string,string>,
	dtFechaCreacion map<string,string>,
	txtNumIdCliente map<string,string>,
	txtTipoProducto map<string,string>,
	txtTipoReclamo map<string,string>,
	cbResponsable map<string,string>,
	txtCerrado map<string,string>,
	txtCodCausalidad map<string,string>,
	txtGuardo map<string,string>,
	txtIdTipoProducto map<string,string>,
	txtIdTipoReclamo map<string,string>,
	TXTIDESTADO map<string,string>,
	txtIdEstadoTrans map<string, array<string>>,
	dtFechasTrans map<string, array<string>>,
	txtEstadoTrans map<string, array<string>>,
	txtCodSucursalCap map<string,string>,
	numTiempoSolucion map<string,string>,
	txtValidarCodCausalidad map<string,string>,
	txtResponsableOrig map<string,string>,
	dtmAsignacionResp map<string,string>,
	txtUsuarioActual map<string,string>,
	txtRadicado map<string,string>,
	txtEstado map<string,string>,
	txtAutor map<string,string>,
	cbSucCaptura map<string,string>,
	txtComentario map<string, array<string>>,
	TXTSUCCAPTURA map<string,string>,
	Form map<string,string>,
	txtHistoria map<string, array<string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/primercontacto/raw';

-- COMMAND ----------

-- DBTITLE 1,Vista Reclamos (Campos Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos AS
SELECT
	UniversalID AS UniversalID,
    txtAutor.value AS Autor,
	dtFechaCreacion.value AS FechaSolucionadoPrimerContacto,
    substring_index(dtFechaCreacion.value, ' ', 1) AS fecha_solicitudes,
    substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart,
    TXTSUCCAPTURA.value AS SucursalCaptura,
    txtTipoProducto.value AS TipoProducto,
    txtTipoReclamo.value AS TipoReclamo,
    txtNumIdCliente.value AS NumeroIdentificacion,
    cbResponsable.value AS Responsable,
    txtCodCausalidad.value AS CodigoCausalidad,
    txtRadicado.value AS Radicado,
    txtEstado.value AS Estado,
    cbSucCaptura.value AS CodSucursalCaptura,
    concat_ws('\n', txthistoria.value) AS Historia,
    concat_ws('\n', txtComentario.value) AS Comentarios,
    concat(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value), ' - ', upper(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value)), ' - ', lower(concat(txtNumIdCliente.value, ' - ', cbResponsable.value, ' - ', txtRadicado.value, ' - ', txtEstado.value, ' - ', txtTipoProducto.value, ' - ', txtTipoReclamo.value))) AS busqueda
FROM
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Tabla ReclamosOcultos (Campos No Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos_Ocultos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos_Ocultos AS
SELECT
	UniversalID AS UniversalID,
    SSConflictAction.value AS SSConflictAction,
    concat_ws('\n', SSRevisions.value) AS SSRevisions,
    concat_ws('\n', SSUpdatedBy.value) AS SSUpdatedBy,
    SSWebFlags.value AS SSWebFlags,
    SSSSReturn.value AS SSSSReturn,
    txtCerrado.value AS ReclamoCerrado,
    txtGuardo.value AS PrimerContactoGuardadoUsuario,
    txtIdTipoProducto.value AS IdTipoProducto,
    txtIdTipoReclamo.value AS IdTipoReclamo,
    TXTIDESTADO.value AS IdEstado,
    concat_ws('\n',txtIdEstadoTrans.value) AS IdEstadosTransitorios,
    concat_ws('\n',dtFechasTrans.value) AS FechasTransitorias,
    concat_ws('\n',txtEstadoTrans.value) AS EstadosTransitorios,
    txtCodSucursalCap.value AS CodigoSucursalCaptura,
    numTiempoSolucion.value AS TiempoEstimadoSolucion,
    txtValidarCodCausalidad.value AS ValidarCodigoCausalidad,
    txtResponsableOrig.value AS ResponsableOriginal,
    dtmAsignacionResp.value AS FechaAsignacionNuevoResponsable,
    txtUsuarioActual.value AS UsuarioActual,
    Form.value AS Formulario
FROM
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia;

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos AS
SELECT
  UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/primercontacto/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart
FROM
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia W
  CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
  CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
  CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN")