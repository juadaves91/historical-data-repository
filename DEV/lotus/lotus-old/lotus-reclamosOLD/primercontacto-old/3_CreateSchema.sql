-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia PrimerContacto Old
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia_Old;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia_Old
(
  UniversalID string,
  attachments array<string>,
  txt_Comentarios map<string,array<string>>,
  iTxt_Cia map<string,string>,
  key_Producto map<string,string>,
  key_TipoReclamo map<string,string>,
  dat_FchCreac map<string,string>,
  dat_SlnPC map<string,string>,
  num_TpoEstSol map<string,string>,
  dat_FchAproxSol map<string,string>,
  txt_CedNit map<string,string>,
  txt_ProductoFVenc map<string,string>,
  nom_Responsable map<string,string>,
  txt_EstadoSlnPC map<string,string>,
  txt_EstadoGestPC map<string,string>,
  key_Estado map<string,string>,
  dat_GestPC map<string,string>,
  txt_CodCausalidad map<string,string>,
  txt_Historia map<string,array<string>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/primercontacto-old/raw';

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimerContacto Old (Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old AS
SELECT
  UniversalID,
  txt_CedNit.value AS CedNit,
  dat_FchCreac.value AS FechaCreacion,
  substring_index(dat_FchCreac.value, ' ', 1) AS fecha_solicitudes,
  substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) AS aniopart,
  concat_ws('\n',txt_Comentarios.value) AS Comentarios,
  key_Producto.value AS Producto,
  key_TipoReclamo.value AS TipoReclamo,
  dat_SlnPC.value AS dat_SlnPC,
  nom_Responsable.value AS Responsable,
  key_Estado.value AS Estado,
  concat_ws('\n',txt_CodCausalidad.value) AS CodCausalidad,
  concat_ws('\n', txt_Historia.value) AS Historia, 
  concat(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value), ' - ',
  upper(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value)), ' - ',
  lower(concat(txt_CedNit.value, ' - ', key_Estado.value, ' - ', key_Producto.value, ' - ', key_TipoReclamo.value))) AS Busqueda
FROM 
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos Old (No visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Ocultos_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Ocultos_Old AS
SELECT
  UniversalID AS UniversalID,
  iTxt_Cia.value AS iTxt_Cia,
  num_TpoEstSol.value AS num_TpoEstSol,
  dat_FchAproxSol.value AS dat_FchAproxSol,
  txt_ProductoFVenc.value AS txt_ProductoFVenc,
  txt_EstadoSlnPC.value AS txt_EstadoSlnPC,
  txt_EstadoGestPC.value AS txt_EstadoGestPC,
  dat_GestPC.value AS dat_GestPC
FROM 
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla primercontacto Anexos Old
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos_Old AS
SELECT
  UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/primercontacto-old/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(dtFechaCreacion.value, ' ', 1),-4,4) AS aniopart
FROM
  Process.Zdp_Lotus_Reclamos_PrimerContacto_TablaIntermedia W
  CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
  CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
  CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN");