-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia Subreclamo Old
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old
(
  UniversalID string,
  attachments array<string>,
  key_ListaCamposTransa map<string,string>,
  key_ListaCamposRecl map<string,string>,
  key_Estados map<string,array<string>>,
  txt_Guardado map<string,string>,
  txt_NumProducto map<string,string>,
  key_TipoCuenta map<string,string>,
  num_NroCuenta map<string,string>,
  num_VlrTotal_1 map<string,string>,
  num_VlrGMF map<string,string>,
  num_VlrComis map<string,string>,
  num_VlrRever map<string,string>,
  key_ClienteRazon map<string,string>,
  key_CargaClte map<string,string>,
  key_PGAreaAdmin map<string,string>,
  txt_Comentarios map<string,array<string>>,
  Server_Name map<string,string>,
  Query_String map<string,string>,
  Path_Info map<string,string>,
  nom_Autor map<string,string>,
  txt_Autor map<string,string>,
  txt_Creador map<string,string>,
  dat_FchCreac map<string,string>,
  txt_CiudCod map<string,string>,
  num_VlrTotal map<string,string>,
  key_PGTarjetas map<string,array<string>>,
  Path map<string,string>,
  Miles map<string,string>,
  txt_IdPadre map<string,string>,
  key_NecesitaPrefijo map<string,string>,
  key_Prefijo map<string,string>,
  key_EsTarjeta map<string,string>,
  key_InvoTransa map<string,string>,
  key_CamposTransa map<string,array<string>>,
  key_CamposRecl map<string,array<string>>,
  key_NumTransa map<string,string>,
  iTxt_Cia map<string,string>,
  txt_CodProdCia map<string,string>,
  num_LongMaxNumProd map<string,string>,
  num_LongNroCta map<string,string>,
  txt_SubSgmtoCliente map<string,string>,
  txt_DobleDig map<string,string>,
  num_DobleDig map<string,string>,
  num_DobleDig_1 map<string,string>,
  num_DobleDig_2 map<string,string>,
  num_DobleDig_3 map<string,string>,
  num_DobleDig_4 map<string,string>,
  num_DobleDig_5 map<string,string>,
  num_DobleDig_6 map<string,string>,
  num_VlrTotalDef map<string,string>,
  txt_ValorAnterior map<string,string>,
  txt_ValorAnterior_1 map<string,string>,
  txt_ValorAnterior_2 map<string,string>,
  num_ValorAnterior_2 map<string,string>,
  txt_ValorAnterior_3 map<string,string>,
  txt_ValorAnterior_4 map<string,string>,
  num_ValorAnterior_4 map<string,string>,
  txt_ValorAnterior_5 map<string,string>,
  num_ValorAnterior_5 map<string,string>,
  txt_ValorAnterior_6 map<string,string>,
  num_ValorAnterior_6 map<string,string>,
  key_CambioRespMS map<string,string>,
  txt_RutaBDSuc map<string,array<string>>,
  txt_RutaBDCalendar map<string,array<string>>,
  txt_RutaBDEstilos map<string,array<string>>,
  txt_RutaBDEmp map<string,array<string>>,
  txt_RutaListin map<string,array<string>>,
  txt_SrvRegionales map<string,array<string>>,
  txt_lstSrvResp map<string,array<string>>,
  txt_lstSrvReplic map<string,array<string>>,
  key_ContabDV map<string,string>,
  num_DVEstadoAnt map<string,string>,
  num_DVParcial map<string,string>,
  num_DRecl map<string,string>,
  num_DAbierto map<string,string>,
  dat_VencPendAbonoTemp map<string,string>,
  num_DVPendAbonoTemp map<string,string>,
  dat_VencAbonoDef map<string,string>,
  num_DVAbonoDef map<string,string>,
  dat_VencAprobAbonoTemp map<string,string>,
  num_DVAprobAbonoTemp map<string,string>,
  dat_VencAbonoTemp map<string,string>,
  num_DVAbonoTemp map<string,string>,
  dat_VencSlnado map<string,string>,
  num_DVSlnado map<string,string>,
  txt_EstadoAnt map<string,string>,
  dat_Abierto map<string,string>,
  dat_PendAbonoTemp map<string,string>,
  dat_AbonoDef map<string,string>,
  dat_AprobAbonoTemp map<string,string>,
  dat_AbonoTemp map<string,string>,
  dat_Solucionado map<string,string>,
  dat_Cerrado map<string,string>,
  txt_EstadoActual map<string,array<string>>,
  txt_FechaHora map<string,array<string>>,
  nom_Usuario map<string,array<string>>,
  num_DV map<string,string>,
  txt_Radicado map<string,string>,
  key_Estado map<string,string>,
  key_Estado_1 map<string,string>,
  key_EstadoVisible map<string,string>,
  nom_Responsable map<string,string>,
  num_DiasAcSrv map<string,string>,
  nom_Responsable_1 map<string,string>,
  txt_CedNit map<string,string>,
  txt_NomCliente map<string,string>,
  key_Producto map<string,string>,
  key_TipoReclamo map<string,string>,
  txt_NumProductoCalc map<string,string>,
  key_CampoDilig map<string,string>,
  key_PGSucError map<string,string>,
  txt_CodPGSucError map<string,string>,
  num_NroCtaTercero map<string,string>,
  key_TipoCtaTercero map<string,string>,
  txt_Historia map<string,array<string>>,
  SSREF map<string,string>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/subreclamos-old/raw';

-- COMMAND ----------

-- DBTITLE 1,Vista Aux Subreclamos Old (Visibles)
/*
--Descripción: Evalua condiciones de ocultamiento para el campo ValorTotalEnPesos
--Autor: Juan David Escobar E, Diego Quiñones.
--Fecha de creación: 22/07/2019
--Fecha de modificación: 22/07/2019
*/

DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Aux_Solicitudes_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Aux_Solicitudes_Old AS
SELECT UniversalID,
       num_VlrTotal.value AS num_VlrTotal,
       num_VlrTotal_1.value AS num_VlrTotal_1,
       num_VlrTotalDef.value AS num_VlrTotalDef,
     
        CASE WHEN (key_NumTransa.value != '' AND COALESCE(cast(key_NumTransa.value as int), 0)  >= 1) OR !array_contains(key_CamposRecl.value, '8') OR array_contains(key_CamposTransa.value, '4')  THEN 
             TRUE
           ELSE FALSE        
        END AS EsnVlrTotalVisible,

        CASE WHEN array_contains(key_CamposRecl.value, '8') OR !array_contains(key_CamposTransa.value, '4') THEN 
              TRUE
            ELSE FALSE        
        END AS EsVlrTotal_1Visible,
        
        CASE WHEN !array_contains(key_CamposRecl.value, '8') AND array_contains(key_CamposTransa.value, '4') THEN 
              TRUE
            ELSE FALSE        
        END AS EsVlrTotalDefVisible

 FROM 
  Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old;

-- COMMAND ----------

-- DBTITLE 1,Tablas Subreclamo (Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Solicitudes_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Solicitudes_Old AS
SELECT
  TblIntermedia.UniversalID AS UniversalID,
  dat_FchCreac.value AS FechaCreacion,
  substring_index(dat_FchCreac.value, ' ', 1) AS fecha_solicitudes,
  substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) AS aniopart,
  txt_NumProducto.value AS NumProducto,
  key_TipoCuenta.value AS TipoCuenta,
  CASE
    WHEN UPPER(key_TipoCuenta.value) = 'S' THEN
      'Ahorros'
    WHEN UPPER(key_TipoCuenta.value) = 'D' THEN
      'Corriente'
    ELSE
      ''
  END AS TipoCuenta_Homologado,
  num_NroCuenta.value AS NroCuenta,
  TblIntermedia.num_VlrTotal_1.value AS VlrTotal_1,
  num_VlrGMF.value AS VlrGMF,
  num_VlrComis.value AS VlrComis,
  num_VlrRever.value AS VlrRever,
  key_ClienteRazon.value AS ClienteRazon,
  key_CargaClte.value AS CargaClte,
  key_PGAreaAdmin.value AS PGAreaAdmin,
  concat_ws('\n', txt_Comentarios.value) AS Comentarios,
  txt_Autor.value AS Autor,
  txt_Creador.value AS Creador,
  TblIntermedia.num_VlrTotal.value AS VlrTotal,
  concat_ws('\n', key_PGTarjetas.value) AS PGTarjetas,
  key_Prefijo.value AS Prefijo,
  num_DAbierto.value AS DAbierto,
  key_Estado.value AS Estado,
  dat_Solucionado.value AS Solucionado,
  num_DV.value AS DV,
  txt_Radicado.value AS Radicado,
  num_DVSlnado.value AS DVSlnado,
  key_Estado_1.value AS Estado_1,
  nom_Responsable.value AS Responsable,
  nom_Responsable_1.value AS Responsable_1,
  txt_CedNit.value AS CedNit,
  txt_NomCliente.value AS NomCliente,
  key_Producto.value AS Producto,
  key_TipoReclamo.value AS TipoReclamo,
  txt_NumProductoCalc.value AS NumProductoCalc,
  key_CampoDilig.value AS CampoDilig,
  TblIntermedia.num_VlrTotalDef.value AS num_VlrTotalDef,
  CASE
    WHEN UPPER(key_CampoDilig.value) = 'C1' THEN
      'P y G Sucursal del Error'
    WHEN UPPER(key_CampoDilig.value) = 'C2' THEN
      'Cuenta de Tercero'
    WHEN UPPER(key_CampoDilig.value) = 'N' THEN 
      'Ninguno'
    ELSE
      ''
  END AS CampoDilig_Homologado,
  key_PGSucError.value AS PGSucError,
  txt_CodPGSucError.value AS CodPGSucError,
  num_NroCtaTercero.value AS NroCtaTercero,
  key_TipoCtaTercero.value AS TipoCtaTercero,
  CASE
    WHEN UPPER(key_TipoCtaTercero.value) = 'S' THEN
      'Ahorros'
    WHEN UPPER(key_TipoCtaTercero.value) = 'D' THEN
      'Corriente'
    ELSE
      ''
  END AS TipoCtaTercero_Homologado,
  concat_ws('\n', txt_Historia.value) AS Historia,
  SSREF.value AS UniversalID_Padre,
  concat(concat(txt_CedNit.value, ' - ', txt_NomCliente.value, ' - ', key_Producto.value, ' - ',key_TipoReclamo.value, key_Estado.value), ' - ',
  upper(concat(txt_CedNit.value, ' - ', txt_NomCliente.value, ' - ', key_Producto.value, ' - ',key_TipoReclamo.value, key_Estado.value)), ' - ',
  lower(concat(txt_CedNit.value, ' - ', txt_NomCliente.value, ' - ', key_Producto.value, ' - ',key_TipoReclamo.value, key_Estado.value))) AS Busqueda,
  
  --Ocultaciones Subreclamos
   CASE
        WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN ''
        WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN TblIntermedia.num_VlrTotalDef.value
        WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal_1.value || ',' || TblIntermedia.num_VlrTotalDef.value) 
        WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value, TblIntermedia.num_VlrTotal_1.value, TblIntermedia.num_VlrTotalDef.value)
        WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotal_1.value)
        WHEN (!TblAuxIntermedia.EsnVlrTotalVisible AND TblAuxIntermedia.EsVlrTotal_1Visible AND !TblAuxIntermedia.EsVlrTotalDefVisible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotalDef.value)
        WHEN (TblAuxIntermedia.EsnVlrTotalVisible AND !TblAuxIntermedia.EsVlrTotal_1Visible AND TblAuxIntermedia.EsVlrTotalDefVisible) THEN TblIntermedia.num_VlrTotal_1.value
        
    END AS ValorTotal
FROM 
      Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old AS TblIntermedia 
      INNER JOIN Process.Zdp_Lotus_Reclamos_Subreclamos_Aux_Solicitudes_Old As TblAuxIntermedia
      ON TblIntermedia.UniversalID = TblAuxIntermedia.UniversalID;

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamo (No visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Subreclamos_Solicitudes_Ocultas_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Subreclamos_Solicitudes_Ocultas_Old AS
SELECT
  UniversalID AS UniversalID,
  key_ListaCamposTransa.value AS key_ListaCamposTransa,
  key_ListaCamposRecl.value AS key_ListaCamposRecl,
  concat_ws('\n', key_Estados.value) AS key_Estados,
  txt_Guardado.value AS txt_Guardado,
  Server_Name.value AS Server_Name,
  Query_String.value AS Query_String,
  Path_Info.value AS Path_Info,
  nom_Autor.value AS nom_Autor,
  txt_CiudCod.value AS txt_CiudCod,
  Path.value AS Path,
  Miles.value AS Miles,
  txt_IdPadre.value AS txt_IdPadre,
  key_NecesitaPrefijo.value AS key_NecesitaPrefijo,
  key_EsTarjeta.value AS key_EsTarjeta,
  key_InvoTransa.value AS key_InvoTransa,
  concat_ws('\n', key_CamposTransa.value) AS key_CamposTransa,
  concat_ws('\n', key_CamposRecl.value) AS key_CamposRecl,
  key_NumTransa.value AS key_NumTransa,
  iTxt_Cia.value AS iTxt_Cia,
  txt_CodProdCia.value AS txt_CodProdCia,
  num_LongMaxNumProd.value AS num_LongMaxNumProd,
  num_LongNroCta.value AS num_LongNroCta,
  txt_SubSgmtoCliente.value AS txt_SubSgmtoCliente,
  txt_DobleDig.value AS txt_DobleDig,
  num_DobleDig.value AS num_DobleDig,
  num_DobleDig_1.value AS num_DobleDig_1,
  num_DobleDig_2.value AS num_DobleDig_2,
  num_DobleDig_3.value AS num_DobleDig_3,
  num_DobleDig_4.value AS num_DobleDig_4,
  num_DobleDig_5.value AS num_DobleDig_5,
  num_DobleDig_6.value AS num_DobleDig_6,
  num_VlrTotalDef.value AS num_VlrTotalDef,
  txt_ValorAnterior.value AS txt_ValorAnterior,
  txt_ValorAnterior_1.value AS txt_ValorAnterior_1,
  txt_ValorAnterior_2.value AS txt_ValorAnterior_2,
  num_ValorAnterior_2.value AS num_ValorAnterior_2,
  txt_ValorAnterior_3.value AS txt_ValorAnterior_3,
  txt_ValorAnterior_4.value AS txt_ValorAnterior_4,
  num_ValorAnterior_4.value AS num_ValorAnterior_4,
  txt_ValorAnterior_5.value AS txt_ValorAnterior_5,
  num_ValorAnterior_5.value AS num_ValorAnterior_5,
  txt_ValorAnterior_6.value AS txt_ValorAnterior_6,
  num_ValorAnterior_6.value AS num_ValorAnterior_6,
  key_CambioRespMS.value AS key_CambioRespMS,
  concat_ws('\n', txt_RutaBDSuc.value) AS txt_RutaBDSuc,
  concat_ws('\n', txt_RutaBDCalendar.value) AS txt_RutaBDCalendar,
  concat_ws('\n', txt_RutaBDEstilos.value) AS txt_RutaBDEstilos,
  concat_ws('\n', txt_RutaBDEmp.value) AS txt_RutaBDEmp,
  concat_ws('\n', txt_RutaListin.value) AS txt_RutaListin,
  concat_ws('\n', txt_SrvRegionales.value) AS txt_SrvRegionales,
  concat_ws('\n', txt_lstSrvResp.value) AS txt_lstSrvResp,
  concat_ws('\n', txt_lstSrvReplic.value) AS txt_lstSrvReplic,
  key_ContabDV.value AS key_ContabDV,
  num_DVEstadoAnt.value AS num_DVEstadoAnt,
  num_DVParcial.value AS num_DVParcial,
  num_DRecl.value AS num_DRecl,
  dat_VencPendAbonoTemp.value AS dat_VencPendAbonoTemp,
  num_DVPendAbonoTemp.value AS num_DVPendAbonoTemp,
  dat_VencAbonoDef.value AS dat_VencAbonoDef,
  num_DVAbonoDef.value AS num_DVAbonoDef,
  dat_VencAprobAbonoTemp.value AS dat_VencAprobAbonoTemp,
  num_DVAprobAbonoTemp.value AS num_DVAprobAbonoTemp,
  dat_VencAbonoTemp.value AS dat_VencAbonoTemp,
  num_DVAbonoTemp.value AS num_DVAbonoTemp,
  dat_VencSlnado.value AS dat_VencSlnado,
  txt_EstadoAnt.value AS txt_EstadoAnt,
  dat_Abierto.value AS dat_Abierto,
  dat_PendAbonoTemp.value AS dat_PendAbonoTemp,
  dat_AbonoDef.value AS dat_AbonoDef,
  dat_AprobAbonoTemp.value AS dat_AprobAbonoTemp,
  dat_AbonoTemp.value AS dat_AbonoTemp,
  dat_Cerrado.value AS dat_Cerrado,
  concat_ws('\n', txt_EstadoActual.value) AS txt_EstadoActual,
  concat_ws('\n', txt_FechaHora.value) AS txt_FechaHora,
  concat_ws('\n', nom_Usuario.value) AS nom_Usuario,
  key_EstadoVisible.value AS key_EstadoVisible,
  num_DiasAcSrv.value AS num_DiasAcSrv
FROM 
  Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos Subreclamo 
DROP VIEW IF EXISTS Process.Zdp_Lotus_reclamos_SubReclamos_Anexos_Old;
CREATE VIEW Process.Zdp_Lotus_reclamos_SubReclamos_Anexos_Old AS
SELECT
  W.UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/subreclamos-old/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(W.dat_FchCreac.value, ' ', 1),-4,4) AS aniopart
FROM Process.Zdp_Lotus_Reclamos_SubReclamos_TablaIntermedia_Old W 
CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN")