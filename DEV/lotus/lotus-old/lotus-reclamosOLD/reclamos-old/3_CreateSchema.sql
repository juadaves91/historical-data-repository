-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Tabla Intermedia reclamos Old
DROP TABLE IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old
( 
  UniversalID string,
  attachments array<string>,
  key_ListaCamposTransa map<string,array<string>>, 
  key_ListaCamposRecl map<string,array<string>>,
  txt_Empresa map<string,string>,
  txt_DatosAuto map<string,array<string>>,
  key_Estados map<string,array<string>>,
  key_EstadosAT map<string,array<string>>,
  key_Productos map<string,array<string>>,
  txt_CausasMR map<string,array<string>>,
  txt_CausasR map<string,array<string>>,
  DatosCli map<string,string>,
  txt_EmpReporta map<string,string>,
  txt_NomCliente map<string,string>,
  txt_FuncEmpresa map<string,string>,
  txt_DirCorr map<string,string>,
  txt_TeleCliente map<string,string>,
  txt_FaxCliente map<string,string>,
  txt_EmailCliente map<string,string>,
  Server_Name map<string,string>,
  Query_String map<string,string>,
  Path_Info map<string,string>,
  txt_CedulaCliente map<string,string>,
  iTxt_CedNit map<string,string>,
  txt_CiudCod map<string,string>,
  key_TipoDocumento map<string,string>,
  txt_Pais map<string,string>,
  txt_Estado map<string,string>,
  txt_DirPostal map<string,string>,
  txt_ApartAereo map<string,string>,
  key_RptaReclamo map<string,string>,
  Path map<string,string>,
  num_NroTransa map<string,string>,
  nom_UsuNotesEmp map<string,string>,
  txt_DescCiudadCorr map<string,string>,
  nom_UsuNotesEmp_1 map<string,string>,
  txt_TelRespGer map<string,string>,
  key_TipoPoliza map<string,string>,
  No_ map<string,string>,
  txt_NumProducto map<string,string>,
  txt_NroCompraRapp map<string,string>,
  txt_NroProductoRapp map<string,string>,
  key_ProdAfectado map<string,string>,
  txt_NumTarjDeb map<string,string>,
  txt_NumPoliza map<string,string>,
  txt_NumEncargo map<string,string>,
  key_NomSucRadica map<string,string>,
  txt_TipoPoliza map<string,string>,
  txt_NumTjtaCtaDeb map<string,string>,
  key_CiudadTransa map<string,string>,
  dat_FchTransac map<string,string>,
  key_NomSucVarios map<string,string>,
  txt_CodEstab map<string,string>,
  key_NumTransa map<string,string>,
  num_VlrTotal map<string,string>,
  key_VlrsOpTblM map<string,array<string>>,
  txt_SopRecCte map<string,string>,
  txt_SopNoRec map<string,string>,
  key_TodosSoportes map<string,string>,
  num_ValorTransa_1 map<string,string>,
  dat_FechaTransa_1 map<string,string>,
  key_OficinaTransa_1 map<string,string>,
  txt_Referencia_1 map<string,string>,
  key_Moneda_1 map<string,string>,
  key_CiudadTransa_1 map<string,string>,
  txt_NumeroCuenta_1 map<string,string>,
  num_ChequeVouch_1 map<string,string>,
  dat_FechaConsigna_1 map<string,string>,
  num_ValorTransa_2 map<string,string>,
  dat_FechaTransa_2 map<string,string>,
  key_OficinaTransa_2 map<string,string>,
  txt_Referencia_2 map<string,string>,
  key_Moneda_2 map<string,string>,
  key_CiudadTransa_2 map<string,string>,
  txt_NumeroCuenta_2 map<string,string>,
  num_ChequeVouch_2 map<string,string>,
  dat_FechaConsigna_2 map<string,string>,
  num_ValorTransa_3 map<string,string>,
  dat_FechaTransa_3 map<string,string>,
  key_OficinaTransa_3 map<string,string>,
  txt_Referencia_3 map<string,string>,
  key_Moneda_3 map<string,string>,
  key_CiudadTransa_3 map<string,string>,
  txt_NumeroCuenta_3 map<string,string>,
  num_ChequeVouch_3 map<string,string>,
  dat_FechaConsigna_3 map<string,string>,
  num_ValorTransa_4 map<string,string>,
  dat_FechaTransa_4 map<string,string>,
  key_OficinaTransa_4 map<string,string>,
  txt_Referencia_4 map<string,string>,
  key_Moneda_4 map<string,string>,
  key_CiudadTransa_4 map<string,string>,
  txt_NumeroCuenta_4 map<string,string>,
  num_ChequeVouch_4 map<string,string>,
  dat_FechaConsigna_4 map<string,string>,
  num_ValorTransa_5 map<string,string>,
  dat_FechaTransa_5 map<string,string>,
  key_OficinaTransa_5 map<string,string>,
  txt_Referencia_5 map<string,string>,
  key_Moneda_5 map<string,string>,
  key_CiudadTransa_5 map<string,string>,
  txt_NumeroCuenta_5 map<string,string>,
  num_ChequeVouch_5 map<string,string>,
  dat_FechaConsigna_5 map<string,string>,
  rch_Pantallas map<string,array<string>>,
  rch_PantallaSln map<string,array<string>>,
  key_TipoTarjeta map<string,string>,
  key_TipoReclamo map<string,string>,
  num_VlrTotalD map<string,string>,
  Miles map<string,string>,
  R5 map<string,string>,
  Guardo map<string,string>,
  CambioEstado map<string,string>,
  txt_Causa map<string,string>,
  txt_ProdAnterior map<string,string>,
  txt_ReclAnterior map<string,string>,
  txt_CambioProductoManual map<string,string>,
  iTxt_Cia map<string,string>,
  iTxt_Prod map<string,string>,
  iTxt_Clasif map<string,string>,
  iTxt_Recl map<string,string>,
  iTxt_NTrans map<string,string>,
  nom_Autor map<string,string>,
  txt_Autor map<string,string>,
  txt_Creador map<string,string>,
  dat_FchCreac map<string,string>,
  key_NomSucCaptura map<string,string>,
  key_SucursalFilial map<string,string>,
  key_ReportadoPor map<string,string>,
  key_OrigenReporte map<string,string>,
  num_AbonoTemp map<string,string>,
  num_AbonoDefinit map<string,string>,
  key_ResponError map<string,string>,
  txt_NomRespError map<string,string>,
  key_ClienteRazon map<string,string>,
  key_CausaNoAbono map<string,string>,
  key_MedioRpta map<string,string>,
  txt_ComentRapp map<string,string>,
  txt_Comentarios map<string,array<string>>,
  rch_RptaCliente map<string,array<string>>,
  rch_Anexos map<string,string>,
  txt_Guardado map<string,string>,
  txt_EsNuevo map<string,string>,
  key_ReclMigrado map<string,string>,
  txt_NotifEnv map<string,array<string>>,
  key_EsTarjeta map<string,string>,
  key_NecesitaPrefijo map<string,string>,
  num_LongMaxNumProd map<string,string>,
  key_TablaMult map<string,string>,
  key_InvoTransa map<string,string>,
  key_CamposTransa map<string,array<string>>,
  key_CamposRecl map<string,array<string>>,
  txt_ProdRecl map<string,string>,
  dat_Fecha map<string,string>,
  txt_SusceptAbono map<string,string>,
  txt_ReqDocAbono map<string,string>,
  txt_SubsegmtosAbono map<string,array<string>>,
  txt_CrearSub map<string,string>,
  key_CambioRespM map<string,string>,
  txt_RutaBDSuc map<string,array<string>>,
  txt_RutaBDCalendar map<string,array<string>>,
  txt_RutaBDEstilos map<string,array<string>>,
  txt_RutaBDEmp map<string,array<string>>,
  txt_RutaListin map<string,array<string>>,
  txt_SrvRegionales map<string,array<string>>,
  txt_GciaOp map<string,string>,
  nom_Responsable map<string,array<string>>,
  txt_Gerenciado map<string,string>,
  txt_lstSrvResp map<string,array<string>>,
  txt_lstSrvReplic map<string,array<string>>,
  key_ContabDV map<string,string>,
  num_DVEstadoAnt map<string,string>,
  num_DVParcial map<string,string>,
  num_DRecl map<string,string>,
  num_DAbierto map<string,string>,
  num_DPendiente map<string,string>,
  num_DMalRad map<string,string>,
  num_DSlnado map<string,string>,
  dat_VencPendiente map<string,string>,
  num_DVPendiente map<string,string>,
  dat_VencMalRad map<string,string>,
  num_DVMalRad map<string,string>,
  dat_VencFraudeTrj map<string,string>,
  num_DVFraudeTrj map<string,string>,
  dat_VencSlnado map<string,string>,
  num_DVSlnado map<string,string>,
  dat_VencSlnadoCarta map<string,string>,
  num_DVSlnadoCarta map<string,string>,
  dat_VencPendAjuste map<string,string>,
  num_DVPendAjuste map<string,string>,
  dat_VencVerif map<string,string>,
  num_DVVerif map<string,string>,
  dat_VencInvest map<string,string>,
  num_DVInvest map<string,string>,
  dat_VencRedes map<string,string>,
  num_DVRedes map<string,string>,
  dat_VencBancos map<string,string>,
  num_DVBancos map<string,string>,
  dat_VencTrmInterno map<string,string>,
  num_DVTrmInterno map<string,string>,
  dat_VencSolicRptaEsc map<string,string>,
  num_DVSolicRptaEsc map<string,string>,
  dat_VencClteNoCont map<string,string>,
  num_DVClteNoCont map<string,string>,
  dat_VencCartaEnRev map<string,string>,
  num_DVCartaEnRev map<string,string>,
  dat_VencAboTempA map<string,string>,
  num_DVAboTempA map<string,string>,
  dat_VencAboTempP map<string,string>,
  num_DVAboTempP map<string,string>,
  txt_EstadoAnt map<string,string>,
  dat_Pendiente map<string,string>,
  dat_Abierto map<string,string>,
  dat_PendAjuste map<string,string>,
  dat_Verif map<string,string>,
  dat_Invest map<string,string>,
  dat_Redes map<string,string>,
  dat_Bancos map<string,string>,
  dat_TrmInterno map<string,string>,
  dat_AbonoTemp map<string,string>,
  dat_MalRadicado map<string,string>,
  dat_FraudeTrj map<string,string>,
  dat_Reabierto map<string,string>,
  dat_Solucionado map<string,string>,
  dat_SolucionadoCarta map<string,string>,
  dat_TramiteExt map<string,string>,
  dat_Cerrado map<string,string>,
  dat_TramiteAbono map<string,string>,
  dat_CerradoSucTel map<string,string>,
  dat_CerradoTele map<string,string>,
  dat_SolRptaEsc map<string,string>,
  dat_ClteNoCont map<string,string>,
  dat_CartaEnRev map<string,string>,
  dat_AboTempA map<string,string>,
  dat_AboTempP map<string,string>,
  key_CartaCreada map<string,string>,
  txt_ComComercial map<string,string>,
  txt_AsuntoCarta map<string,string>,
  txt_EstadoActual map<string,array<string>>,
  txt_FechaHora map<string,array<string>>,
  nom_Usuario map<string,array<string>>,
  txt_Cedula map<string,string>,
  txt_Nomina map<string,string>,
  txt_Condicion map<string,string>,
  txt_IndPendRapp map<string,string>,
  num_DV map<string,string>,
  numV_DVClteNoCont map<string,string>,
  numV_DVCartaEnRev map<string,string>,
  numV_DVAboTempA map<string,string>,
  numV_DVAboTempP map<string,string>,
  key_Soportes map<string,string>,
  key_Estado map<string,string>,
  key_Estado_1 map<string,string>,
  key_EstadoVisible map<string,string>,
  num_DiasAcSrv map<string,string>,
  nom_Responsable_1 map<string,string>,
  nom_UsuCierra map<string,string>,
  txt_CedNit map<string,string>,
  txt_TipoCliente map<string,string>,
  txt_SubSgmtoCliente map<string,string>,
  txt_GteCuenta map<string,string>,
  txt_CiudadCorr map<string,string>,
  txt_DptoCorr map<string,string>,
  txt_IdDireccion map<string,string>,
  nom_RespGer map<string,string>,
  num_TpoEstSol map<string,string>,
  dat_FchAproxSol map<string,string>,
  txt_Cia map<string,string>,
  key_Producto map<string,string>,
  key_Prefijo map<string,string>,
  num_VlrTotal_1 map<string,string>,
  num_VlrTotalD_1 map<string,string>,
  txt_MensajeAyuda map<string,array<string>>,
  txt_MensajeSoportes map<string,array<string>>,
  txt_NomOpTblM map<string,array<string>>,
  txt_Historia map<string,array<string>>,
  txt_RgnCaptura map<string,string>,
  txt_ZonaCaptura map<string,string>,
  txt_ClteGer map<string,string>,
  txt_ClteColombia map<string,string>,
  txt_SubprocRecl map<string,string>,
  num_ValorTransa_6 map<string,string>,
  dat_FechaTransa_6 map<string,string>,
  key_OficinaTransa_6 map<string,string>,
  txt_Referencia_6 map<string,string>,
  key_Moneda_6 map<string,string>,
  key_CiudadTransa_6 map<string,string>,
  txt_NumeroCuenta_6 map<string,string>,
  num_ChequeVouch_6 map<string,string>,
  dat_FechaConsigna_6 map<string,string>,
  num_ValorTransa_7 map<string,string>,
  dat_FechaTransa_7 map<string,string>,
  key_OficinaTransa_7 map<string,string>,
  txt_Referencia_7 map<string,string>,
  key_Moneda_7 map<string,string>,
  key_CiudadTransa_7 map<string,string>,
  txt_NumeroCuenta_7 map<string,string>,
  num_ChequeVouch_7 map<string,string>,
  dat_FechaConsigna_7 map<string,string>,
  num_ValorTransa_8 map<string,string>,
  dat_FechaTransa_8 map<string,string>,
  key_OficinaTransa_8 map<string,string>,
  txt_Referencia_8 map<string,string>,
  key_Moneda_8 map<string,string>,
  key_CiudadTransa_8 map<string,string>,
  txt_NumeroCuenta_8 map<string,string>,
  num_ChequeVouch_8 map<string,string>,
  dat_FechaConsigna_8 map<string,string>,
  num_ValorTransa_9 map<string,string>,
  dat_FechaTransa_9 map<string,string>,
  key_OficinaTransa_9 map<string,string>,
  txt_Referencia_9 map<string,string>,
  key_Moneda_9 map<string,string>,
  key_CiudadTransa_9 map<string,string>,
  txt_NumeroCuenta_9 map<string,string>,
  num_ChequeVouch_9 map<string,string>,
  dat_FechaConsigna_9 map<string,string>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-reclamos/reclamos-old/raw';

-- COMMAND ----------

-- DBTITLE 1,Vista Aux Reclamos Old (Visbles)
/*
--Descripción: Evalua condiciones de ocultamiento para los campos ValorTotalEnPesos y ValorTotalEnDolares
--Autor: Juan David Escobar E, Diego Quiñones.
--Fecha de creación: 19/07/2019
--Fecha de modificación: 19/07/2019
*/

DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_Aux_Solicitudes_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_Aux_Solicitudes_Old AS
SELECT UniversalID,
       num_VlrTotal.value AS num_VlrTotal,
       num_VlrTotal_1.value AS num_VlrTotal_1,
       num_VlrTotalD.value AS num_VlrTotalD,
       num_VlrTotalD_1.value AS num_VlrTotalD_1,
     
        CASE WHEN (key_NumTransa.value != '' AND COALESCE(cast(key_NumTransa.value as int), 0)  >= 1) OR !array_contains(key_CamposRecl.value, '8') OR array_contains(key_CamposTransa.value, '4')  THEN 
             TRUE
           ELSE FALSE        
        END AS EsnVlrTotalVlrTotalDVisible,

        CASE WHEN array_contains(key_CamposRecl.value, '8') OR !array_contains(key_CamposTransa.value, '4') THEN 
              TRUE
            ELSE FALSE        
        END AS EsVlrTotal_1VlrTotalD_1Visible 

 FROM 
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old;

-- COMMAND ----------

-- DBTITLE 1,Tablas Reclamos Old (Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Old AS
SELECT
  TblIntermedia.UniversalID As UniversalID,
  TblIntermedia.dat_FchCreac.value AS FchCreac,
  substring_index(TblIntermedia.dat_FchCreac.value, ' ', 1) AS fecha_solicitudes,
  substr(substring_index(TblIntermedia.dat_FchCreac.value, ' ', 1),-4,4) AS aniopart,
  TblIntermedia.txt_CedulaCliente.value AS CedulaCliente,
  TblIntermedia.txt_NomCliente.value AS NomCliente,
  TblIntermedia.txt_FuncEmpresa.value AS FuncionarioEmpresa,
  TblIntermedia.txt_DirCorr.value AS DirCorrospondencia,
  TblIntermedia.txt_TeleCliente.value AS TelefonoCliente,
  TblIntermedia.txt_FaxCliente.value AS Fax,
  TblIntermedia.txt_EmailCliente.value AS Email,
  TblIntermedia.key_TipoDocumento.value AS TipoDocumento,
  TblIntermedia.txt_Pais.value AS Pais,
  TblIntermedia.txt_Estado.value AS Estado,
  TblIntermedia.txt_DirPostal.value AS DirPostal,
  TblIntermedia.txt_ApartAereo.value AS ApartadoAereo,
  TblIntermedia.key_RptaReclamo.value AS RespuesptaReclamo,
  TblIntermedia.txt_DescCiudadCorr.value AS CiudadCorrespondencia,
  TblIntermedia.txt_TelRespGer.value AS TelResponsable,
  TblIntermedia.txt_NumProducto.value AS NumProducto,
  TblIntermedia.txt_NroCompraRapp.value AS NroCodCompra,
  TblIntermedia.txt_NroProductoRapp.value AS NroProducto,
  TblIntermedia.num_DVPendiente.value AS num_DVPendiente,
  TblIntermedia.num_DVMalRad.value AS num_DVMalRad,
  TblIntermedia.num_DVFraudeTrj.value AS num_DVFraudeTrj,
  TblIntermedia.num_DVSlnado.value AS num_DVSlnado,
  TblIntermedia.num_DVSlnadoCarta.value AS num_DVSlnadoCarta,
  TblIntermedia.num_DVPendAjuste.value AS num_DVPendAjuste,
  TblIntermedia.num_DVVerif.value AS num_DVVerif,
  TblIntermedia.num_DVInvest.value AS num_DVInvest,
  TblIntermedia.num_DVRedes.value AS num_DVRedes,
  TblIntermedia.num_DVBancos.value AS num_DVBancos,
  TblIntermedia.num_DVTrmInterno.value AS num_DVTrmInterno,
  TblIntermedia.num_DVSolicRptaEsc.value AS num_DVSolicRptaEsc,
  TblIntermedia.num_DVClteNoCont.value AS num_DVClteNoCont,
  TblIntermedia.num_DVCartaEnRev.value AS num_DVCartaEnRev,
  TblIntermedia.num_DVAboTempA.value AS num_DVAboTempA,
  TblIntermedia.num_DVAboTempP.value AS num_DVAboTempP,
  TblIntermedia.txt_NumTarjDeb.value AS NumTarjDeb,
  TblIntermedia.txt_NumPoliza.value AS NumPoliza,
  TblIntermedia.txt_NumEncargo.value AS NumEncargo,
  TblIntermedia.key_NomSucRadica.value AS NomSucRadica,
  TblIntermedia.num_DRecl.value AS num_DRecl,
  TblIntermedia.txt_TipoPoliza.value AS TipoPoliza,
  TblIntermedia.txt_NumTjtaCtaDeb.value AS NumTjtaCtaDeb,
  TblIntermedia.key_CiudadTransa.value AS CiudadTransa,
  TblIntermedia.dat_FchTransac.value AS FchTransac,
  TblIntermedia.key_NomSucVarios.value AS NomSucVarios,
  TblIntermedia.txt_CodEstab.value AS CodEstab,
  TblIntermedia.key_NumTransa.value AS NumTransa,
  TblIntermedia.num_VlrTotal.value AS ValorTotal,
  concat_ws('\n', TblIntermedia.key_VlrsOpTblM.value) AS DatoAdicionalesReclamo,
  TblIntermedia.txt_SopRecCte.value AS SopRecibido,
  TblIntermedia.txt_SopNoRec.value AS SopNoRec,
  TblIntermedia.key_TodosSoportes.value AS TodosSoportes,
  TblIntermedia.num_ValorTransa_1.value AS ValorTransa_1,
  TblIntermedia.dat_FechaTransa_1.value AS FechaTransa_1,
  TblIntermedia.key_OficinaTransa_1.value AS OficinaTransa_1,
  TblIntermedia.txt_Referencia_1.value AS Referencia_1,
  TblIntermedia.key_Moneda_1.value AS Moneda_1,
  TblIntermedia.key_CiudadTransa_1.value AS CiudadTransa_1,
  TblIntermedia.txt_NumeroCuenta_1.value AS NumeroCuenta_1,
  TblIntermedia.num_ChequeVouch_1.value AS ChequeVouch_1,
  TblIntermedia.dat_FechaConsigna_1.value AS FechaConsigna_1,
  TblIntermedia.num_ValorTransa_2.value AS ValorTransa_2,
  TblIntermedia.dat_FechaTransa_2.value AS FechaTransa_2,
  TblIntermedia.key_OficinaTransa_2.value AS OficinaTransa_2,
  TblIntermedia.txt_Referencia_2.value AS Referencia_2,
  TblIntermedia.key_Moneda_2.value AS Moneda_2,
  TblIntermedia.key_CiudadTransa_2.value AS CiudadTransa_2,
  TblIntermedia.txt_NumeroCuenta_2.value AS NumeroCuenta_2,
  TblIntermedia.num_ChequeVouch_2.value AS ChequeVouch_2,
  TblIntermedia.dat_FechaConsigna_2.value AS FechaConsigna_2,
  TblIntermedia.num_ValorTransa_3.value AS ValorTransa_3,
  TblIntermedia.dat_FechaTransa_3.value AS FechaTransa_3,
  TblIntermedia.key_OficinaTransa_3.value AS OficinaTransa_3,
  TblIntermedia.txt_Referencia_3.value AS Referencia_3,
  TblIntermedia.key_Moneda_3.value AS Moneda_3,
  TblIntermedia.key_CiudadTransa_3.value AS CiudadTransa_3,
  TblIntermedia.txt_NumeroCuenta_3.value AS NumeroCuenta_3,
  TblIntermedia.num_ChequeVouch_3.value AS ChequeVouch_3,
  TblIntermedia.dat_FechaConsigna_3.value AS FechaConsigna_3,
  TblIntermedia.num_ValorTransa_4.value AS ValorTransa_4,
  TblIntermedia.dat_FechaTransa_4.value AS FechaTransa_4,
  TblIntermedia.key_OficinaTransa_4.value AS OficinaTransa_4,
  TblIntermedia.txt_Referencia_4.value AS Referencia_4,
  TblIntermedia.key_Moneda_4.value AS Moneda_4,
  TblIntermedia.key_CiudadTransa_4.value AS CiudadTransa_4,
  TblIntermedia.txt_NumeroCuenta_4.value AS NumeroCuenta_4,
  TblIntermedia.num_ChequeVouch_4.value AS ChequeVouch_4,
  TblIntermedia.dat_FechaConsigna_4.value AS FechaConsigna_4,
  TblIntermedia.num_ValorTransa_5.value AS ValorTransa_5,
  TblIntermedia.dat_FechaTransa_5.value AS FechaTransa_5,
  TblIntermedia.key_OficinaTransa_5.value AS OficinaTransa_5,
  TblIntermedia.txt_Referencia_5.value AS Referencia_5,
  TblIntermedia.key_Moneda_5.value AS Moneda_5,
  TblIntermedia.key_CiudadTransa_5.value AS CiudadTransa_5,
  TblIntermedia.txt_NumeroCuenta_5.value AS NumeroCuenta_5,
  TblIntermedia.num_ChequeVouch_5.value AS ChequeVouch_5,
  TblIntermedia.dat_FechaConsigna_5.value AS FechaConsigna_5,
  concat_ws('\n', TblIntermedia.rch_Pantallas.value) AS Pantallas,
  concat_ws('\n', TblIntermedia.rch_PantallaSln.value) AS PantallaSln,
  TblIntermedia.key_TipoTarjeta.value AS TipoTarjeta,
  TblIntermedia.key_TipoReclamo.value AS TipoReclamo,
  TblIntermedia.num_VlrTotalD.value AS num_VlrTotalD,
  TblIntermedia.txt_Autor.value AS Autor,
  TblIntermedia.txt_Creador.value AS Creador,
  TblIntermedia.dat_Fecha.value AS Fecha_cerrado,
  TblIntermedia.key_NomSucCaptura.value AS NomSucCaptura,
  TblIntermedia.key_SucursalFilial.value AS SucursalFilial,
  TblIntermedia.key_ReportadoPor.value AS ReportadoPor,
  TblIntermedia.key_OrigenReporte.value AS OrigenReporte,
  TblIntermedia.num_AbonoTemp.value AS num_AbonoTemp,
  TblIntermedia.num_AbonoDefinit.value AS num_AbonoDefinit,
  TblIntermedia.key_ResponError.value AS ResponError,
  TblIntermedia.txt_NomRespError.value AS NomRespError,
  TblIntermedia.key_ClienteRazon.value AS ClienteRazon,
  TblIntermedia.key_CausaNoAbono.value AS CausaNoAbono,
  TblIntermedia.key_MedioRpta.value AS MedioRpta,
  TblIntermedia.txt_ComentRapp.value AS ComentRapp,
  concat_ws('\n', TblIntermedia.txt_Comentarios.value) AS Comentarios,
  concat_ws('\n', TblIntermedia.rch_RptaCliente.value) AS RptaCliente,
  TblIntermedia.rch_Anexos.value AS Anexos,
  TblIntermedia.num_DV.value AS num_DV,
  TblIntermedia.numV_DVClteNoCont.value AS numV_DVClteNoCont,
  TblIntermedia.numV_DVCartaEnRev.value AS numV_DVCartaEnRev,
  TblIntermedia.numV_DVAboTempA.value AS numV_DVAboTempA,
  TblIntermedia.numV_DVAboTempP.value AS numV_DVAboTempP,
  TblIntermedia.key_Estado_1.value AS Estado_1,
  TblIntermedia.nom_Responsable_1.value AS nom_Responsable_1,
  TblIntermedia.nom_UsuCierra.value AS nom_UsuCierra,
  TblIntermedia.txt_CedNit.value AS CedNit,
  TblIntermedia.txt_TipoCliente.value AS TipoCliente,
  TblIntermedia.txt_SubSgmtoCliente.value AS SubSgmtoCliente,
  TblIntermedia.txt_CiudadCorr.value AS CiudadCorr,
  TblIntermedia.txt_DptoCorr.value AS DptoCorr,
  TblIntermedia.txt_IdDireccion.value AS IdDireccion,
  TblIntermedia.nom_RespGer.value AS nom_RespGer,
  TblIntermedia.num_TpoEstSol.value AS num_TpoEstSol,
  TblIntermedia.dat_FchAproxSol.value AS dat_FchAproxSol,
  TblIntermedia.txt_Cia.value AS Companiia,
  TblIntermedia.key_Producto.value AS Producto,
  TblIntermedia.num_VlrTotal_1.value AS num_VlrTotal_1,
  TblIntermedia.num_VlrTotalD_1.value AS num_VlrTotalD_1,
  concat_ws('\n', TblIntermedia.txt_MensajeAyuda.value) AS MensajeAyuda,
  concat_ws('\n', TblIntermedia.txt_MensajeSoportes.value) AS MensajeSoportes,
  concat_ws('\n', TblIntermedia.txt_NomOpTblM.value) AS NomOpTblM,
  concat_ws('\n', TblIntermedia.txt_Historia.value) AS Historia,
  TblIntermedia.txt_RgnCaptura.value AS RgnCaptura,
  TblIntermedia.txt_ZonaCaptura.value AS ZonaCaptura,
  TblIntermedia.txt_ClteGer.value AS ClteGer,
  CASE
    WHEN LENGTH(COALESCE(TblIntermedia.txt_clteger.value,'')) = 0 AND LENGTH(TblIntermedia.txt_CedNit.value) = 0 THEN ''
    WHEN LENGTH(COALESCE(TblIntermedia.txt_clteger.value,'')) = 0 AND LENGTH(TblIntermedia.txt_CedNit.value) > 0 THEN 'No'
    WHEN UPPER(TblIntermedia.txt_clteger.value) = 'G' AND LENGTH(TblIntermedia.txt_CedNit.value) >= 0  THEN 'Si'
    WHEN UPPER(TblIntermedia.txt_clteger.value) = 'N' AND LENGTH(TblIntermedia.txt_CedNit.value) >= 0  THEN 'No'
  END AS ClteGer_homologado,
  TblIntermedia.txt_ClteColombia.value AS ClteColombia,
  CASE
    WHEN LENGTH(COALESCE(TblIntermedia.txt_ClteColombia.value,'')) = 0 AND LENGTH(TblIntermedia.txt_CedNit.value) = 0 THEN ''
    WHEN LENGTH(COALESCE(TblIntermedia.txt_ClteColombia.value,'')) = 0 AND LENGTH(TblIntermedia.txt_CedNit.value) > 0 THEN 'No'
    WHEN UPPER(TblIntermedia.txt_ClteColombia.value) = 'CC' AND LENGTH(TblIntermedia.txt_CedNit.value) >= 0  THEN 'Si'
    WHEN UPPER(TblIntermedia.txt_ClteColombia.value) = 'N' AND LENGTH(TblIntermedia.txt_CedNit.value) >= 0  THEN 'No'
  END AS ClteColombia_homologado,
  TblIntermedia.txt_SubprocRecl.value AS SubprocRecl,
  TblIntermedia.num_ValorTransa_6.value AS ValorTransa_6,
  TblIntermedia.dat_FechaTransa_6.value AS FechaTransa_6,
  TblIntermedia.key_OficinaTransa_6.value AS OficinaTransa_6,
  TblIntermedia.txt_Referencia_6.value AS Referencia_6,
  TblIntermedia.key_Moneda_6.value AS Moneda_6,
  TblIntermedia.key_CiudadTransa_6.value AS CiudadTransa_6,
  TblIntermedia.txt_NumeroCuenta_6.value AS NumeroCuenta_6,
  TblIntermedia.num_ChequeVouch_6.value AS ChequeVouch_6,
  TblIntermedia.dat_FechaConsigna_6.value AS FechaConsigna_6,
  TblIntermedia.num_ValorTransa_7.value AS ValorTransa_7,
  TblIntermedia.dat_FechaTransa_7.value AS FechaTransa_7,
  TblIntermedia.key_OficinaTransa_7.value AS OficinaTransa_7,
  TblIntermedia.txt_Referencia_7.value AS Referencia_7,
  TblIntermedia.key_Moneda_7.value AS Moneda_7,
  TblIntermedia.key_CiudadTransa_7.value AS CiudadTransa_7,
  TblIntermedia.txt_NumeroCuenta_7.value AS NumeroCuenta_7,
  TblIntermedia.num_ChequeVouch_7.value AS ChequeVouch_7,
  TblIntermedia.dat_FechaConsigna_7.value AS FechaConsigna_7,
  TblIntermedia.num_ValorTransa_8.value AS ValorTransa_8,
  TblIntermedia.dat_FechaTransa_8.value AS FechaTransa_8,
  TblIntermedia.key_OficinaTransa_8.value AS OficinaTransa_8,
  TblIntermedia.txt_Referencia_8.value AS Referencia_8,
  TblIntermedia.key_Moneda_8.value AS Moneda_8,
  TblIntermedia.key_CiudadTransa_8.value AS CiudadTransa_8,
  TblIntermedia.txt_NumeroCuenta_8.value AS NumeroCuenta_8,
  TblIntermedia.num_ChequeVouch_8.value AS ChequeVouch_8,
  TblIntermedia.dat_FechaConsigna_8.value AS FechaConsigna_8,
  TblIntermedia.num_ValorTransa_9.value AS ValorTransa_9,
  TblIntermedia.dat_FechaTransa_9.value AS FechaTransa_9,
  TblIntermedia.key_OficinaTransa_9.value AS OficinaTransa_9,
  TblIntermedia.txt_Referencia_9.value AS Referencia_9,
  TblIntermedia.key_Moneda_9.value AS Moneda_9,
  TblIntermedia.key_CiudadTransa_9.value AS CiudadTransa_9,
  TblIntermedia.txt_NumeroCuenta_9.value AS NumeroCuenta_9,
  TblIntermedia.num_ChequeVouch_9.value AS ChequeVouch_9,
  TblIntermedia.dat_FechaConsigna_9.value AS FechaConsigna_9,
  TblIntermedia.txt_GteCuenta.value AS GteCuenta,
  TblIntermedia.num_DAbierto.value AS num_DAbierto,
  TblIntermedia.dat_Solucionado.value AS dat_Solucionado,
  TblIntermedia.key_Prefijo.value AS key_Prefijo,
  concat(concat(TblIntermedia.txt_CedNit.value, ' - ' , TblIntermedia.key_TipoReclamo.value, ' - ', TblIntermedia.key_Producto.value, ' - ', TblIntermedia.num_AbonoDefinit.value, ' - ', TblIntermedia.txt_NomCliente.value), ' - ',
  upper(concat(TblIntermedia.txt_CedNit.value, ' - ' , TblIntermedia.key_TipoReclamo.value, ' - ', TblIntermedia.key_Producto.value, ' - ', TblIntermedia.num_AbonoDefinit.value, ' - ', TblIntermedia.txt_NomCliente.value)), ' - ',
  lower(concat(TblIntermedia.txt_CedNit.value, ' - ' , TblIntermedia.key_TipoReclamo.value, ' - ', TblIntermedia.key_Producto.value, ' - ', TblIntermedia.num_AbonoDefinit.value, ' - ', TblIntermedia.txt_NomCliente.value))) AS Busqueda,
 --Ocultaciones
   CASE
         WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN ''
         WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotal.value
         WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotal_1.value
         WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN concat_ws('\n', TblIntermedia.num_VlrTotal.value || ',' || TblIntermedia.num_VlrTotal_1.value)        
       END AS ValorTotalEnPesos,
       
   CASE
     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN ''
     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotalD.value
     WHEN (TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN TblIntermedia.num_VlrTotalD_1.value
     WHEN (!TblAuxIntermedia.EsnVlrTotalVlrTotalDVisible AND !TblAuxIntermedia.EsVlrTotal_1VlrTotalD_1Visible) THEN concat_ws('\n', TblIntermedia.num_VlrTotalD.value || ',' || TblIntermedia.num_VlrTotalD_1.value)        
   END AS ValorTotalEnDolares

  
 FROM Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old AS TblIntermedia 
     INNER JOIN Process.Zdp_Lotus_Reclamos_Reclamos_Aux_Solicitudes_Old As TblAuxIntermedia
     ON TblIntermedia.UniversalID = TblAuxIntermedia.UniversalID;
  

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos Old (No visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Ocultas_Old;
CREATE VIEW Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Ocultas_Old AS
SELECT
  UniversalID AS UniversalID,
  concat_ws('\n', key_ListaCamposTransa.value) AS key_ListaCamposTransa,
  concat_ws('\n', key_ListaCamposRecl.value) AS key_ListaCamposRecl,
  txt_Empresa.value AS txt_Empresa,
  concat_ws('\n', txt_DatosAuto.value) AS txt_DatosAuto,
  concat_ws('\n', key_Estados.value) AS key_Estados,
  concat_ws('\n', key_EstadosAT.value) AS key_EstadosAT,
  concat_ws('\n', key_Productos.value) AS key_Productos,
  concat_ws('\n', txt_CausasMR.value) AS txt_CausasMR,
  concat_ws('\n', txt_CausasR.value) AS txt_CausasR,
  DatosCli.value AS DatosCli,
  txt_EmpReporta.value AS txt_EmpReporta,
  Server_Name.value AS Server_Name,
  Query_String.value AS Query_String,
  Path_Info.value AS Path_Info,
  iTxt_CedNit.value AS iTxt_CedNit,
  txt_CiudCod.value AS txt_CiudCod,
  Path.value AS Path,
  num_NroTransa.value AS num_NroTransa,
  nom_UsuNotesEmp.value AS nom_UsuNotesEmp,
  nom_UsuNotesEmp_1.value AS nom_UsuNotesEmp_1,
  key_TipoPoliza.value AS key_TipoPoliza,
  No_.value AS No_,
  key_ProdAfectado.value AS key_ProdAfectado,
  Miles.value AS Miles,
  R5.value AS R5,
  Guardo.value AS Guardo,
  CambioEstado.value AS CambioEstado,
  txt_Causa.value AS txt_Causa,
  txt_ProdAnterior.value AS txt_ProdAnterior,
  txt_ReclAnterior.value AS txt_ReclAnterior,
  txt_CambioProductoManual.value AS txt_CambioProductoManual,
  iTxt_Cia.value AS iTxt_Cia,
  iTxt_Prod.value AS iTxt_Prod,
  iTxt_Clasif.value AS iTxt_Clasif,
  iTxt_Recl.value AS iTxt_Recl,
  iTxt_NTrans.value AS iTxt_NTrans,
  nom_Autor.value AS nom_Autor,
  txt_Guardado.value AS txt_Guardado,
  txt_EsNuevo.value AS txt_EsNuevo,
  key_ReclMigrado.value AS key_ReclMigrado,
  concat_ws('\n', txt_NotifEnv.value) AS txt_NotifEnv,
  key_EsTarjeta.value AS key_EsTarjeta,
  key_NecesitaPrefijo.value AS key_NecesitaPrefijo,
  num_LongMaxNumProd.value AS num_LongMaxNumProd,
  key_TablaMult.value AS key_TablaMult,
  key_InvoTransa.value AS key_InvoTransa,
  concat_ws('\n', key_CamposTransa.value) AS key_CamposTransa,
  concat_ws('\n', key_CamposRecl.value) AS key_CamposRecl,
  txt_ProdRecl.value AS txt_ProdRecl,
  txt_SusceptAbono.value AS txt_SusceptAbono,
  txt_ReqDocAbono.value AS txt_ReqDocAbono,
  concat_ws('\n', txt_SubsegmtosAbono.value) AS txt_SubsegmtosAbono,
  txt_CrearSub.value AS txt_CrearSub,
  key_CambioRespM.value AS key_CambioRespM,
  concat_ws('\n', txt_RutaBDSuc.value) AS txt_RutaBDSuc,
  concat_ws('\n', txt_RutaBDCalendar.value) AS txt_RutaBDCalendar,
  concat_ws('\n', txt_RutaBDEstilos.value) AS txt_RutaBDEstilos,
  concat_ws('\n', txt_RutaBDEmp.value) AS txt_RutaBDEmp,
  concat_ws('\n', txt_RutaListin.value) AS txt_RutaListin,
  concat_ws('\n', txt_SrvRegionales.value) AS txt_SrvRegionales,
  txt_GciaOp.value AS txt_GciaOp,
  concat_ws('\n', nom_Responsable.value) AS nom_Responsable,
  txt_Gerenciado.value AS txt_Gerenciado,
  concat_ws('\n', txt_lstSrvResp.value) AS txt_lstSrvResp,
  concat_ws('\n', txt_lstSrvReplic.value) AS txt_lstSrvReplic,
  key_ContabDV.value AS key_ContabDV,
  num_DVEstadoAnt.value AS num_DVEstadoAnt,
  num_DVParcial.value AS num_DVParcial,
  num_DPendiente.value AS num_DPendiente,
  num_DMalRad.value AS num_DMalRad,
  num_DSlnado.value AS num_DSlnado,
  dat_VencPendiente.value AS dat_VencPendiente,
  dat_VencMalRad.value AS dat_VencMalRad,
  dat_VencFraudeTrj.value AS dat_VencFraudeTrj,
  dat_VencSlnado.value AS dat_VencSlnado,
  dat_VencSlnadoCarta.value AS dat_VencSlnadoCarta,
  dat_VencPendAjuste.value AS dat_VencPendAjuste,
  dat_VencVerif.value AS dat_VencVerif,
  dat_VencInvest.value AS dat_VencInvest,
  dat_VencRedes.value AS dat_VencRedes,
  dat_VencBancos.value AS dat_VencBancos,
  dat_VencTrmInterno.value AS dat_VencTrmInterno,
  dat_VencSolicRptaEsc.value AS dat_VencSolicRptaEsc,
  dat_VencClteNoCont.value AS dat_VencClteNoCont,
  dat_VencCartaEnRev.value AS dat_VencCartaEnRev,
  dat_VencAboTempA.value AS dat_VencAboTempA,
  dat_VencAboTempP.value AS dat_VencAboTempP,
  txt_EstadoAnt.value AS txt_EstadoAnt,
  dat_Pendiente.value AS dat_Pendiente,
  dat_Abierto.value AS dat_Abierto,
  dat_PendAjuste.value AS dat_PendAjuste,
  dat_Verif.value AS dat_Verif,
  dat_Invest.value AS dat_Invest,
  dat_Redes.value AS dat_Redes,
  dat_Bancos.value AS dat_Bancos,
  dat_TrmInterno.value AS dat_TrmInterno,
  dat_AbonoTemp.value AS dat_AbonoTemp,
  dat_MalRadicado.value AS dat_MalRadicado,
  dat_FraudeTrj.value AS dat_FraudeTrj,
  dat_Reabierto.value AS dat_Reabierto,
  dat_SolucionadoCarta.value AS dat_SolucionadoCarta,
  dat_TramiteExt.value AS dat_TramiteExt,
  dat_Cerrado.value AS dat_Cerrado,
  dat_TramiteAbono.value AS dat_TramiteAbono,
  dat_CerradoSucTel.value AS dat_CerradoSucTel,
  dat_CerradoTele.value AS dat_CerradoTele,
  dat_SolRptaEsc.value AS dat_SolRptaEsc,
  dat_ClteNoCont.value AS dat_ClteNoCont,
  dat_CartaEnRev.value AS dat_CartaEnRev,
  dat_AboTempA.value AS dat_AboTempA,
  dat_AboTempP.value AS dat_AboTempP,
  key_CartaCreada.value AS key_CartaCreada,
  txt_ComComercial.value AS txt_ComComercial,
  txt_AsuntoCarta.value AS txt_AsuntoCarta,
  concat_ws('\n', txt_EstadoActual.value) AS txt_EstadoActual,
  concat_ws('\n', txt_FechaHora.value) AS txt_FechaHora,
  concat_ws('\n', nom_Usuario.value) AS nom_Usuario,
  txt_Cedula.value AS txt_Cedula,
  txt_Nomina.value AS txt_Nomina,
  txt_Condicion.value AS txt_Condicion,
  txt_IndPendRapp.value AS txt_IndPendRapp,
  key_Soportes.value AS key_Soportes,
  key_Estado.value AS key_Estado,
  key_EstadoVisible.value AS key_EstadoVisible,
  num_DiasAcSrv.value AS num_DiasAcSrv
FROM
  Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old;
          


-- COMMAND ----------

-- DBTITLE 1,Tabla reclamos Anexos Old
DROP VIEW IF EXISTS Process.Zdp_Lotus_reclamos_Reclamos_Anexos_Old;
CREATE VIEW Process.Zdp_Lotus_reclamos_Reclamos_Anexos_Old AS
SELECT
  W.UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/reclamos-old/',W.UniversalID,'.zip',Z.Valor1),'') link,
  substr(substring_index(W.dat_Fecha.value, ' ', 1),-4,4) AS aniopart
FROM Process.Zdp_Lotus_Reclamos_Reclamos_TablaIntermedia_Old W 
CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_RECLAMOS_APP")
CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN")