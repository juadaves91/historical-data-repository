-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Process;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de tiempo
CREATE TABLE IF NOT EXISTS process.zdp_lotus_requerimientos_legales_cat_tiempo AS
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

-- DBTITLE 1,Tabla Intermedia (HIVE - Serde)
DROP TABLE IF EXISTS Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia;
CREATE EXTERNAL TABLE Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia
( 
  UniversalID string,
  attachments array<string>, 
  SSConflictAction map<string,string>,
  SSRevisions map<string,array<string>>,
  SSUpdatedBy map<string,array<string>>,
  SSWebFlags map<string,string>,
  dat_FchProxEscal map<string,string>,
  txt_NivEscalAct map<string,string>,
  EstadoSel map<string,string>,
  txt_IdDoc map<string,string>,
  key_Filial map<string,string>,
  key_SucArea map<string,string>,
  key_EnteSol map<string,string>,
  txt_OtroEnte map<string,string>,
  dat_FechOficio map<string,string>,
  key_TipoReqmto map<string,string>,
  txt_OtroReqmto map<string,string>,
  num_DiaSol map<string,string>,
  key_TipoConteo map<string,string>,
  num_DiasPro map<string,string>,
  txt_CedNit map<string,array<string>>,
  txt_Pcto map<string,string>,
  key_Ciudad map<string,string>,
  txt_Funcionario map<string,string>,
  txt_Direccion map<string,string>,
  txt_Banco map<string,string>,
  txt_CtaDep map<string,string>,
  txt_RutaBDAnexos map<string,array<string>>,
  num_DiasSolNu map<string,string>,
  txt_NumOficio map<string,string>,
  dat_FechVto map<string,string>,
  txt_SubformRelC52 map<string,string>,
  txt_IdAppC52 map<string,string>,
  txt_NomAppC52 map<string,string>,
  txt_TransaccionC52 map<string,string>,
  txt_CanalC52 map<string,string>,
  txt_CategoriaC52 map<string,string>,
  txt_IdentClientesC52 map<string,array<string>>,
  Form map<string,string>,
  key_ViceAtdPed map<string,string>,
  key_NomPedido map<string,string>,
  txt_Autor map<string,string>,
  txt_Creador map<string,string>,
  dat_FchCreac map<string,string>,
  key_TipoEnteExt map<string,string>,
  num_Aleatorio map<string,string>,
  key_AnexosAdic map<string,string>,
  txt_AsuntoPed map<string,string>,
  dat_FchMaxEstado map<string,string>,
  rch_DetallePed map<string,array<string>>,
  txt_Comentarios map<string,array<string>>,
  rch_AnexosPed map<string,array<string>>,
  rch_AnexosAdic map<string,array<string>>,
  txt_LstUsuInteres map<string,array<string>>,
  txt_Titulo1 map<string,string>,
  txt_Titulo2 map<string,string>,
  txt_SrvRegionales map<string,array<string>>,
  txt_RutaBDEmp map<string,array<string>>,
  txt_RutaBDCalendar map<string,array<string>>,
  txt_RutaBDEnc map<string,array<string>>,
  txt_RutaListin map<string,array<string>>,
  txt_RutaAplSist map<string,array<string>>,
  txt_RutaViajes map<string,array<string>>,
  txt_RutaBDSuc map<string,array<string>>,
  txt_RutaPyV map<string,array<string>>,
  key_EmpBanco map<string,string>,
  txt_NomCompEmp_A map<string,string>,
  txt_DepEmp_A map<string,string>,
  txt_RgnEmp_A map<string,string>,
  txt_UbicEmp_A map<string,string>,
  txt_ExtEmp_A map<string,string>,
  txt_EmpEnteExt map<string,string>,
  txt_TelEnteExt map<string,string>,
  key_RefrescarValid map<string,string>,
  txt_Criterio1 map<string,array<string>>,
  txt_Criterio2 map<string,array<string>>,
  txt_Criterio3 map<string,array<string>>,
  txt_EstadoBase map<string,string>,
  key_CalcResp map<string,string>,
  txt_EncuestaSatis map<string,string>,
  txt_EntePrioridad map<string,string>,
  txt_IntegGrupoPrior map<string,array<string>>,
  num_CantParam map<string,string>,
  key_CotizPedido map<string,array<string>>,
  key_EstadoCotiz map<string,string>,
  key_CalcTEstado map<string,string>,
  txt_TipoEstado map<string,string>,
  key_PedidoAuto map<string,string>,
  key_SubPedRegistrado map<string,string>,
  key_CambioAutoEstado map<string,string>,
  key_TipoRespTabla map<string,string>,
  key_EstadoTerm map<string,string>,
  key_EstadoAprob map<string,string>,
  key_CausaECorreo map<string,string>,
  txt_VlrResp map<string,string>,
  nom_Responsable map<string,string>,
  lec_Lectores map<string,string>,
  txt_IntegGrupoResp map<string,array<string>>,
  txt_ReasigAuto map<string,array<string>>,
  key_TipoRespAlt map<string,string>,
  txt_RespAlterno map<string,array<string>>,
  txt_EstadoXResp map<string,array<string>>,
  txt_EstadosPendNotif map<string,array<string>>,
  txt_UnidEncuesta map<string,string>,
  txt_CambioWF map<string,array<string>>,
  key_CambioRespM map<string,string>,
  txt_RespAnt map<string,string>,
  txt_EstadoAnt map<string,string>,
  num_DVEstadoAnt map<string,string>,
  txt_WFPersModif map<string,array<string>>,
  txt_WFEstadoAct map<string,array<string>>,
  txt_WFRespAct map<string,array<string>>,
  txt_WFFchModif map<string,array<string>>,
  txt_WFAccion map<string,array<string>>,
  txt_WFPersModif_S map<string,array<string>>,
  txt_WFEstadoAct_S map<string,array<string>>,
  txt_WFRespAct_S map<string,array<string>>,
  txt_WFFchModif_S map<string,array<string>>,
  txt_WFAccion_S map<string,array<string>>,
  txt_WFEstado map<string,array<string>>,
  txt_WFDiasV map<string,array<string>>,
  dat_FchIniTram map<string,string>,
  num_DVTotal map<string,string>,
  num_DVParcial map<string,string>,
  txt_Estado map<string,string>,
  dat_FchEstadoAct map<string,string>,
  num_TMaxEstado map<string,string>,
  dat_FchMaxEstado_1 map<string,string>,
  num_TCotizado map<string,string>,
  num_TCotizado_1 map<string,string>,
  num_PesosCotiz map<string,string>,
  num_PesosCotiz_1 map<string,string>,
  dat_FchVencEstado map<string,string>,
  txt_Responsable map<string,string>,
  txt_Responsable_1 map<string,string>,
  txt_NomCompEmp_R map<string,string>,
  txt_DepEmp_R map<string,string>,
  txt_RgnEmp_R map<string,string>,
  txt_UbicEmp_R map<string,string>,
  txt_ExtEmp_R map<string,string>,
  txt_Historia map<string,array<string>>,
  NUM_PENDIENTEELIM map<string,string>,
  SSFonts map<string,string>,
  SSOLEOBJINFO map<string,string>,
  key_EstadoIni map<string,string>,
  rch_Detalle map<string,array<string>>,
  txt_CedNitNueva map<string,string>,
  txt_PctosCtas map<string,string>,
  txt_PctosCtasNueva map<string,string>,
  txt_ProductoNuevo map<string,string>
  )
  ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/mnt/lotus-requerimientos-legales/raw';

-- COMMAND ----------

-- DBTITLE 1,Ignorar campos malos de los json
ALTER TABLE Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia SET SERDEPROPERTIES ("ignore.malformed.json" = "true");

-- COMMAND ----------

-- DBTITLE 1,Vista Requerimientos Legales (Campos Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_RequerimientosLegales;
CREATE VIEW Process.Zdp_Lotus_RequerimientosLegales AS
SELECT  UniversalID AS UniversalID,        
        key_Filial.value AS Filial,
        key_SucArea.value AS SucursalArea,
        key_EnteSol.value AS Entesolicitante,
        txt_OtroEnte.value AS NombreEnte,
        dat_FechOficio.value AS FechaOficio,
        key_TipoReqmto.value AS TipoRequerimiento,
        txt_OtroReqmto.value AS Requerimiento,
        num_DiaSol.value AS DiasSolucion,
        key_TipoConteo.value AS ConteoDiasSolucion_key_TipoConteo,
         CASE
         WHEN UPPER(key_TipoConteo.value) = 'C'  THEN 'Calendario'
         WHEN UPPER(key_TipoConteo.value) = 'H' THEN 'Hábiles'
         ELSE ''
        END AS ConteoDiasSolucion_Homologado,
        num_DiasPro.value AS DiasProrroga,
        txt_Pcto.value AS txt_Pcto_Vinculado,
        CASE
         WHEN UPPER(txt_Pcto.value) = 'S'  THEN 'Si'
         WHEN UPPER(txt_Pcto.value) = 'N' THEN 'No'
         ELSE ''
        END AS Vinculado_Homologado,
        key_Ciudad.value AS Ciudad,
        txt_Funcionario.value AS Funcionario,
        txt_Direccion.value AS Direccion,
        txt_Banco.value AS Banco,
        txt_CtaDep.value AS CuentaDeposito,
        txt_NumOficio.value AS NumeroOficio,
        dat_FechVto.value AS FechaVencimientoOficio,
        txt_Autor.value AS Autor,
        txt_Creador.value AS Creador,        
        key_TipoEnteExt.value AS EmpresaTipo,
        txt_AsuntoPed.value AS Asunto,
        dat_FchMaxEstado.value AS FechaMaximaEstado,             
        txt_Titulo1.value AS Titulo1,
        txt_Titulo2.value AS Titulo2,
        txt_NomCompEmp_A.value AS Nombres,
        txt_DepEmp_A.value AS Dependencia,
        txt_RgnEmp_A.value AS Region,
        txt_UbicEmp_A.value AS Ubicacion,
        txt_ExtEmp_A.value AS Telefono,
        txt_EmpEnteExt.value AS EmpresaNombre,
        txt_TelEnteExt.value AS TelefonoExt,
        dat_FchIniTram.value AS FechaEntregaDefinicion,
        num_DVTotal.value AS VencidoDiasHabilesTotales,
        num_DVParcial.value AS VencidoDiasHabilesEstadoActua,
        txt_Estado.value AS Estado,
        dat_FchEstadoAct.value AS FechaEstado,
        num_TMaxEstado.value AS TiempoMaximoEstado,
        dat_FchMaxEstado_1.value AS VisualizadorFchMaxEstado,
        num_TCotizado.value AS TiempoCotizado,
        num_TCotizado_1.value AS VisualizadorTCotizado,
        num_PesosCotiz.value AS PesosCotizados,
        num_PesosCotiz_1.value AS VisualizadorPesosCotiz,
        dat_FchVencEstado.value AS FechaVencimiento,
        txt_Responsable.value AS Responsable,
        txt_Responsable_1.value AS VisualizadorResponsable,
        txt_NomCompEmp_R.value AS InformacionEmpleadoResponsableNombres,
        txt_DepEmp_R.value AS InformacionEmpleadoResponsableDependencia,
        txt_RgnEmp_R.value AS InformacionEmpleadoResponsableRegion,
        txt_UbicEmp_R.value AS InformacionEmpleadoResponsableUbicacion,
        txt_ExtEmp_R.value AS InformacionEmpleadoResponsableTelefono,		   
        dat_FchCreac.value AS FechaRequerimientoLegal,        
        key_NomPedido.value AS NomPedido,  
        key_ViceAtdPed.value AS ViceAtdPed,
        substring_index(dat_FchCreac.value, ' ', 1) AS fecha_requerimiento_legal,
        substr(substring_index(dat_FchCreac.value, ' ', 1),-4,4) AS aniopart,       
        concat_ws('\n', txt_Historia.value) AS Historia,                   
		concat_ws('\n', rch_AnexosAdic.value) AS AnexosAdicionales,        
		concat_ws('\n', rch_AnexosPed.value) AS AnexosPedido,              
		concat_ws('\n', txt_Comentarios.value) AS Comentarios,             
		concat_ws('\n', rch_DetallePed.value) AS DetallePedido,            
		concat_ws('\n', txt_CedNit.value) AS CedulaNit,                    
        concat(concat(txt_AsuntoPed.value, ' - ', key_TipoReqmto.value, ' - ', concat_ws(' - ', txt_CedNit.value)), ' - ',
        upper(concat(txt_AsuntoPed.value, ' - ', key_TipoReqmto.value)), ' - ',
        lower(concat(txt_AsuntoPed.value, ' - ', key_TipoReqmto.value)))  AS busqueda
FROM Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia;

-- COMMAND ----------

-- DBTITLE 1,Vista Requerimientos Legales (Campos No Visibles)
DROP VIEW IF EXISTS Process.Zdp_Lotus_RequerimientosLegalesNoVisibles;
CREATE VIEW Process.Zdp_Lotus_RequerimientosLegalesNoVisibles AS
SELECT  UniversalID AS UniversalID,
        SSConflictAction.value AS SSConflictAction,
        concat_ws('\n', SSRevisions.value) AS SSRevisions,
        concat_ws('\n', SSUpdatedBy.value) AS SSUpdatedBy,
        SSWebFlags.value AS SSWebFlags,
        dat_FchProxEscal.value AS dat_FchProxEscal,
        txt_NivEscalAct.value AS txt_NivEscalAct,
        EstadoSel.value AS EstadoSel,
        txt_IdDoc.value AS txt_IdDoc,
        concat_ws('\n', txt_RutaBDAnexos.value) AS txt_RutaBDAnexos,
        num_DiasSolNu.value AS num_DiasSolNu,
        txt_SubformRelC52.value AS txt_SubformRelC52,
        txt_IdAppC52.value AS txt_IdAppC52,
        txt_NomAppC52.value AS txt_NomAppC52,
        txt_TransaccionC52.value AS txt_TransaccionC52,
        txt_CanalC52.value AS txt_CanalC52,
        txt_CategoriaC52.value AS txt_CategoriaC52,
        concat_ws('\n', txt_IdentClientesC52.value) AS txt_IdentClientesC52,
        Form.value AS Form,                
        num_Aleatorio.value AS num_Aleatorio,
        key_AnexosAdic.value AS key_AnexosAdic,
        concat_ws('\n', txt_LstUsuInteres.value) AS txt_LstUsuInteres,
        concat_ws('\n', txt_SrvRegionales.value) AS txt_SrvRegionales,
        concat_ws('\n', txt_RutaBDEmp.value) AS txt_RutaBDEmp,
        concat_ws('\n', txt_RutaBDCalendar.value) AS txt_RutaBDCalendar,
        concat_ws('\n', txt_RutaBDEnc.value) AS txt_RutaBDEnc,
        concat_ws('\n', txt_RutaListin.value) AS txt_RutaListin,
        concat_ws('\n', txt_RutaAplSist.value) AS txt_RutaAplSist,
        concat_ws('\n', txt_RutaViajes.value) AS txt_RutaViajes,
        concat_ws('\n', txt_RutaBDSuc.value) AS txt_RutaBDSuc,
        concat_ws('\n', txt_RutaPyV.value) AS txt_RutaPyV,
        key_EmpBanco.value AS key_EmpBanco,
        key_RefrescarValid.value AS key_RefrescarValid,
        concat_ws('\n', txt_Criterio1.value) AS txt_Criterio1,
        concat_ws('\n', txt_Criterio2.value) AS txt_Criterio2,
        concat_ws('\n', txt_Criterio3.value) AS txt_Criterio3,
        txt_EstadoBase.value AS txt_EstadoBase,
        key_CalcResp.value AS key_CalcResp,
        txt_EncuestaSatis.value AS txt_EncuestaSatis,
        txt_EntePrioridad.value AS txt_EntePrioridad,
        concat_ws('\n', txt_IntegGrupoPrior.value) AS txt_IntegGrupoPrior,
        num_CantParam.value AS num_CantParam,
        concat_ws('\n', key_CotizPedido.value) AS key_CotizPedido,
        key_EstadoCotiz.value AS key_EstadoCotiz,
        key_CalcTEstado.value AS key_CalcTEstado,
        txt_TipoEstado.value AS txt_TipoEstado,
        key_PedidoAuto.value AS key_PedidoAuto,
        key_SubPedRegistrado.value AS key_SubPedRegistrado,
        key_CambioAutoEstado.value AS key_CambioAutoEstado,
        key_TipoRespTabla.value AS key_TipoRespTabla,
        key_EstadoTerm.value AS key_EstadoTerm,
        key_EstadoAprob.value AS key_EstadoAprob,
        key_CausaECorreo.value AS key_CausaECorreo,
        txt_VlrResp.value AS txt_VlrResp,
        nom_Responsable.value AS nom_Responsable,
        lec_Lectores.value AS lec_Lectores,
        concat_ws('\n', txt_IntegGrupoResp.value) AS txt_IntegGrupoResp,
        concat_ws('\n', txt_ReasigAuto.value) AS txt_ReasigAuto,
        key_TipoRespAlt.value AS key_TipoRespAlt,
        concat_ws('\n', txt_RespAlterno.value) AS txt_RespAlterno,
        concat_ws('\n', txt_EstadoXResp.value) AS txt_EstadoXResp,
        concat_ws('\n', txt_EstadosPendNotif.value) AS txt_EstadosPendNotif,
        txt_UnidEncuesta.value AS txt_UnidEncuesta,
        concat_ws('\n', txt_CambioWF.value) AS txt_CambioWF,
        key_CambioRespM.value AS key_CambioRespM,
        txt_RespAnt.value AS txt_RespAnt,
        txt_EstadoAnt.value AS txt_EstadoAnt,
        num_DVEstadoAnt.value AS num_DVEstadoAnt,
        concat_ws('\n', txt_WFPersModif.value) AS txt_WFPersModif,
        concat_ws('\n', txt_WFEstadoAct.value) AS txt_WFEstadoAct,
        concat_ws('\n', txt_WFRespAct.value) AS txt_WFRespAct,
        concat_ws('\n', txt_WFFchModif.value) AS txt_WFFchModif,
        concat_ws('\n', txt_WFAccion.value) AS txt_WFAccion,
        concat_ws('\n', txt_WFPersModif_S.value) AS txt_WFPersModif_S,
        concat_ws('\n', txt_WFEstadoAct_S.value) AS txt_WFEstadoAct_S,
        concat_ws('\n', txt_WFRespAct_S.value) AS txt_WFRespAct_S,
        concat_ws('\n', txt_WFFchModif_S.value) AS txt_WFFchModif_S,
        concat_ws('\n', txt_WFAccion_S.value) AS txt_WFAccion_S,
        concat_ws('\n', txt_WFEstado.value) AS txt_WFEstado,
        concat_ws('\n', txt_WFDiasV.value) AS txt_WFDiasV,
        NUM_PENDIENTEELIM.value AS NUM_PENDIENTEELIM,
        SSFonts.value AS SSFonts,
        SSOLEOBJINFO.value AS SSOLEOBJINFO,
        key_EstadoIni.value AS key_EstadoIni,
        concat_ws('\n', rch_Detalle.value) AS rch_Detalle,
        txt_CedNitNueva.value AS txt_CedNitNueva,
        txt_PctosCtas.value AS txt_PctosCtas,
        txt_PctosCtasNueva.value AS txt_PctosCtasNueva,
        txt_ProductoNuevo.value AS txt_ProductoNuevo
FROM Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia;

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP VIEW IF EXISTS Process.Zdp_Lotus_RequerimientosLegales_Anexos;
CREATE VIEW Process.Zdp_Lotus_RequerimientosLegales_Anexos AS
SELECT
  W.UniversalID,
  IF(length(concat_ws(',',attachments))>0,CONCAT(X.Valor1,Y.Valor1,'/',W.UniversalID,'.zip',Z.Valor1),'') AS link,
  substr(substring_index(W.dat_FchCreac.value, ' ', 1),-4,4) AS aniopart
FROM Process.Zdp_Lotus_RequerimientosLegales_Tablaintermedia W 
CROSS JOIN Default.parametros X ON (X.CodParametro="PARAM_URL_BLOBSTORAGE")
CROSS JOIN Default.parametros Y ON (Y.CodParametro="PARAM_LOTUS_REQ_LEG_APP")
CROSS JOIN Default.parametros Z ON (Z.CodParametro="PARAM_LOTUS_TOKEN")