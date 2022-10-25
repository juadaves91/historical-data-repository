-- Databricks notebook source
SET hive.exec.dynamic.partition.mode = nonstrict;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos Old (Particionada)
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old PARTITION(aniopart)
SELECT
  UniversalID,
  FchCreac,
  fecha_solicitudes,
  CedulaCliente,
  NomCliente,
  FuncionarioEmpresa,
  DirCorrospondencia,
  TelefonoCliente,
  Fax,
  Email,
  TipoDocumento,
  Pais,
  Estado,
  DirPostal,
  ApartadoAereo,
  RespuesptaReclamo,
  CiudadCorrespondencia,
  TelResponsable,
  NumProducto,
  NroCodCompra,
  NroProducto,
  num_DVPendiente,
  num_DVMalRad,
  num_DVFraudeTrj,
  num_DVSlnado,
  num_DVSlnadoCarta,
  num_DVPendAjuste,
  num_DVVerif,
  num_DVInvest,
  num_DVRedes,
  num_DVBancos,
  num_DVTrmInterno,
  num_DVSolicRptaEsc,
  num_DVClteNoCont,
  num_DVCartaEnRev,
  num_DVAboTempA,
  num_DVAboTempP,
  NumTarjDeb,
  NumPoliza,
  NumEncargo,
  NomSucRadica,
  num_DRecl,
  TipoPoliza,
  NumTjtaCtaDeb,
  CiudadTransa,
  FchTransac,
  NomSucVarios,
  CodEstab,
  NumTransa,
  ValorTotal,
  DatoAdicionalesReclamo,
  SopRecibido,
  SopNoRec,
  TodosSoportes,
  ValorTransa_1,
  FechaTransa_1,
  OficinaTransa_1,
  Referencia_1,
  Moneda_1,
  CiudadTransa_1,
  NumeroCuenta_1,
  ChequeVouch_1,
  FechaConsigna_1,
  ValorTransa_2,
  FechaTransa_2,
  OficinaTransa_2,
  Referencia_2,
  Moneda_2,
  CiudadTransa_2,
  NumeroCuenta_2,
  ChequeVouch_2,
  FechaConsigna_2,
  ValorTransa_3,
  FechaTransa_3,
  OficinaTransa_3,
  Referencia_3,
  Moneda_3,
  CiudadTransa_3,
  NumeroCuenta_3,
  ChequeVouch_3,
  FechaConsigna_3,
  ValorTransa_4,
  FechaTransa_4,
  OficinaTransa_4,
  Referencia_4,
  Moneda_4,
  CiudadTransa_4,
  NumeroCuenta_4,
  ChequeVouch_4,
  FechaConsigna_4,
  ValorTransa_5,
  FechaTransa_5,
  OficinaTransa_5,
  Referencia_5,
  Moneda_5,
  CiudadTransa_5,
  NumeroCuenta_5,
  ChequeVouch_5,
  FechaConsigna_5,
  Pantallas,
  PantallaSln,
  TipoTarjeta,
  TipoReclamo,
  num_VlrTotalD,
  Autor,
  Creador,
  Fecha_cerrado,
  NomSucCaptura,
  SucursalFilial,
  ReportadoPor,
  OrigenReporte,
  num_AbonoTemp,
  num_AbonoDefinit,
  ResponError,
  NomRespError,
  ClienteRazon,
  CausaNoAbono,
  MedioRpta,
  ComentRapp,
  Comentarios,
  RptaCliente,
  Anexos,
  num_DV,
  numV_DVClteNoCont,
  numV_DVCartaEnRev,
  numV_DVAboTempA,
  numV_DVAboTempP,
  Estado_1,
  nom_Responsable_1,
  nom_UsuCierra,
  CedNit,
  TipoCliente,
  SubSgmtoCliente,
  CiudadCorr,
  DptoCorr,
  IdDireccion,
  nom_RespGer,
  num_TpoEstSol,
  dat_FchAproxSol,
  Companiia,
  Producto,
  num_VlrTotal_1,
  num_VlrTotalD_1,
  MensajeAyuda,
  MensajeSoportes,
  NomOpTblM,
  Historia,
  RgnCaptura,
  ZonaCaptura,
  ClteGer_homologado,
  ClteColombia_homologado,
  SubprocRecl,
  ValorTransa_6,
  FechaTransa_6,
  OficinaTransa_6,
  Referencia_6,
  Moneda_6,
  CiudadTransa_6,
  NumeroCuenta_6,
  ChequeVouch_6,
  FechaConsigna_6,
  ValorTransa_7,
  FechaTransa_7,
  OficinaTransa_7,
  Referencia_7,
  Moneda_7,
  CiudadTransa_7,
  NumeroCuenta_7,
  ChequeVouch_7,
  FechaConsigna_7,
  ValorTransa_8,
  FechaTransa_8,
  OficinaTransa_8,
  Referencia_8,
  Moneda_8,
  CiudadTransa_8,
  NumeroCuenta_8,
  ChequeVouch_8,
  FechaConsigna_8,
  ValorTransa_9,
  FechaTransa_9,
  OficinaTransa_9,
  Referencia_9,
  Moneda_9,
  CiudadTransa_9,
  NumeroCuenta_9,
  ChequeVouch_9,
  FechaConsigna_9,
  GteCuenta,
  num_DAbierto,
  dat_Solucionado,
  key_Prefijo,
  Busqueda,
  ValorTotalEnPesos,
  ValorTotalEnDolares,
  aniopart
From
  Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos Principal old
set hive.exec.dynamic.partition.mode=nonstrict
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_Reclamos_Principal_Old PARTITION(aniopart)
SELECT
  UniversalId,	
  CedNit,	
  fecha_solicitudes,	
  Producto,	
  TipoReclamo,	
  num_AbonoDefinit,	
  NomCliente,	
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda,
  aniopart
FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Reclamos Old (unpivot)
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Detalle_Old 
SELECT UniversalId, 'Reclamos' AS vista, 1 as posicion, 'Días Vencidos Totales' AS key, num_DV AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 2 as posicion, 'Días Vencidos de Soportes' AS key, num_DVPendiente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 3 as posicion, 'Días Vencidos Mala radicación' AS key, num_DVMalRad AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 4 as posicion, 'Días Vencidos Pendiente Documentación' AS key, num_DVFraudeTrj AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 5 as posicion, 'Días Vencidos respuesta al CLiente' AS key, num_DVSlnado AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 6 as posicion, 'Días Vencidos Solucionado con Carta' AS key, num_DVSlnadoCarta AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 7 as posicion, 'Días Vencidos Pendiente Ajuste' AS key, num_DVPendAjuste AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 8 as posicion, 'Días Vencidos Verificación' AS key, num_DVVerif AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 9 as posicion, 'Días Vencidos Investigación' AS key, num_DVInvest AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 10 as posicion, 'Días Vencidos Redes' AS key, num_DVRedes AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 11 as posicion, 'Días Vencidos Bancos' AS key, num_DVBancos AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 12 as posicion, 'Días Vencidos Tramite Interno' AS key, num_DVTrmInterno AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 13 as posicion, 'Días Vencidos Solicita Respuesta Escrita' AS key, num_DVSolicRptaEsc AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 14 as posicion, 'Días Vencidos Cliente No Contactado' AS key, num_DVClteNoCont AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 15 as posicion, 'Días Vencidos Carta en Revisión' AS key, num_DVCartaEnRev AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 16 as posicion, 'Días Vencidos Susceptible Abono Temporal Abierto' AS key, num_DVAboTempA AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Reclamos' AS vista, 17 as posicion, 'Días Vencidos Susceptible Abono Temporal Pendiente' AS key, num_DVAboTempP AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, '' AS vista, 18 as posicion, 'Autor' AS key, Autor AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, '' AS vista, 19 as posicion, 'Creador' AS key, Creador AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, '' AS vista, 20 as posicion, 'Fecha de Creación' AS key, FchCreac AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 21 as posicion, 'Sucursal de Captura' AS key, concat(NomSucCaptura, '\n', SucursalFilial) AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 22 as posicion, 'Region de Captura' AS key, RgnCaptura AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 23 as posicion, 'Zona de Captura' AS key, ZonaCaptura AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 24 as posicion, 'Reportado Por' AS key, ReportadoPor AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 25 as posicion, 'Origen del reporte' AS key, Origenreporte AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 26 as posicion, 'Responsable' AS key, nom_Responsable_1 AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 27 as posicion, 'Abono Temporal' AS key, num_AbonoTemp AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 28 as posicion, 'Abono Definitivo' AS key, num_AbonoDefinit AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 29 as posicion, 'Responsable del Error' AS key, ResponError AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 30 as posicion, 'Nombre del Responsable Error' AS key, NomRespError AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 31 as posicion, 'El Cliente tiene la Razon?' AS key, ClienteRazon AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Radicación' AS vista, 32 as posicion, 'Causa de no Abono' AS key, CausaNoAbono AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 33 as posicion, 'Usuario que Cierra' AS key, nom_UsuCierra AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 34 as posicion, 'Tiempo Total (Creado - Cerrado)' AS key, num_DRecl AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Cierre del reclamo' AS vista, 35 as posicion, 'Medio de Respuesta al Cliente' AS key, MedioRpta AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 36 as posicion, 'Tipo de Documento' AS key, TipoDocumento AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 37 as posicion, 'Cedula/Nit' AS key, CedNit AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 38 as posicion, 'Nombre' AS key, NomCliente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 39 as posicion, 'Tipo Cliente' AS key, TipoCliente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 40 as posicion, 'Funcionario de la Empresa' AS key, FuncionarioEmpresa AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 41 as posicion, 'SubSegmento' AS key, SubSgmtoCliente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 42 as posicion, 'Es Cliente Gerenciado?' AS key, ClteGer_homologado AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 43 as posicion, 'Gerente de Cuenta' AS key, GteCuenta AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 44 as posicion, 'Es Cliente Colombia?' AS key, ClteColombia_homologado AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 45 as posicion, 'Ciudad de Correspondencia' AS key, concat(CiudadCorr, ' - ', CiudadCorrespondencia) AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 46 as posicion, 'Dpto Correspondencia' AS key, DptoCorr AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 47 as posicion, 'Dirección Correspondencia' AS key, DirCorrospondencia AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 48 as posicion, 'País' AS key, Pais AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 49 as posicion, 'Estado' AS key, Estado AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 50 as posicion, 'Dirección Postal' AS key, DirPostal AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 51 as posicion, 'Apartado Aereo (P.O BOX)' AS key, ApartadoAereo AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 52 as posicion, 'Identificador Dirección' AS key, IdDireccion AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 53 as posicion, 'Como Desea Recibir Respuesta a su Reclamo?' AS key, RespuesptaReclamo AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 55 as posicion, 'Telefonos' AS key, TelefonoCliente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 56 as posicion, 'Fax' AS key, Fax AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Cliente' AS vista, 57 as posicion, 'E-mail' AS key, Email AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old


UNION ALL
SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 58 as posicion, 'Nombre del Responsable' AS key, nom_respGer AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Responsable del Seguimiento al reclamo' AS vista, 59 as posicion, 'Telefono Directo del Responsable' AS key, TelResponsable AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 60 as posicion, 'Tiempo Estimado de Solución' AS key, num_TpoEstSol AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 61 as posicion, 'Fecha Aprox de Solución' AS key, dat_FchAproxSol AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 62 as posicion, 'Tiempo Real de Solución' AS key, num_DAbierto AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Fechas de la Solucion/Vencimiento' AS vista, 63 as posicion, 'Fecha real de Solución' AS key, dat_Solucionado AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 64 as posicion, 'Compañia' AS key, Companiia AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 65 as posicion, 'Producto' AS key, Producto AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 66 as posicion, 'Clasificación' AS key, TipoTarjeta AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 67 as posicion, 'Tipo Reclamo' AS key, TipoReclamo AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 68 as posicion, 'Número de Producto' AS key,concat(key_Prefijo, ' - ', NumProducto) AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 69 as posicion, 'Codigo de Compra' AS key, NroCodCompra AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 70 as posicion, 'Código de Producto' AS key, NroProducto AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 71 as posicion, 'Sub-Proceso Dueño del Reclamo' AS key, SubprocRecl AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 72 as posicion, 'Número de Tarjeta Debito' AS key, NumTarjDeb AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 73 as posicion, 'Número de Poliza' AS key, NumPoliza AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 74 as posicion, 'Número de Encargo Fiduciario' AS key, NumEncargo AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 75 as posicion, 'Sucursal de Radicacion' AS key, NomSucRadica AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 76 as posicion, 'Tipo de Poliza' AS key, TipoPoliza AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 77 as posicion, 'Número de la Tarjeta/Cuenta que se Debita' AS key, NumTjtaCtaDeb AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 78 as posicion, 'Ciudad de Transacción/Suceso' AS key, CiudadTransa AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 79 as posicion, 'Fecha Transacción' AS key, FchTransac AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 80 as posicion, 'Suc/Area de Queja, Felicitacion o Sugerencia' AS key, NomSucVarios AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 81 as posicion, 'Código Unico de Establecimiento' AS key, CodEstab AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 82 as posicion, 'Número Transacciones' AS key, NumTransa AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 83 as posicion, 'Valor Total en Pesos' AS key, ValorTotalEnPesos AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 84 as posicion, 'Valor Total en Dolares' AS key, ValorTotalEnDolares AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 85 as posicion, 'Mensaje de Ayuda' AS key, MensajeAyuda AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del Reclamo' AS vista, 86 as posicion, 'Soportes requeridos' AS key, MensajeSoportes AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Observaciones' AS vista, 87 as posicion, '' AS key, ComentRapp AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Soportes Recibidos' AS vista, 88 as posicion, 'Soportes recibidos del Cliente' AS key, SopRecibido AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Soportes Recibidos' AS vista, 89 as posicion, 'Cuales no se han Recibido' AS key, SopNoRec AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Soportes Recibidos' AS vista, 90 as posicion, 'Se recibieron todos los Soportes?' AS key, TodosSoportes AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Pantallas que Documentan el Reclamo' AS vista, 91 as posicion, '' AS key, Pantallas AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Pantallas que Certifican la Solución' AS vista, 92 as posicion, '' AS key, PantallaSln AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Comentarios' AS vista, 93 as posicion, '' AS key, Comentarios AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Historia' AS vista, 94 as posicion, '' AS key, Historia AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Respuesta al Cliente' AS vista, 95 as posicion, '' AS key, RptaCliente AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 96 as posicion, '' AS key, NomOpTblM AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Adicionales del Reclamo' AS vista, 97 as posicion, '' AS key, DatoAdicionalesReclamo AS value, aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos_Anexos Old
set hive.exec.dynamic.partition.mode=nonstrict
REFRESH TABLE Process.Zdp_Lotus_reclamos_Reclamos_Anexos_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos_Old PARTITION(aniopart)
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_reclamos_Reclamos_Anexos_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Transacciones Old
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Transacciones_Old
SELECT UniversalId, 'TRANSACCIONES 1' AS vista, 1 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 2 as posicion, 'Valor transacción' AS key, ValorTransa_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 3 as posicion, 'Fecha de transacción' AS key, FechaTransa_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 4 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 5 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_1) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 6 as posicion, 'Moneda' AS key, moneda_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 7 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 8 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 9 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_1' AS vista, 10 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_1 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 2' AS vista, 11 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 12 as posicion, 'Valor transacción' AS key, ValorTransa_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 13 as posicion, 'Fecha de transacción' AS key, FechaTransa_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 14 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 15 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_2) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 16 as posicion, 'Moneda' AS key, moneda_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 17 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 18 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 19 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_2' AS vista, 20 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_2 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 3' AS vista, 21 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 22 as posicion, 'Valor transacción' AS key, ValorTransa_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 23 as posicion, 'Fecha de transacción' AS key, FechaTransa_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 24 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 25 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_3) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 26 as posicion, 'Moneda' AS key, moneda_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 27 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 28 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 29 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_3' AS vista, 30 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_3 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 4' AS vista, 31 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 32 as posicion, 'Valor transacción' AS key, ValorTransa_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 33 as posicion, 'Fecha de transacción' AS key, FechaTransa_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 34 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 35 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_4) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 36 as posicion, 'Moneda' AS key, moneda_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 37 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 38 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 39 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_4' AS vista, 40 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_4 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 5' AS vista, 41 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 42 as posicion, 'Valor transacción' AS key, ValorTransa_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 43 as posicion, 'Fecha de transacción' AS key, FechaTransa_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 44 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 45 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_5) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 46 as posicion, 'Moneda' AS key, moneda_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 47 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 48 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 49 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_5' AS vista, 50 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_5 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 6' AS vista, 51 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 52 as posicion, 'Valor transacción' AS key, ValorTransa_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 53 as posicion, 'Fecha de transacción' AS key, FechaTransa_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 54 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 55 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_6) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 56 as posicion, 'Moneda' AS key, moneda_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 57 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 58 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 59 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_6' AS vista, 60 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_6 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 7' AS vista, 61 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 62 as posicion, 'Valor transacción' AS key, ValorTransa_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 63 as posicion, 'Fecha de transacción' AS key, FechaTransa_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 64 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 65 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_7) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 66 as posicion, 'Moneda' AS key, moneda_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 67 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 68 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 69 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_7' AS vista, 70 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_7 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 8' AS vista, 71 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 72 as posicion, 'Valor transacción' AS key, ValorTransa_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 73 as posicion, 'Fecha de transacción' AS key, FechaTransa_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 74 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 75 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_8) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 76 as posicion, 'Moneda' AS key, moneda_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 77 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 78 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 79 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_8' AS vista, 80 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_8 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
 
UNION ALL
SELECT UniversalId, 'TRANSACCIONES 9' AS vista, 81 as posicion, '' AS key, '' AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 82 as posicion, 'Valor transacción' AS key, ValorTransa_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 83 as posicion, 'Fecha de transacción' AS key, FechaTransa_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 84 as posicion, 'Oficina de Transacción' AS key, OficinaTransa_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 85 as posicion, 'Nombre del establecimiento/Referencia' AS key, UPPER(Referencia_9) AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 86 as posicion, 'Moneda' AS key, moneda_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 87 as posicion, 'Ciudad Transacción' AS key, CiudadTransa_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 88 as posicion, 'Número de Cuenta/Tarjeta Asociada' AS key, NumeroCuenta_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 89 as posicion, 'Número de Cheque/Voucher' AS key, ChequeVouch_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Transacciones_9' AS vista, 90 as posicion, 'Fecha de Consignación' AS key, FechaConsigna_9 AS value,  aniopart as aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old