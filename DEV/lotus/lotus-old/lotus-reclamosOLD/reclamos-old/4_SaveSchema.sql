-- Databricks notebook source
-- DBTITLE 1,Utilidades - Borrado Objeto
-- MAGIC %run /utilidades/Helpers

-- COMMAND ----------

-- DBTITLE 1,Borrar objetos viejos en DataLake
-- MAGIC %python
-- MAGIC fn_delete_folder('lotus-reclamos/reclamos-old') 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes_Old 
(
  UniversalID string,
  FchCreac string,
  fecha_solicitudes string,
  CedulaCliente string,
  NomCliente string,
  FuncionarioEmpresa string,
  DirCorrospondencia string,
  TelefonoCliente string,
  Fax string,
  Email string,
  TipoDocumento string,
  Pais string,
  Estado string,
  DirPostal string,
  ApartadoAereo string,
  RespuesptaReclamo string,
  CiudadCorrespondencia string,
  TelResponsable string,
  NumProducto string,
  NroCodCompra string,
  NroProducto string,
  num_DVPendiente string,
  num_DVMalRad string,
  num_DVFraudeTrj string,
  num_DVSlnado string,
  num_DVSlnadoCarta string,
  num_DVPendAjuste string,
  num_DVVerif string,
  num_DVInvest string,
  num_DVRedes string,
  num_DVBancos string,
  num_DVTrmInterno string,
  num_DVSolicRptaEsc string,
  num_DVClteNoCont string,
  num_DVCartaEnRev string,
  num_DVAboTempA string,
  num_DVAboTempP string,
  NumTarjDeb string,
  NumPoliza string,
  NumEncargo string,
  NomSucRadica string,
  num_DRecl string,
  TipoPoliza string,
  NumTjtaCtaDeb string,
  CiudadTransa string,
  FchTransac string,
  NomSucVarios string,
  CodEstab string,
  NumTransa string,
  ValorTotal string,
  DatoAdicionalesReclamo string,
  SopRecibido string,
  SopNoRec string,
  TodosSoportes string,
  ValorTransa_1 string,
  FechaTransa_1 string,
  OficinaTransa_1 string,
  Referencia_1 string,
  Moneda_1 string,
  CiudadTransa_1 string,
  NumeroCuenta_1 string,
  ChequeVouch_1 string,
  FechaConsigna_1 string,
  ValorTransa_2 string,
  FechaTransa_2 string,
  OficinaTransa_2 string,
  Referencia_2 string,
  Moneda_2 string,
  CiudadTransa_2 string,
  NumeroCuenta_2 string,
  ChequeVouch_2 string,
  FechaConsigna_2 string,
  ValorTransa_3 string,
  FechaTransa_3 string,
  OficinaTransa_3 string,
  Referencia_3 string,
  Moneda_3 string,
  CiudadTransa_3 string,
  NumeroCuenta_3 string,
  ChequeVouch_3 string,
  FechaConsigna_3 string,
  ValorTransa_4 string,
  FechaTransa_4 string,
  OficinaTransa_4 string,
  Referencia_4 string,
  Moneda_4 string,
  CiudadTransa_4 string,
  NumeroCuenta_4 string,
  ChequeVouch_4 string,
  FechaConsigna_4 string,
  ValorTransa_5 string,
  FechaTransa_5 string,
  OficinaTransa_5 string,
  Referencia_5 string,
  Moneda_5 string,
  CiudadTransa_5 string,
  NumeroCuenta_5 string,
  ChequeVouch_5 string,
  FechaConsigna_5 string,
  Pantallas string,
  PantallaSln string,
  TipoTarjeta string,
  TipoReclamo string,
  num_VlrTotalD string,
  Autor string,
  Creador string,
  Fecha_cerrado string,
  NomSucCaptura string,
  SucursalFilial string,
  ReportadoPor string,
  OrigenReporte string,
  num_AbonoTemp string,
  num_AbonoDefinit string,
  ResponError string,
  NomRespError string,
  ClienteRazon string,
  CausaNoAbono string,
  MedioRpta string,
  ComentRapp string,
  Comentarios string,
  RptaCliente string,
  Anexos string,
  num_DV string,
  numV_DVClteNoCont string,
  numV_DVCartaEnRev string,
  numV_DVAboTempA string,
  numV_DVAboTempP string,
  Estado_1 string,
  nom_Responsable_1 string,
  nom_UsuCierra string,
  CedNit string,
  TipoCliente string,
  SubSgmtoCliente string,
  CiudadCorr string,
  DptoCorr string,
  IdDireccion string,
  nom_RespGer string,
  num_TpoEstSol string,
  dat_FchAproxSol string,
  Companiia string,
  Producto string,
  num_VlrTotal_1 string,
  num_VlrTotalD_1 string,
  MensajeAyuda string,
  MensajeSoportes string,
  NomOpTblM string,
  Historia string,
  RgnCaptura string,
  ZonaCaptura string,
  ClteGer_homologado string,
  ClteColombia_homologado string,
  SubprocRecl string,
  ValorTransa_6 string,
  FechaTransa_6 string,
  OficinaTransa_6 string,
  Referencia_6 string,
  Moneda_6 string,
  CiudadTransa_6 string,
  NumeroCuenta_6 string,
  ChequeVouch_6 string,
  FechaConsigna_6 string,
  ValorTransa_7 string,
  FechaTransa_7 string,
  OficinaTransa_7 string,
  Referencia_7 string,
  Moneda_7 string,
  CiudadTransa_7 string,
  NumeroCuenta_7 string,
  ChequeVouch_7 string,
  FechaConsigna_7 string,
  ValorTransa_8 string,
  FechaTransa_8 string,
  OficinaTransa_8 string,
  Referencia_8 string,
  Moneda_8 string,
  CiudadTransa_8 string,
  NumeroCuenta_8 string,
  ChequeVouch_8 string,
  FechaConsigna_8 string,
  ValorTransa_9 string,
  FechaTransa_9 string,
  OficinaTransa_9 string,
  Referencia_9 string,
  Moneda_9 string,
  CiudadTransa_9 string,
  NumeroCuenta_9 string,
  ChequeVouch_9 string,
  FechaConsigna_9 string,
  GteCuenta string,
  num_DAbierto string,
  dat_Solucionado string,
  key_Prefijo string,
  Busqueda string,
  ValorTotalEnPesos string,
  ValorTotalEnDolares string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos-old/results/reclamos-old';


-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','Zdr_uvw_Lotus_Reclamos_Reclamos_Principal_Old')

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Old
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_Reclamos_Principal_Old
( 
  UniversalId	string,	
  CedNit string,	
  fecha_solicitudes string,	
  Producto string,	
  TipoReclamo	string,	
  num_AbonoDefinit string,	
  NomCliente string,	
  Busqueda string	
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos-old/results/reclamos-old-principal';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos_Old
(
  UniversalID	string,
  link string
)
STORED AS PARQUET
PARTITIONED BY(aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos-old/results/anexos-old';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_Reclamos_Detalle_Old')

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Reclamos Old (unpivot)
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Detalle_Old
(
  UniversalId string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos-old/results/reclamos-old-detalle';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_Reclamos_Transacciones_Old')

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Transacciones Old
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Transacciones_Old
(
  UniversalId	string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos-old/results/reclamos-old-detalletransacciones';