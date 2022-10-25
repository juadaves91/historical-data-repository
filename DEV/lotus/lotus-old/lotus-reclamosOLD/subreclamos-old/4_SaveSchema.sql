-- Databricks notebook source
-- DBTITLE 1,Utilidades - Borrado Objeto
-- MAGIC %run
-- MAGIC /utilidades/Helpers

-- COMMAND ----------

-- DBTITLE 1,Borrar objetos viejos en DataLake
-- MAGIC %python
-- MAGIC fn_delete_folder('lotus-reclamos/subreclamos-old') 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes Subreclamos Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old 
(
  UniversalID string,
  FechaCreacion string,
  fecha_solicitudes string,
  NumProducto string,
  TipoCuenta_Homologado string,
  NroCuenta string,
  VlrTotal_1 string,
  VlrGMF string,
  VlrComis string,
  VlrRever string,
  ClienteRazon string,
  CargaClte string,
  PGAreaAdmin string,
  Comentarios string,
  Autor string,
  Creador string,
  VlrTotal string,
  PGTarjetas string,
  Prefijo string,
  DAbierto string,
  Estado string,
  Solucionado string,
  DV string,
  Radicado string,
  DVSlnado string,
  Estado_1 string,
  Responsable string,
  Responsable_1 string,
  CedNit string,
  NomCliente string,
  Producto string,
  TipoReclamo string,
  NumProductoCalc string,
  CampoDilig_Homologado string,
  PGSucError string,
  CodPGSucError string,
  NroCtaTercero string,
  TipoCtaTercero_Homologado string,
  Historia string,
  UniversalID_Padre string,
  Busqueda string,
  aniopart string,
  ValorTotal string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/subreclamos-old';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal_Old')

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Subreclamos Old
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal_Old
(
  UniversalId string,
  FechaCreacion string,
  fecha_solicitudes string,
  Estado string,
  NomCliente string,
  Producto string,
  TipoReclamo string,
  UniversalID_Padre string,
  Busqueda string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/subreclamos-old-principal';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_SubReclamos_Detalle_Old')

-- COMMAND ----------

-- DBTITLE 1,Tabla detalle Subreclamos Old
CREATE TABLE Reclamos.zdr_uvw_lotus_Reclamos_SubReclamos_Detalle_Old
(
  UniversalId	string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/subreclamos-old-detalle';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos Subreclamos Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Anexos_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Anexos_Old
(
  UniversalID	string,
  link string,
  aniopart string
)
STORED AS PARQUET
--PARTITIONED BY(aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos-old/results/anexos-old';