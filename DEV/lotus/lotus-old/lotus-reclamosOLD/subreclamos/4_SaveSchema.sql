-- Databricks notebook source
-- DBTITLE 1,Utilidades - Borrado Objeto
-- MAGIC %run /utilidades/Helpers

-- COMMAND ----------

-- DBTITLE 1,Borrar objetos viejos en DataLake
-- MAGIC %python
-- MAGIC fn_delete_folder('lotus-reclamos/subreclamos')

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamos
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
(
	UniversalID_Sub string,
    UniversalID string,
	Autor string,
	FechaCreacion string,
	fecha_solicitudes string,
	Responsable string,
	Estado string,
	Subreclamo string,
	NumeroIdentificacion string,
	Nombre string,
	Producto string,
	TipoReclamo string,
	NumeroProducto string,
	TipoCuenta string,
	NumeroCuenta string,
	ValorTotalPesos string,
	ValorGMF string,
	ValorComisiones string,
	ValorReversion string,
	ClienteTieneRazon string,
	CampoDiligenciar string,
	CargaCliente string,
	SucursalSobrante string,
	FechaSobrante string,
	PyGSucursalError string,
	PyGAreaAdministrativa string,
	CuentaTercero string,
	TipoCuentaTercero string,
	PyGTarjetas string,
	TiempoRealSolucion string,
	FechaRealSolucion string,
	Comentario string,
	Historia string,
	TotalDias string,
	DiasVencidos string,
	DiasContabilizados string,
    aniopart string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos/results/reclamos';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal')

-- COMMAND ----------

-- DBTITLE 1,Vista Principal
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal
(
  UniversalID_Sub string,
  UniversalID string,
  Subreclamo string,
  FechaCreacion string,
  Autor string,
  Producto string,
  TipoReclamo string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos/results/principal';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_Subreclamos_Detalle')

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle SubReclamos
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_Subreclamos_Detalle
(
  UniversalId	string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION 'mnt/lotus-reclamos/subreclamos/results/subreclamo_detalle';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Anexos;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Anexos
(
  UniversalID_Sub string,
  link string,
  aniopart string
)
STORED AS PARQUET
--PARTITIONED BY(aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos/results/anexos';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','Zdr_Lotus_Reclamos_Subreclamos_CatHistoria')

-- COMMAND ----------

-- DBTITLE 1,Tabla Catalogo Historia
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_CatHistoria
(
	UniversalID_Sub string,
    posicion int,
    FechaEntrada string,
    Estado string,
    TotalDias string,
    DiasVencidos string,
    DiasContabilizados string,
    aniopart string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos/results/cathistoria';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_SubReclamos_Historia')

-- COMMAND ----------

-- DBTITLE 1,Vista Historia (Catalogo 1)
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_SubReclamos_Historia
(
  UniversalID_Sub string,
  FechaEntrada string,
  Estado string,
  TotalDias	string,
  DiasVencidos string,
  DiasContabilizados string
)
STORED AS PARQUET
--PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/subreclamos/results/historia_1';