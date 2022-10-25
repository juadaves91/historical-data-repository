-- Databricks notebook source
-- MAGIC %python
-- MAGIC def fn_vire_or_table(ar_parameter):
-- MAGIC   try:
-- MAGIC     spark.sql('drop view Reclamos.'+ar_parameter)
-- MAGIC   except:
-- MAGIC     spark.sql('drop table Reclamos.'+ar_parameter)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimerContacto
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
(
  UniversalID string,
  Autor string,
  FechaSolucionadoPrimerContacto string,
  fecha_solicitudes string,
  SucursalCaptura string,
  TipoProducto string,
  TipoReclamo string,
  NumeroIdentificacion string,
  Responsable string,
  CodigoCausalidad string,
  Radicado string,
  Estado string,
  CodSucursalCaptura string,
  Historia string,
  Comentarios string,
  busqueda string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto/results/reclamos';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Anexos;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Anexos
(
  UniversalID string,
  link string,
  aniopart string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto/results/anexos';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_vire_or_table('Zdr_uvw_Lotus_Reclamos_PrimerContacto_GeneralPrimerContacto')

-- COMMAND ----------

-- DBTITLE 1,Tabla GeneralPrimerContacto
DROP TABLE IF EXISTS Reclamos.Zdr_uvw_Lotus_Reclamos_PrimerContacto_GeneralPrimerContacto;
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_PrimerContacto_GeneralPrimerContacto
(
  UniversalID string,
  FechaSolucionado string,
  fecha_solicitudes string,
  NumeroIdentificacion string,
  Radicado string,
  Responsable string,
  Estado string,
  Producto string,
  Reclamo string, 
  busqueda string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto/results/generalprimercontacto';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_vire_or_table('zdr_uvw_lotus_Reclamos_PrimerContacto_DetallePrimerContacto')

-- COMMAND ----------

-- DBTITLE 1,Tabla DetallePrimerContacto
DROP TABLE IF EXISTS Reclamos.zdr_uvw_lotus_Reclamos_PrimerContacto_DetallePrimerContacto;
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_PrimerContacto_DetallePrimerContacto
(
  UniversalId	string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto/results/detallegeneralprimercontacto';