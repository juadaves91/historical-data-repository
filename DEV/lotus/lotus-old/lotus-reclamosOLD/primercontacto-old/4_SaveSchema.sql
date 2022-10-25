-- Databricks notebook source
-- DBTITLE 1,Utilidades/BorradoObj
-- MAGIC %run /utilidades/Helpers

-- COMMAND ----------

-- DBTITLE 1,Borrar objetos viejos en DataLake
-- MAGIC %python
-- MAGIC fn_delete_folder('lotus-reclamos/primercontacto-old') 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes PrimerContacto Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old 
(
  UniversalID string,
  CedNit string,
  FechaCreacion string,
  fecha_solicitudes string,
  Comentarios string,
  Producto string,
  TipoReclamo string,
  dat_SlnPC string,
  nom_Responsable string,
  Estado string,
  CodCausalidad string,
  Historia string,
  Busqueda string,
  aniopart string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto-old/results/primercontacto-old';

-- COMMAND ----------

-- DBTITLE 1,Borrado de objetos tipo vista - Tabla principal Old
-- MAGIC %python
-- MAGIC fn_view_or_table("Reclamos", "Zdr_uvw_Lotus_Reclamos_PrimerContacto_Principal_Old") 

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Old
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_PrimerContacto_Principal_Old 
(
    UniversalID	string,	
    CedNit string,	
    fecha_solicitudes string,	
    Estado string,	
    Responsable	string,	
    Producto string,	
    TipoReclamo	string,	
    Busqueda string	
) 
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto-old/results/tabla-principal-old';

-- COMMAND ----------

-- DBTITLE 1,Borrado de objetos tipo vista - Tabla Detalle PrimerContacto Old (unpivot)
-- MAGIC %python
-- MAGIC fn_view_or_table("Reclamos","zdr_uvw_lotus_Reclamos_PrimerContacto_Detalle_Old")

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle PrimerContacto Old (unpivot)
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_PrimerContacto_Detalle_Old
(
  UniversalId string,	
  vista string,	
  posicion int,	
  key string,	
  value string	
) 
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto-old/results/tabla-detalle-primer-contacto-old';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos Old
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_PrimerContato_Anexos_Old;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContato_Anexos_Old
(
  UniversalID	string,
  link string,
  aniopart string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/primercontacto-old/results/anexos-old';