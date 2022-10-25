-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Old
REFRESH TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_PrimerContacto_Principal_Old
SELECT
    UniversalID,
    CedNit,
    fecha_solicitudes,
    Estado,
    Responsable,
    Producto,
    TipoReclamo,
    translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda
FROM 
  Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimerContacto Old (Particionada)
SET hive.exec.dynamic.partition.mode = nonstrict;
REFRESH TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
SELECT
  UniversalID,
  CedNit,
  FechaCreacion,
  fecha_solicitudes,
  Comentarios,
  Producto,
  TipoReclamo,
  dat_SlnPC,
  Responsable,
  Estado,
  CodCausalidad,
  Historia,
  Busqueda,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
  

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle PrimerContacto Old (unpivot)
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_PrimerContacto_Detalle_Old                        
SELECT UniversalId, '' AS vista, 1 as posicion, 'Estado' AS key, Estado AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 2 as posicion, 'Cédula/Nit' AS key, CedNit AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 3 as posicion, 'Producto' AS key, Producto AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 4 as posicion, 'Tipo de Reclamo' AS key, TipoReclamo AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 5 as posicion, 'Responsable' AS key, nom_Responsable AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 6 as posicion, 'Código de Causalidad' AS key, nom_Responsable AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old 
UNION ALL
SELECT UniversalId, 'Comentarios' AS vista, 7 as posicion, ' ' AS key, Comentarios AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old 
UNION ALL
SELECT UniversalId, 'Historia' AS vista, 8 as posicion, ' ' AS key, Historia AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Solicitudes_Old

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimeContacto Anexos Old
set hive.exec.dynamic.partition.mode=nonstrict
REFRESH TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContato_Anexos_Old
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos_Old;