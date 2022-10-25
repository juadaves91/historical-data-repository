-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimerContacto_Reclamos
REFRESH TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos 
SELECT
  UniversalID,
  Autor,
  FechaSolucionadoPrimerContacto,
  fecha_solicitudes,
  SucursalCaptura,
  TipoProducto,
  TipoReclamo,
  NumeroIdentificacion,
  Responsable,
  CodigoCausalidad,
  Radicado,
  Estado,
  CodSucursalCaptura,
  Historia,
  Comentarios,
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda  
FROM Process.Zdp_Lotus_Reclamos_PrimerContacto_Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla PrimerContacto_Anexos
REFRESH TABLE Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Anexos 
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_PrimerContacto_Anexos;

-- COMMAND ----------

-- DBTITLE 1,Tabla GeneralPrimerContacto
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_PrimerContacto_GeneralPrimerContacto
SELECT
  UniversalId,
  FechaSolucionadoPrimerContacto AS FechaSolucionado,
  fecha_solicitudes,
  NumeroIdentificacion,
  Radicado,
  Responsable,
  Estado,
  TipoProducto AS Producto,
  TipoReclamo AS Reclamo,
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS busqueda
FROM 
  Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla DetallePrimerContacto
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_PrimerContacto_DetallePrimerContacto
SELECT UniversalId, 'Datos Generales' as vista, 1 as posicion, 'Autor' AS key, Autor AS value 
FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 2 as posicion, 'Fecha Solucionado Primer Contacto' AS key, FechaSolucionadoPrimerContacto AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 3 as posicion, 'Sucursal Captura' AS key, SucursalCaptura AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 4 as posicion, 'Tipo Producto' AS key, TipoProducto AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 5 as posicion, 'Tipo Reclamo' AS key, TipoReclamo AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 6 as posicion, 'Numero Identificacion' AS key, NumeroIdentificacion AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 7 as posicion, 'Responsable' AS key, Responsable AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 8 as posicion, 'Codigo Causalidad' AS key, CodigoCausalidad AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 9 as posicion, 'Radicado' AS key, Radicado AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 10 as posicion, 'Estado' AS key, Estado AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Datos Generales' as vista, 11 as posicion, 'Código Sucursal Captura' AS key, CodSucursalCaptura AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Historia' as vista, 12 as posicion, 'Historia' AS key, Historia AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
UNION ALL
SELECT UniversalId, 'Comentario' as vista, 13 as posicion, 'Comentarios' AS key, Comentarios AS value FROM Reclamos.Zdr_Lotus_Reclamos_PrimerContacto_Reclamos
ORDER BY UniversalId,key;