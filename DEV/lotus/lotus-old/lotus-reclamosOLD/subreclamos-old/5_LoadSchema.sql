-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamo Old(Particionada)
SET hive.exec.dynamic.partition.mode = nonstrict;
REFRESH TABLE Process.Zdp_Lotus_Reclamos_SubReclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
SELECT
  UniversalID,
  FechaCreacion,
  fecha_solicitudes,
  NumProducto,
  TipoCuenta_Homologado,
  NroCuenta,
  VlrTotal_1,
  VlrGMF,
  VlrComis,
  VlrRever,
  ClienteRazon,
  CargaClte,
  PGAreaAdmin,
  Comentarios,
  Autor,
  Creador,
  VlrTotal,
  PGTarjetas,
  Prefijo,
  DAbierto,
  Estado,
  Solucionado,
  DV,
  Radicado,
  DVSlnado,
  Estado_1,
  Responsable,
  Responsable_1,
  CedNit,
  NomCliente,
  Producto,
  TipoReclamo,
  NumProductoCalc,
  CampoDilig_Homologado,
  PGSucError,
  CodPGSucError,
  NroCtaTercero,
  TipoCtaTercero_Homologado,
  Historia,
  UniversalID_Padre,
  Busqueda,
  aniopart,
  ValorTotal
FROM Process.Zdp_Lotus_Reclamos_SubReclamos_Solicitudes_Old;
     

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Subreclamo Old
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal_Old 
SELECT
  UniversalId,
  FechaCreacion,
  fecha_solicitudes,
  Estado,
  NomCliente,
  Producto,
  TipoReclamo,
  UniversalID_Padre,
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda
FROM
  Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old;

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Subreclamo Old
--REFRESH TABLE Process.Zdp_Lotus_Reclamos_Subreclamos_Solicitudes_Old;
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_SubReclamos_Detalle_Old 
SELECT UniversalId, 'Servicio al Cliente' AS vista, 1 as posicion, 'Días vencidos Totales' AS key, DV AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Servicio al Cliente' AS vista, 2 as posicion, 'Días vencidos Abono Temporal' AS key, DVSlnado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old


UNION ALL
SELECT UniversalId, '' AS vista, 3 as posicion, 'Autor' AS key, Autor AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, '' AS vista, 4 as posicion, 'Creador' AS key, Creador AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, '' AS vista, 5 as posicion, 'Fecha Creación' AS key, FechaCreacion AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, '' AS vista, 6 as posicion, 'Número de Radicación del Subreclamo' AS key, Radicado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL

SELECT UniversalId, '' AS vista, 7 as posicion, ' ' AS key, Estado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, '' AS vista, 8 as posicion, 'Estado Subreclamo' AS key, Estado_1 AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos de Asignación' AS vista, 9 as posicion, 'Responsable' AS key, Responsable AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 10 as posicion, 'Cédula/Nit' AS key, CedNit AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 11 as posicion, 'Nombre' AS key, NomCliente AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 12 as posicion, 'Producto' AS key, Producto AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 13 as posicion, 'Tipo de Reclamo' AS key, TipoReclamo AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 14 as posicion, 'Número de Producto' AS key, concat(Prefijo, " - ", NumProducto) AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 15 as posicion, 'Tipo de Cuenta' AS key, TipoCuenta_Homologado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 16 as posicion, 'Número de Cuenta' AS key, NroCuenta AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 17 as posicion, 'Valor Total en Pesos' AS key, VlrTotal_1 AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 18 as posicion, 'Valor GMF' AS key, VlrGMF AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 19 as posicion, 'Valor Comisiones' AS key, VlrComis AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 20 as posicion, 'Valor Reversión' AS key, VlrRever AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 21 as posicion, 'El Cliente Tiene la Razón?' AS key, ClienteRazon AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 22 as posicion, 'Se le Carga al Cliente?' AS key, CargaClte AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 23 as posicion, 'Campo a Diligenciar' AS key, CampoDilig_Homologado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old 
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 24 as posicion, 'P y G Sucursal Error' AS key, PGSucError AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 25 as posicion, 'P y G Area Administrativa' AS key, PGAreaAdmin AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 26 as posicion, 'Cuenta Tercero' AS key, NroCtaTercero AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 27 as posicion, 'Tipo de Cuenta del Tercero' AS key, TipoCtaTercero_Homologado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos del subreclamo' AS vista, 28 as posicion, 'P y G tarjetas' AS key, PGTarjetas AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 29 as posicion, 'Tiempo Real de la Solución' AS key, DAbierto AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
UNION ALL
SELECT UniversalId, 'Datos de Fechas de Solución' AS vista, 30 as posicion, 'Fecha Real de la Solución' AS key, Solucionado AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old
                                                                                                                                          

UNION ALL
SELECT UniversalId, 'Comentarios' AS vista, 31 as posicion, '' AS key, Comentarios AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old

UNION ALL
SELECT UniversalId, 'Historia' AS vista, 32 as posicion, '' AS key, Historia AS value FROM Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Solicitudes_Old;



-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamo_Anexos
REFRESH TABLE Process.Zdp_Lotus_Reclamos_SubReclamos_Anexos_Old;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_SubReclamos_Anexos_Old
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_Subreclamos_Anexos_Old;