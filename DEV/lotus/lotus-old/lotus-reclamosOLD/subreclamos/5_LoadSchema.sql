-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamos_Reclamos (Particionada)
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
SELECT
  	UniversalID_Sub,
    UniversalID,
	Autor,
	FechaCreacion,
	fecha_solicitudes,
	Responsable,
	Estado,
	Subreclamo,
	NumeroIdentificacion,
	Nombre,
	Producto,
	TipoReclamo,
	NumeroProducto,
	TipoCuenta,
	NumeroCuenta,
	ValorTotalPesos,
	ValorGMF,
	ValorComisiones,
	ValorReversion,
	ClienteTieneRazon,
	CampoDiligenciar,
	CargaCliente,
	SucursalSobrante,
	FechaSobrante,
	PyGSucursalError,
	PyGAreaAdministrativa,
	CuentaTercero,
	TipoCuentaTercero,
	PyGTarjetas,
	TiempoRealSolucion,
	FechaRealSolucion,
	Comentario,
	Historia,
	TotalDias,
	DiasVencidos,
	DiasContabilizados,
    aniopart
FROM Process.Zdp_Lotus_Reclamos_Subreclamos_Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamos Principal
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_SubReclamos_Principal
SELECT
  UniversalID_Sub,
  UniversalID,
  Subreclamo,
  FechaCreacion,
  Autor,
  Producto,
  TipoReclamo
FROM
  Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamos Detalle
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_Subreclamos_Detalle
SELECT UniversalID_Sub, 'Datos Generales' AS vista, 1 as posicion, 'Autor' AS key, Autor AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Datos Generales' AS vista, 2 as posicion, 'FechaCreacion' AS key, FechaCreacion AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Datos Generales' AS vista, 3 as posicion, 'Responsable' AS key, Responsable AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Datos Generales' AS vista, 4 as posicion, 'Estado' AS key, Estado AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Datos Generales' AS vista, 5 as posicion, 'Subreclamo' AS key, Subreclamo AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL

SELECT UniversalID_Sub, 'Subreclamos' AS vista, 6 as posicion, 'NumeroIdentificacion' AS key, NumeroIdentificacion AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 7 as posicion, 'Nombre' AS key, Nombre AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 8 as posicion, 'Producto' AS key, Producto AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 9 as posicion, 'TipoReclamo' AS key, TipoReclamo AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 10 as posicion, 'NumeroProducto' AS key, NumeroProducto AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 11 as posicion, 'TipoCuenta' AS key, TipoCuenta AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 12 as posicion, 'NumeroCuenta' AS key, NumeroCuenta AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 13 as posicion, 'ValorTotalPesos' AS key, ValorTotalPesos AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 14 as posicion, 'ValorGMF' AS key, ValorGMF AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 15 as posicion, 'ValorComisiones' AS key, ValorComisiones AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 16 as posicion, 'ValorReversion' AS key, ValorReversion AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 17 as posicion, 'ClienteTieneRazon' AS key, ClienteTieneRazon AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 18 as posicion, 'CampoDiligenciar' AS key, CampoDiligenciar AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 19 as posicion, 'CargaCliente' AS key, CargaCliente AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 20 as posicion, 'SucursalSobrante' AS key, SucursalSobrante AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 21 as posicion, 'FechaSobrante' AS key, FechaSobrante AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 22 as posicion, 'PyGSucursalError' AS key, PyGSucursalError AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 23 as posicion, 'PyGAreaAdministrativa' AS key, PyGAreaAdministrativa AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 24 as posicion, 'CuentaTercero' AS key, CuentaTercero AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 25 as posicion, 'TipoCuentaTercero' AS key, TipoCuentaTercero AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Subreclamos' AS vista, 26 as posicion, 'PyGTarjetas' AS key, PyGTarjetas AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL

SELECT UniversalID_Sub, 'Fecha de Solucion' AS vista, 27 as posicion, 'TiempoRealSolucion' AS key, TiempoRealSolucion AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL
SELECT UniversalID_Sub, 'Fecha de Solucion' AS vista, 28 as posicion, 'FechaRealSolucion' AS key, FechaRealSolucion AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL

SELECT UniversalID_Sub, 'Comentario' AS vista, 29 as posicion, 'Comentario' AS key, Comentario AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos
UNION ALL

SELECT UniversalID_Sub, 'Historia' AS vista, 30 as posicion, 'Historia' AS key, Historia AS value FROM Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos ORDER BY UniversalID_Sub,posicion;

-- COMMAND ----------

-- DBTITLE 1,Tabla Subreclamos_Anexos
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Subreclamos_Anexos;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Anexos
SELECT
  UniversalID_Sub,
  link,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_Subreclamos_Anexos;

-- COMMAND ----------

-- DBTITLE 1,Tabla Catálogo_Historia
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Subreclamos_CatHistoria;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_CatHistoria
SELECT
  	UniversalID_Sub,
	posicion,
    FechaEntrada,
    Estado,
    TotalDias,
    DiasVencidos,
    DiasContabilizados, 
    aniopart
FROM Process.Zdp_Lotus_Reclamos_Subreclamos_CatHistoria;

-- COMMAND ----------

-- DBTITLE 1,Tabla Catálogo_Historia_1
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_CatHistoria;
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_SubReclamos_Historia 
SELECT
  UniversalID_Sub,
  FechaEntrada,
  Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados
FROM 
  Reclamos.Zdr_Lotus_Reclamos_Subreclamos_CatHistoria
UNION ALL
SELECT 
  UniversalID_Sub,
  '' AS FechaEntrada, 
  'Totales' AS Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados
FROM
  Reclamos.Zdr_Lotus_Reclamos_Subreclamos_Reclamos;