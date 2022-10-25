-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS Requerimientos_Legales;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de Tiempo
DROP TABLE IF EXISTS Requerimientos_Legales.zdr_lotus_requerimientos_legales_cattiempo;
CREATE EXTERNAL TABLE Requerimientos_Legales.zdr_lotus_requerimientos_legales_cattiempo
(  
  fecha date,
  fecha_requerimiento_legal string,
  anio int,
  trimestre BIGINT,
  num_mes INT,
  semana_mes STRING,
  nombre_mes STRING,
  nombre_mes_abre STRING,
  dias_semana STRING,
  nombre_dia_semana STRING, 
  nombre_dia_semana_abre STRING,
  dia_anio INT
)
LOCATION '/mnt/lotus-requerimientos-legales/results/cattiempo';

-- COMMAND ----------

-- DBTITLE 1,Tabla Requerimientos
DROP TABLE IF EXISTS Requerimientos_Legales.Zdr_Lotus_Requerimientos;
CREATE EXTERNAL TABLE Requerimientos_Legales.Zdr_Lotus_Requerimientos
(
  UniversalID string,
  Filial string,
  SucursalArea string,
  Entesolicitante string,
  NombreEnte string,
  FechaOficio string,
  TipoRequerimiento string,
  Requerimiento string,
  DiasSolucion string,
  ConteoDiasSolucion_Homologado string,
  DiasProrroga string,
  Vinculado_Homologado string,
  Ciudad string,
  Funcionario string,
  Direccion string,
  Banco string,
  CuentaDeposito string,
  NumeroOficio string,
  FechaVencimientoOficio string,
  Autor string,
  Creador string,
  EmpresaTipo string,
  Asunto string,
  FechaMaximaEstado string,
  Titulo1 string,
  Titulo2 string,
  Nombres string,
  Dependencia string,
  Region string,
  Ubicacion string,
  Telefono string,
  EmpresaNombre string,
  TelefonoExt string,
  FechaEntregaDefinicion string,
  VencidoDiasHabilesTotales string,
  VencidoDiasHabilesEstadoActua string,
  Estado string,
  FechaEstado string,
  TiempoMaximoEstado string,
  VisualizadorFchMaxEstado string,
  TiempoCotizado string,
  VisualizadorTCotizado string,
  PesosCotizados string,
  VisualizadorPesosCotiz string,
  FechaVencimiento string,
  Responsable string,
  VisualizadorResponsable string,
  InformacionEmpleadoResponsableNombres string,
  InformacionEmpleadoResponsableDependencia string,
  InformacionEmpleadoResponsableRegion string,
  InformacionEmpleadoResponsableUbicacion string,
  InformacionEmpleadoResponsableTelefono string,
  FechaRequerimientoLegal string,
  NomPedido string,
  ViceAtdPed string,     
  fecha_requerimiento_legal string,
  Historia string,
  AnexosAdicionales string,
  AnexosPedido string,
  Comentarios string,
  DetallePedido string,
  CedulaNit string,
  busqueda string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-requerimientos-legales/results/requerimientos';

-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP TABLE IF EXISTS Requerimientos_Legales.Zdr_Lotus_Requerimientos_Anexos;
CREATE EXTERNAL TABLE Requerimientos_Legales.Zdr_Lotus_Requerimientos_Anexos
(
  UniversalID string,
  link string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-requerimientos-legales/results/anexos';

-- COMMAND ----------

-- DBTITLE 1,Vista Principal
DROP VIEW IF EXISTS Requerimientos_Legales.Zdr_uvw_Lotus_Requerimientos_Principal;
CREATE VIEW Requerimientos_Legales.Zdr_uvw_Lotus_Requerimientos_Principal AS
SELECT
  UniversalId,
  Asunto,
  TipoRequerimiento,
  fecha_requerimiento_legal,
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda
FROM
  Requerimientos_Legales.Zdr_Lotus_Requerimientos;

-- COMMAND ----------

-- DBTITLE 1,Vista Detalle Requerimientos Legales (unpivot)
DROP VIEW IF EXISTS Requerimientos_Legales.zdr_uvw_lotus_Requerimientos_Detalle;
CREATE VIEW Requerimientos_Legales.zdr_uvw_lotus_Requerimientos_Detalle AS
SELECT UniversalId, 'Encabezado' AS vista, 1 as posicion, 'Autor' AS key, Autor AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Encabezado' AS vista, 2 as posicion, 'Creador' AS key, Creador AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Encabezado' AS vista, 3 as posicion, 'Fecha creación' AS key, FechaRequerimientoLegal AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 4 as posicion, 'Nombres' AS key, Nombres AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 5 as posicion, 'Dependencia' AS key, Dependencia AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 6 as posicion, 'Región' AS key, Region AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 7 as posicion, 'Ubicación' AS key, Ubicacion AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 8 as posicion, 'Teléfono' AS key, Telefono AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 9 as posicion, 'Tipo Empresa' AS key, EmpresaTipo AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 10 as posicion, 'Nombre Empresa' AS key, EmpresaNombre AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Solicitante' AS vista, 11 as posicion, 'Ext Teléfono Empresa' AS key, TelefonoExt AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 12 as posicion, 'Fecha de entrega de definición' AS key, FechaEntregaDefinicion AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 13 as posicion, 'Vencido (días hábiles totales)' AS key, IF(VencidoDiasHabilesTotales = '', '0', VencidoDiasHabilesTotales) AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Encabezado Datos Generales' AS vista, 14 as posicion, 'Vencido (días hábiles estado actual)' AS key, IF(VencidoDiasHabilesEstadoActua = '', '0', VencidoDiasHabilesEstadoActua) AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 15 as posicion, '' AS key, ViceAtdPed AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 16 as posicion, 'Tipo Solicitud' AS key, NomPedido AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Generales' AS vista, 17 as posicion, 'Asunto' AS key, Asunto AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 18 as posicion, 'Estado' AS key, Estado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 19 as posicion, 'Fecha del estado' AS key, FechaEstado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 20 as posicion, 'Tiempo máximo en el estado (días)' AS key, TiempoMaximoEstado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 21 as posicion, 'Fecha máxima del estado' AS key, FechaMaximaEstado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 22 as posicion, 'Tiempo cotizado (días)' AS key, TiempoCotizado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Estado y Acuerdos de Servicio' AS vista, 23 as posicion, 'Fecha vencimiento' AS key, FechaVencimiento AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Asignación de Responsable' AS vista, 24 as posicion, 'Responsable' AS key, Responsable AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 25 as posicion, 'Nombres' AS key, InformacionEmpleadoResponsableNombres AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 26 as posicion, 'Dependencia' AS key, InformacionEmpleadoResponsableDependencia AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 27 as posicion, 'Región' AS key, InformacionEmpleadoResponsableRegion AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 28 as posicion, 'Ubicación' AS key, InformacionEmpleadoResponsableRegion AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Información del Empleado Responsable' AS vista, 29 as posicion, 'Teléfono' AS key, InformacionEmpleadoResponsableTelefono AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Detalle del Pedido' AS vista, 30 as posicion, 'Detalle pedido' AS key, DetallePedido AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 31 as posicion, 'Filial' AS key, Filial AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 32 as posicion, 'Sucursal o Área origen' AS key, SucursalArea AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 33 as posicion, 'Ente solicitante' AS key, Entesolicitante AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Origen Solicitud' AS vista, 34 as posicion, 'Nombre del Ente' AS key, NombreEnte AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 35 as posicion, 'Número del oficio' AS key, NumeroOficio AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 36 as posicion, 'Fecha del oficio' AS key, FechaOficio AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 37 as posicion, 'Tipo de requerimiento' AS key, TipoRequerimiento AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 38 as posicion, 'Requerimiento' AS key, Requerimiento AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 39 as posicion, 'Días para su solución' AS key, concat(DiasSolucion, ' - ', ConteoDiasSolucion_Homologado) AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 40 as posicion, 'Días de prórroga' AS key, DiasProrroga AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Requerimiento' AS vista, 41 as posicion, 'Fecha de vencimiento del oficio' AS key, FechaVencimientoOficio AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Detalle' AS vista, 42 as posicion, 'Cédula o Nit' AS key, CedulaNit AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Detalle' AS vista, 43 as posicion, '¿Vinculado?' AS key, Vinculado_Homologado AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Entidad' AS vista, 44 as posicion, 'Ciudad' AS key, Ciudad AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Entidad' AS vista, 45 as posicion, 'Funcionario' AS key, Funcionario AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Entidad' AS vista, 46 as posicion, 'Dirección' AS key, Direccion AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Entidad' AS vista, 47 as posicion, 'Banco' AS key, Banco AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Datos Entidad' AS vista, 48 as posicion, 'Cuenta déposito' AS key, CuentaDeposito AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Comentarios' AS vista, 49 as posicion, 'Comentarios' AS key, Comentarios AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos
UNION ALL
SELECT UniversalId, 'Historia' AS vista, 50 as posicion, 'Historia' AS key, Historia AS value FROM Requerimientos_Legales.Zdr_Lotus_Requerimientos