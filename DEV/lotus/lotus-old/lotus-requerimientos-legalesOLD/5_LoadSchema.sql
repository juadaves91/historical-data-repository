-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla Catalogo Tiempo
REFRESH TABLE process.zdp_lotus_requerimientos_legales_cat_tiempo;
INSERT OVERWRITE TABLE Requerimientos_Legales.zdr_lotus_requerimientos_legales_cattiempo
SELECT 
  fecha,
  date_format(fecha, 'dd/MM/yyyy') fecha_requerimiento_legal,
  anio,
  trimestre,
  num_mes,
  semana_mes,
  nombre_mes,
  nombre_mes_abre,
  dias_semana,
  nombre_dia_semana, 
  nombre_dia_semana_abre,
  dia_Anio
FROM process.zdp_lotus_requerimientos_legales_cat_tiempo;

-- COMMAND ----------

-- DBTITLE 1,Tabla Requerimientos (Particionada)
SET hive.exec.dynamic.partition.mode = nonstrict;
REFRESH TABLE Process.Zdp_Lotus_RequerimientosLegales;
INSERT OVERWRITE TABLE Requerimientos_Legales.Zdr_Lotus_Requerimientos PARTITION(aniopart)
SELECT  UniversalID,
        Filial,
        SucursalArea,
        Entesolicitante,
        NombreEnte,
        FechaOficio,
        TipoRequerimiento,
        Requerimiento,
        DiasSolucion,
        ConteoDiasSolucion_Homologado,
        DiasProrroga,
        Vinculado_Homologado,
        Ciudad,
        Funcionario,
        Direccion,
        Banco,
        CuentaDeposito,
        NumeroOficio,
        FechaVencimientoOficio,
        Autor,
        Creador,
        EmpresaTipo,
        Asunto,
        FechaMaximaEstado,
        Titulo1,
        Titulo2,
        Nombres,
        Dependencia,
        Region,
        Ubicacion,
        Telefono,
        EmpresaNombre,
        TelefonoExt,
        FechaEntregaDefinicion,
        VencidoDiasHabilesTotales,
        VencidoDiasHabilesEstadoActua,
        Estado,
        FechaEstado,
        TiempoMaximoEstado,
        VisualizadorFchMaxEstado,
        TiempoCotizado,
        VisualizadorTCotizado,
        PesosCotizados,
        VisualizadorPesosCotiz,
        FechaVencimiento,
        Responsable,
        VisualizadorResponsable,
        InformacionEmpleadoResponsableNombres,
        InformacionEmpleadoResponsableDependencia,
        InformacionEmpleadoResponsableRegion,
        InformacionEmpleadoResponsableUbicacion,
        InformacionEmpleadoResponsableTelefono,
        FechaRequerimientoLegal,
        NomPedido,
        ViceAtdPed,
        fecha_requerimiento_legal,
        Historia,
        AnexosAdicionales,
        AnexosPedido,
        Comentarios,
        DetallePedido,
        CedulaNit,
        busqueda,
        aniopart
FROM Process.Zdp_Lotus_RequerimientosLegales;

-- COMMAND ----------

-- DBTITLE 1,Tabla Requerimientos Anexos
set hive.exec.dynamic.partition.mode=nonstrict
REFRESH TABLE Process.Zdp_Lotus_RequerimientosLegales_Anexos;
INSERT OVERWRITE TABLE Requerimientos_Legales.Zdr_Lotus_Requerimientos_Anexos PARTITION(aniopart)
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_RequerimientosLegales_Anexos;