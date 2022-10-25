-- Databricks notebook source
-- DBTITLE 1,**Comando Requerido para la inserción en las tablas particionadas**
set hive.exec.dynamic.partition.mode=nonstrict

-- COMMAND ----------

-- DBTITLE 1,Tabla Catalogo Tiempo Reclamos
REFRESH TABLE process.zdp_lotus_reclamos_cat_tiempo;
INSERT OVERWRITE TABLE Reclamos.zdr_lotus_reclamos_cat_tiempo
SELECT 
  fecha,
  date_format(fecha, 'dd/MM/yyyy') fecha_solicitudes,
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
FROM process.zdp_lotus_reclamos_cat_tiempo;

-- COMMAND ----------

-- DBTITLE 1,Tabla Principal Reclamos
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;
INSERT OVERWRITE TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_Reclamos_Principal PARTITION(aniopart)
SELECT
  UniversalId,
  Radicado,
  FechaCreacion,
  ClientetieneRazon,
  AbonoTemporal,
  AbonoDefinitivo,
  Producto,
  TipoReclamo,
  fecha_solicitudes,
  translate(busqueda,"áéíóúÁÉÍÓÚñÑ","aeiouAEIOUnN")  AS Busqueda,
  aniopart
FROM
  Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Reclamos
REFRESH TABLE
INSERT OVERWRITE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Detalle PARTITION(aniopart)
SELECT UniversalId, 'Radicación' AS vista, 1 as posicion, 'Autor' AS key, Autor AS value, aniopart aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 2 as posicion, 'Fecha de creación' AS key, FechaCreacion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 3 as posicion, 'Sucursal Captura' AS key, SucursalCaptura AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 4 as posicion, 'Reportado Por' AS key, ReportadoPor AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 5 as posicion, 'Origen del Reporte' AS key, OrigenReporte AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 6 as posicion, 'Responsable' AS key, Responsable AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 7 as posicion, 'Abono Definitivo' AS key, AbonoDefinitivo AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 8 as posicion, 'Responsable del Error' AS key, ResponsableError AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 9 as posicion, 'Nombre del Responsable del Error' AS key, NombreResponsableError AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Radicación' AS vista, 10 as posicion, 'Cliente tiene la Razon' AS key, ClientetieneRazon AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 11 as posicion, 'Número de identificación' AS key, NumeroIdentificacion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 12 as posicion, 'Nombre' AS key, Nombre AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 13 as posicion, 'Segmento' AS key, Segmento AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 14 as posicion, 'Gerente de cuenta' AS key, GerenteCuenta AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 15 as posicion, 'Funcionario de la empresa' AS key, FuncionarioEmpresa AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 16 as posicion, 'Dirección de correspondencia' AS key, DireccionCorrespondencia AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 17 as posicion, 'Ciudad de correspondencia' AS key, CiudadCorrespondencia AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 18 as posicion, 'Código de ciudad' AS key, CodigoCiudad AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 19 as posicion, 'Departamento de correspondencia' AS key, DepartamentoCorrespondencia AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 20 as posicion, 'Teléfono(s)' AS key, Telefono AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 21 as posicion, 'Celular' AS key, Celular AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 22 as posicion, 'Autoriza envío de información via Email' AS key, AutorizaEmail AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 23 as posicion, 'Email' AS key, Email AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Cliente' AS vista, 24 as posicion, 'Fax' AS key, Fax AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 25 as posicion, 'Tiempo de respuesta' AS key, TiempoRespuesta AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 26 as posicion, 'Fecha de respuesta (dd/mm/aaaa)' AS key, FechaRespuesta AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 27 as posicion, 'Tiempo real de solución' AS key, TiempoRealSolucion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 28 as posicion, 'Fecha real de solución (dd/mm/aaaa)' AS key, FecharealSolucion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Fecha de solución / vencimiento' AS vista, 29 as posicion, 'Timpo estimado de solución' AS key, TiempoEstimadoSolucion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 30 as posicion, 'Producto' AS key, Producto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 31 as posicion, 'Clasificación' AS key, Clasificacion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 32 as posicion, 'Tipo de reclamo' AS key, TipoReclamo AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 33 as posicion, 'Número de producto' AS key, NumeroProducto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 34 as posicion, 'Código de compra' AS key, CodigoCompra AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 35 as posicion, 'Código de producto' AS key, CodigoProducto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 36 as posicion, 'Producto afectado' AS key, ProductoAfectado AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 37 as posicion, 'Número de tarjeta débito' AS key, NumeroTarjetaDebito AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 38 as posicion, 'Número de póliza' AS key, NumeroPoliza AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 39 as posicion, 'Número de encargo fiduciario' AS key, NumeroEncargoFiduciario AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 40 as posicion, 'Sucursal de radicación del producto' AS key, SucursalRadicacionProducto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 41 as posicion, 'Tipo de póliza ' AS key, TipoPoliza AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 42 as posicion, 'Número de tarjeta / cuenta que se debita' AS key, NumeroTarjetaCuentaDebita AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 43 as posicion, 'Ciudad de transacción / suceso' AS key, CiudadTransaccion AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 44 as posicion, 'Fecha de transacción' AS key, FechaTransaccionReclamo AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 45 as posicion, 'Sucursal / área de queja, felicitación o sugerencia' AS key, SucursalAreaQueja AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 46 as posicion, 'Código único de establecimiento' AS key, CodigoUnicoEstablecimiento AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 47 as posicion, 'Valor total en pesos' AS key, ValortotalPesos AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 48 as posicion, 'Valor total en dólares' AS key, ValortotalDolares AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 49 as posicion, 'Comentario' AS key, Comentario AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 50 as posicion, 'Observaciones (RappCollins)' AS key, Observaciones AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 51 as posicion, 'Mensaje de ayuda' AS key, MensajeAyuda AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Reclamo' AS vista, 52 as posicion, 'Soportes requeridos' AS key, SoportesRequeridos AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Convenios' AS vista, 53 as posicion, 'Código del convenio	' AS key, CodigoConvenio AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Convenios' AS vista, 54 as posicion, 'Nombre de la empresa' AS key, NombreEmpresa AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Convenios' AS vista, 55 as posicion, 'Nit de la empresa' AS key, NitEmpresa AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Convenios' AS vista, 56 as posicion, 'Código del canal' AS key, CodigoCanal AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Convenios' AS vista, 57 as posicion, 'Nombre del canal' AS key, NombreCanal AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Soportes' AS vista, 58 as posicion, 'Soportes recibidos del cliente' AS key, SoportesRecibidosCliente AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Soportes' AS vista, 59 as posicion, 'Cuáles no se han recibido' AS key, CualesnoseHanRecibido AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Soportes' AS vista, 60 as posicion, 'Se recibieron todos los soportes?' AS key, RecibieronSoportes AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Pantallas' AS vista, 69 as posicion, 'Pantallas que documentan el reclamo - Anexos' AS key, PantallasDocumentanReclamoAnexos AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Pantallas' AS vista, 70 as posicion, 'Pantallas que certifican la solución - Anexos' AS key, PantallasCertificanSolucionAnexos AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Pantallas' AS vista, 71 as posicion, 'Pantallas que documentan el reclamo - Texto	' AS key, PantallasDocumentanreclamoTexto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes
UNION ALL
SELECT UniversalId, 'Pantallas' AS vista, 72 as posicion, 'Pantallas que certifican la solución - Texto' AS key, PantallasCertificanSolucionTexto AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION ALL
SELECT UniversalId, 'Respuesta' AS vista, 73 as posicion, 'Respuesta (Respuesta)' AS key, Respuesta AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes

UNION all
SELECT UniversalId, 'Historia' AS vista, 74 as posicion, 'Comentarios y eventos' AS key, ComentariosEventos AS value, aniopart FROM Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos (Particionada)
SET hive.exec.dynamic.partition.mode = nonstrict;
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes PARTITION(aniopart)
SELECT
  UniversalID,
  fecha_solicitudes,
  NumeroIdentificacion,
  Producto,
  TipoReclamo,
  EtiquetasAdicionales,
  MensajeAyuda,
  SoportesRequeridos,
  ClasificacionLista,
  NumeroProducto,
  CodigoCompra,
  CodigoProducto,
  ProductoAfectado,
  NumeroTarjetaDebito,
  NumeroPoliza,
  NumeroEncargoFiduciario,
  SucursalRadicacionProducto,
  TipoPoliza,
  NumeroTarjetaCuentaDebita,
  CiudadTransaccion,
  FechaTransaccionReclamo,
  SucursalAreaQueja,
  CodigoUnicoEstablecimiento,
  ValortotalPesos,
  ValortotalDolares,
  ValoresEtiquetasAdicionales,
  Observaciones,
  Comentario,
  Autor,
  FechaCreacion,
  Radicado,
  SucursalCaptura,
  ReportadoPor,
  AbonoDefinitivo,
  NombreResponsableError,
  ResponsableError,
  ClientetieneRazon,
  MedioRespuestaCliente,
  UsuarioCierra,
  CausadAbono,
  Estado,
  Nombre,
  Segmento,
  Subsegmento,
  GerenteCuenta,
  FuncionarioEmpresa,
  DireccionCorrespondencia,
  CiudadCorrespondencia,
  DepartamentoCorrespondencia,
  Telefono,
  Email,
  Fax,
  OrigenReporte,
  PrefijoNumeroProducto,
  TiempoEstimadoSolucion,
  TiempoRealSolucion,
  FecharealSolucion,
  SoportesRecibidosCliente,
  CualesnoseHanRecibido,
  RecibieronSoportes,
  ComentariosEventos,
  TotalesTotalDias,
  TotalesDiasVencidos,
  TotalesDiasContabilizados,
  PantallasDocumentanReclamoAnexos,
  PantallasCertificanSolucionAnexos,
  Respuesta,
  Anexos,
  ResponsaReclamoLista,
  ResponsableReclamo,
  TelefonoResponsable_1,
  TelefonoResponsable_2,
  CodigoCiudad,
  Celular,
  AutorizaEmail,
  TiempoRespuesta,
  FechaRespuesta,
  CodigoConvenio,
  NombreEmpresa,
  NitEmpresa,
  CodigoCanal,
  NombreCanal,
  ValorTransaccion,
  FechaTransaccionTransacciones,
  OficinaTransaccion,
  NombreEstablecimiento,
  Moneda,
  CiudadConsignacion,
  NumeroCuenta,
  NumeroCheque,
  FechaConsignacion,
  PantallasDocumentanreclamoTexto,
  PantallasCertificanSolucionTexto,
  Clasificacion,
  Responsable,
  AbonoTemporal,
  IdSubreclamo,
  busqueda,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_Reclamos_Solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Tabla Catalago Historia (Particionada)
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Reclamos_CatHistoria;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatHistoria PARTITION(aniopart)
SELECT 
  UniversalId,
  Posicion,
  FechaEntrada,
  Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_Reclamos_CatHistoria;


-- COMMAND ----------

-- DBTITLE 1,Tabla Catalago Historia 1 (Particionada)
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatHistoria;
REFRESH TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;
INSERT OVERWRITE TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Historia
SELECT
  UniversalId,
  FechaEntrada,
  Estado,
  TotalDias,
  DiasVencidos,
  DiasContabilizados,
  aniopart
FROM 
  Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatHistoria
UNION ALL
SELECT UniversalId,
       '' AS FechaEntrada, 
       'Totales' AS Estado,
       TotalesTotalDias AS TotalDias,
       TotalesDiasVencidos AS DiasVencidos,
       TotalesDiasContabilizados AS DiasContabilizados,
       aniopart
FROM
  Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;

-- COMMAND ----------

-- DBTITLE 1,Tabla Catalago Transacciones Reclamos (Particionada)
REFRESH TABLE Process.Zdp_Lotus_Reclamos_Reclamos_CatTransacciones;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatTransaccion PARTITION(aniopart)
SELECT 
  UniversalId,
  Posicion,
  Valor,
  Fecha,
  Oficina,
  Referencia,
  Moneda,
  Ciudad,
  NoCuenta,
  NoCheque,
  FechaConsignacion,
  aniopart
FROM Process.Zdp_Lotus_Reclamos_Reclamos_CatTransacciones;

-- COMMAND ----------

-- DBTITLE 1,Tabla Reclamos_Anexos
set hive.exec.dynamic.partition.mode=nonstrict
REFRESH TABLE Process.Zdp_Lotus_reclamos_Reclamos_Anexos;
INSERT OVERWRITE TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos PARTITION(aniopart)
SELECT
  UniversalID,
  link,
  aniopart
FROM Process.Zdp_Lotus_reclamos_Reclamos_Anexos;