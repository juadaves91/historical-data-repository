-- Databricks notebook source
-- DBTITLE 1,Utilidades - Borrado Objeto
-- MAGIC %run
-- MAGIC /utilidades/Helpers

-- COMMAND ----------

-- DBTITLE 1,Borrar objetos viejos en DataLake
-- MAGIC %python
-- MAGIC fn_delete_folder('lotus-reclamos/reclamos') 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Reclamos;

-- COMMAND ----------

-- DBTITLE 1,Dimensión de Tiempo
DROP TABLE IF EXISTS Reclamos.zdr_Lotus_Reclamos_cat_tiempo;
CREATE EXTERNAL TABLE Reclamos.zdr_Lotus_Reclamos_cat_tiempo
(  
  fecha date,
  fecha_solicitudes string,
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
LOCATION '/mnt/lotus-reclamos/reclamos/results/cattiempo';

-- COMMAND ----------

-- DBTITLE 1,Tabla Solicitudes
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Solicitudes 
(
  UniversalID string,
  fecha_solicitudes string,
  NumeroIdentificacion string,
  Producto string,
  TipoReclamo string,
  EtiquetasAdicionales string,
  MensajeAyuda string,
  SoportesRequeridos string,
  ClasificacionLista string,
  NumeroProducto string,
  CodigoCompra string,
  CodigoProducto string,
  ProductoAfectado string,
  NumeroTarjetaDebito string,
  NumeroPoliza string,
  NumeroEncargoFiduciario string,
  SucursalRadicacionProducto string,
  TipoPoliza string,
  NumeroTarjetaCuentaDebita string,
  CiudadTransaccion string,
  FechaTransaccionReclamo string,
  SucursalAreaQueja string,
  CodigoUnicoEstablecimiento string,
  ValortotalPesos string,
  ValortotalDolares string,
  ValoresEtiquetasAdicionales string,
  Observaciones string,
  Comentario string,
  Autor string,
  FechaCreacion string,
  Radicado string,
  SucursalCaptura string,
  ReportadoPor string,
  AbonoDefinitivo string,
  NombreResponsableError string,
  ResponsableError string,
  ClientetieneRazon string,
  MedioRespuestaCliente string,
  UsuarioCierra string,
  CausadAbono string,
  Estado string,
  Nombre string,
  Segmento string,
  Subsegmento string,
  GerenteCuenta string,
  FuncionarioEmpresa string,
  DireccionCorrespondencia string,
  CiudadCorrespondencia string,
  DepartamentoCorrespondencia string,
  Telefono string,
  Email string,
  Fax string,
  OrigenReporte string,
  PrefijoNumeroProducto string,
  TiempoEstimadoSolucion string,
  TiempoRealSolucion string,
  FecharealSolucion string,
  SoportesRecibidosCliente string,
  CualesnoseHanRecibido string,
  RecibieronSoportes string,
  ComentariosEventos string,
  TotalesTotalDias string,
  TotalesDiasVencidos string,
  TotalesDiasContabilizados string,
  PantallasDocumentanReclamoAnexos string,
  PantallasCertificanSolucionAnexos string,
  Respuesta string,
  Anexos string,
  ResponsaReclamoLista string,
  ResponsableReclamo string,
  TelefonoResponsable_1 string,
  TelefonoResponsable_2 string,
  CodigoCiudad string,
  Celular string,
  AutorizaEmail string,
  TiempoRespuesta string,
  FechaRespuesta string,
  CodigoConvenio string,
  NombreEmpresa string,
  NitEmpresa string,
  CodigoCanal string,
  NombreCanal string,
  ValorTransaccion string,
  FechaTransaccionTransacciones string,
  OficinaTransaccion string,
  NombreEstablecimiento string,
  Moneda string,
  CiudadConsignacion string,
  NumeroCuenta string,
  NumeroCheque string,
  FechaConsignacion string,
  PantallasDocumentanreclamoTexto string,
  PantallasCertificanSolucionTexto string,
  Clasificacion string,
  Responsable string,
  AbonoTemporal string,
  IdSubreclamo string,
  busqueda string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/reclamos';


-- COMMAND ----------

-- DBTITLE 1,Tabla Anexos
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_Anexos
(
  UniversalID	string,
  link string
)
STORED AS PARQUET
PARTITIONED BY(aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/anexos';

-- COMMAND ----------

-- DBTITLE 1,Catalogo Transacciones
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatTransaccion;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatTransaccion
(
  UniversalId string,
  posicion int,
  Valor string,
  Fecha string,
  Oficina string,
  Referencia string,
  Moneda string,
  Ciudad string,
  NoCuenta string,
  NoCheque string,
  FechaConsignacion string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/cattransacciones';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','Zdr_uvw_Lotus_Reclamos_Reclamos_Principal')

-- COMMAND ----------

-- DBTITLE 1,Vista Principal
CREATE EXTERNAL TABLE Reclamos.Zdr_uvw_Lotus_Reclamos_Reclamos_Principal
(
  UniversalId string,
  Radicado string,
  FechaCreacion string,
  ClientetieneRazon string,
  AbonoTemporal string,
  AbonoDefinitivo string,
  Producto string,
  TipoReclamo string,
  fecha_solicitudes string,
  busqueda string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/principal';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_Reclamos_Detalle')

-- COMMAND ----------

-- DBTITLE 1,Tabla Detalle Reclamos
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Detalle
(
  UniversalId	string,
  vista string,	
  posicion int,	
  key	string,	
  value string
)
STORED AS PARQUET
LOCATION '/mnt/lotus-reclamos/reclamos/results/reclamosdetalle';

-- COMMAND ----------

-- DBTITLE 1,Catalogo Historia
DROP TABLE IF EXISTS Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatHistoria;
CREATE EXTERNAL TABLE Reclamos.Zdr_Lotus_Reclamos_Reclamos_CatHistoria
(
  UniversalId string,
  posicion Int,
  FechaEntrada string,
  Estado string,
  TotalDias string,
  DiasVencidos string,
  DiasContabilizados string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/cathistoria';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fn_view_or_table('Reclamos','zdr_uvw_lotus_Reclamos_Reclamos_Historia')

-- COMMAND ----------

-- DBTITLE 1,Vista Historia (Catalogo 1)
CREATE EXTERNAL TABLE Reclamos.zdr_uvw_lotus_Reclamos_Reclamos_Historia
(
  UniversalId string,
  FechaEntrada string,
  Estado string,
  TotalDias string,
  DiasVencidos string,
  DiasContabilizados string
)
STORED AS PARQUET
PARTITIONED BY (aniopart string)
LOCATION '/mnt/lotus-reclamos/reclamos/results/cathistoria_1';