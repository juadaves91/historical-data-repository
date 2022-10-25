# Databricks notebook source
# MAGIC %md
# MAGIC # ZONA DE PROCESADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Descripción: Se requiere efectuar refresco de la tabla cada vez que se insertan o actualizan datos antes de ser consultada.
# MAGIC Fecha: 25/02/2020
# MAGIC Responsable: Juan David Escobar
# MAGIC */
# MAGIC 
# MAGIC REFRESH TABLE Default.parametros;

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_LOTUS_RECLASIFICADOS_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Creación Panel Principal
sql_String = "SELECT \
    UniversalId \
    ,cast(Str_Radicado as int) Radicado \
    ,str_cliSegmento Segmento \
    ,str_cliGerente Gerente \
    ,str_cliNombre RazonSocial \
    ,str_cliidentificacion NumeroIdentificacion \
    ,Dat_Fcreacion FechaCreacion \
    ,translate(concat(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion) \
    ,upper(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion)) \
    ,lower(concat(Str_Radicado,' ',str_cliSegmento,' ',str_cliGerente,' ',str_cliNombre,' ',str_cliidentificacion))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda \
    ,IF (length(concat_ws(',', attachments)) > 0, CONCAT('" +ValorX + ValorY + "/', UniversalID,'.zip'" + "), '') link \
    ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio \
    ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes \
  FROM \
    Zdc_Lotus_Reclasificados_TablaIntermedia"
panel_principal_reclasificados = sqlContext.sql(sql_String)
sqlContext.registerDataFrameAsTable(panel_principal_reclasificados,"Zdp_Lotus_Reclasificados_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Creación Panel Detalle
panel_detalle_reclasificados = sqlContext.sql("\
  SELECT UniversalId, 'Datos Solicitud' AS vista, 1 as posicion, 'Fecha de Creacion' AS key, Dat_Fcreacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Datos Solicitud' AS vista, 2 as posicion, 'Fecha de Decision Por' AS key, Dat_Fdesicion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Datos Solicitud' AS vista, 3 as posicion, 'Creado Por' AS key, Str_Autor AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Datos Solicitud' AS vista, 4 as posicion, 'Reclasificador' AS key, Str_Reclasificador AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Estado' AS vista, 5 as posicion, 'Estado Actual' AS key, Str_Estado AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Estado' AS vista, 6 as posicion, 'Asignado A' AS key, str_Responsable AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 7 as posicion, 'Numero de Identificacion' AS key, str_cliidentificacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 8 as posicion, 'Tipo Identificacion' AS key, Str_TipIdentificacion AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 9 as posicion, 'Razon Social' AS key, str_cliNombre AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 10 as posicion, 'Gerente' AS key, str_cliGerente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 11 as posicion, 'Codigo Oficial' AS key, str_CodGerente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 12 as posicion, 'Region' AS key, Str_Region AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 13 as posicion, 'Sucursal' AS key, txtSucursal AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 14 as posicion, 'Calificacion Interna Actual' AS key, txtCalificacionInternaCliente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Informacion Cliente' AS vista, 15 as posicion, 'Puntaje' AS key, txtPuntajeCliente AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
    UNION ALL \
  SELECT UniversalId, 'Registro Historico' AS vista, 16 as posicion, 'Registro Historico' AS key, historia AS value, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia")
sqlContext.registerDataFrameAsTable(panel_detalle_reclasificados,"Zdp_Lotus_Reclasificados_Panel_Detalle")

# COMMAND ----------

# DBTITLE 1,Creación Panel Hijos
panel_hijos_reclasificados = sqlContext.sql("\
  SELECT UniversalId, 1 as posicion, Doc1 AS Doc, ok1 as ok_Doc, ok1_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 2 as posicion, Doc2 AS Doc, ok2 as ok_Doc, ok2_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 3 as posicion, Doc3 AS Doc, ok3 as ok_Doc, ok3_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 4 as posicion, Doc4 AS Doc, ok4 as ok_Doc, ok4_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 5 as posicion, Doc5 AS Doc, ok5 as ok_Doc, ok5_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 6 as posicion, Doc6 AS Doc, ok6 as ok_Doc, ok6_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 7 as posicion, Doc7 AS Doc, ok7 as ok_Doc, ok7_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 8 as posicion, Doc8 AS Doc, ok8 as ok_Doc, ok8_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 9 as posicion, Doc9 AS Doc, ok9 as ok_Doc, ok9_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 10 as posicion, Doc10 AS Doc, ok10 as ok_Doc, ok10_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 11 as posicion, Doc11 AS Doc, ok11 as ok_Doc, ok11_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 12 as posicion, Doc12 AS Doc, ok12 as ok_Doc, ok12_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 13 as posicion, Doc13 AS Doc, ok13 as ok_Doc, ok13_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 14 as posicion, Doc14 AS Doc, ok14 as ok_Doc, ok14_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 15 as posicion, Doc15 AS Doc, ok15 as ok_Doc, ok15_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 16 as posicion, Doc16 AS Doc, ok16 as ok_Doc, ok16_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 17 as posicion, Doc17 AS Doc, ok17 as ok_Doc, ok17_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 18 as posicion, Doc18 AS Doc, ok18 as ok_Doc, ok18_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia \
  UNION ALL \
  SELECT UniversalId, 19 as posicion, Doc19 AS Doc, ok19 as ok_Doc, ok19_1 as Pic_Doc ,cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-4,4) as int) AS anio, cast(substr(substring_index(Dat_Fcreacion, ' ', 1),-7,2) as int) AS mes FROM Zdc_Lotus_Reclasificados_TablaIntermedia")
sqlContext.registerDataFrameAsTable(panel_hijos_reclasificados,"Zdp_Lotus_Reclasificados_PanelHijos")

# COMMAND ----------

# DBTITLE 1,Creación Panel Masivos
from pyspark.sql.functions import col,substring,substring_index,translate
masivos_reclasificados_df = reclasificados_df.select(col('UniversalID'),
                                                     col('txtTipoReclasificado').alias('TipoReclasificado'),
                                                     col('Dat_Fcreacion').alias('FechaCreacion'),
                                                     col('Str_Autor').alias('CreadoPor'),
                                                     col('str_cliidentificacion').alias('NumeroIdentificacion'),
                                                     col('str_cliTipIdentificacion').alias('TipoIdentificacion'),
                                                     col('str_cliNombre').alias('RazonSocial'),
                                                     col('str_cliGerente').alias('Gerente'),
                                                     col('str_CodGerente').alias('CodigoOficial'),
                                                     col('Str_Region').alias('Region'),
                                                     col('txtSucursal').alias('Sucursal'),
                                                     col('txtCalificacionInternaCliente').alias('CalificacionInternaActual'),
                                                     col('Str_Radicado').alias('Radicado'),
                                                     col('Dat_Fdesicion').alias('FechaDecision'),
                                                     col('Str_Reclasificador').alias('Reclasificador'),
                                                     col('Str_Estado').alias('EstadoActual'),
                                                     col('str_Responsable').alias('AsignadoA'),
                                                     col('str_cliSegmento').alias('Segmento'),
                                                     col('txtPuntajeCliente').alias('Puntaje'),
                                                     translate('Historia','\r\n','  .  ').alias('RegistroHistorico'),
                                                     col('Doc1'),
                                                     col('Doc2'),
                                                     col('Doc3'),
                                                     col('Doc4'),
                                                     col('Doc5'),
                                                     col('Doc6'),
                                                     col('Doc7'),
                                                     col('Doc8'),
                                                     col('Doc9'),
                                                     col('Doc10'),
                                                     col('Doc11'),
                                                     col('Doc12'),
                                                     col('Doc13'),
                                                     col('Doc14'),
                                                     col('Doc15'),
                                                     col('Doc16'),
                                                     col('Doc17'),
                                                     col('Doc18'),
                                                     col('Doc19'),
                                                     col('ok1'),
                                                     col('ok2'),
                                                     col('ok3'),
                                                     col('ok4'),
                                                     col('ok5'),
                                                     col('ok6'),
                                                     col('ok7'),
                                                     col('ok8'),
                                                     col('ok9'),
                                                     col('ok10'),
                                                     col('ok11'),
                                                     col('ok12'),
                                                     col('ok13'),
                                                     col('ok14'),
                                                     col('ok15'),
                                                     col('ok16'),
                                                     col('ok17'),
                                                     col('ok18'),
                                                     col('ok19'),
                                                     col('ok1_1'),
                                                     col('ok2_1'),
                                                     col('ok3_1'),
                                                     col('ok4_1'),
                                                     col('ok5_1'),
                                                     col('ok6_1'),
                                                     col('ok7_1'),
                                                     col('ok8_1'),
                                                     col('ok9_1'),
                                                     col('ok10_1'),
                                                     col('ok11_1'),
                                                     col('ok12_1'),
                                                     col('ok13_1'),
                                                     col('ok14_1'),
                                                     col('ok15_1'),
                                                     col('ok16_1'),
                                                     col('ok17_1'),
                                                     col('ok18_1'),
                                                     col('ok19_1'),
                                                     substring(substring_index('Dat_Fcreacion', ' ', 1),-4,4).cast('int').alias('anio'),
                                                     substring(substring_index('Dat_Fcreacion', ' ', 1),-7,2).cast('int').alias('mes'))