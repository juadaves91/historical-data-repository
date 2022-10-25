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

print('variables de conexion con cdn')

# COMMAND ----------

ValorX = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_URL_BLOBSTORAGE' LIMIT 1").first()["Valor1"]
ValorY = sqlContext.sql("SELECT Valor1 FROM Default.parametros WHERE CodParametro='PARAM_DBBICORPORATIVO_INDICADORES_APP' LIMIT 1").first()["Valor1"]

# COMMAND ----------

# DBTITLE 1,Creación Panel Principal
sql_query = "SELECT \
   IdRegistro, \
   Proyecto, \
   Estructura, \
   Indicador, \
   Estado, \
   Valor, \
   FechaActualizacion, \
   dtmFecha Fecha, \
   IF (length(concat_ws(',', Anexos)) > 0, CONCAT('" + ValorX + ValorY + "/', IdRegistro,'.zip'" + "), '') link, \
   translate(concat(concat(IdRegistro, ' - ', Proyecto, ' - ', Estructura, ' - ', Indicador, ' - ', Valor,' - ', Estado,' - ',Valor), ' - ', \
             upper(concat(Proyecto, ' - ', 'Estructura')), ' - ', lower(concat(Proyecto, ' - ', 'Estructura'))),'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda, \
   cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS anio, \
   cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS mes \
FROM \
  Zdc_Dbbicorporativo_Indicadores_TablaIntermedia"

panel_principal_indicadores = sqlContext.sql(sql_query)

if origen == "":
  sqlContext.registerDataFrameAsTable(panel_principal_indicadores,"Zdp_Dbbicorporativo_Indicadores_Panel_Principal") 

# COMMAND ----------

# DBTITLE 1,Creación Panel Detalle
panel_detalle_indicadores = sqlContext.sql("\
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 1 as posicion, 'Id Fact Cifras Periodo' AS key, IdFact_CifrasPeriodo AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 2 as posicion, 'Id Proyecto' AS key, IdProyecto AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Tema' AS key, IdTema AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Estructura' AS key, IdEstructura AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Reporte' AS key, IdReporte AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Indicador' AS key, IdIndicador AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Empresa' AS key, IdEmpresa AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Geografia' AS key, IdGeografia AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Canal' AS key, IdCanal AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Producto' AS key, IdProducto AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Clasificacion' AS key, IdClasificacion AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia \
UNION ALL \
SELECT IdRegistro, 'Identificadores Tbl Detalle' AS vista, 3 as posicion, 'Id Tiempo' AS key, IdTiempo AS value, cast(substr(substring_index(dtmFecha, ' ', 1),-10,4) as int) AS Anio, cast(substr(substring_index(dtmFecha, ' ', 1),-5,2) as int) AS Mes FROM Zdc_Dbbicorporativo_Indicadores_TablaIntermedia")

if origen == "":
  sqlContext.registerDataFrameAsTable(panel_detalle_indicadores,"Zdp_Dbbicorporativo_Indicadores_Panel_Detalle")
