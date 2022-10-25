# Databricks notebook source
# MAGIC %md
# MAGIC ## ZONA DE CRUDOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS lotus_processoptimizado;

# COMMAND ----------

# DBTITLE 1,Tabla Intermedia - Aux (Hive)
# MAGIC %sql
# MAGIC --Descripción: Define la metadata en el documento de definición Diccionario de datos, el esquema se aplica a los datos al momneto de 
# MAGIC             -- leer la información de almacenados en la ruta /raw, con el fin de castear los datos de acuerdo a la definición exacta 
# MAGIC             -- de la metadata, la información se alamacena en la tabla Zdc_Lotus_Cayman_Tablaintermedia_Aux.
# MAGIC              
# MAGIC --Autor: Juan David Escobar E.
# MAGIC --Fecha Modificación: 28/08/2019.
# MAGIC --Ejecución: SELECT * FROM Zdc_Lotus_Cayman_Tablaintermedia_Aux
# MAGIC 
# MAGIC DROP TABLE IF EXISTS lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux;
# MAGIC CREATE EXTERNAL TABLE lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux
# MAGIC (
# MAGIC   UniversalID string,
# MAGIC   attachments array<string>, 
# MAGIC   hijos array<struct<
# MAGIC     UniversalID:string, 
# MAGIC     form:map<string,array<string>>, 
# MAGIC     txtMsgErrorDescarga:map<string,array<string>>, 
# MAGIC     dtmUltimaConsultaAS:map<string,array<string>>, 
# MAGIC     txtPrimeraDescarga:map<string,array<string>>, 
# MAGIC     txtRadicado:map<string,array<string>>, 
# MAGIC     txtNumIdCorto:map<string,array<string>>, 
# MAGIC     txtTipoId:map<string,array<string>>, 
# MAGIC     txtNomCliente:map<string,array<string>>, 
# MAGIC     dtmNacimiento:map<string,array<string>>, 
# MAGIC     txtActividadEconomica:map<string,array<string>>, 
# MAGIC     txtCentralIxInterna:map<string,array<string>>, 
# MAGIC     txtCentralIxExterna:map<string,array<string>>, 
# MAGIC     dtmVinculacion:map<string,array<string>>, 
# MAGIC     SSUpdatedBy:map<string,array<string>>, 
# MAGIC     SSRevision:map<string,array<string>>>>,
# MAGIC     SSConflictAction map<string,string>,
# MAGIC     SSRevisions map<string,array<string>>,
# MAGIC     SSUpdatedBy map<string,array<string>>,
# MAGIC     SSWebFlags map<string,string>,
# MAGIC     alertaSocio map<string,string>,
# MAGIC     cbNomTipoSolicitud map<string,string>,
# MAGIC     cbOtroCliTipoIds map<string,string>,
# MAGIC     cbTipoCliente map<string,string>,
# MAGIC     cbTipoSolicitud map<string,string>,
# MAGIC     chkCN map<string,array<string>>,
# MAGIC     chkCNC map<string,array<string>>,
# MAGIC     chkCS map<string,array<string>>,
# MAGIC     chkCSC map<string,array<string>>,
# MAGIC     chkDocumentos map<string,array<string>>,
# MAGIC     chkEN map<string,array<string>>,
# MAGIC     chkENC map<string,array<string>>,
# MAGIC     chkFA map<string,array<string>>,
# MAGIC     chkFAC map<string,array<string>>,
# MAGIC     chkIND map<string,array<string>>,
# MAGIC     chkINDC map<string,array<string>>,
# MAGIC     chkRE map<string,array<string>>,
# MAGIC     chkREC map<string,array<string>>,
# MAGIC     dtmCamComConstitucion map<string,string>,
# MAGIC     dtmCamComExpedicion map<string,string>,
# MAGIC     dtmCMLFecVen map<string,array<string>>,
# MAGIC     dtmCreado map<string,string>,
# MAGIC     dtmDecision map<string,string>,
# MAGIC     dtmNacimiento map<string,string>,
# MAGIC     dtmNacimientoC map<string,string>,
# MAGIC     dtmNacimientoE map<string,string>,
# MAGIC     dtmPapelera map<string,string>,
# MAGIC     dtmTrans map<string,array<string>>,
# MAGIC     dtmUltimaConsultaAS map<string,string>,
# MAGIC     dtmVinculacion map<string,string>,
# MAGIC     form map<string,string>,
# MAGIC     numActFijosGPO map<string,array<string>>,
# MAGIC     numBienesGRA map<string,string>,
# MAGIC     numBienesGRO map<string,string>,
# MAGIC     numCantidadGRA map<string,array<string>>,
# MAGIC     numCartAct map<string,string>,
# MAGIC     numCartAsi map<string,string>,
# MAGIC     numCartGlbAsig map<string,string>,
# MAGIC     numCartGlbAsigC map<string,string>,
# MAGIC     numCartGlbDis map<string,string>,
# MAGIC     numCartGlbDisC map<string,string>,
# MAGIC     numCartPIC map<string,string>,
# MAGIC     numCartSol map<string,string>,
# MAGIC     numCEGPartUltTrimestre map<string,array<string>>,
# MAGIC     numCEGPorcCrecimiento map<string,array<string>>,
# MAGIC     numCEGTotal map<string,array<string>>,
# MAGIC     numCEGTotEntidades map<string,string>,
# MAGIC     numCEGVarMonto map<string,array<string>>,
# MAGIC     numCMLAct map<string,array<string>>,
# MAGIC     numCMLPic map<string,array<string>>,
# MAGIC     numCMLRA map<string,array<string>>,
# MAGIC     numCMLSol map<string,array<string>>,
# MAGIC     numCodeudoresGPO map<string,string>,
# MAGIC     numDeudaActual map<string,string>,
# MAGIC     numDiasCreacion map<string,string>,
# MAGIC     numDiasEstado map<string,string>,
# MAGIC     numIndVisualTiempoEstado map<string,string>,
# MAGIC     numPatBrutoGPO map<string,array<string>>,
# MAGIC     numPatLiquidoGPO map<string,array<string>>,
# MAGIC     numPorcPartSocios map<string,array<string>>,
# MAGIC     numPromTrimCtaAhorro map<string,string>,
# MAGIC     numPromTrimCtaCorriente map<string,string>,
# MAGIC     numRenLiquidaGPO map<string,array<string>>,
# MAGIC     numTiempoTrans map<string,array<string>>,
# MAGIC     numTotActFijosGPO map<string,string>,
# MAGIC     numTotAvalGRA map<string,string>,
# MAGIC     numTotAvalGRO map<string,string>,
# MAGIC     NUMTOTCMLACT map<string,string>,
# MAGIC     NUMTOTCMLPIC map<string,string>,
# MAGIC     NUMTOTCMLRA map<string,string>,
# MAGIC     NUMTOTCMLSOL map<string,string>,
# MAGIC     numTotCubGRO map<string,string>,
# MAGIC     numTotPatBrutoGPO map<string,string>,
# MAGIC     numTotPatLiquidoGPO map<string,string>,
# MAGIC     numTotRenLiquidaGPO map<string,string>,
# MAGIC     numVlrAvalGRA map<string,array<string>>,
# MAGIC     numVlrAvalGRO map<string,array<string>>,
# MAGIC     numVlrCubGRO map<string,array<string>>,
# MAGIC     OriginalModTime map<string,string>,
# MAGIC     SaveOptions map<string,string>,
# MAGIC     txtAbriendoDesde map<string,string>,
# MAGIC     txtActividadEconomica map<string,string>,
# MAGIC     txtAnalista map<string,string>,
# MAGIC     txtAprobador map<string,string>,
# MAGIC     txtAprobadorC map<string,string>,
# MAGIC     txtAsignador map<string,string>,
# MAGIC     txtAutor map<string,string>,
# MAGIC     txtAuxArchivo map<string,string>,
# MAGIC     txtAuxDocumentacion map<string,string>,
# MAGIC     txtAuxGrabacion map<string,string>,
# MAGIC     txtAuxReclasificado map<string,string>,
# MAGIC     txtAvaluoGRO map<string,array<string>>,
# MAGIC     txtCalifAnioParcial map<string,string>,
# MAGIC     txtCalifInternaAnterior map<string,string>,
# MAGIC     txtCalifSuperbancaria map<string,string>,
# MAGIC     txtCalifUltimoAnio map<string,string>,
# MAGIC     txtCamComActEconomica map<string,string>,
# MAGIC     txtCamComCertificadoVigente map<string,string>,
# MAGIC     txtCamComEmbargo map<string,string>,
# MAGIC     txtCamComVigencia map<string,string>,
# MAGIC     txtCarJus map<string,string>,
# MAGIC     txtCarJusC map<string,string>,
# MAGIC     txtCarRec map<string,string>,
# MAGIC     txtCarRecC map<string,string>,
# MAGIC     txtCEGEntidad map<string,array<string>>,
# MAGIC     txtCentralIxExterna map<string,string>,
# MAGIC     txtCentralIxInterna map<string,string>,
# MAGIC     txtCentroCostos map<string,string>,
# MAGIC     txtClaseGRA map<string,array<string>>,
# MAGIC     txtCliEditable map<string,string>,
# MAGIC     txtCMLNumTar map<string,array<string>>,
# MAGIC     txtCodAutor map<string,string>,
# MAGIC     txtCodControl map<string,string>,
# MAGIC     txtCodEstadoVin map<string,string>,
# MAGIC     txtCodGerente map<string,string>,
# MAGIC     txtCodigoCIIU map<string,string>,
# MAGIC     txtCodOficial map<string,string>,
# MAGIC     txtCodRegion map<string,string>,
# MAGIC     txtCodRiesgo map<string,string>,
# MAGIC     txtCodRiesgoC map<string,string>,
# MAGIC     txtCodRiesgoE map<string,string>,
# MAGIC     txtCodSector map<string,string>,
# MAGIC     txtCodSegmento map<string,string>,
# MAGIC     txtCodZona map<string,string>,
# MAGIC     txtConceptoGRO map<string,array<string>>,
# MAGIC     txtDesIdEstado map<string,array<string>>,
# MAGIC     txtDocumentos map<string,array<string>>,
# MAGIC     txtEliminar map<string,string>,
# MAGIC     txtEnviados map<string,string>,
# MAGIC     txtEstado map<string,string>,
# MAGIC     txtEstadoControl map<string,string>,
# MAGIC     txtEstadoTrans map<string,array<string>>,
# MAGIC     txtFacultadRepLegal map<string,string>,
# MAGIC     TXTFALTANTES map<string,array<string>>,
# MAGIC     txtFlujo map<string,string>,
# MAGIC     txtFlujoTrans map<string,array<string>>,
# MAGIC     TXTFORZARDESCARGA map<string,string>,
# MAGIC     txtGerente map<string,string>,
# MAGIC     txtGerenteCredito map<string,string>,
# MAGIC     txtGrabadorLME map<string,string>,
# MAGIC     txtGrupoRiesgo map<string,string>,
# MAGIC     txtGrupoRiesgoC map<string,string>,
# MAGIC     txtGrupoRiesgoE map<string,string>,
# MAGIC     txtIdDoc map<string,string>,
# MAGIC     txtIdDocumentos map<string,array<string>>,
# MAGIC     txtIdentificaciones map<string,array<string>>,
# MAGIC     txtIdentificacionesR map<string,array<string>>,
# MAGIC     txtIdEstado map<string,array<string>>,
# MAGIC     txtIdEstadoSig map<string,string>,
# MAGIC     txtIdEstadoTrans map<string,array<string>>,
# MAGIC     txtIdEstAnterior map<string,array<string>>,
# MAGIC     txtIdFinCredito map<string,string>,
# MAGIC     txtIdFlujo map<string,string>,
# MAGIC     txtIdFlujoSig map<string,string>,
# MAGIC     txtIdFlujoTrans map<string,array<string>>,
# MAGIC     txtIdRepLegal map<string,string>,
# MAGIC     txtIdSocios map<string,array<string>>,
# MAGIC     txtIdsServicios map<string,array<string>>,
# MAGIC     txtIdTipoId map<string,string>,
# MAGIC     txtIndexados map<string,array<string>>,
# MAGIC     txtMenControl map<string,string>,
# MAGIC     txtMsgErrorDescarga map<string,string>,
# MAGIC     txtNombreRepLegal map<string,string>,
# MAGIC     txtNombres map<string,array<string>>,
# MAGIC     txtNombreSocios map<string,array<string>>,
# MAGIC     txtNomCliente map<string,string>,
# MAGIC     txtNroGar map<string,array<string>>,
# MAGIC     txtNumActaJunta map<string,string>,
# MAGIC     txtNumActaJuntaC map<string,string>,
# MAGIC     txtNumId map<string,string>,
# MAGIC     txtNumIdCorto map<string,string>,
# MAGIC     TXTOBLIGATORIOS map<string,array<string>>,
# MAGIC     txtObsCausales map<string,string>,
# MAGIC     txtObsDocumentos map<string,string>,
# MAGIC     txtObsGarantias map<string,string>,
# MAGIC     txtOtroCliNumId map<string,string>,
# MAGIC     txtOtrosSocios map<string,string>,
# MAGIC     txtPrimeraDescarga map<string,string>,
# MAGIC     txtRadicado map<string,string>,
# MAGIC     txtRecCadReciente map<string,string>,
# MAGIC     txtRecComReciente map<string,string>,
# MAGIC     txtRecibidos map<string,string>,
# MAGIC     txtRegion map<string,string>,
# MAGIC     txtRelLaboral map<string,string>,
# MAGIC     txtReprocesos map<string,array<string>>,
# MAGIC     txtResponsable map<string,string>,
# MAGIC     txtResponsableControl map<string,string>,
# MAGIC     txtResponTrans map<string,array<string>>,
# MAGIC     txtSector map<string,string>,
# MAGIC     txtSectorC map<string,string>,
# MAGIC     txtSectorE map<string,string>,
# MAGIC     txtSegmento map<string,string>,
# MAGIC     txtSegmentoC map<string,string>,
# MAGIC     txtSegmentoE map<string,string>,
# MAGIC     txtSocEditable map<string,string>,
# MAGIC     txtTipCliAso map<string,string>,
# MAGIC     txtTipCliAsoC map<string,array<string>>,
# MAGIC     txtTipo map<string,string>,
# MAGIC     txtTipoCliente map<string,string>,
# MAGIC     txtTipoClienteVis map<string,string>,
# MAGIC     txtTipoGRO map<string,array<string>>,
# MAGIC     txtTipoId map<string,string>,
# MAGIC     txtTipoIdAvalistas map<string,array<string>>,
# MAGIC     txtTipoIdRepLegal map<string,string>,
# MAGIC     txtTipoIdSocios map<string,array<string>>,
# MAGIC     txtTipoPersona map<string,string>,
# MAGIC     txtTipoRegMTS map<string,string>,
# MAGIC     txtTiposId map<string,array<string>>,
# MAGIC     txtTiposIdDes map<string,array<string>>,
# MAGIC     txtZona map<string,string>,
# MAGIC     validarSarlaft map<string,string>,
# MAGIC     txtIdEstadosFin map<string,array<string>>,
# MAGIC     txtHistorico map<string,array<string>>,
# MAGIC     txtRecCadena map<string,array<string>>,
# MAGIC     txtRecComerciales map<string,array<string>>
# MAGIC )
# MAGIC ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
# MAGIC LOCATION '/mnt/lotus-cayman/raw';

# COMMAND ----------

# DBTITLE 1,MetaData
import pandas as pd
import numpy as np
path_escritura = '/mnt/lotus-cayman-storage' 
# Eliminar archivo de propiedades existente
dbutils.fs.rm("/dbfs"+path_escritura+"/metadata_lotus_db.txt", True)
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# Describe de la tabla
meta = spark.sql("""DESCRIBE lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux""")
# Conversion a Pandas Dataframe
pmeta = meta.select("*").toPandas()
pmeta.to_csv("/dbfs"+path_escritura+"/metadata_lotus_db.txt", index=False, columns=['col_name', 'data_type'], sep='|')

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE PROCESADOS

# COMMAND ----------

# DBTITLE 1,Panel Principal 
"""
Descripción: Creación de objeto Dataframe en memoria (Zona procesados), contiene la información del 
             panel principal de Cayman (Filtros de busqueda, campos del grid y campos de particionamiento año, mes).
Autor: Juan David Escobar E.
Fecha Modificación: 27/08/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_SolicitudesHijos
"""

panel_principal_cayman = sqlContext.sql("\
SELECT \
  UniversalID, \
  txtAprobadorC.value AS txtAprobadorC, \
  txtAutor.value AS txtAutor, \
  txtNomCliente.value AS txtNomCliente, \
  txtNumIdCorto.value AS txtNumIdCorto, \
  txtRadicado.value AS txtRadicado, \
  dtmCreado.value AS dtmCreado, \
  IF (length(concat_ws(',', attachments)) > 0, CONCAT(X.Valor1, Y.Valor1, '/', UniversalID,'.zip', Z.Valor1), '') AS link, \
  translate(concat(concat(txtAprobadorC.value, ' - ', txtAutor.value, ' - ', txtNomCliente.value, ' - ', txtNumIdCorto.value, ' - ', txtRadicado.value),' \ -',upper(concat(txtAprobadorC.value, ' - ', txtAutor.value, ' - ', txtNomCliente.value, ' - ', txtNumIdCorto.value, ' - ', txtRadicado.value)),' - ', \
  lower(concat(txtAprobadorC.value, ' - ', txtAutor.value, ' - ', txtNomCliente.value, ' - ', txtNumIdCorto.value, ' - ', txtRadicado.value)))  ,'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN') busqueda, \
      cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
      cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  CROSS JOIN Default.parametros X ON (X.CodParametro='PARAM_URL_BLOBSTORAGE') \
  CROSS JOIN Default.parametros Y ON (Y.CodParametro='PARAM_LOTUS_CAYMAN_APP') \
  CROSS JOIN Default.parametros Z ON (Z.CodParametro='PARAM_LOTUS_TOKEN')")

sqlContext.registerDataFrameAsTable(panel_principal_cayman, "Zdp_Lotus_Cayman_Panel_Principal")

# COMMAND ----------

# DBTITLE 1,Catalogo 1 - Aux (REGISTRO DEL FLUJO)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada REGISTRO DEL FLUJO.
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat1_Aux;
"""

panel_detalle_cayman_cat1 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  edtmTrans, \
  etxtFlujoTrans, \
  etxtEstadoTrans, \
  etxtResponTrans, \
  enumTiempoTrans, \
  etxtReprocesos \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(dtmTrans['value'])       edtmTrans       AS posicion,    edtmTrans \
  LATERAL VIEW posexplode(txtFlujoTrans['value'])  etxtFlujoTrans  AS flujo,       etxtFlujoTrans \
  LATERAL VIEW posexplode(txtEstadoTrans['value']) etxtEstadoTrans AS estado,      etxtEstadoTrans \
  LATERAL VIEW posexplode(txtResponTrans['value']) etxtResponTrans AS responsable, etxtResponTrans \
  LATERAL VIEW posexplode(numTiempoTrans['value']) enumTiempoTrans AS tiempo,      enumTiempoTrans \
  LATERAL VIEW posexplode(txtReprocesos['value'])  etxtReprocesos  AS reproceso,   etxtReprocesos \
WHERE \
  posicion = flujo \
  AND posicion = estado \
  AND posicion = responsable \
  AND posicion = tiempo \
  AND posicion = reproceso")   
                                               
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat1, "Zdp_Lotus_Cayman_Cat1_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 2 - Aux (CIFIN)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada CIFIN
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat2_Aux;
"""

panel_detalle_cayman_cat2 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtCEGEntidad, \
  enumCEGTotal, \
  enumCEGPartUltTrimestre, \
  enumCEGVarMonto, \
  enumCEGPorcCrecimiento \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtCEGEntidad['value'])          etxtCEGEntidad          AS posicion,    etxtCEGEntidad \
  LATERAL VIEW posexplode(numCEGTotal['value'])            enumCEGTotal            AS total,       enumCEGTotal \
  LATERAL VIEW posexplode(numCEGPartUltTrimestre['value']) enumCEGPartUltTrimestre AS trimestre,   enumCEGPartUltTrimestre \
  LATERAL VIEW posexplode(numCEGVarMonto['value'])         enumCEGVarMonto         AS monto,       enumCEGVarMonto \
  LATERAL VIEW posexplode(numCEGPorcCrecimiento['value'])  enumCEGPorcCrecimiento  AS crecimiento, enumCEGPorcCrecimiento \
WHERE \
  posicion = total \
  AND posicion = trimestre \
  AND posicion = monto \
  AND posicion = crecimiento")   

sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat2, "Zdp_Lotus_Cayman_Cat2_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 3 - Aux (GARANTIAS)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada GARANTIAS
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat3_Aux;
"""

panel_detalle_cayman_cat3 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtClaseGRA, \
  enumCantidadGRA, \
  enumVlrAvalGRA \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtClaseGRA['value'])    etxtClaseGRA    AS posicion, etxtClaseGRA \
  LATERAL VIEW posexplode(numCantidadGRA['value']) enumCantidadGRA AS cantidad, enumCantidadGRA \
  LATERAL VIEW posexplode(numVlrAvalGRA['value'])  enumVlrAvalGRA  AS aval,     enumVlrAvalGRA \
WHERE \
  posicion = cantidad \
  AND posicion = aval") 
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat3, "Zdp_Lotus_Cayman_Cat3_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 4 - Aux (LISTADO DE DOCUMENTOS)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada LISTADO DE DOCUMENTOS
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat4_Aux;
"""

panel_detalle_cayman_cat4 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes,  \
  posicion, \
  etxtIdDocumentos, \
  etxtDocumentos, \
  CASE echkIND When '' Then '' Else 'X' End AS Anexado, \
  CASE echkEN  When '' Then '' Else 'X' End AS Enviado, \
  CASE echkRE  When '' Then '' Else 'X' End AS Recibido, \
  CASE echkFA  When '' Then '' Else 'X' End AS Faltante \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtIdDocumentos['value']) etxtIdDocumentos AS posicion,   etxtIdDocumentos \
  LATERAL VIEW posexplode(txtDocumentos['value'])   etxtDocumentos   AS doc,        etxtDocumentos \
  LATERAL VIEW posexplode(chkIND['value'])          echkIND          AS ind,        echkIND \
  LATERAL VIEW posexplode(chkEN['value'])           echkEN           AS en,         echkEN \
  LATERAL VIEW posexplode(chkRE['value'])           echkRE           AS re,         echkRE \
  LATERAL VIEW posexplode(chkFA['value'])           echkFA           AS fa,         echkFA \
WHERE \
  posicion = ind \
  AND posicion = doc \
  AND posicion = en \
  AND posicion = re \
  AND posicion = fa")
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat4, "Zdp_Lotus_Cayman_Cat4_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 5 - Aux (INFORMACIÓN DE LOS SOCIOS)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada INFORMACION DE LOS SOCIOS
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat5_Aux;
"""

panel_detalle_cayman_cat5 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes,  \
  posicion, \
  etxtIdSocios, \
  etxtTipoIdSocios, \
  etxtNombreSocios, \
  enumPorcPartSocios \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtIdSocios['value'])       etxtIdSocios       AS posicion,    etxtIdSocios \
  LATERAL VIEW posexplode(txtTipoIdSocios['value'])   etxtTipoIdSocios   AS tiposocio,   etxtTipoIdSocios \
  LATERAL VIEW posexplode(txtNombreSocios['value'])   etxtNombreSocios   AS nombresocio, etxtNombreSocios \
  LATERAL VIEW posexplode(numPorcPartSocios['value']) enumPorcPartSocios AS partesocio,  enumPorcPartSocios \
WHERE \
  posicion = tiposocio \
  AND posicion = nombresocio \
  AND posicion = partesocio")
  
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat5, "Zdp_Lotus_Cayman_Cat5_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 6 - Aux (GARANTIAS)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada GARANTIAS
Autor: Juan David Escobar E.
Fecha de modificación: 28/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat6_Aux;
"""

panel_detalle_cayman_cat6 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes,  \
  posicion, \
  etxtNroGar, \
  etxtTipoGRO, \
  etxtConceptoGRO, \
  etxtAvaluoGRO, \
  enumVlrAvalGRO, \
  enumVlrCubGRO \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtNroGar['value'])      etxtNroGar      AS posicion,    etxtNroGar \
  LATERAL VIEW posexplode(txtTipoGRO['value'])     etxtTipoGRO     AS tipogro,     etxtTipoGRO \
  LATERAL VIEW posexplode(txtConceptoGRO['value']) etxtConceptoGRO AS conceptogro, etxtConceptoGRO \
  LATERAL VIEW posexplode(txtAvaluoGRO['value'])   etxtAvaluoGRO   AS avaluogro,   etxtAvaluoGRO \
  LATERAL VIEW posexplode(numVlrAvalGRO['value'])  enumVlrAvalGRO  AS avalgro,     enumVlrAvalGRO \
  LATERAL VIEW posexplode(numVlrCubGRO['value'])   enumVlrCubGRO   AS cubgro,      enumVlrCubGRO \
WHERE \
  posicion = tipogro \
  AND posicion = conceptogro \
  AND posicion = avaluogro \
  AND posicion = avalgro \
  AND posicion = cubgro")
                                           
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat6, "Zdp_Lotus_Cayman_Cat6_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 7 - Aux (CUPO TARJETA DETALLE)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada CUPO TARJETA DETALLE
Autor: Juan David Escobar E.
Fecha de modificación: 29/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat7_Aux;
"""

panel_detalle_cayman_cat7 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtTipCliAsoC, \
  etxtNombres, \
  enumCMLAct, \
  enumCMLSol, \
  enumCMLPic, \
  enumCMLRA, \
  etxtCMLNumTar, \
  edtmCMLFecVen \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtTipCliAsoC['value']) etxtTipCliAsoC AS posicion,     etxtTipCliAsoC \
  LATERAL VIEW posexplode(txtNombres['value'])    etxtNombres    AS ztxtnombres,   etxtNombres \
  LATERAL VIEW posexplode(numCMLAct['value'])     enumCMLAct     AS znumcmlact,    enumCMLAct \
  LATERAL VIEW posexplode(numCMLSol['value'])     enumCMLSol     AS znumcmlsol,    enumCMLSol \
  LATERAL VIEW posexplode(numCMLPic['value'])     enumCMLPic     AS znumcmlpic,    enumCMLPic \
  LATERAL VIEW posexplode(numCMLRA['value'])      enumCMLRA      AS znumcmlra,     enumCMLRA \
  LATERAL VIEW posexplode(txtCMLNumTar['value'])  etxtCMLNumTar  AS ztxtcmlnumtar, etxtCMLNumTar \
  LATERAL VIEW posexplode(dtmCMLFecVen['value'])  edtmCMLFecVen  AS ztmcmlfecven,  edtmCMLFecVen \
WHERE \
  posicion = ztxtnombres \
  AND posicion = znumcmlact \
  AND posicion = znumcmlsol \
  AND posicion = znumcmlpic \
  AND posicion = znumcmlra \
  AND posicion = ztxtcmlnumtar \
  AND posicion = ztmcmlfecven")
                                           
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat7, "Zdp_Lotus_Cayman_Cat7_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 8 - Aux (RECOMENDACIONES)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada RECOMENDACIONES, CATALOGOS ADICIONALES (arreglos sin relaciones)
Autor: Juan David Escobar E.
Fecha de modificación: 29/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat8_Aux;
"""

panel_detalle_cayman_cat8 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtRecComerciales \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtRecComerciales['value']) etxtRecComerciales AS posicion, etxtRecComerciales") 
                                           
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat8, "Zdp_Lotus_Cayman_Cat8_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 9 - Aux (HISTORICOS)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada HISTORICOS, CATALOGOS ADICIONALES (arreglos sin relaciones)
Autor: Juan David Escobar E.
Fecha de modificación: 29/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat9_Aux;
"""

panel_detalle_cayman_cat9 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtHistorico \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtHistorico['value']) etxtHistorico AS posicion, etxtHistorico")
                                           
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat9, "Zdp_Lotus_Cayman_Cat9_Aux")

# COMMAND ----------

# DBTITLE 1,Catalogo 10 - Aux (RECOMENDACIONES)
"""
Descripción: Tabla Auxiliar, extrae la información de la tabla intermedia relacionada RECOMENDACIONES, CATALOGOS ADICIONALES (arreglos sin relaciones)
Autor: Juan David Escobar E.
Fecha de modificación: 29/08/2019.
Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Cat10_Aux;
"""

panel_detalle_cayman_cat10 = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  posicion, \
  etxtRecCadena \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  LATERAL VIEW posexplode(txtRecCadena['value']) etxtRecCadena AS posicion, etxtRecCadena")
                                            
sqlContext.registerDataFrameAsTable(panel_detalle_cayman_cat10, "Zdp_Lotus_Cayman_Cat10_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: TODOS - Menú: Radicado )
"""
Descripción: Esta tabla contiene la información de la visualización OTROS CLIENTES, menú RADICADO
             los cuales provienen de la tabla .
Autor: Juan David Escobar E.
Fecha Modificación: 27/08/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Radicado
"""

panel_radicado_cayman = sqlContext.sql("\
SELECT UniversalId, \
       txtAprobadorC.value AS txtAprobadorC, \
       txtAutor.value AS txtAutor, \
       txtNomCliente.value AS txtNomCliente, \
       txtNumIdCorto.value AS txtNumIdCorto, \
       txtRadicado.value AS txtRadicado, \
       cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
       cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: Datos Generales)
"""
Descripción: Esta tabla contiene la información de la pagína Datos Generales, menú: 1.Datos generales,
             2. Datos del flujo de aprobación.
Autor: Juan David Escobar E.
Fecha Modificación: 30/08/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Datos_Generales
"""

panel_detalle_datos_generales_cayman = sqlContext.sql("\
SELECT UniversalId, 1  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'DATOS GENERALES' AS key, '' AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 2  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Tipo de solicitud' AS key, cbNomTipoSolicitud.value AS value  FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 3  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Tipo de cliente' AS key, txtTipoClienteVis.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 4  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Estado' AS key, txtEstado.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 5  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Nombre del autor' AS key, txtautor.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 6  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Responsable actual' AS key, txtResponsable.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 7  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Fecha de creacion' AS key, dtmCreado.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 8  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Código del autor' AS key, txtCodAutor.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 9  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'DATOS DEL FLUJO DE APROBACIÓN' AS key, '' AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 10  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Código de quien aprueba' AS key, txtCodOficial.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 11  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Fecha de decisión' AS key, dtmDecision.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 12  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Número de acta de comité de crédito' AS key, txtNumActaJunta.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 13  as posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Nombre de quien aprueba' AS key, txtAprobadorC.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: Cliente Evaluar)
panel_detalle_client_evaluar_cayman = sqlContext.sql("\
SELECT UniversalId, 1 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'CLIENTE A EVALUAR' AS key, '' AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 2 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Zona' AS key, txtZona.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 3 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Tipo de identificación' AS key, txtTipoId.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion   FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 4 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Segmento' AS key, txtSegmento.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 5 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Sector' AS key, txtSector.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 6 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Región' AS key, txtRegion.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 7 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Número de identificación' AS key, txtNumIdCorto.value AS value, '' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 8 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Nombres y apellidos o razón social' AS key, txtNomCliente.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 9 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Grupo de riesgo' AS key, txtGrupoRiesgo.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 10 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Gerente' AS key, txtGerente.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 11 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Fecha de vinculación' AS key, dtmVinculacion.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 12 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Fecha de nacimiento' AS key, dtmNacimiento.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 13 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Código del gerente' AS key, txtCodGerente.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 14 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Código de riesgo' AS key, txtCodRiesgo.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 15 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Código CIIU' AS key, txtCodigoCIIU.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 16 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Centro de costos' AS key, txtCentroCostos.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 17 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Actividad económica del cliente' AS key, txtActividadEconomica.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 18 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'CALIFICACIONES' AS key, '' AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 19 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Último año' AS key, txtCalifUltimoAnio.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 20 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Superbancaria' AS key, txtCalifSuperbancaria.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 21 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Interna anterior' AS key, txtCalifInternaAnterior.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 22 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Año parcial' AS key, txtCalifAnioParcial.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 23 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'PROMEDIO CUENTA TRIMESTRAL (MILES)' AS key, '' AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 24 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Ahorro' AS key, numPromTrimCtaAhorro.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId,25 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Corriente' AS key, numPromTrimCtaCorriente.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 26 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'Deuda actual' AS key, numDeudaActual.value AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 27 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2) as int) AS mes, 'INFORMACIÓN DE LOS SOCIOS' AS key, '' AS value,'' AS NumeroIdentificacionCat5, '' AS TipoIdentificacion, '' AS Nombre, '' AS Participacion    FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 28 AS posicion, anio,  mes, '' AS key, '' AS value, etxtIdSocios AS NumeroIdentificacionCat5, etxtTipoIdSocios AS TipoIdentificacion, etxtNombreSocios AS Nombre, enumPorcPartSocios AS Participacion FROM Zdp_Lotus_Cayman_Cat5_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: CIFIN)
"""
Descripción: Esta tabla contiene la información de la pagína CIFIN, menú: 1.Centrales de información,
             2. Comparativo de endeudamiento global
Autor: Juan David Escobar E.
Fecha Modificación: 30/08/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Cifin
"""

panel_cfin_cayman = sqlContext.sql("\
SELECT UniversalId,cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, '' AS Entidad, '' AS Total, '' AS Participacion_ultimo_trimestre, '' AS Variacion_monto, '' AS Crecimiento, txtCentralIxInterna.value AS txtCentralIxInterna, txtCentralIxExterna.value AS txtCentralIxExterna FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, anio, mes, etxtCEGEntidad AS Entidad, enumCEGTotal AS Total, enumCEGPartUltTrimestre AS Participacion_ultimo_trimestre, enumCEGVarMonto AS Variacion_monto, enumCEGPorcCrecimiento AS Crecimiento, '' AS txtCentralIxInterna, '' AS txtCentralIxExterna \
FROM  Zdp_Lotus_Cayman_Cat2_Aux \
UNION ALL \
SELECT UniversalId, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Total entidades' AS Entidad, numCEGTotEntidades.value AS Total, '' AS Participacion_ultimo_trimestre, '' AS Variacion_monto, '' AS Crecimiento, '' AS txtCentralIxInterna, '' AS txtCentralIxExterna FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux") 

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: CAMARA DE COMERCIO)
"""
Descripción: Esta tabla contiene la información de la pagína CAMARA DE COMERCIO, menú: 1.Verificación camara de comercio,
             2. Representante Legal
Autor: Juan David Escobar E.
Fecha Modificación: 2/09/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_CamaraComercio
"""

panel_camara_comercio_cayman = sqlContext.sql("\
SELECT UniversalId, 1 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'VERIFICACIÓN CAMARA DE COMERCIO' AS key, '' AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 2 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Actividad económica' AS key, txtCamComActEconomica.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 3 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Certificado de cámara de comercio vigente?' AS key, txtCamComCertificadoVigente.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 4 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Fecha de constitución' AS key, dtmCamComConstitucion.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 5 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Fecha de expedición cámara de comercio' AS key, dtmCamComExpedicion.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 6 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast( substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Posee embargos la sociedad?' AS key, txtCamComEmbargo.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 7 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Vigencia' AS key, txtCamComVigencia.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 8 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'REPRESENTATE LEGAL' AS key, '' AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 9 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Facultad' AS key, txtFacultadRepLegal.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 10  AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Nombre' AS key, txtNombreRepLegal.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 11 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Número de identificación' AS key, txtIdRepLegal.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT UniversalId, 12 AS posicion, cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, 'Tipo de identificación' AS key, txtTipoIdRepLegal.value AS value FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Tabla Aux IntermediaHijos 
# MAGIC %sql
# MAGIC --Descripción: Tabla Auxiliar, extrae la información de los items almacenados en la colección tipo array del campo hijos, para generar los registros 
# MAGIC --             asociados de un UniversalId asociados al UniversalID de cada registro.
# MAGIC --Autor: Juan David Escobar E.
# MAGIC --Fecha de modificación: 27/08/2019.
# MAGIC --Ejecución: SELECT * FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Tablaintermedia_Hijos_Aux;
# MAGIC 
# MAGIC --REFRESH TABLE lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS lotus_processoptimizado.Zdp_Lotus_Cayman_Tablaintermedia_Hijos_Aux;
# MAGIC CREATE TABLE lotus_processoptimizado.Zdp_Lotus_Cayman_Tablaintermedia_Hijos_Aux AS
# MAGIC SELECT
# MAGIC   UniversalID,
# MAGIC   posicion,
# MAGIC   ehijos,
# MAGIC   cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, 
# MAGIC   cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes 
# MAGIC FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux
# MAGIC LATERAL VIEW posexplode(hijos) ehijos AS posicion, ehijos;

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: OTROS CLIENTES)
"""
Descripción: Esta tabla contiene la información de la visualización OTROS CLIENTES, menú INFORMACIÓN DE LOS CLIENTES ASOCIADOS
             los cuales provienen de la tabla Zdp_Lotus_Cayman_SolicitudesHijos (Hijos).
Autor: Juan David Escobar E.
Fecha Modificación: 27/08/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_SolicitudesHijos
"""

panel_otros_clientes_cayman = sqlContext.sql("\
SELECT \
  UniversalID, \
  anio, \
  mes, \
  posicion, \
  ehijos.txtnumidcorto.value[0] AS txtnumidcorto, \
  ehijos.txttipoid.value[0] AS txttipoid, \
  ehijos.txtnomcliente.value[0] AS txtnomcliente, \
  ehijos.txtcentralixinterna.value[0] AS txtcentralixinterna, \
  ehijos.txtcentralixexterna.value[0] AS txtcentralixexterna, \
  ehijos.txtactividadeconomica.value[0] as txtactividadeconomica, \
  ehijos.dtmnacimiento.value[0] AS dtmnacimiento, \
  ehijos.dtmvinculacion.value[0] AS dtmvinculacion \
FROM lotus_processoptimizado.Zdp_Lotus_Cayman_Tablaintermedia_Hijos_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: CUPO TARJETA)

"""Descripción: Esta tabla contiene la información de la visualización CUPO TARJETA, menú 1.Cupo maximo de tarjeta (MILES DE DOLARES), 2.Información
                ingresada por el analista, 3.Detalle cupo tarjeta.
                los cuales provienen de la tabla Zdp_Lotus_Cayman_SolicitudesHijos (Hijos).
   Autor: Juan David Escobar E.
   Fecha Modificación: 05/09/2019.
   Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Cupo_Tarjeta"""

panel_cupo_tarjeta_detalle_cayman = sqlContext.sql("\
SELECT \
        S.UniversalId , \
        cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
        cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
        C7.etxtTipCliAsoC  as TipoDeClientesAsociados , \
        C7.etxtNombres as NombresClientesAvalistas , \
        C7.enumCMLAct as CupoActualAsignado , \
        C7.enumCMLSol as CupoAdicionalSolicitado , \
        C7.enumCMLPic as CupoAdicionalPIC , \
        C7.enumCMLRA as CupoAdicionalAprobado , \
        C7.etxtCMLNumTar as NumeroDeTarjeta , \
        C7.edtmCMLFecVen as FechaDeVencimiento \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux S \
INNER JOIN Zdp_Lotus_Cayman_Cat7_Aux C7 \
  ON S.UniversalId = C7.UniversalId \
UNION ALL \
SELECT \
        S2.UniversalId , \
        cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
        cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
        '' as TipoDeClientesAsociados , \
        'TOTAL' as NombresClientesAvalistas , \
        S2.NUMTOTCMLACT.value as TotalCupoAdicionalAsignado , \
        S2.NUMTOTCMLSOL.value as TotalCupoAdicionalSolicitado , \
        S2.NUMTOTCMLPIC.value as TotalCupoaAicionalPIC , \
        S2.NUMTOTCMLRA.value as TotalCupoAdicionalAprobado , \
        '' as NumeroDeTarjeta , \
        '' as FechaDeVencimiento \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux S2 \
INNER JOIN Zdp_Lotus_Cayman_Cat7_Aux C72 \
  ON S2.UniversalId = C72.UniversalId")

panel_cupo_tarjeta_recomendacion_cayman = sqlContext.sql("\
SELECT \
   UniversalId, \
   cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
    cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '0' as posicion, \
  'Recomendacion' as informacion, \
  txtCarRec.value as Recomendacion \
  FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '1' as posicion, \
  'Justificacion' as informacion, \
  txtCarJus.value as Justificacion \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

panel_cupo_tarjeta_cayman = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '0' AS posicion, \
  'Tarjeta de Credito Bancolombia CAYMAN Visa' as Producto, \
  '' AS Detalle, \
  numCartAct.value  AS CupoActualAsignado, \
  numCartSol.value AS CupoAdicionalSolicitado, \
  numCartPIC.value AS CupoAdicionalPIC, \
  numCartAsi.value AS CupoAdicionalAprobado \
  FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  UNION ALL \
  SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '1' AS posicion, \
  'Cupo Global Asignado' AS Producto, \
  numCartGlbAsig.value as Detalle, \
  ''  AS CupoActualAsignado, \
  '' AS CupoAdicionalSolicitado, \
  '' AS CupoAdicionalPIC, \
  '' AS CupoAdicionalAprobado \
  FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
  UNION ALL \
  SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '2' AS posicion, \
  'Cupo Global Disponible' AS Producto, \
  numCartGlbDis.value AS Detalle, \
  ''  AS CupoActualAsignado, \
  '' AS CupoAdicionalSolicitado, \
  '' AS CupoAdicionalPIC, \
  '' AS CupoAdicionalAprobado \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: GARANTIAS)
panel_garantias_reales_cayman = sqlContext.sql("\
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  'Actual' AS Detalle_garantia_real, \
  numBienesGRA.value AS Numero_bienes, \
  numTotAvalGRA.value AS Valor_avaluo, \
  'N/A' AS Valor_cubierto \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  'Ofrecida' AS Detalle_garantia_real, \
  numBienesGRO.value AS Numero_bienes, \
  numTotAvalGRO.value AS Valor_avaluo, \
  numTotCubGRO.value AS Valor_cubierto \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

panel_garantias_actual_cayman = sqlContext.sql("\
SELECT \
  UniversalId, \
  anio, \
  mes, \
  etxtClaseGRA AS Clase_garantia, \
  enumCantidadGRA AS Cantidad, \
  enumVlrAvalGRA AS Valor_avaluo \
FROM \
  Zdp_Lotus_Cayman_Cat3_Aux \
UNION ALL \
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  'TOTAL' AS Clase_garantia, \
  numBienesGRA.value AS Cantidad, \
  numTotAvalGRA.value AS Valor_avaluo \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

panel_garantias_ofrecida_cayman = sqlContext.sql("\
SELECT \
  UniversalId, \
  anio, \
  mes, \
  etxtNroGar AS Nro, \
  etxtTipoGRO AS Tipo_garantia, \
  etxtConceptoGRO AS Concepto_garantia, \
  etxtAvaluoGRO AS Fecha_avaluo, \
  enumVlrAvalGRO AS Valor_avaluo, \
  enumVlrCubGRO AS Valor_cubierto \
FROM \
  Zdp_Lotus_Cayman_Cat6_Aux \
UNION ALL \
SELECT \
  UniversalId, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
  cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
  '' AS Nro, \
  '' AS Tipo_garantia, \
  '' AS Concepto_garantia, \
  'TOTAL' AS Fecha_avaluo, \
  numTotAvalGRO.value AS Valor_avaluo, \
  numTotCubGRO.value AS Valor_cubierto \
FROM \
  lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: RECOMENDACIONES)
"""
Descripción: Esta tabla contiene la información de la visualización RECOMENDACIONES, menú 1.Historia de recomendaciones comerciales, 2.Historia de recomendaciones
             de aprobación, 3. Recomendaciones Comerciales, 4. Recomendaciones cadena de aprobación.
Autor: Juan David Escobar E.
Fecha Modificación: 06/09/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Recomendaciones"""

panel_recomendaciones_cayman = sqlContext.sql("\
SELECT \
UniversalId, \
anio, \
mes, \
posicion, \
'' AS txtRecCadReciente, \
'' AS txtRecComReciente, \
'' AS etxtRecCadena, \
etxtRecComerciales \
FROM Zdp_Lotus_Cayman_Cat8_Aux \
UNION ALL \
SELECT UniversalId, \
anio, \
mes, \
posicion, \
'' AS txtRecCadReciente, \
'' AS txtRecComReciente, \
etxtRecCadena, \
'' AS etxtRecComerciales \
FROM Zdp_Lotus_Cayman_Cat10_Aux \
UNION ALL \
SELECT \
UniversalId, \
cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
'' AS posicion, \
'' AS txtRecCadReciente, \
txtRecComReciente.value, \
'' AS etxtRecCadena, \
'' AS etxtRecComerciales \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux \
UNION ALL \
SELECT \
UniversalId, \
cast(substr(substring_index(dtmCreado.value, ' ', 1),-4,4)as int) AS anio, \
cast(substr(substring_index(dtmCreado.value, ' ', 1),-7,2)as int) AS mes, \
'' AS posicion, \
txtRecCadReciente.value, \
'' as txtRecComReciente, \
'' AS etxtRecCadena, \
'' AS etxtRecComerciales \
FROM lotus_processoptimizado.Zdc_Lotus_Cayman_Tablaintermedia_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: DOCUMENTOS)
"""
Descripción: Esta tabla contiene la información de la visualización DOCUMENTOS, menú 1.Listado de documentos
Autor: Juan David Escobar E.
Fecha Modificación: 06/09/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Documentos"""

panel_documentos_cayman = sqlContext.sql("\
SELECT  UniversalId, \
        anio, \
        mes, \
        etxtDocumentos, \
        Anexado, \
        Enviado, \
        Recibido, \
        Faltante \
FROM Zdp_Lotus_Cayman_Cat4_Aux")

# COMMAND ----------

# DBTITLE 1,Creación Tabla Aux - Panel Detalle (Pag: HISTORICOS)
"""
Descripción: Esta tabla contiene la información de la visualización HISTORICOS, menú 1.Registro e flujo, 2.Comentarios y Eventos
Autor: Juan David Escobar E.
Fecha Modificación: 06/09/2019.
Ejecución: SELECT * FROM Zdp_Lotus_Cayman_Panel_Detalle_Historicos"""

panel_cayman_historicos = sqlContext.sql("\
SELECT UniversalId, \
  anio, \
  mes, \
  '' as posicion, \
  '' as etxtHistorico, \
  edtmTrans, \
  etxtFlujoTrans, \
  etxtEstadoTrans, \
  etxtResponTrans, \
  enumTiempoTrans, \
  etxtReprocesos \
FROM Zdp_Lotus_Cayman_Cat1_Aux \
UNION ALL \
SELECT UniversalId, \
  anio, \
  mes, \
  posicion, \
  etxtHistorico, \
  '' AS edtmTrans, \
  '' AS etxtFlujoTrans, \
  '' AS etxtEstadoTrans, \
  '' AS etxtResponTrans, \
  '' AS enumTiempoTrans, \
  '' AS etxtReprocesos \
FROM Zdp_Lotus_Cayman_Cat9_Aux")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONA DE RESULTADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS lotus_cayman;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_principal_cayman
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Principal"""
panel_principal_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_principal")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Panel_Principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Panel_Principal
# MAGIC (
# MAGIC     UniversalID	string,
# MAGIC     txtAprobadorC string,
# MAGIC     txtAutor string,
# MAGIC     txtNomCliente string,
# MAGIC     txtNumIdCorto string,
# MAGIC     txtRadicado string,
# MAGIC     dtmCreado string,
# MAGIC     link string,
# MAGIC     busqueda string    
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_principal';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Panel_Principal;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_radicado
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Radicado"""
panel_radicado_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_radicado")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Panel_Detalle_Radicado;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Panel_Detalle_Radicado
# MAGIC (
# MAGIC     UniversalId	string,
# MAGIC     txtAprobadorC string,	
# MAGIC     txtAutor string,
# MAGIC     txtNomCliente string,	
# MAGIC     txtNumIdCorto string,	
# MAGIC     txtRadicado	string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_radicado';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Panel_Detalle_Radicado;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_datos_generales
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Datos Generales"""
panel_detalle_datos_generales_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_datos_generales")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Datos_Generales;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Datos_Generales
# MAGIC (
# MAGIC     UniversalId	string,
# MAGIC     Posicion int,
# MAGIC     Key string,	
# MAGIC     Value string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_datos_generales';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Datos_Generales;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_cliente_evaluar
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Cliente Evaluar"""
panel_detalle_client_evaluar_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_cliente_evaluar")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cliente_Evaluar;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cliente_Evaluar
# MAGIC (
# MAGIC     UniversalId	string,
# MAGIC     Posicion Int,
# MAGIC     key	string,
# MAGIC     value string,
# MAGIC     NumeroIdentificacionCat5 string,
# MAGIC     TipoIdentificacion string,
# MAGIC     Nombre string,
# MAGIC     Participacion string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_cliente_evaluar';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cliente_Evaluar;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_cifin
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Cifin"""
panel_cfin_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_Cifin")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cifin;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cifin
# MAGIC (
# MAGIC   UniversalId string,	
# MAGIC   Entidad string,	
# MAGIC   Total string,	
# MAGIC   Participacion_ultimo_trimestre string,	
# MAGIC   Variacion_monto string,
# MAGIC   Crecimiento string,	
# MAGIC   txtCentralIxInterna string,	
# MAGIC   txtCentralIxExterna string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_Cifin';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cifin;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_camara_comercio
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Camara Comercio"""
panel_camara_comercio_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_camara_comercio")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Camara_Comercio;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Camara_Comercio
# MAGIC (
# MAGIC   UniversalId	string,
# MAGIC   Posicion int,
# MAGIC   key	string,
# MAGIC   value	string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_camara_comercio';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Camara_Comercio;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_otros_clientes (Zdp_Lotus_Cayman_Solicitudes_Hijos)
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Otros Clientes"""
panel_otros_clientes_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_otros_clientes")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Solicitudes_Hijos;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Solicitudes_Hijos
# MAGIC (
# MAGIC UniversalID	string,	
# MAGIC posicion int,	
# MAGIC txtnumidcorto string,	
# MAGIC txttipoid string,	
# MAGIC txtnomcliente string,
# MAGIC txtcentralixinterna	string,	
# MAGIC txtcentralixexterna	string,	
# MAGIC txtactividadeconomica string,
# MAGIC dtmnacimiento string,	
# MAGIC dtmvinculacion string	
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_otros_clientes';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Solicitudes_Hijos;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_cupo_tarjeta
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Cupo Tarjeta"""
panel_cupo_tarjeta_detalle_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta_detalle")

sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Recomendacion Cupo Tarjeta"""
panel_cupo_tarjeta_recomendacion_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta_recomendacion")

sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Cupo Tarjeta"""
panel_cupo_tarjeta_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Detalle;
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Recomendacion;
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Detalle
# MAGIC (
# MAGIC   UniversalId	string,
# MAGIC   TipoDeClientesAsociados	string,
# MAGIC   NombresClientesAvalistas string,
# MAGIC   CupoActualAsignado string,
# MAGIC   CupoAdicionalSolicitado string,
# MAGIC   CupoAdicionalPIC string,
# MAGIC   CupoAdicionalAprobado string,
# MAGIC   NumeroDeTarjeta string,
# MAGIC   FechaDeVencimiento string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta_detalle';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Detalle;
# MAGIC 
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Recomendacion
# MAGIC (
# MAGIC   UniversalId string,
# MAGIC   posicion string,
# MAGIC   informacion string,
# MAGIC   Recomendacion string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta_recomendacion';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta_Recomendacion;
# MAGIC 
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta
# MAGIC (
# MAGIC   UniversalId string,
# MAGIC   posicion string,
# MAGIC   Producto string,
# MAGIC   Detalle string,
# MAGIC   CupoActualAsignado string,
# MAGIC   CupoAdicionalSolicitado string,
# MAGIC   CupoAdicionalPIC string,
# MAGIC   CupoAdicionalAprobado string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_cupo_tarjeta';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Cupo_Tarjeta;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_garantias
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Garantias Reales"""
panel_garantias_reales_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_garantias_reales")


sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Garantias Actual"""
panel_garantias_actual_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_garantias_actual")


sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
"""Dataframe Panel Garantias Ofrecidas"""
panel_garantias_ofrecida_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_garantias_ofrecida")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Reales;
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Actual;
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Ofrecida;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Reales
# MAGIC (
# MAGIC   UniversalId string,
# MAGIC   Detalle_garantia_real string,
# MAGIC   Numero_bienes string,
# MAGIC   Valor_avaluo string,
# MAGIC   Valor_cubierto string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_garantias_reales';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Reales;
# MAGIC 
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Actual
# MAGIC (
# MAGIC  UniversalId string,
# MAGIC  Clase_garantia string,
# MAGIC  Cantidad string,
# MAGIC  Valor_avaluo string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_garantias_actual';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Actual;
# MAGIC 
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Ofrecida
# MAGIC (
# MAGIC   UniversalId string,
# MAGIC   Nro string,
# MAGIC   Tipo_garantia string,
# MAGIC   Concepto_garantia string,
# MAGIC   Fecha_avaluo string,
# MAGIC   Valor_avaluo string,
# MAGIC   Valor_cubierto string
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_garantias_ofrecida';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Garantias_Ofrecida;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_recomendaciones
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Recomendaciones"""



panel_recomendaciones_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_recomendaciones")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Recomendaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Recomendaciones
# MAGIC (
# MAGIC   UniversalId	string,	  
# MAGIC   posicion string,	
# MAGIC   txtRecCadReciente string,	
# MAGIC   txtRecComReciente string,	
# MAGIC   etxtRecCadena string,	
# MAGIC   etxtRecComerciales string	
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_recomendaciones';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Recomendaciones;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_documentos
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Documentos"""
panel_documentos_cayman.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_documentos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Documentos

# COMMAND ----------

# MAGIC %sql
# MAGIC USE lotus_cayman;
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Documentos
# MAGIC (
# MAGIC     UniversalId	string, 
# MAGIC     etxtDocumentos string,
# MAGIC     Anexado	string,
# MAGIC     Enviado	string,	
# MAGIC     Recibido string,	
# MAGIC     Faltante string	
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_documentos';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Documentos;

# COMMAND ----------

# DBTITLE 1,Reparticionamiento - Panel_detalle_historicos
sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")

"""Dataframe Panel Detalle Historicos"""
panel_cayman_historicos.repartition("anio","mes").write.mode("Overwrite").partitionBy("anio","mes").parquet("/mnt/lotus-cayman/resultsOptimizado/panel_detalle_historicos")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Historicos

# COMMAND ----------

# MAGIC %sql
# MAGIC USE lotus_cayman;
# MAGIC CREATE EXTERNAL TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Historicos
# MAGIC (
# MAGIC     UniversalId	string,    	
# MAGIC     etxtHistorico string,	
# MAGIC     edtmTrans string,	
# MAGIC     etxtFlujoTrans string,	
# MAGIC     etxtEstadoTrans	string,	
# MAGIC     etxtResponTrans	string,	
# MAGIC     enumTiempoTrans	string,	
# MAGIC     etxtReprocesos string	
# MAGIC )
# MAGIC STORED AS PARQUET
# MAGIC PARTITIONED BY (anio int, mes int)
# MAGIC LOCATION '/mnt/lotus-cayman/resultsOptimizado/panel_detalle_historicos';
# MAGIC MSCK REPAIR TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat_Panel_Historicos;