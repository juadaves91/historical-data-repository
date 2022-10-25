-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ZONA DE RESULTADOS

-- COMMAND ----------

-- DBTITLE 1,Variables de entrada
-- MAGIC %python
-- MAGIC """Archivo JSON a validar"""
-- MAGIC dbutils.widgets.text("from_create_schema", "","")
-- MAGIC dbutils.widgets.get("from_create_schema")
-- MAGIC from_create_schema = str(getArgument("from_create_schema"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def exc_bck(from_create_schema):  
-- MAGIC   if from_create_schema == '' or from_create_schema == 'False':
-- MAGIC     return False
-- MAGIC   elif from_create_schema == 'True':
-- MAGIC     return True

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from_create_schema_aux = str(getArgument("from_create_schema")).strip()
-- MAGIC from_create_schema = exc_bck(from_create_schema_aux)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(from_create_schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.notebook.run('/lotus/lotus-cayman/4_SaveSchema', 3600)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC   sqlContext.sql("refresh table process.zdp_lotus_cayman_dimtiempo")
-- MAGIC   sqlContext.sql(" \
-- MAGIC     INSERT OVERWRITE TABLE lotus_cayman.zdr_lotus_cayman_cattiempo \
-- MAGIC     SELECT \
-- MAGIC       fecha, \
-- MAGIC       date_format(fecha, 'dd/MM/yyyy') fecha_solicitudes, \
-- MAGIC       anio, \
-- MAGIC       trimestre, \
-- MAGIC       num_mes, \
-- MAGIC       semana_mes, \
-- MAGIC       nombre_mes, \
-- MAGIC       nombre_mes_abre, \
-- MAGIC       dias_semana, \
-- MAGIC       nombre_dia_semana, \
-- MAGIC       nombre_dia_semana_abre, \
-- MAGIC       dia_Anio \
-- MAGIC     FROM process.zdp_lotus_cayman_dimtiempo")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC     INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat1 \
-- MAGIC     SELECT * FROM Process.Zdp_Lotus_Cayman_Cat1")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat2 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat3 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat3")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat4 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat4")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat5 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat5")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat6 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat6")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat7 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat7")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat8 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat8")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat9 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat9")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC    sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Cat10 \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_Cat10")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC   sqlContext.sql("refresh table Process.Zdp_Lotus_Cayman_Anexos")
-- MAGIC   sqlContext.sql("INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Anexos \
-- MAGIC                    SELECT * FROM Process.Zdp_Lotus_Cayman_Anexos")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC   sqlContext.sql("refresh table lotus_cayman.Zdr_Lotus_Cayman_SolicitudesHijos")
-- MAGIC   sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_SolicitudesHijos \
-- MAGIC    SELECT * FROM Process.Zdp_Lotus_Cayman_SolicitudesHijos")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC   sqlContext.sql("refresh table lotus_cayman.Zdr_Lotus_Cayman_Solicitudes")
-- MAGIC   sqlContext.sql(" \
-- MAGIC    INSERT OVERWRITE TABLE lotus_cayman.Zdr_Lotus_Cayman_Solicitudes \
-- MAGIC SELECT \
-- MAGIC    	UniversalID, \
-- MAGIC     NUMTOTCMLACT, \
-- MAGIC     NUMTOTCMLSOL, \
-- MAGIC     NUMTOTCMLPIC, \
-- MAGIC     NUMTOTCMLRA, \
-- MAGIC     txtRadicado, \
-- MAGIC     dtmCreado, \
-- MAGIC     regexp_replace(if(length(substring_index(dtmCreado,' ',1))=10,substring_index(dtmCreado,' ',1),concat('0',substring_index(dtmCreado,' ',1))), '-', '/') as Fecha_Solicitudes, \
-- MAGIC     txtAutor, \
-- MAGIC     txtCodAutor, \
-- MAGIC     cbTipoSolicitud, \
-- MAGIC     cbTipoCliente, \
-- MAGIC     txtEstado, \
-- MAGIC     cbNomTipoSolicitud, \
-- MAGIC     txtTipoClienteVis, \
-- MAGIC     txtResponsable, \
-- MAGIC     dtmDecision, \
-- MAGIC     txtAprobadorC, \
-- MAGIC     txtCodOficial, \
-- MAGIC     txtNumActaJunta, \
-- MAGIC     txtNumIdCorto, \
-- MAGIC     txtCalifSuperbancaria, \
-- MAGIC     txtTipoId, \
-- MAGIC     txtCalifInternaAnterior, \
-- MAGIC     txtNomCliente, \
-- MAGIC     txtCalifAnioParcial, \
-- MAGIC     txtRegion, \
-- MAGIC     txtCalifUltimoAnio, \
-- MAGIC     txtZona, \
-- MAGIC     txtSegmento, \
-- MAGIC     txtSector, \
-- MAGIC     txtActividadEconomica, \
-- MAGIC     txtCodigoCIIU, \
-- MAGIC     txtGerente, \
-- MAGIC     txtCodGerente, \
-- MAGIC     numPromTrimCtaCorriente, \
-- MAGIC     txtCentroCostos, \
-- MAGIC     numPromTrimCtaAhorro, \
-- MAGIC     txtGrupoRiesgo, \
-- MAGIC     txtCodRiesgo, \
-- MAGIC     numDeudaActual, \
-- MAGIC     dtmNacimiento, \
-- MAGIC     dtmVinculacion, \
-- MAGIC     txtCentralIxInterna, \
-- MAGIC     txtCentralIxExterna, \
-- MAGIC     numCEGTotEntidades, \
-- MAGIC     dtmCamComConstitucion, \
-- MAGIC     txtCamComVigencia, \
-- MAGIC     txtCamComActEconomica, \
-- MAGIC     txtCamComEmbargo, \
-- MAGIC     dtmCamComExpedicion, \
-- MAGIC     txtCamComCertificadoVigente, \
-- MAGIC     txtIdRepLegal, \
-- MAGIC     txtTipoIdRepLegal, \
-- MAGIC     txtNombreRepLegal, \
-- MAGIC     txtFacultadRepLegal, \
-- MAGIC     numCartSol, \
-- MAGIC     numCartPIC, \
-- MAGIC     numCartAsi, \
-- MAGIC     numCartGlbAsig, \
-- MAGIC     numCartGlbDis, \
-- MAGIC     txtCarRec, \
-- MAGIC     txtCarJus, \
-- MAGIC     numBienesGRA, \
-- MAGIC     numTotAvalGRA, \
-- MAGIC     numBienesGRO, \
-- MAGIC     numTotAvalGRO, \
-- MAGIC     numTotCubGRO, \
-- MAGIC     txtObsGarantias, \
-- MAGIC     txtRecComReciente, \
-- MAGIC     txtRecCadReciente, \
-- MAGIC     txtObsDocumentos, \
-- MAGIC     numCartAct, \
-- MAGIC     numCartGlbAsigC, \
-- MAGIC     numCartGlbDisC, \
-- MAGIC     txtCarRecC, \
-- MAGIC     txtCarJusC, \
-- MAGIC     translate(busqueda,'áéíóúÁÉÍÓÚñÑ','aeiouAEIOUnN')  AS Busqueda \
-- MAGIC FROM Process.Zdp_Lotus_Cayman_Solicitudes")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##CREACIO DE MASIVOS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if from_create_schema:
-- MAGIC   masivos_cayman_df = sqlContext.sql(" \
-- MAGIC    SELECT \
-- MAGIC     ZDC.UniversalID, \
-- MAGIC     ZDC.txtAprobadorC.value NombreQuienAprueba, \
-- MAGIC     ZDC.txtAutor.value NombreAutor, \
-- MAGIC     ZDC.txtNomCliente.value NombreRazonSocial, \
-- MAGIC     ZDC.txtNumIdCorto.value NumeroIdentificacion, \
-- MAGIC     ZDC.txtRadicado.value SolicitudTarjetaCredito, \
-- MAGIC     ZDC.dtmCreado.value FechaCreacion, \
-- MAGIC     ZDC.cbNomTipoSolicitud.value TipoSolicitud, \
-- MAGIC     ZDC.txtTipoClienteVis.value TipoCliente, \
-- MAGIC     ZDC.dtmDecision.value FechaDecision, \
-- MAGIC     ZDC.txtAprobadorC.value NombreQuienSprueba, \
-- MAGIC     ZDC.txtCodOficial.value CodigoQuienAprueba, \
-- MAGIC     ZDC.txtNumActaJunta.value NumeroActaComiteCredito, \
-- MAGIC     ZDC.txtTipoId.value TipoIdentificacion, \
-- MAGIC     ZDC.txtNomCliente.value NombreApellidosRazonSocial, \
-- MAGIC     ZDC.txtRegion.value Region, \
-- MAGIC     ZDC.txtZona.value Zona, \
-- MAGIC     ZDC.txtSegmento.value Segmento, \
-- MAGIC     ZDC.txtSector.value Sector, \
-- MAGIC     ZDC.txtActividadEconomica.value ActividadEconomicaCliente, \
-- MAGIC     ZDC.txtCodigoCIIU.value CodigoCIIU, \
-- MAGIC     ZDC.txtGerente.value Gerente, \
-- MAGIC     ZDC.txtCodGerente.value CodigoGerente, \
-- MAGIC     ZDC.txtCentroCostos.value CentroCostos, \
-- MAGIC     ZDC.txtGrupoRiesgo.value GrupoRiesgo, \
-- MAGIC     ZDC.txtCodRiesgo.value CodigoRiesgo, \
-- MAGIC     ZDC.dtmNacimiento.value FechaNacimiento, \
-- MAGIC     ZDC.dtmVinculacion.value FechaVinculacion, \
-- MAGIC     ZDC.txtCalifSuperbancaria.value Superbancaria, \
-- MAGIC     ZDC.txtCalifInternaAnterior.value InternaAnterior, \
-- MAGIC     ZDC.txtCalifAnioParcial.value AnoParcial, \
-- MAGIC     ZDC.txtCalifUltimoAnio.value ultimoAno, \
-- MAGIC     ZDC.numPromTrimCtaCorriente.value Corriente, \
-- MAGIC     ZDC.numPromTrimCtaAhorro.value Ahorro, \
-- MAGIC     ZDC.numDeudaActual.value DeudaActual, \
-- MAGIC     ZDC.txtCentralIxInterna.value CentralIxInterna, \
-- MAGIC     ZDC.txtIdRepLegal.value NumeroIdentificacionLegal, \
-- MAGIC     ZDC.txtTipoIdRepLegal.value IdTipoIdentificacion, \
-- MAGIC     ZDC.txtNombreRepLegal.value Nombre, \
-- MAGIC     ZDC.txtFacultadRepLegal.value Facultad, \
-- MAGIC     CINFIN.Entidad Entidad, \
-- MAGIC     CINFIN.Total Total, \
-- MAGIC     CINFIN.Participacion_ultimo_trimestre ParticipacionUltimoTrimestre, \
-- MAGIC     CINFIN.Variacion_monto VariacionMonto, \
-- MAGIC     CINFIN.Crecimiento Crecimiento, \
-- MAGIC     HIJOS.txtnumidcorto, \
-- MAGIC     HIJOS.txttipoid, \
-- MAGIC     HIJOS.txtnomcliente, \
-- MAGIC     HIJOS.dtmnacimiento, \
-- MAGIC     HIJOS.txtactividadeconomica, \
-- MAGIC     HIJOS.txtcentralixinterna, \
-- MAGIC     HIJOS.txtcentralixexterna, \
-- MAGIC     HIJOS.dtmvinculacion, \
-- MAGIC     CUTAR.posicion PocisionCupoTarjeta, \
-- MAGIC     CUTAR.Producto, \
-- MAGIC     CUTAR.Detalle, \
-- MAGIC     CUTAR.CupoActualAsignado, \
-- MAGIC     CUTAR.CupoAdicionalSolicitado, \
-- MAGIC     CUTAR.CupoAdicionalPIC, \
-- MAGIC     CUTAR.CupoAdicionalAprobado, \
-- MAGIC     RECOM.posicion PocisionRecomendacion, \
-- MAGIC     RECOM.informacion, \
-- MAGIC     RECOM.Recomendacion, \
-- MAGIC     DET.TipoDeClientesAsociados, \
-- MAGIC     DET.NombresClientesAvalistas, \
-- MAGIC     DET.CupoActualAsignado CupoActualAsignadoDetalle, \
-- MAGIC     DET.CupoAdicionalSolicitado CupoAdicionalSolicitadoDetalle, \
-- MAGIC     DET.CupoAdicionalPIC CupoAdicionalPICDetalle, \
-- MAGIC     DET.CupoAdicionalAprobado CupoAdicionalAprobadoDetalle, \
-- MAGIC     DET.NumeroDeTarjeta, \
-- MAGIC     DET.FechaDeVencimiento \
-- MAGIC   FROM \
-- MAGIC     Process.Zdp_Lotus_Cayman_Tablaintermedia ZDC \
-- MAGIC     LEFT JOIN lotus_cayman.zdr_uvw_lotus_cayman_cifin CINFIN ON CINFIN.UniversalID = ZDC.UniversalID \
-- MAGIC     LEFT JOIN Process.Zdp_Lotus_Cayman_SolicitudesHijos HIJOS ON HIJOS.UniversalID = ZDC.UniversalID \
-- MAGIC     LEFT JOIN lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta CUTAR ON CUTAR.UniversalID = ZDC.UniversalID \
-- MAGIC     LEFT JOIN  lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_recomendacion RECOM ON RECOM.UniversalID = ZDC.UniversalID \
-- MAGIC     LEFT JOIN lotus_cayman.zdr_uvw_lotus_cayman_cupotarjeta_detalle DET ON DET.UniversalID = ZDC.UniversalID")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC """Dataframe Panel Masivos"""
-- MAGIC if from_create_schema:
-- MAGIC   masivos_cayman_df.write.format("delta").mode("overwrite").save("/mnt/lotus-cayman/results/masivos")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS Lotus_Cayman.Zdr_Lotus_Cayman_Masivos;
-- MAGIC CREATE TABLE Lotus_Cayman.Zdr_Lotus_Cayman_Masivos
-- MAGIC USING DELTA
-- MAGIC LOCATION '/mnt/lotus-cayman/results/masivos/';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC OPTIMIZE Lotus_Cayman.Zdr_Lotus_Cayman_Masivos ZORDER BY (UniversalId)
